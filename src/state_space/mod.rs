pub mod cache;
pub mod discovery;
pub mod error;
pub mod filters;

use crate::amms::amm::AmmId;
use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::factory::Factory;

use alloy::consensus::BlockHeader;
use alloy::eips::BlockId;
use alloy::rpc::types::{Block, Filter, FilterSet, Log};
use alloy::{
    network::Network,
    primitives::{Address, B256, FixedBytes},
    providers::Provider,
};
use async_stream::stream;
use cache::StateChange;
use cache::StateChangeCache;

use error::StateSpaceError;
use filters::AMMFilter;
use filters::PoolFilter;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;

pub const CACHE_SIZE: usize = 30;

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    pub latest_block: Arc<AtomicU64>,
    // discovery_manager: Option<DiscoveryManager>,
    pub block_filter: Filter,
    pub provider: P,
    /// Phase 8+: Factories retained at runtime so that incoming `Initialize`/`PoolCreated` logs
    /// can be auto-routed: if either token of a freshly created pool is already in `by_token`,
    /// the pool is initialized via the matching factory and added to the tracked set.
    pub factories: Vec<Factory>,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceManager<N, P>
where
    N: Network,
    P: Provider<N> + Clone,
{
    /// Phase 7+: construct a token-first manager without factory-first historical discovery.
    /// The initial `StateSpace` is whatever the caller has populated (typically empty, then
    /// driven by Redis TRACK messages). `factories` is retained so auto-track and the live
    /// subscribe loop know which singletons to watch.
    pub fn from_state_and_factories(
        state: StateSpace,
        factories: Vec<Factory>,
        latest_block: u64,
        provider: P,
    ) -> Self {
        let mut sigs = HashSet::new();
        for f in &factories {
            for e in f.pool_events() {
                sigs.insert(e);
            }
            sigs.insert(f.discovery_event());
        }
        let block_filter = Filter::new().event_signature(FilterSet::from(
            sigs.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        Self {
            state: Arc::new(RwLock::new(state)),
            latest_block: Arc::new(AtomicU64::new(latest_block)),
            block_filter,
            provider,
            factories,
            phantom: PhantomData,
        }
    }
}

impl<N, P> StateSpaceManager<N, P> {
    pub async fn subscribe(
        &self,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<AmmId>, StateSpaceError>> + Send>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<BlockResponse = Block>,
    {
        let provider = self.provider.clone();
        let latest_block = self.latest_block.clone();
        let state = self.state.clone();
        let factories = self.factories.clone();
        let mut block_filter = self.block_filter.clone();

        // Phase 8+: build a quick lookup `singleton_or_factory_addr -> creation_event_sig` so
        // we can split logs into "creation" vs "sync" buckets per block.
        let creation_lookup: HashMap<B256, Factory> = factories
            .iter()
            .map(|f| (f.discovery_event(), f.clone()))
            .collect();

        let block_stream = provider.subscribe_blocks().await?.into_stream();

        Ok(Box::pin(stream! {
            tokio::pin!(block_stream);

            while let Some(block) = block_stream.next().await {
                let block_number = block.number();
                block_filter = block_filter.select(block_number);

                let logs = provider.get_logs(&block_filter).await?;

                // Phase 8+: split out pool-creation logs and run them through `auto_track` ahead of
                // regular sync. We do this synchronously per block so that subsequent sync logs in
                // the same batch can hit the freshly tracked pool.
                let (creation_logs, sync_logs): (Vec<Log>, Vec<Log>) = logs
                    .into_iter()
                    .partition(|log| {
                        log.topics()
                            .first()
                            .is_some_and(|sig| creation_lookup.contains_key(sig))
                    });

                for log in creation_logs {
                    if let Err(e) = auto_track_from_log(
                        &log,
                        &creation_lookup,
                        state.clone(),
                        provider.clone(),
                    )
                    .await
                    {
                        tracing::warn!(target: "state_space::auto_track", error = %e, "auto-track failed");
                    }
                }

                let affected_amms = state.write().await.sync(&sync_logs)?;
                latest_block.store(block_number, Ordering::Relaxed);

                yield Ok(affected_amms);
            }
        }))
    }
}

/// Phase 8+: handle a single pool-creation log. Decodes the log to an unsynced AMM, checks
/// whether either token is already tracked; if yes, fully initializes the pool via the
/// factory's `init` and inserts via `track_pools`. Otherwise drops it.
async fn auto_track_from_log<N, P>(
    log: &Log,
    creation_lookup: &HashMap<B256, Factory>,
    state: Arc<RwLock<StateSpace>>,
    provider: P,
) -> Result<(), StateSpaceError>
where
    N: Network,
    P: Provider<N> + Clone,
{
    let Some(sig) = log.topics().first() else {
        return Ok(());
    };
    let Some(factory) = creation_lookup.get(sig) else {
        return Ok(());
    };

    let amm = match factory.create_pool(log.clone()) {
        Ok(a) => a,
        Err(e) => {
            tracing::debug!(target: "state_space::auto_track", error = %e, "create_pool failed");
            return Ok(());
        }
    };
    let tokens = amm.tokens();
    let owning = {
        let s = state.read().await;
        tokens
            .iter()
            .find(|t| s.by_token.contains_key(*t))
            .copied()
    };
    let Some(owning) = owning else {
        return Ok(());
    };

    let block = log
        .block_number
        .map(BlockId::from)
        .unwrap_or(BlockId::latest());
    let synced = factory
        .sync(vec![amm], block, provider)
        .await
        .map_err(StateSpaceError::AMMError)?;

    state.write().await.track_pools(synced, owning);
    Ok(())
}

// TODO: Drop impl, create a checkpoint
#[derive(Debug, Default)]
pub struct StateSpaceBuilder<N, P> {
    pub provider: P,
    pub latest_block: u64,
    pub factories: Vec<Factory>,
    pub amms: Vec<AMM>,
    pub filters: Vec<PoolFilter>,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceBuilder<N, P>
where
    N: Network,
    P: Provider<N> + Clone + 'static,
{
    pub fn new(provider: P) -> StateSpaceBuilder<N, P> {
        Self {
            provider,
            latest_block: 0,
            factories: vec![],
            amms: vec![],
            filters: vec![],
            // discovery: false,
            phantom: PhantomData,
        }
    }

    pub fn block(self, latest_block: u64) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder {
            latest_block,
            ..self
        }
    }

    pub fn with_factories(self, factories: Vec<Factory>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { factories, ..self }
    }

    pub fn with_amms(self, amms: Vec<AMM>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { amms, ..self }
    }

    pub fn with_filters(self, filters: Vec<PoolFilter>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { filters, ..self }
    }

    pub async fn sync(self) -> Result<StateSpaceManager<N, P>, AMMError> {
        let chain_tip = BlockId::from(self.provider.get_block_number().await?);
        let factories = self.factories.clone();
        let mut futures = FuturesUnordered::new();

        let mut filter_set = HashSet::new();
        for factory in &self.factories {
            for event in factory.pool_events() {
                filter_set.insert(event);
            }
            // Phase 8+: also subscribe to pool-creation events so that auto-track can catch
            // newly created pools whose tokens are already in the tracked set.
            filter_set.insert(factory.discovery_event());
        }

        for amm in self.amms.iter() {
            for event in amm.sync_events() {
                filter_set.insert(event);
            }
        }

        let block_filter = Filter::new().event_signature(FilterSet::from(
            filter_set.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        let mut amm_variants = HashMap::new();
        for amm in self.amms.into_iter() {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        for factory in factories {
            let provider = self.provider.clone();
            let filters = self.filters.clone();

            let extension = amm_variants.remove(&factory.variant());
            futures.push(tokio::spawn(async move {
                let mut discovered_amms = factory.discover(chain_tip, provider.clone()).await?;

                if let Some(amms) = extension {
                    discovered_amms.extend(amms);
                }

                // Apply discovery filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Discovery {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Discovery filter"
                        );
                    }
                }

                discovered_amms = factory.sync(discovered_amms, chain_tip, provider).await?;

                // Apply sync filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Sync {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Sync filter"
                        );
                    }
                }

                Ok::<Vec<AMM>, AMMError>(discovered_amms)
            }));
        }

        let mut state_space = StateSpace::default();
        while let Some(res) = futures.next().await {
            let synced_amms = res??;

            for amm in synced_amms {
                state_space.state.insert(amm.id(), amm);
            }
        }

        // Sync remaining AMM variants
        for (_, remaining_amms) in amm_variants.drain() {
            for mut amm in remaining_amms {
                amm = amm.init(chain_tip, self.provider.clone()).await?;
                state_space.state.insert(amm.id(), amm);
            }
        }

        // Collect singleton addresses for V4-family pools. All V4 logs route through these,
        // so `StateSpace::route_log` uses this set to decide whether to key by `log.address()`
        // or by `(singleton, topics[1])`.
        //
        // Note: we do NOT add `.address(singletons)` to `block_filter`. The address field of
        // `alloy::Filter` ANDs with `event_signature`, which would silently drop every V2/V3
        // log if any V4 singleton were registered. V4 events are pulled globally by signature
        // and second-filtered against `state` in `StateSpace::sync`, matching the V2/V3 path.
        //
        // Phase 5+: also back-fill `by_token` so the dual index is consistent regardless of
        // whether pools entered via factory-first discovery or token-first `track_pools`.
        for (amm_id, amm) in state_space.state.iter() {
            if let AmmId::V4 { singleton, .. } = amm_id {
                state_space.singletons.insert(*singleton);
            }
            for token in amm.tokens() {
                state_space
                    .by_token
                    .entry(token)
                    .or_default()
                    .insert(*amm_id);
            }
        }

        Ok(StateSpaceManager {
            latest_block: Arc::new(AtomicU64::new(self.latest_block)),
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.provider,
            factories: self.factories,
            phantom: PhantomData,
        })
    }
}

#[derive(Debug, Default)]
pub struct StateSpace {
    pub state: HashMap<AmmId, AMM>,
    /// Addresses of V4-family singleton contracts (Uniswap V4 `PoolManager`,
    /// PancakeSwap Infinity `CLPoolManager`). Logs from these addresses are routed by
    /// `topics[1]` (PoolId) instead of `log.address()`.
    pub singletons: HashSet<Address>,
    pub latest_block: Arc<AtomicU64>,
    cache: StateChangeCache<CACHE_SIZE>,
    /// Phase 5+: reverse index `token -> set of pool ids that contain this token`. Populated by
    /// `track_pools` and by `StateSpaceBuilder::sync` (back-fill for factory-first path).
    pub by_token: HashMap<Address, HashSet<AmmId>>,
    /// Phase 5+: tokens that are protected from untrack (USDT/USDC/WBNB/BUSD etc.). Injected at
    /// startup from config; never removed at runtime.
    pub core_tokens: HashSet<Address>,
}

/// Result of a successful `untrack_token` call.
#[derive(Debug, Clone)]
pub struct UntrackReport {
    pub token: Address,
    pub removed_pool_ids: Vec<AmmId>,
}

#[derive(Debug, thiserror::Error)]
pub enum UntrackError {
    #[error("token {0} is a core token and cannot be untracked")]
    CoreTokenProtected(Address),
}

impl StateSpace {
    pub fn get(&self, id: &AmmId) -> Option<&AMM> {
        self.state.get(id)
    }

    pub fn get_mut(&mut self, id: &AmmId) -> Option<&mut AMM> {
        self.state.get_mut(id)
    }

    /// Phase 5+: insert a batch of pools into the state space. Each pool is indexed under all of
    /// its tokens in `by_token`. `owning_token` is the token whose discovery triggered this batch
    /// (used for cache attribution); it is also indexed so the bucket exists even if no pool ends
    /// up referencing it.
    pub fn track_pools(&mut self, pools: Vec<AMM>, owning_token: Address) {
        for pool in pools {
            let id = pool.id();
            for token in pool.tokens() {
                self.by_token.entry(token).or_default().insert(id);
            }
            if let AmmId::V4 { singleton, .. } = &id {
                self.singletons.insert(*singleton);
            }
            self.state.insert(id, pool);
        }
        self.by_token.entry(owning_token).or_default();
    }

    /// Phase 5+: hard-delete a token. Removes every pool containing this token from `state`, drops
    /// the token's bucket in `by_token`, and clears dangling references to those pools in other
    /// buckets. Core tokens are protected.
    pub fn untrack_token(&mut self, token: Address) -> Result<UntrackReport, UntrackError> {
        if self.core_tokens.contains(&token) {
            return Err(UntrackError::CoreTokenProtected(token));
        }
        let Some(pool_ids) = self.by_token.remove(&token) else {
            return Ok(UntrackReport {
                token,
                removed_pool_ids: vec![],
            });
        };
        let removed_vec: Vec<AmmId> = pool_ids.iter().copied().collect();
        for id in &removed_vec {
            self.state.remove(id);
        }
        for set in self.by_token.values_mut() {
            set.retain(|id| !pool_ids.contains(id));
        }
        Ok(UntrackReport {
            token,
            removed_pool_ids: removed_vec,
        })
    }

    /// Phase 5+: list of tokens currently tracked (have at least one indexed bucket).
    pub fn tracked_tokens(&self) -> Vec<Address> {
        self.by_token.keys().copied().collect()
    }

    /// Phase 5+: inject core tokens. These are protected from `untrack_token`.
    pub fn set_core_tokens(&mut self, tokens: impl IntoIterator<Item = Address>) {
        self.core_tokens = tokens.into_iter().collect();
    }

    /// Resolve a sync log to the AmmId of the pool it targets. V4 logs are routed by `topics[1]`
    /// (PoolId) under a known singleton; everything else routes by `log.address()`.
    fn route_log(&self, log: &Log) -> Option<AmmId> {
        let log_address = log.address();
        if self.singletons.contains(&log_address) {
            let pool_id = B256::from(log.topics().get(1).copied()?);
            Some(AmmId::V4 {
                singleton: log_address,
                pool_id,
            })
        } else {
            Some(AmmId::Address(log_address))
        }
    }

    pub fn sync(&mut self, logs: &[Log]) -> Result<Vec<AmmId>, StateSpaceError> {
        let latest = self.latest_block.load(Ordering::Relaxed);
        let Some(mut block_number) = logs
            .first()
            .map(|log| log.block_number.ok_or(StateSpaceError::MissingBlockNumber))
            .transpose()?
        else {
            return Ok(vec![]);
        };

        // Check if there is a reorg and unwind to state before block_number
        if latest >= block_number {
            info!(
                target: "state_space::sync",
                from = %latest,
                to = %block_number - 1,
                "Unwinding state changes"
            );

            let cached_state = self.cache.unwind_state_changes(block_number);
            for amm in cached_state {
                debug!(target: "state_space::sync", ?amm, "Reverting AMM state");
                self.state.insert(amm.id(), amm);
            }
        }

        let mut cached_amms = HashSet::new();
        let mut affected_amms = HashSet::new();
        for log in logs {
            // If the block number is updated, cache the current block state changes
            let log_block_number = log
                .block_number
                .ok_or(StateSpaceError::MissingBlockNumber)?;
            if log_block_number != block_number {
                let amms = cached_amms.drain().collect::<Vec<AMM>>();
                affected_amms.extend(amms.iter().map(|amm| amm.id()));
                let state_change = StateChange::new(amms, block_number);

                debug!(
                    target: "state_space::sync",
                    state_change = ?state_change,
                    "Caching state change"
                );

                self.cache.push(state_change);
                block_number = log_block_number;
            }

            // If the AMM is in the state space add the current state to cache and sync from log
            let Some(key) = self.route_log(log) else {
                continue;
            };
            if let Some(amm) = self.state.get_mut(&key) {
                cached_amms.insert(amm.clone());
                amm.sync(log)?;

                info!(
                    target: "state_space::sync",
                    ?amm,
                    "Synced AMM"
                );
            }
        }

        if !cached_amms.is_empty() {
            let amms = cached_amms.drain().collect::<Vec<AMM>>();
            affected_amms.extend(amms.iter().map(|amm| amm.id()));
            let state_change = StateChange::new(amms, block_number);

            debug!(
                target: "state_space::sync",
                state_change = ?state_change,
                "Caching state change"
            );

            self.cache.push(state_change);
        }

        Ok(affected_amms.into_iter().collect())
    }
}

#[macro_export]
macro_rules! sync {
    // Sync factories with provider
    ($factories:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .sync()
            .await?
    }};

    // Sync factories with filters
    ($factories:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_filters($filters)
            .sync()
            .await?
    }};

    ($factories:expr, $amms:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_amms($amms)
            .with_filters($filters)
            .sync()
            .await?
    }};
}
