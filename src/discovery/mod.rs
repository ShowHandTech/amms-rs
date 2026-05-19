//! Phase 6+: token-first pool discovery.
//!
//! Discovery here is decoupled from the on-chain `Initialize`-event scan that powers
//! `Factory::discover`. Instead a `TokenPoolIndex` (currently DexTools) is asked "which pools
//! contain this token", and each `PoolDescriptor` is turned into an unsynced `AMM` via the
//! appropriate factory's `from_descriptor`. The result is then handed to `sync_all_pools`.
//!
//! This module is dependency-free at runtime — `TokenPoolIndex` impls live in submodules
//! (`dextools/`) so future indexes (e.g. The Graph) can be added without touching callers.

use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::factory::Factory;
use alloy::eips::BlockId;
use alloy::network::Network;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use async_trait::async_trait;
use thiserror::Error;

pub mod dextools;
pub mod subgraph;

/// A pool reference returned by an external index, normalized enough to be turned into an
/// unsynced `AMM` by the matching factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolDescriptor {
    /// Constant-product (UniswapV2 / PancakeV2 / SushiV2 / forks). `fee` is in the same
    /// `(100000 - fee) / 100000` units the factory expects (e.g. `300` for 30 bps).
    V2 {
        address: Address,
        factory: Address,
        fee: usize,
    },
    /// UniswapV3 / PancakeV3 / forks. `fee` and `tick_spacing` are pulled per-pool at sync time
    /// from on-chain state, so we only need the pool address here.
    V3 { address: Address, factory: Address },
    /// PancakeSwap V4 (Infinity CL). PCS exposes `poolIdToPoolKey()` so we can fall back to
    /// chain lookups if `currency0`/`currency1` are missing from the index response.
    PcsV4Cl {
        pool_id: B256,
        cl_pool_manager: Address,
        currency0: Option<Address>,
        currency1: Option<Address>,
        hooks: Option<Address>,
        parameters: Option<B256>,
    },
    /// Uniswap V4. No on-chain `getPoolKey(id)` exists, so `currency0`/`currency1`/`fee`/
    /// `tick_spacing`/`hooks` must come from somewhere — Phase 8 Initialize-log auto-track gives
    /// them all, DexTools list gives only `currency0`/`currency1` (mainToken/sideToken sorted)
    /// and `fee` (parsed from the percentage float). `tick_spacing` and `hooks` are unresolved
    /// from DexTools alone and must be filled by `enrich_uniswap_v4_via_subgraph` before any
    /// `Factory::from_descriptor` consumes the value.
    UniswapV4 {
        pool_id: B256,
        pool_manager: Address,
        currency0: Option<Address>,
        currency1: Option<Address>,
        fee: Option<u32>,
        tick_spacing: Option<i32>,
        hooks: Option<Address>,
    },
}

impl PoolDescriptor {
    /// Returns the singleton (for V4) or the factory address (for V2/V3), used by routing logic.
    pub fn singleton_or_factory(&self) -> Address {
        match self {
            PoolDescriptor::V2 { factory, .. } => *factory,
            PoolDescriptor::V3 { factory, .. } => *factory,
            PoolDescriptor::PcsV4Cl {
                cl_pool_manager, ..
            } => *cl_pool_manager,
            PoolDescriptor::UniswapV4 { pool_manager, .. } => *pool_manager,
        }
    }
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error(transparent)]
    AMMError(#[from] AMMError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("missing required field in index response: {0}")]
    MissingField(&'static str),
    #[error("no factory registered for descriptor singleton/factory {0}")]
    NoMatchingFactory(Address),
    #[error("index returned malformed data: {0}")]
    Malformed(String),
}

/// External token → pools index. DexTools is the first implementation; The Graph or other
/// sources can be plugged in by implementing this trait.
#[async_trait]
pub trait TokenPoolIndex: Send + Sync {
    /// Return descriptors for every pool containing `token` on `chain`. `chain` is the
    /// implementation-specific chain slug (e.g. DexTools uses `"bsc"`, `"ether"`).
    async fn pools_for_token(
        &self,
        chain: &str,
        token: Address,
    ) -> Result<Vec<PoolDescriptor>, DiscoveryError>;
}

/// Translate descriptors into unsynced `AMM` instances by dispatching to the matching factory.
/// `factories` is matched by `Factory::address()` for V2/V3 (factory address) or by the V4
/// singleton address (encoded inside the factory) for V4-family.
///
/// Descriptors whose factory/singleton isn't in `factories` (e.g. a SushiSwap pool when only
/// PancakeSwap is configured) are dropped with a debug log — DexTools returns every DEX the
/// token trades on, so unknown-factory drops are expected, not an error. Same for
/// `from_descriptor` failures (e.g. enrichment missed a PoolKey field): warn and skip rather
/// than failing the whole batch.
pub fn descriptors_to_amms(descriptors: Vec<PoolDescriptor>, factories: &[Factory]) -> Vec<AMM> {
    let mut out = Vec::with_capacity(descriptors.len());
    let mut dropped_unknown_factory = 0usize;
    let mut dropped_from_descriptor: Vec<(Address, AMMError)> = Vec::new();
    for desc in descriptors {
        let singleton_or_factory = desc.singleton_or_factory();
        let Some(factory) = factories
            .iter()
            .find(|f| f.address() == singleton_or_factory)
        else {
            dropped_unknown_factory += 1;
            tracing::debug!(
                target: "discovery",
                factory = %singleton_or_factory,
                "no factory configured for descriptor — skipping pool"
            );
            continue;
        };
        match factory.from_descriptor(&desc) {
            Ok(amm) => out.push(amm),
            Err(e) => dropped_from_descriptor.push((singleton_or_factory, e)),
        }
    }
    if dropped_unknown_factory > 0 {
        tracing::info!(
            target: "discovery",
            count = dropped_unknown_factory,
            "skipped pools from factories not in config"
        );
    }
    for (factory, err) in &dropped_from_descriptor {
        tracing::warn!(
            target: "discovery",
            %factory,
            error = %err,
            "from_descriptor failed — skipping pool"
        );
    }
    out
}

/// Discover pools for a single token through `index`, materialize them via matching factories,
/// then drive them through `Factory::sync` to fully initialize state.
///
/// `subgraph` is optional. When supplied it is used to resolve PoolKey fields that DexTools
/// doesn't return for Uniswap V4 (tickSpacing, hooks). Without it, UniV4 descriptors with
/// missing fields are dropped with a warn.
pub async fn discover_and_sync_for_token<N, P>(
    chain: &str,
    token: Address,
    index: &dyn TokenPoolIndex,
    factories: &[Factory],
    block: BlockId,
    provider: P,
) -> Result<Vec<AMM>, DiscoveryError>
where
    N: Network,
    P: Provider<N> + Clone,
{
    discover_and_sync_for_token_with_subgraph(
        chain, token, index, None, factories, block, provider,
    )
    .await
}

/// Same as `discover_and_sync_for_token` with explicit subgraph for UniV4 PoolKey enrichment.
pub async fn discover_and_sync_for_token_with_subgraph<N, P>(
    chain: &str,
    token: Address,
    index: &dyn TokenPoolIndex,
    subgraph: Option<&subgraph::SubgraphClient>,
    factories: &[Factory],
    block: BlockId,
    provider: P,
) -> Result<Vec<AMM>, DiscoveryError>
where
    N: Network,
    P: Provider<N> + Clone,
{
    let mut descriptors = index.pools_for_token(chain, token).await?;
    enrich_uniswap_v4_via_subgraph(&mut descriptors, subgraph).await;
    let unsynced = descriptors_to_amms(descriptors, factories);
    // Group by factory address and sync per-factory (each factory only knows how to sync its own).
    let mut grouped: std::collections::HashMap<Address, Vec<AMM>> =
        std::collections::HashMap::new();
    for amm in unsynced {
        let key = match &amm {
            AMM::UniswapV2Pool(_)
            | AMM::UniswapV3Pool(_)
            | AMM::ERC4626Vault(_)
            | AMM::BalancerPool(_) => factories
                .iter()
                .find(|f| matches_amm_variant(f, &amm))
                .map(|f| f.address()),
            AMM::UniswapV4Pool(p) => Some(p.pool_manager),
            AMM::PancakeV4CLPool(p) => Some(p.cl_pool_manager),
        }
        .ok_or_else(|| {
            DiscoveryError::Malformed(format!("could not group AMM {:?} by factory", amm))
        })?;
        grouped.entry(key).or_default().push(amm);
    }
    let mut synced = Vec::new();
    for (factory_addr, amms) in grouped {
        let factory = factories
            .iter()
            .find(|f| f.address() == factory_addr)
            .ok_or(DiscoveryError::NoMatchingFactory(factory_addr))?;
        let s = factory.sync(amms, block, provider.clone()).await?;
        synced.extend(s);
    }
    Ok(synced)
}

fn matches_amm_variant(factory: &Factory, amm: &AMM) -> bool {
    factory.variant() == amm.variant()
}

/// In-place fill of `currency0`/`currency1`/`fee`/`tick_spacing`/`hooks` for any
/// `PoolDescriptor::UniswapV4` whose corresponding fields are `None`. Descriptors that still
/// have missing fields after this pass (subgraph not provided, or subgraph lookup failed) are
/// dropped with a `warn!`.
pub async fn enrich_uniswap_v4_via_subgraph(
    descriptors: &mut Vec<PoolDescriptor>,
    subgraph: Option<&subgraph::SubgraphClient>,
) {
    let mut indices_needing_lookup: Vec<usize> = Vec::new();
    for (i, d) in descriptors.iter().enumerate() {
        if let PoolDescriptor::UniswapV4 {
            currency0,
            currency1,
            fee,
            tick_spacing,
            hooks,
            ..
        } = d
        {
            if currency0.is_none()
                || currency1.is_none()
                || fee.is_none()
                || tick_spacing.is_none()
                || hooks.is_none()
            {
                indices_needing_lookup.push(i);
            }
        }
    }
    if indices_needing_lookup.is_empty() {
        return;
    }

    let Some(sg) = subgraph else {
        tracing::warn!(
            target: "discovery::enrich",
            count = indices_needing_lookup.len(),
            "Uniswap V4 descriptors need PoolKey lookup but no subgraph configured — dropping"
        );
        let to_drop: std::collections::HashSet<usize> =
            indices_needing_lookup.iter().copied().collect();
        let mut i = 0;
        descriptors.retain(|_| {
            let keep = !to_drop.contains(&i);
            i += 1;
            keep
        });
        return;
    };

    let mut to_drop: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for i in indices_needing_lookup {
        let PoolDescriptor::UniswapV4 {
            pool_id,
            currency0,
            currency1,
            fee,
            tick_spacing,
            hooks,
            ..
        } = &mut descriptors[i]
        else {
            continue;
        };
        match sg.uniswap_v4_pool_key(*pool_id).await {
            Ok(key) => {
                if currency0.is_none() {
                    *currency0 = Some(key.currency0);
                }
                if currency1.is_none() {
                    *currency1 = Some(key.currency1);
                }
                if fee.is_none() {
                    *fee = Some(key.fee);
                }
                if tick_spacing.is_none() {
                    *tick_spacing = Some(key.tick_spacing);
                }
                if hooks.is_none() {
                    *hooks = Some(key.hooks);
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "discovery::enrich",
                    %pool_id,
                    error = %e,
                    "subgraph lookup failed — dropping descriptor"
                );
                to_drop.insert(i);
            }
        }
    }
    if !to_drop.is_empty() {
        let mut i = 0;
        descriptors.retain(|_| {
            let keep = !to_drop.contains(&i);
            i += 1;
            keep
        });
    }
}
