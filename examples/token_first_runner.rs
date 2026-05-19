//! Phase 7-9 end-to-end runner: the production token-first deployment.
//!
//! Usage:
//!   DEXTOOLS_API_KEY=... cargo run --release --example token_first_runner -- \
//!     [examples/configs/bsc-token-first.toml]
//!
//! What it does on startup:
//!   1. Connect Redis and read `tokens:{namespace}:active` (SET) for the initial token list
//!   2. For each token: DexTools → PoolDescriptor[] → Factory::from_descriptor → Factory::sync
//!   3. Insert pools into StateSpace via `track_pools` (also fills `by_token` index)
//!   4. Inject core tokens (protected from untrack)
//!   5. Subscribe to `tokens:{namespace}:changes` Pub/Sub channel (TRACK / UNTRACK)
//!   6. Concurrently subscribe to on-chain logs (Swap / ModifyLiquidity / Initialize)
//!      - Initialize on V4 singletons triggers auto-track if currency0/1 is already tracked
//!      - Other events route through `StateSpace::sync` by AmmId
//!   7. After each block, flush touched pools to the Redis mirror layer
//!      (`mirror:{namespace}:pool:*` + `mirror:{namespace}:updates`)

use std::{collections::HashSet, fs, path::PathBuf, sync::Arc};

use alloy::{
    eips::BlockId,
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
};
use amms::{
    amms::{
        amm::AutomatedMarketMaker,
        factory::Factory,
        pancake_v4_cl::PancakeV4CLFactory,
        uniswap_v2::UniswapV2Factory,
        uniswap_v3::UniswapV3Factory,
        uniswap_v4::HookFilter,
    },
    discovery::{
        dextools::DexToolsClient, discover_and_sync_for_token_with_subgraph,
        subgraph::SubgraphClient,
    },

    redis_bridge::{
        fetch_active_tokens, run_subscriber, write_pool_cache, RedisKeys,
    },
    redis_mirror::{flush_block, refresh_tokens_set, MirrorKeys},
    state_space::{StateSpace, StateSpaceManager},
};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct Config {
    rpc: String,
    rpc_ws: String,
    chain_slug: String,
    #[serde(default)]
    log: LogSection,
    dextools: DexToolsSection,
    redis: RedisSection,
    core_tokens: CoreTokensSection,
    #[serde(default)]
    pancake_v4_cl: Option<PancakeV4ClSection>,
    #[serde(default)]
    pancake_v2: Option<V2Section>,
    /// V3 forks. BSC has PancakeSwap V3 + Uniswap V3 + others; one [[v3]] block per fork.
    /// (Fee and tick_spacing are per-pool from chain so each entry only needs the factory.)
    #[serde(default)]
    v3: Vec<V3Section>,
    /// Legacy singular form. Still accepted for back-compat; merged into `v3` on startup.
    #[serde(default)]
    pancake_v3: Option<V3Section>,
    #[serde(default)]
    uniswap_v4: Option<UniswapV4Section>,
    #[serde(default)]
    subgraph: Option<SubgraphSection>,
}

#[derive(Debug, Deserialize)]
struct SubgraphSection {
    /// The Graph API key (free tier at https://thegraph.com/studio). Stored separately so the
    /// URL template can be checked into git without leaking credentials.
    api_key: String,
    /// URL template with `{API_KEY}` placeholder. Example:
    /// `"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/<ID>"`.
    #[serde(default)]
    uniswap_v4: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LogSection {
    #[serde(default = "default_log_level")]
    level: String,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info,state_space::sync=info,redis_bridge=info,hyper=warn,reqwest=warn".to_string()
}

#[derive(Debug, Deserialize)]
struct UniswapV4Section {
    pool_manager: Address,
    #[serde(default)]
    creation_block: u64,
    #[serde(default = "default_hook_filter")]
    hook_filter: String,
}

#[derive(Debug, Deserialize)]
struct DexToolsSection {
    api_key: String,
}

#[derive(Debug, Deserialize)]
struct RedisSection {
    url: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct CoreTokensSection {
    addresses: Vec<Address>,
}

// `creation_block` is required on the Factory struct itself (for factory-first historical
// scan), but token-first mode never reads it. We default it to 0 so the config file can
// stay focused on the things that actually matter at runtime (api keys, rpc, addresses).
#[derive(Debug, Deserialize)]
struct PancakeV4ClSection {
    cl_pool_manager: Address,
    #[serde(default)]
    creation_block: u64,
    #[serde(default = "default_hook_filter")]
    hook_filter: String,
}

#[derive(Debug, Deserialize)]
struct V2Section {
    factory: Address,
    #[serde(default)]
    creation_block: u64,
    fee: usize,
}

#[derive(Debug, Deserialize)]
struct V3Section {
    factory: Address,
    #[serde(default)]
    creation_block: u64,
}

fn default_hook_filter() -> String {
    "none".to_string()
}

fn parse_hook_filter(s: &str) -> eyre::Result<HookFilter> {
    match s {
        "none" => Ok(HookFilter::NoHooks),
        "all" => Ok(HookFilter::AllowAll),
        list => {
            let set = list
                .split(',')
                .map(|s| s.trim().parse::<Address>())
                .collect::<Result<HashSet<_>, _>>()?;
            Ok(HookFilter::Whitelist(set))
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let path: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/bsc-token-first.toml".to_string())
        .into();
    let raw = fs::read_to_string(&path).map_err(|e| {
        eyre::eyre!(
            "failed to read {}: {e}. Copy from {}.example and fill in api keys / urls.",
            path.display(),
            path.display()
        )
    })?;
    let cfg: Config = toml::from_str(&raw)?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&cfg.log.level))
        .init();

    info!(target: "runner", config = %path.display(), log_level = %cfg.log.level, "starting token-first runner");

    let http_provider = ProviderBuilder::new().connect(&cfg.rpc).await?;
    let ws_provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(cfg.rpc_ws.clone()))
        .await?;

    // Build factories from config.
    let mut factories: Vec<Factory> = Vec::new();
    if let Some(v2) = cfg.pancake_v2 {
        factories.push(Factory::UniswapV2Factory(UniswapV2Factory::new(
            v2.factory,
            v2.fee,
            v2.creation_block,
        )));
    }
    // V3: legacy singular `[pancake_v3]` is folded into the same list as `[[v3]]` so configs
    // that already use the singular form keep working when you add a second V3 fork.
    let v3_sections: Vec<V3Section> = cfg.pancake_v3.into_iter().chain(cfg.v3).collect();
    for v3 in v3_sections {
        factories.push(Factory::UniswapV3Factory(UniswapV3Factory::new(
            v3.factory,
            v3.creation_block,
        )));
    }
    if let Some(v4) = cfg.pancake_v4_cl {
        factories.push(Factory::PancakeV4CLFactory(PancakeV4CLFactory {
            cl_pool_manager: v4.cl_pool_manager,
            creation_block: v4.creation_block,
            hook_filter: parse_hook_filter(&v4.hook_filter)?,
        }));
    }
    if let Some(v4) = cfg.uniswap_v4 {
        factories.push(Factory::UniswapV4Factory(
            amms::amms::uniswap_v4::UniswapV4Factory {
                pool_manager: v4.pool_manager,
                creation_block: v4.creation_block,
                hook_filter: parse_hook_filter(&v4.hook_filter)?,
            },
        ));
    }
    if factories.is_empty() {
        eyre::bail!("no factories configured");
    }

    let redis_keys = RedisKeys::for_namespace(&cfg.redis.namespace);
    let mirror_keys = MirrorKeys::for_namespace(&cfg.redis.namespace);

    let redis_client = redis::Client::open(cfg.redis.url.clone())?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client.clone()).await?;

    // === Bootstrap: pull active tokens, run DexTools discovery for each. ===
    let dextools = DexToolsClient::new(cfg.dextools.api_key.clone());
    let subgraph_v4 = cfg.subgraph.as_ref().and_then(|s| {
        s.uniswap_v4
            .as_ref()
            .map(|url| SubgraphClient::new(url, &s.api_key))
    });
    if subgraph_v4.is_some() {
        info!(target: "runner", "Uniswap V4 subgraph enrichment enabled");
    } else {
        info!(target: "runner", "no subgraph configured — Uniswap V4 pools without resolved PoolKey will be dropped");
    }
    let active_tokens = fetch_active_tokens(&mut redis_conn, &redis_keys).await?;
    info!(target: "runner", count = active_tokens.len(), "fetched active tokens from redis");

    let mut state = StateSpace::default();
    state.set_core_tokens(cfg.core_tokens.addresses.iter().copied());

    for token in &active_tokens {
        match discover_and_sync_for_token_with_subgraph(
            &cfg.chain_slug,
            *token,
            &dextools,
            subgraph_v4.as_ref(),
            &factories,
            BlockId::latest(),
            http_provider.clone(),
        )
        .await
        {
            Ok(synced) => {
                info!(target: "runner", %token, pools = synced.len(), "discovered + synced");
                let amm_ids: Vec<_> = synced.iter().map(|a| a.id()).collect();
                state.track_pools(synced, *token);
                if let Ok(payload) = serde_json::to_string(&amm_ids) {
                    if let Err(e) =
                        write_pool_cache(&mut redis_conn, &redis_keys, *token, &payload).await
                    {
                        warn!(target: "runner", error = %e, "pool cache write failed");
                    }
                }
            }
            Err(e) => {
                warn!(target: "runner", %token, error = %e, "discover failed; skipping token");
            }
        }
    }

    let latest_block = http_provider.get_block_number().await?;

    let manager = StateSpaceManager::from_state_and_factories(
        state,
        factories.clone(),
        latest_block,
        ws_provider,
    );

    // Refresh mirror tokens set once at startup.
    {
        let s = manager.state.read().await;
        if let Err(e) = refresh_tokens_set(&mut redis_conn, &mirror_keys, &s).await {
            warn!(target: "runner", error = %e, "mirror tokens refresh failed");
        }
    }

    // === Spawn the Redis pub/sub subscriber (TRACK / UNTRACK) ===
    let sub_state: Arc<RwLock<StateSpace>> = manager.state.clone();
    let sub_provider = http_provider.clone();
    let sub_factories = factories.clone();
    let sub_chain = cfg.chain_slug.clone();
    let sub_url = cfg.redis.url.clone();
    let sub_namespace = cfg.redis.namespace.clone();
    let sub_dextools = dextools.clone();
    let sub_subgraph = subgraph_v4.clone();
    tokio::spawn(async move {
        let on_track = |token: Address| {
            let chain = sub_chain.clone();
            let dextools = sub_dextools.clone();
            let subgraph = sub_subgraph.clone();
            let factories = sub_factories.clone();
            let provider = sub_provider.clone();
            async move {
                discover_and_sync_for_token_with_subgraph(
                    &chain,
                    token,
                    &dextools,
                    subgraph.as_ref(),
                    &factories,
                    BlockId::latest(),
                    provider,
                )
                .await
                .map_err(|e| eyre::eyre!("{e}"))
            }
        };
        if let Err(e) = run_subscriber(&sub_url, &sub_namespace, sub_state, on_track).await {
            warn!(target: "runner", error = %e, "redis subscriber exited");
        }
    });

    // === Drive the on-chain subscription and mirror touched pools each block. ===
    let mut mirror_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    let mut stream = manager.subscribe().await?;
    info!(target: "runner", "subscribed to on-chain logs; entering main loop");
    while let Some(item) = stream.next().await {
        match item {
            Ok(affected) => {
                if affected.is_empty() {
                    continue;
                }
                let block = manager
                    .latest_block
                    .load(std::sync::atomic::Ordering::Relaxed);
                let s = manager.state.read().await;
                if let Err(e) =
                    flush_block(&mut mirror_conn, &mirror_keys, &s, &affected, block).await
                {
                    warn!(target: "runner", error = %e, "mirror flush failed");
                }
            }
            Err(e) => {
                warn!(target: "runner", error = %e, "subscribe stream error");
            }
        }
    }

    Ok(())
}
