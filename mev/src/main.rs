//! MEV binary — confirmed orderbook + single-tx mempool speculation.
//!
//! Phase 10 (this file): bootstrap a `StateSpace` identical to
//! `examples/token_first_runner.rs`. No mempool subscription, no decoder, no
//! speculator yet — those land in Phase 11–14. The intent of Phase 10 is just
//! to prove the workspace conversion and dependency wiring compile and run.
//!
//! Usage:
//!   DEXTOOLS_API_KEY=... cargo run -p mev --release -- mev/configs/bsc.toml

mod config;
mod decoder;
mod mempool;
mod pathfinder;
mod profit;
mod speculator;

use std::{fs, path::PathBuf, sync::Arc};

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
    },
    discovery::{
        dextools::DexToolsClient, discover_and_sync_for_token_with_subgraph,
        subgraph::SubgraphClient,
    },
    redis_bridge::{fetch_active_tokens, run_subscriber, write_pool_cache, RedisKeys},
    redis_mirror::{flush_block, refresh_tokens_set, MirrorKeys},
    state_space::{StateSpace, StateSpaceManager},
};
use futures::StreamExt;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::config::{parse_hook_filter, Config};
use crate::decoder::{pcs_v2, pcs_v3, DecoderRegistry};
use crate::mempool::{public::PublicMempoolFeed, puissant::PuissantFeed, MempoolFeed, MempoolTx};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let path: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "mev/configs/bsc.toml".to_string())
        .into();
    let raw = fs::read_to_string(&path).map_err(|e| {
        eyre::eyre!(
            "failed to read {}: {e}. Copy mev/configs/bsc.toml and fill in api keys / urls.",
            path.display(),
        )
    })?;
    let cfg: Config = toml::from_str(&raw)?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&cfg.log.level))
        .init();

    info!(target: "mev", config = %path.display(), log_level = %cfg.log.level, "starting mev binary");

    let http_provider = ProviderBuilder::new().connect(&cfg.rpc).await?;
    let ws_provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(cfg.rpc_ws.clone()))
        .await?;

    let mut factories: Vec<Factory> = Vec::new();
    if let Some(v2) = &cfg.pancake_v2 {
        factories.push(Factory::UniswapV2Factory(UniswapV2Factory::new(
            v2.factory,
            v2.fee,
            v2.creation_block,
        )));
    }
    if let Some(v3) = &cfg.pancake_v3 {
        factories.push(Factory::UniswapV3Factory(UniswapV3Factory::new(
            v3.factory,
            v3.creation_block,
        )));
    }
    if let Some(v3) = &cfg.uniswap_v3 {
        factories.push(Factory::UniswapV3Factory(UniswapV3Factory::new(
            v3.factory,
            v3.creation_block,
        )));
    }
    if let Some(v4) = &cfg.pancake_v4_cl {
        factories.push(Factory::PancakeV4CLFactory(PancakeV4CLFactory {
            cl_pool_manager: v4.cl_pool_manager,
            creation_block: v4.creation_block,
            hook_filter: parse_hook_filter(&v4.hook_filter)?,
        }));
    }
    if let Some(v4) = &cfg.uniswap_v4 {
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

    let dextools = DexToolsClient::new(cfg.dextools.api_key.clone());
    let subgraph_v4 = cfg.subgraph.as_ref().and_then(|s| {
        s.uniswap_v4
            .as_ref()
            .map(|url| SubgraphClient::new(url, &s.api_key))
    });
    if subgraph_v4.is_some() {
        info!(target: "mev", "Uniswap V4 subgraph enrichment enabled");
    } else {
        info!(target: "mev", "no subgraph configured — Uniswap V4 pools without resolved PoolKey will be dropped");
    }

    let active_tokens = fetch_active_tokens(&mut redis_conn, &redis_keys).await?;
    info!(target: "mev", count = active_tokens.len(), "fetched active tokens from redis");

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
                info!(target: "mev", %token, pools = synced.len(), "discovered + synced");
                let amm_ids: Vec<_> = synced.iter().map(|a| a.id()).collect();
                state.track_pools(synced, *token);
                if let Ok(payload) = serde_json::to_string(&amm_ids) {
                    if let Err(e) =
                        write_pool_cache(&mut redis_conn, &redis_keys, *token, &payload).await
                    {
                        warn!(target: "mev", error = %e, "pool cache write failed");
                    }
                }
            }
            Err(e) => {
                warn!(target: "mev", %token, error = %e, "discover failed; skipping token");
            }
        }
    }

    let latest_block = http_provider.get_block_number().await?;

    let manager =
        StateSpaceManager::from_state_and_factories(state, factories.clone(), latest_block, ws_provider);

    {
        let s = manager.state.read().await;
        if let Err(e) = refresh_tokens_set(&mut redis_conn, &mirror_keys, &s).await {
            warn!(target: "mev", error = %e, "mirror tokens refresh failed");
        }
    }

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
            warn!(target: "mev", error = %e, "redis subscriber exited");
        }
    });

    // === Spawn mempool feeds (Phase 11). Each feed pushes normalized tx onto `mempool_rx`. ===
    // 1024 is enough headroom for BSC's ~100-200 pending tx/sec when downstream is paused
    // briefly (decoder hiccup, GC). On overflow we'd rather drop oldest than block the
    // producer, but tokio mpsc only offers bounded back-pressure; if this becomes a problem
    // we'll switch to a try_send + drop-on-full policy.
    let (mempool_tx, mut mempool_rx) = mpsc::channel::<MempoolTx>(1024);
    {
        let public_feed = Arc::new(PublicMempoolFeed::new(cfg.rpc_ws.clone()));
        let tx_clone = mempool_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = public_feed.subscribe(tx_clone).await {
                warn!(target: "mev", error = %e, "public mempool feed exited");
            }
        });
        let puissant_feed = Arc::new(PuissantFeed::new(None));
        let tx_clone = mempool_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = puissant_feed.subscribe(tx_clone).await {
                warn!(target: "mev", error = %e, "puissant feed exited");
            }
        });
    }
    drop(mempool_tx); // Drop the local handle so the channel closes if all feeds exit.

    // Phase 12 consumer: decoder dispatch. Successfully decoded intents are logged at info; the
    // speculator/pathfinder consume them starting in Phase 13.
    let mut registry = DecoderRegistry::new();
    pcs_v2::register_all(&mut registry, pcs_v2::PCS_V2_ROUTER_BSC);
    if let Some(v3_router) = cfg.pcs_v3_smart_router {
        pcs_v3::register_all(&mut registry, v3_router);
    }
    let registry = Arc::new(registry);
    let gas_ctx = profit::GasContext {
        gas_estimate: cfg.mev.gas_estimate,
        base_fee_wei: cfg.mev.base_fee_wei,
    };
    let profit_cfg = profit::ProfitConfig {
        min_profit: cfg.mev.min_profit,
    };
    {
        let registry = registry.clone();
        let state_for_speculator = manager.state.clone();
        tokio::spawn(async move {
            while let Some(tx) = mempool_rx.recv().await {
                let Some(intent) = registry.decode(&tx) else {
                    debug!(target: "mev::decoder", to = ?tx.to, "tx skipped");
                    continue;
                };
                info!(
                    target: "mev::decoder",
                    source = intent.source.source,
                    tx_hash = ?intent.source.tx_hash,
                    router = ?intent.router,
                    hops = intent.path.len(),
                    token_in = ?intent.path.first().map(|h| h.token_in),
                    token_out = ?intent.path.last().map(|h| h.token_out),
                    amount_in = %intent.amount_in,
                    min_out = %intent.min_amount_out,
                    "decoded swap intent",
                );

                let projection = match speculator::speculate(&state_for_speculator, &intent).await {
                    Ok(p) => p,
                    Err(e) => {
                        debug!(
                            target: "mev::speculator",
                            tx_hash = ?intent.source.tx_hash,
                            error = %e,
                            "speculation failed",
                        );
                        continue;
                    }
                };
                let final_out = projection.last().map(|p| p.amount_out);
                debug!(
                    target: "mev::speculator",
                    tx_hash = ?intent.source.tx_hash,
                    hops = projection.len(),
                    final_out = ?final_out,
                    "projected intent",
                );

                let opportunities =
                    pathfinder::find_backruns(&state_for_speculator, &projection).await;
                for opp in &opportunities {
                    profit::report_opportunity(&intent, opp, gas_ctx, profit_cfg);
                }
                if opportunities.is_empty() {
                    debug!(
                        target: "mev::pathfinder",
                        tx_hash = ?intent.source.tx_hash,
                        "no backrun candidates",
                    );
                }
            }
        });
    }

    let mut mirror_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    let mut stream = manager.subscribe().await?;
    info!(target: "mev", "subscribed to on-chain logs; entering main loop");
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
                    warn!(target: "mev", error = %e, "mirror flush failed");
                }
            }
            Err(e) => {
                warn!(target: "mev", error = %e, "subscribe stream error");
            }
        }
    }

    Ok(())
}
