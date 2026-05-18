//! Phase 6 smoke test: discover pools for a single token through DexTools and sync them.
//!
//! Usage:
//!   DEXTOOLS_API_KEY=... cargo run --release --example discover_via_dextools -- \
//!     <chain_slug> <token_address> [config.toml]
//!
//! Example (BSC USDT):
//!   DEXTOOLS_API_KEY=xxx cargo run --release --example discover_via_dextools -- \
//!     bsc 0x55d398326f99059fF775485246999027B3197955
//!
//! What it does:
//!   1. GET https://public-api.dextools.io/trial/v2/token/{chain}/{token}/pools
//!   2. Classify each pool record as V2 / V3 / PcsV4Cl / UniswapV4 (PoolDescriptor)
//!   3. Look up matching Factory (from the config), materialize unsynced AMMs
//!   4. Drive Factory::sync to fill state (Slot0, TickBitmap, TickData for V3/V4)
//!   5. Print the result
//!
//! No StateSpace is involved — this is a direct library-function call. Useful both as a
//! standalone discovery tool and as the COLD path that MEV consumers can use to fetch a
//! single pool snapshot without going through the runtime tracking system.

use std::{collections::HashSet, fs, path::PathBuf};

use alloy::{eips::BlockId, primitives::Address, providers::ProviderBuilder};
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
        dextools::DexToolsClient, discover_and_sync_for_token,
    },
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Config {
    rpc: String,
    #[serde(default)]
    pancake_v4_cl: Option<PancakeV4ClSection>,
    #[serde(default)]
    pancake_v2: Option<V2Section>,
    #[serde(default)]
    pancake_v3: Option<V3Section>,
}

#[derive(Debug, Deserialize)]
struct PancakeV4ClSection {
    cl_pool_manager: Address,
    creation_block: u64,
    #[serde(default = "default_hook_filter")]
    hook_filter: String,
}

#[derive(Debug, Deserialize)]
struct V2Section {
    factory: Address,
    creation_block: u64,
    fee: usize,
}

#[derive(Debug, Deserialize)]
struct V3Section {
    factory: Address,
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
    tracing_subscriber::fmt::init();

    let mut args = std::env::args().skip(1);
    let chain_slug = args
        .next()
        .ok_or_else(|| eyre::eyre!("usage: <chain_slug> <token> [config.toml]"))?;
    let token: Address = args
        .next()
        .ok_or_else(|| eyre::eyre!("missing token address"))?
        .parse()?;
    let config_path: PathBuf = args
        .next()
        .unwrap_or_else(|| "examples/configs/bsc-token-first.toml".to_string())
        .into();

    let api_key = std::env::var("DEXTOOLS_API_KEY")
        .map_err(|_| eyre::eyre!("DEXTOOLS_API_KEY env var required"))?;

    let raw = fs::read_to_string(&config_path)?;
    let cfg: Config = toml::from_str(&raw)?;

    println!("config:    {}", config_path.display());
    println!("RPC:       {}", cfg.rpc);
    println!("chain:     {chain_slug}");
    println!("token:     {token}");

    let provider = ProviderBuilder::new().connect(&cfg.rpc).await?;

    let mut factories: Vec<Factory> = Vec::new();
    if let Some(v2) = cfg.pancake_v2 {
        factories.push(Factory::UniswapV2Factory(UniswapV2Factory::new(
            v2.factory,
            v2.fee,
            v2.creation_block,
        )));
    }
    if let Some(v3) = cfg.pancake_v3 {
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
    if factories.is_empty() {
        eyre::bail!("no factories configured");
    }
    println!("factories: {}", factories.len());

    let client = DexToolsClient::new(api_key);

    println!("== discovering ==");
    let synced =
        discover_and_sync_for_token(&chain_slug, token, &client, &factories, BlockId::latest(), provider)
            .await?;
    println!("synced {} pools", synced.len());

    for amm in synced.iter().take(10) {
        let tokens = amm.tokens();
        println!("  {:?} tokens={tokens:?}", amm.id());
    }
    if synced.len() > 10 {
        println!("  ... and {} more", synced.len() - 10);
    }

    Ok(())
}
