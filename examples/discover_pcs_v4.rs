//! Discover + sync PancakeSwap V4 CL pools.
//!
//! Usage:
//!   cargo run --release --example discover_pcs_v4 -- [path/to/config.toml]
//!
//! If no path is given, defaults to `examples/configs/bsc-pcs-v4-cl.toml`.

use std::{collections::HashSet, fs, path::PathBuf};

use alloy::{eips::BlockId, primitives::Address, providers::ProviderBuilder};
use amms::amms::{
    amm::AutomatedMarketMaker,
    factory::DiscoverySync,
    pancake_v4_cl::PancakeV4CLFactory,
    uniswap_v4::HookFilter,
};
use serde::Deserialize;

const DEFAULT_CONFIG: &str = "examples/configs/bsc-pcs-v4-cl.toml";

#[derive(Debug, Deserialize)]
struct Config {
    rpc: String,
    #[allow(dead_code)]
    rpc_ws: Option<String>,
    pancake_v4_cl: PancakeV4CLConfig,
}

#[derive(Debug, Deserialize)]
struct PancakeV4CLConfig {
    cl_pool_manager: Address,
    creation_block: u64,
    #[serde(default = "default_hook_filter")]
    hook_filter: String,
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

    let path: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG.into())
        .into();
    let raw = fs::read_to_string(&path)
        .map_err(|e| eyre::eyre!("failed to read {}: {e}", path.display()))?;
    let cfg: Config = toml::from_str(&raw)?;
    let hook_filter = parse_hook_filter(&cfg.pancake_v4_cl.hook_filter)?;

    println!("config:            {}", path.display());
    println!("RPC:               {}", cfg.rpc);
    println!("CLPoolManager:     {}", cfg.pancake_v4_cl.cl_pool_manager);
    println!("creation_block:    {}", cfg.pancake_v4_cl.creation_block);
    println!("hook_filter:       {hook_filter:?}");

    let provider = ProviderBuilder::new().connect(&cfg.rpc).await?;

    let factory = PancakeV4CLFactory {
        cl_pool_manager: cfg.pancake_v4_cl.cl_pool_manager,
        creation_block: cfg.pancake_v4_cl.creation_block,
        hook_filter,
    };

    println!("== discovering ==");
    let pools = factory.discover(BlockId::latest(), provider.clone()).await?;
    println!("discovered {} pools (filtered)", pools.len());

    if pools.is_empty() {
        println!("(empty result — try setting hook_filter = \"all\" in the config to count all pools)");
        return Ok(());
    }

    println!("== syncing ==");
    let synced = factory.sync(pools, BlockId::latest(), provider).await?;
    println!("synced {} pools", synced.len());

    for amm in synced.iter().take(5) {
        let tokens = amm.tokens();
        println!("  {:?} tokens={:?}", amm.id(), tokens);
    }

    Ok(())
}
