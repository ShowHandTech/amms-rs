//! Config schema for the MEV binary. Mirrors `examples/token_first_runner.rs` for the bootstrap
//! sections; MEV-specific sections (mempool / decoder / profit thresholds) land in Phase 11+.

use std::collections::HashSet;

use alloy::primitives::Address;
use amms::amms::uniswap_v4::HookFilter;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub rpc: String,
    pub rpc_ws: String,
    pub chain_slug: String,
    #[serde(default)]
    pub log: LogSection,
    pub dextools: DexToolsSection,
    pub redis: RedisSection,
    pub core_tokens: CoreTokensSection,
    #[serde(default)]
    pub pancake_v4_cl: Option<PancakeV4ClSection>,
    #[serde(default)]
    pub pancake_v2: Option<V2Section>,
    #[serde(default)]
    pub pancake_v3: Option<V3Section>,
    #[serde(default)]
    pub uniswap_v3: Option<V3Section>,
    #[serde(default)]
    pub uniswap_v4: Option<UniswapV4Section>,
    #[serde(default)]
    pub subgraph: Option<SubgraphSection>,
    /// Optional PCS V3 SmartRouter address. When set, mempool decoder registers
    /// `exactInputSingle` handler against this router. If absent, only V2 router traffic is
    /// decoded.
    #[serde(default)]
    pub pcs_v3_smart_router: Option<Address>,
    #[serde(default)]
    pub mev: MevSection,
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct MevSection {
    /// Gas budget for the entire backrun bundle (buy + sell + executor overhead). 250k is a
    /// reasonable v1 default — Phase F replaces this with `eth_estimateGas` against the real
    /// executor calldata.
    #[serde(default = "default_gas_estimate")]
    pub gas_estimate: u64,
    /// Base fee in wei to assume when computing gas cost. 1 gwei = 1e9 wei is BSC's typical
    /// floor.
    #[serde(default = "default_base_fee_wei")]
    pub base_fee_wei: u128,
    /// Minimum gross profit (in `token_out` units) before we log at info. Below this the
    /// opportunity falls through to debug to keep the info stream readable.
    #[serde(default = "default_min_profit")]
    pub min_profit: i128,
}

impl Default for MevSection {
    fn default() -> Self {
        Self {
            gas_estimate: default_gas_estimate(),
            base_fee_wei: default_base_fee_wei(),
            min_profit: default_min_profit(),
        }
    }
}

fn default_gas_estimate() -> u64 {
    250_000
}

fn default_base_fee_wei() -> u128 {
    1_000_000_000 // 1 gwei
}

fn default_min_profit() -> i128 {
    0
}

#[derive(Debug, Deserialize)]
pub struct SubgraphSection {
    pub api_key: String,
    #[serde(default)]
    pub uniswap_v4: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LogSection {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info,state_space::sync=info,redis_bridge=info,mev=info,hyper=warn,reqwest=warn".to_string()
}

#[derive(Debug, Deserialize)]
pub struct UniswapV4Section {
    pub pool_manager: Address,
    #[serde(default)]
    pub creation_block: u64,
    #[serde(default = "default_hook_filter")]
    pub hook_filter: String,
}

#[derive(Debug, Deserialize)]
pub struct DexToolsSection {
    pub api_key: String,
}

#[derive(Debug, Deserialize)]
pub struct RedisSection {
    pub url: String,
    pub namespace: String,
}

#[derive(Debug, Deserialize)]
pub struct CoreTokensSection {
    pub addresses: Vec<Address>,
}

#[derive(Debug, Deserialize)]
pub struct PancakeV4ClSection {
    pub cl_pool_manager: Address,
    #[serde(default)]
    pub creation_block: u64,
    #[serde(default = "default_hook_filter")]
    pub hook_filter: String,
}

#[derive(Debug, Deserialize)]
pub struct V2Section {
    pub factory: Address,
    #[serde(default)]
    pub creation_block: u64,
    pub fee: usize,
}

#[derive(Debug, Deserialize)]
pub struct V3Section {
    pub factory: Address,
    #[serde(default)]
    pub creation_block: u64,
}

fn default_hook_filter() -> String {
    "none".to_string()
}

pub fn parse_hook_filter(s: &str) -> eyre::Result<HookFilter> {
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
