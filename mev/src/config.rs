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

/// On-disk MEV settings. Inputs are human-readable; conversion to integer units happens once
/// in [`MevSection::resolve`] so the rest of the binary never has to remember decimal scaling.
#[derive(Debug, Deserialize, Clone)]
pub struct MevSection {
    /// Gas budget for the entire backrun bundle (buy + sell + executor overhead). 250k is a
    /// reasonable v1 default — Phase F replaces this with `eth_estimateGas` against the real
    /// executor calldata.
    #[serde(default = "default_gas_estimate")]
    pub gas_estimate: u64,
    /// Base fee in **gwei**. BSC's typical floor is `1.0`. Floats are fine.
    #[serde(default = "default_base_fee_gwei")]
    pub base_fee_gwei: f64,
    /// Minimum gross profit before we promote the opportunity log to info. Human-readable
    /// decimal string, e.g. `"0.01"`. Interpreted in `token_out` units; scaling controlled by
    /// `min_profit_decimals`.
    #[serde(default = "default_min_profit")]
    pub min_profit: String,
    /// Decimals to multiply `min_profit` by. Defaults to 18 because BSC's common quote tokens
    /// (USDT, USDC, WBNB, BUSD) all use 18 decimals. If your token_out is something exotic
    /// (USDC on Polygon = 6 decimals, etc.) override this.
    #[serde(default = "default_min_profit_decimals")]
    pub min_profit_decimals: u8,
    #[serde(default)]
    pub pathfinder: PathfinderSection,
}

/// Cycle-search knobs. See `mev/src/pathfinder.rs` for what each one does.
#[derive(Debug, Deserialize, Clone)]
pub struct PathfinderSection {
    /// Max cycle length including the anchor (P_after Y→X) leg. `2` = old backrun behavior;
    /// `3` = triangular arbitrage; `4+` allowed but cost grows quickly.
    #[serde(default = "default_max_hops")]
    pub max_hops: usize,
    /// Pools considered per (from, to) edge from `by_token` intersection. `1` = deepest only.
    #[serde(default = "default_pool_candidates_per_edge")]
    pub pool_candidates_per_edge: usize,
    /// Whitelisted internal nodes (middle hops). Leave empty in TOML to fall back to
    /// `[core_tokens].addresses`. Endpoints X (victim's token_in) and Y (token_out) don't need
    /// to be in this set.
    #[serde(default)]
    pub bridge_tokens: Vec<Address>,
}

impl Default for PathfinderSection {
    fn default() -> Self {
        Self {
            max_hops: default_max_hops(),
            pool_candidates_per_edge: default_pool_candidates_per_edge(),
            bridge_tokens: Vec::new(),
        }
    }
}

fn default_max_hops() -> usize {
    3
}

fn default_pool_candidates_per_edge() -> usize {
    1
}

impl MevSection {
    /// Convert the human-readable settings into integer units. Returns the resolved
    /// gas/profit context the binary actually uses.
    pub fn resolve(&self) -> eyre::Result<ResolvedMev> {
        let base_fee_wei = (self.base_fee_gwei * 1e9) as u128;
        let min_profit = parse_decimal_to_units(&self.min_profit, self.min_profit_decimals)?;
        Ok(ResolvedMev {
            gas_estimate: self.gas_estimate,
            base_fee_wei,
            min_profit,
        })
    }
}

impl Default for MevSection {
    fn default() -> Self {
        Self {
            gas_estimate: default_gas_estimate(),
            base_fee_gwei: default_base_fee_gwei(),
            min_profit: default_min_profit(),
            min_profit_decimals: default_min_profit_decimals(),
            pathfinder: PathfinderSection::default(),
        }
    }
}

/// Integer-units snapshot of the MEV settings — what the binary actually reads at runtime.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedMev {
    pub gas_estimate: u64,
    pub base_fee_wei: u128,
    pub min_profit: i128,
}

fn default_gas_estimate() -> u64 {
    250_000
}

fn default_base_fee_gwei() -> f64 {
    1.0
}

fn default_min_profit() -> String {
    "0".to_string()
}

fn default_min_profit_decimals() -> u8 {
    18
}

/// Parse a decimal string like `"0.01"` or `"-1.5"` into an integer count of `10^decimals`
/// base units. Rejects scientific notation and anything that isn't a simple decimal — keeps
/// the config file self-evident.
fn parse_decimal_to_units(s: &str, decimals: u8) -> eyre::Result<i128> {
    let s = s.trim();
    if s.is_empty() {
        eyre::bail!("min_profit is empty");
    }
    let (sign, rest) = match s.as_bytes()[0] {
        b'-' => (-1i128, &s[1..]),
        b'+' => (1i128, &s[1..]),
        _ => (1i128, s),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        eyre::bail!("min_profit '{s}' is not a decimal number");
    }
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    if frac_part.len() > decimals as usize {
        eyre::bail!(
            "min_profit '{s}' has more fractional digits ({}) than min_profit_decimals ({decimals})",
            frac_part.len(),
        );
    }
    let mut combined = String::with_capacity(int_part.len() + decimals as usize);
    combined.push_str(int_part);
    combined.push_str(frac_part);
    for _ in 0..(decimals as usize - frac_part.len()) {
        combined.push('0');
    }
    let magnitude: i128 = combined
        .parse()
        .map_err(|e| eyre::eyre!("min_profit '{s}' does not fit in i128: {e}"))?;
    Ok(sign * magnitude)
}

#[cfg(test)]
mod tests {
    use super::parse_decimal_to_units;

    #[test]
    fn parses_basics() {
        assert_eq!(parse_decimal_to_units("0", 18).unwrap(), 0);
        assert_eq!(parse_decimal_to_units("1", 18).unwrap(), 1_000_000_000_000_000_000);
        assert_eq!(parse_decimal_to_units("0.01", 18).unwrap(), 10_000_000_000_000_000);
        assert_eq!(parse_decimal_to_units("0.000001", 6).unwrap(), 1);
        assert_eq!(parse_decimal_to_units("-1.5", 18).unwrap(), -1_500_000_000_000_000_000);
        assert_eq!(parse_decimal_to_units(".5", 18).unwrap(), 500_000_000_000_000_000);
    }

    #[test]
    fn rejects_too_many_fractional_digits() {
        assert!(parse_decimal_to_units("0.1234567", 6).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_decimal_to_units("1e9", 18).is_err());
        assert!(parse_decimal_to_units("abc", 18).is_err());
        assert!(parse_decimal_to_units("", 18).is_err());
    }
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
