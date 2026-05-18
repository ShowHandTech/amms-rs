//! DexTools v2 JSON response DTOs.
//!
//! Permissive on purpose — extra/unknown fields are ignored, and most fields are `Option<_>`
//! because schemas vary by chain and exchange. The mapping from DTO to `PoolDescriptor`
//! happens in `super::to_pool_descriptor`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PoolsResponse {
    #[serde(default)]
    pub data: Vec<Pool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pool {
    /// Pool contract address (V2/V3) or PoolId (V4). DexTools returns it under various keys
    /// across versions; we accept `address` first and fall back to `id`.
    #[serde(alias = "id", alias = "poolAddress", alias = "pool_address")]
    pub address: String,

    /// Pool factory contract address (V2/V3) or singleton manager (V4 PoolManager / PCS
    /// CLPoolManager).
    #[serde(default)]
    pub factory: Option<String>,

    /// Human-readable DEX identifier (e.g. `"PancakeSwapV3"`, `"UniswapV4"`,
    /// `"PancakeSwap Infinity CL"`). We classify on this string.
    #[serde(default, alias = "dex", alias = "exchangeName")]
    pub exchange: Option<String>,

    /// Pool fee in 1/100000 units for V2 forks (e.g. `25` = 0.025%, `300` = 0.30%). Per-pool
    /// V3 fee is read from chain at sync time so we don't need it here.
    #[serde(default, alias = "feeBps", alias = "fee_bps")]
    pub fee_bps: Option<usize>,

    /// V4-specific fields. Provided by DexTools for V4 pools; absent for V2/V3.
    #[serde(default, alias = "token0", alias = "currency_0")]
    pub currency0: Option<String>,
    #[serde(default, alias = "token1", alias = "currency_1")]
    pub currency1: Option<String>,
    #[serde(default)]
    pub hooks: Option<String>,
    /// PCS-only: 32-byte packed parameters word (tickSpacing + hook permission bitmap).
    #[serde(default)]
    pub parameters: Option<String>,
    /// Uniswap V4: explicit fee (24-bit) and tickSpacing (int24). Renamed to avoid clashing
    /// with `fee_bps` above; PCS embeds tickSpacing inside `parameters` so it doesn't use these.
    #[serde(default, alias = "v4Fee", alias = "uniV4Fee")]
    pub v4_fee: Option<u32>,
    #[serde(default, alias = "v4TickSpacing", alias = "uniV4TickSpacing", alias = "tickSpacing")]
    pub v4_tick_spacing: Option<i32>,
}
