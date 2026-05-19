//! DexTools v2 JSON response DTOs.
//!
//! Schema reverse-engineered from real `/v2/token/{chain}/{addr}/pools` responses
//! (verified 2026-05-19, BSC). Top level is `{ statusCode, data: PoolsData }` where
//! `data` contains pagination metadata + a `results: [Pool]` array. Most pool fields
//! are `Option<_>` because DexTools varies them by exchange version.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PoolsResponse {
    #[serde(default)]
    pub data: PoolsData,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PoolsData {
    #[serde(default)]
    pub page: u32,
    #[serde(default, alias = "pageSize")]
    pub page_size: u32,
    #[serde(default, alias = "totalPages")]
    pub total_pages: u32,
    #[serde(default)]
    pub results: Vec<Pool>,
}

/// One pool entry from DexTools.
///
/// Note `address` semantics depend on `exchange.name`:
///   - V2 / V3 pools: contract address (Address, 20 bytes hex)
///   - PCS Infinity / Uniswap V4: PoolId (B256, 32 bytes hex)
#[derive(Debug, Clone, Deserialize)]
pub struct Pool {
    pub address: String,
    pub exchange: ExchangeInfo,
    /// PCS Infinity / V4 only — returned as percentage float (e.g. `0.0804` = 0.0804%).
    /// V2/V3 omit this field; V2 fee is hard-coded per factory, V3 fee is read from chain.
    #[serde(default)]
    pub fee: Option<f64>,
    /// DexTools picks `mainToken` by liquidity / popularity, **not** V4 currency0/currency1
    /// ordering. Callers that need V4-style ordering must sort by address themselves.
    #[serde(default, alias = "mainToken")]
    pub main_token: Option<TokenRef>,
    #[serde(default, alias = "sideToken")]
    pub side_token: Option<TokenRef>,
    #[serde(default, alias = "liquidityUsd")]
    pub liquidity_usd: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub factory: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenRef {
    pub address: String,
}
