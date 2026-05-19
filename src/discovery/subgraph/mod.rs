//! The Graph subgraph client for Uniswap V4 PoolKey lookups.
//!
//! Uniswap V4's `PoolManager` does NOT expose a `getPoolKey(id)` getter (PCS Infinity does,
//! but Uniswap's clean version doesn't). The only on-chain source of `PoolKey` is the
//! `Initialize` event log, which is not retrievable on short-retention full nodes (BSC ~3 days).
//! The Graph indexes those events and lets us query `pool(id: $pool_id)` to get the full
//! `(currency0, currency1, fee, tickSpacing, hooks)` tuple back.
//!
//! Endpoint format (gateway): `https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{ID}`.
//! `{API_KEY}` in the configured URL is templated at construction time. Register a free key
//! at https://thegraph.com/studio/ (100K queries/month free).
//!
//! Schema assumed (Uniswap V4 standard subgraph; field names confirmed against
//! https://github.com/Uniswap/v4-subgraph):
//! ```graphql
//! {
//!   pool(id: "0x...") {
//!     id
//!     token0 { id }
//!     token1 { id }
//!     feeTier      # Int / BigInt
//!     tickSpacing  # Int / BigInt
//!     hooks        # Bytes (address); "0x0000000000000000000000000000000000000000" for no-hook pools
//!   }
//! }
//! ```

use super::DiscoveryError;
use alloy::primitives::{Address, B256};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct SubgraphClient {
    url: String,
    /// Sent as `Authorization: Bearer {api_key}` on every request. The Graph gateway accepts
    /// the key either in the URL path (`/api/{API_KEY}/subgraphs/id/{ID}`) or in this header
    /// (`/api/subgraphs/id/{ID}` + Bearer). Sending the header always is safe — the gateway
    /// ignores duplicate auth, and it works for both URL styles.
    api_key: String,
    http: reqwest::Client,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV4PoolKey {
    pub currency0: Address,
    pub currency1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: Address,
}

impl SubgraphClient {
    /// `url_template` may contain the literal `{API_KEY}` placeholder for the old path-style
    /// gateway URL (`https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/<ID>`), or it may
    /// be the new header-style URL with the key omitted
    /// (`https://gateway.thegraph.com/api/subgraphs/id/<ID>`). Both work: the placeholder is
    /// substituted if present, and `Authorization: Bearer {api_key}` is sent on every request
    /// regardless. The header form is preferred (key doesn't leak into URL logs / proxies /
    /// referer).
    pub fn new(url_template: impl AsRef<str>, api_key: &str) -> Self {
        Self {
            url: url_template.as_ref().replace("{API_KEY}", api_key),
            api_key: api_key.to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Query the subgraph for a Uniswap V4 pool by its PoolId and return the full PoolKey.
    pub async fn uniswap_v4_pool_key(
        &self,
        pool_id: B256,
    ) -> Result<UniswapV4PoolKey, DiscoveryError> {
        // Pool IDs are stored as lowercase 0x-prefixed hex strings in the subgraph.
        let id = format!("{pool_id:#x}");
        let query = r#"
            query Pool($id: ID!) {
              pool(id: $id) {
                token0 { id }
                token1 { id }
                feeTier
                tickSpacing
                hooks
              }
            }
        "#;
        let body = serde_json::json!({
            "query": query,
            "variables": { "id": id },
        });

        let resp: GraphQLResponse<PoolWrapper> = self
            .http
            .post(&self.url)
            .header("content-type", "application/json")
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if let Some(errors) = resp.errors {
            if !errors.is_empty() {
                return Err(DiscoveryError::Malformed(format!(
                    "subgraph errors: {}",
                    errors
                        .iter()
                        .map(|e| e.message.clone())
                        .collect::<Vec<_>>()
                        .join("; ")
                )));
            }
        }
        let pool = resp
            .data
            .ok_or_else(|| DiscoveryError::Malformed("subgraph: missing data".to_string()))?
            .pool
            .ok_or_else(|| {
                DiscoveryError::Malformed(format!("subgraph: no pool with id {id}"))
            })?;

        let currency0: Address = pool
            .token0
            .id
            .parse()
            .map_err(|e| DiscoveryError::Malformed(format!("bad token0.id: {e}")))?;
        let currency1: Address = pool
            .token1
            .id
            .parse()
            .map_err(|e| DiscoveryError::Malformed(format!("bad token1.id: {e}")))?;
        let fee: u32 = parse_str_or_int(&pool.fee_tier)
            .map_err(|e| DiscoveryError::Malformed(format!("bad feeTier {}: {e}", pool.fee_tier)))?
            as u32;
        let tick_spacing: i32 =
            parse_str_or_int(&pool.tick_spacing).map_err(|e| {
                DiscoveryError::Malformed(format!("bad tickSpacing {}: {e}", pool.tick_spacing))
            })? as i32;
        let hooks: Address = pool
            .hooks
            .parse()
            .map_err(|e| DiscoveryError::Malformed(format!("bad hooks: {e}")))?;

        Ok(UniswapV4PoolKey {
            currency0: order_min(currency0, currency1),
            currency1: order_max(currency0, currency1),
            fee,
            tick_spacing,
            hooks,
        })
    }
}

fn order_min(a: Address, b: Address) -> Address {
    if a < b {
        a
    } else {
        b
    }
}
fn order_max(a: Address, b: Address) -> Address {
    if a < b {
        b
    } else {
        a
    }
}

fn parse_str_or_int(s: &str) -> Result<i64, std::num::ParseIntError> {
    // Subgraph BigInt fields come back as JSON strings ("3000"); plain Int fields come back as
    // numbers and serde captures them in the same `String` via custom deserialization below.
    s.parse::<i64>()
}

#[derive(Debug, Deserialize)]
#[serde(bound = "T: serde::de::DeserializeOwned")]
struct GraphQLResponse<T> {
    #[serde(default = "none_opt")]
    data: Option<T>,
    #[serde(default)]
    errors: Option<Vec<GraphQLError>>,
}

fn none_opt<T>() -> Option<T> {
    None
}

#[derive(Debug, Deserialize)]
struct GraphQLError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct PoolWrapper {
    #[serde(default)]
    pool: Option<PoolNode>,
}

#[derive(Debug, Deserialize)]
struct PoolNode {
    token0: TokenNode,
    token1: TokenNode,
    /// Subgraph returns this as a string (BigInt). Some forks emit a JSON number — we accept
    /// either via the helper deserializer.
    #[serde(rename = "feeTier", deserialize_with = "de_string_or_int")]
    fee_tier: String,
    #[serde(rename = "tickSpacing", deserialize_with = "de_string_or_int")]
    tick_spacing: String,
    hooks: String,
}

#[derive(Debug, Deserialize)]
struct TokenNode {
    id: String,
}

/// Accept either a JSON string `"3000"` or a JSON integer `3000`, returning the string form.
fn de_string_or_int<'de, D>(d: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(D::Error::custom(format!(
            "expected string or number, got {other}"
        ))),
    }
}
