//! DexTools v2 API client.
//!
//! Endpoint shape (best-effort; the exact response schema may need adjustment once real samples
//! land — see plan Open Item #1):
//!
//!   GET https://public-api.dextools.io/trial/v2/token/{chain}/{address}/pools
//!     Headers: X-API-Key: <key>
//!
//! `chain` is DexTools' chain slug (e.g. `"bsc"`, `"ether"`, `"polygon"`).
//!
//! The current parser is permissive: missing fields degrade gracefully (e.g. unknown DEX
//! variants are skipped, V4 pools without PoolKey fields are emitted with `Option::None`).

use super::{DiscoveryError, PoolDescriptor, TokenPoolIndex};
use alloy::primitives::{Address, B256};
use async_trait::async_trait;

pub mod schema;

const DEFAULT_BASE_URL: &str = "https://public-api.dextools.io/trial";

#[derive(Debug, Clone)]
pub struct DexToolsClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

impl DexToolsClient {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: api_key.into(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }
}

#[async_trait]
impl TokenPoolIndex for DexToolsClient {
    async fn pools_for_token(
        &self,
        chain: &str,
        token: Address,
    ) -> Result<Vec<PoolDescriptor>, DiscoveryError> {
        let url = format!(
            "{}/v2/token/{}/{}/pools",
            self.base_url,
            chain,
            // DexTools accepts checksummed hex; alloy's Display gives 0x-prefixed lower-case
            // which DexTools also accepts.
            token
        );
        let resp = self
            .http
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .header("accept", "application/json")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let parsed: schema::PoolsResponse = serde_json::from_str(&resp)?;
        Ok(parsed
            .data
            .into_iter()
            .filter_map(|raw| to_pool_descriptor(raw).ok())
            .collect())
    }
}

/// Map a single DexTools pool record to our internal `PoolDescriptor`. Unknown DEX variants
/// return `Err(Malformed)` so the caller can filter them out.
fn to_pool_descriptor(raw: schema::Pool) -> Result<PoolDescriptor, DiscoveryError> {
    let dex = raw.exchange.as_deref().unwrap_or("").to_lowercase();
    let factory_addr = raw
        .factory
        .as_deref()
        .map(|s| s.parse::<Address>())
        .transpose()
        .map_err(|e| DiscoveryError::Malformed(format!("bad factory address: {e}")))?;

    // V2-family classification: factory address present + exchange name matches a known V2 fork.
    if dex.contains("v2")
        || dex.contains("pancakeswap")
            && !dex.contains("v3")
            && !dex.contains("v4")
            && !dex.contains("infinity")
    {
        let address = raw
            .address
            .parse::<Address>()
            .map_err(|e| DiscoveryError::Malformed(format!("bad pool address: {e}")))?;
        let factory = factory_addr.ok_or(DiscoveryError::MissingField("factory"))?;
        let fee = raw.fee_bps.unwrap_or(25); // PCS V2 default 25 bps; UniV2 = 30
        return Ok(PoolDescriptor::V2 {
            address,
            factory,
            fee,
        });
    }

    // V3-family.
    if dex.contains("v3") {
        let address = raw
            .address
            .parse::<Address>()
            .map_err(|e| DiscoveryError::Malformed(format!("bad pool address: {e}")))?;
        let factory = factory_addr.ok_or(DiscoveryError::MissingField("factory"))?;
        return Ok(PoolDescriptor::V3 { address, factory });
    }

    // PCS V4 Infinity CL.
    if dex.contains("pancake") && (dex.contains("infinity") || dex.contains("v4")) {
        let pool_id = parse_b256(&raw.address)?;
        let cl_pool_manager = factory_addr.ok_or(DiscoveryError::MissingField("factory"))?;
        let currency0 = raw
            .currency0
            .as_deref()
            .map(|s| s.parse::<Address>())
            .transpose()
            .map_err(|e| DiscoveryError::Malformed(format!("bad currency0: {e}")))?;
        let currency1 = raw
            .currency1
            .as_deref()
            .map(|s| s.parse::<Address>())
            .transpose()
            .map_err(|e| DiscoveryError::Malformed(format!("bad currency1: {e}")))?;
        let hooks = raw
            .hooks
            .as_deref()
            .map(|s| s.parse::<Address>())
            .transpose()
            .map_err(|e| DiscoveryError::Malformed(format!("bad hooks: {e}")))?;
        let parameters = raw
            .parameters
            .as_deref()
            .map(parse_b256)
            .transpose()?;
        return Ok(PoolDescriptor::PcsV4Cl {
            pool_id,
            cl_pool_manager,
            currency0,
            currency1,
            hooks,
            parameters,
        });
    }

    // Uniswap V4.
    if dex.contains("uniswap") && dex.contains("v4") {
        let pool_id = parse_b256(&raw.address)?;
        let pool_manager = factory_addr.ok_or(DiscoveryError::MissingField("factory"))?;
        let currency0 = raw
            .currency0
            .as_deref()
            .ok_or(DiscoveryError::MissingField("currency0"))?
            .parse::<Address>()
            .map_err(|e| DiscoveryError::Malformed(format!("bad currency0: {e}")))?;
        let currency1 = raw
            .currency1
            .as_deref()
            .ok_or(DiscoveryError::MissingField("currency1"))?
            .parse::<Address>()
            .map_err(|e| DiscoveryError::Malformed(format!("bad currency1: {e}")))?;
        let fee = raw.v4_fee.ok_or(DiscoveryError::MissingField("v4_fee"))?;
        let tick_spacing = raw
            .v4_tick_spacing
            .ok_or(DiscoveryError::MissingField("v4_tick_spacing"))?;
        let hooks = raw
            .hooks
            .as_deref()
            .unwrap_or("0x0000000000000000000000000000000000000000")
            .parse::<Address>()
            .map_err(|e| DiscoveryError::Malformed(format!("bad hooks: {e}")))?;
        return Ok(PoolDescriptor::UniswapV4 {
            pool_id,
            pool_manager,
            currency0,
            currency1,
            fee,
            tick_spacing,
            hooks,
        });
    }

    Err(DiscoveryError::Malformed(format!(
        "unrecognized DEX variant: {dex}"
    )))
}

fn parse_b256(s: &str) -> Result<B256, DiscoveryError> {
    s.parse::<B256>()
        .map_err(|e| DiscoveryError::Malformed(format!("bad bytes32 {s}: {e}")))
}
