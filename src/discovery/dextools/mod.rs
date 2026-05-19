//! DexTools v2 API client.
//!
//! Verified endpoint (2026-05-19):
//!
//!   GET https://public-api.dextools.io/trial/v2/token/{chain}/{address}/pools
//!     ?sort=creationTime&order=desc&from=<ISO>&to=<ISO>&page=<n>
//!   Headers: X-API-Key: <key>
//!
//! The endpoint **requires** `sort`, `order`, `from`, `to` (will 400 otherwise) and is
//! paginated. `pools_for_token` walks all pages until `page >= total_pages`.
//!
//! Classification is by `exchange.name` substring (case-insensitive):
//!   - "infinity"             → PCS V4 CL  (hooks/parameters not in response → see TODO)
//!   - "v3"                   → V3 (factory from `exchange.factory`, fee from chain at sync)
//!   - "v2" or just "pancake" → V2 (factory from `exchange.factory`, fee hard-coded per fork)
//!   - "uniswap" + "v4"       → currently dropped (DexTools omits hooks/fee/tickSpacing)

use super::{DiscoveryError, PoolDescriptor, TokenPoolIndex};
use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info, warn};

pub mod schema;

const DEFAULT_BASE_URL: &str = "https://public-api.dextools.io/trial";
// DexTools 400s if from/to are missing. Use a fixed wide window so all pools are returned.
const DEFAULT_FROM: &str = "2020-01-01T00:00:00.000Z";
const DEFAULT_TO: &str = "2030-01-01T00:00:00.000Z";
const PAGE_HARD_LIMIT: u32 = 200;
// Page size. DexTools v2 default is 20; passing pageSize cuts request count.
// Trial tier caps this at 50 (verified 2026-05-19: pageSize=100 returns
// `{"message":"Invalid pageSize, must be less or equal to 50", ...}` 400).
// Higher-tier plans may allow more; bump only after confirming with your key.
const PAGE_SIZE: u32 = 50;
// Trial tier limit is 1 req/s. Add a small buffer so clock skew between local Instant and the
// server's bucket counter doesn't make us bounce off 429s. Override via `with_min_interval`.
const DEFAULT_MIN_INTERVAL: Duration = Duration::from_millis(1100);
// Max retries on 429. The in-process gate handles our own pacing; retry covers cross-process
// contention (another binary using the same key) and transient server-side bucket drain.
const MAX_429_RETRIES: u32 = 5;
// Backoff schedule for 429 retries: 1.2s, 2.4s, 4.8s, 9.6s, 19.2s. Doubles each retry up to
// MAX_429_RETRIES. The first wait is roughly one full bucket window, then we widen exponentially
// so a burst from another caller naturally clears.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(1200);

#[derive(Debug, Clone)]
pub struct DexToolsClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
    /// Minimum spacing between two outgoing requests. Enforced by `gate()` below.
    min_interval: Duration,
    /// Last successful (or attempted) request timestamp. Wrapped in `Arc<Mutex<_>>` so every
    /// clone of the client serializes against the same gate — important because the runner clones
    /// `DexToolsClient` into the bootstrap loop *and* into the Redis-subscriber spawn.
    last_request: Arc<Mutex<Option<Instant>>>,
}

impl DexToolsClient {
    pub fn new(api_key: impl Into<String>) -> Self {
        // DexTools sits behind Cloudflare and flags requests with a bot-y User-Agent (e.g. the
        // reqwest default "reqwest/0.x.y") for the `__cf_bm` challenge — observed as 429 on the
        // very first request even with a fresh API key. We do three things to pass:
        //   1. Send a non-bot User-Agent (a clear, recognizable client identifier; not pretending
        //      to be a browser).
        //   2. Enable cookie_store so the `__cf_bm` challenge cookie set by Cloudflare on the
        //      first response is sent back automatically on subsequent requests — without this
        //      every request looks like a fresh client and gets re-challenged.
        //   3. Enable gzip/brotli + a real Accept-* set, matching what a normal HTTP client sends.
        let http = reqwest::Client::builder()
            .user_agent(concat!("amms-rs/", env!("CARGO_PKG_VERSION")))
            .cookie_store(true)
            .build()
            .expect("default reqwest client should always build");
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: api_key.into(),
            http,
            min_interval: DEFAULT_MIN_INTERVAL,
            last_request: Arc::new(Mutex::new(None)),
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

    /// Override the per-request spacing. Default is 1100ms (trial tier + buffer).
    pub fn with_min_interval(mut self, interval: Duration) -> Self {
        self.min_interval = interval;
        self
    }

    /// Block until at least `min_interval` has passed since the previous request, then bump the
    /// timestamp. Lock is held across the sleep so concurrent callers queue rather than racing.
    async fn gate(&self) {
        let mut last = self.last_request.lock().await;
        if let Some(prev) = *last {
            let elapsed = prev.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *last = Some(Instant::now());
    }

    /// GET with in-process gating + 429 retry. On 429 we honor `Retry-After` (in seconds) if
    /// present, otherwise fall back to exponential backoff. Any other 4xx/5xx propagates as
    /// `DiscoveryError::Http` immediately.
    async fn fetch_with_retry(&self, url: &str) -> Result<String, DiscoveryError> {
        let mut attempt: u32 = 0;
        loop {
            self.gate().await;
            let resp = self
                .http
                .get(url)
                .header("X-API-Key", &self.api_key)
                .header("accept", "application/json")
                .send()
                .await?;
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && attempt < MAX_429_RETRIES {
                // Prefer server-supplied Retry-After (seconds). Fall back to exponential backoff.
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(Duration::from_secs);
                let backoff = retry_after.unwrap_or_else(|| RETRY_BASE_DELAY * (1u32 << attempt));
                warn!(
                    target: "dextools",
                    failed_url = url,
                    attempt = attempt + 1,
                    max_attempts = MAX_429_RETRIES,
                    backoff_ms = backoff.as_millis() as u64,
                    "GET returned 429 — sleeping then retrying the same URL"
                );
                // Drop the response (consumes the body) before sleeping so the connection
                // returns to the pool.
                drop(resp);
                tokio::time::sleep(backoff).await;
                // Reset the gate timestamp so the next call waits a full interval, not just the
                // remainder.
                {
                    let mut last = self.last_request.lock().await;
                    *last = Some(Instant::now());
                }
                attempt += 1;
                continue;
            }
            return Ok(resp.error_for_status()?.text().await?);
        }
    }
}

#[async_trait]
impl TokenPoolIndex for DexToolsClient {
    async fn pools_for_token(
        &self,
        chain: &str,
        token: Address,
    ) -> Result<Vec<PoolDescriptor>, DiscoveryError> {
        let mut out = Vec::new();
        let mut page: u32 = 0;
        let mut total_pages: u32 = 1;
        let mut total_raw: usize = 0;
        let mut total_dropped: usize = 0;

        while page < total_pages && page < PAGE_HARD_LIMIT {
            let url = format!(
                "{}/v2/token/{}/{}/pools?sort=creationTime&order=desc&from={}&to={}&page={}&pageSize={}",
                self.base_url, chain, token, DEFAULT_FROM, DEFAULT_TO, page, PAGE_SIZE,
            );
            let body = self.fetch_with_retry(&url).await?;
            let parsed: schema::PoolsResponse = serde_json::from_str(&body).map_err(|e| {
                DiscoveryError::Malformed(format!(
                    "DexTools response parse failed: {e}; body preview: {}",
                    truncate(&body, 400)
                ))
            })?;

            total_pages = parsed.data.total_pages.max(1);
            let page_raw = parsed.data.results.len();
            total_raw += page_raw;
            let mut page_dropped = 0;
            for raw in parsed.data.results {
                let dex_name = raw.exchange.name.clone();
                let raw_address = raw.address.clone();
                match to_pool_descriptor(raw) {
                    Ok(d) => {
                        debug!(
                            target: "dextools",
                            exchange = %dex_name,
                            address = %raw_address,
                            "keep pool"
                        );
                        out.push(d);
                    }
                    Err(e) => {
                        page_dropped += 1;
                        debug!(
                            target: "dextools",
                            exchange = %dex_name,
                            address = %raw_address,
                            error = %e,
                            "drop pool"
                        );
                    }
                }
            }
            total_dropped += page_dropped;
            debug!(
                target: "dextools",
                %token,
                page,
                total_pages,
                page_raw,
                page_kept = page_raw - page_dropped,
                page_dropped,
                "page processed"
            );
            page += 1;
        }

        info!(
            target: "dextools",
            %token,
            pages = page,
            total_pages,
            raw = total_raw,
            kept = out.len(),
            dropped = total_dropped,
            "discovery complete"
        );
        if page == PAGE_HARD_LIMIT && page < total_pages {
            warn!(
                target: "dextools",
                %token,
                fetched_pages = page,
                total_pages,
                "hit PAGE_HARD_LIMIT; remaining pages skipped"
            );
        }
        Ok(out)
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…(+{})", &s[..max], s.len() - max)
    }
}

/// Map a single DexTools pool record to our internal `PoolDescriptor`.
fn to_pool_descriptor(raw: schema::Pool) -> Result<PoolDescriptor, DiscoveryError> {
    let dex = raw.exchange.name.to_lowercase();
    let factory_str = raw
        .exchange
        .factory
        .as_deref()
        .ok_or(DiscoveryError::MissingField("exchange.factory"))?;
    let factory_addr: Address = factory_str
        .parse()
        .map_err(|e| DiscoveryError::Malformed(format!("bad factory {factory_str}: {e}")))?;

    // Order matters: "infinity" before generic "v2/v3", and "v3" before "v2" (since "v3" name
    // does not contain "v2" but "pancakeswap v3" would match the bare "pancakeswap" V2 branch).

    // PCS V4 Infinity CL.
    //
    // Currently dropped from the token-first path: DexTools omits `hooks` and `parameters`
    // (which encodes `tick_spacing`), and inserting an Infinity pool without `tick_spacing`
    // makes the downstream tickBitmap batch read query the wrong slots. The cleanest fix is
    // a chain lookup via `CLPoolManager.poolIdToPoolKey(bytes32)` — TODO once that batch is
    // written. Until then PCS V4 Infinity pools only enter the StateSpace via the
    // factory-first `discover()` path (which is unavailable on short-retention BSC nodes).
    if dex.contains("infinity") {
        return Err(DiscoveryError::Malformed(
            "PCS V4 Infinity: hooks/parameters missing — needs poolIdToPoolKey chain lookup"
                .to_string(),
        ));
    }

    // Uniswap V4 (must be matched before generic "v4"/"pancake" branches but after Infinity).
    //
    // DexTools returns `address` (=PoolId), `exchange.factory` (=PoolManager), `fee` as percentage,
    // `mainToken`/`sideToken` — but **not** `tickSpacing` and **not** `hooks`. Emit a partial
    // descriptor with the fields we have; `enrich_uniswap_v4_via_subgraph` will fill the rest
    // before any factory tries to use it.
    if dex.contains("uniswap") && dex.contains("v4") {
        let pool_id = parse_b256(&raw.address)?;
        let pool_manager = factory_addr;
        // Tokens come back as `mainToken` (the token DexTools considers "main") and `sideToken`.
        // V4 PoolKey requires `currency0 < currency1` byte ordering, so sort them ourselves.
        let (currency0, currency1) = match sort_token_pair(&raw.main_token, &raw.side_token) {
            Ok(pair) => (Some(pair.0), Some(pair.1)),
            Err(_) => (None, None),
        };
        // DexTools `fee` is a percentage float (e.g. `1.99` = 1.99%). V4 PoolKey fee is in pips
        // (1e6 = 100%). Convert: pct * 10000. Round to nearest integer to absorb FP noise.
        let fee = raw
            .fee
            .map(|pct| (pct * 10_000.0).round() as i64)
            .and_then(|v| {
                if (0..=1_000_000).contains(&v) {
                    Some(v as u32)
                } else {
                    None
                }
            });
        return Ok(PoolDescriptor::UniswapV4 {
            pool_id,
            pool_manager,
            currency0,
            currency1,
            fee,
            tick_spacing: None,
            hooks: None,
        });
    }

    // V3.
    if dex.contains("v3") {
        let address: Address = raw
            .address
            .parse()
            .map_err(|e| DiscoveryError::Malformed(format!("bad pool address: {e}")))?;
        return Ok(PoolDescriptor::V3 {
            address,
            factory: factory_addr,
        });
    }

    // V2 family. Match either explicit "v2" or any pancake variant without v3/v4/infinity markers.
    let is_v2 = dex.contains("v2")
        || (dex.contains("pancakeswap")
            && !dex.contains("v3")
            && !dex.contains("v4")
            && !dex.contains("infinity"));
    if is_v2 {
        let address: Address = raw
            .address
            .parse()
            .map_err(|e| DiscoveryError::Malformed(format!("bad pool address: {e}")))?;
        // PCS V2 = 25 (0.25%); UniV2 / SushiV2 = 30. On BSC PCS dominates; default to 25 when
        // we can't tell. The runner can override per-pool by registering the right factory.
        let fee = if dex.contains("pancake") { 25 } else { 30 };
        return Ok(PoolDescriptor::V2 {
            address,
            factory: factory_addr,
            fee,
        });
    }

    Err(DiscoveryError::Malformed(format!(
        "unrecognized DEX variant: {dex}"
    )))
}

fn sort_token_pair(
    a: &Option<schema::TokenRef>,
    b: &Option<schema::TokenRef>,
) -> Result<(Address, Address), DiscoveryError> {
    let a_addr: Address = a
        .as_ref()
        .ok_or(DiscoveryError::MissingField("mainToken"))?
        .address
        .parse()
        .map_err(|e| DiscoveryError::Malformed(format!("bad mainToken: {e}")))?;
    let b_addr: Address = b
        .as_ref()
        .ok_or(DiscoveryError::MissingField("sideToken"))?
        .address
        .parse()
        .map_err(|e| DiscoveryError::Malformed(format!("bad sideToken: {e}")))?;
    if a_addr < b_addr {
        Ok((a_addr, b_addr))
    } else {
        Ok((b_addr, a_addr))
    }
}

fn parse_b256(s: &str) -> Result<B256, DiscoveryError> {
    s.parse::<B256>()
        .map_err(|e| DiscoveryError::Malformed(format!("bad bytes32 {s}: {e}")))
}
