//! Diagnostic for the "page=1 always 429 after page=0 success" pattern.
//!
//! The earlier version of this test hit the SAME url repeatedly — Cloudflare cached the response
//! at the edge (`cache-control: max-age=120`) so only the very first call ever reached DexTools
//! origin. That hid the actual rate-limit behavior. This version forces every request to be a
//! cache MISS by varying the page number, so each one really hits origin and consumes a token.
//!
//! What we want to measure: at what spacing between back-to-back cache-MISS requests does
//! DexTools start returning 429? Our production gate uses 1100ms; the trial tier docs say 1
//! req/s but the actual window may be wider (sliding window, sub-1.0 burst capacity, etc).
//!
//! Run:
//!   cargo run --release --example debug_dextools_429 -- <API_KEY> [TOKEN_ADDRESS]
//!
//! Sends 6 consecutive requests, each to a different page (page=0..5), at each of these
//! spacings, and counts how many 429: 800ms, 1000ms, 1100ms, 1300ms, 1600ms, 2000ms.

use std::time::Duration;
use tokio::time::sleep;

const DEFAULT_TOKEN: &str = "0x55d398326f99059fF775485246999027B3197955"; // USDT BSC
const CHAIN: &str = "bsc";
const BASE: &str = "https://public-api.dextools.io/trial";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let api_key = args
        .get(1)
        .cloned()
        .or_else(|| std::env::var("DEXTOOLS_API_KEY").ok())
        .expect("usage: debug_dextools_429 <API_KEY> [TOKEN_ADDRESS]  or set DEXTOOLS_API_KEY");
    let token = args.get(2).cloned().unwrap_or_else(|| DEFAULT_TOKEN.to_string());

    let client = reqwest::Client::builder()
        .user_agent("amms-rs/0.7.4")
        .cookie_store(true)
        .build()?;

    // Test spacings — for each, send 6 cache-MISS requests back-to-back at that interval.
    // We vary the page number every request to guarantee cache MISS at the CF edge.
    let spacings_ms: &[u64] = &[800, 1000, 1100, 1300, 1600, 2000];

    // Use a different page-offset per spacing so URLs never repeat across runs (else later runs
    // would hit cached responses from earlier runs).
    let mut page_base: u32 = 0;
    for &spacing_ms in spacings_ms {
        println!(
            "\n=== spacing = {spacing_ms}ms — 6 back-to-back requests, each a different page ===",
        );
        let mut ok = 0;
        let mut bad = 0;
        for i in 0..6 {
            let page = page_base + i;
            let url = format!(
                "{}/v2/token/{}/{}/pools?sort=creationTime&order=desc&from=2020-01-01T00:00:00.000Z&to=2030-01-01T00:00:00.000Z&page={}&pageSize=50",
                BASE, CHAIN, token, page,
            );
            let status = quick_probe(&client, &url, &api_key).await?;
            if status.is_success() {
                ok += 1;
            } else {
                bad += 1;
            }
            if i < 5 {
                sleep(Duration::from_millis(spacing_ms)).await;
            }
        }
        page_base += 6;
        println!("  → {ok} ok / {bad} non-2xx at {spacing_ms}ms spacing");
        // Cool-down between spacings so one run's residue doesn't bleed into the next.
        sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}

async fn quick_probe(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
) -> Result<reqwest::StatusCode, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let resp = client
        .get(url)
        .header("X-API-Key", api_key)
        .header("accept", "application/json")
        .send()
        .await?;
    let elapsed = start.elapsed();
    let status = resp.status();
    let cf_cache = resp
        .headers()
        .get("cf-cache-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("?")
        .to_string();
    let cf_ray = resp
        .headers()
        .get("cf-ray")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("?")
        .to_string();
    // Pull page out of url for compact logging
    let page = url
        .split("page=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .unwrap_or("?");
    println!(
        "  page={page} status={status} cache={cf_cache} ray={cf_ray} {}ms",
        elapsed.as_millis()
    );
    Ok(status)
}

