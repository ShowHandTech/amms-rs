//! Diagnostic for the "first request always 429" mystery.
//!
//! Hits the same DexTools pools endpoint with 5 different client configurations and dumps:
//!   - status code
//!   - all response headers (especially Cloudflare ones: cf-ray, cf-cache-status, server,
//!     set-cookie, retry-after, x-ratelimit-*)
//!   - the first 400 bytes of the body (so we can tell: is it a Cloudflare bot-challenge HTML
//!     page? a DexTools JSON rate-limit message? an empty body?)
//!
//! Run:
//!   cargo run --release --example debug_dextools_429 -- <API_KEY> [TOKEN_ADDRESS]
//!
//! Configurations tried (in order, one per second):
//!   A. raw reqwest::Client::new() — what we had BEFORE the cookie+UA fix
//!   B. + cookie_store + amms-rs User-Agent — what we have NOW
//!   C. + a browser-shaped User-Agent (Chrome on macOS)
//!   D. + Accept-Encoding gzip,br + Accept-Language en-US
//!   E. brand new client per request (no cookie reuse, simulating cold start each time)
//!
//! Look for: which configurations 429? Does B succeed once it has a __cf_bm cookie? Does C
//! always succeed because the UA passes Cloudflare's signature check? Does the 429 body contain
//! "cloudflare" / "ray id" (Cloudflare bot challenge) or "rate limit" (DexTools API limiter)?

use reqwest::header::{HeaderMap, HeaderValue};
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

    let url = format!(
        "{}/v2/token/{}/{}/pools?sort=creationTime&order=desc&from=2020-01-01T00:00:00.000Z&to=2030-01-01T00:00:00.000Z&page=0&pageSize=50",
        BASE, CHAIN, token
    );
    println!("URL: {url}\n");

    // === A: raw reqwest, nothing tweaked ===
    println!("=== A: reqwest::Client::new() (no UA, no cookies) ===");
    let a = reqwest::Client::new();
    probe(&a, &url, &api_key, &[]).await?;
    sleep(Duration::from_millis(1500)).await;

    // === B: current production config — amms-rs UA + cookie store ===
    println!("\n=== B: amms-rs UA + cookie_store(true) ===");
    let b = reqwest::Client::builder()
        .user_agent("amms-rs/0.7.4")
        .cookie_store(true)
        .build()?;
    probe(&b, &url, &api_key, &[]).await?;
    sleep(Duration::from_millis(1500)).await;
    println!("--- B second request (reusing cookie jar) ---");
    probe(&b, &url, &api_key, &[]).await?;
    sleep(Duration::from_millis(1500)).await;

    // === C: browser-ish UA + cookies ===
    println!("\n=== C: Chrome UA + cookie_store(true) ===");
    let c = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        .cookie_store(true)
        .build()?;
    probe(&c, &url, &api_key, &[]).await?;
    sleep(Duration::from_millis(1500)).await;

    // === D: full browser-shaped header set ===
    println!("\n=== D: Chrome UA + cookies + Accept-* + Accept-Language ===");
    let d = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        .cookie_store(true)
        .build()?;
    probe(
        &d,
        &url,
        &api_key,
        &[
            ("accept-language", "en-US,en;q=0.9"),
            ("accept-encoding", "gzip, deflate, br"),
            ("sec-fetch-dest", "empty"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-site", "none"),
        ],
    )
    .await?;
    sleep(Duration::from_millis(1500)).await;

    // === E: brand-new client every time (no cookie carryover) ===
    println!("\n=== E: 3x fresh client, no cookie reuse (cold start each time) ===");
    for i in 1..=3 {
        println!("--- E#{i} ---");
        let e = reqwest::Client::builder()
            .user_agent("amms-rs/0.7.4")
            .cookie_store(true)
            .build()?;
        probe(&e, &url, &api_key, &[]).await?;
        sleep(Duration::from_millis(1500)).await;
    }

    Ok(())
}

async fn probe(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    extra_headers: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert("X-API-Key", HeaderValue::from_str(api_key)?);
    headers.insert("accept", HeaderValue::from_static("application/json"));
    for (k, v) in extra_headers {
        use reqwest::header::HeaderName;
        let name = HeaderName::from_bytes(k.as_bytes())?;
        headers.insert(name, HeaderValue::from_str(v)?);
    }

    let start = std::time::Instant::now();
    let resp = client.get(url).headers(headers).send().await?;
    let elapsed = start.elapsed();
    let status = resp.status();
    println!("status: {status}  ({}ms)", elapsed.as_millis());

    // Dump ALL response headers — Cloudflare leaves fingerprints in cf-ray / server / cf-cache-status
    // and DexTools' own rate limiter would leave x-ratelimit-* headers.
    println!("response headers:");
    let mut keys: Vec<_> = resp.headers().keys().collect();
    keys.sort_by_key(|k| k.as_str().to_lowercase());
    for k in keys {
        for v in resp.headers().get_all(k) {
            let v_str = v.to_str().unwrap_or("<binary>");
            println!("  {}: {}", k, v_str);
        }
    }

    let body = resp.text().await.unwrap_or_default();
    let preview: String = body.chars().take(400).collect();
    println!("body ({} bytes), first 400 chars:\n  {}", body.len(), preview.replace('\n', "\n  "));
    Ok(())
}
