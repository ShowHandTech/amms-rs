//! Phase 7: Redis bridge for token-first runtime.
//!
//! Wires three concerns together:
//!  1. **Token list bootstrap** — on startup, read `tokens:{namespace}:active` (SET) to know
//!     which tokens to track immediately.
//!  2. **Live updates** — subscribe to `tokens:{namespace}:changes` (Pub/Sub) for runtime
//!     TRACK / UNTRACK commands published by the external token service.
//!  3. **Pool list cache** — write `pools:{namespace}:{token}` (HASH) so that on restart we
//!     can skip the DexTools call when the cache is fresh enough.
//!
//! The bridge does NOT drive RPC or simulate swaps — it just translates Redis messages into
//! calls on `StateSpace::track_pools` / `untrack_token`. The actual `discover_and_sync_for_token`
//! call is the caller's responsibility (passed in as a closure) so that the bridge stays
//! provider-agnostic.

use crate::amms::amm::{AutomatedMarketMaker, AMM};
use crate::state_space::{StateSpace, UntrackError};
use alloy::primitives::Address;
use redis::AsyncCommands;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{info, warn};

const POOL_LIST_TTL_SECONDS: usize = 6 * 60 * 60;

#[derive(Debug, Error)]
pub enum RedisBridgeError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("invalid address in redis message: {0}")]
    BadAddress(String),
    #[error("malformed pub/sub command: {0}")]
    BadCommand(String),
}

/// Names of the Redis keys / channels this bridge reads and writes. All names are derived from
/// a single `namespace` slug (e.g. `"bsc"`) so multiple chains can share one Redis instance.
#[derive(Debug, Clone)]
pub struct RedisKeys {
    pub active_set: String,
    pub changes_channel: String,
    pub pool_list_prefix: String,
}

impl RedisKeys {
    pub fn for_namespace(namespace: &str) -> Self {
        Self {
            active_set: format!("tokens:{namespace}:active"),
            changes_channel: format!("tokens:{namespace}:changes"),
            pool_list_prefix: format!("pools:{namespace}"),
        }
    }

    pub fn pool_list_for_token(&self, token: Address) -> String {
        format!("{}:{token}", self.pool_list_prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenCommand {
    Track(Address),
    Untrack(Address),
}

/// Parse a single pub/sub payload. Accepted forms:
///   `TRACK 0x...`     — start tracking a token
///   `UNTRACK 0x...`   — stop tracking a token
pub fn parse_command(payload: &str) -> Result<TokenCommand, RedisBridgeError> {
    let trimmed = payload.trim();
    let (op, rest) = trimmed
        .split_once(|c: char| c.is_whitespace())
        .ok_or_else(|| RedisBridgeError::BadCommand(trimmed.to_string()))?;
    let addr: Address = rest
        .trim()
        .parse()
        .map_err(|_| RedisBridgeError::BadAddress(rest.to_string()))?;
    match op.to_ascii_uppercase().as_str() {
        "TRACK" => Ok(TokenCommand::Track(addr)),
        "UNTRACK" => Ok(TokenCommand::Untrack(addr)),
        other => Err(RedisBridgeError::BadCommand(other.to_string())),
    }
}

/// Snapshot the `tokens:{ns}:active` set at startup. The caller drives `discover` for each.
pub async fn fetch_active_tokens(
    conn: &mut redis::aio::ConnectionManager,
    keys: &RedisKeys,
) -> Result<Vec<Address>, RedisBridgeError> {
    let raw: Vec<String> = conn.smembers(&keys.active_set).await?;
    let mut out = Vec::with_capacity(raw.len());
    for s in raw {
        let addr: Address = s
            .parse()
            .map_err(|_| RedisBridgeError::BadAddress(s.clone()))?;
        out.push(addr);
    }
    Ok(out)
}

/// Read the cached pool list for a token. Returns an empty Vec when there is no cache.
/// The HASH layout is `pools:{ns}:{token}` -> field `ids` -> serialized JSON of AmmIds, plus
/// metadata fields like `cached_at`. The bridge keeps this opaque — the discovery layer is
/// responsible for shape.
pub async fn read_pool_cache(
    conn: &mut redis::aio::ConnectionManager,
    keys: &RedisKeys,
    token: Address,
) -> Result<Option<String>, RedisBridgeError> {
    let key = keys.pool_list_for_token(token);
    let raw: Option<String> = conn.hget(&key, "ids").await?;
    Ok(raw)
}

/// Write the pool list cache. `payload` is opaque (JSON encoded list of `AmmId`s by the caller).
/// Sets a TTL so stale entries get refreshed after a fixed period.
pub async fn write_pool_cache(
    conn: &mut redis::aio::ConnectionManager,
    keys: &RedisKeys,
    token: Address,
    payload: &str,
) -> Result<(), RedisBridgeError> {
    let key = keys.pool_list_for_token(token);
    let _: () = conn.hset(&key, "ids", payload).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let _: () = conn.hset(&key, "cached_at", now).await?;
    let _: () = conn.expire(&key, POOL_LIST_TTL_SECONDS as i64).await?;
    Ok(())
}

/// Delete the pool list cache for a token (used on UNTRACK).
pub async fn delete_pool_cache(
    conn: &mut redis::aio::ConnectionManager,
    keys: &RedisKeys,
    token: Address,
) -> Result<(), RedisBridgeError> {
    let key = keys.pool_list_for_token(token);
    let _: () = conn.del(&key).await?;
    Ok(())
}

/// Drive the live subscription. For every message received, parse it, dispatch to the
/// appropriate handler, and update Redis cache. Runs until the underlying connection ends.
///
/// `on_track` is invoked when a new TRACK arrives; it should perform DexTools discovery + RPC
/// init and return the pools to insert. The bridge then writes them to `state` and updates the
/// pool cache.
pub async fn run_subscriber<F, Fut>(
    redis_url: &str,
    namespace: &str,
    state: Arc<RwLock<StateSpace>>,
    mut on_track: F,
) -> Result<(), RedisBridgeError>
where
    F: FnMut(Address) -> Fut + Send,
    Fut: Future<Output = Result<Vec<AMM>, eyre::Report>> + Send,
{
    let keys = RedisKeys::for_namespace(namespace);
    let client = redis::Client::open(redis_url)?;
    let mut cmd_conn = redis::aio::ConnectionManager::new(client.clone()).await?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(&keys.changes_channel).await?;
    let mut stream = pubsub.on_message();

    info!(
        target: "redis_bridge",
        channel = %keys.changes_channel,
        "Subscribed to token changes"
    );

    use futures::StreamExt;
    while let Some(msg) = stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(e) => {
                warn!(target: "redis_bridge", error = %e, "Bad pub/sub payload");
                continue;
            }
        };
        match parse_command(&payload) {
            Ok(TokenCommand::Track(token)) => {
                info!(target: "redis_bridge", %token, "TRACK");
                match on_track(token).await {
                    Ok(pools) => {
                        let amm_ids: Vec<_> = pools.iter().map(|a| a.id()).collect();
                        state.write().await.track_pools(pools, token);
                        if let Ok(payload) = serde_json::to_string(&amm_ids) {
                            if let Err(e) =
                                write_pool_cache(&mut cmd_conn, &keys, token, &payload).await
                            {
                                warn!(target: "redis_bridge", error = %e, "pool cache write failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(target: "redis_bridge", %token, error = %e, "discover failed");
                    }
                }
            }
            Ok(TokenCommand::Untrack(token)) => {
                info!(target: "redis_bridge", %token, "UNTRACK");
                match state.write().await.untrack_token(token) {
                    Ok(report) => {
                        info!(
                            target: "redis_bridge",
                            %token,
                            removed = report.removed_pool_ids.len(),
                            "untracked"
                        );
                        if let Err(e) = delete_pool_cache(&mut cmd_conn, &keys, token).await {
                            warn!(target: "redis_bridge", error = %e, "pool cache delete failed");
                        }
                    }
                    Err(UntrackError::CoreTokenProtected(_)) => {
                        warn!(target: "redis_bridge", %token, "refused UNTRACK on core token");
                    }
                }
            }
            Err(e) => {
                warn!(target: "redis_bridge", error = %e, payload = %payload, "bad command");
            }
        }
    }
    Ok(())
}
