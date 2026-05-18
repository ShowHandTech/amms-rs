//! Phase 9: Redis mirror layer.
//!
//! Streams pool state changes to Redis so that external consumers (dashboard, notification
//! daemons, slow-strategy bots) can subscribe without holding their own `StateSpace`.
//!
//! Layout, keyed by namespace (e.g. `"bsc"`):
//!  - `mirror:{ns}:pool:{amm_id_json}` — STRING, latest JSON snapshot of the AMM
//!  - `mirror:{ns}:updates` — pub/sub channel; payload is `{"id": <AmmId>, "block": N}`
//!
//! Write granularity is **per affected block**: after `StateSpace::sync` returns the affected
//! `AmmId`s, the runner calls `flush_block` once with the set. This keeps Redis traffic
//! proportional to "blocks that touched our pools" rather than to every log.

use crate::amms::amm::{AmmId, AutomatedMarketMaker, AMM};
use crate::state_space::StateSpace;
use alloy::primitives::Address;
use redis::AsyncCommands;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum MirrorError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct MirrorKeys {
    pub pool_prefix: String,
    pub updates_channel: String,
    pub tokens_set: String,
}

impl MirrorKeys {
    pub fn for_namespace(namespace: &str) -> Self {
        Self {
            pool_prefix: format!("mirror:{namespace}:pool"),
            updates_channel: format!("mirror:{namespace}:updates"),
            tokens_set: format!("mirror:{namespace}:tokens"),
        }
    }

    pub fn pool_key(&self, id: &AmmId) -> String {
        // AmmId is small + deterministic; JSON encoding makes the key human-debuggable.
        let json = serde_json::to_string(id).unwrap_or_else(|_| "<unencodable>".into());
        format!("{}:{}", self.pool_prefix, json)
    }
}

/// Write a single pool snapshot to the mirror and publish an update notification.
pub async fn write_pool(
    conn: &mut redis::aio::ConnectionManager,
    keys: &MirrorKeys,
    amm: &AMM,
    block: u64,
) -> Result<(), MirrorError> {
    let id = amm.id();
    let key = keys.pool_key(&id);
    let payload = serde_json::to_string(amm)?;
    let _: () = conn.set(&key, payload).await?;
    let notice = serde_json::to_string(&serde_json::json!({
        "id": id,
        "block": block,
    }))?;
    let _: () = conn.publish(&keys.updates_channel, notice).await?;
    Ok(())
}

/// Flush a batch of pool updates touched during one block. Logs and continues on per-pool
/// errors instead of aborting the whole batch.
pub async fn flush_block(
    conn: &mut redis::aio::ConnectionManager,
    keys: &MirrorKeys,
    state: &StateSpace,
    affected: &[AmmId],
    block: u64,
) -> Result<(), MirrorError> {
    for id in affected {
        let Some(amm) = state.get(id) else { continue };
        if let Err(e) = write_pool(conn, keys, amm, block).await {
            warn!(target: "redis_mirror", error = %e, ?id, "mirror write failed");
        }
    }
    Ok(())
}

/// Refresh the `mirror:{ns}:tokens` set from current `by_token` keys. Cheap O(N) walk; intended
/// to run on startup and periodically to keep external consumers in sync with the active set.
pub async fn refresh_tokens_set(
    conn: &mut redis::aio::ConnectionManager,
    keys: &MirrorKeys,
    state: &StateSpace,
) -> Result<(), MirrorError> {
    let _: () = conn.del(&keys.tokens_set).await?;
    let tokens: Vec<Address> = state.by_token.keys().copied().collect();
    if tokens.is_empty() {
        return Ok(());
    }
    let as_strings: Vec<String> = tokens.iter().map(|t| t.to_string()).collect();
    let _: () = conn.sadd(&keys.tokens_set, as_strings).await?;
    Ok(())
}
