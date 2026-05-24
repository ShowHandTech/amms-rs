//! Mempool feeds: normalize multiple pending-tx sources into a single `mpsc<MempoolTx>`.
//!
//! Each implementation of [`MempoolFeed`] spawns its own subscription task and pushes normalized
//! [`MempoolTx`] records onto a channel. The MEV pipeline reads from the channel and dispatches
//! to the decoder. Sources are deliberately decoupled from downstream logic so that a single
//! flaky feed (e.g. Puissant going dark) cannot block the others.

pub mod public;
pub mod puissant;

use std::time::Instant;

use alloy::primitives::{Address, Bytes, B256, U256};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Normalized representation of a pending transaction. Different sources fill different subsets
/// of these fields — public mempool generally has everything, Puissant may redact `tx_hash` and
/// only deliver decoded swap intent. The decoder layer reads what it needs and ignores the rest.
#[derive(Debug, Clone)]
pub struct MempoolTx {
    /// Static label identifying which feed produced this record. Used in profit logs to
    /// attribute the underlying signal source.
    pub source: &'static str,
    pub tx_hash: Option<B256>,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub value: U256,
    /// Raw calldata. The decoder dispatches by `(to, selector = input[..4])`.
    pub input: Bytes,
    pub gas_price: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    /// Wall-clock receive time on this process. Used for end-to-end latency stats — never trust
    /// the wall clock on the feed-producer side.
    pub received_at: Instant,
}

/// A pending-transaction source. Implementations own their own connection and reconnection
/// policy; on permanent failure they should return `Err` to let `main` decide how to react
/// (typically: log + exit, since a missing feed is silently catastrophic for a MEV bot).
#[async_trait]
pub trait MempoolFeed: Send + Sync {
    /// Drive the subscription. Should run forever; returning `Ok(())` indicates a clean shutdown
    /// while `Err(_)` is a fatal failure.
    async fn subscribe(self: std::sync::Arc<Self>, tx: mpsc::Sender<MempoolTx>)
        -> eyre::Result<()>;

    fn name(&self) -> &'static str;
}
