//! Decode mempool transactions into [`SwapIntent`] structs the speculator can replay.
//!
//! Dispatch table is keyed by `(to, selector)`. Each handler returns `Option<SwapIntent>` —
//! `None` means "looked like a swap but the decoding failed", which the dispatcher logs at debug
//! and counts in `unknown_call`. Routers / selectors that aren't registered fall through to
//! `unknown_router` / `unknown_selector` counters so we can see the long-tail of MEV opportunity
//! we are missing.

pub mod pcs_v2;
pub mod pcs_v3;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::{Address, U256};
use tracing::debug;

use crate::mempool::MempoolTx;

/// A normalized swap intent extracted from a pending tx. `path` is in execution order, so
/// `path[0].token_in` is the user-supplied input and `path[last].token_out` is the final output.
/// `amount_in` is in `path[0].token_in` units; `min_amount_out` is in `path[last].token_out`
/// units.
#[derive(Debug, Clone)]
pub struct SwapIntent {
    pub source: MempoolTx,
    pub router: Address,
    pub path: Vec<Hop>,
    pub amount_in: U256,
    pub min_amount_out: U256,
    pub deadline: u64,
}

#[derive(Debug, Clone)]
pub struct Hop {
    pub token_in: Address,
    pub token_out: Address,
    /// Most V2 routers don't specify the pool address in calldata (they look it up via
    /// `factory.getPair`), so this is usually `None`. V3 / V4 routers that name a specific
    /// pool can fill it in. The speculator falls back to `by_token` intersection when `None`.
    pub pool_hint: Option<amms::amms::amm::AmmId>,
}

pub type DecodeFn = fn(&MempoolTx) -> Option<SwapIntent>;

/// Counters for the decoder dispatcher. Kept as a struct of atomics so the running binary can
/// log them periodically without locking. Phase 14 promotes these to Prometheus metrics; for
/// now a `debug` dump every N tx is enough to size the long-tail.
#[derive(Debug, Default)]
pub struct DecoderStats {
    pub total: AtomicU64,
    pub decoded: AtomicU64,
    pub unknown_router: AtomicU64,
    pub unknown_selector: AtomicU64,
    pub short_input: AtomicU64,
    pub decode_failed: AtomicU64,
}

pub struct DecoderRegistry {
    /// Set of known routers — used to differentiate "unknown router (we have never registered
    /// it)" from "known router but selector we don't handle yet". The latter is more interesting
    /// because it means there's straightforward MEV we'd capture with one more decoder.
    known_routers: HashMap<Address, &'static str>,
    by_router_selector: HashMap<(Address, [u8; 4]), DecodeFn>,
    pub stats: DecoderStats,
}

impl DecoderRegistry {
    pub fn new() -> Self {
        Self {
            known_routers: HashMap::new(),
            by_router_selector: HashMap::new(),
            stats: DecoderStats::default(),
        }
    }

    pub fn register(
        &mut self,
        router: Address,
        router_name: &'static str,
        selector: [u8; 4],
        decoder: DecodeFn,
    ) {
        self.known_routers.insert(router, router_name);
        self.by_router_selector.insert((router, selector), decoder);
    }

    /// Decode a pending tx into a `SwapIntent` if there's a matching handler. Returns `None`
    /// for any reason a swap cannot be produced (no `to`, short input, unknown router, etc.).
    pub fn decode(&self, tx: &MempoolTx) -> Option<SwapIntent> {
        self.stats.total.fetch_add(1, Ordering::Relaxed);

        let Some(to) = tx.to else {
            return None;
        };
        if tx.input.len() < 4 {
            self.stats.short_input.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let selector: [u8; 4] = [tx.input[0], tx.input[1], tx.input[2], tx.input[3]];

        let Some(handler) = self.by_router_selector.get(&(to, selector)) else {
            if self.known_routers.contains_key(&to) {
                self.stats.unknown_selector.fetch_add(1, Ordering::Relaxed);
                debug!(
                    target: "mev::decoder",
                    router = ?to,
                    router_name = self.known_routers.get(&to).copied().unwrap_or("?"),
                    selector = ?selector,
                    "known router, unknown selector",
                );
            } else {
                self.stats.unknown_router.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        };

        match handler(tx) {
            Some(intent) => {
                self.stats.decoded.fetch_add(1, Ordering::Relaxed);
                Some(intent)
            }
            None => {
                self.stats.decode_failed.fetch_add(1, Ordering::Relaxed);
                debug!(
                    target: "mev::decoder",
                    router = ?to,
                    selector = ?selector,
                    "handler returned None — abi decode failed",
                );
                None
            }
        }
    }
}

impl Default for DecoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}
