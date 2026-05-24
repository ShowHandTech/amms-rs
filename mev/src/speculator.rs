//! Speculator: take a decoded [`SwapIntent`] and replay it on cloned pools to predict the
//! post-tx state of every pool the victim would touch.
//!
//! The output is a list of [`ProjectedHop`]s — one per hop in the intent's path. Each carries a
//! mutated `AMM` clone (the "what the pool would look like after this swap lands"), the
//! `amount_in` actually fed into the hop, and the resulting `amount_out`. The pathfinder
//! (Phase 14) uses these to compute backrun opportunities by comparing the cloned post-state
//! against the rest of `StateSpace` for the same `(token_in, token_out)` pair.
//!
//! Crucially, NONE of this touches the real `StateSpace.state` map — every pool we modify is a
//! `clone()` we own locally. The confirmed orderbook stays pristine.

use std::sync::Arc;

use alloy::primitives::{Address, U256};
use amms::amms::amm::{AmmId, AutomatedMarketMaker, AMM};
use amms::state_space::StateSpace;
use tokio::sync::RwLock;
use tracing::debug;

use crate::decoder::{Hop, SwapIntent};

#[derive(Debug, Clone)]
pub struct ProjectedHop {
    pub pool_id: AmmId,
    pub pool_after: AMM,
    pub amount_in: U256,
    pub amount_out: U256,
    pub token_in: Address,
    pub token_out: Address,
}

#[derive(Debug, thiserror::Error)]
pub enum SpeculateError {
    #[error("no candidate pool found for hop {hop_index}: {token_in} → {token_out}")]
    NoPoolForHop {
        hop_index: usize,
        token_in: Address,
        token_out: Address,
    },
    #[error("simulate_swap_mut failed at hop {hop_index} on pool {pool_id:?}: {source}")]
    SimulateFailed {
        hop_index: usize,
        pool_id: AmmId,
        #[source]
        source: amms::amms::error::AMMError,
    },
}

/// Speculate the chain of swaps. Returns the per-hop projection, or an error identifying which
/// hop failed (the caller logs + drops the intent — partial projections are not useful).
pub async fn speculate(
    state: &Arc<RwLock<StateSpace>>,
    intent: &SwapIntent,
) -> Result<Vec<ProjectedHop>, SpeculateError> {
    let guard = state.read().await;
    let mut projections = Vec::with_capacity(intent.path.len());
    let mut next_amount_in = intent.amount_in;

    for (i, hop) in intent.path.iter().enumerate() {
        let pool_id = match hop.pool_hint {
            Some(id) if guard.state.contains_key(&id) => id,
            _ => match pick_candidate_pool(&guard, hop) {
                Some(id) => id,
                None => {
                    return Err(SpeculateError::NoPoolForHop {
                        hop_index: i,
                        token_in: hop.token_in,
                        token_out: hop.token_out,
                    })
                }
            },
        };

        let mut cloned = guard
            .state
            .get(&pool_id)
            .expect("pool id resolved from current state must exist")
            .clone();

        let amount_in_for_hop = next_amount_in;
        let amount_out = cloned
            .simulate_swap_mut(hop.token_in, hop.token_out, amount_in_for_hop)
            .map_err(|e| SpeculateError::SimulateFailed {
                hop_index: i,
                pool_id,
                source: e,
            })?;

        debug!(
            target: "mev::speculator",
            hop = i,
            ?pool_id,
            token_in = ?hop.token_in,
            token_out = ?hop.token_out,
            amount_in = %amount_in_for_hop,
            amount_out = %amount_out,
            "projected hop",
        );

        projections.push(ProjectedHop {
            pool_id,
            pool_after: cloned,
            amount_in: amount_in_for_hop,
            amount_out,
            token_in: hop.token_in,
            token_out: hop.token_out,
        });
        next_amount_in = amount_out;
    }

    Ok(projections)
}

/// Pick the pool we believe the victim is most likely targeting for `(token_in, token_out)`.
///
/// v1 heuristic: take `by_token[token_in] ∩ by_token[token_out]` and pick the candidate that
/// returns the largest output for a probe swap of 1 unit of token_in. This implicitly favors
/// the deepest / cheapest pool, which is what unpinned routers (UniV2-style) do on-chain via
/// best-price routing. It's not exact (the victim may have a different fee preference, or be
/// routing through a specific pool via custom calldata) but it captures the modal case.
fn pick_candidate_pool(state: &StateSpace, hop: &Hop) -> Option<AmmId> {
    let bucket_in = state.by_token.get(&hop.token_in)?;
    let bucket_out = state.by_token.get(&hop.token_out)?;
    // Iterate the smaller bucket for intersection speed. by_token typically has tens of pools
    // per token in token-first mode, so neither bucket is large in absolute terms.
    let (smaller, larger) = if bucket_in.len() <= bucket_out.len() {
        (bucket_in, bucket_out)
    } else {
        (bucket_out, bucket_in)
    };
    let probe = U256::from(1_000_000u64); // 1 token unit at 6 decimals; deep pools shrug, shallow ones fall over.
    let mut best: Option<(AmmId, U256)> = None;
    for id in smaller {
        if !larger.contains(id) {
            continue;
        }
        let Some(amm) = state.state.get(id) else {
            continue;
        };
        let out = match amm.simulate_swap(hop.token_in, hop.token_out, probe) {
            Ok(o) => o,
            Err(_) => continue,
        };
        match best {
            Some((_, current_best)) if out <= current_best => {}
            _ => best = Some((*id, out)),
        }
    }
    best.map(|(id, _)| id)
}
