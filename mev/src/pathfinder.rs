//! 2-hop backrun pathfinder.
//!
//! Idea: a victim swap on pool P moves its price for `(token_in, token_out)`. After the swap
//! lands, P is mispriced relative to every other pool Q that holds the same pair. A backrun
//! arbitrage **buys from Q at the old price and sells into P_after at the new (distorted)
//! price**, profiting from the gap. Both legs execute atomically in our (future) executor
//! contract — for v1 we just measure the gap and log it.
//!
//! For each [`ProjectedHop`] returned by the speculator we try every Q in
//! `by_token[token_in] ∩ by_token[token_out] \ {P}` and report the best opportunity.
//!
//! Optimal `x` (the amount of `token_out` we feed into the buy leg) is found by a coarse
//! ternary search bounded by `MAX_PROBE` (so we don't probe with absurd amounts on shallow
//! pools, which the v3 math sometimes degrades on). 10 iterations is enough for v1 — the
//! function is unimodal under standard AMM math and we don't need ULP-accuracy for a log.

use std::sync::Arc;

use alloy::primitives::{Address, U256};
use amms::amms::amm::{AmmId, AutomatedMarketMaker, AMM};
use amms::state_space::StateSpace;
use tokio::sync::RwLock;
use tracing::debug;

use crate::speculator::ProjectedHop;

/// Maximum `x` we'll probe in `token_out` units. 1e21 == 1000 tokens at 18 decimals; the
/// backruns that actually matter live well below this. Tunable per-deployment if BSC throws
/// up surprises.
const MAX_PROBE: u128 = 1_000_000_000_000_000_000_000; // 1e21

/// Number of ternary-search iterations. Each iteration narrows the interval by 1/3. 10
/// iterations narrows by ~(2/3)^10 ≈ 1.7%, which is way below the gas-cost noise floor.
const TERNARY_ITERS: usize = 10;

#[derive(Debug, Clone)]
pub struct BackrunOpportunity {
    /// The pool the victim swap touched (where we sell into the new distorted price).
    pub victim_pool: AmmId,
    /// The counterparty pool we buy from at the old price.
    pub backrun_pool: AmmId,
    pub token_in: Address,
    pub token_out: Address,
    /// `x` in `token_out` units that we feed into the buy leg.
    pub backrun_amount_in: U256,
    /// `token_in` we get from the buy leg on `backrun_pool`.
    pub buy_leg_out: U256,
    /// `token_out` we get from the sell leg on the post-victim `victim_pool`.
    pub sell_leg_out: U256,
    /// Profit in `token_out` units (sell_leg_out − backrun_amount_in). Negative means we'd
    /// lose money; pathfinder still returns it and lets the caller filter by threshold.
    pub gross_profit: i128,
}

pub async fn find_backruns(
    state: &Arc<RwLock<StateSpace>>,
    projected: &[ProjectedHop],
) -> Vec<BackrunOpportunity> {
    let guard = state.read().await;
    let mut out = Vec::new();
    for hop in projected {
        if let Some(opp) = best_backrun_for_hop(&guard, hop) {
            out.push(opp);
        }
    }
    out
}

fn best_backrun_for_hop(state: &StateSpace, hop: &ProjectedHop) -> Option<BackrunOpportunity> {
    let bucket_in = state.by_token.get(&hop.token_in)?;
    let bucket_out = state.by_token.get(&hop.token_out)?;
    let (smaller, larger) = if bucket_in.len() <= bucket_out.len() {
        (bucket_in, bucket_out)
    } else {
        (bucket_out, bucket_in)
    };

    let mut best: Option<BackrunOpportunity> = None;
    for &q_id in smaller {
        if q_id == hop.pool_id || !larger.contains(&q_id) {
            continue;
        }
        let Some(q_amm) = state.state.get(&q_id) else {
            continue;
        };
        let Some(candidate) =
            optimize_amount_in(q_amm, &hop.pool_after, hop.token_in, hop.token_out, hop.pool_id, q_id)
        else {
            continue;
        };
        let take = match &best {
            None => true,
            Some(b) => candidate.gross_profit > b.gross_profit,
        };
        if take {
            best = Some(candidate);
        }
    }
    best
}

/// Ternary-search the amount-in `x` (in `token_out` units fed into `backrun_pool`'s buy leg)
/// that maximizes profit. Profit function: for `x` token_out → some `y` token_in from Q,
/// then `y` token_in → `z` token_out from P_after. Profit = z − x.
fn optimize_amount_in(
    backrun_pool: &AMM,
    victim_after: &AMM,
    token_in: Address,
    token_out: Address,
    victim_id: AmmId,
    backrun_id: AmmId,
) -> Option<BackrunOpportunity> {
    let mut lo: u128 = 1;
    let mut hi: u128 = MAX_PROBE;

    // Quick sanity check: if profit at the smallest x is already non-positive, there's no
    // arbitrage even at infinitesimal size and ternary search would just hand us junk.
    let initial = compute_profit(backrun_pool, victim_after, token_in, token_out, lo)?;
    if initial.gross_profit <= 0 {
        // Single probe at MAX_PROBE in case the optimum is wildly displaced (deep pool + tiny
        // victim swap). If that's also non-positive we give up.
        let big = compute_profit(backrun_pool, victim_after, token_in, token_out, hi)?;
        if big.gross_profit <= 0 {
            return None;
        }
    }

    for _ in 0..TERNARY_ITERS {
        let span = hi - lo;
        if span < 3 {
            break;
        }
        let m1 = lo + span / 3;
        let m2 = hi - span / 3;
        let p1 = compute_profit(backrun_pool, victim_after, token_in, token_out, m1)?;
        let p2 = compute_profit(backrun_pool, victim_after, token_in, token_out, m2)?;
        if p1.gross_profit < p2.gross_profit {
            lo = m1;
        } else {
            hi = m2;
        }
    }
    let center = (lo + hi) / 2;
    let probe = compute_profit(backrun_pool, victim_after, token_in, token_out, center)?;
    debug!(
        target: "mev::pathfinder",
        ?victim_id,
        ?backrun_id,
        amount_in = %probe.backrun_amount_in,
        gross_profit = probe.gross_profit,
        "ternary search result",
    );
    Some(BackrunOpportunity {
        victim_pool: victim_id,
        backrun_pool: backrun_id,
        token_in,
        token_out,
        backrun_amount_in: probe.backrun_amount_in,
        buy_leg_out: probe.buy_leg_out,
        sell_leg_out: probe.sell_leg_out,
        gross_profit: probe.gross_profit,
    })
}

/// One-shot evaluation: buy on `backrun_pool` (paying `x` token_out for some `y` token_in),
/// then sell on `victim_after` (paying `y` token_in for some `z` token_out). Profit = z − x.
/// Returns `None` if either leg's simulate_swap fails.
fn compute_profit(
    backrun_pool: &AMM,
    victim_after: &AMM,
    token_in: Address,
    token_out: Address,
    x: u128,
) -> Option<BackrunOpportunity> {
    let x_u256 = U256::from(x);
    let buy_out = backrun_pool
        .simulate_swap(token_out, token_in, x_u256)
        .ok()?;
    if buy_out == U256::ZERO {
        return None;
    }
    let sell_out = victim_after.simulate_swap(token_in, token_out, buy_out).ok()?;
    let sell_u128: u128 = sell_out.try_into().ok()?;
    let profit = (sell_u128 as i128) - (x as i128);
    Some(BackrunOpportunity {
        victim_pool: AmmId::Address(Address::ZERO), // overwritten by caller
        backrun_pool: AmmId::Address(Address::ZERO), // overwritten by caller
        token_in,
        token_out,
        backrun_amount_in: x_u256,
        buy_leg_out: buy_out,
        sell_leg_out: sell_out,
        gross_profit: profit,
    })
}
