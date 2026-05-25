//! Token-centric multi-hop arbitrage search.
//!
//! Idea: a victim swap on pool P moves the (X, Y) price on P. After the victim lands, P_after
//! is the only pool that holds the **distorted** price; every other pool that touches X or Y
//! still reflects the pre-victim market. The profitable arbitrage closes a cycle that uses
//! P_after in the Y→X direction (selling Y back at the bumped price) and acquires the Y from
//! somewhere else — either a direct X→Y on another pool (2-hop = old Phase 14 backrun) or a
//! multi-step path X → T1 → ... → Y through bridge tokens (3-hop and beyond).
//!
//! The anchor leg is fixed = `P_after Y→X`. The search reduces to "find a path from X to Y of
//! length L = max_hops − 1, passing only through bridge tokens internally". Each candidate
//! cycle is sized via ternary search and reported only if `gross_profit > 0`.
//!
//! Everything operates on **cloned** AMM state — the live `StateSpace` is never mutated.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use amms::amms::amm::{AmmId, AutomatedMarketMaker, AMM};
use amms::state_space::StateSpace;
use tokio::sync::RwLock;
use tracing::debug;

use crate::speculator::ProjectedHop;

/// Maximum probe amount for ternary search, in `start_token` minimum units. 1e21 == 1000 tokens
/// at 18 decimals — large enough that the optimum lives well inside the interval for almost any
/// real-world cycle, small enough that AMM math doesn't degrade on shallow pools. Hard-coded
/// because BSC's quote tokens (USDT/USDC/WBNB/USD1) are all 18 decimals; if you add a 6-decimal
/// stablecoin as start_token, scale this accordingly.
const MAX_PROBE: u128 = 1_000_000_000_000_000_000_000;

/// Ternary-search iterations. Each iteration narrows by ~1/3; 10 rounds ≈ 1.7% of the initial
/// span, far below the gas-cost noise floor.
const TERNARY_ITERS: usize = 10;

#[derive(Debug, Clone)]
pub struct PathfinderConfig {
    /// Maximum cycle length including the anchor leg. `2` reproduces the old Phase 14 backrun
    /// behavior; `3` admits triangular arbitrage; `4+` is allowed but search cost grows
    /// roughly as `|bridge_tokens|^(max_hops-2) × pool_candidates_per_edge^(max_hops-1)`.
    pub max_hops: usize,
    /// Tokens that may sit at **internal** positions of the cycle (between X and Y in the
    /// non-anchor portion). Endpoints X and Y don't need to be in this set.
    pub bridge_tokens: HashSet<Address>,
    /// For each (from, to) edge in the cycle, how many candidate pools to consider from
    /// `by_token[from] ∩ by_token[to]`. 1 = deepest pool only; larger = more variants × clone
    /// cost.
    pub pool_candidates_per_edge: usize,
}

#[derive(Debug, Clone)]
pub struct CycleLeg {
    pub pool_id: AmmId,
    pub from: Address,
    pub to: Address,
}

#[derive(Debug, Clone)]
pub struct ArbCycle {
    /// Legs in execution order. The last leg is always the anchor (P_after Y→X).
    pub legs: Vec<CycleLeg>,
    /// Token the cycle starts and ends at. v1 always equals the victim's X (token_in of the
    /// anchor leg = `legs.last().to`).
    pub start_token: Address,
    /// Best `amount_in` found by ternary search, in `start_token` minimum units.
    pub start_amount_in: U256,
    /// Amount returned to `start_token` after running the full cycle.
    pub final_out: U256,
    /// `final_out − start_amount_in` cast to i128. Negative cycles are filtered out by
    /// `find_arb_cycles`, but the field is signed so callers can still see how close to zero a
    /// rejected cycle was during debugging.
    pub gross_profit_units: i128,
}

pub async fn find_arb_cycles(
    state: &Arc<RwLock<StateSpace>>,
    projected: &[ProjectedHop],
    cfg: &PathfinderConfig,
) -> Vec<ArbCycle> {
    if cfg.max_hops < 2 {
        return Vec::new();
    }
    let guard = state.read().await;
    let mut out = Vec::new();
    for hop in projected {
        find_for_hop(&guard, hop, cfg, &mut out);
    }
    out
}

fn find_for_hop(state: &StateSpace, hop: &ProjectedHop, cfg: &PathfinderConfig, out: &mut Vec<ArbCycle>) {
    let x = hop.token_in;
    let y = hop.token_out;
    let p = hop.pool_id;

    // Length of the non-anchor portion of the cycle (path from X back to Y).
    let path_len = cfg.max_hops - 1;

    let mut paths: Vec<Vec<CycleLeg>> = Vec::new();
    let mut path_acc: Vec<CycleLeg> = Vec::with_capacity(path_len);
    let mut visited_tokens: HashSet<Address> = HashSet::new();
    visited_tokens.insert(x);
    enumerate_paths(
        state,
        x,
        y,
        path_len,
        p,
        cfg,
        &mut path_acc,
        &mut visited_tokens,
        &mut paths,
    );

    for path in paths {
        // Append anchor leg: P_after Y→X, using hop.pool_after as the underlying pool state.
        let mut legs = path;
        legs.push(CycleLeg { pool_id: p, from: y, to: x });

        // The anchor pool's pre-cloned post-victim state lives in `hop.pool_after`. For the
        // non-anchor legs we use the live `state` snapshot — they'll be cloned again inside
        // `evaluate_cycle`. We don't pass the pool_after into the AMM lookup map either;
        // evaluate_cycle is given an override map { p -> &hop.pool_after } for that.
        let cycle = match optimize_cycle(state, &legs, hop, x) {
            Some(c) if c.gross_profit_units > 0 => c,
            other => {
                if let Some(c) = other {
                    debug!(
                        target: "mev::pathfinder",
                        victim_pool = ?p,
                        legs = ?c.legs.iter().map(|l| (l.pool_id, l.from, l.to)).collect::<Vec<_>>(),
                        gross = c.gross_profit_units,
                        "cycle non-positive; dropped",
                    );
                }
                continue;
            }
        };
        debug!(
            target: "mev::pathfinder",
            victim_pool = ?p,
            hops = cycle.legs.len(),
            amount_in = %cycle.start_amount_in,
            final_out = %cycle.final_out,
            gross = cycle.gross_profit_units,
            "cycle accepted",
        );
        out.push(cycle);
    }
}

#[allow(clippy::too_many_arguments)]
fn enumerate_paths(
    state: &StateSpace,
    cur: Address,
    target: Address,
    remaining_hops: usize,
    excluded_pool: AmmId,
    cfg: &PathfinderConfig,
    path_acc: &mut Vec<CycleLeg>,
    visited_tokens: &mut HashSet<Address>,
    out: &mut Vec<Vec<CycleLeg>>,
) {
    if remaining_hops == 0 {
        return; // path completion is handled when we *take* the final step below
    }
    let Some(bucket_cur) = state.by_token.get(&cur) else { return };

    // Group candidate pools by their "other" token, so we can rank candidates per neighbor.
    let mut neighbor_pools: HashMap<Address, Vec<AmmId>> = HashMap::new();
    for &pool_id in bucket_cur {
        if pool_id == excluded_pool {
            continue;
        }
        let Some(amm) = state.state.get(&pool_id) else { continue };
        let tokens = amm.tokens();
        let other = match tokens.as_slice() {
            [a, b] if *a == cur => *b,
            [a, b] if *b == cur => *a,
            _ => continue, // V4 with native (address(0)) or non-pair AMMs — skip for v1.
        };
        neighbor_pools.entry(other).or_default().push(pool_id);
    }

    let is_final_step = remaining_hops == 1;
    for (next, mut pools) in neighbor_pools {
        if is_final_step {
            if next != target {
                continue;
            }
        } else {
            if next == target {
                continue; // hit target too early; we need exact length.
            }
            if !cfg.bridge_tokens.contains(&next) {
                continue;
            }
            if visited_tokens.contains(&next) {
                continue;
            }
        }

        // Top-K candidate pools for this edge, ranked by rough_liquidity descending.
        pools.sort_by_key(|id| {
            std::cmp::Reverse(state.state.get(id).map(rough_liquidity).unwrap_or(0))
        });
        pools.truncate(cfg.pool_candidates_per_edge.max(1));

        for pool_id in pools {
            path_acc.push(CycleLeg { pool_id, from: cur, to: next });
            if !is_final_step {
                visited_tokens.insert(next);
                enumerate_paths(
                    state,
                    next,
                    target,
                    remaining_hops - 1,
                    excluded_pool,
                    cfg,
                    path_acc,
                    visited_tokens,
                    out,
                );
                visited_tokens.remove(&next);
            } else {
                out.push(path_acc.clone());
            }
            path_acc.pop();
        }
    }
}

/// Ternary-search the optimal `start_amount_in` for a fully-specified cycle. Returns the cycle
/// annotated with the best amount and resulting profit (which may be ≤ 0; caller filters).
fn optimize_cycle(
    state: &StateSpace,
    legs: &[CycleLeg],
    victim_hop: &ProjectedHop,
    start_token: Address,
) -> Option<ArbCycle> {
    let mut lo: u128 = 1;
    let mut hi: u128 = MAX_PROBE;

    // Sanity probe at both ends. If both are non-positive there's no arb; if at least one is
    // positive, ternary will hill-climb.
    let initial = run_cycle(state, legs, victim_hop, lo)?;
    let big = run_cycle(state, legs, victim_hop, hi)?;
    if initial.1 <= 0 && big.1 <= 0 {
        return Some(ArbCycle {
            legs: legs.to_vec(),
            start_token,
            start_amount_in: U256::from(lo),
            final_out: initial.0,
            gross_profit_units: initial.1,
        });
    }

    for _ in 0..TERNARY_ITERS {
        let span = hi - lo;
        if span < 3 {
            break;
        }
        let m1 = lo + span / 3;
        let m2 = hi - span / 3;
        let p1 = run_cycle(state, legs, victim_hop, m1)?;
        let p2 = run_cycle(state, legs, victim_hop, m2)?;
        if p1.1 < p2.1 {
            lo = m1;
        } else {
            hi = m2;
        }
    }
    let center = (lo + hi) / 2;
    let (final_out, gross) = run_cycle(state, legs, victim_hop, center)?;
    Some(ArbCycle {
        legs: legs.to_vec(),
        start_token,
        start_amount_in: U256::from(center),
        final_out,
        gross_profit_units: gross,
    })
}

/// Execute one cycle on cloned pools. The anchor leg (the last leg) uses
/// `victim_hop.pool_after` so the cycle reflects the post-victim state; non-anchor legs use the
/// pre-victim `state` snapshot.
fn run_cycle(
    state: &StateSpace,
    legs: &[CycleLeg],
    victim_hop: &ProjectedHop,
    start_amount_in: u128,
) -> Option<(U256, i128)> {
    let mut amount = U256::from(start_amount_in);
    let anchor_idx = legs.len() - 1;
    for (i, leg) in legs.iter().enumerate() {
        let mut cloned = if i == anchor_idx {
            victim_hop.pool_after.clone()
        } else {
            state.state.get(&leg.pool_id)?.clone()
        };
        let out = cloned.simulate_swap_mut(leg.from, leg.to, amount).ok()?;
        if out == U256::ZERO {
            return None;
        }
        amount = out;
    }
    let final_out = amount;
    let final_u128: u128 = final_out.try_into().ok()?;
    let gross = (final_u128 as i128) - (start_amount_in as i128);
    Some((final_out, gross))
}

/// Rough liquidity signal, used only to rank candidate pools per edge. Exact value is not
/// meaningful across DEX types — we just need a stable per-DEX score that scales with depth.
fn rough_liquidity(amm: &AMM) -> u128 {
    match amm {
        AMM::UniswapV2Pool(p) => p.reserve_0.min(p.reserve_1),
        AMM::UniswapV3Pool(p) => p.liquidity,
        AMM::UniswapV4Pool(p) => p.state.liquidity,
        AMM::PancakeV4CLPool(p) => p.state.liquidity,
        AMM::ERC4626Vault(_) => 0,
        AMM::BalancerPool(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use amms::amms::amm::AMM;
    use amms::amms::uniswap_v2::UniswapV2Pool;
    use amms::amms::Token;

    const USDT: Address = address!("0x55d398326f99059fF775485246999027B3197955");
    const WBNB: Address = address!("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c");
    const NXPC: Address = address!("0x1111111111111111111111111111111111111111");
    const USDC: Address = address!("0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d");

    fn v2_pool(addr: Address, a: Address, b: Address, ra: u128, rb: u128) -> AMM {
        AMM::UniswapV2Pool(UniswapV2Pool {
            address: addr,
            token_a: Token::new_with_decimals(a, 18),
            token_b: Token::new_with_decimals(b, 18),
            reserve_0: ra,
            reserve_1: rb,
            fee: 250,
        })
    }

    fn build_state(pools: Vec<AMM>) -> StateSpace {
        let mut s = StateSpace::default();
        let by_token: HashMap<Address, Address> = HashMap::new();
        let _ = by_token;
        let owning_token = USDT; // arbitrary; track_pools just needs an anchor token for indexing
        s.track_pools(pools, owning_token);
        s
    }

    #[test]
    fn rough_liquidity_v2_is_min_reserve() {
        let amm = v2_pool(
            address!("0x2222222222222222222222222222222222222222"),
            USDT,
            WBNB,
            1_000_000_000u128,
            500u128,
        );
        assert_eq!(rough_liquidity(&amm), 500);
    }

    #[test]
    fn enumerate_paths_max_hops_2_finds_direct_partner() {
        // P (excluded) + Q1 (USDT/NXPC) — only Q1 should appear as a single-hop direct path.
        let p_addr = address!("0xAAaaAAaaaAAaaAaaAAAAAaaaAaAaAaAaaAAAaaaA");
        let q1_addr = address!("0xBbBbBbBbBBBBBbBBbBbbBBbBbbBbbBbBbBbBBbBB");
        let p = v2_pool(p_addr, USDT, NXPC, 10_000_000u128, 1_000_000u128);
        let q1 = v2_pool(q1_addr, USDT, NXPC, 20_000_000u128, 2_000_000u128);
        let state = build_state(vec![p.clone(), q1.clone()]);

        let cfg = PathfinderConfig {
            max_hops: 2,
            bridge_tokens: [USDT].into_iter().collect(),
            pool_candidates_per_edge: 1,
        };
        let mut paths = Vec::new();
        let mut acc = Vec::new();
        let mut visited = HashSet::from([USDT]);
        enumerate_paths(
            &state,
            USDT,
            NXPC,
            cfg.max_hops - 1,
            AmmId::Address(p_addr),
            &cfg,
            &mut acc,
            &mut visited,
            &mut paths,
        );
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].len(), 1);
        assert_eq!(paths[0][0].pool_id, AmmId::Address(q1_addr));
        assert_eq!(paths[0][0].from, USDT);
        assert_eq!(paths[0][0].to, NXPC);
    }

    #[test]
    fn enumerate_paths_max_hops_3_uses_bridge() {
        // P (USDT/NXPC, excluded) + Q1 (USDT/WBNB) + Q2 (WBNB/NXPC). With max_hops=3 the only
        // length-2 path from USDT to NXPC is USDT -> WBNB -> NXPC.
        let p_addr = address!("0xAAaaAAaaaAAaaAaaAAAAAaaaAaAaAaAaaAAAaaaA");
        let q1_addr = address!("0xBbBbBbBbBBBBBbBBbBbbBBbBbbBbbBbBbBbBBbBB");
        let q2_addr = address!("0xCccCCCCcCcCcCccCcCccCcCccCcCcCccCcCcCcCC");
        let p = v2_pool(p_addr, USDT, NXPC, 10_000_000u128, 1_000_000u128);
        let q1 = v2_pool(q1_addr, USDT, WBNB, 50_000_000u128, 50_000u128);
        let q2 = v2_pool(q2_addr, WBNB, NXPC, 40_000u128, 3_000_000u128);
        let state = build_state(vec![p, q1, q2]);

        let cfg = PathfinderConfig {
            max_hops: 3,
            bridge_tokens: [USDT, WBNB, USDC].into_iter().collect(),
            pool_candidates_per_edge: 1,
        };
        let mut paths = Vec::new();
        let mut acc = Vec::new();
        let mut visited = HashSet::from([USDT]);
        enumerate_paths(
            &state,
            USDT,
            NXPC,
            cfg.max_hops - 1,
            AmmId::Address(p_addr),
            &cfg,
            &mut acc,
            &mut visited,
            &mut paths,
        );
        assert_eq!(paths.len(), 1, "expected exactly one bridged path");
        assert_eq!(paths[0].len(), 2);
        assert_eq!(paths[0][0].pool_id, AmmId::Address(q1_addr));
        assert_eq!(paths[0][0].from, USDT);
        assert_eq!(paths[0][0].to, WBNB);
        assert_eq!(paths[0][1].pool_id, AmmId::Address(q2_addr));
        assert_eq!(paths[0][1].from, WBNB);
        assert_eq!(paths[0][1].to, NXPC);
    }

    #[test]
    fn enumerate_paths_skips_non_bridge_middle() {
        // Same as above but bridge_tokens = [USDT] excludes WBNB → no path should be returned.
        let p_addr = address!("0xAAaaAAaaaAAaaAaaAAAAAaaaAaAaAaAaaAAAaaaA");
        let q1_addr = address!("0xBbBbBbBbBBBBBbBBbBbbBBbBbbBbbBbBbBbBBbBB");
        let q2_addr = address!("0xCccCCCCcCcCcCccCcCccCcCccCcCcCccCcCcCcCC");
        let p = v2_pool(p_addr, USDT, NXPC, 10_000_000u128, 1_000_000u128);
        let q1 = v2_pool(q1_addr, USDT, WBNB, 50_000_000u128, 50_000u128);
        let q2 = v2_pool(q2_addr, WBNB, NXPC, 40_000u128, 3_000_000u128);
        let state = build_state(vec![p, q1, q2]);

        let cfg = PathfinderConfig {
            max_hops: 3,
            bridge_tokens: [USDT].into_iter().collect(),
            pool_candidates_per_edge: 1,
        };
        let mut paths = Vec::new();
        let mut acc = Vec::new();
        let mut visited = HashSet::from([USDT]);
        enumerate_paths(
            &state,
            USDT,
            NXPC,
            cfg.max_hops - 1,
            AmmId::Address(p_addr),
            &cfg,
            &mut acc,
            &mut visited,
            &mut paths,
        );
        assert!(paths.is_empty(), "non-bridge intermediate must be skipped");
    }
}
