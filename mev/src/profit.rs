//! Compute net profit on an [`ArbCycle`] and emit a single structured log per opportunity.
//!
//! v1 gas accounting is intentionally crude: a fixed `gas_estimate` (configurable per
//! deployment) multiplied by the current base fee. Net profit is in `start_token` units; we do
//! NOT convert to a fiat / BNB equivalent. The user's executor contract (Phase F) will sweep
//! everything to USDT, so leaving profit in `start_token` units is honest about what's
//! actually recoverable.
//!
//! Logs that pass the configured `min_profit` threshold go through at `info`; everything else
//! drops to `debug`. Addresses are printed in full (per global instructions) — no truncation.

use alloy::primitives::U256;
use tracing::{debug, info};

use crate::decoder::SwapIntent;
use crate::pathfinder::ArbCycle;

#[derive(Debug, Clone, Copy)]
pub struct GasContext {
    /// Estimated gas for the cycle's bundle (every leg + executor overhead).
    pub gas_estimate: u64,
    /// Current base fee in wei. BSC's base fee is typically 1 gwei = 1e9.
    pub base_fee_wei: u128,
}

#[derive(Debug, Clone, Copy)]
pub struct ProfitConfig {
    /// Minimum gross profit (in `start_token` minimum units, raw integer) to log at info level.
    pub min_profit: i128,
}

pub fn report_opportunity(
    intent: &SwapIntent,
    cycle: &ArbCycle,
    gas: GasContext,
    cfg: ProfitConfig,
) {
    let gas_cost_wei = (gas.gas_estimate as u128).saturating_mul(gas.base_fee_wei);
    let pass = cycle.gross_profit_units >= cfg.min_profit;
    let cycle_path = format_cycle_path(cycle);
    let victim_path = format_victim_path(intent);
    let anchor_pool = cycle
        .legs
        .last()
        .map(|l| l.pool_id)
        .expect("ArbCycle always has at least the anchor leg");
    if pass {
        info!(
            target: "mev::profit",
            source_tx = ?intent.source.tx_hash,
            source_feed = intent.source.source,
            victim_router = ?intent.router,
            victim_path = %victim_path,
            victim_amount_in = %intent.amount_in,
            victim_min_out = %intent.min_amount_out,
            cycle_hops = cycle.legs.len(),
            cycle_path = %cycle_path,
            anchor_pool = ?anchor_pool,
            start_token = ?cycle.start_token,
            start_amount_in = %cycle.start_amount_in,
            final_out = %cycle.final_out,
            gross_profit = cycle.gross_profit_units,
            gas_estimate = gas.gas_estimate,
            base_fee_wei = %U256::from(gas.base_fee_wei),
            gas_cost_native_wei = %U256::from(gas_cost_wei),
            min_profit_threshold = cfg.min_profit,
            "arb cycle",
        );
    } else {
        debug!(
            target: "mev::profit",
            source_tx = ?intent.source.tx_hash,
            cycle_hops = cycle.legs.len(),
            cycle_path = %cycle_path,
            gross_profit = cycle.gross_profit_units,
            min_profit_threshold = cfg.min_profit,
            "cycle below threshold",
        );
    }
}

/// "0xfullA -> 0xfullB -> 0xfullC -> 0xfullA". Full addresses always; never truncated.
fn format_cycle_path(cycle: &ArbCycle) -> String {
    let mut s = String::new();
    if let Some(first) = cycle.legs.first() {
        s.push_str(&format!("{}", first.from));
    }
    for leg in &cycle.legs {
        s.push_str(&format!(" -> {}", leg.to));
    }
    s
}

fn format_victim_path(intent: &SwapIntent) -> String {
    let mut s = String::new();
    if let Some(first) = intent.path.first() {
        s.push_str(&format!("{}", first.token_in));
    }
    for hop in &intent.path {
        s.push_str(&format!(" -> {}", hop.token_out));
    }
    s
}

