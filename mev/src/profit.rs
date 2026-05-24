//! Compute net profit and emit a single structured log per opportunity.
//!
//! v1 gas accounting is intentionally crude: a fixed `gas_estimate` (configurable per
//! deployment) multiplied by the current base fee. Net profit is in `token_out` units; we do
//! NOT convert to a fiat / BNB equivalent. The user's executor contract (Phase F) will sweep
//! everything to USDT, so leaving profit in `token_out` units is honest about what's actually
//! recoverable.
//!
//! Logs that pass the configured `min_profit` threshold go through at `info`; everything else
//! drops to `debug`.

use alloy::primitives::U256;
use tracing::{debug, info};

use crate::decoder::SwapIntent;
use crate::pathfinder::BackrunOpportunity;

#[derive(Debug, Clone, Copy)]
pub struct GasContext {
    /// Estimated gas for the backrun bundle (buy leg + sell leg + executor overhead).
    pub gas_estimate: u64,
    /// Current base fee in wei. BSC's base fee is typically 1 gwei = 1e9.
    pub base_fee_wei: u128,
}

#[derive(Debug, Clone, Copy)]
pub struct ProfitConfig {
    /// Minimum net profit (in `token_out` units, raw integer) to log at info level.
    pub min_profit: i128,
}

pub fn report_opportunity(
    intent: &SwapIntent,
    opp: &BackrunOpportunity,
    gas: GasContext,
    cfg: ProfitConfig,
) {
    // Gas cost in native (BNB) wei. We do NOT subtract this from gross_profit directly
    // because gross_profit is denominated in `token_out`, not BNB — the unit mismatch is real
    // and pretending otherwise misleads the reader. The log shows both side-by-side so a
    // human can sanity-check whether the trade still clears after rough fx.
    let gas_cost_wei = (gas.gas_estimate as u128).saturating_mul(gas.base_fee_wei);

    let pass = opp.gross_profit >= cfg.min_profit;
    let victim_path_str = format_path(intent);
    if pass {
        info!(
            target: "mev::profit",
            source_tx = ?intent.source.tx_hash,
            source_feed = intent.source.source,
            victim_router = ?intent.router,
            victim_path = %victim_path_str,
            victim_amount_in = %intent.amount_in,
            victim_min_out = %intent.min_amount_out,
            victim_pool = ?opp.victim_pool,
            backrun_pool = ?opp.backrun_pool,
            backrun_amount_in = %opp.backrun_amount_in,
            backrun_buy_leg_out = %opp.buy_leg_out,
            backrun_sell_leg_out = %opp.sell_leg_out,
            gross_profit_token_out = opp.gross_profit,
            gas_estimate = gas.gas_estimate,
            base_fee_wei = %U256::from(gas.base_fee_wei),
            gas_cost_native_wei = %U256::from(gas_cost_wei),
            min_profit_threshold = cfg.min_profit,
            "backrun opportunity",
        );
    } else {
        debug!(
            target: "mev::profit",
            source_tx = ?intent.source.tx_hash,
            victim_pool = ?opp.victim_pool,
            backrun_pool = ?opp.backrun_pool,
            gross_profit_token_out = opp.gross_profit,
            min_profit_threshold = cfg.min_profit,
            "opportunity below threshold",
        );
    }
}

fn format_path(intent: &SwapIntent) -> String {
    let mut s = String::new();
    if let Some(first) = intent.path.first() {
        s.push_str(&format!("{}", first.token_in));
    }
    for hop in &intent.path {
        s.push_str(&format!(" -> {}", hop.token_out));
    }
    s
}
