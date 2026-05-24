//! PancakeSwap V3 SmartRouter decoder. Only `exactInputSingle` is wired in v1.
//!
//! PCS's SmartRouter wraps both V2 and V3 calls and is multi-call: many real PCS-V3 swaps come
//! through as `multicall([exactInputSingle(...), refundETH(), ...])`. Phase 12 deliberately
//! does NOT walk `multicall` arguments — that's a Phase 12+ extension once we measure how much
//! traffic we miss. The single-call `exactInputSingle` form alone still picks up the swaps that
//! traders construct manually via PCS frontend "advanced mode" and a meaningful slice of bots.
//!
//! The struct argument is keyed to PCS's variant (no `deadline` field; deadline is at the
//! multicall layer when used) — selector 0x04e45aaf.

use alloy::primitives::Address;
use alloy::sol;
use alloy::sol_types::SolCall;

use crate::decoder::{DecoderRegistry, Hop, SwapIntent};
use crate::mempool::MempoolTx;

sol! {
    struct PcsV3ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    interface IPancakeV3SwapRouter {
        function exactInputSingle(PcsV3ExactInputSingleParams params)
            external payable returns (uint256 amountOut);
    }
}

pub const SEL_EXACT_INPUT_SINGLE: [u8; 4] = IPancakeV3SwapRouter::exactInputSingleCall::SELECTOR;

pub fn decode_exact_input_single(tx: &MempoolTx) -> Option<SwapIntent> {
    let call = IPancakeV3SwapRouter::exactInputSingleCall::abi_decode(&tx.input).ok()?;
    let p = call.params;
    Some(SwapIntent {
        source: tx.clone(),
        router: tx.to?,
        path: vec![Hop {
            token_in: p.tokenIn,
            token_out: p.tokenOut,
            pool_hint: None,
        }],
        amount_in: p.amountIn,
        min_amount_out: p.amountOutMinimum,
        // PCS-V3 SmartRouter form doesn't carry a deadline at this layer; we set 0 so the
        // speculator treats it as "no deadline check at this layer". (The multicall wrapper
        // would carry the real deadline — Phase 12+ will surface it when we walk multicall.)
        deadline: 0,
    })
}

pub fn register_all(registry: &mut DecoderRegistry, router: Address) {
    registry.register(
        router,
        "pcs_v3",
        SEL_EXACT_INPUT_SINGLE,
        decode_exact_input_single,
    );
}
