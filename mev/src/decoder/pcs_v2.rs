//! PancakeSwap V2 Router decoders. Same ABI as Uniswap V2 Router02.
//!
//! Each handler decodes one (selector, signature) pair. The four selectors covered here
//! account for the overwhelming majority of BSC swap volume that goes through a V2 router:
//!
//!  - `swapExactTokensForTokens`                                  (0x38ed1739)
//!  - `swapExactETHForTokens` (payable, WBNB injected as path[0]) (0x7ff36ab5)
//!  - `swapExactTokensForETH`  (WBNB at path[N-1])                (0x18cbafe5)
//!  - `swapExactTokensForTokensSupportingFeeOnTransferTokens`     (0x5c11d795)
//!
//! `swapTokensForExactTokens` (exact-output) is deliberately skipped: it specifies the desired
//! output amount, not amount_in, which makes speculation noisier. We accept the tail loss.

use alloy::primitives::{Address, U256};
use alloy::sol;
use alloy::sol_types::SolCall;

use crate::decoder::{Hop, SwapIntent};
use crate::mempool::MempoolTx;

sol! {
    interface IPancakeV2Router {
        function swapExactTokensForTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external returns (uint256[] amounts);

        function swapExactETHForTokens(
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external payable returns (uint256[] amounts);

        function swapExactTokensForETH(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external returns (uint256[] amounts);

        function swapExactTokensForTokensSupportingFeeOnTransferTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] path,
            address to,
            uint256 deadline
        ) external;
    }
}

fn path_to_hops(path: &[Address]) -> Option<Vec<Hop>> {
    if path.len() < 2 {
        return None;
    }
    Some(
        path.windows(2)
            .map(|w| Hop {
                token_in: w[0],
                token_out: w[1],
                pool_hint: None,
            })
            .collect(),
    )
}

pub fn decode_swap_exact_tokens_for_tokens(tx: &MempoolTx) -> Option<SwapIntent> {
    let call = IPancakeV2Router::swapExactTokensForTokensCall::abi_decode(&tx.input).ok()?;
    Some(SwapIntent {
        source: tx.clone(),
        router: tx.to?,
        path: path_to_hops(&call.path)?,
        amount_in: call.amountIn,
        min_amount_out: call.amountOutMin,
        deadline: u64::try_from(call.deadline).unwrap_or(u64::MAX),
    })
}

pub fn decode_swap_exact_eth_for_tokens(tx: &MempoolTx) -> Option<SwapIntent> {
    let call = IPancakeV2Router::swapExactETHForTokensCall::abi_decode(&tx.input).ok()?;
    // amount_in for the ETH-in variant comes from `tx.value`. The router wraps it into WBNB
    // (= path[0]) before swapping. We treat WBNB as the input token throughout — `path[0]`
    // is already WBNB in calldata, so the speculator sees a normal token→token hop list.
    Some(SwapIntent {
        source: tx.clone(),
        router: tx.to?,
        path: path_to_hops(&call.path)?,
        amount_in: tx.value,
        min_amount_out: call.amountOutMin,
        deadline: u64::try_from(call.deadline).unwrap_or(u64::MAX),
    })
}

pub fn decode_swap_exact_tokens_for_eth(tx: &MempoolTx) -> Option<SwapIntent> {
    let call = IPancakeV2Router::swapExactTokensForETHCall::abi_decode(&tx.input).ok()?;
    Some(SwapIntent {
        source: tx.clone(),
        router: tx.to?,
        path: path_to_hops(&call.path)?,
        amount_in: call.amountIn,
        min_amount_out: call.amountOutMin,
        deadline: u64::try_from(call.deadline).unwrap_or(u64::MAX),
    })
}

pub fn decode_swap_exact_tokens_for_tokens_supporting_fee(tx: &MempoolTx) -> Option<SwapIntent> {
    let call =
        IPancakeV2Router::swapExactTokensForTokensSupportingFeeOnTransferTokensCall::abi_decode(
            &tx.input,
        )
        .ok()?;
    Some(SwapIntent {
        source: tx.clone(),
        router: tx.to?,
        path: path_to_hops(&call.path)?,
        amount_in: call.amountIn,
        min_amount_out: call.amountOutMin,
        deadline: u64::try_from(call.deadline).unwrap_or(u64::MAX),
    })
}

/// Selector constants — kept here next to the decoders for easy review against BSCScan.
pub const SEL_SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] =
    IPancakeV2Router::swapExactTokensForTokensCall::SELECTOR;
pub const SEL_SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] =
    IPancakeV2Router::swapExactETHForTokensCall::SELECTOR;
pub const SEL_SWAP_EXACT_TOKENS_FOR_ETH: [u8; 4] =
    IPancakeV2Router::swapExactTokensForETHCall::SELECTOR;
pub const SEL_SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE: [u8; 4] =
    IPancakeV2Router::swapExactTokensForTokensSupportingFeeOnTransferTokensCall::SELECTOR;

/// Convenience: the canonical PancakeSwap V2 Router address on BSC.
/// (Address full per global convention; never abbreviate in logs.)
pub const PCS_V2_ROUTER_BSC: Address = Address::new([
    0x10, 0xED, 0x43, 0xC7, 0x18, 0x71, 0x4e, 0xb6, 0x3d, 0x5a, 0xA5, 0x7B, 0x78, 0xB5, 0x47, 0x04,
    0xE2, 0x56, 0x02, 0x4E,
]);

pub fn register_all(registry: &mut crate::decoder::DecoderRegistry, router: Address) {
    registry.register(
        router,
        "pcs_v2",
        SEL_SWAP_EXACT_TOKENS_FOR_TOKENS,
        decode_swap_exact_tokens_for_tokens,
    );
    registry.register(
        router,
        "pcs_v2",
        SEL_SWAP_EXACT_ETH_FOR_TOKENS,
        decode_swap_exact_eth_for_tokens,
    );
    registry.register(
        router,
        "pcs_v2",
        SEL_SWAP_EXACT_TOKENS_FOR_ETH,
        decode_swap_exact_tokens_for_eth,
    );
    registry.register(
        router,
        "pcs_v2",
        SEL_SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE,
        decode_swap_exact_tokens_for_tokens_supporting_fee,
    );
}

// Suppress unused warning for U256 import if no constant uses it.
#[allow(dead_code)]
fn _unused() -> U256 {
    U256::ZERO
}
