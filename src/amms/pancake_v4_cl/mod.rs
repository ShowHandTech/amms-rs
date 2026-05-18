//! PancakeSwap V4 Infinity concentrated-liquidity pool support.
//!
//! PCS V4 mirrors Uniswap V4 architecturally — a singleton `CLPoolManager` holds every pool's
//! state, keyed by `PoolId = keccak256(abi.encode(PoolKey))`. The differences from Uniswap V4:
//!
//! - PoolKey has 6 fields: `(currency0, currency1, hooks, poolManager, fee, parameters)`. The
//!   extra `parameters: bytes32` low-bits-encodes `tickSpacing` (bits [16, 40)) plus a hook
//!   permission bitmap (bits [0, 16)).
//! - The `Initialize` event carries `parameters` instead of `tickSpacing`. We extract the spacing
//!   inline from the bytes32.
//! - The `Swap` event has one extra `protocolFee` field.
//! - CLPoolManager exposes public view getters (`getSlot0`, `getLiquidity`, `getPoolBitmapInfo`,
//!   `getPoolTickInfo`, `poolIdToPoolKey`). Our batch readers call those getters instead of
//!   `extsload`, which insulates us from any future CLPoolManager storage layout change.
//!
//! Swap math is the same V3 CLAMM, so we delegate to `uniswap_v3_math` and reuse `V4CLState`
//! from [`crate::amms::uniswap_v4`].

use super::{
    amm::{AmmId, AutomatedMarketMaker, AMM},
    error::AMMError,
    factory::{AutomatedMarketMakerFactory, DiscoverySync},
    get_token_decimals,
    uniswap_v3::{Info, UniswapV3Error},
    uniswap_v4::{HookFilter, V4CLState},
};
use alloy::{
    eips::BlockId,
    network::Network,
    primitives::{Address, Bytes, Signed, B256, I256, U256},
    providers::Provider,
    rpc::types::{Filter, FilterSet, Log},
    sol,
    sol_types::{SolEvent, SolValue},
    transports::BoxFuture,
};
use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    future::Future,
    str::FromStr,
};
use tracing::info;
use uniswap_v3_math::tick_math::{MAX_SQRT_RATIO, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK};

use crate::amms::consts::U256_1;

sol! {
    /// Subset of `ICLPoolManager` events needed for sync + discovery.
    #[derive(Debug, PartialEq, Eq)]
    #[sol(rpc)]
    contract ICLPoolManager {
        /// Emitted once per pool, when the pool is initialized.
        /// Note: PCS encodes `tickSpacing` inside `parameters` (bits 16..40), not as a
        /// standalone field like Uniswap V4 does.
        event Initialize(
            bytes32 indexed id,
            address indexed currency0,
            address indexed currency1,
            address hooks,
            uint24 fee,
            bytes32 parameters,
            uint160 sqrtPriceX96,
            int24 tick
        );

        /// Emitted on every swap. One more field than Uniswap V4's Swap (extra `protocolFee`).
        event Swap(
            bytes32 indexed id,
            address indexed sender,
            int128 amount0,
            int128 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint24 swapFee,
            uint24 protocolFee
        );

        /// Emitted on add or remove liquidity. Identical to Uniswap V4 ModifyLiquidity.
        event ModifyLiquidity(
            bytes32 indexed id,
            address indexed sender,
            int24 tickLower,
            int24 tickUpper,
            int256 liquidityDelta,
            bytes32 salt
        );
    }
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GetPancakeV4CLPoolSlot0BatchRequest,
    "src/amms/abi/GetPancakeV4CLPoolSlot0BatchRequest.json",
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GetPancakeV4CLPoolTickBitmapBatchRequest,
    "src/amms/abi/GetPancakeV4CLPoolTickBitmapBatchRequest.json",
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GetPancakeV4CLPoolTickDataBatchRequest,
    "src/amms/abi/GetPancakeV4CLPoolTickDataBatchRequest.json",
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PancakeV4CLPool {
    pub pool_id: B256,
    /// Address of the singleton `CLPoolManager` that holds this pool's state.
    pub cl_pool_manager: Address,
    /// `address(0)` represents native ETH/BNB on PCS V4.
    pub currency0: Address,
    pub currency0_decimals: u8,
    pub currency1: Address,
    pub currency1_decimals: u8,
    /// Hook contract; `address(0)` means no hook.
    pub hooks: Address,
    /// PCS-specific 32-byte parameters word. Bits [0, 16) are the hook permission bitmap; bits
    /// [16, 40) encode `tickSpacing` as a 24-bit signed integer. We keep the raw word so we can
    /// reconstruct `PoolKey` if needed (e.g. for on-chain `poolIdToPoolKey` round-trips).
    pub parameters: B256,
    pub state: V4CLState,
}

impl PancakeV4CLPool {
    pub fn new(cl_pool_manager: Address, pool_id: B256) -> Self {
        Self {
            pool_id,
            cl_pool_manager,
            ..Default::default()
        }
    }
}

/// Decode `tickSpacing` from the PCS `parameters` bytes32.
///
/// Per `CLPoolParametersHelper.sol`: bits [16, 40) hold a 24-bit signed `tickSpacing`. We treat
/// `parameters` as little-endian-by-byte (matching Solidity's `bytes32 >> 16` view) and extract
/// the low 24 bits as a signed 24-bit value, sign-extending to i32.
fn tick_spacing_from_parameters(parameters: B256) -> i32 {
    let bytes = parameters.0;
    // `parameters` is laid out big-endian in storage. The Encoded library's `decodeUint24(p, 16)`
    // reads 24 bits starting at *bit offset 16 from the right* (i.e. (params >> 16) & 0xFFFFFF).
    // Convert the big-endian 32 bytes to a U256 and shift.
    let value = U256::from_be_bytes(bytes);
    let raw = ((value >> 16usize) & U256::from(0xFFFFFFu32)).to::<u32>();
    // Sign-extend from 24 bits to 32 bits.
    if raw & 0x800000 != 0 {
        (raw | 0xFF000000) as i32
    } else {
        raw as i32
    }
}

impl AutomatedMarketMaker for PancakeV4CLPool {
    fn id(&self) -> AmmId {
        AmmId::V4 {
            singleton: self.cl_pool_manager,
            pool_id: self.pool_id,
        }
    }

    fn sync_events(&self) -> Vec<B256> {
        vec![
            ICLPoolManager::Swap::SIGNATURE_HASH,
            ICLPoolManager::ModifyLiquidity::SIGNATURE_HASH,
        ]
    }

    fn sync(&mut self, log: &Log) -> Result<(), AMMError> {
        let event_signature = log.topics()[0];
        match event_signature {
            ICLPoolManager::Swap::SIGNATURE_HASH => {
                let swap_event = ICLPoolManager::Swap::decode_log(log.as_ref())?;

                self.state.sqrt_price = U256::from(swap_event.sqrtPriceX96);
                self.state.liquidity = swap_event.liquidity;
                self.state.tick = swap_event.tick.unchecked_into();

                info!(
                    target = "amms::pancake_v4_cl::sync",
                    pool_id = ?self.pool_id,
                    sqrt_price = ?self.state.sqrt_price,
                    liquidity = ?self.state.liquidity,
                    tick = ?self.state.tick,
                    "Swap"
                );
            }
            ICLPoolManager::ModifyLiquidity::SIGNATURE_HASH => {
                let event = ICLPoolManager::ModifyLiquidity::decode_log(log.as_ref())?;

                let delta: i128 = event
                    .liquidityDelta
                    .try_into()
                    .map_err(|_| AMMError::UnrecognizedEventSignature(event_signature))?;

                self.modify_position(
                    event.tickLower.unchecked_into(),
                    event.tickUpper.unchecked_into(),
                    delta,
                )?;

                info!(
                    target = "amms::pancake_v4_cl::sync",
                    pool_id = ?self.pool_id,
                    sqrt_price = ?self.state.sqrt_price,
                    liquidity = ?self.state.liquidity,
                    tick = ?self.state.tick,
                    "ModifyLiquidity"
                );
            }
            _ => return Err(AMMError::UnrecognizedEventSignature(event_signature)),
        }
        Ok(())
    }

    fn tokens(&self) -> Vec<Address> {
        vec![self.currency0, self.currency1]
    }

    fn calculate_price(&self, base_token: Address, _quote_token: Address) -> Result<f64, AMMError> {
        let tick = uniswap_v3_math::tick_math::get_tick_at_sqrt_ratio(self.state.sqrt_price)
            .map_err(UniswapV3Error::from)?;
        let shift = self.currency0_decimals as i8 - self.currency1_decimals as i8;

        let price = match shift.cmp(&0) {
            Ordering::Less => 1.0001_f64.powi(tick) / 10_f64.powi(-shift as i32),
            Ordering::Greater => 1.0001_f64.powi(tick) * 10_f64.powi(shift as i32),
            Ordering::Equal => 1.0001_f64.powi(tick),
        };

        if base_token == self.currency0 {
            Ok(price)
        } else {
            Ok(1.0 / price)
        }
    }

    /// **WARNING:** Same caveat as Uniswap V4 — hooked pools may apply dynamic fees or rewrite
    /// swap math; this simulator only knows the static `fee`. The default `HookFilter::NoHooks`
    /// excludes hooked pools, so this only matters if you opt into a hook whitelist.
    fn simulate_swap(
        &self,
        base_token: Address,
        _quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        if amount_in.is_zero() {
            return Ok(U256::ZERO);
        }
        let (amount_out, _) =
            simulate_swap_inner(&self.state, base_token == self.currency0, amount_in)?;
        Ok(amount_out)
    }

    fn simulate_swap_mut(
        &mut self,
        base_token: Address,
        _quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        if amount_in.is_zero() {
            return Ok(U256::ZERO);
        }
        let (amount_out, end_state) =
            simulate_swap_inner(&self.state, base_token == self.currency0, amount_in)?;

        self.state.liquidity = end_state.liquidity;
        self.state.sqrt_price = end_state.sqrt_price_x_96;
        self.state.tick = end_state.tick;

        Ok(amount_out)
    }

    async fn init<N, P>(self, _block_number: BlockId, _provider: P) -> Result<Self, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        // PCS V4 pools have no standalone init path: state lives in the CLPoolManager singleton
        // and per-pool PoolKey fields are only known to the factory. The factory's static sync
        // helpers are the supported entry point.
        Ok(self)
    }
}

impl PancakeV4CLPool {
    pub fn modify_position(
        &mut self,
        tick_lower: i32,
        tick_upper: i32,
        liquidity_delta: i128,
    ) -> Result<(), AMMError> {
        self.update_position(tick_lower, tick_upper, liquidity_delta)?;

        if liquidity_delta != 0 && self.state.tick >= tick_lower && self.state.tick < tick_upper {
            self.state.liquidity = if liquidity_delta < 0 {
                self.state.liquidity - ((-liquidity_delta) as u128)
            } else {
                self.state.liquidity + (liquidity_delta as u128)
            };
        }
        Ok(())
    }

    fn update_position(
        &mut self,
        tick_lower: i32,
        tick_upper: i32,
        liquidity_delta: i128,
    ) -> Result<(), AMMError> {
        let mut flipped_lower = false;
        let mut flipped_upper = false;

        if liquidity_delta != 0 {
            flipped_lower = self.update_tick(tick_lower, liquidity_delta, false)?;
            flipped_upper = self.update_tick(tick_upper, liquidity_delta, true)?;
            if flipped_lower {
                self.flip_tick(tick_lower);
            }
            if flipped_upper {
                self.flip_tick(tick_upper);
            }
        }

        if liquidity_delta < 0 {
            if flipped_lower {
                self.state.ticks.remove(&tick_lower);
            }
            if flipped_upper {
                self.state.ticks.remove(&tick_upper);
            }
        }
        Ok(())
    }

    fn update_tick(
        &mut self,
        tick: i32,
        liquidity_delta: i128,
        upper: bool,
    ) -> Result<bool, AMMError> {
        let info = self.state.ticks.entry(tick).or_default();
        let liquidity_gross_before = info.liquidity_gross;
        let liquidity_gross_after = if liquidity_delta < 0 {
            liquidity_gross_before - ((-liquidity_delta) as u128)
        } else {
            liquidity_gross_before + (liquidity_delta as u128)
        };
        let flipped = (liquidity_gross_after == 0) != (liquidity_gross_before == 0);
        if liquidity_gross_before == 0 {
            info.initialized = true;
        }
        info.liquidity_gross = liquidity_gross_after;
        info.liquidity_net = if upper {
            info.liquidity_net - liquidity_delta
        } else {
            info.liquidity_net + liquidity_delta
        };
        Ok(flipped)
    }

    fn flip_tick(&mut self, tick: i32) {
        let (word_pos, bit_pos) =
            uniswap_v3_math::tick_bitmap::position(tick / self.state.tick_spacing);
        let mask = U256::from(1) << bit_pos;
        if let Some(word) = self.state.tick_bitmap.get_mut(&word_pos) {
            *word ^= mask;
        } else {
            self.state.tick_bitmap.insert(word_pos, mask);
        }
    }
}

struct InnerSwapState {
    sqrt_price_x_96: U256,
    tick: i32,
    liquidity: u128,
}

fn simulate_swap_inner(
    state: &V4CLState,
    zero_for_one: bool,
    amount_in: U256,
) -> Result<(U256, InnerSwapState), AMMError> {
    let sqrt_price_limit_x_96 = if zero_for_one {
        MIN_SQRT_RATIO + U256_1
    } else {
        MAX_SQRT_RATIO - U256_1
    };

    let mut sqrt_price_x_96 = state.sqrt_price;
    let mut amount_calculated = I256::ZERO;
    let mut amount_specified_remaining = I256::from_raw(amount_in);
    let mut tick = state.tick;
    let mut liquidity = state.liquidity;

    while amount_specified_remaining != I256::ZERO && sqrt_price_x_96 != sqrt_price_limit_x_96 {
        let sqrt_price_start_x_96 = sqrt_price_x_96;

        let (mut tick_next, initialized) =
            uniswap_v3_math::tick_bitmap::next_initialized_tick_within_one_word(
                &state.tick_bitmap,
                tick,
                state.tick_spacing,
                zero_for_one,
            )
            .map_err(UniswapV3Error::from)?;

        tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);

        let sqrt_price_next_x96 = uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick(tick_next)
            .map_err(UniswapV3Error::from)?;

        let swap_target_sqrt_ratio = if zero_for_one {
            sqrt_price_next_x96.max(sqrt_price_limit_x_96)
        } else {
            sqrt_price_next_x96.min(sqrt_price_limit_x_96)
        };

        let (next_sqrt_price, amount_in_step, amount_out_step, fee_amount) =
            uniswap_v3_math::swap_math::compute_swap_step(
                sqrt_price_x_96,
                swap_target_sqrt_ratio,
                liquidity,
                amount_specified_remaining,
                state.fee,
            )
            .map_err(UniswapV3Error::from)?;
        sqrt_price_x_96 = next_sqrt_price;

        amount_specified_remaining = amount_specified_remaining
            .overflowing_sub(I256::from_raw(
                amount_in_step.overflowing_add(fee_amount).0,
            ))
            .0;
        amount_calculated -= I256::from_raw(amount_out_step);

        if sqrt_price_x_96 == sqrt_price_next_x96 {
            if initialized {
                let mut liquidity_net = state
                    .ticks
                    .get(&tick_next)
                    .map(|info| info.liquidity_net)
                    .unwrap_or_default();
                if zero_for_one {
                    liquidity_net = -liquidity_net;
                }
                liquidity = if liquidity_net < 0 {
                    if liquidity < (-liquidity_net as u128) {
                        return Err(UniswapV3Error::LiquidityUnderflow.into());
                    } else {
                        liquidity - (-liquidity_net as u128)
                    }
                } else {
                    liquidity + (liquidity_net as u128)
                };
            }
            tick = if zero_for_one {
                tick_next.wrapping_sub(1)
            } else {
                tick_next
            };
        } else if sqrt_price_x_96 != sqrt_price_start_x_96 {
            tick = uniswap_v3_math::tick_math::get_tick_at_sqrt_ratio(sqrt_price_x_96)
                .map_err(UniswapV3Error::from)?;
        }
    }

    let amount_out = (-amount_calculated).into_raw();
    Ok((
        amount_out,
        InnerSwapState {
            sqrt_price_x_96,
            tick,
            liquidity,
        },
    ))
}

/// Discovery + sync entry point for PancakeSwap V4 Infinity CL pools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PancakeV4CLFactory {
    pub cl_pool_manager: Address,
    pub creation_block: u64,
    pub hook_filter: HookFilter,
}

impl PancakeV4CLFactory {
    pub fn new(cl_pool_manager: Address, creation_block: u64) -> Self {
        Self {
            cl_pool_manager,
            creation_block,
            hook_filter: HookFilter::default(),
        }
    }

    pub fn with_hook_filter(mut self, hook_filter: HookFilter) -> Self {
        self.hook_filter = hook_filter;
        self
    }

    pub async fn get_all_pools<N, P>(
        &self,
        block_number: BlockId,
        provider: P,
    ) -> Result<Vec<AMM>, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let disc_filter = Filter::new()
            .event_signature(FilterSet::from(vec![self.pool_creation_event()]))
            .address(vec![self.cl_pool_manager]);

        let sync_provider = provider.clone();
        let mut futures = FuturesUnordered::new();

        let sync_step = 100_000;
        let mut latest_block = self.creation_block;
        while latest_block < block_number.as_u64().unwrap_or_default() {
            let mut block_filter = disc_filter.clone();
            let from_block = latest_block;
            let to_block = (from_block + sync_step).min(block_number.as_u64().unwrap_or_default());

            block_filter = block_filter.from_block(from_block);
            block_filter = block_filter.to_block(to_block);

            let sync_provider = sync_provider.clone();
            futures.push(async move { sync_provider.get_logs(&block_filter).await });

            latest_block = to_block + 1;
        }

        let mut pools = vec![];
        while let Some(res) = futures.next().await {
            let logs = res?;
            for log in logs {
                let amm = self.create_pool(log)?;
                if let AMM::PancakeV4CLPool(ref pool) = amm {
                    if !self.hook_filter.accept(pool.hooks) {
                        continue;
                    }
                }
                pools.push(amm);
            }
        }

        Ok(pools)
    }

    pub async fn sync_all_pools<N, P>(
        mut pools: Vec<AMM>,
        cl_pool_manager: Address,
        block_number: BlockId,
        provider: P,
    ) -> Result<Vec<AMM>, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        Self::sync_slot_0(&mut pools, cl_pool_manager, block_number, provider.clone()).await?;
        Self::sync_token_decimals(&mut pools, provider.clone()).await?;

        pools.retain(|pool| match pool {
            AMM::PancakeV4CLPool(pcs_pool) => {
                pcs_pool.state.liquidity > 0
                    && pcs_pool.currency0_decimals > 0
                    && pcs_pool.currency1_decimals > 0
            }
            _ => true,
        });

        Self::sync_tick_bitmaps(&mut pools, cl_pool_manager, block_number, provider.clone())
            .await?;
        Self::sync_tick_data(&mut pools, cl_pool_manager, block_number, provider.clone()).await?;

        Ok(pools)
    }

    async fn sync_token_decimals<N, P>(
        pools: &mut [AMM],
        provider: P,
    ) -> Result<(), crate::amms::error::BatchContractError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let mut tokens = HashSet::new();
        for pool in pools.iter() {
            for token in pool.tokens() {
                if token != Address::ZERO {
                    tokens.insert(token);
                }
            }
        }
        let token_decimals = get_token_decimals(tokens.into_iter().collect(), provider).await?;

        for pool in pools.iter_mut() {
            let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                continue;
            };
            // Native gas token (BNB on BSC, ETH on Ethereum) defaults to 18 decimals.
            pcs_pool.currency0_decimals = if pcs_pool.currency0 == Address::ZERO {
                18
            } else {
                token_decimals
                    .get(&pcs_pool.currency0)
                    .copied()
                    .unwrap_or_default()
            };
            pcs_pool.currency1_decimals = if pcs_pool.currency1 == Address::ZERO {
                18
            } else {
                token_decimals
                    .get(&pcs_pool.currency1)
                    .copied()
                    .unwrap_or_default()
            };
        }

        Ok(())
    }

    async fn sync_slot_0<N, P>(
        pools: &mut [AMM],
        cl_pool_manager: Address,
        block_number: BlockId,
        provider: P,
    ) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let step = 255;
        let mut futures = FuturesUnordered::new();

        pools.chunks_mut(step).for_each(|group| {
            let provider = provider.clone();
            let pool_ids = group
                .iter()
                .filter_map(|pool| match pool {
                    AMM::PancakeV4CLPool(pcs_pool) => Some(pcs_pool.pool_id),
                    _ => None,
                })
                .collect::<Vec<_>>();

            futures.push(async move {
                Ok::<(&mut [AMM], Bytes), AMMError>((
                    group,
                    GetPancakeV4CLPoolSlot0BatchRequest::deploy_builder(
                        provider,
                        cl_pool_manager,
                        pool_ids,
                    )
                    .call_raw()
                    .block(block_number)
                    .await?,
                ))
            });
        });

        while let Some(res) = futures.next().await {
            let (pools, return_data) = res?;
            let return_data =
                <Vec<(U256, i32, u32, u32, u128)> as SolValue>::abi_decode(&return_data)?;

            for (slot_0_data, pool) in return_data.iter().zip(pools.iter_mut()) {
                let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                    continue;
                };
                pcs_pool.state.sqrt_price = slot_0_data.0;
                pcs_pool.state.tick = slot_0_data.1;
                pcs_pool.state.liquidity = slot_0_data.4;
            }
        }

        Ok(())
    }

    async fn sync_tick_bitmaps<N, P>(
        pools: &mut [AMM],
        cl_pool_manager: Address,
        block_number: BlockId,
        provider: P,
    ) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let mut futures: FuturesUnordered<BoxFuture<'_, _>> = FuturesUnordered::new();

        let max_range = 6900;
        let mut group_range: i32 = 0;
        let mut group: Vec<GetPancakeV4CLPoolTickBitmapBatchRequest::TickBitmapInfo> = vec![];

        for pool in pools.iter() {
            let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                continue;
            };

            let mut min_word = tick_to_word(MIN_TICK, pcs_pool.state.tick_spacing);
            let max_word = tick_to_word(MAX_TICK, pcs_pool.state.tick_spacing);
            let mut word_range = max_word - min_word;

            while word_range > 0 {
                let remaining_range = max_range - group_range;
                let range = word_range.min(remaining_range);

                group.push(
                    GetPancakeV4CLPoolTickBitmapBatchRequest::TickBitmapInfo {
                        poolId: pcs_pool.pool_id,
                        minWord: min_word as i16,
                        maxWord: (min_word + range) as i16,
                    },
                );

                word_range -= range;
                min_word += range - 1;
                group_range += range;

                if group_range >= max_range {
                    let provider = provider.clone();
                    let ids = group.iter().map(|info| info.poolId).collect::<Vec<_>>();
                    let calldata = std::mem::take(&mut group);
                    group_range = 0;

                    futures.push(Box::pin(async move {
                        Ok::<(Vec<B256>, Bytes), AMMError>((
                            ids,
                            GetPancakeV4CLPoolTickBitmapBatchRequest::deploy_builder(
                                provider,
                                cl_pool_manager,
                                calldata,
                            )
                            .call_raw()
                            .block(block_number)
                            .await?,
                        ))
                    }));
                }
            }
        }

        if !group.is_empty() {
            let provider = provider.clone();
            let ids = group.iter().map(|info| info.poolId).collect::<Vec<_>>();
            let calldata = std::mem::take(&mut group);

            futures.push(Box::pin(async move {
                Ok::<(Vec<B256>, Bytes), AMMError>((
                    ids,
                    GetPancakeV4CLPoolTickBitmapBatchRequest::deploy_builder(
                        provider,
                        cl_pool_manager,
                        calldata,
                    )
                    .call_raw()
                    .block(block_number)
                    .await?,
                ))
            }));
        }

        let mut pool_set = pools
            .iter_mut()
            .filter_map(|pool| match pool {
                AMM::PancakeV4CLPool(pcs_pool) => Some((pcs_pool.pool_id, pool)),
                _ => None,
            })
            .collect::<HashMap<B256, &mut AMM>>();

        while let Some(res) = futures.next().await {
            let (ids, return_data) = res?;
            let return_data = <Vec<Vec<U256>> as SolValue>::abi_decode(&return_data)?;

            for (tick_bitmaps, pool_id) in return_data.iter().zip(ids.iter()) {
                let Some(pool) = pool_set.get_mut(pool_id) else {
                    continue;
                };
                let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                    continue;
                };

                for chunk in tick_bitmaps.chunks_exact(2) {
                    let word_pos = I256::from_raw(chunk[0]).as_i16();
                    let tick_bitmap = chunk[1];
                    pcs_pool.state.tick_bitmap.insert(word_pos, tick_bitmap);
                }
            }
        }

        Ok(())
    }

    async fn sync_tick_data<N, P>(
        pools: &mut [AMM],
        cl_pool_manager: Address,
        block_number: BlockId,
        provider: P,
    ) -> Result<(), AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        let pool_ticks = pools
            .iter()
            .filter_map(|pool| {
                let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                    return None;
                };

                let min_word = tick_to_word(MIN_TICK, pcs_pool.state.tick_spacing);
                let max_word = tick_to_word(MAX_TICK, pcs_pool.state.tick_spacing);

                let initialized_ticks: Vec<Signed<24, 1>> = (min_word..=max_word)
                    .filter_map(|word_pos| {
                        pcs_pool
                            .state
                            .tick_bitmap
                            .get(&(word_pos as i16))
                            .filter(|&bitmap| *bitmap != U256::ZERO)
                            .map(|&bitmap| (word_pos, bitmap))
                    })
                    .flat_map(|(word_pos, bitmap)| {
                        let tick_spacing = pcs_pool.state.tick_spacing;
                        (0..256)
                            .filter(move |i| {
                                (bitmap & (U256::from(1) << U256::from(*i))) != U256::ZERO
                            })
                            .map(move |i| {
                                let tick_index = (word_pos * 256 + i) * tick_spacing;
                                Signed::<24, 1>::from_str(&tick_index.to_string()).unwrap()
                            })
                    })
                    .collect();

                if initialized_ticks.is_empty() {
                    None
                } else {
                    Some((pcs_pool.pool_id, initialized_ticks))
                }
            })
            .collect::<Vec<(B256, Vec<Signed<24, 1>>)>>();

        let mut futures: FuturesUnordered<BoxFuture<'_, _>> = FuturesUnordered::new();
        let max_ticks = 60;
        let mut group_ticks = 0;
        let mut group: Vec<GetPancakeV4CLPoolTickDataBatchRequest::TickDataInfo> = vec![];

        for (pool_id, mut ticks) in pool_ticks {
            while !ticks.is_empty() {
                let remaining_ticks = max_ticks - group_ticks;
                let selected_ticks = ticks.drain(0..remaining_ticks.min(ticks.len()));
                group_ticks += selected_ticks.len();

                group.push(GetPancakeV4CLPoolTickDataBatchRequest::TickDataInfo {
                    poolId: pool_id,
                    ticks: selected_ticks.collect(),
                });

                if group_ticks >= max_ticks {
                    let provider = provider.clone();
                    let calldata = std::mem::take(&mut group);
                    group_ticks = 0;

                    futures.push(Box::pin(async move {
                        Ok::<
                            (
                                Vec<GetPancakeV4CLPoolTickDataBatchRequest::TickDataInfo>,
                                Bytes,
                            ),
                            AMMError,
                        >((
                            calldata.clone(),
                            GetPancakeV4CLPoolTickDataBatchRequest::deploy_builder(
                                provider,
                                cl_pool_manager,
                                calldata,
                            )
                            .call_raw()
                            .block(block_number)
                            .await?,
                        ))
                    }));
                }
            }
        }

        if !group.is_empty() {
            let provider = provider.clone();
            let calldata = std::mem::take(&mut group);

            futures.push(Box::pin(async move {
                Ok::<
                    (
                        Vec<GetPancakeV4CLPoolTickDataBatchRequest::TickDataInfo>,
                        Bytes,
                    ),
                    AMMError,
                >((
                    calldata.clone(),
                    GetPancakeV4CLPoolTickDataBatchRequest::deploy_builder(
                        provider,
                        cl_pool_manager,
                        calldata,
                    )
                    .call_raw()
                    .block(block_number)
                    .await?,
                ))
            }));
        }

        let mut pool_set = pools
            .iter_mut()
            .filter_map(|pool| match pool {
                AMM::PancakeV4CLPool(pcs_pool) => Some((pcs_pool.pool_id, pool)),
                _ => None,
            })
            .collect::<HashMap<B256, &mut AMM>>();

        while let Some(res) = futures.next().await {
            let (tick_info, return_data) = res?;
            let return_data =
                <Vec<Vec<(bool, u128, i128)>> as SolValue>::abi_decode(&return_data)?;

            for (tick_data, tick_info) in return_data.iter().zip(tick_info.iter()) {
                let Some(pool) = pool_set.get_mut(&tick_info.poolId) else {
                    continue;
                };
                let AMM::PancakeV4CLPool(pcs_pool) = pool else {
                    continue;
                };

                for (tick, tick_idx) in tick_data.iter().zip(tick_info.ticks.iter()) {
                    let info = Info {
                        liquidity_gross: tick.1,
                        liquidity_net: tick.2,
                        initialized: tick.0,
                    };
                    pcs_pool.state.ticks.insert(tick_idx.as_i32(), info);
                }
            }
        }

        Ok(())
    }
}

fn tick_to_word(tick: i32, tick_spacing: i32) -> i32 {
    let mut compressed = tick / tick_spacing;
    if tick < 0 && tick % tick_spacing != 0 {
        compressed -= 1;
    }
    compressed >> 8
}

impl AutomatedMarketMakerFactory for PancakeV4CLFactory {
    type PoolVariant = PancakeV4CLPool;

    /// PCS V4 has no separate factory contract — the `CLPoolManager` singleton is the discovery
    /// target.
    fn address(&self) -> Address {
        self.cl_pool_manager
    }

    fn pool_creation_event(&self) -> B256 {
        ICLPoolManager::Initialize::SIGNATURE_HASH
    }

    /// Decodes a PCS `Initialize` log into an unsynced `PancakeV4CLPool`. `tick_spacing` is
    /// extracted from the `parameters` bytes32 per `CLPoolParametersHelper.sol`.
    fn create_pool(&self, log: Log) -> Result<AMM, AMMError> {
        let init = ICLPoolManager::Initialize::decode_log(&log.inner)?;

        let tick_spacing = tick_spacing_from_parameters(init.parameters);
        let fee: u32 = init.fee.to::<u32>();
        let sqrt_price = U256::from(init.sqrtPriceX96);
        let tick: i32 = init.tick.unchecked_into();

        Ok(AMM::PancakeV4CLPool(PancakeV4CLPool {
            pool_id: init.id,
            cl_pool_manager: self.cl_pool_manager,
            currency0: init.currency0,
            currency0_decimals: 0,
            currency1: init.currency1,
            currency1_decimals: 0,
            hooks: init.hooks,
            parameters: init.parameters,
            state: V4CLState {
                liquidity: 0,
                sqrt_price,
                tick,
                fee,
                tick_spacing,
                tick_bitmap: HashMap::new(),
                ticks: HashMap::new(),
            },
        }))
    }

    fn creation_block(&self) -> u64 {
        self.creation_block
    }
}

impl PancakeV4CLFactory {
    /// Phase 6+: convert a `PoolDescriptor::PcsV4Cl` into an unsynced `PancakeV4CLPool`. If
    /// `currency0`/`currency1`/`hooks`/`parameters` are not supplied by the descriptor, the
    /// resulting pool is partially initialized; a subsequent on-chain `poolIdToPoolKey()`
    /// round-trip can backfill these fields. `tick_spacing` is decoded from `parameters` when
    /// present, otherwise left at 0 for the sync stage to populate.
    pub fn from_descriptor(
        &self,
        desc: &crate::discovery::PoolDescriptor,
    ) -> Result<AMM, AMMError> {
        match desc {
            crate::discovery::PoolDescriptor::PcsV4Cl {
                pool_id,
                cl_pool_manager,
                currency0,
                currency1,
                hooks,
                parameters,
            } => {
                if *cl_pool_manager != self.cl_pool_manager {
                    return Err(AMMError::DescriptorSingletonMismatch {
                        got: *cl_pool_manager,
                        expected: self.cl_pool_manager,
                    });
                }
                let parameters = parameters.unwrap_or_default();
                let tick_spacing = if parameters == B256::ZERO {
                    0
                } else {
                    tick_spacing_from_parameters(parameters)
                };
                Ok(AMM::PancakeV4CLPool(PancakeV4CLPool {
                    pool_id: *pool_id,
                    cl_pool_manager: *cl_pool_manager,
                    currency0: currency0.unwrap_or_default(),
                    currency0_decimals: 0,
                    currency1: currency1.unwrap_or_default(),
                    currency1_decimals: 0,
                    hooks: hooks.unwrap_or_default(),
                    parameters,
                    state: V4CLState {
                        liquidity: 0,
                        sqrt_price: U256::ZERO,
                        tick: 0,
                        fee: 0,
                        tick_spacing,
                        tick_bitmap: HashMap::new(),
                        ticks: HashMap::new(),
                    },
                }))
            }
            _ => Err(AMMError::IncompatibleDescriptor),
        }
    }
}

impl DiscoverySync for PancakeV4CLFactory {
    fn discover<N, P>(
        &self,
        to_block: BlockId,
        provider: P,
    ) -> impl Future<Output = Result<Vec<AMM>, AMMError>>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        info!(
            target = "amms::pancake_v4_cl::discover",
            cl_pool_manager = ?self.cl_pool_manager,
            "Discovering all PCS V4 CL pools"
        );
        self.get_all_pools(to_block, provider)
    }

    fn sync<N, P>(
        &self,
        amms: Vec<AMM>,
        to_block: BlockId,
        provider: P,
    ) -> impl Future<Output = Result<Vec<AMM>, AMMError>>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        info!(
            target = "amms::pancake_v4_cl::sync",
            cl_pool_manager = ?self.cl_pool_manager,
            "Syncing all PCS V4 CL pools"
        );
        PancakeV4CLFactory::sync_all_pools(amms, self.cl_pool_manager, to_block, provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_spacing_positive() {
        // tickSpacing = 60, packed at bit-offset 16 (low 24 bits of [16, 40))
        let raw: U256 = U256::from(60u32) << 16usize;
        let p: B256 = raw.to_be_bytes::<32>().into();
        assert_eq!(tick_spacing_from_parameters(p), 60);
    }

    #[test]
    fn tick_spacing_negative() {
        // tickSpacing = -1 → 24-bit two's complement = 0xFFFFFF, packed at bit-offset 16
        let raw: U256 = U256::from(0xFFFFFFu32) << 16usize;
        let p: B256 = raw.to_be_bytes::<32>().into();
        assert_eq!(tick_spacing_from_parameters(p), -1);
    }

    #[test]
    fn tick_spacing_unused_bits_ignored() {
        // tickSpacing = 10, plus garbage in the [40, 256) unused range and [0, 16) hook bits.
        let mut raw: U256 = U256::from(10u32) << 16usize;
        raw |= U256::from(0xABCDu32); // hook bitmap, ignored
        raw |= U256::from(0xDEADBEEFu64) << 40usize; // unused, ignored
        let p: B256 = raw.to_be_bytes::<32>().into();
        assert_eq!(tick_spacing_from_parameters(p), 10);
    }
}
