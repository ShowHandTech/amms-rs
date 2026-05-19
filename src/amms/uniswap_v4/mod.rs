//! Uniswap V4 concentrated-liquidity pool support.
//!
//! V4 lives inside a single `PoolManager` contract — there is no per-pool address.
//! Pools are identified by `PoolId = keccak256(abi.encode(PoolKey))`. State (slot0,
//! liquidity, tickBitmap, ticks) is read out of the manager's storage via `extsload`
//! through deployless batch contracts (see `contracts/src/UniswapV4/`).
//!
//! Swap math is identical to V3 (same CLAMM), so we delegate to the `uniswap_v3_math` crate.

use super::{
    amm::{AmmId, AutomatedMarketMaker, AMM},
    error::AMMError,
    factory::{AutomatedMarketMakerFactory, DiscoverySync},
    get_token_decimals,
    uniswap_v3::{Info, UniswapV3Error},
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
    /// Subset of `IPoolManager` events we need for sync + discovery.
    #[derive(Debug, PartialEq, Eq)]
    #[sol(rpc)]
    contract IUniswapV4PoolManager {
        /// Emitted exactly once per pool, when the pool is created.
        event Initialize(
            bytes32 indexed id,
            address indexed currency0,
            address indexed currency1,
            uint24 fee,
            int24 tickSpacing,
            address hooks,
            uint160 sqrtPriceX96,
            int24 tick
        );

        /// Emitted on every swap. `id` is the PoolId.
        event Swap(
            bytes32 indexed id,
            address indexed sender,
            int128 amount0,
            int128 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint24 fee
        );

        /// Emitted when liquidity is added or removed. `id` is the PoolId.
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
    GetUniswapV4PoolSlot0BatchRequest,
    "src/amms/abi/GetUniswapV4PoolSlot0BatchRequest.json",
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GetUniswapV4PoolTickBitmapBatchRequest,
    "src/amms/abi/GetUniswapV4PoolTickBitmapBatchRequest.json",
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    GetUniswapV4PoolTickDataBatchRequest,
    "src/amms/abi/GetUniswapV4PoolTickDataBatchRequest.json",
}

/// State shared by all V4-family CLAMM pools (Uniswap V4 + PancakeSwap Infinity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct V4CLState {
    pub liquidity: u128,
    pub sqrt_price: U256,
    pub tick: i32,
    /// Static LP fee. Dynamic-fee hooks override this at swap time, in which case
    /// `simulate_swap` will be inaccurate (see warning on `simulate_swap`).
    pub fee: u32,
    pub tick_spacing: i32,
    pub tick_bitmap: HashMap<i16, U256>,
    pub ticks: HashMap<i32, Info>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UniswapV4Pool {
    pub pool_id: B256,
    /// Address of the singleton `PoolManager` that holds this pool's state.
    pub pool_manager: Address,
    /// `address(0)` represents native ETH on Uniswap V4.
    pub currency0: Address,
    pub currency0_decimals: u8,
    pub currency1: Address,
    pub currency1_decimals: u8,
    /// Hook contract; `address(0)` means no hook.
    pub hooks: Address,
    pub state: V4CLState,
}

impl UniswapV4Pool {
    pub fn new(pool_manager: Address, pool_id: B256) -> Self {
        Self {
            pool_id,
            pool_manager,
            ..Default::default()
        }
    }
}

impl AutomatedMarketMaker for UniswapV4Pool {
    fn id(&self) -> AmmId {
        AmmId::V4 {
            singleton: self.pool_manager,
            pool_id: self.pool_id,
        }
    }

    fn sync_events(&self) -> Vec<B256> {
        vec![
            IUniswapV4PoolManager::Swap::SIGNATURE_HASH,
            IUniswapV4PoolManager::ModifyLiquidity::SIGNATURE_HASH,
        ]
    }

    fn sync(&mut self, log: &Log) -> Result<(), AMMError> {
        let event_signature = log.topics()[0];
        match event_signature {
            IUniswapV4PoolManager::Swap::SIGNATURE_HASH => {
                let swap_event = IUniswapV4PoolManager::Swap::decode_log(log.as_ref())?;

                self.state.sqrt_price = U256::from(swap_event.sqrtPriceX96);
                self.state.liquidity = swap_event.liquidity;
                self.state.tick = swap_event.tick.unchecked_into();

                info!(
                    target = "amms::uniswap_v4::sync",
                    pool_id = ?self.pool_id,
                    sqrt_price = ?self.state.sqrt_price,
                    liquidity = ?self.state.liquidity,
                    tick = ?self.state.tick,
                    "Swap"
                );
            }
            IUniswapV4PoolManager::ModifyLiquidity::SIGNATURE_HASH => {
                let event = IUniswapV4PoolManager::ModifyLiquidity::decode_log(log.as_ref())?;

                let delta: i128 = event.liquidityDelta.try_into().map_err(|_| {
                    AMMError::UnrecognizedEventSignature(event_signature)
                })?;

                self.modify_position(
                    event.tickLower.unchecked_into(),
                    event.tickUpper.unchecked_into(),
                    delta,
                )?;

                info!(
                    target = "amms::uniswap_v4::sync",
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

    /// **WARNING:** Pools with a hook contract may apply dynamic fees or custom curve logic
    /// during `beforeSwap`/`afterSwap`. This simulator only knows the static `fee` and cannot
    /// model hook behaviour, so its output may diverge from the on-chain quoter for hooked
    /// pools. The default factory configuration excludes hooked pools.
    fn simulate_swap(
        &self,
        base_token: Address,
        _quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError> {
        if amount_in.is_zero() {
            return Ok(U256::ZERO);
        }
        let (amount_out, _) = simulate_swap_inner(&self.state, base_token == self.currency0, amount_in)?;
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
        // V4 pools have no standalone init path: state lives in the singleton and the per-pool
        // PoolKey fields (currency0/1, hooks, tickSpacing) are only known to the factory. The
        // `UniswapV4Factory::sync_*` static helpers are the supported entry point and are
        // invoked by `StateSpaceBuilder` for V4 pools that were registered via a factory.
        Ok(self)
    }
}

impl UniswapV4Pool {
    /// Apply a Mint/Burn (V3) or ModifyLiquidity (V4) delta to the per-tick map and current
    /// liquidity. Mirrors `UniswapV3Pool::modify_position`.
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

    fn update_tick(&mut self, tick: i32, liquidity_delta: i128, upper: bool) -> Result<bool, AMMError> {
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

/// V3-style CLAMM swap loop. Returns `(amount_out, end_state)`.
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

/// Discovery-time filter for V4 hook contracts.
///
/// V4 hooks can rewrite fees, alter swap math, or take side effects during `beforeSwap` /
/// `afterSwap`. The `simulate_swap` implementation is V3 CLAMM math with a static fee; it
/// cannot model hook behaviour. By default we drop every pool whose `hooks != address(0)`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum HookFilter {
    /// Only allow pools with `hooks == address(0)`. **Default.**
    #[default]
    NoHooks,
    /// Allow pools with `hooks == address(0)` OR `hooks` in this set. Use after auditing the
    /// specific hook contracts — `simulate_swap` accuracy is the caller's problem.
    Whitelist(HashSet<Address>),
    /// Allow all pools. Debug / research only — `simulate_swap` will be wrong for any pool
    /// whose hook applies a dynamic fee or custom curve.
    AllowAll,
}

impl HookFilter {
    pub fn accept(&self, hooks: Address) -> bool {
        match self {
            HookFilter::NoHooks => hooks == Address::ZERO,
            HookFilter::Whitelist(set) => hooks == Address::ZERO || set.contains(&hooks),
            HookFilter::AllowAll => true,
        }
    }
}

/// Discovery + sync entry point for Uniswap V4 pools.
///
/// V4 has no separate factory contract; pools live inside the singleton `PoolManager`.
/// We use `pool_manager` as the factory identity (and as the address filter for log
/// subscription).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapV4Factory {
    pub pool_manager: Address,
    pub creation_block: u64,
    pub hook_filter: HookFilter,
}

impl UniswapV4Factory {
    pub fn new(pool_manager: Address, creation_block: u64) -> Self {
        Self {
            pool_manager,
            creation_block,
            hook_filter: HookFilter::NoHooks,
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
            .address(vec![self.pool_manager]);

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
                if let AMM::UniswapV4Pool(ref v4_pool) = amm {
                    if !self.hook_filter.accept(v4_pool.hooks) {
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
        pool_manager: Address,
        block_number: BlockId,
        provider: P,
    ) -> Result<Vec<AMM>, AMMError>
    where
        N: Network,
        P: Provider<N> + Clone,
    {
        Self::sync_slot_0(&mut pools, pool_manager, block_number, provider.clone()).await?;
        Self::sync_token_decimals(&mut pools, provider.clone()).await?;

        pools.retain(|pool| match pool {
            AMM::UniswapV4Pool(v4_pool) => {
                v4_pool.state.liquidity > 0
                    && v4_pool.currency0_decimals > 0
                    && v4_pool.currency1_decimals > 0
            }
            _ => true,
        });

        Self::sync_tick_bitmaps(&mut pools, pool_manager, block_number, provider.clone()).await?;
        Self::sync_tick_data(&mut pools, pool_manager, block_number, provider.clone()).await?;

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
        // V4 supports native ETH as `address(0)`. Skip it — `decimals()` would revert.
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
            let AMM::UniswapV4Pool(v4_pool) = pool else {
                continue;
            };

            // Native ETH defaults to 18 decimals.
            v4_pool.currency0_decimals = if v4_pool.currency0 == Address::ZERO {
                18
            } else {
                token_decimals
                    .get(&v4_pool.currency0)
                    .copied()
                    .unwrap_or_default()
            };
            v4_pool.currency1_decimals = if v4_pool.currency1 == Address::ZERO {
                18
            } else {
                token_decimals
                    .get(&v4_pool.currency1)
                    .copied()
                    .unwrap_or_default()
            };
        }

        Ok(())
    }

    async fn sync_slot_0<N, P>(
        pools: &mut [AMM],
        pool_manager: Address,
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
                    AMM::UniswapV4Pool(v4_pool) => Some(v4_pool.pool_id),
                    _ => None,
                })
                .collect::<Vec<_>>();

            futures.push(async move {
                Ok::<(&mut [AMM], Bytes), AMMError>((
                    group,
                    GetUniswapV4PoolSlot0BatchRequest::deploy_builder(
                        provider,
                        pool_manager,
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
            // Slot0Data { uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee, uint128 liquidity }
            let return_data =
                <Vec<(U256, i32, u32, u32, u128)> as SolValue>::abi_decode(&return_data)?;

            for (slot_0_data, pool) in return_data.iter().zip(pools.iter_mut()) {
                let AMM::UniswapV4Pool(v4_pool) = pool else {
                    continue;
                };
                v4_pool.state.sqrt_price = slot_0_data.0;
                v4_pool.state.tick = slot_0_data.1;
                // slot_0_data.2 is protocolFee; we keep the static lpFee already on the pool.
                // slot_0_data.3 is the active lpFee; for static-fee pools this matches `state.fee`.
                v4_pool.state.liquidity = slot_0_data.4;
            }
        }

        Ok(())
    }

    async fn sync_tick_bitmaps<N, P>(
        pools: &mut [AMM],
        pool_manager: Address,
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
        let mut group: Vec<GetUniswapV4PoolTickBitmapBatchRequest::TickBitmapInfo> = vec![];

        for pool in pools.iter() {
            let AMM::UniswapV4Pool(v4_pool) = pool else {
                continue;
            };

            let mut min_word = tick_to_word(MIN_TICK, v4_pool.state.tick_spacing);
            let max_word = tick_to_word(MAX_TICK, v4_pool.state.tick_spacing);
            let mut word_range = max_word - min_word;

            while word_range > 0 {
                let remaining_range = max_range - group_range;
                let range = word_range.min(remaining_range);

                group.push(GetUniswapV4PoolTickBitmapBatchRequest::TickBitmapInfo {
                    poolId: v4_pool.pool_id,
                    minWord: min_word as i16,
                    maxWord: (min_word + range) as i16,
                });

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
                            GetUniswapV4PoolTickBitmapBatchRequest::deploy_builder(
                                provider,
                                pool_manager,
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
                    GetUniswapV4PoolTickBitmapBatchRequest::deploy_builder(
                        provider,
                        pool_manager,
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
                AMM::UniswapV4Pool(v4_pool) => Some((v4_pool.pool_id, pool)),
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
                let AMM::UniswapV4Pool(v4_pool) = pool else {
                    continue;
                };

                for chunk in tick_bitmaps.chunks_exact(2) {
                    let word_pos = I256::from_raw(chunk[0]).as_i16();
                    let tick_bitmap = chunk[1];
                    v4_pool.state.tick_bitmap.insert(word_pos, tick_bitmap);
                }
            }
        }

        Ok(())
    }

    async fn sync_tick_data<N, P>(
        pools: &mut [AMM],
        pool_manager: Address,
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
                let AMM::UniswapV4Pool(v4_pool) = pool else {
                    return None;
                };

                let min_word = tick_to_word(MIN_TICK, v4_pool.state.tick_spacing);
                let max_word = tick_to_word(MAX_TICK, v4_pool.state.tick_spacing);

                let initialized_ticks: Vec<Signed<24, 1>> = (min_word..=max_word)
                    .filter_map(|word_pos| {
                        v4_pool
                            .state
                            .tick_bitmap
                            .get(&(word_pos as i16))
                            .filter(|&bitmap| *bitmap != U256::ZERO)
                            .map(|&bitmap| (word_pos, bitmap))
                    })
                    .flat_map(|(word_pos, bitmap)| {
                        let tick_spacing = v4_pool.state.tick_spacing;
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
                    Some((v4_pool.pool_id, initialized_ticks))
                }
            })
            .collect::<Vec<(B256, Vec<Signed<24, 1>>)>>();

        let mut futures: FuturesUnordered<BoxFuture<'_, _>> = FuturesUnordered::new();
        let max_ticks = 60;
        let mut group_ticks = 0;
        let mut group: Vec<GetUniswapV4PoolTickDataBatchRequest::TickDataInfo> = vec![];

        for (pool_id, mut ticks) in pool_ticks {
            while !ticks.is_empty() {
                let remaining_ticks = max_ticks - group_ticks;
                let selected_ticks = ticks.drain(0..remaining_ticks.min(ticks.len()));
                group_ticks += selected_ticks.len();

                group.push(GetUniswapV4PoolTickDataBatchRequest::TickDataInfo {
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
                                Vec<GetUniswapV4PoolTickDataBatchRequest::TickDataInfo>,
                                Bytes,
                            ),
                            AMMError,
                        >((
                            calldata.clone(),
                            GetUniswapV4PoolTickDataBatchRequest::deploy_builder(
                                provider,
                                pool_manager,
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
                        Vec<GetUniswapV4PoolTickDataBatchRequest::TickDataInfo>,
                        Bytes,
                    ),
                    AMMError,
                >((
                    calldata.clone(),
                    GetUniswapV4PoolTickDataBatchRequest::deploy_builder(
                        provider,
                        pool_manager,
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
                AMM::UniswapV4Pool(v4_pool) => Some((v4_pool.pool_id, pool)),
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
                let AMM::UniswapV4Pool(v4_pool) = pool else {
                    continue;
                };

                for (tick, tick_idx) in tick_data.iter().zip(tick_info.ticks.iter()) {
                    let info = Info {
                        liquidity_gross: tick.1,
                        liquidity_net: tick.2,
                        initialized: tick.0,
                    };
                    v4_pool.state.ticks.insert(tick_idx.as_i32(), info);
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

impl AutomatedMarketMakerFactory for UniswapV4Factory {
    type PoolVariant = UniswapV4Pool;

    /// V4 has no separate factory contract — the singleton `PoolManager` is the discovery
    /// target, so we report its address.
    fn address(&self) -> Address {
        self.pool_manager
    }

    fn pool_creation_event(&self) -> B256 {
        IUniswapV4PoolManager::Initialize::SIGNATURE_HASH
    }

    /// Decodes an `Initialize` log into an unsynced `UniswapV4Pool`. Per-pool state (slot0,
    /// tickBitmap, ticks) is populated later by `sync_all_pools`.
    fn create_pool(&self, log: Log) -> Result<AMM, AMMError> {
        let init_event = IUniswapV4PoolManager::Initialize::decode_log(&log.inner)?;

        let tick_spacing: i32 = init_event.tickSpacing.unchecked_into();
        let fee: u32 = init_event.fee.to::<u32>();
        let sqrt_price: U256 = U256::from(init_event.sqrtPriceX96);
        let tick: i32 = init_event.tick.unchecked_into();

        Ok(AMM::UniswapV4Pool(UniswapV4Pool {
            pool_id: init_event.id,
            pool_manager: self.pool_manager,
            currency0: init_event.currency0,
            currency0_decimals: 0,
            currency1: init_event.currency1,
            currency1_decimals: 0,
            hooks: init_event.hooks,
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

impl UniswapV4Factory {
    /// Phase 6+: convert a `PoolDescriptor::UniswapV4` into an unsynced `UniswapV4Pool`. PoolKey
    /// fields (currency0/1/fee/tickSpacing/hooks) must be supplied by the descriptor since the
    /// Uniswap V4 `PoolManager` does not expose `poolIdToPoolKey()`.
    pub fn from_descriptor(
        &self,
        desc: &crate::discovery::PoolDescriptor,
    ) -> Result<AMM, AMMError> {
        match desc {
            crate::discovery::PoolDescriptor::UniswapV4 {
                pool_id,
                pool_manager,
                currency0,
                currency1,
                fee,
                tick_spacing,
                hooks,
            } => {
                if *pool_manager != self.pool_manager {
                    return Err(AMMError::DescriptorSingletonMismatch {
                        got: *pool_manager,
                        expected: self.pool_manager,
                    });
                }
                // All PoolKey fields must be resolved by the caller before reaching here.
                // For DexTools-sourced descriptors, that is `enrich_uniswap_v4_via_subgraph`.
                // For Initialize-log auto-track, the factory parses them out of the log itself.
                let currency0 = currency0.ok_or(AMMError::DescriptorMissingPoolKeyField(
                    "currency0",
                ))?;
                let currency1 = currency1.ok_or(AMMError::DescriptorMissingPoolKeyField(
                    "currency1",
                ))?;
                let fee = fee.ok_or(AMMError::DescriptorMissingPoolKeyField("fee"))?;
                let tick_spacing = tick_spacing
                    .ok_or(AMMError::DescriptorMissingPoolKeyField("tick_spacing"))?;
                let hooks = hooks.ok_or(AMMError::DescriptorMissingPoolKeyField("hooks"))?;
                Ok(AMM::UniswapV4Pool(UniswapV4Pool {
                    pool_id: *pool_id,
                    pool_manager: *pool_manager,
                    currency0,
                    currency0_decimals: 0,
                    currency1,
                    currency1_decimals: 0,
                    hooks,
                    state: V4CLState {
                        liquidity: 0,
                        sqrt_price: U256::ZERO,
                        tick: 0,
                        fee,
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

impl DiscoverySync for UniswapV4Factory {
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
            target = "amms::uniswap_v4::discover",
            pool_manager = ?self.pool_manager,
            "Discovering all V4 pools"
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
            target = "amms::uniswap_v4::sync",
            pool_manager = ?self.pool_manager,
            "Syncing all V4 pools"
        );
        UniswapV4Factory::sync_all_pools(amms, self.pool_manager, to_block, provider)
    }
}
