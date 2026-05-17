use super::{
    balancer::BalancerPool, erc_4626::ERC4626Vault, error::AMMError,
    pancake_v4_cl::PancakeV4CLPool, uniswap_v2::UniswapV2Pool, uniswap_v3::UniswapV3Pool,
    uniswap_v4::UniswapV4Pool,
};
use alloy::{
    eips::BlockId,
    network::Network,
    primitives::{Address, B256, U256},
    providers::Provider,
    rpc::types::Log,
};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Stable identity for an AMM instance.
///
/// Pre-V4 pools each have their own contract address; V4-family pools live inside a singleton
/// (Uniswap V4 `PoolManager`, PCS Infinity `CLPoolManager`) and are keyed by `PoolId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AmmId {
    Address(Address),
    V4 { singleton: Address, pool_id: B256 },
}

impl AmmId {
    /// Returns the contract address if this AMM owns one (V2/V3/Balancer/ERC4626).
    /// V4 pools return `None` because they share a singleton.
    pub fn as_address(&self) -> Option<Address> {
        match self {
            AmmId::Address(a) => Some(*a),
            AmmId::V4 { .. } => None,
        }
    }
}

impl From<Address> for AmmId {
    fn from(a: Address) -> Self {
        AmmId::Address(a)
    }
}

#[allow(async_fn_in_trait)]
pub trait AutomatedMarketMaker {
    /// Stable identity for this AMM. Replaces the old `address()` method since V4 pools have no
    /// dedicated contract address.
    fn id(&self) -> AmmId;

    /// Event signatures that indicate when the AMM should be synced
    fn sync_events(&self) -> Vec<B256>;

    /// Syncs the AMM state
    fn sync(&mut self, log: &Log) -> Result<(), AMMError>;

    /// Returns a list of token addresses used in the AMM
    fn tokens(&self) -> Vec<Address>;

    /// Calculates the price of `base_token` in terms of `quote_token`
    fn calculate_price(&self, base_token: Address, quote_token: Address) -> Result<f64, AMMError>;

    /// Simulate a swap
    /// Returns the amount_out in `quote token` for a given `amount_in` of `base_token`
    fn simulate_swap(
        &self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError>;

    /// Simulate a swap, mutating the AMM state
    /// Returns the amount_out in `quote token` for a given `amount_in` of `base_token`
    fn simulate_swap_mut(
        &mut self,
        base_token: Address,
        quote_token: Address,
        amount_in: U256,
    ) -> Result<U256, AMMError>;

    // Initializes an empty pool and syncs state up to `block_number`
    async fn init<N, P>(self, block_number: BlockId, provider: P) -> Result<Self, AMMError>
    where
        Self: Sized,
        N: Network,
        P: Provider<N> + Clone;
}

macro_rules! amm {
    ($($pool_type:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub enum AMM {
            $($pool_type($pool_type),)+
        }

        impl AutomatedMarketMaker for AMM {
            fn id(&self) -> AmmId {
                match self {
                    $(AMM::$pool_type(pool) => pool.id(),)+
                }
            }

            fn sync_events(&self) -> Vec<B256> {
                match self {
                    $(AMM::$pool_type(pool) => pool.sync_events(),)+
                }
            }

            fn sync(&mut self, log: &Log) -> Result<(), AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.sync(log),)+
                }
            }

            fn simulate_swap(&self, base_token: Address, quote_token: Address,amount_in: U256) -> Result<U256, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.simulate_swap(base_token, quote_token, amount_in),)+
                }
            }

            fn simulate_swap_mut(&mut self, base_token: Address, quote_token: Address, amount_in: U256) -> Result<U256, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.simulate_swap_mut(base_token, quote_token, amount_in),)+
                }
            }

            fn tokens(&self) -> Vec<Address> {
                match self {
                    $(AMM::$pool_type(pool) => pool.tokens(),)+
                }
            }

            fn calculate_price(&self, base_token: Address, quote_token: Address) -> Result<f64, AMMError> {
                match self {
                    $(AMM::$pool_type(pool) => pool.calculate_price(base_token, quote_token),)+
                }
            }

            async fn init<N, P>(self, block_number: BlockId, provider: P) -> Result<Self, AMMError>
            where
                Self: Sized,
                N: Network,
                P: Provider<N> + Clone,
            {
                match self {
                    $(AMM::$pool_type(pool) => pool.init(block_number, provider).await.map(AMM::$pool_type),)+
                }
            }
        }


        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum Variant {
            $($pool_type,)+
        }

        impl AMM {
            pub fn variant(&self) -> Variant {
                match self {
                    $(AMM::$pool_type(_) => Variant::$pool_type,)+
                }
            }
        }

        impl Hash for AMM {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.id().hash(state);
            }
        }

        impl PartialEq for AMM {
            fn eq(&self, other: &Self) -> bool {
                self.id() == other.id()
            }
        }

        impl Eq for AMM {}

        $(
            impl From<$pool_type> for AMM {
                fn from(amm: $pool_type) -> Self {
                    AMM::$pool_type(amm)
                }
            }
        )+
    };
}

amm!(
    UniswapV2Pool,
    UniswapV3Pool,
    ERC4626Vault,
    BalancerPool,
    UniswapV4Pool,
    PancakeV4CLPool,
);
