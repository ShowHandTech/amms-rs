//! Public mempool feed: `eth_subscribe newPendingTransactions` with full bodies.
//!
//! Requires a WebSocket endpoint that supports `pubsub` (any geth-flavored full node does).
//! BSC public nodes (publicnode, bsc.nodereal) all accept this; user's own BSC full node is the
//! intended production source. The subscription yields whole `Transaction` objects (no second
//! `getTransactionByHash` round-trip), which is the only thing that makes this latency-tolerable
//! for MEV.

use std::sync::Arc;
use std::time::Instant;

use alloy::consensus::Transaction as ConsensusTransaction;
use alloy::network::TransactionResponse;
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::{MempoolFeed, MempoolTx};

const SOURCE: &str = "public";

pub struct PublicMempoolFeed {
    ws_url: String,
}

impl PublicMempoolFeed {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self { ws_url: ws_url.into() }
    }
}

#[async_trait]
impl MempoolFeed for PublicMempoolFeed {
    fn name(&self) -> &'static str {
        SOURCE
    }

    async fn subscribe(
        self: Arc<Self>,
        out: mpsc::Sender<MempoolTx>,
    ) -> eyre::Result<()> {
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(self.ws_url.clone()))
            .await?;
        info!(target: "mev::mempool::public", ws = %self.ws_url, "subscribed to newPendingTransactions (full bodies)");

        let sub = provider.subscribe_full_pending_transactions().await?;
        let mut stream = sub.into_stream();
        while let Some(tx) = stream.next().await {
            let received_at = Instant::now();
            let envelope = tx.inner.inner();
            let tx_hash = Some(*envelope.tx_hash());
            let from = Some(tx.from());
            let to = envelope.to();
            let value = envelope.value();
            let input = envelope.input().clone();
            let gas_price = envelope.gas_price().map(U256::from);
            let max_fee_per_gas = Some(U256::from(envelope.max_fee_per_gas()));
            let max_priority_fee_per_gas =
                envelope.max_priority_fee_per_gas().map(U256::from);

            let normalized = MempoolTx {
                source: SOURCE,
                tx_hash,
                from,
                to,
                value,
                input,
                gas_price,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                received_at,
            };

            debug!(
                target: "mev::mempool::public",
                tx_hash = ?normalized.tx_hash,
                to = ?normalized.to,
                input_len = normalized.input.len(),
                "pending tx",
            );

            if out.send(normalized).await.is_err() {
                warn!(target: "mev::mempool::public", "downstream channel closed; ending subscription");
                break;
            }
        }
        Ok(())
    }
}
