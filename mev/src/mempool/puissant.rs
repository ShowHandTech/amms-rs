//! Puissant (48 Club) feed stub.
//!
//! 48 Club's Puissant relay delivers a `pending tx`-shaped stream that often **redacts the
//! original tx_hash and from address** (anti-frontrun protection for users routing through
//! their endpoint). In some modes it also delivers a log-shaped event rather than full
//! calldata — the decoder will need a `PuissantLog -> MempoolTx` translation layer once the
//! real message format is sampled.
//!
//! Phase 11 deliberately leaves this as a stub: it logs a warning and returns immediately so
//! `main` can `tokio::spawn` it without crashing. Phase 11+1 (after a real sample is captured)
//! fills in the parser. Until then the public mempool feed carries the load.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;

use super::{MempoolFeed, MempoolTx};

const SOURCE: &str = "puissant";

pub struct PuissantFeed {
    /// Endpoint URL (WS) once we have the real one. Held but unused by the stub.
    #[allow(dead_code)]
    pub endpoint: Option<String>,
}

impl PuissantFeed {
    pub fn new(endpoint: Option<String>) -> Self {
        Self { endpoint }
    }
}

#[async_trait]
impl MempoolFeed for PuissantFeed {
    fn name(&self) -> &'static str {
        SOURCE
    }

    async fn subscribe(
        self: Arc<Self>,
        _out: mpsc::Sender<MempoolTx>,
    ) -> eyre::Result<()> {
        warn!(
            target: "mev::mempool::puissant",
            "puissant feed not yet implemented — needs a real message sample to define the parser; \
             see plan Open Item #1. Stub exits immediately."
        );
        Ok(())
    }
}
