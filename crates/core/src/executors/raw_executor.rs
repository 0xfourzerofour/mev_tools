use std::sync::Arc;

use crate::types::Executor;
use alloy::{consensus::TypedTransaction, primitives::U256, providers::Provider};
use anyhow::Result;
use async_trait::async_trait;

pub struct MempoolExecutor<P> {
    _client: Arc<P>,
}

#[derive(Debug, Clone)]
pub struct GasBidInfo {
    pub total_profit: U256,
    pub bid_percentage: u64,
}

#[derive(Debug, Clone)]
pub struct SubmitTxToMempool {
    pub tx: TypedTransaction,
    pub gas_bid_info: Option<GasBidInfo>,
}

impl<P: Provider> MempoolExecutor<P> {
    pub fn new(client: Arc<P>) -> Self {
        Self { _client: client }
    }
}

#[async_trait]
impl<P, C> Executor<SubmitTxToMempool, C> for MempoolExecutor<P>
where
    P: Provider,
    C: Send + Sync,
{
    async fn execute(&self, mut _action: SubmitTxToMempool) -> Result<Vec<C>> {
        // Send tx to public mempool
        // Return empty vec since this executor doesn't emit callbacks
        Ok(vec![])
    }
}
