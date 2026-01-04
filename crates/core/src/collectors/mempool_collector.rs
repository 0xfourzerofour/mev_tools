use alloy::{primitives::FixedBytes, providers::Provider};
use async_trait::async_trait;
use std::sync::Arc;

use crate::types::{Collector, CollectorStream};
use anyhow::Result;

pub struct MempoolCollector<P> {
    provider: Arc<P>,
}

impl<P> MempoolCollector<P> {
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> Collector<FixedBytes<32>> for MempoolCollector<P>
where
    P: Provider,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, FixedBytes<32>>> {
        let stream = self
            .provider
            .subscribe_pending_transactions()
            .channel_size(256)
            .await?
            .into_stream();

        Ok(Box::pin(stream))
    }
}
