use crate::types::{Collector, CollectorStream};
use alloy::{
    providers::Provider,
    rpc::types::{Filter, Log},
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio_stream::StreamExt;

pub struct LogCollector<P> {
    provider: Arc<P>,
    filter: Filter,
}

impl<P> LogCollector<P> {
    pub fn new(provider: Arc<P>, filter: Filter) -> Self {
        Self { provider, filter }
    }
}

#[async_trait]
impl<P> Collector<Log> for LogCollector<P>
where
    P: Provider,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Log>> {
        let stream = self
            .provider
            .subscribe_logs(&self.filter)
            .await?
            .into_stream();

        let stream = stream.filter_map(Some);
        Ok(Box::pin(stream))
    }
}
