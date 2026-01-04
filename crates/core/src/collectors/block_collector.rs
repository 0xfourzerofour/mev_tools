use crate::types::{Collector, CollectorStream};
use alloy::{primitives::BlockHash, providers::Provider};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio_stream::StreamExt;

pub struct BlockCollector<P> {
    provider: Arc<P>,
}

#[derive(Debug, Clone)]
pub struct NewBlock {
    pub hash: BlockHash,
    pub number: u64,
}

impl<P> BlockCollector<P> {
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> Collector<NewBlock> for BlockCollector<P>
where
    P: Provider,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let stream = self.provider.subscribe_blocks().await?.into_stream();
        let stream = stream.filter_map(|block| {
            Some(NewBlock {
                hash: block.hash,
                number: block.number,
            })
        });

        Ok(Box::pin(stream))
    }
}
