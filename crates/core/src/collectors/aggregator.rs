use crate::types::{Collector, CollectorReducer, CollectorStream};
use anyhow::Result;
use futures_util::stream::{self, SelectAll};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Collector that merges multiple input collector streams and reduces them into a single output stream.
pub struct AggregatingCollector<EIn, EOut, State, Reducer> {
    collectors: Vec<Box<dyn Collector<EIn>>>,
    state_factory: Arc<dyn Fn() -> State + Send + Sync>,
    reducer_factory: Arc<dyn Fn() -> Reducer + Send + Sync>,
    buffer: usize,
    _phantom: PhantomData<EOut>,
}

impl<EIn, EOut, State, Reducer> AggregatingCollector<EIn, EOut, State, Reducer> {
    pub fn new(
        collectors: Vec<Box<dyn Collector<EIn>>>,
        state_factory: Arc<dyn Fn() -> State + Send + Sync>,
        reducer_factory: Arc<dyn Fn() -> Reducer + Send + Sync>,
        buffer: usize,
    ) -> Self {
        Self {
            collectors,
            state_factory,
            reducer_factory,
            buffer,
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<EIn, EOut, State, Reducer> Collector<EOut> for AggregatingCollector<EIn, EOut, State, Reducer>
where
    EIn: Send + Sync + 'static,
    EOut: Send + Sync + 'static,
    State: Send + 'static,
    Reducer: CollectorReducer<EIn, EOut, State> + Send + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, EOut>> {
        let mut streams: SelectAll<CollectorStream<'_, EIn>> = SelectAll::new();
        for collector in &self.collectors {
            streams.push(collector.get_event_stream().await?);
        }

        let reducer = (self.reducer_factory)();
        let state = (self.state_factory)();
        let out_buf: VecDeque<EOut> = VecDeque::with_capacity(self.buffer);

        let stream = stream::unfold((streams, reducer, state, out_buf), |mut st| async move {
            let (streams, reducer, state, out_buf) = &mut st;
            loop {
                if let Some(out) = out_buf.pop_front() {
                    return Some((out, st));
                }
                match streams.next().await {
                    Some(event) => {
                        let outs = reducer.apply(state, event);
                        out_buf.extend(outs);
                    }
                    None => return None,
                }
            }
        });

        Ok(Box::pin(stream))
    }
}
