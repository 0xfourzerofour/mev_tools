use anyhow::Result;
use tokio::sync::broadcast::{self, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::types::{Collector, Executor, ExecutorChannels, Strategy, StrategyEntry, StrategyRegistry};

/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
///
/// # Type Parameters
/// * `E` - Collector event type (external data)
/// * `C` - Executor callback type (execution feedback)
/// * `A` - Action type
pub struct Engine<E, C, A> {
    /// The set of collectors that the engine will use to collect events.
    collectors: Vec<Box<dyn Collector<E>>>,

    /// The set of strategies that the engine will use to process events and callbacks.
    strategies: Vec<Box<dyn Strategy<E, C, A>>>,

    /// The set of executors that the engine will use to execute actions.
    executors: Vec<Box<dyn Executor<A, C>>>,

    /// The capacity of the event channel (for collector events).
    event_channel_capacity: usize,

    /// The capacity of the callback channel (for executor callbacks).
    callback_channel_capacity: usize,

    /// The capacity of the action channel.
    action_channel_capacity: usize,
}

impl<E, C, A> Engine<E, C, A> {
    pub fn new() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            callback_channel_capacity: 512,
            action_channel_capacity: 512,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_callback_channel_capacity(mut self, capacity: usize) -> Self {
        self.callback_channel_capacity = capacity;
        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }
}

impl<E, C, A> Default for Engine<E, C, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, C, A> Engine<E, C, A>
where
    E: Send + Clone + 'static + std::fmt::Debug,
    C: Send + Clone + 'static + std::fmt::Debug,
    A: Send + Clone + 'static + std::fmt::Debug,
{
    /// Adds a collector to be used by the engine.
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    /// Adds a strategy to be used by the engine.
    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, C, A>>) {
        self.strategies.push(strategy);
    }

    /// Adds an executor to be used by the engine.
    pub fn add_executor(&mut self, executor: Box<dyn Executor<A, C>>) {
        self.executors.push(executor);
    }

    /// Load strategies from config entries using a registry.
    ///
    /// This method creates strategies from TOML config entries using the provided
    /// registry and context. Only strategies with `enabled = true` are loaded.
    ///
    /// # Example
    /// ```ignore
    /// let mut registry = StrategyRegistry::new();
    /// registry.register::<KellyArgs, _>("kelly", |args, ctx| {
    ///     Ok(Box::new(KellyStrategy::new(args)))
    /// });
    ///
    /// engine.load_strategies(&entries, &registry, &ctx)?;
    /// ```
    pub fn load_strategies<Ctx>(
        &mut self,
        entries: &[StrategyEntry],
        registry: &StrategyRegistry<E, C, A, Ctx>,
        ctx: &Ctx,
    ) -> Result<()> {
        for entry in entries {
            if !entry.enabled {
                info!("Skipping disabled strategy: {}", entry.strategy_name);
                continue;
            }
            info!("Loading strategy: {}", entry.strategy_name);
            let strategy = registry.create_strategy(entry, ctx)?;
            self.strategies.push(strategy);
        }
        Ok(())
    }

    /// The core run loop of the engine. This function will spawn a thread for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    ///
    /// Collectors emit events (E) which are sent to strategies via process_event().
    /// Executors emit callbacks (C) which are sent to strategies via process_callback().
    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error>> {
        // Create separate channels for events (from collectors) and callbacks (from executors)
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (callback_sender, _): (Sender<C>, _) = broadcast::channel(self.callback_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let mut set = JoinSet::new();

        // Spawn executors in separate threads with bidirectional channels.
        for executor in self.executors {
            let channels = ExecutorChannels {
                actions: action_sender.subscribe(),
                callbacks: callback_sender.clone(),
            };

            set.spawn(async move {
                info!("starting executor... ");
                match executor.run(channels).await {
                    Ok(_) => info!("executor finished successfully"),
                    Err(e) => error!("executor error: {}", e),
                }
            });
        }

        // Spawn strategies in separate threads.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let mut callback_receiver = callback_sender.subscribe();
            let action_sender = action_sender.clone();
            strategy.sync_state().await?;

            set.spawn(async move {
                info!("starting strategy... ");
                loop {
                    tokio::select! {
                        // Process collector events
                        event_result = event_receiver.recv() => {
                            match event_result {
                                Ok(event) => {
                                    for action in strategy.process_event(event).await {
                                        match action_sender.send(action) {
                                            Ok(_) => {}
                                            Err(e) => error!("error sending action: {}", e),
                                        }
                                    }
                                }
                                Err(e) => error!("error receiving event: {}", e),
                            }
                        }

                        // Process executor callbacks
                        callback_result = callback_receiver.recv() => {
                            match callback_result {
                                Ok(callback) => {
                                    for action in strategy.process_callback(callback).await {
                                        match action_sender.send(action) {
                                            Ok(_) => {}
                                            Err(e) => error!("error sending action: {}", e),
                                        }
                                    }
                                }
                                Err(e) => error!("error receiving callback: {}", e),
                            }
                        }
                    }
                }
            });
        }

        // Spawn collectors in separate threads.
        for collector in self.collectors {
            let event_sender = event_sender.clone();
            set.spawn(async move {
                info!("starting collector... ");
                let mut event_stream = collector.get_event_stream().await.unwrap();
                while let Some(event) = event_stream.next().await {
                    match event_sender.send(event) {
                        Ok(_) => {}
                        Err(e) => error!("error sending event: {}", e),
                    }
                }
            });
        }

        Ok(set)
    }
}
