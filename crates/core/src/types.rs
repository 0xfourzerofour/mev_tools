use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

/// A stream of events emitted by a [Collector](Collector).
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

/// Collector trait, which defines a source of events.
#[async_trait]
pub trait Collector<E>: Send + Sync {
    /// Returns the core event stream for the collector.
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

/// Strategy trait, which defines the core logic for each opportunity.
///
/// # Type Parameters
/// * `E` - Collector event type (external data from collectors)
/// * `C` - Executor callback type (execution feedback from executors)
/// * `A` - Action type
#[async_trait]
pub trait Strategy<E, C, A>: Send + Sync
where
    E: Send + 'static,
    C: Send + 'static,
{
    /// Sync the initial state of the strategy if needed, usually by fetching
    /// onchain data.
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an external event from collectors (market data, price updates, etc).
    async fn process_event(&mut self, event: E) -> Vec<A>;

    /// Process an execution callback from executors (order filled, failed, etc).
    ///
    /// Default implementation ignores callbacks. Override to handle execution feedback.
    async fn process_callback(&mut self, callback: C) -> Vec<A> {
        let _ = callback;
        vec![]
    }
}

/// Channels provided to executors for bidirectional communication
pub struct ExecutorChannels<A, C> {
    /// Receiver for actions from strategies
    pub actions: tokio::sync::broadcast::Receiver<A>,
    /// Sender for callbacks back to strategies
    pub callbacks: tokio::sync::broadcast::Sender<C>,
}

/// Executor trait, responsible for executing actions returned by strategies.
///
/// The executor receives actions from strategies and returns callbacks (execution feedback).
///
/// # Type Parameters
/// * `A` - Action type
/// * `C` - Callback type (execution feedback like OrderFilled, OrderFailed, etc)
///
/// # Implementation Modes
///
/// **Simple mode (no feedback):**
/// ```ignore
/// impl Executor<Action, Callback> for MyExecutor {
///     async fn execute(&self, action: Action) -> Result<Vec<Callback>> {
///         // Execute action, return empty vec if no callbacks needed
///         do_work(action).await?;
///         Ok(vec![])
///     }
/// }
/// ```
///
/// **With feedback:**
/// ```ignore
/// impl Executor<Action, Callback> for MyExecutor {
///     async fn execute(&self, action: Action) -> Result<Vec<Callback>> {
///         let result = do_work(action).await?;
///
///         // Return callbacks for strategy to process
///         Ok(vec![Callback::ExecutionComplete { ... }])
///     }
/// }
/// ```
#[async_trait]
pub trait Executor<A, C>: Send + Sync
where
    A: Send + 'static,
{
    /// Execute a single action and return callbacks to emit.
    ///
    /// Return a Vec of callbacks that should be sent back to strategies.
    /// The engine will handle broadcasting these callbacks automatically.
    async fn execute(&self, action: A) -> Result<Vec<C>> {
        let _ = action;
        panic!("Executor must implement execute()");
    }

    /// Run the executor with bidirectional channels (advanced mode - with callback feedback).
    ///
    /// This gives the executor full control over its lifecycle and allows emitting
    /// callbacks back to strategies.
    ///
    /// Default implementation calls `execute()` in a loop and sends returned callbacks.
    async fn run(mut self: Box<Self>, mut channels: ExecutorChannels<A, C>) -> Result<()>
    where
        A: Send + Clone + 'static,
        C: Send + Clone + 'static,
    {
        loop {
            match channels.actions.recv().await {
                Ok(action) => {
                    match self.execute(action).await {
                        Ok(callbacks) => {
                            // Send all returned callbacks back to strategies
                            for callback in callbacks {
                                if let Err(e) = channels.callbacks.send(callback) {
                                    tracing::error!("Error sending callback: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error executing action: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error receiving action: {}", e);
                    break Ok(());
                }
            }
        }
    }
}

/// CollectorMap is a wrapper around a [Collector](Collector) that maps outgoing
/// events to a different type.
pub struct CollectorMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}
impl<E, F> CollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.map(f);
        Ok(Box::pin(stream))
    }
}

/// ExecutorMap is a wrapper around an [Executor](Executor) that maps incoming
/// actions to a different type.
pub struct ExecutorMap<A, C, F> {
    executor: Box<dyn Executor<A, C>>,
    f: F,
}

impl<A, C, F> ExecutorMap<A, C, F> {
    pub fn new(executor: Box<dyn Executor<A, C>>, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A1, A2, C, F> Executor<A1, C> for ExecutorMap<A2, C, F>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    C: Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + Clone + 'static,
{
    async fn execute(&self, action: A1) -> Result<Vec<C>> {
        let action = (self.f)(action);
        match action {
            Some(action) => self.executor.execute(action).await,
            None => Ok(vec![]),
        }
    }
}

/// Strategy configuration entry from TOML config file.
///
/// Example TOML:
/// ```toml
/// [[strategies]]
/// strategy_name = "kelly"
/// enabled = true
///
/// [strategies.args]
/// min_price_threshold = 0.95
/// assumed_edge = 0.01
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrategyEntry {
    pub strategy_name: String,
    pub enabled: bool,
    #[serde(default)]
    pub args: toml::Table,
}

/// A factory function that creates a strategy from TOML args.
///
/// The context `Ctx` allows passing shared dependencies (e.g., Arc<Config>, providers).
pub type StrategyFactoryFn<E, C, A, Ctx> =
    Arc<dyn Fn(&toml::Table, &Ctx) -> Result<Box<dyn Strategy<E, C, A>>> + Send + Sync>;

/// Registry for strategy factories.
///
/// Register strategy factories by name, then use `create_strategy` to instantiate
/// strategies from config entries.
pub struct StrategyRegistry<E, C, A, Ctx> {
    factories: HashMap<String, StrategyFactoryFn<E, C, A, Ctx>>,
}

impl<E, C, A, Ctx> StrategyRegistry<E, C, A, Ctx> {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
}

impl<E, C, A, Ctx> Default for StrategyRegistry<E, C, A, Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, C, A, Ctx> StrategyRegistry<E, C, A, Ctx>
where
    E: Send + 'static,
    C: Send + 'static,
    A: 'static,
{
    /// Register a strategy factory by name with explicit args type.
    ///
    /// The args type `Args` is automatically deserialized from the TOML config
    /// when the strategy is created, making the relationship between strategy
    /// name and its config type explicit.
    ///
    /// Example:
    /// ```ignore
    /// registry.register::<KellyArgs>("kelly", |args, ctx| {
    ///     Ok(Box::new(KellyStrategy::new(args)))
    /// });
    /// ```
    pub fn register<Args, F>(&mut self, name: impl Into<String>, factory: F)
    where
        Args: serde::de::DeserializeOwned + Default + 'static,
        F: Fn(Args, &Ctx) -> Result<Box<dyn Strategy<E, C, A>>> + Send + Sync + 'static,
    {
        let wrapped = move |table: &toml::Table, ctx: &Ctx| -> Result<Box<dyn Strategy<E, C, A>>> {
            let args: Args = if table.is_empty() {
                Args::default()
            } else {
                toml::Value::Table(table.clone())
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to parse strategy args: {}", e))?
            };
            factory(args, ctx)
        };
        self.factories.insert(name.into(), Arc::new(wrapped));
    }

    /// Create a strategy from a config entry.
    pub fn create_strategy(
        &self,
        entry: &StrategyEntry,
        ctx: &Ctx,
    ) -> Result<Box<dyn Strategy<E, C, A>>> {
        let factory = self
            .factories
            .get(&entry.strategy_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", entry.strategy_name))?;

        factory(&entry.args, ctx)
    }

    /// Create all strategies from a list of config entries.
    pub fn create_strategies(
        &self,
        entries: &[StrategyEntry],
        ctx: &Ctx,
    ) -> Result<Vec<Box<dyn Strategy<E, C, A>>>> {
        entries
            .iter()
            .map(|entry| self.create_strategy(entry, ctx))
            .collect()
    }

    /// Check if a strategy is registered.
    pub fn has_strategy(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    /// Get list of registered strategy names.
    pub fn registered_strategies(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }
}

/// Helper trait for deserializing strategy args from TOML table.
pub trait FromTomlArgs: Sized {
    fn from_toml_args(args: &toml::Table) -> Result<Self>;
}

impl<T: DeserializeOwned + Default> FromTomlArgs for T {
    fn from_toml_args(args: &toml::Table) -> Result<Self> {
        if args.is_empty() {
            return Ok(T::default());
        }
        let value = toml::Value::Table(args.clone());
        value
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to parse strategy args: {}", e))
    }
}
