/// This collector listens to a stream of new blocks.
pub mod block_collector;

/// This collector merges multiple collector streams into a unified output stream.
pub mod aggregator;

/// This collector listens to a stream of new event logs.
pub mod log_collector;

/// This collector listens to a stream of new pending transactions.
pub mod mempool_collector;
