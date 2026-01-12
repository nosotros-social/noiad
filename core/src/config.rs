use std::sync::Arc;

use persist::db::PersistStore;

#[derive(Debug, Clone)]
pub struct Config {
    pub worker_id: usize,
    pub worker_count: usize,
    /// Persisted storage for events and interner
    pub persist: Arc<PersistStore>,
    /// Number of PageRank iterations to perform, don't use less than 20 for meaningful results.
    pub pagerank_iterations: usize,
    /// Batch size for saving ranks to the database.
    pub pagerank_sink_batch_size: usize,
    /// Maximum number of pending replication messages before applying backpressure.
    pub replication_max_pending: usize,
}
