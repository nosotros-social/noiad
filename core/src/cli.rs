use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "Nostr stream data processing", version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value_t = 4)]
    pub workers: usize,

    #[arg(long, default_value_t = 20)]
    pub pagerank_iterations: usize,

    #[arg(long, default_value_t = 250_000)]
    pub pagerank_sink_batch_size: usize,

    /// Maximum number of pending replication messages before applying backpressure.
    #[arg(long, default_value_t = 10_000)]
    pub replication_max_pending: usize,

    #[arg(long, default_value_t = ("./data/db").to_owned())]
    pub persist_path: String,

    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub max_total_wal_size: u64,
}
