use clap::Parser;
use transport::socket::TransportAddr;

#[derive(Parser, Debug)]
#[command(name = "Nostr stream data processing", version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value_t = 4)]
    pub workers: usize,

    #[arg(long, default_value_t = 30)]
    pub pagerank_iterations: usize,

    #[arg(long, default_value_t = 250_000)]
    pub pagerank_sink_batch_size: usize,

    #[arg(long, default_value_t = 500)]
    pub trusted_lists_ranks_k: usize,

    /// Maximum number of pending replication messages before applying backpressure.
    #[arg(long, default_value_t = 10_000)]
    pub replication_max_pending: usize,

    /// Disable Postgres logical replication after snapshot/checkpoint startup.
    #[arg(long, default_value_t = false)]
    pub no_replication: bool,

    #[arg(long, default_value_t = ("./data/db").to_owned())]
    pub persist_path: String,

    #[arg(env = "TRUSTED_ASSERTIONS_NSEC", long)]
    pub trusted_assertions_nsec: Option<String>,

    #[arg(env = "EMBEDDING_IMPORT_MANIFEST", long)]
    pub embedding_import_manifest: Option<String>,

    /// Enable parquet dataset export.
    #[arg(long, default_value_t = false)]
    pub dataset_sink: bool,

    /// Recompute locally generated events, such as trusted assertions and embeddings.
    #[arg(long, default_value_t = false)]
    pub recompute: bool,

    #[arg(env = "TRUSTED_SEEDS", long, value_delimiter = '\n')]
    pub trusted_seeds: Vec<String>,

    #[arg(long, default_value = "127.0.0.1:5551")]
    pub server_addr: TransportAddr,
}
