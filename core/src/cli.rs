use std::{net::SocketAddr, str::FromStr};

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

    #[arg(long, default_value_t = ("./data/db").to_owned())]
    pub persist_path: String,

    #[arg(env = "TRUSTED_ASSERTIONS_NSEC", long)]
    pub trusted_assertions_nsec: Option<String>,

    #[arg(long, default_value = "127.0.0.1:5551")]
    pub server_addr: TransportAddr,
}
