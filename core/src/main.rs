use clap::Parser;
use jemallocator::Jemalloc;
use noiad_core::cli::Cli;
use noiad_core::dataflow::{DataflowConfig, TimelyConfig, build_dataflow};
use noiad_core::server::Handler;
use noiad_core::state::State;
use noiad_core::worker::Worker;
use persist::db::PersistStore;
use std::sync::Arc;
use types::types::Node;

use dotenvy::dotenv;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

async fn run(cli: Cli) {
    let persist =
        Arc::new(PersistStore::open(&cli.persist_path).expect("Failed to open PersistStore"));
    let trusted_seeds: Vec<Node> = cli
        .trusted_seeds
        .iter()
        .map(|seed| {
            let bytes = hex::decode(seed).expect("trusted seed must be hex");
            persist
                .intern(&bytes)
                .unwrap_or_else(|_| panic!("failed to intern trusted seed: {}", seed))
        })
        .collect();
    let timely = TimelyConfig {
        workers: cli.workers,
    };
    let config = DataflowConfig {
        timely,
        persist: persist.clone(),
        persist_path: cli.persist_path.clone(),
        pagerank_iterations: cli.pagerank_iterations,
        pagerank_sink_batch_size: cli.pagerank_sink_batch_size,
        replication_max_pending: cli.replication_max_pending,
        no_replication: cli.no_replication,
        trusted_assertions_nsec: cli.trusted_assertions_nsec.clone(),
        embedding_import_manifest: cli.embedding_import_manifest.clone(),
        dataset_sink_path: cli.dataset_sink.then(|| {
            format!(
                "data/dataset/{}",
                chrono::Local::now().format("%Y-%m-%d_%H-%M-%S")
            )
        }),
        recompute: cli.recompute,
        trusted_seeds,
        trusted_lists_ranks_k: cli.trusted_lists_ranks_k,
    };

    let runtime = tokio::runtime::Handle::current();
    let runtime_handle = runtime.clone();

    let persist_timely = persist.clone();
    let (req_txs, req_rxs, res_txs, res_rxs) = Handler::build_worker_channels(cli.workers);

    let _worker_guards = timely::execute(
        timely::Config::process(config.timely.workers),
        move |timely_worker| {
            let _enter = runtime_handle.enter();
            let worker_id = timely_worker.index();
            let req_rx = req_rxs[worker_id].clone();
            let res_tx = res_txs[worker_id].clone();
            let state = State::new(persist_timely.clone());
            let mut worker = Worker {
                worker_id,
                timely_worker,
                state,
                req_rx,
                res_tx,
            };
            build_dataflow(&mut worker, config.clone());
            worker.run();
        },
    );

    tracing::info!("[Main] Timely workers started");

    let server_handler = move || Handler::new(Arc::clone(&req_txs), Arc::clone(&res_rxs));

    transport::server::run_server(cli.server_addr, server_handler)
        .await
        .expect("Server failed");
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_file(false)
        .with_line_number(false)
        .init();

    dotenv().ok();

    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.workers)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(run(cli));
}
