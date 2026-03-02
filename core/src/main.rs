use clap::Parser;
use differential_dataflow::operators::CountTotal;
use differential_dataflow::operators::arrange::ArrangeByKey;
use noiad_core::algorithms::pagerank::pagerank;
use noiad_core::dataflow::{DataflowConfig, TimelyConfig, build_dataflow};
use noiad_core::operators::probe::{Handle, ProbeNotify};
use noiad_core::operators::top_k::TopK;
use noiad_core::server::Handler;
use noiad_core::sources::postgres::render;
use noiad_core::state::State;
use noiad_core::worker::Worker;
use noiad_core::{cli::Cli, sources::persist::persist_source::persist_source};
use nostr_sdk::Kind;
use persist::db::PersistStore;
use persist::query::PersistQuery;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use timely::dataflow::operators::Exchange;
use timely::{
    container::CapacityContainerBuilder,
    dataflow::{channels::pact::Pipeline, operators::Operator},
};
use types::edges::EdgeLabel;
use types::types::Node;

use dotenvy::dotenv;

async fn run(cli: Cli) {
    let persist =
        Arc::new(PersistStore::open(&cli.persist_path).expect("Failed to open PersistStore"));
    let timely = TimelyConfig {
        workers: cli.workers,
    };
    let config = DataflowConfig {
        timely,
        persist: persist.clone(),
        pagerank_iterations: cli.pagerank_iterations,
        pagerank_sink_batch_size: cli.pagerank_sink_batch_size,
        replication_max_pending: cli.replication_max_pending,
        trusted_assertions_nsec: cli.trusted_assertions_nsec.clone(),
        trusted_lists_ranks_k: cli.trusted_lists_ranks_k,
    };

    let runtime = tokio::runtime::Handle::current();
    let runtime_handle = runtime.clone();

    let persist_timely = persist.clone();
    let (req_txs, req_rxs, res_txs, res_rxs) = Handler::build_worker_channels(cli.workers);

    let worker_guards = timely::execute(
        timely::Config::process(config.timely.workers),
        move |timely_worker| {
            let _enter = runtime_handle.enter();
            let worker_id = timely_worker.index();
            let req_rx = req_rxs[worker_id].clone();
            let res_tx = res_txs[worker_id].clone();
            let mut state = State::new(persist_timely.clone());
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

    let persist_server = persist.clone();
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
