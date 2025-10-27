use clap::Parser;

use dotenvy::dotenv;

use differential_dataflow::operators::CountTotal;
use timely::dataflow::operators::Inspect;

use crate::cli::Cli;
use crate::config::Config;
use crate::sources::postgres::render;

pub mod cli;
pub mod config;
pub mod errors;
pub mod operators;
pub mod sources;

fn main() {
    tracing_subscriber::fmt().with_line_number(true).init();
    dotenv().ok();

    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.workers)
        .enable_all()
        .build()
        .unwrap();

    let runtime_handle = runtime.handle().clone();

    let _ = timely::execute(timely::Config::process(cli.workers), move |worker| {
        let _enter = runtime_handle.enter();

        let index = worker.index();

        tracing::info!("Worker {index} started");

        worker.dataflow::<u64, _, _>(|scope| {
            let config = Config {
                worker_id: scope.index(),
                worker_count: scope.peers(),
            };
            let snapshot_updates = render(scope.clone(), config);

            let total = snapshot_updates.map(|_| ()).count_total();
            total.inner.inspect(|(((), total), _t, diff)| {
                if *diff > 0 {
                    tracing::info!("events decoded: {total}");
                }
            });
        });
    });

    let _ = runtime.block_on(async { tokio::signal::ctrl_c().await });
}
