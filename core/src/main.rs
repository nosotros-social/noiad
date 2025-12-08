use clap::Parser;
use core::algorithms::pagerank::pagerank;
use core::cli::Cli;
use core::config::Config;
use core::event::EdgeLabel;
use core::operators::top_k::TopK;
use core::sources::postgres::render;
use timely::dataflow::operators::Inspect;

use differential_dataflow::operators::CountTotal;
use dotenvy::dotenv;

fn main() {
    tracing_subscriber::fmt().init();
    dotenv().ok();

    let start = std::time::Instant::now();
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

            let events = render(scope, config.clone());

            let edges_labels = events
                .filter(|event| event.kind == 3)
                .flat_map(|e| e.to_edges());

            let pubkey_edges = edges_labels
                .filter(|&(_, _, label)| label == EdgeLabel::Pubkey)
                .map(|(from, to, _)| (from, to));

            let ranks = pagerank(cli.page_rank_iterations, &pubkey_edges).consolidate();

            ranks
                .count_total()
                .map(|(node, rank)| (rank, node))
                .top_k(50)
                .inspect(|((rank, node), _, _)| {
                    tracing::info!("top 50 pubkeys: {:?} {:?}", hex::encode(node), rank)
                });

            edges_labels
                .map(|_| ())
                .count_total()
                .inner
                .inspect(|(((), total), _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("event edges decoded: {total}");
                    }
                });

            events
                .map(|_| ())
                .count_total()
                .inspect(|(((), total), _t, diff)| {
                    if *diff > 0 {
                        tracing::info!("events decoded: {total}");
                    }
                });
        });
    });

    tracing::info!("Completed in {:?}", start.elapsed());
}
