use anyhow::Error;
use differential_dataflow::{AsCollection, Data, VecCollection};
use futures::StreamExt;
use persist::query::PersistQuery;
use timely::Container;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, StreamCore};
use types::{event::EventRow, types::Diff};

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::operators::probe::Handle;

pub fn persist_source<G, D>(
    scope: &G,
    config: DataflowConfig,
    query: PersistQuery,
    start_stream: &StreamCore<G, D>,
    probe: &Handle<u64>,
) -> (VecCollection<G, EventRow, Diff>, StreamCore<G, Vec<u64>>)
where
    G: Scope<Timestamp = u64>,
    D: Container + Data,
{
    let mut builder = AsyncOperatorBuilder::new("PersistSource".to_string(), scope.clone());

    let mut signal_input = builder.new_disconnected_input(start_stream, Pipeline);

    let (event_handle, event_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(EventRow, u64, Diff)>>>();
    let (bootstrap_handle, bootstrap_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

    let probe = probe.clone();
    let worker_id = scope.index();
    let workers = scope.peers();
    let batch_size = 10_000_000;
    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            let [event_cap, bootstrap_cap]: &mut [_; 2] = capabilities.try_into().unwrap();

            while let Some(event) = signal_input.next().await {
                if let Event::Progress(frontier) = event
                    && !frontier.less_equal(&0)
                {
                    break;
                }
            }

            let mut subscription = config.persist.subscribe(worker_id, workers);

            let mut counter = 0usize;
            let mut ts = 0u64;
            let started_at = std::time::Instant::now();
            for event in config
                .persist
                .iter_events_for_worker(query.clone(), worker_id, workers)
            {
                counter += 1;
                let diff = 1;
                event_handle.give(&event_cap[0], (event, ts, diff));
                if counter.is_multiple_of(batch_size) {
                    ts += 1;
                    event_cap.downgrade([ts]);
                }
                if counter.is_multiple_of(5000) {
                    tokio::task::yield_now().await;
                }
            }
            tracing::info!(
                "[Worker {}] persisted source emitted {} events in {:?}",
                worker_id,
                counter,
                started_at.elapsed()
            );

            let bootstrap_frontier = ts + 1;
            event_cap.downgrade([bootstrap_frontier]);

            while let Ok((event, _ts, diff)) = subscription.try_recv() {
                if !query.matches_kind(event.kind) {
                    continue;
                }

                event_handle.give(
                    &event_cap[0],
                    (
                        EventRow {
                            id: event.id,
                            pubkey: event.pubkey,
                            kind: event.kind,
                            created_at: event.created_at,
                            edges: event.tags,
                        },
                        bootstrap_frontier,
                        diff as Diff,
                    ),
                );
            }

            bootstrap_handle.give(&bootstrap_cap[0], bootstrap_frontier);
            bootstrap_cap.downgrade(std::iter::empty::<u64>());

            let mut live_output_ts = bootstrap_frontier;

            while let Some((event, ts, diff)) = subscription.recv().await {
                if !query.matches_kind(event.kind) {
                    continue;
                }

                let output_ts = live_output_ts.max(ts);
                let d = diff as Diff;
                event_handle.give(
                    &event_cap[0],
                    (
                        EventRow {
                            id: event.id,
                            pubkey: event.pubkey,
                            kind: event.kind,
                            created_at: event.created_at,
                            edges: event.tags,
                        },
                        output_ts,
                        d,
                    ),
                );

                if subscription.is_empty() {
                    let closed_through = output_ts;
                    let new_frontier = closed_through.saturating_add(1);
                    live_output_ts = new_frontier;
                    event_cap.downgrade([new_frontier]);

                    while probe.with_frontier(|f| f.less_equal(&closed_through)) {
                        probe.progressed().await;
                    }
                }
            }

            Ok::<(), Error>(())
        })
    });

    (event_stream.as_collection(), bootstrap_stream)
}
