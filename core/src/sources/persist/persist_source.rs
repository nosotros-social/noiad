use anyhow::Error;
use differential_dataflow::{AsCollection, Data, VecCollection};
use futures::StreamExt;
use persist::event::EventRecord;
use persist::iter::PersistQueryIter;
use timely::Container;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, StreamCore};
use tokio::sync::broadcast::error::RecvError;

use crate::config::Config;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::operators::probe::Handle;
use crate::types::Diff;

pub fn persist_source<G, D, Q>(
    scope: &G,
    config: Config,
    query: Q,
    start_stream: &StreamCore<G, D>,
    probe: &Handle<u64>,
) -> VecCollection<G, EventRecord, Diff>
where
    G: Scope<Timestamp = u64>,
    D: Container + Data,
    Q: PersistQueryIter + Clone + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("PersistSource".to_string(), scope.clone());

    let mut signal_input = builder.new_disconnected_input(start_stream, Pipeline);

    let (event_handle, event_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(EventRecord, u64, Diff)>>>();

    let probe = probe.clone();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            if config.worker_id != 0 {
                return Ok(());
            }
            let [event_cap]: &mut [_; 1] = capabilities.try_into().unwrap();

            while let Some(event) = signal_input.next().await {
                if let Event::Progress(frontier) = event
                    && !frontier.less_equal(&0)
                {
                    break;
                }
            }

            let mut counter = 0usize;
            let started_at = std::time::Instant::now();
            for event in query.clone().iter(&config.persist) {
                counter += 1;
                event_handle.give(&event_cap[0], (event, 0, 1));
                if counter.is_multiple_of(5000) {
                    tokio::task::yield_now().await;
                }
            }
            tracing::info!(
                "[Worker {}] persisted source emitted {} events in {:?}",
                config.worker_id,
                counter,
                started_at.elapsed()
            );

            event_cap.downgrade([1]);

            let mut subscription = config.persist.subscribe();
            let mut max_ts = 0u64;

            loop {
                match subscription.recv().await {
                    Ok((event, ts, diff)) => {
                        if !query.matches(&event) {
                            continue;
                        }

                        max_ts = max_ts.max(ts);
                        event_handle.give(&event_cap[0], (event, ts, diff as Diff));

                        if subscription.is_empty() {
                            tracing::info!(
                                "[Worker {}] persist_source advancing frontier to {}",
                                config.worker_id,
                                max_ts + 1
                            );
                            event_cap.downgrade([max_ts + 1]);

                            while probe.with_frontier(|f| f.less_equal(&max_ts)) {
                                probe.progressed().await;
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!("persist_source lagged {} messages", n);
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }

            Ok::<(), Error>(())
        })
    });

    event_stream.as_collection()
}
