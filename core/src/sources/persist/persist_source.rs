use std::time::Duration;

use anyhow::Error;
use differential_dataflow::{AsCollection, Data, VecCollection};
use futures::StreamExt;
use persist::event::EventRecord;
use persist::iter::PersistQueryIter;
use timely::Container;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, StreamCore};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::time::interval;
use types::event::EventRow;
use types::types::Diff;

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event};
use crate::operators::probe::Handle;

pub fn persist_source<G, D, Q>(
    scope: &G,
    config: DataflowConfig,
    query: Q,
    start_stream: &StreamCore<G, D>,
    probe: &Handle<u64>,
) -> VecCollection<G, EventRow, Diff>
where
    G: Scope<Timestamp = u64>,
    D: Container + Data,
    Q: PersistQueryIter + Clone + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("PersistSource".to_string(), scope.clone());

    let mut signal_input = builder.new_disconnected_input(start_stream, Pipeline);

    let (event_handle, event_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<(EventRow, u64, Diff)>>>();

    let probe = probe.clone();
    let worker_id = scope.index();

    let _ = builder.build_fallible(move |capabilities| {
        Box::pin(async move {
            if worker_id != 0 {
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
                let ts = 0;
                let diff = 1;
                event_handle.give(&event_cap[0], (event, ts, diff));
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

            event_cap.downgrade([1]);

            let mut subscription = config.persist.subscribe();
            let mut next_ts = 1u64;

            loop {
                match subscription.recv().await {
                    Ok((event, ts, diff)) => {
                        let event = EventRow {
                            id: event.id,
                            pubkey: event.pubkey,
                            kind: event.kind,
                            created_at: event.created_at,
                            tags: event.tags,
                        };
                        if !query.matches(&event) {
                            continue;
                        }

                        next_ts = next_ts.max(ts);
                        event_handle.give(&event_cap[0], (event, next_ts, diff as Diff));

                        if subscription.is_empty() {
                            tracing::info!(
                                "[Worker {}] persist_source advancing frontier to {}",
                                worker_id,
                                next_ts + 1
                            );
                            event_cap.downgrade([next_ts]);

                            while probe.with_frontier(|f| f.less_equal(&(next_ts - 1))) {
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
