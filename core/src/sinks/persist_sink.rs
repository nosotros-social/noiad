use anyhow::Error;
use differential_dataflow::VecCollection;
use futures::StreamExt;
use persist::db::PersistInputUpdate;
use persist::event::EventRaw;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream, StreamCore};
use types::types::Diff;

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::AsyncOperatorBuilder;
use crate::operators::builder_async::Event;

// TODO: make this configurable
const BATCH_SIZE: usize = 20000;

static TOTAL_WRITTEN: AtomicUsize = AtomicUsize::new(0);

pub fn persist_sink<G>(
    scope: &G,
    config: DataflowConfig,
    input: &VecCollection<G, EventRaw, Diff>,
    lsn_stream: &Stream<G, u64>,
) -> StreamCore<G, Vec<()>>
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("PersistSink".to_string(), scope.clone());

    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
    let mut lsn_input = builder.new_disconnected_input(lsn_stream, Pipeline);

    let (_done_output, done_stream) = builder.new_output::<CapacityContainerBuilder<Vec<()>>>();

    let worker_id = scope.index();

    let _ = builder.build_fallible(move |capability_sets| {
        Box::pin(async move {
            let done_caps = &mut capability_sets[0];
            let mut buffer: Vec<PersistInputUpdate> = Vec::with_capacity(BATCH_SIZE);

            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_time, mut data) => {
                        for (event, ts, diff) in data.drain(..) {
                            buffer.push((event, ts, diff));

                            if buffer.len() >= BATCH_SIZE {
                                flush_buffer(worker_id, &config, &mut buffer);
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        if !buffer.is_empty() {
                            flush_buffer(worker_id, &config, &mut buffer);
                        }

                        done_caps.downgrade(frontier.iter().clone());

                        if frontier.is_empty() {
                            break;
                        }
                    }
                }
            }

            done_caps.downgrade(std::iter::empty::<u64>());

            Ok::<(), Error>(())
        })
    });

    done_stream
}

fn flush_buffer(worker_id: usize, config: &DataflowConfig, buffer: &mut Vec<PersistInputUpdate>) {
    let count = buffer.len();
    let started = Instant::now();

    if let Err(e) = config.persist.apply_updates(buffer) {
        tracing::error!("Failed to apply updates: {:?}", e);
    }

    TOTAL_WRITTEN.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
    buffer.clear();
    tracing::info!(
        "[worker {}] Applied {} updates in {:?} (total: {})",
        worker_id,
        count,
        started.elapsed(),
        TOTAL_WRITTEN.load(std::sync::atomic::Ordering::SeqCst),
    );
}
