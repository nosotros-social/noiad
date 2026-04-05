use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use chrono::Local;
use differential_dataflow::VecCollection;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, StreamCore};
use types::{
    parquet::{SnapshotSink, ToParquet},
    types::Diff,
};

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event as AsyncEvent};

pub fn parquet_sink<G, S>(
    scope: &G,
    config: DataflowConfig,
    input: &VecCollection<G, S::Item, Diff>,
    bootstrap_done: &StreamCore<G, Vec<u64>>,
    file_stem: &'static str,
) where
    G: Scope<Timestamp = u64>,
    S: SnapshotSink + 'static,
    S::Item: timely::Data + Clone + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("ParquetSink".to_string(), scope.clone());
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
    let mut bootstrap_done = builder.new_disconnected_input(bootstrap_done, Pipeline);
    let worker_id = scope.index();

    let _ = builder.build(move |_capabilities| {
        Box::pin(async move {
            if config.dataset_sink_path.is_none() {
                return;
            }

            let mut snapshot = S::default();
            let mut bootstrap_frontier = None;
            let mut input_frontier = None;
            let mut bootstrap_received = false;

            loop {
                tokio::select! {
                    event = input.next() => {
                        match event {
                            Some(AsyncEvent::Data(_, mut data)) => {
                                for (item, _ts, diff) in data.drain(..) {
                                    snapshot.apply_diff(item, diff);
                                }
                            }
                            Some(AsyncEvent::Progress(frontier)) => {
                                if let Some(min) = frontier.iter().min().copied() {
                                    input_frontier = Some(min);
                                }
                            }
                            None => break,
                        }
                    }
                    event = bootstrap_done.next(), if !bootstrap_received => {
                        match event {
                            Some(AsyncEvent::Data(_, mut data)) => {
                                for ts in data.drain(..) {
                                    bootstrap_frontier = Some(
                                        bootstrap_frontier.map_or(ts, |current: u64| current.max(ts))
                                    );
                                }
                                bootstrap_received = true;
                            }
                            Some(AsyncEvent::Progress(_)) => {}
                            None => {}
                        }
                    }
                }

                if let (Some(target_frontier), Some(current_frontier)) =
                    (bootstrap_frontier, input_frontier)
                    && current_frontier >= target_frontier
                {
                    let Some(raw_path) = config.dataset_sink_path.as_ref() else {
                        return;
                    };
                    let path = resolve_output_path(raw_path, worker_id, file_stem);
                    if let Err(err) = write_parquet(worker_id, &snapshot, path, file_stem) {
                        tracing::error!(
                            "[Worker {}] {} failed to write snapshot: {err:?}",
                            worker_id,
                            file_stem
                        );
                        return;
                    }
                    break;
                }
            }
        })
    });
}

fn resolve_output_path(raw_path: &str, worker_id: usize, file_stem: &str) -> PathBuf {
    let path = PathBuf::from(raw_path);
    let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S");

    if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
        let parent = path.parent().map(Path::to_path_buf).unwrap_or_default();
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(file_stem);
        parent.join(format!("{stem}-w{worker_id}-{timestamp}.parquet"))
    } else {
        path.join(format!("{file_stem}-w{worker_id}-{timestamp}.parquet"))
    }
}

fn write_parquet<T: ToParquet>(
    worker_id: usize,
    snapshot: &T,
    path: PathBuf,
    label: &str,
) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let (schema, columns) = snapshot.as_parquet();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    tracing::info!(
        "[Worker {}] {} wrote {} rows to {}",
        worker_id,
        label,
        batch.num_rows(),
        path.display()
    );

    Ok(())
}
