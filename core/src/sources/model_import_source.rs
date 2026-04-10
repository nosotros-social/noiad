use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, BinaryArray, FixedSizeListArray, Float32Array, UInt32Array};
use differential_dataflow::{AsCollection, VecCollection};
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, StreamCore};
use types::embeddings::{EmbeddingImportRecord, ModelManifest, UserEmbedding};
use types::types::Diff;

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{
    AsyncOperatorBuilder, AsyncOutputHandle, Event as AsyncEvent,
};

type ModelImportOutput =
    CapacityContainerBuilder<Vec<((String, EmbeddingImportRecord), u64, Diff)>>;

pub fn model_import_source<G>(
    scope: &G,
    config: DataflowConfig,
    bootstrap_done: &StreamCore<G, Vec<u64>>,
) -> VecCollection<G, (String, EmbeddingImportRecord), Diff>
where
    G: Scope<Timestamp = u64>,
{
    let mut builder = AsyncOperatorBuilder::new("ModelImportSource".to_string(), scope.clone());
    let mut bootstrap_done = builder.new_disconnected_input(bootstrap_done, Pipeline);
    let (output, stream) = builder
        .new_output::<CapacityContainerBuilder<Vec<((String, EmbeddingImportRecord), u64, Diff)>>>(
        );
    let worker_id = scope.index();

    let _ = builder.build(move |mut capabilities| {
        Box::pin(async move {
            let cap = capabilities.pop().expect("model import source capability");

            let Some(manifest_path) =
                resolve_manifest_path(config.embedding_import_manifest.clone())
            else {
                tracing::info!(
                    "[Worker {}] model import source disabled, no manifest found",
                    worker_id
                );
                drop(cap);
                return;
            };

            while let Some(event) = bootstrap_done.next().await {
                match event {
                    AsyncEvent::Data(_, mut data) => {
                        data.clear();
                    }
                    AsyncEvent::Progress(frontier) if frontier.is_empty() => {
                        tracing::warn!(
                            "[Worker {}] model import source bootstrap stream closed before unlock",
                            worker_id
                        );
                        drop(cap);
                        return;
                    }
                    AsyncEvent::Progress(frontier) if !frontier.less_equal(&0) => {
                        tracing::info!(
                            "[Worker {}] model import source global replay complete, starting import",
                            worker_id
                        );
                        break;
                    }
                    _ => {}
                }
            }

            if worker_id != 0 {
                drop(cap);
                return;
            }

            let started_at = std::time::Instant::now();
            let manifest_path = PathBuf::from(manifest_path);
            tracing::info!(
                "[Worker {}] model import source reading manifest {}",
                worker_id,
                manifest_path.display()
            );
            let manifest = match read_manifest(&manifest_path) {
                Ok(manifest) => manifest,
                Err(err) => {
                    tracing::error!(
                        "[Worker {}] model import source failed to read manifest {}: {err:?}",
                        worker_id,
                        manifest_path.display()
                    );
                    drop(cap);
                    return;
                }
            };
            let import_stats = match emit_import(&output, &cap, &manifest_path, &manifest, &config).await {
                Ok(stats) => stats,
                Err(err) => {
                    tracing::error!(
                        "[Worker {}] model import source failed for {}: {err:?}",
                        worker_id,
                        manifest.model_id
                    );
                    drop(cap);
                    return;
                }
            };
            tracing::info!(
                "[Worker {}] model import source emitted {} embeddings for {} skipped_missing_pubkey={} skipped_non_pubkey={} skipped_non_finite_embedding={} in {:?}",
                worker_id,
                import_stats.emitted_embeddings,
                manifest.model_id,
                import_stats.skipped_missing_pubkey,
                import_stats.skipped_non_pubkey,
                import_stats.skipped_non_finite_embedding,
                started_at.elapsed()
            );

            drop(cap.delayed(&1));
        })
    });

    stream.as_collection()
}

async fn emit_import(
    output: &AsyncOutputHandle<u64, ModelImportOutput>,
    capability: &Capability<u64>,
    manifest_path: &Path,
    manifest: &ModelManifest,
    config: &DataflowConfig,
) -> Result<ModelImportStats> {
    output.give(
        capability,
        (
            (
                manifest.identifier(),
                EmbeddingImportRecord::Model(manifest.clone()),
            ),
            0, // ts
            1, // diff
        ),
    );

    let parquet_path = resolve_parquet_path(manifest_path);
    let file = File::open(&parquet_path).with_context(|| {
        format!(
            "failed to open embeddings parquet {}",
            parquet_path.display()
        )
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| {
        format!(
            "failed to create parquet reader for {}",
            parquet_path.display()
        )
    })?;
    let reader = builder
        .with_batch_size(4096)
        .build()
        .with_context(|| format!("failed to build parquet reader {}", parquet_path.display()))?;

    let mut stats = ModelImportStats::default();
    for batch in reader {
        let batch = batch?;
        let schema = batch.schema();
        let node_id_idx = schema
            .index_of("node_id")
            .map_err(|_| anyhow!("missing node_id column in {}", parquet_path.display()))?;
        let embedding_idx = schema
            .index_of("embedding")
            .map_err(|_| anyhow!("missing embedding column in {}", parquet_path.display()))?;

        let node_ids = batch.column(node_id_idx);
        let node_ids = node_ids
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| anyhow!("node_id column must be UInt32"))?;

        let embeddings = batch.column(embedding_idx);
        for row in 0..batch.num_rows() {
            if node_ids.is_null(row) || embeddings.is_null(row) {
                continue;
            }

            let embedding = read_embedding_row(embeddings.as_ref(), row)?;
            if embedding.iter().any(|value| !value.is_finite()) {
                stats.skipped_non_finite_embedding += 1;
                continue;
            }

            let node_id = node_ids.value(row);
            let Some(pubkey) = config.persist.resolve_node(node_id)? else {
                stats.skipped_missing_pubkey += 1;
                continue;
            };
            if pubkey.len() != 32 {
                stats.skipped_non_pubkey += 1;
                continue;
            }

            let record = UserEmbedding {
                model_id: manifest.identifier(),
                pubkey: hex::encode(pubkey),
                embedding,
            };

            output.give(
                capability,
                (
                    (
                        record.identifier(),
                        EmbeddingImportRecord::Embedding(record),
                    ),
                    0,
                    1,
                ),
            );
            stats.emitted_embeddings += 1;
        }

        // Give downstream operators a chance to drain the embedding stream between
        // parquet batches. Without this, recompute can enqueue millions of owned
        // embedding records before event_sink/persist_source are scheduled.
        tokio::task::yield_now().await;
    }

    Ok(stats)
}

#[derive(Debug, Default)]
struct ModelImportStats {
    emitted_embeddings: usize,
    skipped_missing_pubkey: usize,
    skipped_non_pubkey: usize,
    skipped_non_finite_embedding: usize,
}

fn read_manifest(manifest_path: &Path) -> Result<ModelManifest> {
    let file = File::open(manifest_path)
        .with_context(|| format!("failed to open manifest {}", manifest_path.display()))?;
    serde_json::from_reader(file)
        .with_context(|| format!("failed to parse manifest {}", manifest_path.display()))
}

fn resolve_manifest_path(configured: Option<String>) -> Option<String> {
    if let Some(path) = configured {
        return Some(path);
    }

    let mut search_roots = Vec::new();
    if let Ok(current_dir) = std::env::current_dir() {
        search_roots.push(current_dir);
    }
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        search_roots.push(parent.to_path_buf());
    }
    search_roots.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    for root in search_roots {
        for ancestor in root.ancestors() {
            for candidate in [
                ancestor.join("models").join("manifest.json"),
                ancestor.join("data").join("models").join("manifest.json"),
            ] {
                if candidate.exists() {
                    return Some(candidate.to_string_lossy().into_owned());
                }
            }
        }
    }

    None
}

fn resolve_parquet_path(manifest_path: &Path) -> PathBuf {
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("embeddings.parquet")
}

fn read_embedding_row(array: &dyn Array, row: usize) -> Result<Vec<f32>> {
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let width = usize::try_from(array.value_length())
            .map_err(|_| anyhow!("invalid fixed-size embedding width"))?;
        let values = array.values();
        let values = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow!("fixed-size embedding values must be Float32"))?;
        let start = row * width;
        let mut embedding = Vec::with_capacity(width);
        for idx in start..start + width {
            embedding.push(values.value(idx));
        }
        return Ok(embedding);
    }

    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        let bytes = array.value(row);
        if bytes.len() % std::mem::size_of::<f32>() != 0 {
            bail!("binary embedding column must be packed f32 bytes");
        }

        let mut embedding = Vec::with_capacity(bytes.len() / std::mem::size_of::<f32>());
        for chunk in bytes.chunks_exact(std::mem::size_of::<f32>()) {
            let bytes: [u8; 4] = chunk
                .try_into()
                .map_err(|_| anyhow!("invalid binary embedding row"))?;
            embedding.push(f32::from_le_bytes(bytes));
        }
        return Ok(embedding);
    }

    bail!("embedding column must be FixedSizeList<Float32> or Binary")
}
