use std::collections::BTreeMap;

use anyhow::{Result, bail};
use nostr_sdk::{Event, EventBuilder, Keys, Kind, Tag, TagKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::traits::IntoNostrEvent;

pub const KIND_MODEL: Kind = Kind::Custom(31312);
pub const KIND_USER_EMBEDDING: Kind = Kind::Custom(31313);
pub const KIND_NOTE_EMBEDDING: Kind = Kind::Custom(31314);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelTensorMeta {
    pub name: String,
    pub shape: Vec<usize>,
    pub dtype: String,
    pub offset_bytes: Option<usize>,
    pub length_bytes: Option<usize>,
    pub count: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelArtifact {
    pub name: String,
    pub role: Option<String>,
    pub format: String,
    pub url: String,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelArchitecture {
    pub name: String,
    #[serde(flatten)]
    pub params: BTreeMap<String, Value>,
}

impl ModelArchitecture {
    fn push_tags(&self, tags: &mut Vec<Tag>) -> Result<()> {
        for (key, value) in &self.params {
            if let Some(values) = value.as_array() {
                if let Some(strings) = values
                    .iter()
                    .map(|value| value.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
                {
                    let mut parts = Vec::with_capacity(strings.len() + 1);
                    parts.push(key.clone());
                    parts.extend(strings);
                    tags.push(tag(parts));
                } else {
                    tags.push(tag([key.clone(), serde_json::to_string(value)?]));
                }
            } else if let Some(string) = value.as_str() {
                push_meta(tags, key, string);
            } else if let Some(boolean) = value.as_bool() {
                push_meta(tags, key, boolean);
            } else if let Some(number) = value.as_i64() {
                push_meta(tags, key, number);
            } else if let Some(number) = value.as_u64() {
                push_meta(tags, key, number);
            } else if let Some(number) = value.as_f64() {
                push_meta(tags, key, number);
            } else {
                tags.push(tag([key.clone(), serde_json::to_string(value)?]));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelManifest {
    pub model_id: String,
    pub version: Option<String>,
    pub architecture: ModelArchitecture,
    #[serde(alias = "dim")]
    pub embedding_dim: usize,
    pub feature_dim: Option<usize>,
    pub feature_columns: Vec<String>,
    pub feature_transform: Option<String>,
    pub feature_mean: Option<Vec<f32>>,
    pub feature_std: Option<Vec<f32>>,
    pub scorer: String,
    pub num_nodes: usize,
    #[serde(default)]
    pub artifacts: Vec<ModelArtifact>,
    pub param_count: Option<usize>,
    pub tensors: Option<Vec<ModelTensorMeta>>,
}

impl ModelManifest {
    pub fn identifier(&self) -> String {
        match self.version.as_deref() {
            Some(version) if !version.is_empty() => format!("{}-{}", self.model_id, version),
            _ => self.model_id.clone(),
        }
    }

    fn artifact(&self, name: &str) -> Option<&ModelArtifact> {
        self.artifacts.iter().find(|artifact| artifact.name == name)
    }
}

fn tag<I, S>(parts: I) -> Tag
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    Tag::parse(parts).expect("valid custom tag")
}

fn push_meta(tags: &mut Vec<Tag>, key: &str, value: impl ToString) {
    tags.push(Tag::custom(TagKind::custom("meta"), [key.to_string(), value.to_string()]));
}

fn shape_label(shape: &[usize]) -> String {
    if shape.is_empty() {
        "scalar".to_string()
    } else {
        shape.iter()
            .map(|dim| dim.to_string())
            .collect::<Vec<_>>()
            .join("x")
    }
}

fn artifact_mime(format: &str) -> &'static str {
    match format {
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
}

fn push_imeta(tags: &mut Vec<Tag>, artifact: &ModelArtifact) {
    let alt = match artifact.name.as_str() {
        "weights" => "weights",
        "model" => "model",
        "embeddings" => "embeddings",
        "index_node_id" => "index node ids",
        "node_id_pubkey" => "node id pubkey mapping",
        _ => artifact.name.as_str(),
    };

    let mut parts = vec![
        "imeta".to_string(),
        format!("url {}", artifact.url),
        format!("x {}", artifact.sha256),
        format!("m {}", artifact_mime(&artifact.format)),
        format!("name {}", artifact.name),
        format!("format {}", artifact.format),
        format!("alt {alt}"),
    ];
    if let Some(role) = &artifact.role {
        parts.push(format!("role {role}"));
    }

    tags.push(tag(parts));
}

impl IntoNostrEvent for ModelManifest {
    fn into_nostr_event(self, identifier: String, keys: &Keys) -> Result<Event> {
        let versioned_model_id = self.identifier();
        let mut tags = vec![
            Tag::identifier(identifier),
            Tag::custom(TagKind::custom("model_id"), [versioned_model_id]),
        ];

        tags.push(Tag::custom(
            TagKind::custom("architecture"),
            [self.architecture.name.clone()],
        ));

        push_meta(&mut tags, "embedding_dim", self.embedding_dim);
        if let Some(feature_dim) = self.feature_dim {
            push_meta(&mut tags, "feature_dim", feature_dim);
        }
        if let Some(feature_transform) = &self.feature_transform {
            push_meta(&mut tags, "feature_transform", feature_transform);
        }
        push_meta(&mut tags, "scorer", &self.scorer);
        self.architecture.push_tags(&mut tags)?;
        push_meta(&mut tags, "num_nodes", self.num_nodes);
        if let Some(param_count) = self.param_count {
            push_meta(&mut tags, "param_count", param_count);
        }
        if let Some(weights_artifact) = self.artifact("weights") {
            let weights_format = if weights_artifact.format == "bin" {
                "flatbin-v1"
            } else {
                weights_artifact.format.as_str()
            };
            push_meta(&mut tags, "weights_format", weights_format);
            push_meta(&mut tags, "weights_dtype", "float32");
            push_meta(&mut tags, "endianness", "little");
        }
        if let Some(tensors) = &self.tensors {
            push_meta(&mut tags, "tensor_count", tensors.len());
        }

        let mut features = Vec::with_capacity(self.feature_columns.len() + 1);
        features.push("features".to_string());
        features.extend(self.feature_columns.iter().cloned());
        tags.push(tag(features));

        if let Some(feature_mean) = &self.feature_mean {
            tags.push(tag([
                "feature_mean".to_string(),
                serde_json::to_string(feature_mean)?,
            ]));
        }
        if let Some(feature_std) = &self.feature_std {
            tags.push(tag([
                "feature_std".to_string(),
                serde_json::to_string(feature_std)?,
            ]));
        }

        if let Some(weights_artifact) = self.artifact("weights") {
            tags.push(tag([
                "artifact".to_string(),
                "weights".to_string(),
                weights_artifact.sha256.clone(),
            ]));
        }

        for artifact in &self.artifacts {
            push_imeta(&mut tags, artifact);
        }

        if let Some(tensors) = &self.tensors {
            for tensor in tensors {
                let mut tensor_tag = vec![
                    "tensor".to_string(),
                    tensor.name.clone(),
                    shape_label(&tensor.shape),
                    tensor.dtype.clone(),
                ];

                if let Some(offset_bytes) = tensor.offset_bytes {
                    tensor_tag.push(offset_bytes.to_string());
                }
                if let Some(length_bytes) = tensor.length_bytes {
                    tensor_tag.push(length_bytes.to_string());
                }
                if let Some(count) = tensor.count {
                    tensor_tag.push(count.to_string());
                }

                tags.push(tag(tensor_tag));
            }
        }

        Ok(EventBuilder::new(KIND_MODEL, "")
            .tags(tags)
            .sign_with_keys(keys)?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserEmbedding {
    pub model_id: String,
    pub pubkey: String,
    pub embedding: Vec<f32>,
}

impl UserEmbedding {
    pub fn identifier(&self) -> String {
        format!("{}_{}", self.model_id, self.pubkey)
    }
}

impl IntoNostrEvent for UserEmbedding {
    fn into_nostr_event(self, identifier: String, keys: &Keys) -> Result<Event> {
        if let Some((idx, value)) = self
            .embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            bail!(
                "embedding contains non-finite value at index {}: {}",
                idx,
                value
            );
        }

        let vector = serde_json::to_string(&self.embedding)?;
        let tags = vec![
            Tag::identifier(identifier),
            Tag::custom(TagKind::custom("model_id"), [self.model_id.clone()]),
            Tag::public_key(self.pubkey.parse()?),
            Tag::custom(TagKind::custom("vector"), [vector]),
        ];

        Ok(EventBuilder::new(KIND_USER_EMBEDDING, "")
            .tags(tags)
            .sign_with_keys(keys)?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EmbeddingImportRecord {
    Model(ModelManifest),
    Embedding(UserEmbedding),
}

impl IntoNostrEvent for EmbeddingImportRecord {
    fn into_nostr_event(self, identifier: String, keys: &Keys) -> Result<Event> {
        match self {
            Self::Model(model) => model.into_nostr_event(identifier, keys),
            Self::Embedding(embedding) => embedding.into_nostr_event(identifier, keys),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nostr_sdk::Keys;
    use serde_json::Value;

    use crate::traits::IntoNostrEvent;

    use super::{
        KIND_MODEL, ModelArchitecture, ModelArtifact, ModelManifest, ModelTensorMeta,
        UserEmbedding,
    };

    #[test]
    fn user_embedding_rejects_non_finite_vector_values() {
        let keys = Keys::generate();
        let record = UserEmbedding {
            model_id: "sage".to_string(),
            pubkey: keys.public_key().to_hex(),
            embedding: vec![0.0, f32::NAN],
        };

        let identifier = record.identifier();
        let err = record
            .into_nostr_event(identifier, &keys)
            .unwrap_err()
            .to_string();

        assert!(err.contains("embedding contains non-finite value"));
    }

    #[test]
    fn model_manifest_generates_full_contract_event() {
        let keys = Keys::generate();
        let manifest = ModelManifest {
            model_id: "nostr-sage".to_string(),
            version: Some("v1".to_string()),
            architecture: ModelArchitecture {
                name: "graphsage".to_string(),
                params: BTreeMap::from([
                    ("hidden_channels".to_string(), Value::from(128_u64)),
                    ("num_layers".to_string(), Value::from(2_u64)),
                    ("aggregator".to_string(), Value::from("mean")),
                    ("activation".to_string(), Value::from("relu")),
                    ("output_normalization".to_string(), Value::from("none")),
                    ("root_weight".to_string(), Value::from(true)),
                    ("bias".to_string(), Value::from(true)),
                    (
                        "edge_types".to_string(),
                        Value::Array(vec![Value::from("follow")]),
                    ),
                    ("edge_direction".to_string(), Value::from("directed")),
                    ("self_loops".to_string(), Value::from(false)),
                ]),
            },
            embedding_dim: 64,
            feature_dim: Some(16),
            feature_columns: vec!["rank".to_string(), "follower_cnt".to_string()],
            feature_transform: Some("zero_fill_then_zscore_v1".to_string()),
            feature_mean: Some(vec![0.5, 1.5]),
            feature_std: Some(vec![2.0, 3.0]),
            scorer: "dot_product".to_string(),
            num_nodes: 42,
            artifacts: vec![
                ModelArtifact {
                    name: "model".to_string(),
                    role: Some("training".to_string()),
                    format: "pt".to_string(),
                    url: "https://example.com/model.zip".to_string(),
                    sha256: "deadbeef".to_string(),
                },
                ModelArtifact {
                    name: "embeddings".to_string(),
                    role: Some("dataset".to_string()),
                    format: "pt".to_string(),
                    url: "https://example.com/embeddings.zip".to_string(),
                    sha256: "feedface".to_string(),
                },
                ModelArtifact {
                    name: "index_node_id".to_string(),
                    role: Some("dataset".to_string()),
                    format: "npy".to_string(),
                    url: "https://example.com/index.npy".to_string(),
                    sha256: "abc123".to_string(),
                },
                ModelArtifact {
                    name: "node_id_pubkey".to_string(),
                    role: Some("dataset".to_string()),
                    format: "parquet".to_string(),
                    url: "https://example.com/node_id_pubkey.parquet".to_string(),
                    sha256: "bead1234".to_string(),
                },
                ModelArtifact {
                    name: "weights".to_string(),
                    role: Some("inference".to_string()),
                    format: "bin".to_string(),
                    url: "https://example.com/weights.bin".to_string(),
                    sha256: "cafebabe".to_string(),
                },
            ],
            param_count: Some(20672),
            tensors: Some(vec![ModelTensorMeta {
                name: "conv1.lin_l.weight".to_string(),
                shape: vec![128, 16],
                dtype: "float32".to_string(),
                offset_bytes: Some(0),
                length_bytes: Some(8192),
                count: Some(2048),
            }]),
        };

        let identifier = manifest.identifier();
        let event = manifest.into_nostr_event(identifier.clone(), &keys).unwrap();

        assert_eq!(identifier, "nostr-sage-v1");
        assert_eq!(event.kind, KIND_MODEL);
        assert!(event.content.is_empty());
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["d", "nostr-sage-v1"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["model_id", "nostr-sage-v1"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["meta", "feature_transform", "zero_fill_then_zscore_v1"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["edge_types", "follow"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["artifact", "weights", "cafebabe"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == [
            "imeta",
            "url https://example.com/weights.bin",
            "x cafebabe",
            "m application/octet-stream",
            "name weights",
            "format bin",
            "alt weights",
            "role inference",
        ]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["tensor", "conv1.lin_l.weight", "128x16", "float32", "0", "8192", "2048"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["feature_mean", "[0.5,1.5]"]));
        assert!(event.tags.iter().any(|tag| tag.as_slice() == ["feature_std", "[2.0,3.0]"]));
    }
}
