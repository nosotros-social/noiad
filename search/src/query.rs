use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, params};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub pubkey: String,
    pub event_json: String,
    pub name: String,
    pub display_name: String,
    pub rank: f32,
    pub follower_cnt: i64,
    pub score: f32,
}

pub fn upsert_embedding(
    conn: &Connection,
    pubkey: &str,
    model_id: &str,
    embedding: &[f32],
) -> Result<()> {
    let embedding_id = format!("{}_{}", model_id, pubkey);
    let blob = embedding_to_bytes(embedding);

    conn.execute(
        "INSERT INTO embeddings (embedding_id, pubkey, model_id, dimensions, embedding)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(embedding_id) DO UPDATE SET
            pubkey = excluded.pubkey,
            model_id = excluded.model_id,
            dimensions = excluded.dimensions,
            embedding = excluded.embedding",
        params![
            embedding_id,
            pubkey,
            model_id,
            i64::try_from(embedding.len())?,
            blob,
        ],
    )?;

    Ok(())
}

pub fn load_embedding(
    conn: &Connection,
    model_id: &str,
    pubkey: &str,
) -> Result<Option<Vec<f32>>> {
    let row = conn.query_row(
        "SELECT dimensions, embedding
         FROM embeddings
         WHERE model_id = ?1 AND pubkey = ?2",
        params![model_id, pubkey],
        |row| {
            let dimensions: i64 = row.get(0)?;
            let blob: Vec<u8> = row.get(1)?;
            Ok((dimensions, blob))
        },
    );

    match row {
        Ok((dimensions, blob)) => {
            let values = bytes_to_embedding(&blob)?;
            if usize::try_from(dimensions)? != values.len() {
                bail!(
                    "embedding dimensions mismatch for pubkey {} model {}: expected {}, got {}",
                    pubkey,
                    model_id,
                    dimensions,
                    values.len()
                );
            }
            Ok(Some(values))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn search_users(
    conn: &Connection,
    text_query: &str,
    personalize: Option<(&str, &str)>,
    limit: usize,
) -> Result<Vec<SearchResult>> {
    let sanitized = sanitize_fts_query(text_query);
    if sanitized.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let mut candidates = fts_candidates(conn, &sanitized, personalize.is_none(), limit)?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let Some((model_id, requester_pubkey)) = personalize else {
        candidates.truncate(limit);
        return Ok(candidates);
    };

    let Some(requester_embedding) = load_embedding(conn, model_id, requester_pubkey)? else {
        candidates.truncate(limit);
        return Ok(candidates);
    };

    let candidate_pubkeys = candidates
        .iter()
        .map(|candidate| candidate.pubkey.clone())
        .collect::<Vec<_>>();
    let candidate_embeddings = load_embeddings_for_pubkeys(conn, model_id, &candidate_pubkeys)?;

    let mut reranked = Vec::with_capacity(candidates.len());
    for mut candidate in candidates {
        let Some(embedding) = candidate_embeddings.get(&candidate.pubkey) else {
            continue;
        };

        candidate.score = cosine_similarity(&requester_embedding, embedding);
        reranked.push(candidate);
    }

    reranked.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    reranked.truncate(limit);
    Ok(reranked)
}

fn fts_candidates(
    conn: &Connection,
    query: &str,
    apply_limit: bool,
    limit: usize,
) -> Result<Vec<SearchResult>> {
    let sql = if apply_limit {
        "SELECT fts.pubkey, p.event_json, p.name, p.display_name, p.rank, p.follower_cnt
         FROM profiles_fts fts
         JOIN profiles p ON p.pubkey = fts.pubkey
         WHERE profiles_fts MATCH ?1
           AND (p.name != '' OR p.display_name != '')
         ORDER BY bm25(profiles_fts)
         LIMIT ?2"
    } else {
        "SELECT fts.pubkey, p.event_json, p.name, p.display_name, p.rank, p.follower_cnt
         FROM profiles_fts fts
         JOIN profiles p ON p.pubkey = fts.pubkey
         WHERE profiles_fts MATCH ?1
           AND (p.name != '' OR p.display_name != '')
         ORDER BY bm25(profiles_fts)"
    };
    let mut stmt = conn.prepare(sql)?;

    let mapper = |row: &rusqlite::Row<'_>| {
        Ok(SearchResult {
            pubkey: row.get(0)?,
            event_json: row.get(1)?,
            name: row.get(2)?,
            display_name: row.get(3)?,
            rank: row.get(4)?,
            follower_cnt: row.get(5)?,
            score: 0.0,
        })
    };
    let results = if apply_limit {
        stmt.query_map(params![query, i64::try_from(limit)?], mapper)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        stmt.query_map(params![query], mapper)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    Ok(results)
}

fn load_embeddings_for_pubkeys(
    conn: &Connection,
    model_id: &str,
    pubkeys: &[String],
) -> Result<HashMap<String, Vec<f32>>> {
    const SQLITE_IN_CHUNK: usize = 900;

    let mut embeddings = HashMap::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(SQLITE_IN_CHUNK) {
        if chunk.is_empty() {
            continue;
        }

        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT pubkey, dimensions, embedding
             FROM embeddings
             WHERE model_id = ? AND pubkey IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&sql)?;
        let mut params = Vec::with_capacity(chunk.len() + 1);
        params.push(rusqlite::types::Value::from(model_id.to_owned()));
        params.extend(chunk.iter().cloned().map(rusqlite::types::Value::from));

        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
            let pubkey: String = row.get(0)?;
            let dimensions: i64 = row.get(1)?;
            let blob: Vec<u8> = row.get(2)?;
            Ok((pubkey, dimensions, blob))
        })?;

        for row in rows {
            let (pubkey, dimensions, blob) = row?;
            let values = bytes_to_embedding(&blob)?;
            if usize::try_from(dimensions)? != values.len() {
                bail!(
                    "embedding dimensions mismatch for pubkey {} model {}: expected {}, got {}",
                    pubkey,
                    model_id,
                    dimensions,
                    values.len()
                );
            }
            embeddings.insert(pubkey, values);
        }
    }

    Ok(embeddings)
}

fn embedding_to_bytes(values: &[f32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * std::mem::size_of::<f32>());
    for value in values {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf
}

fn bytes_to_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        bail!("embedding blob length {} is not a multiple of 4", bytes.len());
    }

    bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| {
            let bytes: [u8; 4] = chunk
                .try_into()
                .map_err(|_| anyhow!("failed to decode f32 bytes"))?;
            Ok(f32::from_le_bytes(bytes))
        })
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    if norm_a <= f32::EPSILON || norm_b <= f32::EPSILON {
        return f32::NEG_INFINITY;
    }

    dot / (norm_a.sqrt() * norm_b.sqrt())
}

fn sanitize_fts_query(query: &str) -> String {
    query
        .split_whitespace()
        .map(|token| {
            let cleaned: String = token
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                .collect();
            if cleaned.is_empty() {
                return String::new();
            }
            format!("\"{}\"*", cleaned)
        })
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn parse_personalization_tag(value: &str) -> Option<(&str, &str)> {
    let separator_idx = value.rfind('_')?;
    let model_id = &value[..separator_idx];
    let pubkey = &value[separator_idx + 1..];

    if model_id.is_empty() || pubkey.len() != 64 {
        return None;
    }

    if !pubkey.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Some((model_id, pubkey))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SearchDb, upsert_profile};

    fn test_db(name: &str) -> SearchDb {
        let tmp = std::env::temp_dir().join(format!(
            "noiad-search-query-{}-{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        SearchDb::open(&tmp).unwrap()
    }

    #[test]
    fn embedding_round_trip() {
        let db = test_db("roundtrip");
        let values: Vec<f32> = (0..64).map(|idx| idx as f32 * 0.25).collect();

        upsert_embedding(db.connection(), "aabb", "nostr-sage-v1", &values).unwrap();

        let loaded = load_embedding(db.connection(), "nostr-sage-v1", "aabb")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, values);
    }

    #[test]
    fn lexical_search_works_without_personalization() {
        let db = test_db("fts");
        upsert_profile(
            db.connection(),
            "aabb",
            1000,
            r#"{"kind":0}"#,
            "Vitor",
            "Vitor Pamplona",
            Some("vitor@example.com"),
        )
        .unwrap();
        upsert_profile(
            db.connection(),
            "ccdd",
            1000,
            r#"{"kind":0}"#,
            "Alice",
            "Alice Smith",
            None,
        )
        .unwrap();

        let results = search_users(db.connection(), "vit", None, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].pubkey, "aabb");
    }

    #[test]
    fn personalized_search_reranks_candidates() {
        let db = test_db("rerank");

        upsert_profile(db.connection(), "requester", 1000, "{}", "Req", "Req", None).unwrap();
        upsert_profile(db.connection(), "far", 1000, "{}", "Victor A", "Victor A", None)
            .unwrap();
        upsert_profile(db.connection(), "close", 1000, "{}", "Victor B", "Victor B", None)
            .unwrap();
        upsert_profile(
            db.connection(),
            "missing",
            1000,
            "{}",
            "Victor Missing",
            "Victor Missing",
            None,
        )
        .unwrap();

        upsert_embedding(db.connection(), "requester", "nostr-sage-v1", &[1.0; 64]).unwrap();
        upsert_embedding(db.connection(), "far", "nostr-sage-v1", &[-0.5; 64]).unwrap();
        upsert_embedding(db.connection(), "close", "nostr-sage-v1", &[0.9; 64]).unwrap();

        let results = search_users(
            db.connection(),
            "victor",
            Some(("nostr-sage-v1", "requester")),
            10,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].pubkey, "close");
        assert_eq!(results[1].pubkey, "far");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn missing_requester_embedding_falls_back_to_lexical_order() {
        let db = test_db("fallback");
        upsert_profile(db.connection(), "aabb", 1000, "{}", "Victor", "Victor", None).unwrap();

        let results = search_users(
            db.connection(),
            "victor",
            Some(("nostr-sage-v1", "missing")),
            10,
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].pubkey, "aabb");
    }

    #[test]
    fn parse_personalization_tag_accepts_versioned_identifier() {
        let value =
            "nostr-sage-v1_c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e";
        let parsed = parse_personalization_tag(value).unwrap();
        assert_eq!(parsed.0, "nostr-sage-v1");
        assert_eq!(
            parsed.1,
            "c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e"
        );
    }
}
