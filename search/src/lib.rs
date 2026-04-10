use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use rusqlite::{Connection, OpenFlags};

mod profile;
mod query;

pub use profile::{rebuild_fts, upsert_profile};
pub use query::{
    SearchResult, load_embedding, parse_personalization_tag, search_users, upsert_embedding,
};

const SQLITE_FILENAME: &str = "search.sqlite";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPaths {
    pub sqlite_path: PathBuf,
}

impl SearchPaths {
    pub fn from_db_root(db_root: impl AsRef<Path>) -> Self {
        let db_root = db_root.as_ref();
        Self {
            sqlite_path: db_root.join(SQLITE_FILENAME),
        }
    }
}

pub struct SearchDb {
    conn: Connection,
    paths: SearchPaths,
}

impl std::fmt::Debug for SearchDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchDb")
            .field("paths", &self.paths)
            .finish()
    }
}

impl SearchDb {
    pub fn open(db_root: impl AsRef<Path>) -> Result<Self> {
        let db_root = db_root.as_ref();
        std::fs::create_dir_all(db_root)?;

        let paths = SearchPaths::from_db_root(db_root);
        let conn = Connection::open(&paths.sqlite_path)?;
        conn.busy_timeout(Duration::from_secs(30))?;
        initialize_schema(&conn)?;

        Ok(Self { conn, paths })
    }

    pub fn open_readonly(db_root: impl AsRef<Path>) -> Result<Self> {
        let db_root = db_root.as_ref();
        let paths = SearchPaths::from_db_root(db_root);
        let conn = Connection::open_with_flags(
            &paths.sqlite_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(5))?;

        Ok(Self { conn, paths })
    }

    pub fn paths(&self) -> &SearchPaths {
        &self.paths
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}

fn initialize_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = OFF;

        CREATE TABLE IF NOT EXISTS profiles (
            pubkey TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            event_json TEXT NOT NULL,
            name TEXT NOT NULL,
            display_name TEXT NOT NULL,
            nip05 TEXT,
            rank REAL NOT NULL DEFAULT 0,
            follower_cnt INTEGER NOT NULL DEFAULT 0
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS profiles_fts USING fts5(
            pubkey UNINDEXED,
            name,
            display_name,
            nip05,
            tokenize = 'unicode61 remove_diacritics 2'
        );

        CREATE TABLE IF NOT EXISTS embeddings (
            embedding_id TEXT PRIMARY KEY,
            pubkey TEXT NOT NULL UNIQUE,
            model_id TEXT NOT NULL,
            dimensions INTEGER NOT NULL,
            embedding BLOB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS embeddings_pubkey_idx
        ON embeddings(pubkey);

        CREATE INDEX IF NOT EXISTS embeddings_model_pubkey_idx
        ON embeddings(model_id, pubkey);
        "#,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_paths_use_expected_filename() {
        let paths = SearchPaths::from_db_root("/tmp/noiad-db");
        assert!(paths.sqlite_path.ends_with("search.sqlite"));
    }

    #[test]
    fn open_creates_schema() {
        let tmp = std::env::temp_dir().join(format!(
            "noiad-search-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);

        let db = SearchDb::open(&tmp).unwrap();

        let profiles_exists: String = db
            .connection()
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='profiles'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let embeddings_exists: String = db
            .connection()
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='embeddings'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(profiles_exists, "profiles");
        assert_eq!(embeddings_exists, "embeddings");
        assert!(db.paths().sqlite_path.exists());
    }
}
