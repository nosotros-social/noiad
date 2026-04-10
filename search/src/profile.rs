use anyhow::Result;
use rusqlite::{Connection, params};

pub fn upsert_profile(
    conn: &Connection,
    pubkey: &str,
    created_at: u32,
    event_json: &str,
    name: &str,
    display_name: &str,
    nip05: Option<&str>,
) -> Result<()> {
    let changed = conn.execute(
        "INSERT INTO profiles (pubkey, created_at, event_json, name, display_name, nip05)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(pubkey) DO UPDATE SET
            created_at = excluded.created_at,
            event_json = excluded.event_json,
            name = excluded.name,
            display_name = excluded.display_name,
            nip05 = excluded.nip05
         WHERE excluded.created_at >= profiles.created_at",
        params![
            pubkey,
            i64::from(created_at),
            event_json,
            name,
            display_name,
            nip05,
        ],
    )?;

    if changed > 0 {
        sync_fts_for_pubkey(conn, pubkey)?;
    }

    Ok(())
}

fn sync_fts_for_pubkey(conn: &Connection, pubkey: &str) -> Result<()> {
    conn.execute("DELETE FROM profiles_fts WHERE pubkey = ?1", params![pubkey])?;
    conn.execute(
        "INSERT INTO profiles_fts (pubkey, name, display_name, nip05)
         SELECT pubkey, name, display_name, COALESCE(nip05, '')
         FROM profiles
         WHERE pubkey = ?1",
        params![pubkey],
    )?;
    Ok(())
}

pub fn rebuild_fts(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "DELETE FROM profiles_fts;
         INSERT INTO profiles_fts (pubkey, name, display_name, nip05)
         SELECT pubkey, name, display_name, COALESCE(nip05, '')
         FROM profiles;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SearchDb;

    #[test]
    fn upsert_profile_populates_fts() {
        let tmp = std::env::temp_dir().join(format!("noiad-search-profile-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let db = SearchDb::open(&tmp).unwrap();

        upsert_profile(
            db.connection(),
            "aabb",
            1000,
            r#"{"name":"Vitor","display_name":"Vitor Pamplona"}"#,
            "Vitor",
            "Vitor Pamplona",
            Some("vitor@example.com"),
        )
        .unwrap();

        let count: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM profiles_fts WHERE profiles_fts MATCH 'vitor'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn stale_profile_does_not_overwrite_newer() {
        let tmp =
            std::env::temp_dir().join(format!("noiad-search-profile-stale-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let db = SearchDb::open(&tmp).unwrap();

        upsert_profile(db.connection(), "aabb", 2000, "{}", "New", "New", None).unwrap();
        upsert_profile(db.connection(), "aabb", 1000, "{}", "Old", "Old", None).unwrap();

        let name: String = db
            .connection()
            .query_row(
                "SELECT name FROM profiles WHERE pubkey = ?1",
                params!["aabb"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "New");
    }
}
