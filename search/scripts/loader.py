#!/usr/bin/env python3
"""
One-off offline builder for search.sqlite.

Defaults:
- profiles source: Postgres `events` table (kind 0 only) via `psql`
- embeddings source: `core/models/embeddings.parquet`
- node_id -> pubkey source: `core/models/node_id_pubkey.parquet`
- output: `core/data/db/search.sqlite`

Required Python dependency:
- pyarrow

No required CLI args. Override with env vars if needed:
- DATABASE_URL
- SEARCH_DB_ROOT
- SEARCH_MANIFEST_PATH
- SEARCH_EMBEDDINGS_PATH
- SEARCH_NODE_ID_PUBKEY_PATH
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_ROOT = REPO_ROOT / "core" / "data" / "db"
DEFAULT_MANIFEST_PATH = REPO_ROOT / "core" / "models" / "manifest.json"
DEFAULT_EMBEDDINGS_PATH = REPO_ROOT / "core" / "models" / "embeddings.parquet"
DEFAULT_NODE_ID_PUBKEY_PATH = REPO_ROOT / "core" / "models" / "node_id_pubkey.parquet"
DEFAULT_DATASET_ROOT = REPO_ROOT / "core" / "data" / "dataset"

PROFILE_LOG_EVERY = 10_000
EMBEDDING_LOG_EVERY = 100_000
SQLITE_BATCH_SIZE = 5_000


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return "".join(
        ch if not 0xD800 <= ord(ch) <= 0xDFFF else "\uFFFD"
        for ch in value
    )


def clean_jsonish(value):
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, list):
        return [clean_jsonish(item) for item in value]
    if isinstance(value, dict):
        return {clean_jsonish(k): clean_jsonish(v) for k, v in value.items()}
    return value


def require_pyarrow():
    try:
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            "pyarrow is required for search/scripts/loader.py. "
            "Install it with `python3 -m pip install pyarrow`."
        ) from exc


def model_identifier(manifest: dict) -> str:
    model_id = manifest["model_id"]
    version = manifest.get("version")
    if version:
        return f"{model_id}-{version}"
    return model_id


def psql_command(sql: str) -> list[str]:
    cmd = [
        shutil.which("psql") or "psql",
        "-X",
        "--no-psqlrc",
        "--csv",
        "-v",
        "ON_ERROR_STOP=1",
    ]
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        cmd.append(database_url)
    cmd.extend(["-c", sql])
    return cmd


def iter_latest_profiles() -> Iterable[dict]:
    csv.field_size_limit(sys.maxsize)

    sql = """
        SELECT DISTINCT ON (pubkey)
            id,
            pubkey,
            created_at,
            kind,
            tags::text AS tags,
            content,
            sig
        FROM events
        WHERE kind = 0
        ORDER BY pubkey, created_at DESC, id DESC
    """

    cmd = psql_command(sql)
    log(f"loading latest kind 0 profiles via psql: {' '.join(cmd[:5])} ...")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None

    reader = csv.DictReader(proc.stdout)
    try:
        for row in reader:
            yield row
    finally:
        proc.stdout.close()
        try:
            stderr = proc.stderr.read()
        except BrokenPipeError:
            stderr = ""
        proc.stderr.close()
        rc = proc.wait()
        if rc not in (0, -13):
            raise RuntimeError(f"psql exited with code {rc}: {stderr.strip()}")


def extract_profile_fields(content: str) -> tuple[str, str, str | None]:
    content = clean_text(content)
    try:
        value = json.loads(content)
    except json.JSONDecodeError:
        return "", "", None

    if not isinstance(value, dict):
        return "", "", None

    name = clean_text(str(value.get("name") or "")).strip()
    display_name = clean_text(str(value.get("display_name") or "")).strip()
    nip05 = clean_text(str(value.get("nip05") or "")).strip() or None
    return name, display_name, nip05


def build_event_json(row: dict) -> str:
    try:
        tags = json.loads(row["tags"])
    except json.JSONDecodeError:
        tags = []
    tags = clean_jsonish(tags)

    event = {
        "id": clean_text(row["id"]),
        "pubkey": clean_text(row["pubkey"]),
        "created_at": int(row["created_at"]),
        "kind": int(row["kind"]),
        "tags": tags,
        "content": clean_text(row["content"]),
        "sig": clean_text(row["sig"]),
    }
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False)


def open_sqlite(db_root: Path) -> tuple[sqlite3.Connection, Path]:
    db_root.mkdir(parents=True, exist_ok=True)
    sqlite_path = db_root / "search.sqlite"
    temp_path = db_root / "search.sqlite.tmp"
    if temp_path.exists():
        temp_path.unlink()

    conn = sqlite3.connect(temp_path)
    # Offline builder: avoid relying on sidecar WAL files because we atomically
    # replace only the main sqlite file at the end.
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -200000")
    conn.executescript(
        """
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
        """
    )
    return conn, sqlite_path


def detect_latest_features_files(dataset_root: Path) -> list[Path]:
    if not dataset_root.exists():
        log(f"quality: dataset root {dataset_root} not found; using default rank=0 follower_cnt=0")
        return []

    feature_files = sorted(dataset_root.glob("features-*.parquet"))
    if feature_files:
        return feature_files

    timestamp_dirs = sorted([p for p in dataset_root.iterdir() if p.is_dir()], reverse=True)
    for directory in timestamp_dirs:
        files = sorted(directory.glob("features-*.parquet"))
        if files:
            log(f"quality: using feature parquet from {directory}")
            return files

    log(f"quality: no feature parquet found under {dataset_root}; using default rank=0 follower_cnt=0")
    return []


def load_profile_quality(
    node_id_to_pubkey: dict[int, str],
    feature_files: list[Path],
) -> dict[str, tuple[float, int]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not feature_files:
        return {}

    quality: dict[str, tuple[float, int]] = {}
    total = 0
    started = time.time()

    for feature_file in feature_files:
        parquet = pq.ParquetFile(feature_file)
        schema = parquet.schema_arrow

        node_field = None
        rank_field = None
        follower_field = None

        for field in schema:
            name = field.name.lower()
            if node_field is None and pa.types.is_integer(field.type) and "node" in name and "id" in name:
                node_field = field.name
            if rank_field is None and "rank" == name:
                rank_field = field.name
            if follower_field is None and "follower_cnt" == name:
                follower_field = field.name

        if node_field is None or rank_field is None or follower_field is None:
            raise RuntimeError(
                f"failed to detect node_id/rank/follower_cnt columns in {feature_file}: {schema.names}"
            )

        log(
            f"quality: reading {feature_file} using columns node={node_field} rank={rank_field} followers={follower_field}"
        )

        for batch in parquet.iter_batches(batch_size=50_000):
            node_ids = batch.column(batch.schema.get_field_index(node_field)).to_pylist()
            ranks = batch.column(batch.schema.get_field_index(rank_field)).to_pylist()
            followers = batch.column(batch.schema.get_field_index(follower_field)).to_pylist()

            for node_id, rank, follower_cnt in zip(node_ids, ranks, followers):
                if node_id is None:
                    continue
                pubkey = node_id_to_pubkey.get(int(node_id))
                if pubkey is None:
                    continue

                rank_value = float(rank) if rank is not None else 0.0
                follower_value = int(follower_cnt) if follower_cnt is not None else 0
                quality[pubkey] = (rank_value, follower_value)
                total += 1

                if total % 500_000 == 0:
                    log(f"quality: loaded {total}")

    log(f"quality: loaded {total} pubkey quality rows in {time.time() - started:.1f}s")
    return quality


def insert_profiles(
    conn: sqlite3.Connection,
    quality_by_pubkey: dict[str, tuple[float, int]],
) -> int:
    started = time.time()
    rows: list[tuple[str, int, str, str, str, str | None, float, int]] = []
    total = 0

    sql = """
        INSERT INTO profiles (pubkey, created_at, event_json, name, display_name, nip05, rank, follower_cnt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pubkey) DO UPDATE SET
            created_at = excluded.created_at,
            event_json = excluded.event_json,
            name = excluded.name,
            display_name = excluded.display_name,
            nip05 = excluded.nip05,
            rank = excluded.rank,
            follower_cnt = excluded.follower_cnt
        WHERE excluded.created_at >= profiles.created_at
    """

    for row in iter_latest_profiles():
        event_json = build_event_json(row)
        name, display_name, nip05 = extract_profile_fields(row["content"])
        pubkey = clean_text(row["pubkey"])
        profile_rank, follower_cnt = quality_by_pubkey.get(pubkey, (0.0, 0))
        rows.append(
            (
                pubkey,
                int(row["created_at"]),
                event_json,
                name,
                display_name,
                clean_text(nip05) if nip05 else None,
                profile_rank,
                follower_cnt,
            )
        )
        if len(rows) >= SQLITE_BATCH_SIZE:
            with conn:
                conn.executemany(sql, rows)
            total += len(rows)
            rows.clear()
            if total % PROFILE_LOG_EVERY == 0:
                log(f"profiles: indexed {total}")

    if rows:
        with conn:
            conn.executemany(sql, rows)
        total += len(rows)

    with conn:
        conn.execute("DELETE FROM profiles_fts")
        conn.execute(
            """
            INSERT INTO profiles_fts (pubkey, name, display_name, nip05)
            SELECT pubkey, name, display_name, COALESCE(nip05, '')
            FROM profiles
            """
        )

    log(f"profiles: indexed {total} in {time.time() - started:.1f}s")
    return total


def normalize_pubkey(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        if len(value) == 32:
            return value.hex()
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    value = str(value).strip().lower()
    if value.startswith("\\x") and len(value) == 66:
        value = value[2:]
    if len(value) == 64 and all(c in "0123456789abcdef" for c in value):
        return value
    return None


def load_node_id_map(node_id_pubkey_path: Path) -> dict[int, str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(node_id_pubkey_path)
    schema = parquet.schema_arrow

    node_field = None
    pubkey_field = None
    for field in schema:
        name = field.name.lower()
        if node_field is None and pa.types.is_integer(field.type) and "node" in name and "id" in name:
            node_field = field.name
        if pubkey_field is None and ("pubkey" in name or (pa.types.is_binary(field.type) or pa.types.is_string(field.type))):
            pubkey_field = field.name

    if node_field is None:
        for field in schema:
            if pa.types.is_integer(field.type):
                node_field = field.name
                break
    if pubkey_field is None:
        for field in schema:
            if pa.types.is_binary(field.type) or pa.types.is_string(field.type):
                pubkey_field = field.name
                break

    if node_field is None or pubkey_field is None:
        raise RuntimeError(
            f"failed to detect node_id/pubkey columns in {node_id_pubkey_path}"
        )

    log(
        f"node_id map: reading {node_id_pubkey_path} using columns node={node_field} pubkey={pubkey_field}"
    )

    mapping: dict[int, str] = {}
    total = 0
    started = time.time()
    for batch in parquet.iter_batches(batch_size=50_000):
        node_ids = batch.column(batch.schema.get_field_index(node_field)).to_pylist()
        pubkeys = batch.column(batch.schema.get_field_index(pubkey_field)).to_pylist()
        for node_id, pubkey in zip(node_ids, pubkeys):
            normalized = normalize_pubkey(pubkey)
            if normalized is None or node_id is None:
                continue
            mapping[int(node_id)] = normalized
            total += 1
            if total % 500_000 == 0:
                log(f"node_id map: loaded {total}")

    log(f"node_id map: loaded {total} entries in {time.time() - started:.1f}s")
    return mapping


def decode_embedding(value) -> list[float]:
    if value is None:
        raise ValueError("embedding row is null")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) % 4 != 0:
            raise ValueError("embedding blob length is not a multiple of 4")
        dims = len(raw) // 4
        return list(struct.unpack(f"<{dims}f", raw))
    if isinstance(value, list):
        return [float(v) for v in value]
    raise TypeError(f"unsupported embedding value type: {type(value)!r}")


def pack_embedding(values: list[float]) -> bytes:
    return struct.pack(f"<{len(values)}f", *values)


def insert_embeddings(
    conn: sqlite3.Connection,
    embeddings_path: Path,
    node_id_to_pubkey: dict[int, str],
    versioned_model_id: str,
) -> int:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(embeddings_path)
    schema = parquet.schema_arrow

    if "node_id" not in schema.names or "embedding" not in schema.names:
        raise RuntimeError(
            f"expected node_id and embedding columns in {embeddings_path}, got {schema.names}"
        )

    sql = """
        INSERT INTO embeddings (embedding_id, pubkey, model_id, dimensions, embedding)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(embedding_id) DO UPDATE SET
            pubkey = excluded.pubkey,
            model_id = excluded.model_id,
            dimensions = excluded.dimensions,
            embedding = excluded.embedding
    """

    rows: list[tuple[str, str, str, int, bytes]] = []
    total = 0
    skipped_missing_pubkey = 0
    skipped_bad_embedding = 0
    started = time.time()
    log(f"embeddings: reading {embeddings_path}")

    for batch in parquet.iter_batches(batch_size=4_096):
        node_ids = batch.column(batch.schema.get_field_index("node_id")).to_pylist()
        embeddings = batch.column(batch.schema.get_field_index("embedding")).to_pylist()

        for node_id, embedding_value in zip(node_ids, embeddings):
            if node_id is None or embedding_value is None:
                continue

            pubkey = node_id_to_pubkey.get(int(node_id))
            if pubkey is None:
                skipped_missing_pubkey += 1
                continue

            try:
                embedding = decode_embedding(embedding_value)
            except Exception:
                skipped_bad_embedding += 1
                continue

            rows.append(
                (
                    f"{versioned_model_id}_{pubkey}",
                    pubkey,
                    versioned_model_id,
                    len(embedding),
                    sqlite3.Binary(pack_embedding(embedding)),
                )
            )

            if len(rows) >= SQLITE_BATCH_SIZE:
                with conn:
                    conn.executemany(sql, rows)
                total += len(rows)
                rows.clear()
                if total % EMBEDDING_LOG_EVERY == 0:
                    log(f"embeddings: indexed {total}")

    if rows:
        with conn:
            conn.executemany(sql, rows)
        total += len(rows)

    log(
        "embeddings: indexed "
        f"{total} in {time.time() - started:.1f}s "
        f"(skipped_missing_pubkey={skipped_missing_pubkey}, skipped_bad_embedding={skipped_bad_embedding})"
    )
    return total


def main() -> int:
    require_pyarrow()

    db_root = Path(os.getenv("SEARCH_DB_ROOT", DEFAULT_DB_ROOT))
    manifest_path = Path(os.getenv("SEARCH_MANIFEST_PATH", DEFAULT_MANIFEST_PATH))
    embeddings_path = Path(os.getenv("SEARCH_EMBEDDINGS_PATH", DEFAULT_EMBEDDINGS_PATH))
    node_id_pubkey_path = Path(
        os.getenv("SEARCH_NODE_ID_PUBKEY_PATH", DEFAULT_NODE_ID_PUBKEY_PATH)
    )
    dataset_root = Path(os.getenv("SEARCH_DATASET_ROOT", DEFAULT_DATASET_ROOT))

    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    versioned_model_id = model_identifier(manifest)

    log(f"building search sqlite under {db_root}")
    log(f"model identifier: {versioned_model_id}")

    node_id_to_pubkey = load_node_id_map(node_id_pubkey_path)
    feature_files = detect_latest_features_files(dataset_root)
    quality_by_pubkey = load_profile_quality(node_id_to_pubkey, feature_files)

    conn, final_sqlite_path = open_sqlite(db_root)
    try:
        profile_count = insert_profiles(conn, quality_by_pubkey)
        embedding_count = insert_embeddings(
            conn,
            embeddings_path=embeddings_path,
            node_id_to_pubkey=node_id_to_pubkey,
            versioned_model_id=versioned_model_id,
        )
        conn.execute("ANALYZE")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise RuntimeError(f"sqlite integrity_check failed: {integrity!r}")
        conn.commit()
        conn.close()
    except Exception:
        conn.close()
        temp_path = db_root / "search.sqlite.tmp"
        if temp_path.exists():
            temp_path.unlink()
        raise

    temp_path = db_root / "search.sqlite.tmp"
    temp_path.replace(final_sqlite_path)

    log(
        f"done: wrote {final_sqlite_path} "
        f"(profiles={profile_count}, embeddings={embedding_count})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
