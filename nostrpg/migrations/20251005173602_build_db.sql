CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    pubkey     TEXT    NOT NULL,
    created_at BIGINT  NOT NULL,
    kind       INTEGER NOT NULL,
    tags       JSONB   NOT NULL,
    content    TEXT    NOT NULL,
    sig        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pubkey ON events (pubkey);
CREATE INDEX IF NOT EXISTS idx_created_at ON events (created_at desc);
CREATE INDEX IF NOT EXISTS idx_kind_created_at ON events (kind asc, created_at desc);
CREATE INDEX IF NOT EXISTS idx_kind ON events (kind);
CREATE INDEX IF NOT EXISTS idx_tags_gin ON events USING GIN (tags jsonb_path_ops);
