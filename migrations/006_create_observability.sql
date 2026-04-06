CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL   PRIMARY KEY,
    pipeline        VARCHAR(50) NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20),
    rows_ingested   INTEGER,
    error_msg       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
)