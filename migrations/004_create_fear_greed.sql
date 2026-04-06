CREATE TABLE IF NOT EXISTS stg_fear_greed (
    id          BIGSERIAL   PRIMARY KEY,
    fg_index    INTEGER     NOT NULL CHECK (fg_index >= 0 AND fg_index <= 100),
    record_date DATE        NOT NULL UNIQUE,
    label       VARCHAR(16) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
)