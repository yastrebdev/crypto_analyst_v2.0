CREATE TABLE IF NOT EXISTS stg_whale_events (
    txid        VARCHAR(64)     PRIMARY KEY,
    amount_btc  NUMERIC(18, 8)  NOT NULL,
    amount_usd  NUMERIC(18, 8)  NOT NULL,
    block_time  TIMESTAMPTZ     NOT NULL,
    confirmed   BOOLEAN         NOT NULL,
    direction   VARCHAR(24)     NOT NULL,
    created_at  TIMESTAMPTZ     DEFAULT NOW()
)