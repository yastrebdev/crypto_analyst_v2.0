CREATE TABLE IF NOT EXISTS stg_candles (
    symbol          VARCHAR(10)     NOT NULL,
    start_time      TIMESTAMPTZ     NOT NULL,
    end_time        TIMESTAMPTZ     NOT NULL,
    candle_interval VARCHAR(10)     NOT NULL,
    open_price      NUMERIC(18, 8)  NOT NULL,
    close_price     NUMERIC(18, 8)  NOT NULL,
    low_price       NUMERIC(18, 8)  NOT NULL,
    high_price      NUMERIC(18, 8)  NOT NULL,
    volume          NUMERIC(20, 8)  NOT NULL,
    trade_count     INTEGER,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (symbol, start_time)
)