CREATE TABLE IF NOT EXISTS stg_social_posts (
    post_id         VARCHAR(255) PRIMARY KEY,
    source          VARCHAR(20)  NOT NULL,
    title           VARCHAR(255) NOT NULL,
    body            TEXT,
    url             TEXT         NOT NULL,
    sentiment_score NUMERIC(5,3) NOT NULL,
    sentiment_label VARCHAR(10)  NOT NULL,
    sentiment_model VARCHAR(10)  NOT NULL,
    published_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
)