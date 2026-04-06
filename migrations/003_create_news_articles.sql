CREATE TABLE IF NOT EXISTS stg_news_articles (
    article_id      BIGSERIAL       PRIMARY KEY,
    url             TEXT            NOT NULL UNIQUE,
    source          VARCHAR(255),
    author          VARCHAR(64),
    title           VARCHAR(512)    NOT NULL,
    description     TEXT,
    published_at    TIMESTAMPTZ     NOT NULL,
    content         TEXT,
    sentiment_score NUMERIC(5,3)    NOT NULL,
    sentiment_label VARCHAR(10)     NOT NULL,
    sentiment_model VARCHAR(10)     NOT NULL,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
)