import psycopg
from clickhouse_driver import Client
import logging

from config import (
    SILVER_POSTGRES_DB,
    SILVER_POSTGRES_USER,
    SILVER_POSTGRES_PASSWORD,
    SILVER_POSTGRES_HOST,
    SILVER_POSTGRES_PORT,
    CLICKHOUSE_DB,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT
)

logger = logging.getLogger(__name__)


def get_pg_connection() -> psycopg.Connection:
    try:
        conn = psycopg.connect(
            dbname=SILVER_POSTGRES_DB,
            user=SILVER_POSTGRES_USER,
            password=SILVER_POSTGRES_PASSWORD,
            host=SILVER_POSTGRES_HOST,
            port=SILVER_POSTGRES_PORT
        )
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

    logger.info(f"Connected to PostgreSQL")

    return conn


def get_ch_client() -> Client:
    try:
        client = Client(
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT
        )
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise

    logger.info(f"Connected to ClickHouse")

    return client