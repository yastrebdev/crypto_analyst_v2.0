import json
import logging

import psycopg
from kafka import KafkaConsumer

from db import get_pg_connection
from models import Candle
from utils.aggregate_candles import aggregate_candle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def write_to_postgres(candle: Candle, conn: psycopg.Connection):
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO stg_candles (
                symbol, start_time, end_time, open_price, 
                close_price, low_price, high_price, candle_interval,
                volume, trade_count, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, start_time) DO NOTHING
            """,
            (
                candle["symbol"],
                candle["start_time"],
                candle["end_time"],
                candle["open_price"],
                candle["close_price"],
                candle["low_price"],
                candle["high_price"],
                candle["candle_interval"],
                candle["volume"],
                candle["trade_count"],
                candle["created_at"]
            )
        )

        conn.commit()

        logger.info(f"Candle written: {candle['symbol']} {candle['start_time']} trades={candle['trade_count']}")
    except Exception as e:
        conn.rollback()
        logger.warning(f"Not insert candle: {candle['symbol']} {candle['start_time']} trades={candle['trade_count']}")
        raise e


def run_consumer():
    conn = get_pg_connection()

    consumer = KafkaConsumer(
        "trades",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="silver-psql-writer"
    )

    batch = []

    for message in consumer:
        if not batch:
            batch.append(message.value)
        elif message.value["T"] // 60000 > batch[0]["T"] // 60000:
            candle = aggregate_candle(batch, "1m")
            write_to_postgres(candle, conn)
            batch = []
        else:
            batch.append(message.value)


if __name__ == "__main__":
    run_consumer()