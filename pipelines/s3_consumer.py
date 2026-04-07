import json
import logging
from datetime import datetime

from kafka import KafkaConsumer

from pipelines.storage import init_storage, flush_to_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_to_s3():
    consumer = KafkaConsumer(
        "trades",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="s3-writer"
    )

    init_storage()

    batch = []
    last_flush = datetime.now()

    try:
        for message in consumer:
            batch.append(message.value)

            elapsed = (datetime.now() - last_flush).seconds
            if len(batch) >= 1000 or elapsed >= 60:
                now = datetime.now()
                flush_to_s3(
                    date=now.strftime("%Y-%m-%d"),
                    hour=now.hour,
                    prefix="trades",
                    body=batch
                )
                logger.info(f"Flushed {len(batch)} trades to S3")
                batch = []
                last_flush = datetime.now()
    except Exception as e:
        logger.error(f"Failed to flush batch: {e}")
        raise e


if __name__ == "__main__":
    write_to_s3()


