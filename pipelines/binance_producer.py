import asyncio
import json
import logging
from datetime import datetime

import websockets
from kafka import KafkaProducer

from config import BINANCE_WS_URL
from pipelines.storage import flush_to_s3
from quality.contract_validator import validate

logger = logging.getLogger(__name__)

BTCUSDT_TRADE_URL = BINANCE_WS_URL + "btcusdt@trade"

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


async def get_trades():
    async with websockets.connect(BTCUSDT_TRADE_URL) as ws:
        batch = []
        last_flush = datetime.now()

        while True:
            message = await ws.recv()
            data = json.loads(message)
            violations = validate(data, "binance_trade")

            if violations:
                logger.warning(f"Contract violations: {violations}")
                continue

            producer.send("trades", value=data)

            batch.append(data)

            if len(batch) >= 1000 or (datetime.now() - last_flush).seconds >= 60:
                now = datetime.now()
                date = now.strftime("%Y-%m-%d")
                hour = now.hour
                flush_to_s3(
                    date=date,
                    hour=hour,
                    prefix="trades",
                    body=batch
                )

                logger.info(f"Flushed {len(batch)} trades to S3")

                batch = []
                last_flush = datetime.now()


try:
    asyncio.run(get_trades())
except KeyboardInterrupt:
    logger.info("Producer stopped")
except Exception as e:
    logger.error(f"The connection is broken: {e}")
    raise
finally:
    producer.flush()