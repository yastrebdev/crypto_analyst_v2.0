import asyncio
import json
import logging
from datetime import datetime

import websockets
from kafka import KafkaProducer

from config import BINANCE_WS_URL
from pipelines.storage import flush_to_s3, init_storage
from quality.contract_validator import validate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BTCUSDT_TRADE_URL = BINANCE_WS_URL + "btcusdt@trade"


async def get_trades():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    init_storage()

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

    producer.flush()


try:
    asyncio.run(get_trades())
except KeyboardInterrupt:
    logger.info("Producer stopped")
except Exception as e:
    logger.error(f"The connection is broken: {e}")
    raise