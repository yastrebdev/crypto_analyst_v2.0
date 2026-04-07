import asyncio
import json
import logging

import websockets
from kafka import KafkaProducer

from config import BINANCE_WS_URL
from quality.contract_validator import validate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BTCUSDT_TRADE_URL = BINANCE_WS_URL + "btcusdt@trade"


async def get_trades():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    try:
        async with websockets.connect(BTCUSDT_TRADE_URL) as ws:
            while True:
                message = await ws.recv()
                data = json.loads(message)
                violations = validate(data, "binance_trade")

                if violations:
                    logger.warning(f"Contract violations: {violations}")
                    continue

                producer.send("trades", value=data)
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    except Exception as e:
        logger.error(f"The connection is broken: {e}")
        raise
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    asyncio.run(get_trades())

