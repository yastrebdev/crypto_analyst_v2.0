from datetime import datetime, timezone
from typing import Literal

from models import Candle

CandleInterval = Literal["1m", "5m", "10m"]


def aggregate_candle(
    trades: list[dict],
    candle_interval: CandleInterval
) -> Candle:
    sort_trades_by_trade_time = sorted(trades, key=lambda t: t["T"])
    first_trade = sort_trades_by_trade_time[0]
    last_trade = sort_trades_by_trade_time[-1]

    prices = [float(t["p"]) for t in trades]
    low_price = min(prices)
    high_price = max(prices)

    volume = sum(float(t["q"]) for t in trades)
    trade_count = len(trades)

    first_trade["T"] = datetime.fromtimestamp(first_trade["T"] / 1000, tz=timezone.utc)
    last_trade["T"] = datetime.fromtimestamp(last_trade["T"] / 1000, tz=timezone.utc)

    return {
        "symbol": first_trade["s"],
        "start_time": first_trade["T"],
        "end_time": last_trade["T"],
        "open_price": float(first_trade["p"]),
        "close_price": float(last_trade["p"]),
        "low_price": low_price,
        "high_price": high_price,
        "candle_interval": candle_interval,
        "volume": volume,
        "trade_count": trade_count,
        "created_at": datetime.now()
    }