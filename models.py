from typing import TypedDict
from datetime import datetime

class Candle(TypedDict):
    symbol: str
    start_time: datetime
    end_time: datetime
    open_price: float
    close_price: float
    low_price: float
    high_price: float
    volume: float
    trade_count: int
    candle_interval: str
    created_at: datetime