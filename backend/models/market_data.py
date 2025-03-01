from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime

class MarketData(BaseModel):
    market: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    interval: str

class TradeSignal(BaseModel):
    market: str
    exchange: str
    thread_id: str
    signal_strength: float
    price: float
    strategy_data: Dict[str, Any]

class DailyProfit(BaseModel):
    date: datetime
    total_profit: float
    trade_count: int
    win_rate: float
    markets: Dict[str, float]
    details: Optional[Dict[str, Any]] 