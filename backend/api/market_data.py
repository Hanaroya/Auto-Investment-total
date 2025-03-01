from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from database.mongodb_manager import MongoDBManager
from trading.trading_manager import TradingManager
from models.market_data import MarketData, TradeSignal
from datetime import datetime, timedelta
from utils.time_utils import TimeUtils

router = APIRouter(prefix="/api/market", tags=["market"])
db = MongoDBManager(exchange_name="upbit")
trading_manager = TradingManager("upbit")

@router.get("/data/{market}")
async def get_market_data(market: str):
    """특정 마켓의 데이터 조회"""
    try:
        market_data = await db.get_collection("market_data").find_one(
            {"market": market},
            sort=[("timestamp", -1)]
        )
        
        if not market_data:
            raise HTTPException(status_code=404, detail="마켓 데이터를 찾을 수 없습니다")
            
        # ObjectId를 문자열로 변환
        market_data["_id"] = str(market_data["_id"])
        return market_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/candles/{market}")
async def get_candle_data(market: str, interval: str = "240", limit: int = 100):
    """캔들 데이터 조회"""
    try:
        candles = await db.get_collection("candle_data").find(
            {"market": market, "interval": interval},
            sort=[("timestamp", -1)]
        ).limit(limit).to_list(length=limit)
        
        if not candles:
            raise HTTPException(status_code=404, detail="캔들 데이터를 찾을 수 없습니다")
            
        # ObjectId를 문자열로 변환
        for candle in candles:
            candle["_id"] = str(candle["_id"])
        return candles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/daily-profit")
async def get_daily_profit(days: int = 7):
    """일일 수익 데이터 조회"""
    try:
        start_date = TimeUtils.get_current_kst() - timedelta(days=days)
        profits = await db.get_collection("daily_profit").find(
            {"timestamp": {"$gte": start_date}},
            sort=[("timestamp", -1)]
        ).to_list(length=days)
        
        if not profits:
            raise HTTPException(status_code=404, detail="수익 데이터를 찾을 수 없습니다")
            
        # ObjectId를 문자열로 변환
        for profit in profits:
            profit["_id"] = str(profit["_id"])
        return profits
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/trade/buy")
async def process_buy_signal(signal: TradeSignal):
    """매수 신호 처리"""
    try:
        result = trading_manager.process_buy_signal(
            market=signal.market,
            exchange=signal.exchange,
            thread_id=signal.thread_id,
            signal_strength=signal.signal_strength,
            price=signal.price,
            strategy_data=signal.strategy_data
        )
        return {"success": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/trade/sell")
async def process_sell_signal(signal: TradeSignal):
    """매도 신호 처리"""
    try:
        result = trading_manager.process_sell_signal(
            market=signal.market,
            exchange=signal.exchange,
            thread_id=signal.thread_id,
            signal_strength=signal.signal_strength,
            price=signal.price,
            strategy_data=signal.strategy_data
        )
        return {"success": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 