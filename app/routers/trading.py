from fastapi import APIRouter, HTTPException
from typing import Dict, List
import sys
import os

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from trading.trading_manager import TradingManager
from trading.thread_manager import ThreadManager
from trade_market_api.UpbitCall import UpbitCall

router = APIRouter()
trading_manager = TradingManager()
thread_manager = ThreadManager()
upbit = UpbitCall("your_access_key", "your_secret_key")

@router.post("/start")
async def start_trading():
    """거래 시작"""
    try:
        markets = await upbit.get_krw_markets()
        await thread_manager.start(markets)
        return {"status": "success", "message": "거래가 시작되었습니다."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/buy")
async def buy_order(symbol: str, amount: float):
    return await trading_manager.create_buy_order(symbol, amount)

@router.post("/sell")
async def sell_order(symbol: str, amount: float):
    return await trading_manager.create_sell_order(symbol, amount)

@router.get("/status")
async def get_trading_status():
    return await trading_manager.get_status()

@router.post("/stop")
async def stop_trading():
    return await trading_manager.stop_and_liquidate() 