from fastapi import APIRouter
from .websocket import router as websocket_router
from typing import List, Dict, Any
from datetime import datetime

router = APIRouter()

# 웹소켓 라우터 포함
router.include_router(websocket_router)

# 기본 API 엔드포인트
@router.get("/")
async def root():
    return {"message": "Trading API System is running"}

@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

# 마켓 데이터 관련 엔드포인트
@router.get("/markets")
async def get_markets() -> List[str]:
    """사용 가능한 마켓 목록 조회"""
    # TODO: 실제 마켓 데이터 조회 로직 구현 필요
    return ["KRW-BTC", "KRW-ETH", "KRW-XRP"]

@router.get("/market/{market_id}")
async def get_market_info(market_id: str) -> Dict[str, Any]:
    """특정 마켓의 상세 정보 조회"""
    # TODO: 실제 마켓 정보 조회 로직 구현 필요
    return {
        "market": market_id,
        "current_price": 0,
        "change_rate": 0,
        "volume": 0,
        "timestamp": datetime.now().isoformat()
    }

# 거래 관련 엔드포인트
@router.get("/trades/active")
async def get_active_trades() -> List[Dict[str, Any]]:
    """활성 거래 목록 조회"""
    # TODO: 실제 활성 거래 조회 로직 구현 필요
    return []

@router.get("/trades/history")
async def get_trade_history() -> List[Dict[str, Any]]:
    """거래 히스토리 조회"""
    # TODO: 실제 거래 히스토리 조회 로직 구현 필요
    return []

# 시스템 상태 관련 엔드포인트
@router.get("/system/status")
async def get_system_status() -> Dict[str, Any]:
    """시스템 상태 조회"""
    return {
        "status": "running",
        "uptime": "0:00:00",
        "active_threads": 0,
        "memory_usage": "0MB",
        "timestamp": datetime.now().isoformat()
    } 