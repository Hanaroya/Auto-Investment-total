from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import Dict, List
import asyncio
from datetime import datetime

router = APIRouter()

class ChartConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        
    async def connect(self, websocket: WebSocket, market: str):
        await websocket.accept()
        if market not in self.active_connections:
            self.active_connections[market] = []
        self.active_connections[market].append(websocket)
        
    async def disconnect(self, websocket: WebSocket, market: str):
        self.active_connections[market].remove(websocket)
        
    async def broadcast_market_data(self, market: str, data: dict):
        if market in self.active_connections:
            for connection in self.active_connections[market]:
                try:
                    await connection.send_json(data)
                except:
                    await self.disconnect(connection, market)

manager = ChartConnectionManager()

@router.websocket("/ws/chart/{market}")
async def websocket_endpoint(websocket: WebSocket, market: str):
    await manager.connect(websocket, market)
    try:
        while True:
            # 클라이언트로부터 메시지 수신
            data = await websocket.receive_text()
            
            # 마켓 데이터 조회 및 전송
            market_data = await get_market_data(market)
            await manager.broadcast_market_data(market, market_data)
            
            await asyncio.sleep(1)  # 1초 간격으로 업데이트
    except WebSocketDisconnect:
        await manager.disconnect(websocket, market) 