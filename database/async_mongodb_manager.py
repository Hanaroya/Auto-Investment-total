from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any
import logging

class AsyncMongoDBManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AsyncMongoDBManager, cls).__new__(cls)
        return cls._instance

    async def initialize(self):
        if not hasattr(self, 'client'):
            try:
                self.client = AsyncIOMotorClient('mongodb://localhost:27017/')
                self.db = self.client.crypto_trading
                logging.info("Async MongoDB 연결 성공")
            except Exception as e:
                logging.error(f"Async MongoDB 연결 실패: {str(e)}")
                raise

    async def get_active_trades(self):
        """활성 거래 조회"""
        cursor = self.db.trades.find({'status': 'active'})
        return await cursor.to_list(length=None)

    async def get_thread_status(self, thread_id: int):
        """스레드 상태 조회"""
        return await self.db.thread_status.find_one({'thread_id': thread_id}) 