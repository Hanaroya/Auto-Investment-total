from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any
import logging
from config.mongodb_config import MONGODB_CONFIG, INITIAL_SYSTEM_CONFIG

class AsyncMongoDBManager:
    """
    MongoDB 비동기 연결 및 작업을 관리하는 싱글톤 클래스
    데이터베이스 연결과 쿼리 작업을 비동기적으로 처리합니다.
    """
    _instance = None

    def __new__(cls):
        """
        싱글톤 패턴 구현
        한 번만 인스턴스를 생성하고 이후에는 동일한 인스턴스를 반환합니다.
        """
        if cls._instance is None:
            cls._instance = super(AsyncMongoDBManager, cls).__new__(cls)
        return cls._instance

    async def initialize(self):
        """
        MongoDB 데이터베이스 연결 초기화
        """
        if not hasattr(self, 'client'):
            try:
                connection_url = (
                    f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}"
                    f"@{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/{MONGODB_CONFIG['db_name']}"
                )
                self.client = AsyncIOMotorClient(
                    connection_url,
                    authSource=MONGODB_CONFIG['db_name']
                )
                self.db = self.client[MONGODB_CONFIG['db_name']]
                
                # 시스템 설정 초기화 확인
                await self._ensure_system_config()
                
                logging.info("Async MongoDB 연결 성공")
            except Exception as e:
                logging.error(f"Async MongoDB 연결 실패: {str(e)}")
                raise

    async def _ensure_system_config(self):
        """시스템 설정이 없는 경우 초기 설정을 생성합니다."""
        config_collection = self.db[MONGODB_CONFIG['collections']['system_config']]
        if await config_collection.count_documents({}) == 0:
            await config_collection.insert_one(INITIAL_SYSTEM_CONFIG)
            logging.info("시스템 초기 설정이 생성되었습니다.")

    async def get_active_trades(self):
        """
        활성 상태인 거래 내역 조회
        
        Returns:
            list: 상태가 'active'인 모든 거래 목록
        """
        cursor = self.db.trades.find({'status': 'active'})
        return await cursor.to_list(length=None)

    async def get_thread_status(self, thread_id: int):
        """
        특정 스레드의 상태 정보 조회
        
        Args:
            thread_id (int): 조회할 스레드의 ID
            
        Returns:
            dict: 스레드 상태 정보를 담은 문서
            None: 해당 스레드 ID가 존재하지 않는 경우
        """
        return await self.db.thread_status.find_one({'thread_id': thread_id}) 