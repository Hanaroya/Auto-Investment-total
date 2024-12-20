from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from typing import Optional, Dict, Any
import logging
from datetime import datetime

class MongoDBManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.client: Optional[MongoClient] = None
            self.db: Optional[Database] = None
            self.initialized = True
            self._connect()
            self._setup_collections()

    def _connect(self):
        try:
            # MongoDB 연결 설정
            self.client = MongoClient('mongodb://localhost:27017/')
            self.db = self.client['crypto_trading']
            logging.info("MongoDB 연결 성공")
        except Exception as e:
            logging.error(f"MongoDB 연결 실패: {str(e)}")
            raise

    def _setup_collections(self):
        """컬렉션 초기 설정 및 인덱스 생성"""
        # 거래 기록 컬렉션
        trades_collection = self.db.trades
        trades_collection.create_index([("coin", 1), ("timestamp", -1)])
        trades_collection.create_index([("thread_id", 1)])
        trades_collection.create_index([("status", 1)])

        # 시장 데이터 컬렉션
        market_data_collection = self.db.market_data
        market_data_collection.create_index([("coin", 1), ("timestamp", -1)])

        # 스레드 상태 컬렉션
        thread_status_collection = self.db.thread_status
        thread_status_collection.create_index([("thread_id", 1)], unique=True)

    def get_collection(self, collection_name: str) -> Collection:
        """컬렉션 가져오기"""
        return self.db[collection_name]

    # 거래 관련 메서드
    def insert_trade(self, trade_data: Dict[str, Any]) -> str:
        """새로운 거래 기록 추가"""
        trade_data['timestamp'] = datetime.utcnow()
        result = self.db.trades.insert_one(trade_data)
        return str(result.inserted_id)

    def update_trade(self, trade_id: str, update_data: Dict[str, Any]) -> bool:
        """거래 기록 업데이트"""
        result = self.db.trades.update_one(
            {'_id': trade_id},
            {'$set': update_data}
        )
        return result.modified_count > 0

    # 시장 데이터 관련 메서드
    def update_market_data(self, coin: str, market_data: Dict[str, Any]) -> bool:
        """시장 데이터 업데이트"""
        result = self.db.market_data.update_one(
            {'coin': coin},
            {'$set': market_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False

    # 스레드 상태 관련 메서드
    def update_thread_status(self, thread_id: int, status_data: Dict[str, Any]) -> bool:
        """스레드 상태 업데이트"""
        status_data['last_updated'] = datetime.utcnow()
        result = self.db.thread_status.update_one(
            {'thread_id': thread_id},
            {'$set': status_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False

    # 시스템 설정 관련 메서드
    def get_system_config(self) -> Dict[str, Any]:
        """시스템 설정 가져오기"""
        config = self.db.system_config.find_one({'_id': 'config'})
        return config if config else {}

    def update_system_config(self, config_data: Dict[str, Any]) -> bool:
        """시스템 설정 업데이트"""
        result = self.db.system_config.update_one(
            {'_id': 'config'},
            {'$set': config_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False 