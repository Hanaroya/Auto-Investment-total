from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from typing import Optional, Dict, Any
import logging
from datetime import datetime
import sys

from config.mongodb_config import MONGODB_CONFIG

class MongoDBManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.client = None
            self.db = None
            self.initialized = True
            self._connect()
            self._setup_collections()

    def __del__(self):
        """소멸자에서 연결 종료"""
        try:
            if hasattr(self, 'client') and self.client and not hasattr(sys, 'is_finalizing'):
                self.client.close()
                logging.info("MongoDB 연결 종료")
        except Exception as e:
            if not hasattr(sys, 'is_finalizing'):
                logging.error(f"MongoDB 연결 종료 실패: {str(e)}")

    def _connect(self):
        """MongoDB 연결"""
        try:
            self.client = MongoClient(
                host=MONGODB_CONFIG['host'],
                port=MONGODB_CONFIG['port'],
                serverSelectionTimeoutMS=5000,  # 5초 타임아웃
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            # 연결 테스트
            self.client.server_info()
            self.db = self.client[MONGODB_CONFIG['db_name']]
            logging.info("MongoDB 연결 성공")
        except Exception as e:
            logging.error(f"MongoDB 연결 실패: {str(e)}")
            raise

    def _setup_collections(self):
        """컬렉션 초기화"""
        try:
            self.trades = self.db['trades']
            self.trades.create_index([("market", 1), ("timestamp", -1)])
            self.trades.create_index([("status", 1)])
            logging.info("MongoDB 컬렉션 설정 완료")
        except Exception as e:
            logging.error(f"컬렉션 설정 실패: {str(e)}")
            raise

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

    def _initialize_system_config(self):
        """시스템 설정 초기화"""
        try:
            config_collection = self.db['system_config']
            if not config_collection.find_one({'_id': 'config'}):
                initial_config = {
                    '_id': 'config',
                    'initial_investment': 1000000,  # 초기 투자금
                    'min_trade_amount': 5000,      # 최소 거래금액
                    'max_thread_investment': 80000, # 스레드당 최대 투자금액
                    'reserve_amount': 200000,       # 예비금
                    'total_max_investment': 800000, # 총 최대 투자금액
                    'emergency_stop': False,        # 긴급정지 플래그
                    'created_at': datetime.utcnow()
                }
                config_collection.insert_one(initial_config)
                logging.info("시스템 설정 초기화 완료")
        except Exception as e:
            logging.error(f"시스템 설정 초기화 실패: {str(e)}")
            raise 

    def close(self):
        """MongoDB 연결 종료"""
        try:
            if self.client:
                self.client.close()
                logging.info("MongoDB 연결 종료")
        except Exception as e:
            logging.error(f"MongoDB 연결 종료 실패: {str(e)}")