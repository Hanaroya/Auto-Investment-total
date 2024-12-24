from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from typing import Optional, Dict, Any
import logging
from datetime import datetime
import sys

from config.mongodb_config import MONGODB_CONFIG

class MongoDBManager:
    """
    MongoDB 데이터베이스 관리를 위한 싱글톤 클래스
    모든 데이터베이스 연결과 작업을 관리합니다.
    """
    _instance = None

    def __new__(cls):
        """
        싱글톤 패턴 구현
        한 번만 인스턴스화되도록 보장합니다.
        """
        if cls._instance is None:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        초기화 메서드
        - 이미 초기화된 경우 재연결하지 않습니다.
        """
        if not hasattr(self, 'initialized'):
            self.client = None
            self.db = None
            self.initialized = True
            self._connect()
            self._setup_collections()

    def __del__(self):
        """
        소멸자에서 연결 종료
        - 연결이 열려 있는 경우 닫습니다.
        """
        try:
            if hasattr(self, 'client') and self.client and not hasattr(sys, 'is_finalizing'):
                self.client.close()
                logging.info("MongoDB 연결 종료")
        except Exception as e:
            if not hasattr(sys, 'is_finalizing'):
                logging.error(f"MongoDB 연결 종료 실패: {str(e)}")

    def _connect(self):
        """
        MongoDB 서버에 연결을 설정합니다.
        - host, port: 설정 파일에서 정의된 연결 정보 사용
        - 타임아웃: 연결(5초), 소켓(5초), 서버선택(5초) 설정
        - 연결 실패시 예외 발생
        """
        try:
            self.client = MongoClient(
                host=MONGODB_CONFIG['host'],
                port=MONGODB_CONFIG['port'],
                username=MONGODB_CONFIG['username'],
                password=MONGODB_CONFIG['password'],
                authSource='admin',  # 인증 데이터베이스
                serverSelectionTimeoutMS=5000,
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
        """
        필요한 컬렉션들을 초기화하고 인덱스를 생성합니다.
        - trades 컬렉션: 거래 기록 저장
        - 인덱스: market(오름차순), timestamp(내림차순), status(오름차순)
        """
        try:
            self.trades = self.db['trades']
            self.trades.create_index([("market", 1), ("timestamp", -1)])
            self.trades.create_index([("status", 1)])
            logging.info("MongoDB 컬렉션 설정 완료")
        except Exception as e:
            logging.error(f"컬렉션 설정 실패: {str(e)}")
            raise

    def get_collection(self, collection_name: str) -> Collection:
        """
        특정 컬렉션을 가져옵니다.
        """
        return self.db[collection_name]

    # 거래 관련 메서드
    def insert_trade(self, trade_data: Dict[str, Any]) -> str:
        """
        새로운 거래 기록을 데이터베이스에 추가합니다.
        
        Args:
            trade_data: 거래 정보를 담은 딕셔너리
            
        Returns:
            str: 생성된 거래 기록의 ID
        """
        trade_data['timestamp'] = datetime.utcnow()
        result = self.db.trades.insert_one(trade_data)
        return str(result.inserted_id)

    def update_trade(self, trade_id: str, update_data: Dict[str, Any]) -> bool:
        """
        특정 거래 기록을 업데이트합니다.
        
        Args:
            trade_id: 업데이트할 거래의 ID
            update_data: 업데이트할 데이터를 담은 딕셔너리
            
        Returns:
            bool: 업데이트 성공 여부
        """
        result = self.db.trades.update_one(
            {'_id': trade_id},
            {'$set': update_data}
        )
        return result.modified_count > 0

    # 시장 데이터 관련 메서드
    def update_market_data(self, coin: str, market_data: Dict[str, Any]) -> bool:
        """
        특정 코인의 시장 데이터를 업데이트합니다.
        
        Args:
            coin: 코인 식별자
            market_data: 업데이트할 시장 데이터
            
        Returns:
            bool: 업데이트 성공 여부
        """
        result = self.db.market_data.update_one(
            {'coin': coin},
            {'$set': market_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False

    # 스레드 상태 관련 메서드
    def update_thread_status(self, thread_id: int, status_data: Dict[str, Any]) -> bool:
        """
        특정 스레드의 상태 정보를 업데이트합니다.

        Args:
            thread_id: 업데이트할 스레드의 ID
            status_data: 업데이트할 상태 데이터
            
        Returns:
            bool: 업데이트 성공 여부
        """
        status_data['last_updated'] = datetime.utcnow()
        result = self.db.thread_status.update_one(
            {'thread_id': thread_id},
            {'$set': status_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False

    # 시스템 설정 관련 메서드
    def get_system_config(self) -> Dict[str, Any]:
        """
        시스템 설정을 가져옵니다.
        """
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
        """
        시스템 설정을 초기화합니다.
        기본 설정값:
        - initial_investment: 초기 투자금 (1,000,000원)
        - min_trade_amount: 최소 거래금액 (5,000원)
        - max_thread_investment: 스레드당 최대 투자금액 (80,000원)
        - reserve_amount: 예비금 (200,000원)
        - total_max_investment: 총 최대 투자금액 (800,000원)
        - emergency_stop: 긴급정지 플래그
        """
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
        """
        MongoDB 연결 종료
        - 연결이 열려 있는 경우 닫습니다.
        """
        try:
            if self.client:
                self.client.close()
                logging.info("MongoDB 연결 종료")
        except Exception as e:
            logging.error(f"MongoDB 연결 종료 실패: {str(e)}")