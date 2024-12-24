from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Dict, Any
import logging
from datetime import datetime
import sys
from config.mongodb_config import MONGODB_CONFIG, INITIAL_SYSTEM_CONFIG
import os
from urllib.parse import quote_plus

class MongoDBManager:
    _instance = None
    """
    MongoDB 연결 및 작업을 관리하는 싱글톤 클래스
    """
    def __new__(cls):
        """
        싱글톤 패턴 구현
        한 번만 인스턴스를 생성하고 이후에는 동일한 인스턴스를 반환합니다.
        """
        if cls._instance is None:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """초기화 메서드
        MongoDB 연결 및 컬렉션 설정을 초기화합니다.
        """
        
        if not hasattr(self, 'initialized'):
            self.client = None
            self.db = None
            self.initialized = True
            self._check_docker_container()
            self._connect()
            self._setup_collections()

    def __del__(self):
        """소멸자에서 연결 종료
        MongoDB 연결을 종료하고 로깅합니다.
        """
        try:
            if hasattr(self, 'client') and self.client and not hasattr(sys, 'is_finalizing'):
                self.client.close()
                logging.info("MongoDB 연결 종료")
        except Exception as e:
            if not hasattr(sys, 'is_finalizing'):
                logging.error(f"MongoDB 연결 종료 실패: {str(e)}")

    def _connect(self):
        """MongoDB 서버에 연결
        MongoDB 연결 URL을 생성하고 MongoClient를 사용하여 연결합니다.
        """
        try:
            # MongoDB 연결 문자열 구성
            username = os.getenv('MONGO_ROOT_USERNAME')
            password = os.getenv('MONGO_ROOT_PASSWORD')
            host = os.getenv('MONGO_HOST', 'localhost')
            port = int(os.getenv('MONGO_PORT', '25000'))
            db_name = os.getenv('MONGO_DB_NAME', 'trading_db')

            # URL 인코딩된 비밀번호 생성
            encoded_password = quote_plus(password)

            # MongoDB 연결 (authSource=admin 추가)
            connection_string = f"mongodb://{username}:{encoded_password}@{host}:{port}/{db_name}?authSource=admin"
            
            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            
            # 연결 테스트
            self.client.server_info()
            logging.info("MongoDB 연결 성공")

        except Exception as e:
            logging.error(f"MongoDB 연결 실패: {str(e)}")
            raise

    def _setup_collections(self):
        """컬렉션 설정 및 인덱스 생성
        컬렉션 참조 설정 및 인덱스 생성을 수행합니다.
        """
        try:
            # 컬렉션 참조 설정
            self.trades = self.db[MONGODB_CONFIG['collections']['trades']]
            self.market_data = self.db[MONGODB_CONFIG['collections']['market_data']]
            self.thread_status = self.db[MONGODB_CONFIG['collections']['thread_status']]
            self.system_config = self.db[MONGODB_CONFIG['collections']['system_config']]

            # 인덱스 생성
            self.trades.create_index([("market", 1), ("timestamp", -1)])
            self.trades.create_index([("status", 1)])
            
            # 시스템 설정 초기화 확인
            self._initialize_system_config()
            
            logging.info("MongoDB 컬렉션 설정 완료")
        except Exception as e:
            logging.error(f"컬렉션 설정 실패: {str(e)}")
            raise

    def _initialize_system_config(self):
        """시스템 설정 초기화
        시스템 설정이 초기화되지 않은 경우 초기 설정을 삽입합니다.
        """
        try:
            if not self.system_config.find_one({'_id': 'config'}):
                initial_config = {
                    '_id': 'config',
                    **INITIAL_SYSTEM_CONFIG,
                    'created_at': datetime.utcnow()
                }
                self.system_config.insert_one(initial_config)
                logging.info("시스템 설정 초기화 완료")
        except Exception as e:
            logging.error(f"시스템 설정 초기화 실패: {str(e)}")
            raise

    def get_collection(self, collection_name: str) -> Collection:
        """특정 컬렉션 반환
        컬렉션 이름을 기반으로 컬렉션 참조를 반환합니다.
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

        Returns:
            Dict[str, Any]: 시스템 설정 데이터
        """
        config = self.db.system_config.find_one({'_id': 'config'})
        return config if config else {}

    def update_system_config(self, config_data: Dict[str, Any]) -> bool:
        """시스템 설정 업데이트
        - 시스템 설정을 업데이트하고 결과를 반환합니다.

        Returns:
            bool: 업데이트 성공 여부
        """
        result = self.db.system_config.update_one(
            {'_id': 'config'},
            {'$set': config_data},
            upsert=True
        )
        return True if result.upserted_id or result.modified_count > 0 else False

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

    def _check_docker_container(self):
        """도커 컨테이너 상태 확인 및 실행
        auto_trading_db 컨테이너가 실행 중인지 확인하고, 없으면 실행합니다.
        """
        try:
            import docker
            client = docker.from_env()
            
            containers = client.containers.list(all=True, filters={'name': 'auto_trading_db'})
            
            if not containers:
                logging.warning("auto_trading_db 컨테이너를 찾을 수 없습니다. 새로 실행합니다.")
                client.containers.run(
                    'mongo:latest',
                    name='auto_trading_db',
                    ports={'25000/tcp': 25000},  # 27017을 25000으로 변경
                    command='mongod --port 25000',  # MongoDB 서버 포트를 25000으로 설정
                    environment={
                        'MONGO_INITDB_ROOT_USERNAME': MONGODB_CONFIG['username'],
                        'MONGO_INITDB_ROOT_PASSWORD': MONGODB_CONFIG['password'],
                        'MONGO_INITDB_DATABASE': MONGODB_CONFIG['db_name']
                    },
                    detach=True
                )
                logging.info("auto_trading_db 컨테이너가 성공적으로 시작되었습니다.")
            else:
                container = containers[0]
                if container.status != 'running':
                    container.start()
                    logging.info("auto_trading_db 컨테이너를 시작했습니다.")
                
                container_info = container.attrs
                port_bindings = container_info['HostConfig']['PortBindings']
                if '25000/tcp' not in port_bindings or port_bindings['25000/tcp'][0]['HostPort'] != '25000':
                    raise Exception("auto_trading_db 컨테이너의 포트가 25000이 아닙니다.")
                
        except Exception as e:
            logging.error(f"도커 컨테이너 확인 중 오류 발생: {str(e)}")
            raise