from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Dict, Any, List
import logging
from utils.time_utils import TimeUtils
import sys
from config.mongodb_config import MONGODB_CONFIG, INITIAL_SYSTEM_CONFIG
import os
from urllib.parse import quote_plus
import motor
import asyncio
import time
import threading

class MongoDBManager:
    _instance = None
    _instance_lock = threading.Lock()  # 인스턴스 생성을 위한 락 추가
    """
    MongoDB 연결 및 작업을 관리하는 싱글톤 클래스
    """
    _collection_locks = {
        'portfolio': threading.Lock(),
        'trades': threading.Lock(),
        'strategy_data': threading.Lock(),
        'market_data': threading.Lock(),
        'thread_status': threading.Lock(),
        'system_config': threading.Lock(),
        'market_index': threading.Lock()
    }

    def __new__(cls, *args, **kwargs):
        """스레드 안전한 싱글톤 패턴 구현"""
        if cls._instance is None:
            with cls._instance_lock:  # 인스턴스 생성 시 락 사용
                if cls._instance is None:  # double-checking
                    cls._instance = super(MongoDBManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, exchange_name: str):
        """초기화는 한 번만 수행"""
        if not getattr(self, '_initialized', False):
            with self._instance_lock:
                if not getattr(self, '_initialized', False):
                    self.logger = logging.getLogger('investment_center')
                    
                    try:
                        # Docker 컨테이너 상태 확인 및 실행
                        container_status = self._check_docker_container()
                        
                        # MongoDB 설정 가져오기
                        config = MONGODB_CONFIG
                        
                        # 새로 생성된 컨테이너인 경우에만 사용자 생성 시도
                        if container_status == 'new':
                            self.logger.info("새 컨테이너 감지: 사용자 생성 시도")
                            self._create_mongodb_user()
                        elif container_status == 'running':
                            self.logger.info("기존 컨테이너 감지: 사용자 생성 건너뜀")
                        
                        # 연결 문자열 로깅 (비밀번호는 가림)
                        safe_connection_string = f'mongodb://{config["username"]}:****@{config["host"]}:{config["port"]}/{config["db_name"]}?authSource=admin'
                        self.logger.info(f"MongoDB 연결 시도: {safe_connection_string}")
                        
                        # 동기식 클라이언트로 연결
                        self.client = MongoClient(
                            host=config['host'],
                            port=config['port'],
                            username=os.getenv('MONGO_ROOT_USERNAME'),
                            password=os.getenv('MONGO_ROOT_PASSWORD'),
                            authSource='admin'
                        )
                        
                        # 데이터베이스와 컬렉션 설정
                        self.db = self.client[config['db_name']]

                        # 컬렉션 설정
                        self._setup_collections()
                        self.exchange_name = exchange_name
                        
                        # 시스템 설정 초기화 (추가)
                        self._initialize_system_config()
                        
                        self._initialized = True
                        self.logger.debug("MongoDBManager 인스턴스 초기화 완료")
                        
                    except Exception as e:
                        self.logger.error(f"MongoDB 연결 실패: {str(e)}")
                        raise

    def update_system_config(self, config_data: Dict[str, Any]) -> bool:
        """시스템 설정 업데이트"""
        with self._get_collection_lock('system_config'):
            try:
                result = self.system_config.update_one(
                    {'_id': 'system_config'},
                    {'$set': config_data},
                    upsert=True
                )
                # modified_count 또는 upserted_id가 있는 경우 성공
                return bool(result.modified_count > 0 or result.upserted_id is not None)
            except Exception as e:
                self.logger.error(f"시스템 설정 업데이트 중 오류: {str(e)}")
                return False

    def test_connection(self):
        """동기식 연결 테스트"""
        try:
            # 현재 연결 확인
            if not hasattr(self, 'client') or self.client is None:
                # 환경 변수 값 직접 확인
                username = os.getenv('MONGO_ROOT_USERNAME')
                password = os.getenv('MONGO_ROOT_PASSWORD')
                host = os.getenv('MONGO_HOST', 'localhost')
                port = int(os.getenv('MONGO_PORT', '25000'))
                db_name = os.getenv('MONGO_DB_NAME', 'trading_db')
                
                # 연결 정보 로깅
                self.logger.info("=== MongoDB 연결 정보 ===")
                self.logger.info(f"Username: {username}")
                self.logger.info(f"Host: {host}")
                self.logger.info(f"Port: {port}")
                self.logger.info(f"DB: {db_name}")
                
                self.client = MongoClient(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    authSource='admin',
                    serverSelectionTimeoutMS=5000
                )
                self.db = self.client[db_name]
            
            # 연결 테스트
            self.client.admin.command('ping')
            self.logger.info("MongoDB 연결 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"MongoDB 연결 테스트 실패: {str(e)}")
            self.logger.error(f"상세 오류: {str(e.__dict__)}")
            return False

    def initialize(self):
        """동기식 초기화"""
        try:
            # MongoDB 접속 정보
            username = quote_plus(os.getenv('MONGO_ROOT_USERNAME'))
            password = quote_plus(os.getenv('MONGO_ROOT_PASSWORD'))
            host = os.getenv('MONGO_HOST', 'localhost')
            port = int(os.getenv('MONGO_PORT', '25000'))
            db_name = os.getenv('MONGO_DB_NAME', 'trading_db')
            
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?authSource=admin"
            self.logger.info(f"MongoDB 연결 시도: {connection_string.replace(password, '****')}")
            
            # 동기식 클라이언트로 연결
            self.client = MongoClient(
                host=host,
                port=port,
                username=username,
                password=password,
                authSource='admin'
            )
            self.db = self.client[db_name]
            self.logger.info("MongoDB 연결 성공")
            
        except Exception as e:
            self.logger.error(f"MongoDB 연결 실패: {str(e)}")
            raise

    def _check_docker_container(self):
        """도커 컨테이너 상태 확인 및 실행"""
        try:
            import docker
            client = docker.from_env()
            
            try:
                # 기존 컨테이너 확인
                container = client.containers.get('auto_trading_db')
                
                # 컨테이너가 실행 중이 아니면 시작
                if container.status != 'running':
                    self.logger.info("기존 auto_trading_db 컨테이너 시작")
                    container.start()
                    time.sleep(5)  # 컨테이너 시작 대기
                
                self.logger.info("기존 auto_trading_db 컨테이너가 실행 중입니다")
                return 'running'
                    
            except docker.errors.NotFound:
                # MongoDB 설정 가져오기
                config = MONGODB_CONFIG
                
                # 컨테이너가 없는 경우에만 새로 생성
                self.logger.info("새로운 auto_trading_db 컨테이너 생성")
                container = client.containers.run(
                    'mongo:latest',
                    name='auto_trading_db',
                    ports={'27017/tcp': config['port']},
                    environment={
                        'MONGO_INITDB_ROOT_USERNAME': os.getenv('MONGO_ROOT_USERNAME'),
                        'MONGO_INITDB_ROOT_PASSWORD': os.getenv('MONGO_ROOT_PASSWORD'),
                        'MONGO_INITDB_DATABASE': config['db_name']
                    },
                    detach=True
                )
                
                # 새 컨테이너 시작 대기
                self.logger.info("MongoDB 컨테이너 시작 대기 중...")
                time.sleep(10)
                
                # 컨테이너 상태 확인
                container.reload()
                if container.status == 'running':
                    self.logger.info("새 MongoDB 컨테이너가 정상적으로 실행 중입니다")
                    return 'new'
                
                self.logger.error("MongoDB 컨테이너가 실행되지 않았습니다")
                return 'failed'
            
        except Exception as e:
            self.logger.error(f"도커 컨테이너 확인 중 오류 발생: {str(e)}")
            raise

    def _create_mongodb_user(self):
        """MongoDB 사용자 생성"""
        try:
            config = MONGODB_CONFIG
            
            # 환경 변수 값 직접 확인
            root_username = os.getenv('MONGO_ROOT_USERNAME')
            root_password = os.getenv('MONGO_ROOT_PASSWORD')
            self.logger.debug(f"Root 계정으로 연결 시도 - username: {root_username}")
            
            # root 계정으로 연결
            admin_client = MongoClient(
                host=config['host'],
                port=config['port'],
                username=root_username,
                password=root_password,
                authSource='admin'
            )
            
            admin_db = admin_client['admin']
            
            try:
                # 먼저 사용자 존재 여부 확인
                try:
                    user_info = admin_db.command('usersInfo', {
                        'user': config['username'],
                        'db': config['db_name']
                    })
                    
                    if user_info.get('users'):
                        self.logger.info(f"사용자 '{config['username']}'가 이미 존재합니다. 사용자 생성을 건너뜁니다.")
                        return
                        
                except Exception as e:
                    self.logger.error(f"사용자 정보 확인 중 오류: {str(e)}")
                    # 사용자 정보 확인 실패 시에도 계속 진행 (사용자가 없을 수 있음)
                
                # 사용자가 없는 경우에만 생성
                self.logger.info(f"새 사용자 '{config['username']}' 생성 시도")
                admin_db.command(
                    'createUser',
                    config['username'],
                    pwd=config['password'],
                    roles=[{'role': 'readWrite', 'db': config['db_name']}]
                )
                self.logger.info("MongoDB 사용자 생성 완료")
                
            except Exception as e:
                if 'already exists' in str(e):
                    self.logger.info(f"사용자 '{config['username']}'가 이미 존재합니다.")
                else:
                    self.logger.error(f"사용자 생성 중 오류: {str(e)}")
                    raise
            
            finally:
                admin_client.close()
            
        except Exception as e:
            self.logger.error(f"MongoDB 사용자 생성 실패: {str(e)}")
            raise

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

    def _setup_collections(self):
        """컬렉션 설정 및 인덱스 생성"""
        try:
            # 컬렉션 초기화
            self.trades = self.db['trades']
            self.market_data = self.db[MONGODB_CONFIG['collections']['market_data']]
            self.thread_status = self.db[MONGODB_CONFIG['collections']['thread_status']]
            self.system_config = self.db['system_config']
            self.strategy_data = self.db['strategy_data']
            self.trading_history = self.db['trading_history']
            self.daily_profit = self.db['daily_profit']
            self.portfolio = self.db['portfolio']
            self.market_index = self.db['market_index']  # AFR 데이터를 위한 컬렉션 추가
            
            # 인덱스 생성
            self.trades.create_index([("market", 1), ("thread_id", 1), ("status", 1)])
            self.trades.create_index([("thread_id", 1)])
            self.strategy_data.create_index([("market", 1), ("timestamp", -1)])
            self.thread_status.create_index([("thread_id", 1), ("exchange", 1)])
            self.daily_profit.create_index([("timestamp", -1)])
            self.portfolio.create_index([("_id", 1), ("exchange", 1)])
            self.market_index.create_index([
                ("exchange", 1),
                ("timestamp", -1)
            ])
            self.market_index.create_index([("timestamp", -1)])
            
            self.logger.info("컬렉션 및 인덱스 설정 완료")
            
        except Exception as e:
            self.logger.error(f"컬렉션 설정 실패: {str(e)}")
            raise

    def _initialize_system_config(self):
        """시스템 설정 초기화
        시스템 설정이 초기화되지 않은 경우에만 초기 설정을 삽입합니다.
        """
        try:
            # 기존 설정 확인
            existing_config = self.system_config.find_one({'exchange': self.exchange_name})
            if not existing_config:
                initial_config = {
                    'exchange': self.exchange_name,
                    'initial_investment': float(os.getenv('INITIAL_INVESTMENT', 1000000)),
                    'min_trade_amount': float(os.getenv('MIN_TRADE_AMOUNT', 5000)),
                    'max_thread_investment': float(os.getenv('MAX_THREAD_INVESTMENT', 80000)),
                    'reserve_amount': float(os.getenv('RESERVE_AMOUNT', 200000)),
                    'total_max_investment': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'emergency_stop': False,
                    'created_at': TimeUtils.get_current_kst()
                }
                self.system_config.insert_one(initial_config)
                self.logger.info("시스템 설정 초기화 완료")
                
                # 설정값 로깅
                self.logger.info(f"초기 투자금: {initial_config['initial_investment']:,}원")
                self.logger.info(f"최소 거래금액: {initial_config['min_trade_amount']:,}원")
                self.logger.info(f"스레드당 최대 투자금: {initial_config['max_thread_investment']:,}원")
                self.logger.info(f"예비금: {initial_config['reserve_amount']:,}원")
                self.logger.info(f"총 최대 투자금: {initial_config['total_max_investment']:,}원")
            else:
                self.logger.info("기존 시스템 설정이 존재합니다. 초기화를 건너뜁니다.")
                
        except Exception as e:
            self.logger.error(f"시스템 설정 초기화 실패: {str(e)}")
            raise

    def _initialize_portfolio(self):
        """포트폴리오 초기 설정
        포트폴리오가 없는 경우에만 초기화합니다.
        """
        try:
            # 기존 포트폴리오 확인
            existing_portfolio = self.portfolio.find_one({'exchange': self.exchange_name})
            if not existing_portfolio:
                initial_portfolio = {
                    'market_list': {},
                    'exchange': self.exchange_name,
                    'investment_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000)),
                    'available_investment': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'reserve_amount': float(os.getenv('RESERVE_AMOUNT', 200000)),
                    'current_amount': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'profit_earned': 0,
                        'created_at': TimeUtils.get_current_kst(),
                    'last_updated': TimeUtils.get_current_kst()
                }
                self.portfolio.insert_one(initial_portfolio)
                self.logger.info("포트폴리오 초기화 완료")
            else:
                self.logger.info("기존 포트폴리오가 존재합니다. 초기화를 건너뜁니다.")
                
        except Exception as e:
            self.logger.error(f"포트폴리오 초기화 실패: {str(e)}")
            raise

    def update_daily_profit_report_status(self, reported: bool = True) -> bool:
        """일일 수익 리포트 상태 업데이트"""
        try:
            today = TimeUtils.get_current_kst().replace(hour=0, minute=0, second=0, microsecond=0)
            result = self.daily_profit.update_one(
                {'date': today},
                {'$set': {'reported': reported}}
            )
            return bool(result.modified_count > 0)
        except Exception as e:
            self.logger.error(f"일일 수익 리포트 상태 업데이트 실패: {str(e)}")
            return False

    def update_daily_profit(self, profit_data: Dict[str, Any]) -> bool:
        """일일 수익 업데이트"""
        with self._get_collection_lock('daily_profit'):
            try:
                profit_data['timestamp'] = TimeUtils.get_current_kst()
                result = self.daily_profit.insert_one(profit_data)
                return bool(result.inserted_id)
            except Exception as e:
                self.logger.error(f"일일 수익 업데이트 실패: {str(e)}")
                return False

    def update_portfolio(self, update_data: Dict[str, Any]) -> bool:
        """포트폴리오 업데이트"""
        with self._get_collection_lock('portfolio'):
            try:
                update_data['last_updated'] = TimeUtils.get_current_kst()
                result = self.portfolio.update_one(
                    {'exchange': update_data['exchange']},
                    {'$set': update_data},
                    upsert=True
                )
                return bool(result.modified_count > 0 or result.upserted_id)
            except Exception as e:
                self.logger.error(f"포트폴리오 업데이트 실패: {str(e)}")
                return False

    def get_portfolio(self, exchange_name: str) -> Dict:
        """현재 포트폴리오 조회 및 없으면 생성"""
        try:
            # 포트폴리오 조회
            portfolio = self.db.portfolio.find_one({'exchange': exchange_name})
            
            # 포트폴리오가 없으면 새로 생성
            if not portfolio:
                portfolio = {
                    'market_list': {},
                    'exchange': exchange_name,
                    'investment_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000)),
                    'available_investment': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'reserve_amount': float(os.getenv('RESERVE_AMOUNT', 200000)),
                    'current_amount': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'profit_earned': 0,
                    'created_at': TimeUtils.get_current_kst(),
                    'last_updated': TimeUtils.get_current_kst()
                }
                
                # 새 포트폴리오 저장
                self.db.portfolio.insert_one(portfolio)
                self.logger.info("새 포트폴리오 생성 완료")
                
            return portfolio
            
        except Exception as e:
            self.logger.error(f"포트폴리오 조회/생성 실패: {str(e)}")
            return {}

    def get_collection(self, name):
        """비동기 컬렉션 반환"""
        return self.async_db[name]

    # 거래 관련 메서드
    def insert_trade(self, trade_data: Dict[str, Any]) -> str:
        """
        새로운 거래 기록을 데이터베이스에 추가합니다.
        
        Args:
            trade_data: 거래 정보를 담은 딕셔너리
            
        Returns:
            str: 생성된 거래 기록의 ID
        """
        with self._get_collection_lock('trades'):
            try:
                # KST 시간을 MongoDB용 UTC로 변환
                kst_time = TimeUtils.get_current_kst()
                trade_data['timestamp'] = TimeUtils.to_mongo_date(kst_time)
                result = self.trades.insert_one(trade_data)
                return str(result.inserted_id)
            except Exception as e:
                self.logger.error(f"거래 기록 추가 실패: {str(e)}")
                return None
                
    def get_trade(self, query: Dict) -> Dict:
        """거래 기록 조회"""
        trade = self.trades.find_one(query)
        if trade and 'timestamp' in trade:
            # MongoDB의 UTC를 KST로 변환
            trade['timestamp'] = TimeUtils.from_mongo_date(trade['timestamp'])
        return trade

    def update_trade(self, trade_id: str, update_data: Dict[str, Any]) -> bool:
        """
        특정 거래 기록을 업데이트합니다.
        
        Args:
            trade_id: 업데이트할 거래의 ID
            update_data: 업데이트할 데이터를 담은 딕셔너리
            
        Returns:
            bool: 업데이트 성공 여부
        """
        with self._get_collection_lock('trades'):
            try:
                result = self.trades.update_one(
                    {'_id': trade_id},
                    {'$set': update_data}
                )
                return result.modified_count > 0
            except Exception as e:
                self.logger.error(f"거래 기록 업데이트 실패: {str(e)}")
                return False

    # 시장 데이터 관련 메서드
    def update_market_data(self, exchange: str, market: str, market_data: Dict[str, Any]) -> bool:
        """
        특정 마켓의 시장 데이터를 업데이트합니다.
        
        Args:
            exchange: 거래소 이름
            market: 마켓 식별자
            market_data: 업데이트할 시장 데이터
            
        Returns:
            bool: 업데이트 성공 여부
        """
        with self._get_collection_lock('market_data'):
            try:
                result = self.db.market_data.update_one(
                    {'market': market, 'exchange': exchange},
                    {'$set': market_data},
                    upsert=True
                )
                return True if result.upserted_id or result.modified_count > 0 else False
            except Exception as e:
                self.logger.error(f"시장 데이터 업데이트 실패: {str(e)}")
                return False

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
        with self._get_collection_lock('thread_status'):
            status_data['last_updated'] = TimeUtils.get_current_kst()
            result = self.db.thread_status.update_one(
                {
                    'thread_id': thread_id,
                    'exchange': status_data['exchange']
                },
                {'$set': status_data},
                upsert=True
            )
            return True if result.upserted_id or result.modified_count > 0 else False

    # 시스템 설정 관련 메서드
    def get_system_config(self, exchange_name: str) -> Dict[str, Any]:
        """
        시스템 설정을 가져옵니다.

        Returns:
            Dict[str, Any]: 시스템 설정 데이터
        """
        config = self.db.system_config.find_one({'exchange': exchange_name})
        return config if config else {}

    def get_sync_collection(self, name: str):
        """동기식 컬렉션 반환
        컬렉션이 없으면 새로 생성합니다.

        Args:
            name (str): 컬렉션 이름

        Returns:
            Collection: MongoDB 컬렉션 객체
        """
        try:
            # 컬렉션 존재 여부 확인
            if name not in self.db.list_collection_names():
                self.logger.info(f"새로운 컬렉션 '{name}' 생성")
                # 컬렉션 생성 및 기본 문서 삽입
                self.db[name].insert_one({
                    '_id': 'init',
                    'created_at': TimeUtils.get_current_kst(),
                    'status': 'initialized'
                })
                
                # 컬렉션 별 기본 인덱스 설정
                if name == 'scheduled_tasks':
                    self.db[name].create_index([('last_updated', -1)])
                    self.db[name].create_index([('status', 1)])
                    self.logger.info(f"'{name}' 컬렉션 인덱스 생성 완료")

            return self.db[name]

        except Exception as e:
            self.logger.error(f"컬렉션 '{name}' 가져오기/생성 실패: {str(e)}")
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

    def save_strategy_data(self, market: str, exchange: str, strategy_data: Dict[str, Any]) -> bool:
        """마켓별 전략 데이터 저장

        Args:
            market: 마켓 심볼
            exchange: 거래소 이름
            strategy_data: 전략 데이터 딕셔너리

        Returns:
            bool: 저장 성공 여부
        """
        with self._get_collection_lock('strategy_data'):
            try:
                document = {
                    'market': market,
                    'exchange': exchange,
                    'timestamp': TimeUtils.get_current_kst(), 
                    'current_price': strategy_data.get('current_price', 0),
                    'strategies': {
                        'rsi': {
                            'value': strategy_data.get('rsi', 0),
                            'signal': strategy_data.get('rsi_signal', 0),
                            'buy_threshold': strategy_data.get('rsi_buy_threshold', 30),
                            'sell_threshold': strategy_data.get('rsi_sell_threshold', 70)
                        },
                        'macd': {
                            'macd': strategy_data.get('macd', 0),
                            'signal': strategy_data.get('macd_signal', 0),
                            'histogram': strategy_data.get('macd_hist', 0),
                            'buy_threshold': strategy_data.get('macd_buy_threshold', 0),
                            'sell_threshold': strategy_data.get('macd_sell_threshold', 0)
                        },
                        'bollinger': {
                            'upper': strategy_data.get('bb_upper', 0),
                            'middle': strategy_data.get('bb_middle', 0),
                            'lower': strategy_data.get('bb_lower', 0),
                            'buy_threshold': strategy_data.get('bb_buy_threshold', 0),
                            'sell_threshold': strategy_data.get('bb_sell_threshold', 0)
                        },
                        'volume': {
                            'current': strategy_data.get('current_volume', 0),
                            'average': strategy_data.get('average_volume', 0),
                            'change_rate': strategy_data.get('volume_change_rate', 0)
                        },
                        'price_change': {
                            'rate': strategy_data.get('price_change_rate', 0),
                            'threshold': strategy_data.get('price_change_threshold', 0.02)
                        },
                        'moving_average': {
                            'ma5': strategy_data.get('ma5', 0),
                            'ma20': strategy_data.get('ma20', 0)
                        },
                        'momentum': {
                            'value': strategy_data.get('momentum', 0)
                        },
                        'stochastic': {
                            'k': strategy_data.get('stoch_k', 0),
                            'd': strategy_data.get('stoch_d', 0),
                            'buy_threshold': strategy_data.get('stoch_buy_threshold', 20),
                            'sell_threshold': strategy_data.get('stoch_sell_threshold', 80)
                        },
                        'ichimoku': {
                            'cloud_top': strategy_data.get('ichimoku_cloud_top', 0),
                            'cloud_bottom': strategy_data.get('ichimoku_cloud_bottom', 0)
                        },
                        'market_sentiment': {
                            'value': strategy_data.get('market_sentiment', 0)
                        },
                        'downtrend_end': {
                            'trend_strength': strategy_data.get('trend_strength', 0),
                            'volume_change': strategy_data.get('volume_change_24h', 0)
                        },
                        'uptrend_end': {
                            'trend_strength': strategy_data.get('trend_strength', 0),
                            'resistance_level': strategy_data.get('resistance_level', 0)
                        },
                        'divergence': {
                            'price_rsi': strategy_data.get('price_rsi_divergence', 0),
                            'price_macd': strategy_data.get('price_macd_divergence', 0)
                        }
                    },
                    'signals': {
                        'buy_strength': strategy_data.get('buy_signal', 0),
                        'sell_strength': strategy_data.get('sell_signal', 0),
                        'overall_signal': strategy_data.get('overall_signal', 0),
                        'combined_threshold': {
                            'buy': strategy_data.get('combined_buy_threshold', 0.7),
                            'sell': strategy_data.get('combined_sell_threshold', 0.3)
                        }
                    },
                    'market_metrics': {
                        'volume': strategy_data.get('volume', 0),
                        'market_cap': strategy_data.get('market_cap', 0),
                        'rank': strategy_data.get('market_rank', 0),
                        'price_change_24h': strategy_data.get('price_change_24h', 0),
                        'volume_change_24h': strategy_data.get('volume_change_24h', 0)
                    },
                    'thresholds': {
                        'price_change': strategy_data.get('price_change_threshold', 0.02),
                        'volume_change': strategy_data.get('volume_change_threshold', 0.5),
                        'trend_strength': strategy_data.get('trend_strength_threshold', 0.6)
                    }
                }

                result = self.strategy_data.update_one(
                    {
                        'market': market,
                        'exchange': exchange
                    },
                    {'$set': document},
                    upsert=True
                )
                
                success = bool(result.upserted_id or result.modified_count > 0)
                
                if success:
                    self.logger.debug(f"전략 데이터 저장 성공 - market: {market}, exchange: {exchange}, ID: {result.upserted_id}")
                    self.logger.debug(f"저장된 데이터: RSI={document['strategies']['rsi']['value']:.2f}, "
                                  f"MACD={document['strategies']['macd']['macd']:.2f}, "
                                  f"매수신호={document['signals']['buy_strength']:.2f}, "
                                  f"매도신호={document['signals']['sell_strength']:.2f}")
                else:
                    self.logger.warning(f"전략 데이터 저장 실패 - market: {market}, exchange: {exchange}")
                    
                return success

            except Exception as e:
                self.logger.error(f"전략 데이터 저장 실패 - market: {market}, exchange: {exchange}, 오류: {str(e)}")
                return False

    def get_latest_strategy_data(self, market: str, exchange: str) -> Dict:
        """특정 마켓의 최신 전략 데이터 조회

        Args:
            market: 마켓 심볼
            exchange: 거래소 이름

        Returns:
            Dict: 최신 전략 데이터
        """
        try:
            result = self.strategy_data.find_one(
                {
                    'market': market,
                    'exchange': exchange
                },
                sort=[('timestamp', -1)]
            )
            
            if result:
                self.logger.debug(f"최신 전략 데이터 조회 성공 - market: {market}, exchange: {exchange}, 시간: {result['timestamp']}")
            else:
                self.logger.warning(f"전략 데이터 없음 - market: {market}, exchange: {exchange}")
                
            return result if result else {}

        except Exception as e:
            self.logger.error(f"전략 데이터 조회 실패 - market: {market}, exchange: {exchange}, 오류: {str(e)}")
            return {}

    def cleanup_strategy_data(self, exchange_name: str):
        """strategy_data 컬렉션 정리"""
        try:
            self.db.strategy_data.delete_many({'exchange': exchange_name})
            self.logger.info(f"strategy_data {exchange_name} 거래소 전략 데이터 초기화 완료")
        except Exception as e:
            self.logger.error(f"strategy_data 컬렉션 정리 실패: {str(e)}")
            
    def cleanup_portfolio(self, exchange: str):
        """portfolio 컬렉션 정리"""
        with self._get_collection_lock('portfolio'):
            try:
                # portfolio 컬렉션 초기화
                self.db.drop_collection('portfolio')
                self.logger.info("portfolio 컬렉션 삭제 완료")
                
                # portfolio 컬렉션 재생성 및 초기 데이터 설정
                self.portfolio = self.db['portfolio']
                initial_portfolio = {
                    'market_list': {},
                    'exchange': exchange,
                    'investment_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000)),
                    'available_investment': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'reserve_amount': float(os.getenv('RESERVE_AMOUNT', 200000)),
                    'current_amount': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'profit_earned': 0,
                    'created_at': TimeUtils.get_current_kst(),
                    'last_updated': TimeUtils.get_current_kst()
                }
                self.portfolio.insert_one(initial_portfolio)
                self.portfolio.create_index([("_id", 1)])
            
                self.logger.info("portfolio 컬렉션 재설정 완료")
            except Exception as e:
                self.logger.error(f"portfolio 컬렉션 정리 실패: {str(e)}")
            
    def cleanup_trades(self, trading_manager: object):
        """trades, trading_history, portfolio 컬렉션 정리"""
        with self._get_collection_lock('trades'):
            try:
                # trades 컬렉션 정리
                self.db.drop_collection('trades')
                self.logger.info("trades 컬렉션 삭제 완료")
                
                # trades 컬렉션 재생성 및 인덱스 설정
                self.trades = self.db['trades']
                self.trades.create_index([("market", 1), ("exchange", 1), ("thread_id", 1), ("status", 1)])
                self.trades.create_index([("thread_id", 1)])
                
                self.logger.info("trades 컬렉션 재설정 완료")
                
                # 오늘 날짜의 daily_profit 문서 확인
                kst_now = TimeUtils.get_current_kst()
                today = kst_now.replace(hour=0, minute=0, second=0, microsecond=0)
                daily_profit_doc = self.daily_profit.find_one({'date': today})
                
                # daily_profit 문서가 없으면 일일 리포트 생성
                if not daily_profit_doc:
                    trading_manager.generate_daily_report()
                    daily_profit_doc = self.daily_profit.find_one({'date': today})
                
                # 리포트가 전송된 경우에만 trading_history와 portfolio 초기화
                if daily_profit_doc and daily_profit_doc.get('reported', False):
                    # trading_history 컬렉션 정리
                    self.db.drop_collection('trading_history')
                    self.logger.info("trading_history 컬렉션 삭제 완료")
                    
                    # trading_history 컬렉션 재생성 및 인덱스 설정
                    self.trading_history = self.db['trading_history']
                    self.trading_history.create_index([("market", 1), ("exchange", 1), ("thread_id", 1)])
                    self.trading_history.create_index([("buy_timestamp", -1)])
                    self.trading_history.create_index([("sell_timestamp", -1)])
                    
                    self.logger.info("trading_history 컬렉션 재설정 완료")
                else:
                    self.logger.info("오늘의 일일 리포트가 아직 전송되지 않아 trading_history와 portfolio 컬렉션 유지")
                    
            except Exception as e:
                self.logger.error(f"trades/trading_history/portfolio 컬렉션 정리 실패: {str(e)}")

    def _get_collection_lock(self, collection_name: str) -> threading.Lock:
        """컬렉션별 락 반환
        
        Args:
            collection_name (str): 컬렉션 이름
            
        Returns:
            threading.Lock: 해당 컬렉션의 락 객체
        """
        lock = self._collection_locks.get(collection_name, threading.Lock())
        self.logger.debug(f"Thread {threading.current_thread().name} getting lock for {collection_name}")
        return lock

    def update_market_index(self, data: Dict) -> bool:
        """
        시장 지표(AFR 등) 데이터 업데이트
        
        Args:
            market_data (Dict): 업데이트할 시장 지표 데이터
                {
                    'exchange': str,          # 거래소 이름
                    'timestamp': datetime,    # 타임스탬프
                    'AFR': float,            # AFR 값
                    'current_change': float,  # 현재 변화율
                    'fear_and_greed': float,  # 공포/탐욕 지수
                    'market_feargreed': List[Dict[str, Any]]  # 마켓별 공포/탐욕 데이터
                }
        
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            # timestamp를 datetime 객체로 변환
            if isinstance(data.get('last_updated'), str):
                data['last_updated'] = TimeUtils.get_current_kst()
            
            result = self.market_index.update_one(
                {'exchange': data['exchange']},
                {'$set': {
                    'AFR': data['AFR'],
                    'current_change': data['current_change'],
                    'fear_and_greed': data['fear_and_greed'],
                    'market_feargreed': data['market_feargreed'],
                    'last_updated': data['last_updated']
                }},
                upsert=True
            )
            return bool(result.modified_count > 0 or result.upserted_id)
            
        except Exception as e:
            logging.getLogger('investment_center').error(f"시장 지표 데이터 업데이트 실패: {str(e)}")
            return False

    def get_market_index(self, exchange: str) -> Dict:
        """
        시장 지표 데이터 조회
        
        Args:
            exchange (str): 거래소 이름
            
        Returns:
            Dict: 시장 지표 데이터
            {
                'exchange': str,
                'AFR': [최근 20개 값],
                'current_change': [최근 20개 값],
                'fear_and_greed': [최근 20개 값],
                'last_updated': datetime
            }
        """
        try:
            with self._get_collection_lock('market_index'):
                return self.market_index.find_one({'exchange': exchange}) or {
                    'exchange': exchange,
                    'AFR': [],
                    'current_change': [],
                    'fear_and_greed': [],
                    'last_updated': TimeUtils.get_current_kst()
                }
                
        except Exception as e:
            self.logger.error(f"시장 지표 데이터 조회 실패: {str(e)}")
            return {
                'exchange': exchange,
                'AFR': [],
                'current_change': [],
                'fear_and_greed': [],
                'last_updated': TimeUtils.get_current_kst()
            }
