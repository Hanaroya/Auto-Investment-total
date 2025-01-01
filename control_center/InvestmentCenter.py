from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import time
import schedule
from datetime import datetime
import yaml
import logging
from pathlib import Path
from trade_market_api.UpbitCall import UpbitCall
from messenger.Messenger import Messenger
from strategy.StrategyBase import StrategyManager
from strategy.Strategies import *
from trade_market_api.MarketDataConverter import MarketDataConverter
from utils.scheduler import Scheduler
from trading.thread_manager import ThreadManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
import asyncio

class ExchangeFactory:
    """
    거래소 객체 생성을 담당하는 팩토리 클래스
    각 거래소별 구현체를 생성하고 설정을 주입
    """
    @staticmethod
    def create_exchange(exchange_name: str, config: Dict) -> Any:
        mode = config.get('mode', 'market')  # 기본 모드는 실제 거래
        if exchange_name.lower() == "upbit":
            if mode == 'test':
                # 테스트 환경: 더미 API 키 사용
                return UpbitCall(
                    access_key="test_access_key",
                    secret_key="test_secret_key"
                )
            # 실제 환경: 설정 파일의 API 키 사용
            return UpbitCall(
                access_key=config['api_keys']['upbit']['access_key'],
                secret_key=config['api_keys']['upbit']['secret_key']
            )
        else:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")

class InvestmentCenter:
    """
    투자 센터 메인 클래스
    거래소 연동, 전략 실행, 주문 처리 등 핵심 기능 담당
    """
    def __init__(self, exchange_name: str):
        """
        초기화 메서드
        Args:
            exchange_name (str): 사용할 거래소 이름 (예: "upbit")
        """
        # 로거를 가장 먼저 설정
        self.logger = self._setup_logger()
        
        # 설정 파일 로드
        self.config = self._load_config()
        self.mode = self.config.get('mode', 'market')
        
        # 거래소 초기화
        self.exchange = self._initialize_exchange(exchange_name)
        
        # 메신저 초기화
        self.messenger = self._initialize_messenger()
        
        # 스케줄러 초기화
        self.scheduler = Scheduler()
        
        # 마켓 분석기 초기화
        self.market_analyzer = MarketAnalyzer(self.config)
        
        # 거래 매니저 초기화
        self.trading_manager = TradingManager()
        
        # 스레드 매니저 초기화
        self.thread_manager = ThreadManager(self.config)
        
        # 기타 속성 초기화
        self.is_running = False

    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        config_path = Path("resource/application.yml")
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise RuntimeError(f"설정 파일 로드 실패: {str(e)}")

    def _setup_logger(self) -> logging.Logger:
        """로깅 설정
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - 투자 결정은 WARNING 레벨로 기록
            - 시장 데이터 분석은 INFO 레벨로 처리
            - 디버그 모드에서는 모든 로그 저장
        """
        try:
            # 설정 파일 로드
            with open('resource/application.yml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            logger = logging.getLogger('InvestmentCenter')
            
            # 로그 레벨 설정
            log_level = config.get('logging', {}).get('level', 'INFO')
            logger.setLevel(getattr(logging, log_level.upper()))
            
            # 이미 핸들러가 있다면 제거
            if logger.handlers:
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
            
            # 로그 포맷 설정
            log_format = config.get('logging', {}).get('format', 
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            formatter = logging.Formatter(log_format)
            
            # 콘솔 핸들러 설정
            if config.get('logging', {}).get('console', {}).get('enabled', True):
                console_handler = logging.StreamHandler()
                console_level = config.get('logging', {}).get('console', {}).get('level', 'INFO')
                console_handler.setLevel(getattr(logging, console_level.upper()))
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
            
            # 파일 핸들러 설정
            if config.get('logging', {}).get('file', {}).get('enabled', True):
                log_dir = Path(config.get('logging', {}).get('file', {}).get('path', 'log'))
                log_dir.mkdir(exist_ok=True)
                
                # 파일명 패턴 설정
                filename_pattern = config.get('logging', {}).get('file', {}).get(
                    'filename', '{date}-investment.log')
                today = datetime.now().strftime('%Y-%m-%d')
                filename = filename_pattern.format(date=today)
                
                file_handler = logging.FileHandler(
                    log_dir / filename,
                    encoding='utf-8'
                )
                file_level = config.get('logging', {}).get('file', {}).get('level', 'DEBUG')
                file_handler.setLevel(getattr(logging, file_level.upper()))
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
            
            return logger
            
        except Exception as e:
            # 기본 로거 설정 (설정 파일 로드 실패 시)
            print(f"로거 설정 파일 로드 실패: {str(e)}, 기본 설정 사용")
            logger = logging.getLogger('InvestmentCenter')
            logger.setLevel(logging.INFO)
            
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)
            
            return logger

    def _initialize_exchange(self, exchange_name: str) -> Any:
        """거래소 초기화"""
        try:
            exchange = ExchangeFactory.create_exchange(exchange_name, self.config)
            self.logger.info(f"{exchange_name} 거래소 초기화 성공")
            return exchange
        except Exception as e:
            self.logger.error(f"거래소 초기화 실패: {str(e)}")
            raise

    def _initialize_messenger(self) -> Messenger:
        """메신저 초기화"""
        try:
            messenger = Messenger(self.config)
            self.logger.info("메신저 초기화 성공")
            return messenger
        except Exception as e:
            self.logger.error(f"메신저 초기화 실패: {str(e)}")
            raise

    def _initialize_strategies(self) -> StrategyManager:
        """전략 초기화"""
        manager = StrategyManager()
        
        # 기본 전략들 추가
        strategies = [
            RSIStrategy(),
            MACDStrategy(),
            BollingerBandStrategy(),
            VolumeStrategy(),
            PriceChangeStrategy(),
            MovingAverageStrategy(),
            MomentumStrategy(),
            StochasticStrategy(),
            IchimokuStrategy(),
            MarketSentimentStrategy()
        ]
        
        for strategy in strategies:
            manager.add_strategy(strategy)
            
        return manager
        
    

    def _check_api_status(self) -> bool:
        """API 상태 확인"""
        try:
            markets = self.exchange.get_krw_markets() # 원화 마켓 목록 조회 (거래량 순)
            return bool(markets)
        except Exception:
            return False

    def _handle_emergency(self) -> None:
        """비상 상황 처리"""
        self.logger.warning("비상 상황 발생: API 연결 실패")
        self.messenger.send_message(message="⚠️ 거래소 API 연결 실패. 시스템 일시 중지.", messenger_type="slack")
        
        while not self._check_api_status():
            self.logger.info("API 재연결 시도 중...")
            time.sleep(60)  # 1분마다 재시도
            
        self.logger.info("API 재연결 성공")
        self.messenger.send_message(message="✅ API 재연결 성공. 시스템 재개.", messenger_type="slack")

    async def start(self):
        """
        투자 센터 시작
        """
        try:
            self.is_running = True
            self.logger.info("투자 센터 시작")
            
            # 시스템 상태 초기화
            self._initialize_system_state()
            
            # 코인 시장 정보 수집 및 정렬
            markets = await self.market_analyzer.get_sorted_markets()
            if not markets:
                await self.messenger.send_message(message="마켓 정보를 가져오는데 실패했습니다.", messenger_type="slack")
                return
            
            self.logger.info(f"총 {len(markets)}개의 마켓 분석을 시작합니다.")
            self.messenger.send_message(message=f"총 {len(markets)}개의 마켓 분석을 시작합니다.", messenger_type="slack")
            
            # 스레드 매니저 시작
            await self.thread_manager.start_threads(markets)
            
            # 스케줄러 시작
            asyncio.create_task(self.scheduler.start())
            
            # 일일/시간별 리포트 스케줄러 설정
            await self.scheduler.schedule_task(
                'daily_report',
                self.trading_manager.generate_daily_report,
                cron='0 20 * * *',
                immediate=False
            )
            
            await self.scheduler.schedule_task(
                'hourly_report',
                self.trading_manager.generate_hourly_report,
                cron='0 * * * *',
                immediate=False
            )
            
            # 메인 루프
            while self.is_running:
                try:
                    # 스레드 상태 체크
                    thread_health = await self.thread_manager.check_thread_health()
                    if not thread_health:
                        self.logger.warning("마켓 데이터 업데이트 지연 감지")
                        await self.messenger.send_message(message="마켓 데이터 업데이트가 지연되고 있습니다.", messenger_type="slack")
                    
                    # 활성 거래 상태 체크
                    active_trades = self.trading_manager.get_active_trades()
                    self.logger.info(f"현재 활성 거래: {len(active_trades)}건")
                    
                    await asyncio.sleep(60)
                    
                except Exception as e:
                    self.logger.error(f"메인 루프 실행 중 오류: {str(e)}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"투자 센터 시작 실패: {str(e)}")
            self.is_running = False
            raise

    def stop(self):
        """
        투자 센터 종료
        """
        try:
            self.is_running = False
            self.logger.info("투자 센터 종료")
            
            # 스레드 매니저 종료
            self.thread_manager.stop_all_threads()
            
            # 스케줄러 정리
            asyncio.create_task(self.scheduler.stop())
            
            # 진행 중인 작업 정리
            self._cleanup()
            
        except Exception as e:
            self.logger.error(f"투자 센터 종료 중 오류: {str(e)}")

    def _initialize_system_state(self):
        """
        시스템 상태 초기화
        """
        try:
            # API 상태 확인
            if not self._check_api_status():
                raise Exception("API 연결 실패")
            
            # 시장 정보 초기화
            markets = self.exchange.get_krw_markets()
            if not markets:
                raise Exception("마켓 정보 조회 실패")
            
            self.logger.info(f"총 {len(markets)}개의 마켓 감시 시작")
            
        except Exception as e:
            self.logger.error(f"시스템 상태 초기화 실패: {str(e)}")
            raise

    def _cleanup(self):
        """
        정리 작업 수행
        """
        try:
            self.logger.info("정리 작업 시작...")
            
            # 진행 중인 주문 취소 및 리소스 정리
            self.thread_manager.stop_all_threads()
            
            # strategy_data 컬렉션, trades 컬렉션 정리
            from database.mongodb_manager import MongoDBManager
            db = MongoDBManager()
            db.cleanup_strategy_data()
            db.cleanup_trades()
            
            # 메신저로 종료 메시지 전송
            asyncio.create_task(self.messenger.send_message(message="시스템이 안전하게 종료되었습니다.", messenger_type="slack"))
            
            self.logger.info("정리 작업 완료")
        except Exception as e:
            self.logger.error(f"정리 작업 중 오류: {str(e)}")

if __name__ == "__main__":
    # 사용 예시
    """
    투자 센터 초기화 및 스케줄 작업 예시
    """
    try:
        center = InvestmentCenter("upbit")
        print("투자 센터 초기화 성공")
        
        # 스케줄 작업 예시
        def daily_report():
            print("일일 리포트 생성")
        
        center.schedule_task(daily_report, "17:00")
        center.start()
        
    except Exception as e:
        print(f"초기화 실패: {str(e)}")
