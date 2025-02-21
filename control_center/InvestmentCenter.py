from typing import Dict, Any
import time
from utils.time_utils import TimeUtils
import yaml
import logging
from pathlib import Path
from database.mongodb_manager import MongoDBManager
from trade_market_api.UpbitCall import UpbitCall
from messenger.Messenger import Messenger
from strategy.StrategyBase import StrategyManager
from strategy.Strategies import *
from trading.thread_manager import ThreadManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
import asyncio
import os
from utils.logger_config import setup_logger
import schedule
from control_center.exchange_factory import ExchangeFactory

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
        self.logger = setup_logger()
        
        # 설정 파일 로드
        self.config = self._load_config()
        self.mode = self.config.get('mode', 'market')
        
        # 거래소 초기화
        self.exchange = self._initialize_exchange(exchange_name)
        self.exchange_name = exchange_name
        
        # 메신저 초기화
        self.messenger = self._initialize_messenger()
        
        # 마켓 분석기 초기화
        self.market_analyzer = MarketAnalyzer(self.config, exchange_name)
        
        # 거래 매니저 초기화
        self.trading_manager = TradingManager(exchange_name)
        
        # 스레드 매니저 초기화
        self.thread_manager = ThreadManager(self.config, self)

        # 데이터베이스 초기화
        self.db = MongoDBManager(exchange_name=exchange_name)
        
        # 기타 속성 초기화
        self.is_running = False
        # 잔고 업데이트 스케줄러 시작
        self.start_balance_update_scheduler()

    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        config_path = Path("resource/application.yml")
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise RuntimeError(f"설정 파일 로드 실패: {str(e)}")

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
            
            # 거래소별 마켓 정보 조회
            markets = self.exchange.get_krw_markets()  # 비동기로 변경
            if not markets:
                raise Exception("마켓 정보 조회 실패")
            
            self.logger.info(f"총 {len(markets)}개의 마켓 분석을 시작합니다.")
            self.messenger.send_message(message=f"총 {len(markets)}개의 마켓 분석을 시작합니다.", messenger_type="slack")
            
            # 스레드 시작
            self.thread_manager.start_threads(markets)
            
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
                    
                    # 스케줄러 상태 확인
                    jobs = self.scheduler.scheduler.get_jobs()
                    self.logger.debug(f"현재 등록된 스케줄러 작업: {len(jobs)}개")
                    for job in jobs:
                        self.logger.debug(f"작업: {job.id}, 다음 실행 시간: {job.next_run_time}")
                    
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
            self.logger.info("투자 센터 종료 시작")
            
            # 활성 거래 조회
            active_trades = self.trading_manager.get_active_trades()
            total_profit = 0
            
            # 모든 활성 거래 청산
            for trade in active_trades:
                try:
                    current_price = self.exchange.get_current_price(trade['market'])  
                    profit = (current_price - trade['price']) * trade['executed_volume']
                    total_profit += profit
                    
                    # 매도 처리
                    self.trading_manager.process_sell_signal(
                        market=trade['market'],
                        exchange=trade['exchange'],
                        thread_id=trade['thread_id'],
                        signal_strength=0,
                        price=current_price,
                        strategy_data={'force_sell': True},
                        sell_message="일반 매도"
                    )
                except Exception as e:
                    self.logger.error(f"거래 청산 중 오류: {str(e)}")
            
            # system_config 업데이트
            current_config = self.db.system_config.find_one({'exchange': self.exchange_name})
            if not current_config:
                self.logger.error("system_config를 찾을 수 없습니다. 기본값 사용")
                current_config = {
                    'total_max_investment': float(os.getenv('TOTAL_MAX_INVESTMENT', 800000)),
                    'reserve_amount': float(os.getenv('RESERVE_AMOUNT', 200000))
                }
            
            new_total_investment = current_config.get('total_max_investment', 0) + total_profit
            
            self.db.system_config.update_one(
                {'exchange': self.exchange_name},
                {
                    '$set': {
                        'total_max_investment': new_total_investment,
                        'last_updated': TimeUtils.get_current_kst(),
                        'reserve_amount': new_total_investment * 0.2
                    }
                },
                upsert=True  # 문서가 없으면 생성
            )
            
            # daily_profit 기록
            self.db.daily_profit.insert_one({
                'timestamp': TimeUtils.get_current_kst(),
                'profit_earned': total_profit,
                'total_max_investment': new_total_investment,
                'reserve_amount': current_config.get('reserve_amount', 200000),
                'type': 'system_shutdown'
            })
            
            # 나머지 정리 작업
            self.thread_manager.stop_all_threads()
            self.scheduler.stop()
            self._cleanup()
            
            self.logger.info(f"투자 센터 종료 완료 (최종 수익: {total_profit:,.0f}원)")
            
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
            db = MongoDBManager(exchange_name=self.exchange_name)
            db.cleanup_strategy_data(self.exchange_name)
            
            # 메신저로 종료 메시지 전송
            asyncio.create_task(self.messenger.send_message(message="시스템이 안전하게 종료되었습니다.", messenger_type="slack"))
            
            self.logger.info("정리 작업 완료")
        except Exception as e:
            self.logger.error(f"정리 작업 중 오류: {str(e)}")

    async def initialize(self):
        """초기화"""
        try:
            self.logger.info("초기화 시작...")
            
            # ... 기존 코드 유지 ...
            
            # 장기 투자 스케줄러 작업 등록
            self.trading_manager.register_scheduler_tasks(self.scheduler)
            
            # ... 기존 코드 유지 ...
            
        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {str(e)}")
            raise

    def update_exchange_balance(self):
        """실제 거래소 잔고 조회 및 업데이트"""
        try:
            system_config = self.db.system_config.find_one({'exchange': self.exchange_name})
            if not system_config:
                self.logger.error("시스템 설정을 찾을 수 없습니다.")
                return False

            # 테스트 모드 확인
            is_test_mode = system_config.get('test_mode', True)
            if is_test_mode:
                self.logger.info("테스트 모드에서는 실제 잔고를 조회하지 않습니다.")
                return True

            # 실제 거래소 잔고 조회
            exchange_balance = self.exchange.get_balance()
            if not exchange_balance:
                self.logger.error("거래소 잔고 조회 실패")
                return False

            total_balance = float(exchange_balance.get('total_balance', 0))
            available_balance = float(exchange_balance.get('available_balance', 0))

            # system_config 업데이트
            self.db.system_config.update_one(
                {'exchange': self.exchange_name},
                {'$set': {
                    'total_max_investment': total_balance,
                    'available_balance': available_balance,
                    'last_balance_update': TimeUtils.get_current_kst()
                }}
            )

            # portfolio 업데이트
            self.db.portfolio.update_one(
                {'exchange': self.exchange_name},
                {'$set': {
                    'current_amount': total_balance,
                    'available_amount': available_balance,
                    'last_balance_update': TimeUtils.get_current_kst()
                }}
            )

            self.logger.info(
                f"거래소 잔고 업데이트 완료 - "
                f"총 잔고: {total_balance:,.0f}원, "
                f"가용 잔고: {available_balance:,.0f}원"
            )
            return True

        except Exception as e:
            self.logger.error(f"거래소 잔고 업데이트 중 오류: {str(e)}")
            return False

    def start_balance_update_scheduler(self):
        """잔고 업데이트 스케줄러 시작"""
        try:
            system_config = self.db.system_config.find_one({'exchange': self.exchange_name})
            if not system_config or system_config.get('test_mode', True):
                return

            def update_balance_job():
                self.update_exchange_balance()

            # 10분마다 잔고 업데이트
            schedule.every(10).minutes.do(update_balance_job)
            
            # 최초 1회 실행
            update_balance_job()

            self.logger.info("잔고 업데이트 스케줄러 시작")

        except Exception as e:
            self.logger.error(f"잔고 업데이트 스케줄러 시작 중 오류: {str(e)}")

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
