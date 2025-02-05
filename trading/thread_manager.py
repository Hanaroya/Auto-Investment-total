import threading
import asyncio
import logging
from typing import List, Dict

from math import floor
from database.mongodb_manager import MongoDBManager
from trading.trading_manager import TradingManager
from datetime import datetime, timedelta, timezone
from utils.time_utils import TimeUtils
import time
import signal
from trade_market_api.UpbitCall import UpbitCall
import sys
import os
import schedule
from .trading_thread import TradingThread
from .afr_monitor_thread import AFRMonitorThread

class SchedulerThread(threading.Thread):
    """스케줄러 전용 스레드"""
    def __init__(self, scheduler, stop_flag: threading.Event):
        super().__init__()
        self.scheduler = scheduler
        self.stop_flag = stop_flag
        self.logger = logging.getLogger('investment-center')

    def run(self):
        """스케줄러 실행"""
        self.logger.info("스케줄러 스레드 시작")
        try:
            while not self.stop_flag.is_set():
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            self.logger.error(f"스케줄러 스레드 오류: {str(e)}")
        finally:
            self.logger.info("스케줄러 스레드 종료")

class ThreadManager:
    """
    여러 거래 스레드를 관리하는 매니저 클래스
    코인 목록을 여러 스레드로 분할하여 병렬 처리를 관리합니다.
    """
    
    def __init__(self, config: Dict, investment_center=None):
        """
        ThreadManager 초기화
        
        Args:
            config (Dict): 설정 정보
            investment_center: InvestmentCenter 인스턴스
        """
        self.config = config
        self.investment_center = investment_center  # InvestmentCenter 인스턴스 저장
        self.threads = []
        self.running = False
        self.db = MongoDBManager()
        self.logger = logging.getLogger('investment_center')
        self.stop_flag = threading.Event()
        # 공유 락 초기화
        self.shared_locks = {
            'candle_data': threading.Lock(),
            'trade': threading.Lock(),
            'market_data': threading.Lock()
        }

        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.scheduler_thread = None
        self.afr_monitor_thread = None
        self.afr_ready = threading.Event()  # AFR 데이터 준비 상태를 위한 이벤트 추가

    def signal_handler(self, signum, frame):
        """시그널 핸들러"""
        self.logger.info(f"Signal {signum} received, initiating shutdown...")
        self.stop_flag.set()
        self.stop_all_threads()

    def stop_all_threads(self):
        """모든 스레드 강제 종료"""
        try:
            self.logger.info("모든 스레드 종료 시작...")
            self.stop_flag.set()
            
            # 스케줄러 스레드 종료
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=2)
            
            # AFR 모니터링 스레드 종료
            if self.afr_monitor_thread and self.afr_monitor_thread.is_alive():
                self.afr_monitor_thread.join(timeout=2)
            
            # 먼저 stop_flag 설정
            self.stop_flag.set()

            # 모든 거래 강제 판매
            existing_trades = self.db.trades.find({
                'status': 'active'
            })
            
            trading_manager = TradingManager()  # 클래스 레벨로 이동
            
            if existing_trades:
                upbit = UpbitCall(self.config['api_keys']['upbit']['access_key'],
                                  self.config['api_keys']['upbit']['secret_key'])
                
                for trade in existing_trades:
                    try:
                        current_price = upbit.get_current_price(trade['coin'])
                        trading_manager.process_sell_signal(
                            coin=trade['coin'],
                            thread_id=trade['thread_id'],
                            signal_strength=0,
                            price=current_price,
                            strategy_data={'force_sell': True},
                            sell_message="일반 매도"
                        )
                        time.sleep(0.07)
                    except Exception as e:
                        self.logger.error(f"강제 매도 처리 중 오류 발생: {str(e)}")
                        continue
                
                del upbit

            # 각 스레드 종료 대기
            for thread in self.threads:
                try:
                    if thread.is_alive():
                        thread.stop_flag.set()
                        thread.join(timeout=2)  # 2초 대기
                        
                        # 여전히 살아있다면 더 강력한 종료 시도
                        if thread.is_alive():
                            self.logger.warning(f"Thread {thread.thread_id} 강제 종료 시도")
                            try:
                                thread._stop()
                            except:
                                pass
                except Exception as e:
                    self.logger.error(f"Thread {thread.thread_id} 종료 중 오류: {str(e)}")
                    continue  # 한 스레드의 오류가 다른 스레드 종료에 영향을 주지 않도록 함
            
            self.threads.clear()
            self.logger.info("모든 스레드 종료 완료")
            
            # add profit earn to system_config's total_max_investment
            current_config = self.db.system_config.find_one({'_id': 'config'})
            portfolio = self.db.portfolio.find_one({'_id': 'main'})
            if current_config and portfolio:
                total_profit = portfolio.get('profit_earned', 0)
                new_total_investment = current_config.get('total_max_investment', 0) + total_profit
                self.db.system_config.update_one(
                    {'_id': 'config'},
                    {'$set': {'total_max_investment': new_total_investment}}
                )
                self.db.portfolio.update_one(
                    {'_id': 'main'},
                    {'$set': {
                        'exchange': self.investment_center.exchange_name,
                        'investment_amount': floor(new_total_investment),
                        'current_amount': floor(new_total_investment * 0.8),
                        'available_investment': floor(new_total_investment * 0.8),
                        'reserve_amount': floor(new_total_investment * 0.2),
                        'profit_earned': 0
                        }
                    }
                )
            
            # 데이터베이스 정리 작업
            try:
                from database.mongodb_manager import MongoDBManager
                db = MongoDBManager()
                
                try:
                    db.cleanup_strategy_data(self.investment_center.exchange_name)
                    self.logger.info(f"strategy_data {self.investment_center.exchange_name} 거래소 전략 데이터 초기화 완료")
                except Exception as e:
                    self.logger.error(f"strategy_data 정리 실패: {str(e)}")
                
                try:
                    db.cleanup_trades(trading_manager=trading_manager)  # 여기서 trading_manager 사용
                    self.logger.info("trades 컬렉션 정리 완료")
                except Exception as e:
                    self.logger.error(f"trades 정리 실패: {str(e)}")
                
                del trading_manager  # 사용 완료 후 정리
                
            except Exception as e:
                self.logger.error(f"데이터베이스 정리 중 오류: {str(e)}")
            
            # 프로그램 종료
            try:
                os._exit(0)
            except:
                sys.exit(0)
                
        except Exception as e:
            self.logger.error(f"스레드 종료 중 오류: {str(e)}")
            try:
                os._exit(1)
            except:
                sys.exit(1)

    def start_threads(self, markets: List[str], thread_count: int = 10):
        """스레드 시작"""
        try:
            self.running = True
            
            # AFR 모니터링 스레드 먼저 시작
            if self.investment_center:
                self.afr_monitor_thread = AFRMonitorThread(
                    investment_center=self.investment_center,
                    stop_flag=self.stop_flag,
                    db_manager=self.db,
                    afr_ready=self.afr_ready  # AFR 준비 이벤트 전달
                )
                self.afr_monitor_thread.start()
                
                # AFR 데이터가 준비될 때까지 대기
                self.logger.info("AFR 데이터 초기화 대기 중...")
                if not self.afr_ready.wait(timeout=300):  # 5분 타임아웃
                    self.logger.error("AFR 데이터 초기화 타임아웃")
                    self.stop_all_threads()
                    return
                self.logger.info("AFR 데이터 초기화 완료")
            
            # 마켓 분배
            market_groups = self.split_markets(markets)
            
            # AFR 데이터가 준비된 후 거래 스레드 시작
            for i, market_group in enumerate(market_groups):
                if not market_group:
                    continue
                    
                thread = TradingThread(
                    thread_id=i,
                    coins=market_group,
                    config=self.config,
                    exchange_name=self.investment_center.exchange_name,
                    shared_locks=self.shared_locks,
                    stop_flag=self.stop_flag,
                    db=self.db,
                    investment_center=self.investment_center
                )
                thread.start()
                self.threads.append(thread)
                
            self.logger.info(f"{len(self.threads)}개 스레드 시작됨")
            
            # 메인 루프
            try:
                while self.running and not self.stop_flag.is_set():
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("키보드 인터럽트 감지")
                self.stop_all_threads()
            except Exception as e:
                self.logger.error(f"메인 루프 오류: {str(e)}")
                self.stop_all_threads()
                
        except Exception as e:
            self.logger.error(f"스레드 시작 실패: {str(e)}")
            self.stop_all_threads()

    def split_markets(self, markets: List) -> List[List]:
        """
        전체 마켓 목록을 10개의 균등한 그룹으로 분할합니다.
        
        Args:
            markets (List): 분할할 마켓 목록
            
        Returns:
            List[List]: 분할된 마켓 그룹 목록
        """
        try:
            # 이미 거래량으로 정렬된 마켓 리스트를 역순으로 변경
            sorted_markets = list(markets)
            
            # 스레드 수에 따라 균등 분할
            num_threads = 10  # 기본 스레드 수
            group_size = len(sorted_markets) // num_threads
            market_groups = []
            
            for i in range(num_threads):
                start_idx = i * group_size
                end_idx = start_idx + group_size if i < num_threads - 1 else len(sorted_markets)
                group = sorted_markets[start_idx:end_idx]
                market_groups.append(group)
                
                # 각 그룹의 첫 번째 코인 로깅
                self.logger.debug(f"Thread {i}에 할당된 첫 번째 코인: {group[0] if group else 'None'}")   
            
            return market_groups
            
        except Exception as e:
            self.logger.error(f"마켓 분할 중 오류: {str(e)}")
            raise

    async def check_thread_health(self):
        """
        각 스레드의 상태를 모니터링하고 문제가 있는 스레드를 감지합니다.
        
        Returns:
            bool: 모든 스레드가 정상이면 True, 문제가 있으면 False
        """
        try:
            all_threads_healthy = True
            
            for thread_id in range(len(self.threads)):
                # 동기식으로 변경
                status = self.db.thread_status.find_one({
                    'thread_id': thread_id
                })
                
                if not status:
                    self.logger.error(f"Thread {thread_id}의 상태 정보를 찾을 수 없습니다.")
                    all_threads_healthy = False
                    continue
                
                if not status['is_active']:
                    self.logger.error(f"Thread {thread_id}가 비활성 상태입니다.")
                    all_threads_healthy = False
                    continue
                
                last_updated = status['last_updated']
                if datetime.utcnow() - last_updated > timedelta(minutes=5):
                    self.logger.warning(f"Thread {thread_id}가 5분 이상 업데이트되지 않았습니다.")
                    # 경고는 하지만 즉시 실패로 처리하지는 않음
                
            return all_threads_healthy

        except Exception as e:
            self.logger.error(f"스레드 상태 확인 중 오류 발생: {str(e)}")
            return False

    async def cleanup_market_data(self):
        """마켓 데이터 정리"""
        try:
            self.db.market_data.delete_many({})
            self.logger.info("Market data cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Error cleaning up market data: {str(e)}")

    def handle_interrupt(self, signum=None, frame=None):
        """키보드 인터럽트나 시그널 처리"""
        self.logger.info("Interrupt received, starting cleanup process...")
        
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

    def start_scheduler(self, scheduler):
        """스케줄러 스레드 시작"""
        try:
            self.scheduler_thread = SchedulerThread(scheduler, self.stop_flag)
            self.scheduler_thread.start()
            self.logger.info("스케줄러 스레드 시작됨")
        except Exception as e:
            self.logger.error(f"스케줄러 스레드 시작 실패: {str(e)}")

    def update_market_distribution(self, exchange: str):
        """4시간마다 코인 목록을 재조회하고 스레드에 재분배"""
        try:
            # 현재 시간이 4시간 간격인지 확인
            current_hour = TimeUtils.get_current_kst().hour
            if current_hour % 4 != 0:
                return
                
            self.logger.info("코인 목록 재분배 시작")
            
            # UpbitCall 인스턴스 생성
            upbit = UpbitCall(
                self.config['api_keys'][exchange]['access_key'],
                self.config['api_keys'][exchange]['secret_key']
            )
            
            # 새로운 마켓 목록 조회 (거래량 순으로 이미 정렬되어 있음)
            markets = upbit.get_krw_markets()
            if not markets:
                self.logger.error("마켓 목록 조회 실패")
                return
                
            # 마켓 재분배
            market_groups = self.split_markets(markets)
            
            # 각 스레드의 코인 목록 업데이트
            for i, thread in enumerate(self.threads):
                if i < len(market_groups):
                    thread.coins = market_groups[i]
                    self.logger.info(f"Thread {i}: {len(market_groups[i])} 개의 코인 재할당")
                    # 첫 번째 코인 로깅
                    if market_groups[i]:
                        self.logger.debug(f"Thread {i}의 첫 번째 코인: {market_groups[i][0]}")
                    
            self.logger.info("코인 목록 재분배 완료")
            
        except Exception as e:
            self.logger.error(f"코인 목록 재분배 중 오류: {str(e)}")

    async def watch_orders(self):
        """주문 감시 스레드"""
        while True:
            try:
                # 대기 중인 주문 조회
                pending_orders = await self.db.get_collection('order_list').find({
                    'status': 'pending'
                }).to_list(None)
                
                for order in pending_orders:
                    current_price = self.upbit.get_current_price(order['coin'])
                    
                    if order['type'] == 'buy':
                        if current_price <= order['price']:
                            # 매수 조건 충족
                            await self.trading_manager.process_buy_signal(
                                coin=order['coin'],
                                thread_id=0,
                                signal_strength=1.0,
                                price=current_price,
                                strategy_data=order['strategy_data'],
                                buy_message="일반 매수"
                            )
                            # 주문 상태 업데이트
                            await self.db.get_collection('order_list').update_one(
                                {'_id': order['_id']},
                                {'$set': {
                                    'status': 'completed',
                                    'executed_price': current_price,
                                    'updated_at': TimeUtils.get_current_kst()    
                                }}
                            )
                    
                    elif order['type'] == 'sell':
                        if current_price >= order['price']:
                            # 매도 조건 충족
                            await self.trading_manager.process_sell_signal(
                                coin=order['coin'],
                                thread_id=order['trade_data']['thread_id'],
                                signal_strength=1.0,
                                price=current_price,
                                strategy_data={'forced_sell': True},
                                sell_message="일반 매도"
                            )
                            # 주문 상태 업데이트
                            await self.db.get_collection('order_list').update_one(
                                {'_id': order['_id']},
                                {'$set': {
                                    'status': 'completed',
                                    'executed_price': current_price,
                                    'updated_at': TimeUtils.get_current_kst()
                                }}
                            )
                
                # 1초 대기
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"주문 감시 중 오류 발생: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생시 5초 대기
