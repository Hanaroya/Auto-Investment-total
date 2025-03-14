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
from monitoring.memory_monitor import MemoryProfiler, memory_profiler

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
    마켓 목록을 여러 스레드로 분할하여 병렬 처리를 관리합니다.
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
        self.db = MongoDBManager(exchange_name=self.investment_center.exchange_name)
        self.exchange_name = self.investment_center.exchange_name
        self.logger = logging.getLogger('investment_center')
        self.stop_flag = threading.Event()
        # 공유 락 초기화
        self.shared_locks = {
            'candle_data': threading.Lock(),
            'trade': threading.Lock(),
            'market_data': threading.Lock(),
            'long_term_trades': threading.Lock(),
            'portfolio': threading.Lock()
        }

        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.trading_manager = TradingManager(exchange_name=self.investment_center.exchange_name)
        self.scheduler_thread = None
        self.afr_monitor_thread = None
        self.afr_ready = threading.Event()  # AFR 데이터 준비 상태를 위한 이벤트 추가
        self.memory_profiler = MemoryProfiler()
        self.order_monitor_thread = None

    
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
                'status': {'$in': ['active', 'converted']}
            })
            
            trading_manager = TradingManager(exchange_name=self.investment_center.exchange_name)
            
            if existing_trades:
                upbit = UpbitCall(self.config['api_keys']['upbit']['access_key'],
                                  self.config['api_keys']['upbit']['secret_key'])
                
                # 일반 거래 강제 매도
                for trade in existing_trades:
                    try:
                        current_price = upbit.get_current_price(trade['market'])  
                        trading_manager.process_sell_signal(
                            market=trade['market'],
                            exchange=trade['exchange'],
                            thread_id=trade['thread_id'],
                            signal_strength=0,
                            price=current_price,
                            strategy_data={'force_sell': True},
                            sell_message="강제 매도"
                        )
                        time.sleep(0.07)
                    except Exception as e:
                        self.logger.error(f"일반 거래 강제 매도 처리 중 오류 발생: {str(e)}")
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
            current_config = self.db.system_config.find_one({'exchange': self.investment_center.exchange_name})
            portfolio = self.db.portfolio.find_one({'exchange': self.investment_center.exchange_name})
            if current_config and portfolio:
                total_profit = portfolio.get('profit_earned', 0)
                new_total_investment = current_config.get('total_max_investment', 0) + total_profit
                self.db.system_config.update_one(
                    {'exchange': self.investment_center.exchange_name},
                    {'$set': {'total_max_investment': new_total_investment}}
                )
                self.db.portfolio.update_one(
                    {'exchange': self.investment_center.exchange_name},
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
                db = MongoDBManager(exchange_name=self.investment_center.exchange_name)
                
                # 기존 컬렉션 정리
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
                    markets=market_group,
                    config=self.config,
                    exchange_name=self.investment_center.exchange_name,
                    shared_locks=self.shared_locks,
                    stop_flag=self.stop_flag,
                    db=self.db,
                    investment_center=self.investment_center
                )
                thread.start()
                self.threads.append(thread)
                
            # 판매 스레드 시작
            selling_thread = TradingThread(
                thread_id=10,
                markets=[],
                config=self.config,
                exchange_name=self.investment_center.exchange_name,
                shared_locks=self.shared_locks,
                stop_flag=self.stop_flag,
                db=self.db,
                investment_center=self.investment_center
            )
            selling_thread.start()
            self.threads.append(selling_thread)
            self.logger.info("판매용 Thread 10 시작")
                
            self.logger.info(f"{len(self.threads)}개 스레드 시작됨")
            
            # 메인 루프
            try:
                while self.running and not self.stop_flag.is_set():
                    now = datetime.now()
                    if now.minute % 10 == 0 and 10 > now.second > 0:
                        self.update_investment_limits()
                        
                    # 매시 정각에 장기 투자 거래 업데이트 실행
                    if now.minute == 0 and 10 > now.second > 0:
                        self.update_long_term_trades()
                        
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

    
    def update_long_term_trades(self):
        """장기 투자 거래 업데이트
        trades 컬렉션에서 is_long_term이 true인 거래를 long_term_trades로 이동
        """
        try:
            # is_long_term이 true인 trades 조회
            long_term_trades = self.db.trades.find({
                'is_long_term': True,
                'exchange': self.exchange_name
            })

            for trade in long_term_trades:
                # long_term_trades에 이미 존재하는지 확인
                existing_long_term = self.db.long_term_trades.find_one({
                    'market': trade['market'],
                    'exchange': self.exchange_name,
                    'original_trade_id': str(trade['_id'])
                })

                if not existing_long_term:
                    # 장기 투자 거래 생성
                    long_term_trade = {
                        'market': trade['market'],
                        'thread_id': trade['thread_id'],
                        'exchange': self.exchange_name,
                        'status': 'active',
                        'initial_investment': trade.get('investment_amount', 0),
                        'total_investment': trade.get('investment_amount', 0),
                        'average_price': trade.get('price', 0),
                        'target_profit_rate': 5,  # 5% 목표 수익률
                        'positions': [{
                            'position': 'long term',
                            'price': trade.get('price', 0),
                            'amount': trade.get('investment_amount', 0),
                            'volume': trade.get('volume', 0),
                            'timestamp': TimeUtils.get_current_kst()
                        }],
                        'from_short_term': True,
                        'original_trade_id': str(trade['_id']),
                        'test_mode': trade.get('test_mode', False),
                        'created_at': TimeUtils.get_current_kst(),
                        'last_investment_time': TimeUtils.get_current_kst()
                    }
                    
                    # 장기 투자 거래 저장
                    self.db.long_term_trades.insert_one(long_term_trade)
                    self.logger.info(f"{trade['market']} 장기 투자로 이동 완료")

                    # 기존 trade는 converted로 변경
                    self.db.trades.update_one(
                        {'_id': trade['_id']},
                        {'$set': {
                            'status': 'converted',
                            'conversion_timestamp': TimeUtils.get_current_kst(),
                            'conversion_price': trade.get('price', 0),
                            'conversion_reason': f"{trade.get('profit_rate', 0)}% 손실로 인한 장기 투자 전환"
                        }}
                    )

        except Exception as e:
            self.logger.error(f"장기 투자 거래 업데이트 중 오류: {str(e)}")

    
    def update_investment_limits(self):
        """system_config에서 투자 한도를 업데이트"""
        try:
            system_config = self.db.system_config.find_one({'exchange': self.exchange_name})
            if system_config:
                # 테스트 모드 확인
                is_test_mode = system_config.get('test_mode', True)
                
                if not is_test_mode:
                    # 실제 거래소 잔고 조회
                    try:
                        exchange_balance = self.investment_center.exchange.get_balance()
                        if exchange_balance:
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
                            
                            self.total_max_investment = floor(total_balance * 0.8)
                            self.max_investment = floor(self.total_max_investment * 0.1)
                            self.investment_each = floor(self.total_max_investment / 20)
                    except Exception as e:
                        self.logger.error(f"실제 거래소 잔고 조회 중 오류: {str(e)}")
                        return
                else:
                    # 테스트 모드일 경우 기존 로직 유지
                    total_max_investment = system_config.get('total_max_investment', 1000000)
                    self.total_max_investment = total_max_investment
                    self.max_investment = floor((self.total_max_investment * 0.8) * 0.1)
                    self.investment_each = floor((self.total_max_investment * 0.8) / 20)
                
                # 포트폴리오 업데이트
                existing_portfolio = self.db.portfolio.find_one({'exchange': self.exchange_name})
                if existing_portfolio:
                    # 기존 profit_earned 값 보존
                    profit_earned = existing_portfolio.get('profit_earned', 0)
                    
                    # 현재 활성 거래들의 총 투자 금액 계산
                    active_trades = self.db.trades.find({
                        'exchange': self.exchange_name,
                        'status': {'$in': ['active', 'converted']}
                    })
                    
                    total_invested = sum(trade.get('investment_amount', 0) for trade in active_trades)
                    
                    # 가용 금액 계산 (전체 투자 가능 금액 - 현재 투자된 금액)
                    available_amount = floor(self.total_max_investment * 0.8)
                    current_amount = floor(self.total_max_investment * 0.8) + profit_earned - total_invested
                    
                    with self.shared_locks['portfolio']:
                        self.db.portfolio.update_one(
                            {'exchange': self.exchange_name},
                            {'$set': {
                            'test_mode': is_test_mode,
                            'investment_amount': self.total_max_investment,
                            'available_investment': available_amount,
                            'current_amount': current_amount,
                            'reserve_amount': floor(self.total_max_investment * 0.2),
                            'profit_earned': profit_earned,
                            'last_updated': TimeUtils.get_current_kst()
                        }}
                    )
                else:
                    with self.shared_locks['portfolio']:
                        self.db.portfolio.insert_one({
                            'exchange': self.exchange_name,
                            'current_amount': floor(self.total_max_investment * 0.8),
                            'available_amount': floor(self.total_max_investment * 0.8),
                            'reserve_amount': floor(self.total_max_investment * 0.2),
                            'investment_amount': self.total_max_investment,
                            'profit_earned': 0,
                            'market_list': [],
                            'last_updated': TimeUtils.get_current_kst(),
                            'test_mode': is_test_mode,
                            'global_tradeable': False
                        })

                self.logger.info(
                    f"Thread {self.exchange_name} 투자 한도 업데이트 "
                    f"(테스트 모드: {is_test_mode}): "
                    f"최대 투자금: {self.max_investment:,}원, "
                    f"마켓당 투자금: {self.investment_each:,}원, "
                    f"현재 투자된 금액: {total_invested:,}원, "
                    f"가용 금액: {available_amount:,}원"
                )
                
        except Exception as e:
            self.logger.error(f"투자 한도 업데이트 중 오류: {str(e)}")

    
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
                
                # 각 그룹의 첫 번째 마켓 로깅
                self.logger.debug(f"Thread {i}에 할당된 첫 번째 마켓: {group[0] if group else 'None'}")   
            
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
                    'thread_id': thread_id,
                    'exchange': self.investment_center.exchange_name
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
        """4시간마다 마켓 목록을 재조회하고 스레드에 재분배"""
        try:
            # 현재 시간이 4시간 간격인지 확인
            current_hour = TimeUtils.get_current_kst().hour
            if current_hour % 4 != 0:
                return
                
            self.logger.info("마켓 목록 재분배 시작")
            
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
            
            # 각 스레드의 거래항목 목록 업데이트
            for i, thread in enumerate(self.threads):
                if i < len(market_groups):
                    thread.markets = market_groups[i]
                    self.logger.info(f"Thread {i}: {len(market_groups[i])} 개의 마켓 재할당")
                    # 첫 번째 거래항목 로깅
                    if market_groups[i]:
                        self.logger.debug(f"Thread {i}의 첫 번째 마켓: {market_groups[i][0]}")
                    
            self.logger.info("마켓 목록 재분배 완료")
            
        except Exception as e:
            self.logger.error(f"마켓 목록 재분배 중 오류: {str(e)}")

    
    def start_order_monitor(self):
        """주문 감시 스레드 시작"""
        self.order_monitor_thread = threading.Thread(
            target=self._monitor_orders,
            daemon=True
        )
        self.order_monitor_thread.start()
        
    def _monitor_orders(self):
        """주문 상태 모니터링"""
        while not self.stop_flag.is_set():
            try:
                # 활성 주문 조회
                active_orders = self.db.get_active_orders()
                for order in active_orders:
                    # 주문 상태 업데이트
                    current_status = self.exchange.get_order_status(order['uuid'])
                    if current_status != order['status']:
                        self.db.update_order_status(order['uuid'], current_status)
                        
                time.sleep(1)  # 1초 대기
            except Exception as e:
                self.logger.error(f"주문 모니터링 중 오류: {str(e)}")
                time.sleep(5)
