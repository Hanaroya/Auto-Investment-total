import threading
import asyncio
import logging
from typing import List, Dict

from math import floor
from database.mongodb_manager import MongoDBManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from datetime import datetime, timedelta, timezone
import time
import signal
from trade_market_api.UpbitCall import UpbitCall
import sys
import os
import schedule

class TradingThread(threading.Thread):
    """
    개별 코인 그룹을 처리하는 거래 스레드
    각 스레드는 할당된 코인들에 대해 독립적으로 거래 분석 및 실행을 담당합니다.
    """
    def __init__(self, thread_id: int, coins: List[str], db: MongoDBManager, config: Dict, shared_locks: Dict, stop_flag: threading.Event):
        """
        Args:
            thread_id (int): 스레드 식별자
            coins (List[str]): 처리할 코인 목록
            db (MongoDBManager): 데이터베이스 인스턴스
            config: 설정 정보가 담긴 딕셔너리
            shared_locks (Dict): 공유 락 딕셔너리
            stop_flag (threading.Event): 전역 중지 플래그
        """
        super().__init__()
        self.thread_id = thread_id
        self.coins = coins
        self.db = db
        self.config = config
        self.shared_locks = shared_locks
        self.stop_flag = stop_flag
        self.logger = logging.getLogger(f"InvestmentCenter.Thread-{thread_id}")
        self.loop = None
        
        # 각 인스턴스 생성
        self.market_analyzer = MarketAnalyzer(config=self.config)
        self.trading_manager = TradingManager()
        
        # UpbitCall 인스턴스 생성
        self.upbit = UpbitCall(
            self.config['api_keys']['upbit']['access_key'],
            self.config['api_keys']['upbit']['secret_key']
        )
        
        # system_config에서 설정값 가져오기
        system_config = self.db.system_config.find_one({'_id': 'system_config'})
        if not system_config:
            self.logger.error("system_config를 찾을 수 없습니다. 기본값 사용")
            self.max_investment = float(os.getenv('MAX_THREAD_INVESTMENT', 80000))
            self.total_max_investment = float(os.getenv('TOTAL_MAX_INVESTMENT', 800000))
            self.investment_each = self.total_max_investment / 40
        else:
            self.max_investment = system_config.get('max_thread_investment', 80000)
            self.total_max_investment = system_config.get('total_max_investment', 800000)
            self.investment_each = self.total_max_investment / 40
        
        self.logger.info(f"Thread {thread_id} 초기화 완료 (최대 투자금: {self.max_investment:,}원)")
        
        # system_config 모니터링 및 업데이트를 위한 마지막 체크 시간 추가
        self.last_config_check = datetime.now()
        self.update_investment_limits()

    def update_investment_limits(self):
        """system_config에서 투자 한도를 업데이트"""
        try:
            system_config = self.db.system_config.find_one({'_id': 'system_config'})
            if system_config:
                total_max_investment = system_config.get('total_max_investment', 1000000)
                # total_max_investment를 initial_investment의 80%로 설정
                self.total_max_investment = floor(total_max_investment * 0.8)
                # 스레드당 최대 투자금은 total_max_investment의 10%로 설정
                self.max_investment = floor(self.total_max_investment * 0.1)
                # 코인당 투자금은 total_max_investment를 40으로 나눈 값
                self.investment_each = floor(self.total_max_investment / 40)
                
                self.logger.info(f"Thread {self.thread_id} 투자 한도 업데이트: "
                               f"최대 투자금: {self.max_investment:,}원, "
                               f"코인당 투자금: {self.investment_each:,}원")
        except Exception as e:
            self.logger.error(f"투자 한도 업데이트 중 오류: {str(e)}")

    def run(self):
        """스레드 실행"""
        try:
            self.logger.info(f"Thread {self.thread_id}: 마켓 분석 시작 - {len(self.coins)} 개의 코인")
            
            while not self.stop_flag.is_set():
                cycle_start_time = time.time()
                
                # 스레드 ID에 따라 다른 대기 시간 설정
                if self.thread_id <= 3:
                    wait_time = 10  # 0~3번 스레드는 10초마다
                    initial_delay = self.thread_id * 1  # 1초 간격으로 시작 시간 분배
                else:
                    wait_time = 600  # 4~10번 스레드는 600초(10분)마다
                    initial_delay = (self.thread_id - 4) * 1  # 1초 간격으로 시작 시간 분배
                
                # 초기 지연 적용
                time.sleep(initial_delay)
                
                for coin in self.coins:
                    if self.stop_flag.is_set():
                        break
                        
                    try:
                        self.process_single_coin(coin)
                    except Exception as e:
                        self.logger.error(f"Error processing {coin}: {str(e)}")
                        continue
                
                # 사이클 완료 시간 계산
                cycle_duration = time.time() - cycle_start_time
                
                # 설정된 대기 시간에서 실제 소요 시간과 초기 지연 시간을 뺀 만큼 대기
                remaining_time = wait_time - cycle_duration - initial_delay
                if remaining_time > 0:
                    time.sleep(remaining_time)
                    
            self.logger.info(f"Thread {self.thread_id} 종료")
        
        except Exception as e:
            self.logger.error(f"Thread {self.thread_id} error: {str(e)}")
        finally:
            self.logger.info(f"Thread {self.thread_id} 정리 작업 완료")

    def process_single_coin(self, coin: str):
        """단일 코인 처리"""
        try:
            # 5분마다 투자 한도 업데이트 체크
            current_time = datetime.now()
            if (current_time - self.last_config_check).total_seconds() >= 300:  # 5분
                self.update_investment_limits()
                self.last_config_check = current_time

            # 캔들 데이터 조회 - 락으로 보호
            with self.shared_locks['candle_data']:
                self.logger.debug(f"Thread {self.thread_id} acquired lock for {coin}")
                # thread_id에 따라 다른 시간 간격 설정
                interval = '1' if self.thread_id <= 3 else '240'
                candles = self.upbit.get_candle(market=coin, interval=interval, count=300)
                self.logger.debug(f"Thread {self.thread_id} released lock for {coin}")

            # 현재 투자 상태 확인
            active_trades = self.db.trades.find({
                'thread_id': self.thread_id, 
                'status': 'active'
            })
            current_investment = sum(trade.get('total_investment', 0) for trade in active_trades)

            # 최대 투자금 체크
            if current_investment >= self.total_max_investment:
                self.logger.info(f"Thread {self.thread_id}: {coin} - 최대 투자금 도달")
                return

            # 마켓 분석 수행
            signals = self.market_analyzer.analyze_market(coin, candles)
            
            # 전략 데이터 저장
            current_price = candles[-1]['close']
            self.trading_manager.update_strategy_data(coin, self.thread_id, current_price, signals)

            # 분석 결과 저장 및 거래 신호 처리
            with self.shared_locks['trade']:
                try:
                    # 현재 코인의 활성 거래 확인 및 로깅
                    active_trade = self.db.trades.find_one({
                        'coin': coin,
                        'status': 'active'
                    })
                    
                    self.logger.info(f"Thread {self.thread_id}: {coin} - Active trade check result: {active_trade is not None}")
                    self.logger.debug(f"Signals: {signals}")
                    self.logger.debug(f"Current investment: {current_investment}, Max investment: {self.total_max_investment}")

                    if active_trade:
                        current_profit_rate = active_trade.get('profit_rate', 0)
                        price_trend = signals.get('price_trend', 0)  # 가격 추세 (-1 ~ 1)
                        volatility = signals.get('volatility', 0)    # 변동성 지표
                        current_investment = active_trade.get('investment_amount', 0)
                        averaging_down_count = active_trade.get('averaging_down_count', 0)

                        # 매도 조건 확인
                        should_sell = (
                            # 1. 급격한 하락 감지
                            (price_trend < -0.7 and volatility > 0.8) or
                            
                            # 2. 지속적인 하락 추세
                            (price_trend < -0.3 and current_profit_rate < -2) or
                            
                            # 3. 목표 수익 달성 후 하락 추세
                            (current_profit_rate > 3 and price_trend < -0.2) or
                            
                            # 4. 과도한 손실 방지
                            (current_profit_rate < -3) or
                            
                            # 5. 변동성 급증 시 이익 실현
                            (current_profit_rate > 2 and volatility > 0.9) or
                            
                            # 6. 평균 매수 가격보다 10% 이상 상승한 경우
                            (current_profit_rate > 10 and current_price > active_trade.get('price', 0) * 1.1) or
                            
                            # 7. sell_threshold 이하
                            (signals.get('overall_signal', 0.0) <= self.config['strategy']['sell_threshold'] and (
                                current_profit_rate > 0.15)) or

                            # 8. 사용자 호출
                            (active_trade.get('user_call', False))
                        )

                        # 디버깅 로깅
                        self.logger.debug(f"{coin} - 수익률: {current_profit_rate:.2f}%, "
                                          f"should_sell: {should_sell}, "
                                         f"투자금: {current_investment:,}원, "
                                         f"물타기 횟수: {averaging_down_count}")
                        
                        if should_sell:
                            self.logger.info(f"매도 신호 감지: {coin} - Profit: {current_profit_rate:.2f}%, "
                                        f"Trend: {price_trend:.2f}, Volatility: {volatility:.2f}")
                            if self.trading_manager.process_sell_signal(
                                coin=coin,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals
                            ):
                                self.logger.info(f"매도 신호 처리 완료: {coin}")
                        
                        # 물타기 조건 확인
                        should_average_down = (
                            current_profit_rate <= -2 and  # 수익률이 -2% 이하
                            current_investment < self.total_max_investment * 0.8 and  # 최대 투자금의 80% 미만 사용
                            averaging_down_count < 3  # 최대 3회까지만 물타기
                        )
                        if should_average_down and should_sell == False:
                            # 물타기 투자금 계산 (기존 투자금의 50%)
                            averaging_down_amount = min(
                                floor(current_investment * 0.5),
                                self.total_max_investment - current_investment
                            )
                            # self.logger.warning(f"물타기 투자금 계산: {averaging_down_amount:,}원")

                            if signals.get('overall_signal', 0.0) >= self.config['strategy']['buy_threshold'] and averaging_down_amount >= 5000:  # 최소 주문금액 5000원 이상
                                self.logger.info(f"물타기 신호 감지: {coin} - 현재 수익률: {current_profit_rate:.2f}%")
                                
                                # 물타기용 전략 데이터 업데이트
                                signals['investment_amount'] = averaging_down_amount
                                signals['is_averaging_down'] = True
                                signals['existing_trade_id'] = active_trade['_id']
                                
                                self.trading_manager.process_buy_signal(
                                    coin=coin,
                                    thread_id=self.thread_id,
                                    signal_strength=0.8,  # 물타기용 신호 강도
                                    price=current_price,
                                    strategy_data=signals
                                )
                                self.logger.info(f"물타기 주문 처리 완료: {coin} - 추가 투자금액: {averaging_down_amount:,}원")
                    
                    else:
                        # 일반 매수 신호 처리 (기존 로직)
                        if signals.get('overall_signal', 0.0) >= self.config['strategy']['buy_threshold'] and current_investment < self.max_investment:
                            self.logger.info(f"매수 신호 감지: {coin} - Signal strength: {signals.get('overall_signal')}")
                            investment_amount = min(floor((self.investment_each)), self.max_investment - current_investment)
                            
                            # strategy_data에 investment_amount 추가
                            signals['investment_amount'] = investment_amount
                            
                            self.trading_manager.process_buy_signal(
                                coin=coin,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals
                            )
                            self.logger.info(f"매수 신호 처리 완료: {coin} - 투자금액: {investment_amount:,}원")
                        else:
                            self.logger.debug(f"매수 조건 미충족: {coin} - Signal: {signals.get('overall_signal')}, Investment: {current_investment}/{self.max_investment}")

                except Exception as e:
                    self.logger.error(f"거래 신호 처리 중 오류 발생: {str(e)}", exc_info=True)

            # 스레드 상태 업데이트
            self.db.thread_status.update_one(
                {'thread_id': self.thread_id},
                {'$set': {
                    'last_coin': coin,
                    'last_update': datetime.now(timezone(timedelta(hours=9))),
                    'status': 'running',
                    'is_active': True
                }},
                upsert=True
            )

            self.logger.debug(f"Thread {self.thread_id}: {coin} - 처리 완료")

        except Exception as e:
            self.logger.error(f"Error processing {coin}: {str(e)}")

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
    
    def __init__(self, config: Dict):
        """
        ThreadManager 초기화
        
        Args:
            config (Dict): 설정 정보
        """
        self.config = config
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
            
            # 먼저 stop_flag 설정
            self.stop_flag.set()

            # 모든 거래 강제 판매
            
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
            
            # 데이터베이스 정리 작업
            try:
                from database.mongodb_manager import MongoDBManager
                db = MongoDBManager()
                
                # 각 정리 작업을 개별적으로 try-except로 감싸서 처리
                try:
                    db.cleanup_strategy_data()
                    self.logger.info("strategy_data 컬렉션 정리 완료")
                except Exception as e:
                    self.logger.error(f"strategy_data 정리 실패: {str(e)}")
                
                try:
                    db.cleanup_trades()
                    self.logger.info("trades 컬렉션 정리 완료")
                except Exception as e:
                    self.logger.error(f"trades 정리 실패: {str(e)}")
                
            except Exception as e:
                self.logger.error(f"데이터베이스 정리 중 오류: {str(e)}")
            
            # 프로그램 종료
            try:
                os._exit(0)  # 더 강력한 종료 방법 사용
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
            
            # 마켓 분배
            market_groups = self.split_markets(markets)
            
            for i, market_group in enumerate(market_groups):
                if not market_group:
                    continue
                    
                thread = TradingThread(
                    thread_id=i,
                    coins=market_group,
                    config=self.config,
                    shared_locks=self.shared_locks,
                    stop_flag=self.stop_flag,
                    db=self.db
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

    def update_market_distribution(self):
        """4시간마다 코인 목록을 재조회하고 스레드에 재분배"""
        try:
            # 현재 시간이 4시간 간격인지 확인
            current_hour = datetime.now(timezone(timedelta(hours=9))).hour
            if current_hour % 4 != 0:
                return
                
            self.logger.info("코인 목록 재분배 시작")
            
            # UpbitCall 인스턴스 생성
            upbit = UpbitCall(
                self.config['api_keys']['upbit']['access_key'],
                self.config['api_keys']['upbit']['secret_key']
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
                                strategy_data=order['strategy_data']
                            )
                            # 주문 상태 업데이트
                            await self.db.get_collection('order_list').update_one(
                                {'_id': order['_id']},
                                {'$set': {
                                    'status': 'completed',
                                    'executed_price': current_price,
                                    'updated_at': datetime.now(timezone(timedelta(hours=9)))
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
                                strategy_data={'forced_sell': True}
                            )
                            # 주문 상태 업데이트
                            await self.db.get_collection('order_list').update_one(
                                {'_id': order['_id']},
                                {'$set': {
                                    'status': 'completed',
                                    'executed_price': current_price,
                                    'updated_at': datetime.now(timezone(timedelta(hours=9)))
                                }}
                            )
                
                # 1초 대기
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"주문 감시 중 오류 발생: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생시 5초 대기
