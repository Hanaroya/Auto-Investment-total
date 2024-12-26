import threading
import asyncio
import logging
from typing import List, Dict
from queue import Queue
from database.mongodb_manager import MongoDBManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from datetime import datetime, timedelta, timezone
import time
import signal
from control_center.InvestmentCenter import InvestmentCenter
from trade_market_api.UpbitCall import UpbitCall

class TradingThread(threading.Thread):
    """
    개별 코인 그룹을 처리하는 거래 스레드
    각 스레드는 할당된 코인들에 대해 독립적으로 거래 분석 및 실행을 담당합니다.
    """
    def __init__(self, thread_id: int, coins: List[str], db: MongoDBManager, config: Dict, shared_locks: Dict):
        """
        Args:
            thread_id (int): 스레드 식별자
            coins (List[str]): 처리할 코인 목록
            db (MongoDBManager): 데이터베이스 인스턴스
            config: 설정 정보가 담긴 딕셔너리
            shared_locks (Dict): 공유 락 딕셔너리
        """
        super().__init__()
        self.thread_id = thread_id
        self.coins = coins
        self.db = db
        self.config = config
        self.shared_locks = shared_locks
        self.logger = logging.getLogger(f"InvestmentCenter.Thread-{thread_id}")
        self.stop_flag = threading.Event()
        self.loop = None
        
        # 각 인스턴스 생성
        self.market_analyzer = MarketAnalyzer(config=self.config)
        self.trading_manager = TradingManager()
        
        # UpbitCall 인스턴스 생성
        self.upbit = UpbitCall(
            self.config['api_keys']['upbit']['access_key'],
            self.config['api_keys']['upbit']['secret_key']
        )
        
        # 설정에서 최대 투자금 가져오기 (기본값: 80,000원)
        self.max_investment = config.get('trading_settings', {}).get('max_thread_investment', 80000)

    def run(self):
        """스레드 실행"""
        try:
            # 각 스레드별 새로운 이벤트 루프 생성
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # 각 스레드마다 새로운 MongoDBManager 인스턴스 생성
            self.db = MongoDBManager()
            
            # 이벤트 루프에서 process_coins 실행
            self.loop.run_until_complete(self.process_coins())
        finally:
            self.loop.close()

    async def process_coins(self):
        """코인 목록 처리"""
        self.logger.info(f"Thread {self.thread_id}: 마켓 분석 시작 - {len(self.coins)} 개의 코인")
        
        for coin in self.coins:
            if self.stop_flag.is_set():
                break
                
            try:
                # 현재 스레드의 이벤트 루프 컨텍스트에서 실행
                await self.process_single_coin(coin)
            except Exception as e:
                self.logger.error(f"Error processing {coin}: {e}")
                continue

    async def process_single_coin(self, coin: str):
        """단일 코인 처리"""
        try:
            # 캔들 데이터 조회 - 락으로 보호
            with self.shared_locks['candle_data']:
                self.logger.debug(f"Thread {self.thread_id} acquired lock for {coin}")
                candles = self.upbit.get_candle(market=coin, interval='1', count=200)
                self.logger.debug(f"Thread {self.thread_id} released lock for {coin}")
                
            if not candles or len(candles) < 100:
                self.logger.warning(f"Thread {self.thread_id}: {coin} - 불충분한 캔들 데이터 (수신: {len(candles)})")
                return

            # 현재 투자 상태 확인
            collection = self.db.get_collection('trades')
            pipeline = [
                {'$match': {'thread_id': self.thread_id, 'status': 'active'}},
                {'$group': {'_id': None, 'total': {'$sum': '$total_investment'}}}
            ]
            cursor = collection.aggregate(pipeline)
            result = await cursor.to_list(length=None)
            current_investment = result[0]['total'] if result else 0

            # 최대 투자금 체크
            if current_investment >= self.max_investment:
                self.logger.info(f"Thread {self.thread_id}: {coin} - 최대 투자금 도달")
                return
            # 마켓 분석 수행
            signals = self.market_analyzer.analyze_market(coin, candles)

            # 분석 결과 저장 및 거래 신호 처리
            with self.shared_locks['trade']:
                strategy_collection = self.db.get_collection('strategy_data')
                trades_collection = self.db.get_collection('trades')
                
                # 현재 코인의 활성 거래 확인
                active_trade = trades_collection.find_one({
                    'coin': coin,
                    'thread_id': self.thread_id,
                    'status': 'active'
                })

                current_price = candles[-1]['close']
                
                # 거래 신호에 따른 처리
                if active_trade:
                    # 매도 신호 확인
                    if signals.get('overall_signal') == 'sell':
                        trades_collection.update_one(
                            {'_id': active_trade['_id']},
                            {
                                '$set': {
                                    'status': 'completed',
                                    'sell_price': current_price,
                                    'sell_time': datetime.now(timezone(timedelta(hours=9))),
                                    'profit_rate': (current_price - active_trade['buy_price']) / active_trade['buy_price'] * 100
                                }
                            }
                        )
                        self.logger.info(f"매도 신호: {coin} - 수익률: {((current_price - active_trade['buy_price']) / active_trade['buy_price'] * 100):.2f}%")
                
                else:
                    # 매수 신호 확인
                    if signals.get('overall_signal') == 'buy' and current_investment < self.max_investment:
                        investment_amount = min(10000, self.max_investment - current_investment)  # 최소 투자금
                        
                        trades_collection.insert_one({
                            'thread_id': self.thread_id,
                            'coin': coin,
                            'buy_price': current_price,
                            'buy_time': datetime.now(timezone(timedelta(hours=9))),
                            'total_investment': investment_amount,
                            'status': 'active',
                            'signals_at_buy': signals
                        })
                        self.logger.info(f"매수 신호: {coin} - 투자금액: {investment_amount:,}원")

                # 전략 데이터 업데이트
                strategy_collection.update_one(
                    {'coin': coin},
                    {'$set': {
                        'signals': signals,
                        'current_price': current_price,
                        'updated_at': datetime.now(timezone(timedelta(hours=9)))
                    }},
                    upsert=True
                )

            # 스레드 상태 업데이트
            status_collection = self.db.get_collection('thread_status')
            status_collection.update_one(
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
        self.logger = logging.getLogger('InvestmentCenter.ThreadManager')
        self.event_loop = asyncio.get_event_loop()
        
        # 공유 락 초기화
        self.shared_locks = {
            'candle_data': threading.Lock(),
            'trade': threading.Lock(),
            'market_data': threading.Lock()
        }

    async def start(self, markets: List[Dict]):
        """
        거래 스레드들을 초기화하고 시작합니다.
        
        Args:
            markets (List[Dict]): 분석할 전체 마켓 목록
        """
        try:
            self.running = True
            self.threads = []
            
            # 마켓 그룹화
            market_groups = self.split_markets(markets)
            
            # 기존 스레드 상태 초기화
            self.db.thread_status.delete_many({})
            
            # 각 마켓 그룹에 대한 스레드 생성
            for thread_id, market_group in enumerate(market_groups):
                # 현재 시간 (KST)
                kst_now = datetime.now(timezone(timedelta(hours=9)))
                
                # DB에 스레드 상태 등록 (KST 시간 사용)
                self.db.thread_status.insert_one({
                    'thread_id': thread_id,
                    'assigned_coins': market_group,
                    'current_investment': 0,
                    'is_active': True,
                    'last_updated': kst_now,
                    'created_at': kst_now,
                    'updated_at': kst_now.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                thread = TradingThread(
                    thread_id=thread_id,
                    coins=market_group,
                    db=self.db,
                    config=self.config,
                    shared_locks=self.shared_locks
                )
                thread.start()
                self.threads.append(thread)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error starting threads: {str(e)}")
            return False

    def split_markets(self, markets: List[Dict]) -> List[List[Dict]]:
        """
        전체 마켓 목록을 10개의 균등한 그룹으로 분할합니다.
        
        Args:
            markets (List[Dict]): 분할할 마켓 목록
            
        Returns:
            List[List[Dict]]: 분할된 마켓 그룹 목록
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
                self.logger.debug(f"Thread {i}에 할당된 첫 번째 코인: {group[0]['market'] if group else 'None'}")
            
            return market_groups
            
        except Exception as e:
            self.logger.error(f"마켓 분할 중 오류: {str(e)}")
            raise

    async def stop_all_threads(self):
        """
        모든 거래 스레드를 안전하게 중지시킵니다.
        
        프로세스:
            1. 각 스레드에 중지 신호 전송
            2. 스레드 종료 대기
            3. 마켓 데이터 정리
            4. 스레드 상태 업데이트
            5. 스레드 목록 정리
        """
        try:
            # 스레드 중지
            for thread in self.threads:
                thread.stop_flag.set()
                
            # 스레드 종료 대기
            for thread in self.threads:
                thread.join()
                
            # 마켓 데이터 정리
            self.cleanup_market_data()
                
            # 스레드 상태 업데이트
            for thread_id in range(len(self.threads)):
                self.db.update_thread_status(thread_id, {
                    'is_active': False,
                    'last_updated': datetime.utcnow()
                })
                
            self.threads.clear()
            self.logger.info("All threads stopped and data cleaned up successfully")

        except Exception as e:
            self.logger.error(f"Error stopping threads: {e}")

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
        """
        키보드 인터럽트나 시그널 처리
        """
        self.logger.info("Interrupt received, starting cleanup process...")
        
        # 이벤트 루프가 없는 경우 새로 생성
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # 정리 작업 실행
        loop.run_until_complete(self.stop_all_threads())
        loop.close() 