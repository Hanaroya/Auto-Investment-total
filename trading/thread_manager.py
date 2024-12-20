import threading
import asyncio
import logging
from typing import List, Dict
from queue import Queue
from database.mongodb_manager import MongoDBManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from datetime import datetime, timedelta

class TradingThread(threading.Thread):
    def __init__(self, thread_id: int, coins: List[str], 
                 market_analyzer: MarketAnalyzer, 
                 trading_manager: TradingManager,
                 event_loop):
        super().__init__()
        self.thread_id = thread_id
        self.coins = coins
        self.market_analyzer = market_analyzer
        self.trading_manager = trading_manager
        self.db = MongoDBManager()
        self.event_loop = event_loop
        self.stop_flag = threading.Event()
        self.logger = logging.getLogger(f"TradingThread-{thread_id}")

    def run(self):
        """스레드 실행"""
        asyncio.set_event_loop(self.event_loop)
        while not self.stop_flag.is_set():
            try:
                self.process_coins()
                # 4시간 캔들 기준으로 대기
                self.stop_flag.wait(60)  # 1분마다 체크
            except Exception as e:
                self.logger.error(f"Error in thread {self.thread_id}: {e}")

    def process_coins(self):
        """할당된 코인들 처리"""
        for coin in self.coins:
            if self.stop_flag.is_set():
                break

            try:
                # 비동기 함수 실행을 위한 future 생성
                future = asyncio.run_coroutine_threadsafe(
                    self.process_single_coin(coin),
                    self.event_loop
                )
                future.result()  # 결과 대기
            except Exception as e:
                self.logger.error(f"Error processing coin {coin}: {e}")

    async def process_single_coin(self, coin: str):
        """단일 코인 처리"""
        try:
            # 캔들 데이터 조회
            candle_data = await self.market_analyzer.get_candle_data(coin)
            if not candle_data:
                return

            # 현재 투자 상태 확인
            current_investment = await self.get_current_investment()
            
            # 최대 투자 금액 체크
            if current_investment >= 80000:  # 스레드당 최대 투자금액
                await self.check_sell_signals(coin, candle_data)
                return

            # 매수/매도 신호 확인
            await self.check_trading_signals(coin, candle_data)

        except Exception as e:
            self.logger.error(f"Error in process_single_coin for {coin}: {e}")

    async def check_trading_signals(self, coin: str, candle_data: List[Dict]):
        """매매 신호 확인"""
        try:
            # 전략 실행 및 신호 확인
            signal = await self.analyze_signals(candle_data)
            
            if signal['action'] == 'buy':
                await self.trading_manager.process_buy_signal(
                    coin=coin,
                    thread_id=self.thread_id,
                    signal_strength=signal['strength'],
                    price=signal['price'],
                    strategy_data=signal['strategy_data']
                )
            elif signal['action'] == 'sell':
                await self.trading_manager.process_sell_signal(
                    coin=coin,
                    thread_id=self.thread_id,
                    signal_strength=signal['strength'],
                    price=signal['price'],
                    strategy_data=signal['strategy_data']
                )

        except Exception as e:
            self.logger.error(f"Error in check_trading_signals for {coin}: {e}")

    async def get_current_investment(self) -> float:
        """현재 스레드의 총 투자금액 조회"""
        try:
            result = await self.db.get_collection('trades').aggregate([
                {
                    '$match': {
                        'thread_id': self.thread_id,
                        'status': 'active'
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'total': {'$sum': '$total_investment'}
                    }
                }
            ]).to_list(1)

            return result[0]['total'] if result else 0

        except Exception as e:
            self.logger.error(f"Error getting current investment: {e}")
            return 0

    async def analyze_signals(self, candle_data: List[Dict]) -> Dict:
        """
        캔들 데이터를 분석하여 매매 신호를 생성
        """
        try:
            # MarketAnalyzer를 통해 신호 분석
            signal = await self.market_analyzer.analyze_trading_signals(candle_data)
            
            return {
                'action': signal.get('action', 'hold'),  # 'buy', 'sell', 'hold' 중 하나
                'strength': signal.get('strength', 0),   # 신호 강도 (0-1)
                'price': signal.get('price', 0),         # 현재 가격
                'strategy_data': signal.get('strategy_data', {})  # 전략 관련 추가 데이터
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing signals: {e}")
            return {
                'action': 'hold',
                'strength': 0,
                'price': 0,
                'strategy_data': {}
            }

class ThreadManager:
    def __init__(self):
        self.threads: List[TradingThread] = []
        self.market_analyzer = MarketAnalyzer()
        self.trading_manager = TradingManager()
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.event_loop = asyncio.get_event_loop()

    async def start(self, markets: List[Dict]):
        """스레드 시작"""
        try:
            # 코인 리스트를 10개의 그룹으로 분할
            coin_groups = self.split_markets(markets)
            
            # 각 그룹에 대해 스레드 생성 및 시작
            for thread_id, coins in enumerate(coin_groups):
                thread = TradingThread(
                    thread_id=thread_id,
                    coins=coins,
                    market_analyzer=self.market_analyzer,
                    trading_manager=self.trading_manager,
                    event_loop=self.event_loop
                )
                
                # 스레드 상태 초기화
                await self.db.update_thread_status(thread_id, {
                    'assigned_coins': coins,
                    'current_investment': 0,
                    'is_active': True,
                    'last_updated': datetime.utcnow()
                })
                
                thread.start()
                self.threads.append(thread)
                
            self.logger.info(f"Started {len(self.threads)} trading threads")

        except Exception as e:
            self.logger.error(f"Error starting threads: {e}")
            await self.stop_all_threads()

    def split_markets(self, markets: List[Dict]) -> List[List[str]]:
        """마켓 리스트를 10개의 그룹으로 분할"""
        coins = [market['market'] for market in markets]
        n = len(coins)
        k = 10  # 스레드 수
        
        # 각 그룹의 크기 계산
        group_size = n // k
        remainder = n % k
        
        groups = []
        start = 0
        
        for i in range(k):
            # 나머지가 있는 경우 앞쪽 그룹에 하나씩 추가
            end = start + group_size + (1 if i < remainder else 0)
            groups.append(coins[start:end])
            start = end
            
        return groups

    async def stop_all_threads(self):
        """모든 스레드 중지"""
        try:
            for thread in self.threads:
                thread.stop_flag.set()
                
            # 스레드 종료 대기
            for thread in self.threads:
                thread.join()
                
            # 스레드 상태 업데이트
            for thread_id in range(len(self.threads)):
                await self.db.update_thread_status(thread_id, {
                    'is_active': False,
                    'last_updated': datetime.utcnow()
                })
                
            self.threads.clear()
            self.logger.info("All threads stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping threads: {e}")

    async def check_thread_health(self):
        """스레드 상태 확인"""
        try:
            for thread_id in range(len(self.threads)):
                status = await self.db.get_collection('thread_status').find_one({
                    'thread_id': thread_id
                })
                
                if status and status['is_active']:
                    last_updated = status['last_updated']
                    if datetime.utcnow() - last_updated > timedelta(minutes=5):
                        self.logger.warning(f"Thread {thread_id} may be stuck")
                        # 필요한 경우 스레드 재시작 로직 추가

        except Exception as e:
            self.logger.error(f"Error checking thread health: {e}") 