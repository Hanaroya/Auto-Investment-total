import threading
import asyncio
import logging
from typing import List, Dict
from queue import Queue
from database.mongodb_manager import MongoDBManager
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from datetime import datetime, timedelta
import time
import signal

class TradingThread(threading.Thread):
    """
    개별 코인 그룹을 처리하는 거래 스레드
    각 스레드는 할당된 코인들에 대해 독립적으로 거래 분석 및 실행을 담당합니다.
    """
    def __init__(self, thread_id: int, coins: List[str], 
                 market_analyzer: MarketAnalyzer, 
                 trading_manager: TradingManager,
                 event_loop,
                 shared_locks: Dict[str, threading.Lock]):
        """
        Args:
            thread_id (int): 스레드 식별자
            coins (List[str]): 처리할 코인 목록
            market_analyzer (MarketAnalyzer): 시장 분석기 인스턴스
            trading_manager (TradingManager): 거래 관리자 인스턴스
            event_loop: 비동기 이벤트 루프
            shared_locks (Dict[str, threading.Lock]): 공유 리소스에 대한 락
        """
        super().__init__()
        self.thread_id = thread_id
        self.coins = coins
        self.market_analyzer = market_analyzer
        self.trading_manager = trading_manager
        self.db = MongoDBManager()
        self.event_loop = event_loop
        self.stop_flag = threading.Event()
        self.logger = logging.getLogger(f"TradingThread-{thread_id}")
        self.shared_locks = shared_locks

    def run(self):
        """스레드 실행"""
        asyncio.set_event_loop(self.event_loop)
        while not self.stop_flag.is_set():
            try:
                self.logger.info(f"Thread {self.thread_id}: Analyzing {len(self.coins)} markets...")
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
        """
        단일 코인에 대한 거래 프로세스를 실행합니다.
        
        Args:
            coin (str): 처리할 코인 심볼
            
        프로세스:
            1. 캔들 데이터 조회
            2. 현재 투자 상태 확인
            3. 최대 투자 금액 제한 확인
            4. 매수/매도 신호 분석 및 실행
        """
        try:
            # 캔들 데이터 조회 시 락 사용
            with self.shared_locks['candle_data']:
                candle_data = await self.market_analyzer.get_candle_data(coin)
                if not candle_data:
                    return

            # 현재 투자 상태 확인
            current_investment = await self.get_current_investment()
            
            # 최대 투자 금액 체크
            if current_investment >= 80000:  # 스레드당 최대 투자금액
                analysis_result = await self.check_sell_signals(coin, candle_data)
                if analysis_result:
                    self.logger.info(f"Thread {self.thread_id}: {coin} - {analysis_result}")
                return

            # 매수/매도 신호 확인
            analysis_result = await self.check_trading_signals(coin, candle_data)
            if analysis_result:
                self.logger.info(f"Thread {self.thread_id}: {coin} - {analysis_result}")

        except Exception as e:
            self.logger.error(f"Error in process_single_coin for {coin}: {e}")

    async def check_trading_signals(self, coin: str, candle_data: List[Dict]):
        """
        코인의 매매 신호를 확인하고 적절한 거래를 실행합니다.
        
        Args:
            coin (str): 대상 코인
            candle_data (List[Dict]): 분석할 캔들 데이터
            
        동작:
            - 전략 분석 실행
            - 매수/매도 신호에 따른 거래 처리
        """
        try:
            # 전략 실행 및 신호 확인
            signal = await self.analyze_signals(candle_data)
            
            if signal['action'] == 'buy':
                # 매수 시 락 사용
                with self.shared_locks['buy']:
                    await self.trading_manager.process_buy_signal(
                        coin=coin,
                        thread_id=self.thread_id,
                        signal_strength=signal['strength'],
                        price=signal['price'],
                        strategy_data=signal['strategy_data']
                    )
            elif signal['action'] == 'sell':
                # 매도 시 락 사용
                with self.shared_locks['sell']:
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
        """
        현재 스레드의 활성 투자 총액을 조회합니다.
        
        Returns:
            float: 현재 투자된 총 금액
        """
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
        캔들 데이터를 분석하여 거래 신호를 생성합니다.
        
        Args:
            candle_data (List[Dict]): 분석할 캔들 데이터
            
        Returns:
            Dict: {
                'action': 거래 행동 ('buy', 'sell', 'hold'),
                'strength': 신호 강도 (0-1),
                'price': 현재 가격,
                'strategy_data': 전략 분석 상세 데이터
            }
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
    """
    여러 거래 스레드를 관리하는 매니저 클래스
    코인 목록을 여러 스레드로 분할하여 병렬 처리를 관리합니다.
    """
    
    def __init__(self, config):
        """
        ThreadManager 초기화
        Args:
            config: 설정 정보가 담긴 딕셔너리
        """
        self.config = config
        self.market_analyzer = MarketAnalyzer(self.config)
        self.threads: List[TradingThread] = []
        self.trading_manager = TradingManager()
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.event_loop = asyncio.get_event_loop()
        
        # 공유 리소스에 대한 락 초기화
        self.shared_locks = {
            'buy': threading.Lock(),      # 매수 작업을 위한 락
            'sell': threading.Lock(),     # 매도 작업을 위한 락
            'candle_data': threading.Lock()  # 캔들 데이터 조회를 위한 락
        }
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self.handle_interrupt)
        signal.signal(signal.SIGTERM, self.handle_interrupt)

    async def start(self, markets: List[Dict]):
        """
        거래 스레드들을 초기화하고 시작합니다.
        
        Args:
            markets (List[Dict]): 분석할 전체 마켓 목록
        """
        try:
            self.running = True
            self.threads = []
            
            # split_markets 메서드를 사용하여 마켓 그룹화
            market_groups = self.split_markets(markets)
            
            # 기존 스레드 상태 초기화
            self.db.get_collection('thread_status').delete_many({})
            
            # 각 그룹에 대한 TradingThread 생성
            for group_idx, market_group in enumerate(market_groups):
                market_codes = [market_data['market'] for market_data in market_group]
                
                # DB에 스레드 상태 등록
                self.db.get_collection('thread_status').insert_one({
                    'thread_id': group_idx,
                    'assigned_coins': market_codes,
                    'current_investment': 0,
                    'is_active': True,
                    'last_updated': datetime.utcnow()
                })
                
                # 스레드 생성 및 시작
                thread = TradingThread(
                    thread_id=group_idx,
                    coins=market_codes,
                    market_analyzer=self.market_analyzer,
                    trading_manager=self.trading_manager,
                    event_loop=self.event_loop,
                    shared_locks=self.shared_locks
                )
                thread.start()
                self.threads.append(thread)
                
            self.logger.info(f"{len(self.threads)} analysis threads started for {len(markets)} markets")
            
        except Exception as e:
            self.logger.error(f"Error starting threads: {e}")
            await self.stop_all_threads()

    def split_markets(self, markets: List[Dict]) -> List[List[Dict]]:
        """
        전체 마켓 목록을 10개의 균등한 그룹으로 분할합니다.
        
        Args:
            markets (List[Dict]): 분할할 마켓 목록
            
        Returns:
            List[List[Dict]]: 분할된 마켓 그룹 리스트
        """
        n = len(markets)
        k = min(10, n)  # 마켓 수가 10개 미만일 경우 마켓 수만큼만 생성
        
        # 각 그룹의 최소 크기 계산
        min_size = n // k
        remainder = n % k
        
        groups = []
        start = 0
        
        for i in range(k):
            # 나머지를 균등하게 분배
            group_size = min_size + (1 if i < remainder else 0)
            end = start + group_size
            
            if end > start:  # 빈 그룹 방지
                groups.append(markets[start:end])
            
            start = end
            
        # 실제 생성된 그룹 수 로깅
        self.logger.info(f"Created {len(groups)} market groups")
        for i, group in enumerate(groups):
            self.logger.info(f"Group {i}: {len(group)} markets")
            
        return groups

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
            await self.cleanup_market_data()
                
            # 스레드 상태 업데이트
            for thread_id in range(len(self.threads)):
                await self.db.update_thread_status(thread_id, {
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
        
        검사 항목:
            - 스레드 활성 상태
            - 마지막 업데이트 시간 확인 (5분 이상 경과시 경고)
        """
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

    async def market_analysis_loop(self, thread_id: int, coins: List[str]):
        """
        마켓 분석 루프
        - 10분마다 코인별 매수/매도 조건 체크
        """
        try:
            while self.running:
                self.logger.info(f"Thread {thread_id}: Analyzing {len(coins)} markets...")
                
                for coin in coins:
                    try:
                        # 마켓 분석 및 거래 실행
                        analysis_result = await self.market_analyzer.analyze_market(coin)
                        if analysis_result:
                            self.logger.info(f"Thread {thread_id}: {coin} - {analysis_result}")
                    except Exception as e:
                        self.logger.error(f"Thread {thread_id}: Error analyzing {coin}: {e}")
                
                # 스레드 상태 업데이트
                await self.update_thread_status(thread_id, coins)
                
                # 10분 대기
                await asyncio.sleep(600)
                
        except Exception as e:
            self.logger.error(f"Thread {thread_id}: Analysis loop error: {e}")
            self.running = False

    async def cleanup_market_data(self):
        """
        마켓 데이터 정리
        """
        try:
            # DB에서 마켓 데이터 삭제
            self.db.get_collection('market_data').delete_many({})
            self.logger.info("Market data cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up market data: {e}")

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