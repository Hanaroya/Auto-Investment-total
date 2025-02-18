import sys
import threading
import logging
import time
from typing import List, Dict
import os
from utils.time_utils import TimeUtils
from datetime import datetime, timezone, timedelta
from math import floor
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from database.mongodb_manager import MongoDBManager
from trading.trading_strategy import TradingStrategy
import asyncio
from trading.long_term_trading_manager import LongTermTradingManager

class TradingError(Exception):
    """거래 관련 기본 예외 클래스"""
    pass

class DataFetchError(TradingError):
    """데이터 조회 실패"""
    pass

class OrderExecutionError(TradingError):
    """주문 실행 실패"""
    pass

class RecoveryManager:
    def __init__(self):
        self.retry_count = 0
        self.max_retries = 3
        self.recovery_delay = 5  # 초
        
    async def execute_with_recovery(self, func, *args, **kwargs):
        """재시도 메커니즘이 포함된 실행"""
        while self.retry_count < self.max_retries:
            try:
                result = await func(*args, **kwargs)
                self.retry_count = 0  # 성공 시 카운트 리셋
                return result
            except DataFetchError as e:
                self.retry_count += 1
                await asyncio.sleep(self.recovery_delay)
                if self.retry_count >= self.max_retries:
                    raise
            except OrderExecutionError as e:
                # 주문 실패는 즉시 상위로 전파
                raise
                
    async def recover_trade_state(self, trade_id: str):
        """거래 상태 복구"""
        # 미완료 거래 상태 확인
        # 부분 체결 확인
        # 주문 취소 필요 시 취소 처리
        pass

class TradingThread(threading.Thread):
    """
    개별 마켓 그룹을 처리하는 거래 스레드
    각 스레드는 할당된 마켓들에 대해 독립적으로 거래 분석 및 실행을 담당합니다.
    """
    def __init__(self, thread_id: int, markets: List[str], db: MongoDBManager, exchange_name: str, config: Dict, shared_locks: Dict, stop_flag: threading.Event, investment_center=None):
        """
        Args:
            thread_id (int): 스레드 식별자
            markets (List[str]): 처리할 마켓 목록
            db (MongoDBManager): 데이터베이스 인스턴스
            config: 설정 정보가 담긴 딕셔너리
            shared_locks (Dict): 공유 락 딕셔너리
            stop_flag (threading.Event): 전역 중지 플래그
            investment_center: InvestmentCenter 인스턴스
        """
        super().__init__()
        self.thread_id = thread_id
        self.markets = markets
        self.db = db
        self.config = config
        self.shared_locks = shared_locks
        self.stop_flag = stop_flag
        self.logger = logging.getLogger(f"InvestmentCenter.Thread-{thread_id}")
        self.loop = None
        self.exchange_name = exchange_name
        
        # 각 인스턴스 생성
        self.market_analyzer = MarketAnalyzer(config=self.config, exchange_name=exchange_name)
        self.trading_manager = TradingManager(exchange_name=exchange_name)
        
        # system_config에서 설정값 가져오기
        system_config = self.db.system_config.find_one({'exchange': self.exchange_name})
        if not system_config:
            self.logger.error("system_config를 찾을 수 없습니다. 기본값 사용")
            self.max_investment = float(os.getenv('MAX_THREAD_INVESTMENT', 80000))
            self.total_max_investment = float(os.getenv('TOTAL_MAX_INVESTMENT', 1000000))
            self.investment_each = (self.total_max_investment * 0.8) / 20
        else:
            self.max_investment = system_config.get('max_thread_investment', 80000)
            self.total_max_investment = system_config.get('total_max_investment', 1000000)
            self.investment_each = (self.total_max_investment * 0.8) / 20
        
        # 동적 임계값 조정을 위한 전략 초기화
        self.trading_strategy = TradingStrategy(config, self.total_max_investment)
        
        self.db.portfolio.update_one(
                    {'exchange': self.exchange_name},
                    {'$set': {
                        'investment_amount': system_config.get('total_max_investment', 1000000),
                        'available_investment': floor(self.total_max_investment * 0.8),
                        'current_amount': floor(self.total_max_investment * 0.8),
                        'reserve_amount': floor(system_config.get('total_max_investment', 1000000) * 0.2)
                        }
                     }
                )
        
        self.logger.info(f"Thread {thread_id} 초기화 완료 (최대 투자금: {self.max_investment:,}원)")
        
        # system_config 모니터링 및 업데이트를 위한 마지막 체크 시간 추가
        self.last_config_check = datetime.now()

        self.investment_center = investment_center  # InvestmentCenter 인스턴스 저장
        self.long_term_trades = {}  # 장기 투자 거래 추적을 위한 딕셔너리 추가

        # LongTermTradingManager 인스턴스 추가
        self.long_term_manager = LongTermTradingManager(
            db=self.db,
            exchange_name=exchange_name,
            config=self.config
        )

    def run(self):
        """스레드 실행"""
        try:
            # 비동기 이벤트 루프 생성
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.logger.info(f"Thread {self.thread_id}: 마켓 분석 시작 - {len(self.markets)} 개의 마켓")
            
            while not self.stop_flag.is_set():
                cycle_start_time = time.time()
                
                # 스레드 ID에 따라 다른 대기 시간 설정
                if self.thread_id < 4:
                    wait_time = 6
                    initial_delay = self.thread_id * 1
                else:
                    wait_time = 180
                    initial_delay = (self.thread_id - 4) * 1
                
                time.sleep(initial_delay)
                
                for market in self.markets:
                    if self.stop_flag.is_set():
                        break
                        
                    try:
                        # 비동기 함수를 동기적으로 실행
                        self.loop.run_until_complete(self.process_single_market(market))
                    except Exception as e:
                        import traceback
                        tb = traceback.extract_tb(sys.exc_info()[2])[-1]
                        error_statement = tb.line  # 실제 에러가 발생한 코드 라인의 내용
                        self.logger.error(f"Error processing {market}: {str(e)} in statement: '{error_statement}' at {tb.filename}:{tb.lineno}")
                        continue
                
                cycle_duration = time.time() - cycle_start_time
                remaining_time = wait_time - cycle_duration - initial_delay
                if remaining_time > 0:
                    time.sleep(remaining_time)
                    
            self.logger.info(f"Thread {self.thread_id} 종료")
        
        except Exception as e:
            self.logger.error(f"Thread {self.thread_id} error: {str(e)}")
        finally:
            if self.loop and self.loop.is_running():
                self.loop.close()
            self.logger.info(f"Thread {self.thread_id} 정리 작업 완료")

    async def process_single_market(self, market: str):
        """단일 마켓 처리"""
        try:
            # 시장 상태 조회 
            market_condition = self._get_market_condition(exchange=self.investment_center.exchange_name, market=market)
            if not market_condition:
                self.logger.debug(f"{market}: 시장 상태 데이터 없음")
                return
            
            # AFR 데이터 유효성 검사
            if any(market_condition.get(key) is None for key in ['AFR', 'current_change', 'market_fear_and_greed']):
                self.logger.debug(f"{market}: AFR 데이터 누락")
                return

            current_fear_greed = None
            market_fear_greed = None
            
            # 시장 상태 분석
            try:
                analyzed_market = self._analyze_market_condition(
                    current_afr=market_condition['AFR'],
                    current_change=market_condition['current_change'],
                    current_fear_greed=market_condition['market_fear_and_greed'],
                    afr_history=market_condition.get('AFR_history', []),
                    change_history=market_condition.get('change_history', []),
                    fear_greed_history=market_condition.get('fear_greed_history', [])
                )
                
                if not analyzed_market:
                    self.logger.debug(f"{market}: 시장 분석 실패")
                    return
                    
                market_condition.update(analyzed_market)
                current_fear_greed = market_condition['market_fear_and_greed']
                market_fear_greed = market_condition['feargreed']
            except Exception as e:
                self.logger.error(f"{market}: 시장 분석 중 오류 - {str(e)}")
                return

            # 여러 시간대의 캔들 데이터 조회
            with self.shared_locks['candle_data']:
                candles_1m = None
                candles_15m = None
                candles_240m = None
                
                try:
                    if self.thread_id < 4:  # 0~3번 스레드
                        candles_1m = self.investment_center.exchange.get_candle(
                            market=market, interval='1', count=300)
                        if not candles_1m:
                            self.logger.warning(f"{market}: 1분봉 데이터 없음")
                            return
                        
                        candles_15m = self.investment_center.exchange.get_candle(
                            market=market, interval='15', count=300)
                        if not candles_15m:
                            self.logger.warning(f"{market}: 15분봉 데이터 없음")
                            return
                        
                        candles_240m = self.investment_center.exchange.get_candle(
                            market=market, interval='240', count=300)
                        if not candles_240m:
                            self.logger.warning(f"{market}: 4시간봉 데이터 없음")
                            return  
                        
                    else:  # 4~9번 스레드
                        candles_15m = self.investment_center.exchange.get_candle(
                            market=market, interval='15', count=300)
                        if not candles_15m:
                            self.logger.warning(f"{market}: 15분봉 데이터 없음")
                            return
                        
                        candles_240m = self.investment_center.exchange.get_candle(
                            market=market, interval='240', count=300)
                        if not candles_240m:
                            self.logger.warning(f"{market}: 4시간봉 데이터 없음")
                            return
                        
                    # 캔들 데이터 길이 검증
                    if self.thread_id < 4:
                        if len(candles_1m) < 2:
                            self.logger.warning(f"{market}: 1분봉 데이터 부족 (개수: {len(candles_1m)})")
                            return
                        if len(candles_15m) < 2:
                            self.logger.warning(f"{market}: 15분봉 데이터 부족 (개수: {len(candles_15m)})")
                            return
                    else:
                        if len(candles_15m) < 2:
                            self.logger.warning(f"{market}: 15분봉 데이터 부족 (개수: {len(candles_15m)})")
                            return
                        if len(candles_240m) < 2:
                            self.logger.warning(f"{market}: 4시간봉 데이터 부족 (개수: {len(candles_240m)})")
                            return
                        
                    # 마지막 캔들 데이터 검증
                    if self.thread_id < 4:
                        if not candles_1m[-1] or not candles_15m[-1]:
                            self.logger.warning(f"{market}: 최근 캔들 데이터 누락")
                            return
                    else:
                        if not candles_15m[-1] or not candles_240m[-1]:
                            self.logger.warning(f"{market}: 최근 캔들 데이터 누락")
                            return
                    
                except Exception as e:
                    self.logger.error(f"{market}: 캔들 데이터 조회 실패 - {str(e)}")
                    return

            # 시간대별 추세 분석 전 데이터 검증
            if self.thread_id < 4 and (not isinstance(candles_1m, list) or not isinstance(candles_15m, list)):
                self.logger.error(f"{market}: 잘못된 캔들 데이터 형식")
                return
            elif self.thread_id >= 4 and (not isinstance(candles_15m, list) or not isinstance(candles_240m, list)):
                self.logger.error(f"{market}: 잘못된 캔들 데이터 형식")
                return

            # 시간대별 추세 분석
            try:
                trends = self._analyze_multi_timeframe_trends(candles_1m, candles_15m, candles_240m)
                if not trends:
                    self.logger.warning(f"{market}: 추세 분석 실패")
                    return
            except Exception as e:
                self.logger.error(f"{market}: 추세 분석 중 오류 - {str(e)}")
                return
            
            # 동적 임계값 조정
            thresholds = self.trading_strategy.adjust_thresholds(market_condition, trends)
            
            # 현재 투자 상태 확인
            active_trades = self.db.trades.find({
                'thread_id': self.thread_id, 
                'status': {'$in': ['active', 'converted']}
            })
            current_investment = sum(trade.get('investment_amount', 0) for trade in active_trades)

            # 최대 투자금 체크 및 시장 상태 확인
            if current_investment >= self.total_max_investment:
                self.logger.info(f"Thread {self.thread_id}: {market} - 거래 제한 "
                               f"(투자금 초과: {current_investment >= self.total_max_investment})")
                return

            # 마켓 분석 수행 시 시장 상태 정보 추가
            signals = self.market_analyzer.analyze_market(market, candles_1m)
            signals.update(market_condition)
            current_price = candles_1m[-1]['close'] if self.thread_id < 4 else candles_15m[-1]['close']
            self.logger.warning(f"{market}: 현재 가격 - {current_price}")
            self.trading_manager.update_strategy_data(market=market, exchange=self.exchange_name, thread_id=self.thread_id, price=current_price, strategy_results=signals)

            # 전역 거래 가능 여부 확인
            with self.shared_locks['portfolio']:
                portfolio = self.db.portfolio.find_one({'exchange': self.exchange_name})
                if portfolio is None:
                    self.logger.error(f"포트폴리오 정보를 찾을 수 없습니다: {self.exchange_name}")
                    # 포트폴리오가 없으면 생성하고 global_tradeable을 false로 설정
                    self.db.portfolio.insert_one({
                        'exchange': self.exchange_name,
                        'current_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000) * 0.8),
                        'available_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000) * 0.8),
                        'reserve_amount': float(os.getenv('INITIAL_INVESTMENT', 1000000) * 0.2),
                        'invested_amount': 0,
                        'profit_earned': 0,
                        'market_list': [],
                        'last_updated': TimeUtils.get_current_kst(),
                        'global_tradeable': False  # 기본값을 False로 설정
                    })
                    self.logger.info(f"새로운 포트폴리오 생성 - global_tradeable: False")
                
                # global_tradeable 필드가 없으면 False로 업데이트
                if 'global_tradeable' not in portfolio:
                    self.db.portfolio.update_one({
                        'exchange': self.exchange_name
                    }, {
                        '$set': {'global_tradeable': False}
                    })
                    self.logger.info(f"포트폴리오 global_tradeable 필드 추가 - 기본값: False")

                if 'global_tradeable' in portfolio and portfolio.get('global_tradeable', False):  # 기본값을 False로 변경
                    self.logger.info(f"전체 마켓 거래 중지 상태")
                    return

            # 개별 마켓 거래 가능 여부 확인
            with self.shared_locks['trade']:
                market_trade = self.db.trades.find_one({
                    'market': market,
                    'exchange': self.exchange_name,
                    'status': {'$in': ['active', 'converted']}
                })
                if market_trade and 'is_tradeable' in market_trade and market_trade.get('is_tradeable', False):  # 기본값을 False로 변경
                    self.logger.info(f"{market}: 거래 중지 상태")
                    return

            # 장기 투자 거래 확인 및 처리
            with self.shared_locks['long_term_trades']:
                long_term_trades = self.db.long_term_trades.find({
                    'market': market,
                    'exchange': self.exchange_name,
                    'status': 'active',
                    'thread_id': self.thread_id
                })

                for long_term_trade in long_term_trades:
                    try:
                        # 1. 매도 조건 확인
                        if self.long_term_manager.check_sell_conditions(
                            trade=long_term_trade,
                            current_price=current_price,
                            market_condition=market_condition,
                            trends=trends
                        ):
                            # 매도 신호 처리
                            sell_reason = "장기 투자 목표 달성 또는 손절"
                            if self.trading_manager.process_sell_signal(
                                market=market,
                                exchange=self.exchange_name,
                                thread_id=self.thread_id,
                                signal_strength=market_condition.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data={
                                    'trade_type': 'long_term',
                                    'trade_id': str(long_term_trade['_id']),
                                    'profit_rate': ((current_price - long_term_trade['average_price']) / long_term_trade['average_price']) * 100,
                                    'market_condition': market_condition,
                                    'trends': trends
                                },
                                sell_message=sell_reason,
                                test_mode=self.config.get('test_mode', True)
                            ):
                                self.logger.info(f"{market} 장기 투자 매도 신호 처리 완료")
                        
                        # 2. 추가 매수 조건 확인
                        if long_term_trade.get('positions'):  # positions 배열이 존재하는지 확인
                            # 마지막 position의 timestamp 가져오기
                            last_position = long_term_trade['positions'][-1]
                            last_investment_time = last_position.get('timestamp')
                            
                            if last_investment_time:
                                time_diff = TimeUtils.get_current_kst() - TimeUtils.from_mongo_date(last_investment_time)
                                if time_diff.total_seconds() >= 3600:  # 1시간 이상 경과
                                    # 최소 투자금의 2배 계산
                                    min_trade_amount = self.config.get('min_trade_amount', 5000)
                                    investment_amount = min_trade_amount * 2
                                    buy_reason = "장기 투자 추가 매수"

                                    # 투자 가능한 최대 금액 확인 및 시장 상황에 관계 없이 지속적으로 투자 
                                    portfolio = self.db.portfolio.find_one({'exchange': self.exchange_name})
                                    if portfolio and portfolio.get('available_amount', 0) >= investment_amount:
                                        strategy_data = {
                                            'investment_amount': investment_amount,
                                            'trade_type': 'long_term_additional',
                                            'is_long_term_trade': True,
                                            'total_investment': long_term_trade.get('total_investment', 0),
                                            'average_price': long_term_trade.get('average_price', 0),
                                            'executed_volume': long_term_trade.get('executed_volume', 0),
                                            'positions': long_term_trade.get('positions', []),
                                            'original_trade_id': long_term_trade.get('original_trade_id'),
                                            'target_profit_rate': long_term_trade.get('target_profit_rate', 5)
                                        }

                                        self.trading_manager.process_buy_signal(
                                            market=market,
                                            exchange=self.exchange_name,
                                            thread_id=self.thread_id,
                                            signal_strength=market_condition.get('overall_signal', 0.0),
                                            price=current_price,
                                            strategy_data=strategy_data,
                                            buy_message=buy_reason
                                        )
                                        self.logger.info(f"{market} 장기 투자 추가 매수 처리 완료 - 투자금액: {investment_amount:,}원")

                    except Exception as e:
                        self.logger.error(f"장기 투자 처리 중 오류 ({market}): {str(e)}")
                        continue

            # 분석 결과 저장 및 거래 신호 처리
            with self.shared_locks['trade']:
                try:
                    # 현재 마켓의 활성 거래 확인 및 로깅
                    active_trade = self.db.trades.find_one({
                        'market': market,
                        'exchange': self.exchange_name,
                        'status': {'$in': ['active', 'converted']}
                    })
                    
                    self.logger.info(f"Thread {self.thread_id}: {market} - Active trade check result: {active_trade is not None}")
                    self.logger.debug(f"Signals: {signals}")
                    self.logger.debug(f"Current investment: {current_investment:,}원, Max investment: {self.total_max_investment:,}원")

                    if active_trade:
                        current_profit_rate = active_trade.get('profit_rate', 0)
                        price_trend = signals.get('price_trend', 0)
                        volatility = signals.get('volatility', 0)
                        
                        # MA 대비 가격 확인
                        ma_condition = trends['240m']['price_vs_ma'] < -35 if trends['240m'].get('ma20') else False
                        
                        # 시장 상황에 따른 동적 임계값 조정
                        market_risk = market_condition['risk_level']
                        
                        # 마켓별 공포탐욕지수에 따른 수익 실현 임계값 조정
                        base_profit = 0.15  # 기본 수익률 0.15%
                        
                        if market_fear_greed >= 65:  # 극도의 탐욕
                            profit_threshold = base_profit + 0.3  # 0.45%
                        elif 65 > market_fear_greed >= 55:  # 탐욕
                            profit_threshold = base_profit + 0.2  # 0.35%
                        elif 55 > market_fear_greed >= 45:  # 약한 공포
                            profit_threshold = base_profit + 0.1  # 0.25%
                        else:  # 중립 또는 공포
                            profit_threshold = base_profit  # 0.15%
                        
                        # 시장 상황별 손실 및 변동성 임계값 설정
                        if market_risk > 0.7:  # 고위험 시장
                            loss_threshold = -5.0
                            volatility_threshold = 0.25
                            stagnation_threshold = 0.05
                        elif market_risk > 0.5:  # 중위험 시장
                            loss_threshold = -4.0
                            volatility_threshold = 0.3
                            stagnation_threshold = 0.08
                        else:  # 저위험 시장
                            loss_threshold = -3.0
                            volatility_threshold = 0.3
                            stagnation_threshold = 0.08
                        
                        # 1. 수익 실현 조건 수정 (5-10분 내 매도 목표)
                        profit_take_condition = (
                            (current_profit_rate >= (profit_threshold * 2)) or  # 최소 수익의 2배 달성 시
                            (current_profit_rate >= profit_threshold and (  # 최소 수익 달성 시
                                (self.thread_id < 4 and (
                                    trends['1m']['trend'] < 0.07 or  # 1분봉 하락 전환 즉시
                                    trends['15m']['trend'] < -0.07  # 15분봉 미세 하락
                                )) or
                                (self.thread_id >= 4 and (
                                    trends['15m']['trend'] < 0.07 or  # 15분봉 하락 전환 즉시
                                    trends['240m']['trend'] < -0.07
                                ))
                            ))
                        )

                        # 2. 손실 방지 조건 (빠른 대응)
                        loss_prevention_condition = (
                            current_profit_rate < loss_threshold or  # 손실 임계값 도달 시 즉시 매도
                            (current_profit_rate < (loss_threshold * 0.3) and (  # 손실 임계값의 30% 발생 시
                                (self.thread_id < 4 and (
                                    trends['1m']['trend'] < -0.1 or  # 1분봉 하락세 강화
                                    trends['1m']['volatility'] > volatility_threshold
                                )) or
                                (self.thread_id >= 4 and (
                                    trends['15m']['trend'] < -0.1 or
                                    trends['15m']['volatility'] > volatility_threshold
                                ))
                            ))
                        )

                        # 3. 시장 상태 기반 매도 (빠른 대응)
                        market_condition_sell = (
                            current_profit_rate > profit_threshold and (
                                market_condition['AFR'] < -0.05 or  # AFR 하락 즉시
                                (current_fear_greed < 45 and market_fear_greed < 48)  # 공포 지수 급락 시
                            )
                        )

                        # 4. 정체 상태 감지 (2-3분 내 판단)
                        if self.thread_id < 4:  # 1분봉 사용 스레드
                            recent_prices = [float(candle['close']) for candle in candles_1m[-3:]]  # 3분 데이터
                            price_changes = [abs((recent_prices[i] - recent_prices[i-1])/recent_prices[i-1]*100) 
                                            for i in range(1, len(recent_prices))]
                            
                            is_stagnant = all(change <= stagnation_threshold for change in price_changes)
                            latest_change = ((recent_prices[-1] - recent_prices[-2])/recent_prices[-2]*100)
                            
                            # 수익 구간에서의 정체
                            profit_stagnation = (
                                is_stagnant and 
                                current_profit_rate >= 0.15 and (  # 0.15% 이상 수익 시
                                    latest_change < -0.02 or  # 직전 봉 대비 -0.02% 하락
                                    trends['1m']['trend'] < -0.05  # 1분봉 약한 하락
                                )
                            )
                            
                            # 손실 구간에서의 정체
                            loss_stagnation = (
                                is_stagnant and 
                                current_profit_rate < (loss_threshold * 0.2) and (  # 손실 상태에서
                                    latest_change < -0.03 or  # 직전 봉 대비 -0.03% 추가 하락
                                    trends['1m']['trend'] < -0.08  # 1분봉 하락세 강화
                                )
                            )
                            
                            stagnation_sell_condition = profit_stagnation or loss_stagnation
                            
                        else:  # 15분봉 사용 스레드
                            recent_prices = [float(candle['close']) for candle in candles_15m[-2:]]  # 30분 데이터
                            latest_change = ((recent_prices[-1] - recent_prices[-2])/recent_prices[-2]*100)
                            
                            # 수익 구간에서의 정체
                            profit_stagnation = (
                                current_profit_rate >= 0.15 and (  # 0.15% 이상 수익 시
                                    latest_change < -0.03 or  # 직전 봉 대비 -0.03% 하락
                                    trends['15m']['trend'] < -0.05  # 15분봉 약한 하락
                                )
                            )
                            
                            # 손실 구간에서의 정체
                            loss_stagnation = (
                                current_profit_rate < (loss_threshold * 0.2) and (  # 손실 상태에서
                                    latest_change < -0.05 or  # 직전 봉 대비 -0.05% 추가 하락
                                    trends['15m']['trend'] < -0.1  # 15분봉 하락세 강화
                                )
                            )
                            
                            stagnation_sell_condition = profit_stagnation or loss_stagnation
                        
                        # 5. 사용자 호출 매도 (유지)
                        user_call_sell_condition = active_trade.get('user_call', False)

                        # 장기 투자 전환 조건 개선
                        should_convert_to_long_term = (
                            # 기본 손실 조건
                            (loss_prevention_condition or stagnation_sell_condition or ma_condition) and  
                            active_trade.get('is_long_term', False) == False and
                            self.get_total_investment() < self.total_max_investment * 0.8
                        )

                        # 매도 조건 통합 (물타기 관련 조건 제거)
                        should_sell = (
                            profit_take_condition or
                            loss_prevention_condition or
                            market_condition_sell or
                            user_call_sell_condition or
                            stagnation_sell_condition or
                            ma_condition  # MA 조건
                        ) and not should_convert_to_long_term

                        # 매도 사유 저장 로직 수정
                        sell_reason = []
                        if profit_take_condition:
                            if current_profit_rate >= 1.0:
                                sell_reason.append("목표 수익(1%) 달성")
                            else:
                                sell_reason.append("수익 실현({}%+)".format(round(profit_threshold, 2)))
                        if loss_prevention_condition:
                            sell_reason.append("손실 방지")
                        if market_condition_sell:
                            sell_reason.append("시장 상태 악화")
                        if user_call_sell_condition:
                            sell_reason.append("사용자 호출")
                        if stagnation_sell_condition:
                            sell_reason.append("정체 후 하락")
                        if ma_condition:
                            sell_reason.append("MA20 대비 -15% 이하")

                        signals['sell_reason'] = ", ".join(sell_reason)
                        
                        # 디버깅 로깅
                        self.logger.debug(f"{market} - 수익률: {current_profit_rate:.2f}%, "
                                        f"should_sell: {should_sell}, "
                                        f"투자금: {current_investment:,}원, "
                                        f"시장 위험도: {market_condition['risk_level']}, "
                                        f"마켓 공포지수: {market_fear_greed}")
                        
                        if should_sell:
                            self.logger.info(f"매도 신호 감지: {market} - Profit: {current_profit_rate:.2f}%, "
                                        f"Trend: {price_trend:.2f}, Volatility: {volatility:.2f}")
                            if self.trading_manager.process_sell_signal(
                                market=market,
                                exchange=self.exchange_name,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals,
                                sell_message=signals['sell_reason']
                            ):
                                self.logger.info(f"매도 신호 처리 완료: {market}")
                        
                        elif should_convert_to_long_term:
                            # 장기 투자 전환
                            conversion_data = {
                                'market': market,
                                'thread_id': self.thread_id,
                                'original_trade': active_trade,
                                'conversion_price': current_price,
                                'conversion_reason': f"{round(active_trade['profit_rate'], 2)}% 손실로 인한 장기 투자 전환",
                                'test_mode': active_trade.get('test_mode', False)
                            }
                            
                            if self.db.save_trade_conversion(conversion_data):
                                # 기존 거래 상태 업데이트
                                self.db.trades.update_one(
                                    {'_id': active_trade['_id']},
                                    {
                                        '$set': {
                                            'status': 'converted',
                                            'is_long_term': True,
                                            'conversion_timestamp': TimeUtils.get_current_kst(),
                                            'conversion_price': current_price
                                        }
                                    }
                                )
                                
                                # 장기 투자 거래 생성
                                long_term_trade = {
                                    'market': market,
                                    'thread_id': self.thread_id,
                                    'exchange': self.exchange_name,
                                    'status': 'active',
                                    'initial_investment': active_trade['investment_amount'],
                                    'total_investment': active_trade['investment_amount'],
                                    'price': current_price,
                                    'profit_rate': current_profit_rate,
                                    'average_price': active_trade['price'],
                                    'executed_volume': active_trade.get('executed_volume', 0),
                                    'target_profit_rate': 5,  # 5% 목표 수익률
                                    'positions': [{
                                        'price': active_trade['price'],
                                        'amount': active_trade['investment_amount'],
                                        'executed_volume': active_trade.get('executed_volume', 0),
                                        'timestamp': TimeUtils.get_current_kst()
                                    }],
                                    'from_short_term': True,
                                    'original_trade_id': str(active_trade['_id']),
                                    'test_mode': active_trade.get('test_mode', False),
                                    'created_at': TimeUtils.get_current_kst()
                                }
                                
                                if self.db.save_long_term_trade(long_term_trade):
                                    # 장기 투자 전환 메시지 생성 및 전송
                                    self.trading_manager.create_long_term_message(
                                        trade_data=active_trade,
                                        conversion_price=current_price,
                                        reason=conversion_data['conversion_reason']
                                    )

                                    self.logger.info(f"{market}: 장기 투자 전환 완료 (거래 ID: {active_trade['_id']})")
                                    self.long_term_trades[market] = long_term_trade
                    
                    else:
                        # MA 대비 가격 확인
                        price_below_ma = -35 < trends['240m']['price_vs_ma'] <= -18 if trends['240m'].get('ma20') else False
                        
                        # 1. 일반 매수 신호 처리 (상승세)
                        normal_buy_condition = (
                            signals.get('overall_signal', 0.0) >= thresholds['buy_threshold'] and
                            current_investment < self.max_investment 
                        )
                        
                        # 2. 하락세 매수 조건 (MA 기반)
                        ma_buy_condition = (
                            price_below_ma and
                            current_investment < self.max_investment and
                            signals.get('overall_signal', 0.0) >= thresholds['buy_threshold'] * 0.7
                        )
                        
                        # 매수 신호 처리
                        if normal_buy_condition or ma_buy_condition:
                            # 전체 투자량의 80% 제한 체크
                            if current_investment >= (self.total_max_investment * 0.8):
                                self.logger.debug(f"{market}: 전체 투자 한도(80%) 초과 - 현재 투자: {current_investment:,}원")
                                return
                                
                            investment_amount = self.trading_strategy.calculate_position_size(
                                market, market_condition, trends
                            )
                            buy_reason = "일반 매수" if normal_buy_condition else "MA 하락세 매수"
                            
                            signals['investment_amount'] = investment_amount
                            
                            self.trading_manager.process_buy_signal(
                                market=market,
                                exchange=self.exchange_name,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals,
                                buy_message=buy_reason
                            )
                            self.logger.info(f"매수 신호 처리 완료: {market} - 투자금액: {investment_amount:,}원 ({buy_reason})")
                        
                        # 최저 신호 대비 반등 매수 전략
                        elif current_investment < (self.total_max_investment * 0.8):  # 80% 제한으로 수정
                            # 기존 최저점 조회
                            existing_lowest = self.db.strategy_data.find_one({'market': market, 'exchange': self.exchange_name})
                            
                            # 기존 최저점이 없거나 현재 신호가 기존 최저점보다 낮을 때, 현재 가격이 기존 최저가보다 낮을 때 업데이트
                            lowest_price = existing_lowest.get('lowest_price', float('inf'))
                            lowest_signal = existing_lowest.get('lowest_signal', float('inf'))
                            current_signal = signals.get('overall_signal', 0.0)
                            
                            if (not existing_lowest or 
                                lowest_signal is None or 
                                lowest_price is None or
                                current_signal < lowest_signal or
                                current_price < lowest_price):
                                # 최저 신호 정보 업데이트
                                self.db.strategy_data.update_one(
                                    {'market': market, 'exchange': self.exchange_name},
                                    {
                                        '$set': {
                                            'lowest_signal': current_signal,
                                            'lowest_price': current_price,
                                            'timestamp': TimeUtils.get_current_kst() 
                                        }
                                    },
                                    upsert=True
                                )
                                self.logger.debug(f"{market} - 새로운 최저 신호 기록: {current_signal:.4f}")
                        
                        # 최저 신호 정보 조회
                        lowest_data = self.db.strategy_data.find_one({'market': market, 'exchange': self.exchange_name})
                        
                        if lowest_data and 'lowest_signal' in lowest_data and 'lowest_price' in lowest_data:
                            signal_increase = ((signals.get('overall_signal', 0.0) - lowest_data['lowest_signal']) 
                                              / abs(lowest_data['lowest_signal'])) * 100 if lowest_data['lowest_signal'] != 0 else 0
                            price_increase = ((current_price - lowest_data['lowest_price']) 
                                             / abs(lowest_data['lowest_price'])) * 100 if lowest_data['lowest_price'] != 0 else 0
                            
                            # 반등 매수 조건 강화
                            if (signal_increase >= 20 and
                                price_increase >= 0.8 and
                                market_condition['risk_level'] < 0.6 and
                                market_condition['AFR'] > 0 and
                                (
                                    (self.thread_id < 4 and trends['1m']['trend'] > 0.2 and trends['1m']['volatility'] < 0.5) or
                                    (self.thread_id >= 4 and trends['15m']['trend'] > 0.2 and trends['15m']['volatility'] < 0.5)
                                ) and
                                # 추가: 전체 투자량 80% 제한 재확인
                                current_investment < (self.total_max_investment * 0.8)):
                                
                                buy_reason = "반등 매수"
                                investment_amount = self.trading_strategy.calculate_position_size(
                                    market, market_condition, trends
                                )
                                
                                 # 최저 신호 정보 초기화
                                self.db.strategy_data.update_one(
                                    {'market': market, 'exchange': self.exchange_name},
                                    {
                                        '$set': {
                                            'lowest_signal': 0,
                                            'lowest_price': 0,
                                            'timestamp': TimeUtils.get_current_kst() 
                                        }
                                    },
                                    upsert=True
                                )
                                self.logger.debug(f"{market} - 최저 신호 기록 초기화")
                                
                                signals['investment_amount'] = investment_amount
                                signals['rebound_buy'] = True
                                signals['signal_increase'] = signal_increase
                                
                                self.trading_manager.process_buy_signal(
                                    market=market,
                                    exchange=self.exchange_name,
                                    thread_id=self.thread_id,
                                    signal_strength=signals.get('overall_signal', 0.0),  # 반등 매수용 신호 강도
                                    price=current_price,
                                    strategy_data=signals,
                                    buy_message=buy_reason
                                )
                                self.logger.info(f"반등 매수 신호 처리 완료: {market} - 투자금액: {investment_amount:,}원")
                                
                                # 최저 신호 정보 초기화
                                self.db.strategy_data.delete_one({'market': market, 'exchange': self.exchange_name})
                        else:
                            self.logger.debug(f"매수 조건 미충족: {market} - Signal: {signals.get('overall_signal')}, Investment: {current_investment}/{self.max_investment}")

                except Exception as e:
                    self.logger.error(f"거래 신호 처리 중 오류 발생: {str(e)}", exc_info=True)

            # 스레드 상태 업데이트
            self.db.thread_status.update_one(
                {
                    'thread_id': self.thread_id,
                    'exchange': self.exchange_name
                },
                {'$set': {
                    'last_market': market,
                    'last_update': TimeUtils.get_current_kst(),  
                    'status': 'running',
                    'is_active': True
                }},
                upsert=True
            )

            self.logger.debug(f"Thread {self.thread_id}: {market} - 처리 완료")

        except Exception as e:
            import traceback
            error_location = traceback.extract_tb(sys.exc_info()[2])[-1]
            self.logger.error(f"Error processing {market}: {str(e)} at {error_location.filename}:{error_location.lineno} in {error_location.name}")

    def _analyze_market_condition(self, current_afr: float, current_change: float, 
                                current_fear_greed: float, afr_history: list,
                                change_history: list, fear_greed_history: list) -> dict:
        """시장 상태 분석
        
        Returns:
            dict: 시장 상태 정보
                - is_tradeable: 거래 가능 여부
                - market_trend: 시장 추세 (-1: 하락, 0: 중립, 1: 상승)
                - risk_level: 위험도 (0: 낮음 ~ 1: 높음)
                - message: 상태 메시지
        """
        try:
            # 기본 반환값
            result = {
                'is_tradeable': True,
                'market_trend': 0,
                'risk_level': 0.5,
                'message': "정상"
            }
            
            # 1. Fear & Greed 지수 분석
            if current_fear_greed < 20:  # 극도의 공포
                result.update({
                    'is_tradeable': False,
                    'risk_level': 0.9,
                    'message': "극도의 공포 상태"
                })
                return result
            elif current_fear_greed > 80:  # 극도의 탐욕
                result.update({
                    'is_tradeable': False,
                    'risk_level': 0.8,
                    'message': "극도의 탐욕 상태"
                })
                return result
                
            # 2. AFR 변화 분석
            if len(afr_history) >= 2:
                afr_change = ((current_afr - afr_history[-2]) / afr_history[-2]) * 100
                if afr_change < -5:  # 급격한 하락
                    result.update({
                        'is_tradeable': False,
                        'market_trend': -1,
                        'risk_level': 0.7,
                        'message': "급격한 자금 유출"
                    })
                    return result
                elif afr_change > 5:  # 급격한 상승
                    result['market_trend'] = 1
                    result['risk_level'] = 0.6
                
            # 3. 현재 변화율 분석
            if current_change < -3:  # 큰 폭의 하락
                result.update({
                    'is_tradeable': False,
                    'market_trend': -1,
                    'risk_level': 0.7,
                    'message': "큰 폭의 시장 하락"
                })
                return result
                
            # 4. 추세 분석
            if len(change_history) >= 5:
                recent_changes = change_history[-5:]
                neg_count = sum(1 for x in recent_changes if x < 0)
                if neg_count >= 4:  # 최근 5회 중 4회 이상 하락
                    result.update({
                        'market_trend': -1,
                        'risk_level': 0.6,
                        'message': "지속적 하락 추세"
                    })
                elif neg_count <= 1:  # 최근 5회 중 4회 이상 상승
                    result.update({
                        'market_trend': 1,
                        'risk_level': 0.4,
                        'message': "지속적 상승 추세"
                    })
            
            return result
            
        except Exception as e:
            self.logger.error(f"시장 상태 분석 중 오류: {str(e)}")
            return {
                'is_tradeable': False,
                'market_trend': 0,
                'risk_level': 1.0,
                'message': "분석 오류"
            }

    def _analyze_multi_timeframe_trends(self, candles_1m, candles_15m, candles_240m):
        """여러 시간대의 추세를 분석"""
        try:
            trends = {
                '1m': {'trend': 0, 'volatility': 0},
                '15m': {'trend': 0, 'volatility': 0},
                '240m': {'trend': 0, 'volatility': 0}
            }
            
            # 1분봉 분석
            if candles_1m:
                trends['1m'] = self._calculate_trend_and_volatility(candles_1m)
                self.logger.warning(f"1분봉 분석 결과: {trends['1m']}")
            
            # 15분봉 분석
            if candles_15m:
                trends['15m'] = self._calculate_trend_and_volatility(candles_15m)
                self.logger.warning(f"15분봉 분석 결과: {trends['15m']}")
            
            # 240분봉 분석
            if candles_240m:
                trends['240m'] = self._calculate_trend_and_volatility(candles_240m)
                self.logger.warning(f"4시간봉 분석 결과: {trends['240m']}")
            
            return trends
            
        except Exception as e:
            self.logger.error(f"추세 분석 중 오류: {str(e)}")
            return None
        
    def _calculate_trend_and_volatility(self, candles):
        """단일 시간대의 추세와 변동성 계산"""
        try:
            if not candles or len(candles) < 2:
                self.logger.warning(f"캔들 데이터 부족: {len(candles) if candles else 0}개")
                return {'trend': 0, 'volatility': 0, 'ma20': None, 'price_vs_ma': 0}
            
            prices = [float(candle['close']) for candle in candles]
            
            # 20일 이동평균선 계산
            if len(prices) >= 20:
                ma20 = sum(prices[-20:]) / 20
                current_price = prices[-1]
                price_vs_ma = ((current_price - ma20) / ma20) * 100
            else:
                ma20 = None
                price_vs_ma = 0
            
            # 기존 추세 및 변동성 계산 로직
            changes = []
            weights = []
            for i in range(1, min(20, len(prices))):
                change = (prices[-i] - prices[-i-1]) / prices[-i-1]
                changes.append(change)
                weights.append(1 / i)
            
            trend = sum(c * w for c, w in zip(changes, weights)) / sum(weights)
            
            recent_prices = prices[-20:]
            mean_price = sum(recent_prices) / len(recent_prices)
            variance = sum((p - mean_price) ** 2 for p in recent_prices) / len(recent_prices)
            volatility = (variance ** 0.5) / mean_price
            
            return {
                'trend': max(min(trend * 10, 1), -1),
                'volatility': min(volatility * 10, 1),
                'ma20': ma20,
                'price_vs_ma': price_vs_ma
            }
            
        except Exception as e:
            self.logger.error(f"추세/변동성 계산 중 오류: {str(e)}")
            return {'trend': 0, 'volatility': 0, 'ma20': None, 'price_vs_ma': 0}
        
    def _get_market_condition(self, exchange: str, market: str) -> dict:
        """시장 상태 조회"""
        try:
            # 최신 AFR 데이터 조회
            market_index = self.db.market_index.find_one(
                {'exchange': exchange},
                sort=[('last_updated', -1)]
            )
            
            if not market_index:
                self.logger.warning(f"{market}: market_index 데이터 없음")
                return None
            
            # 필수 필드 존재 확인
            required_fields = ['market_feargreed', 'AFR', 'current_change', 'fear_and_greed']
            if not all(field in market_index for field in required_fields):
                self.logger.warning(f"{market}: 필수 필드 누락 - {[f for f in required_fields if f not in market_index]}")
                return None
            
            # market_feargreed 리스트에서 해당 마켓 데이터 찾기
            market_fear_greed = None
            market_feargreed = market_index.get('market_feargreed', [])
            
            if isinstance(market_feargreed, list):
                for item in market_feargreed:
                    if isinstance(item, dict) and item.get('market') == market:
                        market_fear_greed = item
                        break
                
            if not market_fear_greed:
                self.logger.warning(f"{market}: fear_greed 데이터 없음")
                return None
            
            try:
                return {
                    'feargreed': float(market_fear_greed.get('feargreed', 50)),
                    'state': str(market_fear_greed.get('state', '중립')),
                    'timestamp': market_fear_greed.get('timestamp'),
                    'AFR': float(market_index['AFR'][-1]) if market_index.get('AFR') else None,
                    'current_change': float(market_index['current_change'][-1]) if market_index.get('current_change') else None,
                    'market_fear_and_greed': float(market_index['fear_and_greed'][-1]) if market_index.get('fear_and_greed') else 50
                }
            except (IndexError, ValueError, TypeError) as e:
                self.logger.warning(f"{market}: 데이터 변환 중 오류 - {str(e)}")
                return None
            
        except Exception as e:
            self.logger.error(f"시장 상태 조회 중 오류 ({market}): {str(e)}")
            return None
        
    def get_total_investment(self) -> float:
        """전체 투자금액 계산 (단기 + 장기)"""
        try:
            # 단기 투자 금액
            active_trades = self.db.trades.find({
                'status': 'active'
            })
            short_term_investment = sum(trade.get('investment_amount', 0) for trade in active_trades)
            
            # 장기 투자 금액
            long_term_trades = self.db.long_term_trades.find({
                'status': 'active'
            })
            long_term_investment = sum(trade.get('total_investment', 0) for trade in long_term_trades)
            
            return short_term_investment + long_term_investment
        except Exception as e:
            self.logger.error(f"투자금액 계산 중 오류: {str(e)}")
            return 0

    def set_market_tradeable(self, market: str, tradeable: bool, reason: str = None) -> bool:
        """
        특정 마켓 거래 가능 여부 설정
        """
        try:
            update_result = self.db.trades.update_many(
                {
                    'market': market,
                    'exchange': self.exchange_name,
                    'status': 'active'
                },
                {
                    '$set': {
                        'is_tradeable': tradeable,
                        'tradeable_update_reason': reason,
                        'tradeable_updated_at': TimeUtils.get_current_kst()
                    }
                }
            )
            
            self.logger.info(f"{market} 거래 {'가능' if tradeable else '중지'} 설정 완료: {reason}")
            return True
            
        except Exception as e:
            self.logger.error(f"마켓 거래 가능 여부 설정 중 오류: {str(e)}")
            return False

    def set_global_tradeable(self, tradeable: bool, reason: str = None) -> bool:
        """
        전체 마켓 거래 가능 여부 설정
        """
        try:
            self.db.portfolio.update_one(
                {'exchange': self.exchange_name},
                {
                    '$set': {
                        'global_tradeable': tradeable,
                        'global_tradeable_reason': reason,
                        'global_tradeable_updated_at': TimeUtils.get_current_kst()
                    }
                }
            )
            
            self.logger.info(f"전체 마켓 거래 {'가능' if tradeable else '중지'} 설정 완료: {reason}")
            return True
            
        except Exception as e:
            self.logger.error(f"전체 거래 가능 여부 설정 중 오류: {str(e)}")
            return False
        
        