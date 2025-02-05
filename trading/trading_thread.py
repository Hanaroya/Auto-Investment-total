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
    개별 코인 그룹을 처리하는 거래 스레드
    각 스레드는 할당된 코인들에 대해 독립적으로 거래 분석 및 실행을 담당합니다.
    """
    def __init__(self, thread_id: int, coins: List[str], db: MongoDBManager, config: Dict, shared_locks: Dict, stop_flag: threading.Event, investment_center=None):
        """
        Args:
            thread_id (int): 스레드 식별자
            coins (List[str]): 처리할 코인 목록
            db (MongoDBManager): 데이터베이스 인스턴스
            config: 설정 정보가 담긴 딕셔너리
            shared_locks (Dict): 공유 락 딕셔너리
            stop_flag (threading.Event): 전역 중지 플래그
            investment_center: InvestmentCenter 인스턴스
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
        
        # system_config에서 설정값 가져오기
        system_config = self.db.system_config.find_one({'_id': 'config'})
        if not system_config:
            self.logger.error("system_config를 찾을 수 없습니다. 기본값 사용")
            self.max_investment = float(os.getenv('MAX_THREAD_INVESTMENT', 80000))
            self.total_max_investment = float(os.getenv('TOTAL_MAX_INVESTMENT', 800000))
            self.investment_each = self.total_max_investment / 20
        else:
            self.max_investment = system_config.get('max_thread_investment', 80000)
            self.total_max_investment = system_config.get('total_max_investment', 1000000)
            self.investment_each = (self.total_max_investment * 0.8) / 20
        
        # 동적 임계값 조정을 위한 전략 초기화
        self.trading_strategy = TradingStrategy(config, self.total_max_investment)
        
        self.db.portfolio.update_one(
                    {'_id': 'main'},
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
        self.update_investment_limits()

        self.investment_center = investment_center  # InvestmentCenter 인스턴스 저장

    def update_investment_limits(self):
        """system_config에서 투자 한도를 업데이트"""
        try:
            system_config = self.db.system_config.find_one({'_id': 'config'})
            if system_config:
                total_max_investment = system_config.get('total_max_investment', 1000000)
                # total_max_investment를 initial_investment의 80%로 설정
                self.total_max_investment = floor(total_max_investment * 0.8)
                # 스레드당 최대 투자금은 total_max_investment의 10%로 설정
                self.max_investment = floor(self.total_max_investment * 0.1)
                # 코인당 투자금은 total_max_investment를 20으로 나눈 값
                self.investment_each = floor(self.total_max_investment / 20)
                
                self.logger.info(f"Thread {self.thread_id} 투자 한도 업데이트: "
                               f"최대 투자금: {self.max_investment:,}원, "
                               f"코인당 투자금: {self.investment_each:,}원")
                
                # 현재 활성화된 거래들의 총 투자금액 계산
                active_trades = self.db.trades.find({"status": "active"})
                total_invested = sum(trade.get('investment_amount', 0) for trade in active_trades)
                
                # 기존 포트폴리오 정보 가져오기
                existing_portfolio = self.db.portfolio.find_one({'_id': 'main'})
                if existing_portfolio:
                    # 기존 profit_earned 값 보존
                    profit_earned = existing_portfolio.get('profit_earned', 0)
                    
                    # portfolio 컬렉션 업데이트 (기존 값 유지하면서 필요한 부분만 업데이트)
                    self.db.portfolio.update_one(
                        {'_id': 'main'},
                        {'$set': {
                            'current_amount': floor(self.total_max_investment - total_invested),
                            'last_updated': TimeUtils.get_current_kst()  
                            }
                         }
                    )
                else:
                    # 포트폴리오가 없는 경우에만 전체 초기화
                    self.db.portfolio.update_one(
                        {'_id': 'main'},
                        {'$set': {
                            'investment_amount': floor(total_max_investment),
                            'available_investment': self.total_max_investment,
                            'current_amount': floor(self.total_max_investment - total_invested),
                            'reserve_amount': floor(total_max_investment * 0.2),
                            'profit_earned': 0
                            }
                         },
                        upsert=True
                    )
                
        except Exception as e:
            self.logger.error(f"투자 한도 업데이트 중 오류: {str(e)}")

    def run(self):
        """스레드 실행"""
        try:
            # 비동기 이벤트 루프 생성
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.logger.info(f"Thread {self.thread_id}: 마켓 분석 시작 - {len(self.coins)} 개의 코인")
            
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
                
                for coin in self.coins:
                    if self.stop_flag.is_set():
                        break
                        
                    try:
                        # 비동기 함수를 동기적으로 실행
                        self.loop.run_until_complete(self.process_single_coin(coin))
                    except Exception as e:
                        import traceback
                        tb = traceback.extract_tb(sys.exc_info()[2])[-1]
                        error_statement = tb.line  # 실제 에러가 발생한 코드 라인의 내용
                        self.logger.error(f"Error processing {coin}: {str(e)} in statement: '{error_statement}' at {tb.filename}:{tb.lineno}")
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

    async def process_single_coin(self, coin: str):
        """단일 코인 처리"""
        try:
            # 시장 상태 조회 
            market_condition = self._get_market_condition(exchange=self.investment_center.exchange_name, coin= coin)
            if not market_condition:
                self.logger.debug(f"{coin}: 시장 상태 데이터 없음")
                return
            
            # AFR 데이터 유효성 검사
            if any(market_condition.get(key) is None for key in ['AFR', 'current_change', 'market_fear_and_greed']):
                self.logger.debug(f"{coin}: AFR 데이터 누락")
                return

            current_fear_greed = None
            coin_fear_greed = None
            
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
                    self.logger.debug(f"{coin}: 시장 분석 실패")
                    return
                    
                market_condition.update(analyzed_market)
                current_fear_greed = market_condition['market_fear_and_greed']
                coin_fear_greed = market_condition['feargreed']
            except Exception as e:
                self.logger.error(f"{coin}: 시장 분석 중 오류 - {str(e)}")
                return

            # 여러 시간대의 캔들 데이터 조회
            with self.shared_locks['candle_data']:
                candles_1m = None
                candles_15m = None
                candles_240m = None
                
                try:
                    if self.thread_id < 4:  # 0~3번 스레드
                        candles_1m = self.investment_center.exchange.get_candle(
                            market=coin, interval='1', count=300)
                        if not candles_1m:
                            self.logger.warning(f"{coin}: 1분봉 데이터 없음")
                            return
                        
                        candles_15m = self.investment_center.exchange.get_candle(
                            market=coin, interval='15', count=300)
                        if not candles_15m:
                            self.logger.warning(f"{coin}: 15분봉 데이터 없음")
                            return
                        
                        candles_240m = self.investment_center.exchange.get_candle(
                            market=coin, interval='240', count=300)
                        if not candles_240m:
                            self.logger.warning(f"{coin}: 4시간봉 데이터 없음")
                            return  
                        
                    else:  # 4~9번 스레드
                        candles_15m = self.investment_center.exchange.get_candle(
                            market=coin, interval='15', count=300)
                        if not candles_15m:
                            self.logger.warning(f"{coin}: 15분봉 데이터 없음")
                            return
                        
                        candles_240m = self.investment_center.exchange.get_candle(
                            market=coin, interval='240', count=300)
                        if not candles_240m:
                            self.logger.warning(f"{coin}: 4시간봉 데이터 없음")
                            return
                        
                    # 캔들 데이터 길이 검증
                    if self.thread_id < 4:
                        if len(candles_1m) < 2:
                            self.logger.warning(f"{coin}: 1분봉 데이터 부족 (개수: {len(candles_1m)})")
                            return
                        if len(candles_15m) < 2:
                            self.logger.warning(f"{coin}: 15분봉 데이터 부족 (개수: {len(candles_15m)})")
                            return
                    else:
                        if len(candles_15m) < 2:
                            self.logger.warning(f"{coin}: 15분봉 데이터 부족 (개수: {len(candles_15m)})")
                            return
                        if len(candles_240m) < 2:
                            self.logger.warning(f"{coin}: 4시간봉 데이터 부족 (개수: {len(candles_240m)})")
                            return
                        
                    # 마지막 캔들 데이터 검증
                    if self.thread_id < 4:
                        if not candles_1m[-1] or not candles_15m[-1]:
                            self.logger.warning(f"{coin}: 최근 캔들 데이터 누락")
                            return
                    else:
                        if not candles_15m[-1] or not candles_240m[-1]:
                            self.logger.warning(f"{coin}: 최근 캔들 데이터 누락")
                            return
                    
                except Exception as e:
                    self.logger.error(f"{coin}: 캔들 데이터 조회 실패 - {str(e)}")
                    return

            # 시간대별 추세 분석 전 데이터 검증
            if self.thread_id < 4 and (not isinstance(candles_1m, list) or not isinstance(candles_15m, list)):
                self.logger.error(f"{coin}: 잘못된 캔들 데이터 형식")
                return
            elif self.thread_id >= 4 and (not isinstance(candles_15m, list) or not isinstance(candles_240m, list)):
                self.logger.error(f"{coin}: 잘못된 캔들 데이터 형식")
                return

            # 시간대별 추세 분석
            try:
                trends = self._analyze_multi_timeframe_trends(candles_1m, candles_15m, candles_240m)
                if not trends:
                    self.logger.warning(f"{coin}: 추세 분석 실패")
                    return
            except Exception as e:
                self.logger.error(f"{coin}: 추세 분석 중 오류 - {str(e)}")
                return
            
            # 동적 임계값 조정
            thresholds = self.trading_strategy.adjust_thresholds(market_condition, trends)
            
            # 현재 투자 상태 확인
            active_trades = self.db.trades.find({
                'thread_id': self.thread_id, 
                'status': 'active'
            })
            current_investment = sum(trade.get('investment_amount', 0) for trade in active_trades)

            # 최대 투자금 체크 및 시장 상태 확인
            if current_investment >= self.total_max_investment:
                self.logger.info(f"Thread {self.thread_id}: {coin} - 거래 제한 "
                               f"(투자금 초과: {current_investment >= self.total_max_investment})")
                return

            # 마켓 분석 수행 시 시장 상태 정보 추가
            signals = self.market_analyzer.analyze_market(coin, candles_1m)
            signals.update(market_condition)
            current_price = candles_1m[-1]['close'] if self.thread_id < 4 else candles_15m[-1]['close']
            self.logger.warning(f"{coin}: 현재 가격 - {current_price}")
            self.logger.warning(f"{coin}: 현재 가격 - {current_price}")
            self.trading_manager.update_strategy_data(market=coin, exchange=self.investment_center.exchange_name, thread_id=self.thread_id, price=current_price, strategy_results=signals)

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
                        price_trend = signals.get('price_trend', 0)
                        volatility = signals.get('volatility', 0)
                        
                        # MA 대비 가격 확인
                        ma_condition = trends['240m']['price_vs_ma'] <= -15 if trends['240m'].get('ma20') else False
                        
                        # 시장 상황에 따른 동적 임계값 조정
                        market_risk = market_condition['risk_level']
                        
                        # 코인별 공포탐욕지수에 따른 수익 실현 임계값 조정
                        base_profit = 0.15  # 기본 수익률 0.15%
                        
                        if coin_fear_greed >= 65:  # 극도의 탐욕
                            profit_threshold = base_profit + 0.3  # 0.45%
                        elif 65 > coin_fear_greed >= 55:  # 탐욕
                            profit_threshold = base_profit + 0.2  # 0.35%
                        elif 55 > coin_fear_greed >= 45:  # 약한 공포
                            profit_threshold = base_profit + 0.1  # 0.25%
                        else:  # 중립 또는 공포
                            profit_threshold = base_profit  # 0.15%
                        
                        # 시장 상황별 손실 및 변동성 임계값 설정
                        if market_risk > 0.7:  # 고위험 시장
                            loss_threshold = -2.0
                            volatility_threshold = 0.25
                            stagnation_threshold = 0.05
                        elif market_risk > 0.5:  # 중위험 시장
                            loss_threshold = -2.0
                            volatility_threshold = 0.3
                            stagnation_threshold = 0.08
                        else:  # 저위험 시장
                            loss_threshold = -4.0
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
                                market_condition['AFR'] < -0.2 or  # AFR 하락 즉시
                                (current_fear_greed < 25 and coin_fear_greed < 30)  # 공포 지수 급락 시
                            )
                        )

                        # 4. 정체 상태 감지 (2-3분 내 판단)
                        if self.thread_id < 4:  # 1분봉 사용 스레드
                            recent_prices = [float(candle['close']) for candle in candles_1m[-3:]]  # 3분 데이터
                            price_changes = [abs((recent_prices[i] - recent_prices[i-1])/recent_prices[i-1]*100) 
                                            for i in range(1, len(recent_prices))]
                            
                            is_stagnant = all(change <= stagnation_threshold for change in price_changes)
                            
                            if is_stagnant and current_profit_rate >= 0.15:  # 0.15% 이상 수익 시
                                latest_change = ((recent_prices[-1] - recent_prices[-2])/recent_prices[-2]*100)
                                stagnation_sell_condition = (
                                    latest_change < -0.02 or  # 직전 봉 대비 -0.02% 하락
                                    trends['1m']['trend'] < -0.05  # 1분봉 약한 하락
                                )
                            else:
                                stagnation_sell_condition = False
                        else:  # 15분봉 사용 스레드
                            recent_prices = [float(candle['close']) for candle in candles_15m[-2:]]  # 30분 데이터
                            latest_change = ((recent_prices[-1] - recent_prices[-2])/recent_prices[-2]*100)
                            
                            stagnation_sell_condition = (
                                current_profit_rate >= 0.15 and (  # 0.15% 이상 수익 시
                                    latest_change < -0.03 or  # 직전 봉 대비 -0.03% 하락
                                    trends['15m']['trend'] < -0.05  # 15분봉 약한 하락
                                )
                            )

                        # 5. 사용자 호출 매도 (유지)
                        user_call_sell_condition = active_trade.get('user_call', False)

                        # 매도 조건 통합
                        should_sell = (
                            profit_take_condition or
                            loss_prevention_condition or
                            market_condition_sell or
                            user_call_sell_condition or
                            stagnation_sell_condition or
                            ma_condition  # MA 조건 추가
                        )

                        # 매도 사유 저장 로직 수정
                        sell_reason = []
                        if profit_take_condition:
                            if current_profit_rate >= 1.0:
                                sell_reason.append("목표 수익(1%) 달성")
                            else:
                                sell_reason.append("수익 실현({}%+)".format(profit_threshold))
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

                        averaging_down_count = active_trade.get('averaging_down_count', 0)
                        # 물타기 조건 확인
                        should_average_down = (
                            # 기본 조건
                            current_profit_rate <= -2 and  # 수익률이 -2% 이하
                            current_investment < self.total_max_investment * 0.8 and  # 최대 투자금의 80% 미만 사용
                            averaging_down_count < 3 and  # 최대 3회까지만 물타기
                            
                            # 시장 상태 확인
                            market_condition['risk_level'] < 0.7 and  # 시장 위험도가 높지 않을 때
                            coin_fear_greed > 20 and  # 극도의 공포 상태가 아닐 때
                            
                            # 시간대별 추세 확인
                            (
                                # 0~3번 스레드: 1분봉과 15분봉 기준
                                (self.thread_id < 4 and (
                                    trends['1m']['trend'] > -0.5 and  # 1분봉 급락이 아님
                                    trends['1m']['volatility'] < 0.8 and  # 변동성 안정화
                                    trends['15m']['trend'] > -0.3  # 15분봉 하락세 완화
                                )) or
                                # 4~9번 스레드: 15분봉과 240분봉 기준
                                (self.thread_id >= 4 and (
                                    trends['15m']['trend'] > -0.5 and  # 15분봉 급락이 아님
                                    trends['15m']['volatility'] < 0.8 and  # 변동성 안정화
                                    trends['240m']['trend'] > -0.3  # 240분봉 하락세 완화
                                ))
                            ) and
                            
                            # 신호 강도 확인
                            (
                                signals.get('overall_signal', 0.0) >= (thresholds['sell_threshold'] * 0.3)  # 매도 임계값의 30% 이상
                            )
                        )

                        # 디버깅 로깅
                        self.logger.debug(f"{coin} - 수익률: {current_profit_rate:.2f}%, "
                                        f"should_sell: {should_sell}, "
                                        f"should_average_down: {should_average_down}, "
                                        f"투자금: {current_investment:,}원, "
                                        f"물타기 횟수: {averaging_down_count}, "
                                        f"시장 위험도: {market_condition['risk_level']}, "
                                        f"코인 공포지수: {coin_fear_greed}")
                        
                        if should_sell and not should_average_down:
                            self.logger.info(f"매도 신호 감지: {coin} - Profit: {current_profit_rate:.2f}%, "
                                        f"Trend: {price_trend:.2f}, Volatility: {volatility:.2f}")
                            if self.trading_manager.process_sell_signal(
                                coin=coin,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals,
                                sell_message=signals['sell_reason']
                            ):
                                self.logger.info(f"매도 신호 처리 완료: {coin}")
                        
                        if should_average_down:
                            # 물타기 투자금 계산 (기존 투자금의 50%)
                            averaging_down_amount = min(
                                floor(current_investment * 0.5),
                                self.total_max_investment - current_investment
                            )
                            
                            if signals.get('overall_signal', 0.0) >= thresholds['sell_threshold'] and averaging_down_amount >= 5000:  # 최소 주문금액 5000원 이상
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
                                    strategy_data=signals,
                                    buy_message="물타기"
                                )
                                self.logger.info(f"물타기 주문 처리 완료: {coin} - 추가 투자금액: {averaging_down_amount:,}원")
                    
                    else:
                        # MA 대비 가격 확인
                        price_below_ma = -15 < trends['240m']['price_vs_ma'] <= -8 if trends['240m'].get('ma20') else False
                        
                         # 매수 임계값 동적 조정
                        market_fg = market_condition['market_fear_and_greed']
                        base_threshold = thresholds['buy_threshold']
                        
                        # 전체 시장 상태에 따른 임계값 조정
                        if market_fg <= 45:  # 공포 상태
                            threshold_multiplier = 1.05  # 기준 5% 상향
                        elif market_fg >= 55:  # 탐욕 상태
                            threshold_multiplier = 0.95  # 기준 5% 하향
                        else:  # 중립 상태
                            threshold_multiplier = 1.0  # 기준 유지
                        
                        # 코인별 공포탐욕지수에 따른 추가 조정
                        if coin_fear_greed <= 30:  # 극도의 공포
                            threshold_multiplier += 0.1  # 추가 10% 상향
                        elif coin_fear_greed <= 45:  # 공포
                            threshold_multiplier += 0.05  # 추가 5% 상향
                        elif coin_fear_greed >= 61:  # 극도의 탐욕
                            threshold_multiplier -= 0.15  # 10% 하향
                        
                        adjusted_threshold = base_threshold * threshold_multiplier
                        
                        # 1. 일반 매수 신호 처리 (상승세)
                        normal_buy_condition = (
                            signals.get('overall_signal', 0.0) >= adjusted_threshold and
                            current_investment < self.max_investment 
                        )
                        
                        # 2. 하락세 매수 조건 (MA 기반)
                        ma_buy_condition = (
                            price_below_ma and
                            current_investment < self.max_investment and
                            market_condition['risk_level'] < 0.7 
                        )
                        
                        # 매수 신호 처리
                        if normal_buy_condition or ma_buy_condition:
                            # 전체 투자량의 80% 제한 체크
                            if current_investment >= (self.total_max_investment * 0.8):
                                self.logger.debug(f"{coin}: 전체 투자 한도(80%) 초과 - 현재 투자: {current_investment:,}원")
                                return
                                
                            investment_amount = self.trading_strategy.calculate_position_size(
                                coin, market_condition, trends
                            )
                            buy_reason = "일반 매수" if normal_buy_condition else "MA 하락세 매수"
                            
                            signals['investment_amount'] = investment_amount
                            
                            self.trading_manager.process_buy_signal(
                                coin=coin,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals,
                                buy_message=buy_reason
                            )
                            self.logger.info(f"매수 신호 처리 완료: {coin} - 투자금액: {investment_amount:,}원 ({buy_reason})")
                        
                        # 최저 신호 대비 반등 매수 전략
                        elif current_investment < (self.total_max_investment * 0.8):  # 80% 제한으로 수정
                            # 기존 최저점 조회
                            existing_lowest = self.db.strategy_data.find_one({'coin': coin})
                            
                            # 기존 최저점이 없거나 현재 신호가 기존 최저점보다 낮을 때, 현재 가격이 기존 최저가보다 낮을 때 업데이트
                            if (not existing_lowest
                                ) or (signals.get('overall_signal', 0.0) < existing_lowest.get('lowest_signal', float('inf'))
                                ) or (current_price < existing_lowest.get('lowest_price', float('inf'))):
                                # 최저 신호 정보 업데이트
                                self.db.strategy_data.update_one(
                                    {'market': coin, 'exchange': self.exchange_name},
                                    {
                                        '$set': {
                                            'lowest_signal': signals.get('overall_signal', 0.0),
                                            'lowest_price': current_price,
                                            'timestamp': TimeUtils.get_current_kst() 
                                        }
                                    },
                                    upsert=True
                                )
                                self.logger.debug(f"{coin} - 새로운 최저 신호 기록: {signals.get('overall_signal', 0.0):.4f}")
                        
                        # 최저 신호 정보 조회
                        lowest_data = self.db.strategy_data.find_one({'coin': coin})
                        
                        if lowest_data and 'lowest_signal' in lowest_data:
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
                                    coin, market_condition, trends
                                )
                                
                                signals['investment_amount'] = investment_amount
                                signals['rebound_buy'] = True
                                signals['signal_increase'] = signal_increase
                                
                                self.trading_manager.process_buy_signal(
                                    coin=coin,
                                    thread_id=self.thread_id,
                                    signal_strength=signals.get('overall_signal', 0.0),  # 반등 매수용 신호 강도
                                    price=current_price,
                                    strategy_data=signals,
                                    buy_message=buy_reason
                                )
                                self.logger.info(f"반등 매수 신호 처리 완료: {coin} - 투자금액: {investment_amount:,}원")
                                
                                # 최저 신호 정보 초기화
                                self.db.strategy_data.delete_one({'coin': coin})
                        else:
                            self.logger.debug(f"매수 조건 미충족: {coin} - Signal: {signals.get('overall_signal')}, Investment: {current_investment}/{self.max_investment}")

                except Exception as e:
                    self.logger.error(f"거래 신호 처리 중 오류 발생: {str(e)}", exc_info=True)

            # 스레드 상태 업데이트
            self.db.thread_status.update_one(
                {'thread_id': self.thread_id},
                {'$set': {
                    'last_coin': coin,
                    'last_update': TimeUtils.get_current_kst(),  
                    'status': 'running',
                    'is_active': True
                }},
                upsert=True
            )

            self.logger.debug(f"Thread {self.thread_id}: {coin} - 처리 완료")

        except Exception as e:
            import traceback
            error_location = traceback.extract_tb(sys.exc_info()[2])[-1]
            self.logger.error(f"Error processing {coin}: {str(e)} at {error_location.filename}:{error_location.lineno} in {error_location.name}")

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
        
    def _get_market_condition(self, exchange: str, coin: str) -> dict:
        """시장 상태 조회"""
        try:
            # 최신 AFR 데이터 조회
            market_index = self.db.market_index.find_one(
                {'exchange': exchange},
                sort=[('last_updated', -1)]
            )
            
            if not market_index:
                self.logger.warning(f"{coin}: market_index 데이터 없음")
                return None
            
            # 필수 필드 존재 확인
            required_fields = ['market_feargreed', 'AFR', 'current_change', 'fear_and_greed']
            if not all(field in market_index for field in required_fields):
                self.logger.warning(f"{coin}: 필수 필드 누락 - {[f for f in required_fields if f not in market_index]}")
                return None
            
            # market_feargreed 리스트에서 해당 코인 데이터 찾기
            coin_fear_greed = None
            market_feargreed = market_index.get('market_feargreed', [])
            
            if isinstance(market_feargreed, list):
                for item in market_feargreed:
                    if isinstance(item, dict) and item.get('market') == coin:
                        coin_fear_greed = item
                        break
                
            if not coin_fear_greed:
                self.logger.warning(f"{coin}: fear_greed 데이터 없음")
                return None
            
            try:
                return {
                    'feargreed': float(coin_fear_greed.get('feargreed', 50)),
                    'state': str(coin_fear_greed.get('state', '중립')),
                    'timestamp': coin_fear_greed.get('timestamp'),
                    'AFR': float(market_index['AFR'][-1]) if market_index.get('AFR') else None,
                    'current_change': float(market_index['current_change'][-1]) if market_index.get('current_change') else None,
                    'market_fear_and_greed': float(market_index['fear_and_greed'][-1]) if market_index.get('fear_and_greed') else 50
                }
            except (IndexError, ValueError, TypeError) as e:
                self.logger.warning(f"{coin}: 데이터 변환 중 오류 - {str(e)}")
                return None
            
        except Exception as e:
            self.logger.error(f"시장 상태 조회 중 오류 ({coin}): {str(e)}")
            return None
        
        