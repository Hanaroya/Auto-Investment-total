import threading
import logging
import time
from typing import List, Dict
import os
from datetime import datetime, timezone, timedelta
from math import floor
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from database.mongodb_manager import MongoDBManager

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
                            'last_updated': datetime.now(timezone(timedelta(hours=9)))
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
            self.logger.info(f"Thread {self.thread_id}: 마켓 분석 시작 - {len(self.coins)} 개의 코인")
            
            while not self.stop_flag.is_set():
                cycle_start_time = time.time()
                
                # 스레드 ID에 따라 다른 대기 시간 설정
                if self.thread_id < 4:  # 0,1,2,3 번 스레드
                    wait_time = 40  # 40초마다
                    initial_delay = self.thread_id * 1  # 1초 간격으로 시작 시간 분배
                else:  # 4번 이상 스레드
                    wait_time = 300  # 300초(5분)마다
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
            
            # 시장 지표 데이터 조회 - 거래소별 데이터 사용
            market_index = self.db.get_market_index(self.investment_center.exchange_name)
            if not market_index:
                self.logger.warning(f"시장 지표 데이터를 찾을 수 없음")
                return
                
            # 시장 지표 분석
            afr_list = market_index.get('AFR', [])
            change_list = market_index.get('current_change', [])
            fear_greed_list = market_index.get('fear_and_greed', [])
            
            if not all([afr_list, change_list, fear_greed_list]):
                self.logger.warning(f"일부 시장 지표 데이터 누락")
                return
                
            # 최신 지표값 가져오기
            current_afr = afr_list[-1] if afr_list else 0
            current_change = change_list[-1] if change_list else 0
            current_fear_greed = fear_greed_list[-1] if fear_greed_list else 50
            
            # 시장 상태 분석
            market_condition = self._analyze_market_condition(
                current_afr, current_change, current_fear_greed,
                afr_list, change_list, fear_greed_list
            )
            
            # 캔들 데이터 조회 - 거래소 API 사용
            with self.shared_locks['candle_data']:
                interval = '1' if self.thread_id < 4 else '240'
                candles = self.investment_center.exchange.get_candle(
                    market=coin,
                    interval=interval,
                    count=300
                )

            # 현재 투자 상태 확인
            active_trades = self.db.trades.find({
                'thread_id': self.thread_id, 
                'status': 'active'
            })
            current_investment = sum(trade.get('investment_amount', 0) for trade in active_trades)

            # 최대 투자금 체크 및 시장 상태 확인
            if current_investment >= self.total_max_investment or not market_condition['is_tradeable']:
                self.logger.info(f"Thread {self.thread_id}: {coin} - 거래 제한 "
                               f"(투자금 초과: {current_investment >= self.total_max_investment}, "
                               f"시장상태: {market_condition['message']})")
                return

            # 마켓 분석 수행 시 시장 상태 정보 추가
            signals = self.market_analyzer.analyze_market(coin, candles)
            signals.update(market_condition)
            
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
                        
                        # 매도 조건 감지
                        # 1. 급격한 하락 감지
                        radical_sell_condition = (price_trend < -0.7 and volatility > 0.8)
                        # 2. 지속적인 하락 추세
                        price_down_condition = (price_trend < -0.3 and current_profit_rate < -2)
                        # 3. 목표 수익 달성 후 하락 추세
                        profit_goal_sell_condition = (current_profit_rate > 3 and price_trend < -0.2)
                        # 4. 과도한 손실 방지
                        loss_limit_sell_condition = (current_profit_rate < -3)
                        # 5. 변동성 급증 시 이익 실현
                        volatility_sell_condition = (current_profit_rate > 2 and volatility > 0.9)
                        # 6. 평균 매수 가격보다 10% 이상 상승한 경우
                        price_increase_sell_condition = (current_profit_rate > 10 and current_price > active_trade.get('price', 0) * 1.1)
                        # 7. sell_threshold 이하이고 수익이 있는 경우
                        sell_threshold_sell_condition = (signals.get('overall_signal', 0.0) <= self.config['strategy']['sell_threshold'] and current_profit_rate > 0.15)
                        # 8. 사용자 호출
                        user_call_sell_condition = (active_trade.get('user_call', False))
                        
                        # 물타기 조건 확인
                        should_average_down = (
                            (current_profit_rate <= -2 and (
                                signals.get('overall_signal', 0.0) <= (self.config['strategy']['sell_threshold'] * 0.2))
                             ) and  # 수익률이 -2% 이하
                            current_investment < self.total_max_investment * 0.8 and  # 최대 투자금의 80% 미만 사용
                            averaging_down_count < 3  # 최대 3회까지만 물타기
                        )
                        
                        # 매도 조건 확인
                        should_sell = (
                            radical_sell_condition or
                            price_down_condition or
                            profit_goal_sell_condition or
                            loss_limit_sell_condition or
                            volatility_sell_condition or
                            price_increase_sell_condition or
                            sell_threshold_sell_condition or
                            user_call_sell_condition
                        )

                        # 매도 조건 충족 시 추가 검사
                        if should_sell:
                            # 매도 사유 저장
                            sell_reason = ""
                            if radical_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "급격한 하락"
                            if price_down_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "지속적 하락"
                            if profit_goal_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "목표 수익 달성 후 하락"
                            if loss_limit_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "과도한 손실"
                            if volatility_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "변동성 급증"
                            if price_increase_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "고수익 달성"
                            if sell_threshold_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "매도 신호"
                            if user_call_sell_condition:
                                if sell_reason != "":
                                    sell_reason += ", "
                                sell_reason += "사용자 호출"

                            signals['sell_reason'] = sell_reason

                        # 디버깅 로깅
                        self.logger.debug(f"{coin} - 수익률: {current_profit_rate:.2f}%, "
                                          f"should_sell: {should_sell}, "
                                         f"투자금: {current_investment:,}원, "
                                         f"물타기 횟수: {averaging_down_count}")
                        
                        if should_sell and should_average_down == False:
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
                            
                            if signals.get('overall_signal', 0.0) >= self.config['strategy']['sell_threshold'] and averaging_down_amount >= 5000:  # 최소 주문금액 5000원 이상
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
                        # 일반 매수 신호 처리 (기존 로직)
                        if (signals.get('overall_signal', 0.0) >= self.config['strategy']['buy_threshold'] and current_investment < self.max_investment):
                            self.logger.info(f"매수 신호 감지: {coin} - Signal strength: {signals.get('overall_signal')}")
                            investment_amount = min(floor((self.investment_each)), self.max_investment - current_investment)
                            buy_reason = "일반 매수"
                            # strategy_data에 investment_amount 추가
                            signals['investment_amount'] = investment_amount
                            
                            self.trading_manager.process_buy_signal(
                                coin=coin,
                                thread_id=self.thread_id,
                                signal_strength=signals.get('overall_signal', 0.0),
                                price=current_price,
                                strategy_data=signals,
                                buy_message=buy_reason
                            )
                            self.logger.info(f"매수 신호 처리 완료: {coin} - 투자금액: {investment_amount:,}원")
                        
                        # 최저 신호 대비 반등 매수 전략
                        elif current_investment < self.max_investment:
                            current_signal = signals.get('overall_signal', 0.0)
                            
                            # 현재 신호가 매도 기준치의 30% 이하일 때 최저점 검사
                            if current_signal < self.config['strategy']['sell_threshold'] * 0.3:
                                # 기존 최저점 조회
                                existing_lowest = self.db.strategy_data.find_one({'coin': coin})
                                
                                # 기존 최저점이 없거나 현재 신호가 기존 최저점보다 낮을 때, 현재 가격이 기존 최저가보다 낮을 때 업데이트
                                if (not existing_lowest
                                    ) or (current_signal < existing_lowest.get('lowest_signal', float('inf'))
                                    ) or (current_price < existing_lowest.get('lowest_price', float('inf'))):
                                    # 최저 신호 정보 업데이트
                                    self.db.strategy_data.update_one(
                                        {'coin': coin},
                                        {
                                            '$set': {
                                                'lowest_signal': current_signal,
                                                'lowest_price': current_price,
                                                'timestamp': datetime.now(timezone(timedelta(hours=9)))
                                            }
                                        },
                                        upsert=True
                                    )
                                    self.logger.debug(f"{coin} - 새로운 최저 신호 기록: {current_signal:.4f}")
                            
                            # 최저 신호 정보 조회
                            lowest_data = self.db.strategy_data.find_one({'coin': coin})
                            
                            if lowest_data and 'lowest_signal' in lowest_data:
                                signal_increase = ((current_signal - lowest_data['lowest_signal']) / abs(lowest_data['lowest_signal'])) * 100 if lowest_data['lowest_signal'] != 0 else 0
                                price_increase = ((current_price - lowest_data['lowest_price']) / abs(lowest_data['lowest_price'])) * 100 if lowest_data['lowest_price'] != 0 else 0
                                buy_reason = "반등 매수"
                                # 최저 신호 대비 15% 이상 개선된 경우
                                if signal_increase >= 15 and price_increase >= 0.5:
                                    self.logger.info(f"반등 매수 신호 감지: {coin} - 신호 개선률: {signal_increase:.2f}%")
                                    investment_amount = min(floor((self.investment_each)), self.max_investment - current_investment)
                                    
                                    signals['investment_amount'] = investment_amount
                                    signals['rebound_buy'] = True
                                    signals['signal_increase'] = signal_increase
                                    
                                    self.trading_manager.process_buy_signal(
                                        coin=coin,
                                        thread_id=self.thread_id,
                                        signal_strength=current_signal,  # 반등 매수용 신호 강도
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
                    'last_update': datetime.now(timezone(timedelta(hours=9))),
                    'status': 'running',
                    'is_active': True
                }},
                upsert=True
            )

            self.logger.debug(f"Thread {self.thread_id}: {coin} - 처리 완료")

        except Exception as e:
            self.logger.error(f"Error processing {coin}: {str(e)}")

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