import logging
from typing import Dict, Any, Optional, List
from utils.time_utils import TimeUtils
from decimal import Decimal
import os
from math import floor
from datetime import datetime, timedelta, timezone
from control_center.exchange_factory import ExchangeFactory
from monitoring.memory_monitor import MemoryProfiler, memory_profiler
class LongTermTradingManager:
    """
    장기 투자 관리자
    
    장기 투자 전략의 실행과 관리를 담당합니다.
    - 1시간 주기 추가 투자
    - 투자 상태 관리
    - 매도 시점 결정
    """
    
    def __init__(self, db, exchange_name: str, config: dict):
        self.db = db
        self.exchange_name = exchange_name
        self.config = config
        self.logger = logging.getLogger(f"LongTermTradingManager")
        self.exchange = self._initialize_exchange(self.exchange_name)
        self.memory_profiler = MemoryProfiler()
    
    
    def _initialize_exchange(self, exchange_name: str) -> Any:
        """거래소 초기화"""
        try:
            exchange = ExchangeFactory.create_exchange(exchange_name, self.config)
            self.logger.info(f"{exchange_name} 거래소 초기화 성공")
            return exchange
        except Exception as e:
            self.logger.error(f"거래소 초기화 실패: {str(e)}")
            raise

    
    def convert_to_long_term(self, trade: dict, market_condition: dict, trends: dict) -> bool:
        """단기 거래를 장기 거래로 전환"""
        try:
            # 전환 조건 검증
            if not self._validate_conversion_conditions(trade, market_condition, trends):
                return False
                
            # 기존 거래 정보로 장기 거래 생성
            long_term_trade = {
                'market': trade['market'],
                'exchange': self.exchange_name,
                'thread_id': trade['thread_id'],
                'initial_investment': trade['investment_amount'],
                'total_investment': trade['investment_amount'],
                'average_price': trade['price'],
                'positions': [{
                    'price': trade['price'],
                    'amount': trade['investment_amount'],
                    'quantity': trade['quantity'],
                    'timestamp': trade['created_at']
                }],
                'status': 'active',
                'created_at': TimeUtils.get_current_kst(),
                'last_updated': TimeUtils.get_current_kst(),
                'is_long_term': True,
                'original_trade_id': trade['_id']
            }
            
            # 장기 거래 저장
            self.db.long_term_trades.insert_one(long_term_trade)
            
            # 기존 거래 상태 업데이트
            self.db.trades.update_one(
                {'_id': trade['_id']},
                {'$set': {
                    'status': 'converted',
                    'converted_to_long_term': True,
                    'converted_at': TimeUtils.get_current_kst()
                }}
            )
            
            self.logger.info(f"거래 {trade['_id']} 장기 투자로 전환 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"장기 투자 전환 중 오류: {str(e)}")
            return False

    
    def _validate_conversion_conditions(self, trade: dict, market_condition: dict, trends: dict) -> bool:
        """장기 투자 전환 조건 검증"""
        try:
            # 기본 손실 조건
            current_profit_rate = ((trade['current_price'] - trade['price']) 
                                / trade['price']) * 100
                                
            should_convert = (
                # 기본 손실 조건
                current_profit_rate <= -3 and  
                not trade.get('is_long_term', False) and
                
                # 시장 상황 기반 추가 조건
                (
                    # 1. 시장이 전반적으로 안정적인 경우
                    market_condition['risk_level'] < 0.7 and
                    market_condition.get('AFR', 0) > -0.03 and
                    
                    # 2. 장기적 상승 가능성이 있는 경우
                    (
                        trends['240m']['trend'] > -0.3 or  # 4시간봉 급락이 아닌 경우
                        trends['240m']['price_vs_ma'] < -25  # MA 대비 큰 하락
                    ) and
                    
                    # 3. 연속 손실 거래 방지
                    self._check_consecutive_losses(trade['market']) < 2 and
                    
                    # 4. 거래량 확인
                    self._check_volume_stability(trade['market'])
                )
            )
            
            return should_convert
            
        except Exception as e:
            self.logger.error(f"전환 조건 검증 중 오류: {str(e)}")
            return False

    
    def _check_consecutive_losses(self, market: str) -> int:
        """연속 손실 거래 횟수 확인"""
        try:
            recent_trades = self.db.trades.find({
                'market': market,
                'exchange': self.exchange_name,
                'status': 'closed',
                'closed_at': {'$gte': TimeUtils.get_past_kst(hours=24)}
            }).sort('closed_at', -1).limit(3)

            consecutive_losses = 0
            for trade in recent_trades:
                if trade.get('profit_rate', 0) < 0:
                    consecutive_losses += 1
                else:
                    break
            return consecutive_losses

        except Exception as e:
            self.logger.error(f"연속 손실 거래 확인 중 오류: {str(e)}")
            return 0

    
    def _check_volume_stability(self, market: str) -> bool:
        """거래량 안정성 확인"""
        try:
            # 4시간 캔들 데이터 조회 (최소 100개 필요)
            candles = self.exchange.get_candle(
                market=market, 
                interval='240', 
                count=200
            )
            
            if not candles or len(candles) < 100:
                return False
            
            # 최근 15개 캔들만 사용
            recent_candles = candles[-15:]
            volumes = [float(candle['volume']) for candle in recent_candles]
            avg_volume = sum(volumes) / len(volumes)
            volume_changes = [abs((v - avg_volume) / avg_volume) for v in volumes]
            
            is_stable = all(change < 0.5 for change in volume_changes)
            has_sufficient_volume = volumes[-1] > (avg_volume * 0.2)
            
            return is_stable and has_sufficient_volume

        except Exception as e:
            self.logger.error(f"거래량 안정성 확인 중 오류: {str(e)}")
            return False

    
    def process_additional_investment(self, trade: dict, current_price: float = None, 
                                    market_condition: dict = None, trends: dict = None) -> bool:
        """추가 투자 처리
        Args:
            trade (dict): 거래 정보
            current_price (float, optional): 현재 가격. None일 경우 실시간 조회
            market_condition (dict, optional): 시장 상황 데이터. None일 경우 실시간 분석
            trends (dict, optional): 시장 트렌드 데이터. None일 경우 실시간 분석
        Returns:
            bool: 투자 가능 여부 (True: 투자 조건 적합, False: 부적합)
        """
        try:
            # 1. 기본 정보 계산
            if current_price is None:
                current_price = self.exchange.get_current_price(trade['market'])
            
            if market_condition is None:
                market_condition = self.market_analyzer.get_market_condition()
            
            if trends is None:
                trends = self.market_analyzer.get_market_trends(trade['market'])
            
            current_profit_rate = ((current_price - trade['average_price']) 
                                 / trade['average_price']) * 100
            total_investment = trade.get('total_investment', 0)
            initial_investment = trade.get('initial_investment', 0)
            max_additional_ratio = 3
            
            # 2. 추가 매수 가능 여부 확인
            can_add_position = (
                total_investment < (initial_investment * max_additional_ratio) and
                current_profit_rate < -5
            )
            
            if not can_add_position:
                return False
                
            # 3. 시장 상황 평가
            market_score = self._evaluate_market_for_addition(
                market_condition=market_condition,
                trends=trends,
                current_profit_rate=current_profit_rate
            )
            
            # 4. 투자 가능 여부 반환
            return market_score >= 0.7
            
        except Exception as e:
            self.logger.error(f"추가 투자 조건 확인 중 오류: {str(e)}")
            return False

    
    def _evaluate_market_for_addition(self, market_condition: dict,
                                    trends: dict,
                                    current_profit_rate: float) -> float:
        """추가 매수를 위한 시장 상황 평가"""
        try:
            scores = []
            
            # 1. 기술적 지표 평가 (40%)
            technical_score = 0
            if trends['240m']['trend'] > -0.2:
                technical_score += 0.2
            if trends['240m']['price_vs_ma'] < -20:
                technical_score += 0.2
                
            scores.append(technical_score * 0.4)
            
            # 2. 시장 심리 평가 (30%)
            sentiment_score = 0
            if market_condition.get('market_fear_and_greed', 50) > 40:
                sentiment_score += 0.15
            if market_condition.get('AFR', 0) > -0.03:
                sentiment_score += 0.15
                
            scores.append(sentiment_score * 0.3)
            
            # 3. 손실 규모 평가 (30%)
            loss_score = 0
            if -15 <= current_profit_rate <= -5:
                loss_score += 0.3
            elif -20 <= current_profit_rate < -15:
                loss_score += 0.15
                
            scores.append(loss_score * 0.3)
            
            return sum(scores)
            
        except Exception as e:
            self.logger.error(f"시장 평가 중 오류: {str(e)}")
            return 0

    
    def _calculate_additional_amount(self, trade: dict,
                                   market_score: float,
                                   current_price: float) -> float:
        """추가 매수 금액 계산"""
        try:
            initial_investment = trade.get('initial_investment', 0)
            total_investment = trade.get('total_investment', 0)
            
            base_amount = initial_investment * 0.5
            adjusted_amount = base_amount * market_score
            
            if adjusted_amount < 5000:
                return 0
                
            remaining_limit = (initial_investment * 3) - total_investment
            adjusted_amount = min(adjusted_amount, remaining_limit)
            
            return floor(adjusted_amount)
            
        except Exception as e:
            self.logger.error(f"추가 매수 금액 계산 중 오류: {str(e)}")
            return 0

    
    def check_sell_conditions(self, trade: dict, current_price: float,
                            market_condition: dict, trends: dict) -> bool:
        """매도 조건 확인"""
        try:
            current_profit_rate = ((current_price - trade['average_price']) 
                                / trade['average_price']) * 100
                                
            # 1. 동적 목표 수익률 계산
            base_profit_target = 5.0
            adjusted_profit_target = self._calculate_dynamic_profit_target(
                base_target=base_profit_target,
                market_condition=market_condition,
                trends=trends,
                investment_duration=self.calculate_investment_duration(trade)
            )
            
            # 2. 수익률 목표 달성 확인
            profit_target_reached = (
                current_profit_rate >= adjusted_profit_target
            )
            
            # 3. 시장 상황 기반 매도 조건
            market_condition_sell = (
                current_profit_rate > adjusted_profit_target * 0.7 and (
                    market_condition['AFR'] < -0.05 or
                    (market_condition.get('market_fear_and_greed', 50) < 40 and 
                     market_condition.get('feargreed', 50) < 45) or
                    (trends['240m']['trend'] < -0.5 and trends['240m']['volatility'] > 0.8) or
                    (market_condition['risk_level'] > 0.8) or
                    (current_profit_rate > adjusted_profit_target * 0.8 and 
                     market_condition['market_trend'] == -1)
                )
            )
            
            return profit_target_reached or market_condition_sell
            
        except Exception as e:
            self.logger.error(f"매도 조건 확인 중 오류: {str(e)}")
            return False

    
    def _calculate_dynamic_profit_target(self, base_target: float,
                                       market_condition: dict,
                                       trends: dict,
                                       investment_duration: timedelta) -> float:
        """동적 목표 수익률 계산"""
        try:
            adjusted_target = base_target
            
            # 1. 투자 기간에 따른 조정
            if investment_duration.total_seconds() > 72 * 3600:
                adjusted_target *= 0.9
            elif investment_duration.total_seconds() > 168 * 3600:
                adjusted_target *= 0.8
                
            # 2. 시장 위험도에 따른 조정
            risk_level = market_condition.get('risk_level', 0.5)
            if risk_level > 0.7:
                adjusted_target *= 0.9
                
            # 3. 시장 추세에 따른 조정
            if trends['240m']['trend'] < -0.3:
                adjusted_target *= 0.95
                
            # 4. 공포탐욕지수에 따른 조정
            market_fear_greed = market_condition.get('market_fear_and_greed', 50)
            if market_fear_greed < 30:
                adjusted_target *= 0.9
                
            return max(adjusted_target, 2.0)
            
        except Exception as e:
            self.logger.error(f"동적 목표 수익률 계산 중 오류: {str(e)}")
            return base_target

    
    def _confirm_profit_stability(self, market: str,
                                current_profit_rate: float,
                                trends: dict) -> bool:
        """수익률 안정성 확인"""
        try:
            # 1. 단기 추세 확인
            short_term_trend = trends['15m']['trend']
            short_term_volatility = trends['15m']['volatility']
            
            # 2. 변동성 체크
            if short_term_volatility > 0.5:
                return False
                
            # 3. 하락 추세 체크
            if short_term_trend < -0.2:
                return False
                
            # 4. 거래량 안정성 확인
            if not self._check_volume_stability(market):
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"수익률 안정성 확인 중 오류: {str(e)}")
            return False

    
    def calculate_investment_duration(self, trade: dict) -> timedelta:
        """투자 지속 시간 계산"""
        try:
            if 'created_at' not in trade:
                return timedelta(0)
                
            created_at = trade['created_at']
            if isinstance(created_at, str):
                created_at = TimeUtils.from_string(created_at)
            
            # naive datetime을 aware datetime으로 변환
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
                
            current_time = TimeUtils.get_current_kst()
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
                
            return current_time - created_at
            
        except Exception as e:
            self.logger.error(f"투자 지속 시간 계산 중 오류: {str(e)}")
            return timedelta(0)

    
    def add_position(self, trade: dict, current_price: float, amount: float):
        """기존 장기 투자에 새로운 포지션 추가
        
        Args:
            trade_id (str): 장기 투자 거래 ID
            current_price (float): 현재 가격
            
        Returns:
            bool: 포지션 추가 성공 여부
        """
        try:
            # 매수 수량 계산
            fee_rate = 0.0005  # 수수료율 0.05%
            available_amount = amount * (1 - fee_rate)  # 수수료를 제외한 실제 매수 가능 금액
            executed_volume = available_amount / current_price  # 매수 수량
            
            # 새로운 포지션 정보
            new_position = {
                'price': current_price,
                'amount': amount,
                'executed_volume': executed_volume,
                'timestamp': TimeUtils.get_current_kst()
            }
            
            # 기존 거래 정보 업데이트
            self.db.long_term_trades.update_one(
                {'_id': trade['_id']},
                {
                    '$push': {'positions': new_position},
                    '$inc': {
                        'total_investment': amount,
                        'executed_volume': executed_volume  # 전체 executed_volume 증가
                    },
                    '$set': {
                        'last_investment_time': TimeUtils.get_current_kst(),
                        'average_price': (trade.get('average_price', 0) * trade.get('executed_volume', 0) + current_price * executed_volume) / (trade.get('executed_volume', 0) + executed_volume)
                    }
                }
            )
            
            self.logger.info(f"{trade['market']} 새로운 포지션 추가 완료 (가격: {current_price:,}원, 수량: {executed_volume:.8f})")
            return True
            
        except Exception as e:
            self.logger.error(f"포지션 추가 중 오류: {str(e)}")
            return False

    
    def calculate_current_profit_rate(self, trade: Dict[str, Any], current_price: float) -> float:
        """현재 수익률 계산
        
        Args:
            trade (Dict[str, Any]): 장기 투자 거래 정보
            current_price (float): 현재 가격
            
        Returns:
            float: 수익률 (소수점 형태, 예: 0.05 = 5%)
        """
        try:
            total_amount = sum(pos['amount'] for pos in trade['positions'])
            current_value = total_amount * current_price
            profit_rate = (current_value - trade['total_investment']) / trade['total_investment']
            return float(Decimal(str(profit_rate)).quantize(Decimal('0.0001')))
        except Exception as e:
            self.logger.error(f"수익률 계산 실패: {str(e)}")
            return 0.0

     
    def get_active_trades(self) -> List[Dict]:
        """활성 상태인 장기 투자 목록 조회"""
        try:
            return self.db.get_active_long_term_trades(self.exchange_name)
        except Exception as e:
            self.logger.error(f"장기 투자 목록 조회 실패: {str(e)}")
            return []

     
    def calculate_dynamic_target_profit(self, duration: timedelta) -> float:
        """투자 기간에 따른 동적 목표 수익률 계산"""
        try:
            # timedelta를 시간으로 변환 (total_seconds()를 사용하여 초를 시간으로 변환)
            hours = duration.total_seconds() / 3600
            
            # 기본 목표 수익률 (5%)
            base_target = 5.0
            
            # 24시간마다 0.5%씩 증가 (최대 20%까지)
            additional_target = min((hours / 24) * 0.5, 15.0)
            
            target_profit = base_target + additional_target
            
            self.logger.debug(f"투자 시간: {hours:.1f}시간, 목표 수익률: {target_profit:.1f}%")
            return target_profit
            
        except Exception as e:
            self.logger.error(f"동적 목표 수익률 계산 중 오류: {str(e)}")
            return 5.0  # 오류 발생시 기본값 반환