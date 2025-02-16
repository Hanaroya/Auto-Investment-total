import logging
from typing import Dict, Any, Optional, List
from database.mongodb_manager import MongoDBManager
from utils.time_utils import TimeUtils
from decimal import Decimal
import os
from math import floor

class LongTermTradingManager:
    """
    장기 투자 관리자
    
    장기 투자 전략의 실행과 관리를 담당합니다.
    - 1시간 주기 추가 투자
    - 투자 상태 관리
    - 매도 시점 결정
    """
    
    def __init__(self, exchange_name: str, trading_manager=None):
        """
        Args:
            exchange_name (str): 거래소 이름
            trading_manager: 거래 관리자 인스턴스
        """
        self.exchange_name = exchange_name
        self.db = MongoDBManager(exchange_name=exchange_name)
        self.logger = logging.getLogger('investment-center')
        self.trading_manager = trading_manager
        
        # 시스템 설정 로드
        self.system_config = self.db.get_system_config(exchange_name)
        self.min_trade_amount = self.system_config.get('min_trade_amount', 5000)
        self.max_investment = self.system_config.get('total_max_investment', 1000000)
        
    async def convert_to_long_term(self, trade_id: str, current_price: float) -> bool:
        """단기 거래를 장기 투자로 전환
        
        Args:
            trade_id (str): 전환할 거래 ID
            current_price (float): 현재 가격
            
        Returns:
            bool: 전환 성공 여부
        """
        try:
            # 기존 거래 정보 조회
            trade = self.db.get_trade({'_id': trade_id})
            if not trade:
                self.logger.error(f"전환할 거래를 찾을 수 없음: {trade_id}")
                return False
                
            # 장기 투자 거래 데이터 생성
            long_term_trade = {
                'market': trade['market'],
                'exchange': self.exchange_name,
                'status': 'active',
                'initial_investment': trade['investment_amount'],
                'total_investment': trade['investment_amount'],
                'average_price': trade['price'],
                'target_profit_rate': 5,  # 5% 목표 수익률
                'positions': [{
                    'price': trade['price'],
                    'amount': trade['amount'],
                    'timestamp': TimeUtils.get_current_kst()
                }],
                'from_short_term': True,
                'original_trade_id': trade_id,
                'test_mode': trade.get('test_mode', False)
            }
            
            # 장기 투자 거래 저장
            if not self.db.save_long_term_trade(long_term_trade):
                return False
                
            # 전환 기록 저장
            conversion_record = {
                'original_trade_id': trade_id,
                'market': trade['market'],
                'exchange': self.exchange_name,
                'conversion_price': current_price,
                'investment_amount': trade['investment_amount'],
                'test_mode': trade.get('test_mode', False)
            }
            self.db.save_trade_conversion(conversion_record)
            
            # 기존 거래 상태 업데이트
            self.db.update_trade(trade_id, {
                'status': 'converted',
                'converted_to_long_term': True,
                'conversion_timestamp': TimeUtils.get_current_kst()
            })
            
            self.logger.info(f"거래 {trade_id} 장기 투자로 전환 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"장기 투자 전환 실패: {str(e)}")
            return False
            
    async def add_position(self, trade_id: str, current_price: float) -> bool:
        """기존 장기 투자에 새로운 포지션 추가
        
        Args:
            trade_id (str): 장기 투자 거래 ID
            current_price (float): 현재 가격
            
        Returns:
            bool: 포지션 추가 성공 여부
        """
        try:
            trade = self.db.get_long_term_trade(trade_id)
            if not trade:
                self.logger.error(f"장기 투자 거래를 찾을 수 없음: {trade_id}")
                return False
                
            # 추가 투자금 계산 (이전 투자금의 2배)
            new_investment = trade['initial_investment'] * 2
            
            # 새로운 포지션 추가
            new_position = {
                'price': current_price,
                'amount': new_investment / current_price,
                'timestamp': TimeUtils.get_current_kst()
            }
            
            # 평균 매수가 재계산
            total_amount = sum(pos['amount'] for pos in trade['positions']) + new_position['amount']
            total_value = sum(pos['amount'] * pos['price'] for pos in trade['positions'])
            new_average = (total_value + (new_position['amount'] * new_position['price'])) / total_amount
            
            # 거래 정보 업데이트
            trade['positions'].append(new_position)
            trade['total_investment'] += new_investment
            trade['average_price'] = new_average
            trade['last_updated'] = TimeUtils.get_current_kst()
            
            return self.db.save_long_term_trade(trade)
            
        except Exception as e:
            self.logger.error(f"포지션 추가 실패: {str(e)}")
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
            
    def check_additional_investment(self, trade: Dict) -> bool:
        """추가 투자 가능 여부 확인
        
        Args:
            trade: 장기 투자 거래 정보
            
        Returns:
            bool: 추가 투자 가능 여부
        """
        try:
            # 마지막 투자로부터 1시간 경과 확인
            last_investment = trade.get('last_investment_time')
            if last_investment:
                time_diff = TimeUtils.get_current_kst() - TimeUtils.from_mongo_date(last_investment)
                if time_diff.total_seconds() < 3600:  # 1시간 = 3600초
                    return False
                    
            # 투자 한도 확인
            current_investment = trade.get('total_investment', 0)
            next_investment = self.min_trade_amount * 2  # 최소 투자금액의 2배
            
            if current_investment + next_investment > self.max_investment:
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"추가 투자 가능 여부 확인 중 오류: {str(e)}")
            return False
            
    def process_additional_investment(self, trade: Dict) -> bool:
        """추가 투자 처리
        
        Args:
            trade: 장기 투자 거래 정보
            
        Returns:
            bool: 추가 투자 성공 여부
        """
        try:
            if not self.check_additional_investment(trade):
                return False
                
            # 추가 투자 금액 계산
            additional_amount = self.min_trade_amount * 2
            
            # 투자 실행 (trading_manager 사용)
            if self.trading_manager:
                investment_result = self.trading_manager.process_buy_signal(
                    market=trade['market'],
                    exchange=self.exchange_name,
                    thread_id=trade['thread_id'],
                    signal_strength=1.0,
                    price=trade.get('current_price', 0),
                    strategy_data={
                        'investment_amount': additional_amount,
                        'is_long_term': True,
                        'existing_trade_id': trade['_id']
                    },
                    buy_message="장기 투자 추가"
                )
                
                if investment_result:
                    # 투자 정보 업데이트
                    self.db.long_term_trades.update_one(
                        {'_id': trade['_id']},
                        {
                            '$set': {
                                'last_investment_time': TimeUtils.get_current_kst(),
                                'total_investment': trade['total_investment'] + additional_amount
                            }
                        }
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.logger.error(f"추가 투자 처리 중 오류: {str(e)}")
            return False

    def register_scheduler_tasks(self, scheduler):
        """스케줄러 작업 등록
        
        Args:
            scheduler: SimpleScheduler 인스턴스
        """
        try:
            # 1시간마다 장기 투자 체크 (매시 정각)
            scheduler.schedule_task(
                'long_term_investment_check',
                self.check_long_term_investments,
                hour=-1,  # 매시간 실행
                minute=0  # 정각에 실행
            )
            
            self.logger.info("장기 투자 스케줄러 작업 등록 완료")
            
        except Exception as e:
            self.logger.error(f"스케줄러 작업 등록 실패: {str(e)}")

    async def check_long_term_investments(self):
        """장기 투자 상태 체크 및 추가 투자 처리"""
        try:
            active_trades = self.get_active_trades()
            self.logger.info(f"활성 장기 투자 거래 수: {len(active_trades)}")
            
            for trade in active_trades:
                # 추가 투자 가능 여부 확인
                if self.check_additional_investment(trade):
                    # 추가 투자 실행
                    if await self.process_additional_investment(trade):
                        self.logger.info(f"장기 투자 추가 완료: {trade['market']}")
                    else:
                        self.logger.warning(f"장기 투자 추가 실패: {trade['market']}")
                    
        except Exception as e:
            self.logger.error(f"장기 투자 체크 중 오류: {str(e)}") 