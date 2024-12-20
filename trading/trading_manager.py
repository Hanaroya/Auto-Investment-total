from typing import Dict, List
import logging
from database.mongodb_manager import MongoDBManager
from messenger import Messenger
from datetime import datetime
import pandas as pd

class TradingManager:
    def __init__(self):
        self.db = MongoDBManager()
        self.messenger = Messenger()
        self.logger = logging.getLogger(__name__)

    async def process_buy_signal(self, coin: str, thread_id: int, signal_strength: float, 
                               price: float, strategy_data: Dict):
        """매수 신호 처리"""
        try:
            # 투자 가능 금액 확인
            if not await self.check_investment_limit(thread_id):
                return False

            trade_data = {
                'coin': coin,
                'type': 'buy',
                'timestamp': datetime.utcnow(),
                'price': price,
                'signal_strength': signal_strength,
                'thread_id': thread_id,
                'strategy_data': strategy_data,
                'status': 'active'
            }

            trade_id = await self.db.insert_trade(trade_data)
            
            # 메신저로 매수 알림
            message = self.create_buy_message(trade_data)
            await self.messenger.send_message(message)
            
            return True

        except Exception as e:
            self.logger.error(f"Error in process_buy_signal: {e}")
            return False

    async def process_sell_signal(self, coin: str, thread_id: int, signal_strength: float,
                                price: float, strategy_data: Dict):
        """매도 신호 처리"""
        try:
            # 활성 거래 조회
            active_trade = await self.db.get_collection('trades').find_one({
                'coin': coin,
                'thread_id': thread_id,
                'status': 'active'
            })

            if not active_trade:
                return False

            # 거래 종료 처리
            await self.db.update_trade(active_trade['_id'], {
                'status': 'closed',
                'sell_price': price,
                'sell_timestamp': datetime.utcnow(),
                'sell_signal_strength': signal_strength
            })

            # 메신저로 매도 알림
            message = self.create_sell_message(active_trade, price, signal_strength)
            await self.messenger.send_message(message)

            return True

        except Exception as e:
            self.logger.error(f"Error in process_sell_signal: {e}")
            return False

    async def generate_daily_report(self):
        """일일 리포트 생성"""
        try:
            trades = await self.db.get_collection('trades').find({}).to_list(None)
            
            # DataFrame 생성
            df = pd.DataFrame(trades)
            
            # 엑셀 파일 생성
            filename = f"투자현황-{datetime.now().strftime('%Y%m%d')}.xlsx"
            df.to_excel(filename)
            
            # 이메일 전송
            await self.messenger.send_email(
                subject="일일 투자 현황 리포트",
                body="일일 투자 현황 리포트가 첨부되어 있습니다.",
                attachment=filename
            )
            
            # 메신저 알림
            await self.messenger.send_message(f"{filename} 파일이 전달되었습니다.")

        except Exception as e:
            self.logger.error(f"Error generating daily report: {e}") 

    def create_buy_message(self, trade_data: Dict) -> str:
        """매수 메시지 생성"""
        strategy_data = trade_data['strategy_data']
        
        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 구매\n"
            f" 구매 시간: {trade_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 구매 가격: {trade_data['price']:,}\n"
            f" 구매 신호: {trade_data['signal_strength']:.2f}\n"
            f" Coin-rank: {strategy_data.get('coin_rank', 'N/A')}\n"
            f" 투자 금액: W{trade_data.get('investment_amount', 0):,}\n"
        )

        # 전략별 결과 추가
        if 'rsi' in strategy_data:
            message += f" RSI: [{strategy_data['rsi']:.2f} - 결과: {strategy_data['rsi_signal']:.1f}]\n"
        
        if 'stochastic_k' in strategy_data and 'stochastic_d' in strategy_data:
            message += (f" Stochastic RSI: [K: {strategy_data['stochastic_k']:.0f}, "
                       f"D: {strategy_data['stochastic_d']:.0f} - "
                       f"결과: {strategy_data.get('stochastic_signal', 0):.1f}]\n")

        # 기타 전략 결과들 추가
        for key, value in strategy_data.items():
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'coin_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        message += "\n------------------------------------------------"
        return message

    def create_sell_message(self, trade_data: Dict, sell_price: float, 
                           sell_signal: float) -> str:
        """매도 메시지 생성"""
        strategy_data = trade_data['strategy_data']
        total_investment = trade_data.get('total_investment', 0)
        
        message = (
            f"------------------------------------------------\n"
            f"Coin: {trade_data['coin']}, 판매\n"
            f" 판매 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" 판매 가격: {sell_price:,}\n"
            f" 판매 신호: {sell_signal:.2f}\n"
            f" Coin-rank: {strategy_data.get('coin_rank', 'N/A')}\n"
            f" 총 투자 금액: W{total_investment:,}\n"
        )

        # 전략별 결과 추가 (판매 시점의 지표들)
        current_strategy_data = trade_data.get('current_strategy_data', {})
        
        if 'rsi' in current_strategy_data:
            message += f" RSI: [{current_strategy_data['rsi']:.2f} - 결과: {current_strategy_data['rsi_signal']:.1f}]\n"
        
        if 'stochastic_k' in current_strategy_data and 'stochastic_d' in current_strategy_data:
            message += (f" Stochastic RSI: [K: {current_strategy_data['stochastic_k']:.0f}, "
                       f"D: {current_strategy_data['stochastic_d']:.0f} - "
                       f"결과: {current_strategy_data.get('stochastic_signal', 0):.1f}]\n")

        # 기타 전략 결과들 추가
        for key, value in current_strategy_data.items():
            if key not in ['rsi', 'stochastic_k', 'stochastic_d', 'coin_rank'] and '_signal' in key:
                strategy_name = key.replace('_signal', '').upper()
                message += f" {strategy_name}: [{value:.1f}]\n"

        # 수익률 정보 추가
        profit_rate = ((sell_price - trade_data['price']) / trade_data['price']) * 100
        message += f" 수익률: {profit_rate:.2f}%\n"

        message += "\n------------------------------------------------"
        return message