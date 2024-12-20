from datetime import datetime
from typing import List, Dict
import logging
from database.mongodb_manager import MongoDBManager
from strategy.StrategyBase import StrategyBase, StrategyManager

class MarketAnalyzer:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.strategy_manager = StrategyManager()

    async def get_sorted_markets(self) -> List[Dict]:
        try:
            markets = await self.get_krw_markets()
            sorted_markets = sorted(
                markets, 
                key=lambda x: float(x.get('accTradeVolume', 0)), 
                reverse=True
            )
            
            # 시장 데이터 저장
            for market in sorted_markets:
                await self.db.update_market_data(
                    market['market'],
                    {
                        'acc_trade_volume': market['accTradeVolume'],
                        'timestamp': datetime.utcnow()
                    }
                )
            
            return sorted_markets
        except Exception as e:
            self.logger.error(f"Error in get_sorted_markets: {e}")
            return []

    async def get_candle_data(self, market: str, interval: str = "240"):
        """4시간 봉 데이터 조회"""
        try:
            candle_data = await self.get_candle(market, interval)
            return self.convert_candle_data(candle_data)
        except Exception as e:
            self.logger.error(f"Error getting candle data for {market}: {e}")
            return None

    def convert_candle_data(self, raw_data: List[Dict]) -> List[Dict]:
        """캔들 데이터 변환"""
        converted_data = []
        for candle in raw_data:
            converted_data.append({
                'timestamp': candle['timestamp'],
                'open': float(candle['opening_price']),
                'high': float(candle['high_price']),
                'low': float(candle['low_price']),
                'close': float(candle['trade_price']),
                'volume': float(candle['candle_acc_trade_volume'])
            })
        return converted_data 

    async def analyze_trading_signals(self, candle_data: List[Dict]) -> Dict:
        """
        캔들 데이터를 분석하여 매매 신호를 생성
        여러 전략의 결과를 종합하여 최종 신호 생성
        """
        try:
            if not candle_data or len(candle_data) < 20:
                return {
                    'action': 'hold',
                    'strength': 0,
                    'price': 0,
                    'strategy_data': {}
                }

            current_price = candle_data[0]['close']
            
            # 시장 데이터 준비
            market_data = {
                'candles': candle_data,
                'current_price': current_price
            }
            
            # StrategyManager를 통한 최종 결정
            decision = self.strategy_manager.get_decision(market_data)
            
            # 결정을 action과 strength로 변환
            action_map = {
                'buy': {'action': 'buy', 'strength': 0.8},
                'sell': {'action': 'sell', 'strength': 0.8},
                'hold': {'action': 'hold', 'strength': 0}
            }
            
            result = action_map[decision]
            
            return {
                'action': result['action'],
                'strength': result['strength'],
                'price': current_price,
                'strategy_data': {
                    'decision': decision,
                    'strategies_used': len(self.strategy_manager.strategies)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in analyze_trading_signals: {e}")
            return {
                'action': 'hold',
                'strength': 0,
                'price': 0,
                'strategy_data': {}
            }

    def add_strategy(self, strategy: StrategyBase):
        """전략 추가"""
        self.strategy_manager.add_strategy(strategy)