from datetime import datetime
from typing import List, Dict
import logging
from database.mongodb_manager import MongoDBManager

class MarketAnalyzer:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)

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