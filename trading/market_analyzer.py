from asyncio.log import logger
from datetime import datetime
from typing import List, Dict
import logging
from database.mongodb_manager import MongoDBManager
from strategy.Strategies import (
    RSIStrategy,
    MACDStrategy,
    BollingerBandStrategy,
    VolumeStrategy,
    PriceChangeStrategy,
    MovingAverageStrategy,
    MomentumStrategy,
    StochasticStrategy,
    IchimokuStrategy,
    MarketSentimentStrategy,
    DowntrendEndStrategy,
    UptrendEndStrategy
)
import pandas as pd

class MarketAnalyzer:
    def __init__(self):
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.strategies = {
            'RSI': RSIStrategy(),
            'MACD': MACDStrategy(),
            'Bollinger': BollingerBandStrategy(),
            'Volume': VolumeStrategy(),
            'PriceChange': PriceChangeStrategy(),
            'Moving_Averages': MovingAverageStrategy(),
            'Momentum': MomentumStrategy(),
            'Stochastic': StochasticStrategy(),
            'Ichimoku': IchimokuStrategy(),
            'Market_Sentiment': MarketSentimentStrategy(),
            'Downtrend_End': DowntrendEndStrategy(),
            'Uptrend_End': UptrendEndStrategy()
        }

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

    def _process_strategy_result(self, result):
        """전략 결과 처리"""
        if isinstance(result, (int, float)):
            return {'signal': 'hold', 'strength': float(result)}
        return result

    async def analyze_market(self, market: str, candles: List[Dict]) -> Dict:
        """시장 분석 수행"""
        try:
            if not candles:
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            # 캔들 데이터를 DataFrame으로 변환
            df = pd.DataFrame(candles)
            
            if df.empty:
                self.logger.warning(f"{market}: 캔들 데이터 없음")
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            strategy_results = {}
            market_data = {
                'df': df,
                'current_price': df['trade_price'].iloc[-1],
                'volume': df['candle_acc_trade_volume'].iloc[-1]
            }
            
            # 각 전략 실행
            for name, strategy in self.strategies.items():
                try:
                    result = strategy.analyze(market_data)
                    # float 값을 반환하는 전략 결과를 딕셔너리로 변환
                    if isinstance(result, (int, float)):
                        strategy_results[name] = {'signal': 'hold', 'strength': float(result)}
                    else:
                        strategy_results[name] = result
                except Exception as e:
                    self.logger.error(f"{market} - {name} 전략 분석 실패: {str(e)}")
                    strategy_results[name] = {'signal': 'hold', 'strength': 0}

            # 전략 결과 로깅
            self.logger.info(f"\n[{market}] 전략 분석 결과:")
            for strategy, result in strategy_results.items():
                self.logger.info(f"{strategy}: {result}")

            # 종합 강도 계산 (모든 결과가 딕셔너리임을 보장)
            total_strength = sum(r['strength'] for r in strategy_results.values()) / len(strategy_results)
            self.logger.info(f"종합 강도: {round(total_strength, 2)}")

            return {
                'action': 'buy' if total_strength >= 0.65 else 'hold',
                'strength': round(total_strength, 2),
                'price': float(market_data['current_price']),
                'strategy_data': strategy_results
            }

        except Exception as e:
            self.logger.error(f"시장 분석 중 오류 발생 ({market}): {str(e)}", exc_info=True)
            return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}