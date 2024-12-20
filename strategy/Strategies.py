from typing import Dict, Any
import numpy as np
from .StrategyBase import StrategyBase

class RSIStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        rsi = market_data.get('rsi', 50)
        if rsi < 30:  # 과매도
            return 0.9
        elif rsi > 70:  # 과매수
            return 0.1
        return 0.5

class MACDStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        macd = market_data.get('macd', 0)
        if macd > 0:
            return 0.7
        return 0.3

class BollingerBandStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        price = market_data.get('current_price', 0)
        lower = market_data.get('bollinger_lower', price * 0.95)
        upper = market_data.get('bollinger_upper', price * 1.05)
        
        if price < lower:
            return 0.8
        elif price > upper:
            return 0.2
        return 0.5

class VolumeStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        avg_volume = market_data.get('average_volume', 1000)
        current_volume = market_data.get('current_volume', 1000)
        
        if current_volume > avg_volume * 1.5:
            return 0.7
        return 0.4

class PriceChangeStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        change_rate = market_data.get('price_change_rate', 0)
        if change_rate < -5:
            return 0.8  # 큰 하락 시 매수 신호
        elif change_rate > 5:
            return 0.2  # 큰 상승 시 매도 신호
        return 0.5

class MovingAverageStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        ma5 = market_data.get('ma5', 0)
        ma20 = market_data.get('ma20', 0)
        
        if ma5 > ma20:
            return 0.7
        return 0.3

class MomentumStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        momentum = market_data.get('momentum', 0)
        if momentum > 0:
            return 0.6
        return 0.4

class StochasticStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        k = market_data.get('stochastic_k', 50)
        d = market_data.get('stochastic_d', 50)
        
        if k < 20 and d < 20:
            return 0.8
        elif k > 80 and d > 80:
            return 0.2
        return 0.5

class IchimokuStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        price = market_data.get('current_price', 0)
        cloud_top = market_data.get('ichimoku_cloud_top', price)
        cloud_bottom = market_data.get('ichimoku_cloud_bottom', price)
        
        if price > cloud_top:
            return 0.7
        elif price < cloud_bottom:
            return 0.3
        return 0.5

class MarketSentimentStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        sentiment = market_data.get('market_sentiment', 0)  # -1 to 1
        return (sentiment + 1) / 2  # normalize to 0-1 

__all__ = [
    'RSIStrategy',
    'MACDStrategy',
    'BollingerBandStrategy',
    'VolumeStrategy',
    'PriceChangeStrategy',
    'MovingAverageStrategy',
    'MomentumStrategy',
    'StochasticStrategy',
    'IchimokuStrategy',
    'MarketSentimentStrategy'
]