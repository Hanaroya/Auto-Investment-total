from typing import Dict, Any
import numpy as np
from .StrategyBase import StrategyBase
import logging

class RSIStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        rsi = market_data.get('rsi', 50)
        
        # 과매도 상태에서 하락세 종료 감지
        if rsi < 30:
            recent_rsi = market_data.get('rsi_history', [])[-5:]
            if recent_rsi and min(recent_rsi) < rsi:  # RSI가 반등 시작
                return 0.9  # 하락세 종료 신호
        
        # 상승세 감지
        elif 30 <= rsi <= 70:
            recent_rsi = market_data.get('rsi_history', [])[-5:]
            if recent_rsi and all(x < y for x, y in zip(recent_rsi[:-1], recent_rsi[1:])):
                return 0.8  # 상승세 감지
                
        # 과매수 상태
        elif rsi > 70:
            return 0.1
            
        return 0.5

class MACDStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        macd = market_data.get('macd', 0)
        signal = market_data.get('signal', 0)
        
        # 하락세 종료 감지 (MACD가 시그널선을 상향 돌파)
        if macd > signal and macd < 0:
            recent_macd = market_data.get('macd_history', [])[-5:]
            if recent_macd and min(recent_macd) < macd:
                return 0.9  # 하락세 종료 신호
        
        # 상승세 감지
        elif macd > 0 and macd > signal:
            recent_macd = market_data.get('macd_history', [])[-5:]
            if recent_macd and all(x < y for x, y in zip(recent_macd[:-1], recent_macd[1:])):
                return 0.8  # 상승세 감지
                
        return 0.3

class BollingerBandStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        price = market_data.get('current_price', 0)
        lower = market_data.get('lower_band', price * 0.95)
        upper = market_data.get('upper_band', price * 1.05)
        middle = market_data.get('middle_band', price)
        
        # 하락세 종료 감지 (하단 밴드 접촉 후 반등)
        if price <= lower:
            recent_prices = market_data.get('price_history', [])[-5:]
            if recent_prices and min(recent_prices) > price:
                return 0.9  # 하락세 종료 신호
        
        # 상승세 감지 (중간 밴드 상향 돌파)
        elif lower < price < upper:
            if price > middle:
                recent_prices = market_data.get('price_history', [])[-5:]
                if recent_prices and all(x < y for x, y in zip(recent_prices[:-1], recent_prices[1:])):
                    return 0.8  # 상승세 감지
        
        # 과매수 상태
        elif price >= upper:
            return 0.1
            
        return 0.5

class VolumeStrategy(StrategyBase):
    def analyze(self, market_data: Dict[str, Any]) -> float:
        avg_volume = market_data.get('average_volume', 1000)
        current_volume = market_data.get('current_volume', 1000)
        price_change = market_data.get('price_change_rate', 0)
        
        # 하락세 종료 감지 (거래량 급증 + 가격 반등)
        if current_volume > avg_volume * 2 and price_change > 0:
            return 0.9  # 하락세 종료 신호
        
        # 상승세 감지 (거래량 증가 + 상승추세)
        elif current_volume > avg_volume * 1.5 and price_change > 1:
            return 0.8  # 상승세 감지
            
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
        k = market_data.get('stoch_k', 50)
        d = market_data.get('stoch_d', 50)
        
        # 과매도 상태에서 하락세 종료 감지
        if k < 20 and d < 20:
            if k > d:  # 골든 크로스 형성
                return 0.9  # 하락세 종료 신호
        
        # 상승세 감지
        elif 20 <= k <= 80:
            recent_k = market_data.get('stoch_k_history', [])[-5:]
            if recent_k and all(x < y for x, y in zip(recent_k[:-1], recent_k[1:])):
                return 0.8  # 상승세 감지
        
        # 과매수 상태
        elif k > 80 and d > 80:
            return 0.1
            
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

class DowntrendEndStrategy(StrategyBase):
    def analyze(self, market_data: Dict) -> Dict:
        """하락장 종료 감지"""
        try:
            df = market_data['df']
            price = df['trade_price']  # 종가 사용
            volume = df['candle_acc_trade_volume']  # 거래량 사용
            
            # 20일 이동평균선
            ma20 = price.rolling(window=20).mean()
            # 거래량 이동평균
            vol_ma20 = volume.rolling(window=20).mean()
            
            # 하락 추세 반전 조건:
            # 1. 현재 가격이 20일 이동평균선 위로 돌파
            # 2. 거래량 증가 (현재 거래량이 20일 평균 거래량보다 큼)
            price_above_ma = price.iloc[-1] > ma20.iloc[-1] and price.iloc[-2] <= ma20.iloc[-2]
            volume_increase = volume.iloc[-1] > vol_ma20.iloc[-1] * 1.5  # 50% 이상 거래량 증가
            
            if price_above_ma and volume_increase:
                return {
                    'signal': 'buy',
                    'strength': 0.8,
                    'ma20': ma20.iloc[-1],
                    'current_price': price.iloc[-1],
                    'volume_ratio': volume.iloc[-1] / vol_ma20.iloc[-1]
                }
            
            return {'signal': 'hold', 'strength': 0.5}
            
        except Exception as e:
            logging.error(f"하락장 종료 분석 실패: {str(e)}")
            return {'signal': 'hold', 'strength': 0}

class UptrendEndStrategy(StrategyBase):
    def analyze(self, market_data: Dict) -> Dict:
        """상승장 종료 감지"""
        try:
            df = market_data['df']
            price = df['trade_price']  # 종가 사용
            volume = df['candle_acc_trade_volume']  # 거래량 사용
            
            # 20일 이동평균선
            ma20 = price.rolling(window=20).mean()
            # 거래량 이동평균
            vol_ma20 = volume.rolling(window=20).mean()
            
            # 상승 추세 반전 조건:
            # 1. 현재 가격이 20일 이동평균선 아래로 돌파
            # 2. 거래량 증가 (현재 거래량이 20일 평균 거래량보다 큼)
            # 3. 최근 고점 대비 하락
            price_below_ma = price.iloc[-1] < ma20.iloc[-1] and price.iloc[-2] >= ma20.iloc[-2]
            volume_increase = volume.iloc[-1] > vol_ma20.iloc[-1] * 1.5  # 50% 이상 거래량 증가
            recent_high = price.rolling(window=10).max().iloc[-1]  # 최근 10일 고점
            price_decline = price.iloc[-1] < recent_high * 0.95  # 5% 이상 하락
            
            if price_below_ma and volume_increase and price_decline:
                return {
                    'signal': 'sell',
                    'strength': 0.2,
                    'ma20': ma20.iloc[-1],
                    'current_price': price.iloc[-1],
                    'volume_ratio': volume.iloc[-1] / vol_ma20.iloc[-1],
                    'decline_rate': (recent_high - price.iloc[-1]) / recent_high
                }
            
            return {'signal': 'hold', 'strength': 0.5}
            
        except Exception as e:
            logging.error(f"상승장 종료 분석 실패: {str(e)}")
            return {'signal': 'hold', 'strength': 0}

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
    'MarketSentimentStrategy',
    'DowntrendEndStrategy',
    'UptrendEndStrategy'
]