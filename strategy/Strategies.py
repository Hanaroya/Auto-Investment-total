"""
다양한 투자 전략 구현 모듈

이 모듈은 여러 기술적 분석 전략들을 구현한 클래스들을 포함합니다.
각 전략은 StrategyBase를 상속받아 독립적으로 동작합니다.
"""

from typing import Dict, Any
import numpy as np
from .StrategyBase import StrategyBase
import logging

class RSIStrategy(StrategyBase):
    """
    RSI(Relative Strength Index) 기반 투자 전략
    
    과매수/과매도 구간을 활용하여 매수/매도 시점을 포착합니다.
    
    Notes:
        - RSI < 30: 과매도 구간으로 매수 기회 탐색
        - RSI > 70: 과매수 구간으로 매도 기회 탐색
        - 30 <= RSI <= 70: 추세 방향 분석
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        RSI 지표를 기반으로 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - rsi: 현재 RSI 값
                - rsi_history: 최근 RSI 이력 (최소 5개 데이터)
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (하락세 종료)
                - 0.8: 상승세 감지
                - 0.1: 강한 매도 신호 (과매수)
                - 0.5: 중립
        """
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
    """
    MACD(Moving Average Convergence Divergence) 기반 투자 전략
    
    MACD와 신호선의 교차를 통해 매수/매도 시점을 포착합니다.
    
    Notes:
        - MACD > Signal: 상승 추세 시작
        - MACD < Signal: 하락 추세 시작
        - MACD 방향성: 추세 강도 확인
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        MACD 지표를 기반으로 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - macd: 현재 MACD 값
                - signal: 현재 시그널 값
                - macd_history: 최근 MACD 이력
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (하락세 종료)
                - 0.8: 상승세 감지
                - 0.3: 매도 신호
        """
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
    """
    볼린저 밴드 기반 투자 전략
    
    가격이 밴드를 벗어나는 상황을 통해 매수/매도 시점을 포착합니다.
    
    Notes:
        - 하단 밴드 접촉: 매수 기회
        - 상단 밴드 접촉: 매도 기회
        - 밴드 폭: 변동성 확인
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        볼린저 밴드 기반 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - current_price: 현재 가격
                - lower_band: 하단 밴드
                - upper_band: 상단 밴드
                - middle_band: 중간 밴드
                - price_history: 가격 이력
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (하단 밴드 반등)
                - 0.7: 상승세 감지
                - 0.3: 매도 신호
        """
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
                    return 0.7  # 상승세 감지
                    
        return 0.3

class VolumeStrategy(StrategyBase):
    """
    거래량 기반 투자 전략
    
    거래량의 급격한 변화와 가격 변동을 분석하여 매수/매도 시점을 포착합니다.
    
    Notes:
        - 거래량 급증 + 가격 상승: 강한 매수 신호
        - 거래량 증가 + 상승 추세: 매수 기회
        - 거래량 감소: 추세 약화 신호
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        거래량 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - average_volume: 평균 거래량
                - current_volume: 현재 거래량
                - price_change_rate: 가격 변화율(%)
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (거래량 급증 + 가격 반등)
                - 0.8: 상승세 감지 (거래량 증가 + 상승추세)
                - 0.4: 중립적 신호
        """
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
    """
    가격 변동 기반 투자 전략
    
    급격한 가격 변동을 통해 반등 기회를 포착합니다.
    
    Notes:
        - 급격한 하락 후 반등 기대
        - 급격한 상승 후 조정 예상
        - 변동성이 낮을 때는 중립 유지
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        가격 변동 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - price_change_rate: 가격 변화율(%)
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.8: 매수 신호 (큰 하락 후)
                - 0.2: 매도 신호 (큰 상승 후)
                - 0.5: 중립
        """
        change_rate = market_data.get('price_change_rate', 0)
        if change_rate < -5:
            return 0.8  # 큰 하락 시 매수 신호
        elif change_rate > 5:
            return 0.2  # 큰 상승 시 매도 신호
        return 0.5

class MovingAverageStrategy(StrategyBase):
    """
    이동평균선 기반 투자 전략
    
    단기/장기 이동평균선의 교차를 통해 추세를 파악합니다.
    
    Notes:
        - 단기선이 장기선 상향 돌파: 상승 추세 시작
        - 단기선이 장기선 하향 돌파: 하락 추세 시작
        - 이동평균선 간격: 추세 강도 확인
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        이동평균선 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - ma5: 5일 이동평균
                - ma20: 20일 이동평균
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.7: 매수 신호 (단기선 상향 돌파)
                - 0.3: 매도 신호 (단기선 하향 돌파)
        """
        ma5 = market_data.get('ma5', 0)
        ma20 = market_data.get('ma20', 0)
        
        if ma5 > ma20:
            return 0.7  # 상승 추세
        return 0.3  # 하락 추세

class MomentumStrategy(StrategyBase):
    """
    모멘텀 기반 투자 전략
    
    가격 변화의 운동량을 분석하여 추세의 강도를 측정합니다.
    
    Notes:
        - 양의 모멘텀: 상승 추세 지속 예상
        - 음의 모멘텀: 하락 추세 지속 예상
        - 모멘텀 변화: 추세 전환 신호
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        모멘텀 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - momentum: 현재 모멘텀 값
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.6: 매수 신호 (양의 모멘텀)
                - 0.4: 매도 신호 (음의 모멘텀)
        """
        momentum = market_data.get('momentum', 0)
        if momentum > 0:
            return 0.6
        return 0.4

class StochasticStrategy(StrategyBase):
    """
    스토캐스틱 기반 투자 전략
    
    가격의 상대적 위치를 통해 과매수/과매도 구간을 판단합니다.
    
    Notes:
        - K선과 D선의 교차 활용
        - 과매수/과매도 구간 판단
        - 신호선 교차 시점 포착
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        스토캐스틱 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - stoch_k: 스토캐스틱 K값
                - stoch_d: 스토캐스틱 D값
                - stoch_k_history: K값 이력
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (과매도 반등)
                - 0.8: 상승세 감지
                - 0.1: 강한 매도 신호 (과매수)
                - 0.5: 중립
        """
        k = market_data.get('stoch_k', 50)
        d = market_data.get('stoch_d', 50)
        
        # 과매도 상태에서 반등
        if k < 20 and d < 20:
            if k > d:  # 골든 크로스
                return 0.9
        
        # 상승세 감지
        elif 20 <= k <= 80:
            recent_k = market_data.get('stoch_k_history', [])[-5:]
            if recent_k and all(x < y for x, y in zip(recent_k[:-1], recent_k[1:])):
                return 0.8
        
        # 과매수 상태
        elif k > 80 and d > 80:
            return 0.1
            
        return 0.5

class IchimokuStrategy(StrategyBase):
    """
    일목균형표 기반 투자 전략
    
    여러 지표선의 관계를 통해 추세와 지지/저항을 분석합니다.
    
    Notes:
        - 구름대 위치 확인
        - 전환선과 기준선의 관계
        - 선행스팬의 방향성
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        일목균형표 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - current_price: 현재 가격
                - ichimoku_cloud_top: 구름대 상단
                - ichimoku_cloud_bottom: 구름대 하단
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.7: 매수 신호 (구름대 상향 돌파)
                - 0.3: 매도 신호 (구름대 하향 돌파)
                - 0.5: 중립 (구름대 내)
        """
        price = market_data.get('current_price', 0)
        cloud_top = market_data.get('ichimoku_cloud_top', price)
        cloud_bottom = market_data.get('ichimoku_cloud_bottom', price)
        
        if price > cloud_top:
            return 0.7  # 구름대 상향 돌파
        elif price < cloud_bottom:
            return 0.3  # 구름대 하향 돌파
        return 0.5  # 구름대 내

class MarketSentimentStrategy(StrategyBase):
    """
    시장 심리 기반 투자 전략
    
    시장의 전반적인 분위기를 분석하여 투자 판단에 활용합니다.
    
    Notes:
        - 시장 심리 지수 활용
        - 과열/침체 구간 판단
        - 투자자 심리 변화 감지
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        시장 심리 분석을 통한 매수/매도 신호 생성
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - market_sentiment: 시장 심리 지수 (-1 ~ 1)
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                (시장 심리 지수를 0~1 범위로 정규화)
        """
        sentiment = market_data.get('market_sentiment', 0)  # -1 to 1
        return (sentiment + 1) / 2  # normalize to 0-1

class DowntrendEndStrategy(StrategyBase):
    """
    하락장 종료 감지 전략
    
    하락 추세의 종료 시점을 포착하여 매수 기회를 찾습니다.
    
    Notes:
        - 하락 추세 강도 약화 감지
        - 거래량 변화 패턴 분석
        - 가격 반등 신호 확인
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        하락장 종료 신호 분석
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - price_history: 최근 가격 이력
                - volume_history: 최근 거래량 이력
                - trend_strength: 추세 강도 (-1 ~ 1)
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 반등 신호
                - 0.7: 하락세 약화 감지
                - 0.5: 중립
                - 0.3: 하락 지속
        """
        trend_strength = market_data.get('trend_strength', 0)
        price_history = market_data.get('price_history', [])
        volume_history = market_data.get('volume_history', [])
        
        if not price_history or not volume_history:
            return 0.5
            
        # 하락세 약화 + 거래량 증가
        if trend_strength > -0.3 and trend_strength < 0:
            if volume_history[-1] > sum(volume_history[:-1]) / len(volume_history[:-1]):
                return 0.7
                
        # 강한 반등 신호
        if len(price_history) >= 3:
            if all(price_history[i] < price_history[i+1] for i in range(len(price_history)-2)):
                return 0.9
                
        # 하락 지속
        if trend_strength < -0.7:
            return 0.3
            
        return 0.5

class UptrendEndStrategy(StrategyBase):
    """
    상승장 종료 감지 전략
    
    상승 추세의 종료 시점을 포착하여 매도 기회를 찾습니다.
    
    Notes:
        - 상승 추세 모멘텀 약화 감지
        - 거래량 감소 패턴 분석
        - 가격 조정 신호 확인
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        상승장 종료 신호 분석
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - price_history: 최근 가격 이력
                - volume_history: 최근 거래량 이력
                - momentum: 모멘텀 지표 (-1 ~ 1)
                - volatility: 변동성 지표
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.1: 강한 매도 신호 (상승세 종료)
                - 0.3: 상승세 약화 감지
                - 0.5: 중립
                - 0.7: 상승 지속
        """
        momentum = market_data.get('momentum', 0)
        volatility = market_data.get('volatility', 0)
        volume_history = market_data.get('volume_history', [])
        
        if not volume_history:
            return 0.5
            
        # 모멘텀 약화 + 변동성 증가
        if momentum < 0.3 and momentum > 0 and volatility > 0.2:
            if volume_history[-1] < sum(volume_history[:-1]) / len(volume_history[:-1]):
                return 0.1  # 강한 매도 신호
                
        # 상승세 약화 감지
        if 0 < momentum < 0.5:
            return 0.3
            
        # 강한 상승 지속
        if momentum > 0.7:
            return 0.7
            
        return 0.5

class DivergenceStrategy(StrategyBase):
    """
    다이버전스 감지 전략
    
    가격과 지표 간의 불일치를 통해 추세 전환 시점을 포착합니다.
    
    Notes:
        - 가격과 RSI 다이버전스
        - 가격과 MACD 다이버전스
        - 가격과 거래량 다이버전스
    """
    
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        다이버전스 패턴 분석
        
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - price_history: 최근 가격 이력
                - rsi_history: RSI 이력
                - macd_history: MACD 이력
                - volume_history: 거래량 이력
        
        Returns:
            float: 0~1 사이의 매수 신호 강도
                - 0.9: 강한 매수 신호 (긍정적 다이버전스)
                - 0.2: 강한 매도 신호 (부정적 다이버전스)
                - 0.5: 다이버전스 없음
        """
        price_history = market_data.get('price_history', [])
        rsi_history = market_data.get('rsi_history', [])
        macd_history = market_data.get('macd_history', [])
        
        if len(price_history) < 2 or len(rsi_history) < 2:
            return 0.5
            
        # 긍정적 다이버전스 (가격 하락, 지표 상승)
        if price_history[-1] < price_history[-2] and rsi_history[-1] > rsi_history[-2]:
            if macd_history and macd_history[-1] > macd_history[-2]:
                return 0.9
                
        # 부정적 다이버전스 (가격 상승, 지표 하락)
        if price_history[-1] > price_history[-2] and rsi_history[-1] < rsi_history[-2]:
            if macd_history and macd_history[-1] < macd_history[-2]:
                return 0.2
                
        return 0.5

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
    'UptrendEndStrategy',
    'DivergenceStrategy'
]