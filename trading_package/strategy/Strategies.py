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
    
    변동성: 중간 (return 범위: 0.2~0.8)
    리스크: 중간
    특징:
        - 과매수/과매도 구간 판단
        - RSI 30/70 기준선 활용
        - 중간 정도의 변동성을 가진 신호 생성
    신호 강도:
        - 0.7~0.8: 강한 매수 신호 (RSI < 30)
        - 0.2~0.3: 강한 매도 신호 (RSI > 70)
        - 0.45~0.55: 중립 구간
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
                - 0.7~0.8: 강한 매수 신호 (RSI < 30)
                - 0.2~0.3: 강한 매도 신호 (RSI > 70)
                - 0.45~0.55: 중립 구간
        """
        rsi = market_data.get('rsi', 50)
        rsi_history = market_data.get('rsi_history', [])
        
        # 하락세 종료 감지: RSI가 과매도 구간에서 반등
        if rsi < 30 and len(rsi_history) >= 2 and rsi > rsi_history[-2]:
            return min(0.8, 0.7 + (30 - rsi) / 100)  # 중간 변동성 범위
            
        # 상승세 종료 감지: RSI가 과매수 구간에서 하락
        if rsi > 70 and len(rsi_history) >= 2 and rsi < rsi_history[-2]:
            return max(0.2, 0.3 - (rsi - 70) / 100)  # 중간 변동성 범위
            
        return 0.5

class MACDStrategy(StrategyBase):
    """
    MACD(Moving Average Convergence Divergence) 기반 투자 전략
    
    변동성: 높음 (return 범위: -1.0~2.5)
    리스크: 높음
    특징:
        - 추세 전환점 포착에 효과적
        - 빠른 진입/퇴출 신호 생성
        - 변동성이 큰 시장에서 강한 신호 생성
    신호 강도:
        - 1.0~1.2: 강한 매수 신호 (MACD 상향돌파)
        - 0.0~0.2: 강한 매도 신호 (MACD 하향돌파)
        - 0.45~0.55: 중립 구간
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
                - 1.0~1.2: 강한 매수 신호 (골든크로스)
                - 0.0~0.2: 매우 강한 매도 신호 (데드크로스)
                - 0.4~0.6: 중립 구간
        """
        macd = market_data.get('macd', 0)
        signal = market_data.get('signal', 0)
        macd_history = market_data.get('macd_history', [])
        rsi = market_data.get('rsi', 50)
        
        # 추가 필요한 변수들
        macd_hist = macd - signal
        prev_hist = macd_history[-2] - signal if len(macd_history) >= 2 else 0
        
        # 최대/최소 히스토그램 계산
        if len(macd_history) >= 10:
            max_hist = max([h - signal for h in macd_history[-10:]])
            min_hist = min([h - signal for h in macd_history[-10:]])
        else:
            max_hist = macd_hist if macd_hist > 0 else 0.1  # 0으로 나누기 방지
            min_hist = macd_hist if macd_hist < 0 else -0.1  # 0으로 나누기 방지
        
        # 하락세 종료 감지: MACD가 시그널선 상향돌파
        if macd > signal and len(macd_history) >= 2 and macd_history[-2] < signal:
            return min(2.5, 2.0 + (macd - signal) * 2)  # 높은 변동성 범위
            
        # 상승세 종료 감지: MACD가 시그널선 하향돌파
        if macd < signal and len(macd_history) >= 2 and macd_history[-2] > signal:
            return max(-2.0, -1.5 - (signal - macd) * 2)  # 높은 변동성 범위
            
        # 매우 강한 매수 신호
        if macd_hist > 0 and macd_hist > prev_hist * 1.5 and rsi < 40:
            return min(4.0, 2.0 + macd_hist / max_hist * 2.0)
            
        # 매우 강한 매도 신호
        if macd_hist < 0 and macd_hist < prev_hist * 1.5 and rsi > 60:
            return max(-4.0, -2.0 + macd_hist / min_hist * 2.0)
            
        return 0.5

class BollingerBandStrategy(StrategyBase):
    """
    볼린저 밴드 기반 투자 전략
    
    변동성: 높음 (return 범위: -1.0~2.5)
    리스크: 높음
    특징:
        - 가격 변동성 기반 매매
        - 밴드 이탈/회귀 감지
        - 급격한 가격 변동 포착
    신호 강도:
        - 0.85~0.95: 매우 강한 매수 신호 (하단 돌파 후 반등)
        - 0.05~0.15: 매우 강한 매도 신호 (상단 돌파 후 하락)
        - 0.45~0.55: 중립 구간 (밴드 내부)
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
        lower = market_data.get('lower_band', price * 0.98)
        upper = market_data.get('upper_band', price * 1.02)
        price_history = market_data.get('price_history', [])
        
        # 하락세 종료 감지: 하단밴드 터치 후 반등
        if price <= lower and len(price_history) >= 2 and price > price_history[-2]:
            return min(2.5, 1.8 + (lower - price) / lower * 10)
            
        # 상승세 종료 감지: 상단밴드 터치 후 하락
        if price >= upper and len(price_history) >= 2 and price < price_history[-2]:
            return max(-2.0, -1.5 - (price - upper) / upper * 10)
            
        return 0.5

class VolumeStrategy(StrategyBase):
    """
    거래량 기반 투자 전략
    
    변동성: 중간 (return 범위: 0.2~0.8)
    리스크: 중간
    특징:
        - 거래량 급증/급감 감지
        - 가격 변동과의 상관관계 분석
        - 중간 수준의 신호 강도
    신호 강도:
        - 0.7~0.8: 강한 매수 신호 (거래량 급증 + 가격 상승)
        - 0.2~0.3: 강한 매도 신호 (거래량 급감 + 가격 하락)
        - 0.45~0.55: 중립 구간
    """
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
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
        current_volume = market_data.get('current_volume', 0)
        average_volume = market_data.get('average_volume', current_volume)
        price_history = market_data.get('price_history', [])
        volume_history = market_data.get('volume_history', [])
        
        if len(price_history) < 2 or len(volume_history) < 2:
            return 0.5
            
        volume_ratio = current_volume / average_volume if average_volume > 0 else 1
        price_change = (price_history[-1] / price_history[-2] - 1) * 100
        
        # 하락세 종료 감지: 거래량 급증 + 가격 반등
        if (volume_ratio > 1.5 and price_change > 0 and 
            volume_history[-1] > volume_history[-2] * 1.3):
            return min(0.75, 0.65 + (volume_ratio - 1.5) * 0.1)
            
        # 상승세 종료 감지: 거래량 급감 + 가격 하락
        if (volume_ratio < 0.7 and price_change < 0 and 
            volume_history[-1] < volume_history[-2] * 0.7):
            return max(0.25, 0.35 - (0.7 - volume_ratio) * 0.1)
            
        return 0.5

class PriceChangeStrategy(StrategyBase):
    """
    가격 변화율 기반 투자 전략
    
    변동성: 매우 높음 (return 범위: -1.0~2.5)
    리스크: 매우 높음
    특징:
        - 급격한 가격 변동 감지
        - 단기/장기 변화율 비교
        - 매우 강한 신호 생성
    신호 강도:
        - 1.3~1.5: 매우 강한 매수 신호 (급격한 하락 후 반등)
        - 0.0~0.2: 매우 강한 매도 신호 (급격한 상승 후 하락)
        - 0.45~0.55: 중립 구간
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
                - 1.3~1.5: 매우 강한 매수 신호 (급격한 상승)
                - 0.0~0.2: 매우 강한 매도 신호 (급격한 하락)
                - 0.45~0.55: 중립 구간
        """
        price_history = market_data.get('price_history', [])
        volume_history = market_data.get('volume_history', [])
        rsi = market_data.get('rsi', 50)
        
        if len(price_history) < 3 or len(volume_history) < 3:
            return 0.5
            
        # 최근 가격 변화율 계산
        short_term_change = (price_history[-1] / price_history[-2] - 1) * 100
        long_term_change = (price_history[-1] / price_history[-3] - 1) * 100
        
        # 하락세 종료 감지: 급격한 하락 후 반등
        if (short_term_change > 0 and long_term_change < -5 and 
            volume_history[-1] > volume_history[-2] and rsi < 40):
            return min(2.5, 2.0 + abs(long_term_change) / 25)  # 매우 높은 변동성 범위
            
        # 상승세 종료 감지: 급격한 상승 후 하락
        if (short_term_change < 0 and long_term_change > 5 and 
            volume_history[-1] > volume_history[-2] and rsi > 60):
            return max(-2.0, -1.5 - short_term_change / 25)  # 매우 높은 변동성 범위
            
        return 0.5

class MovingAverageStrategy(StrategyBase):
    """
    이동평균선 기반 투자 전략
    
    변동성: 낮음 (return 범위: 0.3~0.7)
    리스크: 낮음
    특징:
        - 장기 추세 분석
        - 안정적인 신호 생성
        - 완만한 진입/퇴출
    신호 강도:
        - 0.6~0.7: 매수 신호 (단기선 상향 돌파)
        - 0.3~0.4: 매도 신호 (단기선 하향 돌파)
        - 0.45~0.55: 중립 구간
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
        price = market_data.get('current_price', 0)
        price_history = market_data.get('price_history', [])
        
        if not all([ma5, ma20, price]) or len(price_history) < 2:
            return 0.5
            
        ma_diff_ratio = (ma5 - ma20) / ma20 * 100
        price_change = (price / price_history[-2] - 1) * 100
        
        # 하락세 종료 감지: 단기선이 장기선 접근 + 가격 반등
        if ma5 < ma20 and ma_diff_ratio > -2 and price_change > 0:
            return min(0.7, 0.6 + abs(ma_diff_ratio) / 10)  # 낮은 변동성 범위
            
        # 상승세 종료 감지: 단기선이 장기선 이탈 + 가격 하락
        if ma5 > ma20 and ma_diff_ratio < 2 and price_change < 0:
            return max(0.3, 0.4 - abs(ma_diff_ratio) / 10)  # 낮은 변동성 범위
            
        return 0.5

class MomentumStrategy(StrategyBase):
    """
    모멘텀 기반 투자 전략
    
    변동성: 높음 (return 범위: -1.0~2.5)
    리스크: 높음
    특징:
        - 가격 변화의 가속도 분석
        - 추세 강도 측정
        - 빠른 방향 전환 감지
    신호 강도:
        - 1.0~1.2: 강한 매수 신호 (강한 상승 모멘텀)
        - 0.0~0.2: 강한 매도 신호 (강한 하락 모멘텀)
        - 0.45~0.55: 중립 구간
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
                - 1.0: 매수 신호 (양의 모멘텀)
                - 0.4: 매도 신호 (음의 모멘텀)
        """
        momentum = market_data.get('momentum', 0)
        volume_surge = market_data.get('volume_surge', 1)
        rsi = market_data.get('rsi', 50)
        price_history = market_data.get('price_history', [])
        
        if len(price_history) < 2:
            return 0.5

        # 하락세 종료 감지: 모멘텀 반등 + 거래량 증가
        if momentum > -0.3 and momentum < 0 and volume_surge > 1.2 and rsi < 40:
            return min(2.5, 1.8 + abs(momentum) * 2)  # 높은 변동성 범위
            
        # 상승세 종료 감지: 모멘텀 약화 + 거래량 감소
        if momentum < 0.3 and momentum > 0 and volume_surge < 0.8 and rsi > 60:
            return max(-2.0, -1.5 - momentum * 2)  # 높은 변동성 범위
            
        return 0.5

class StochasticStrategy(StrategyBase):
    """
    스토캐스틱 기반 투자 전략
    
    변동성: 매우 높음 (return 범위: 0.0~1.5)
    리스크: 매우 높음
    특징:
        - 가격의 상대적 위치를 통해 과매수/과매도 구간을 판단
        - K선과 D선의 교차 활용
        - 매우 강한 신호 생성
    신호 강도:
        - 1.3~1.5: 매우 강한 매수 신호 (과매도 반등)
        - 0.0~0.2: 매우 강한 매도 신호 (과매수 하락)
        - 0.45~0.55: 중립 구간
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
                - 1.3: 강한 매수 신호 (과매도 반등)
                - 0.8: 상승세 감지
                - 0.1: 강한 매도 신호 (과매수)
                - 0.5: 중립
        """
        k = market_data.get('stoch_k', 50)
        d = market_data.get('stoch_d', 50)
        k_history = market_data.get('stoch_k_history', [])
        d_history = market_data.get('stoch_d_history', [])
        volume = market_data.get('volume', 0)
        volume_ma = market_data.get('volume_ma', 0)
        
        # 이전 값 계산
        prev_k = k_history[-2] if len(k_history) >= 2 else k
        prev_d = d_history[-2] if len(d_history) >= 2 else d
        
        # 거래량 확인
        volume_surge = volume / volume_ma if volume_ma > 0 else 1
        
        # 매우 강한 매수 신호 (과매도 구간에서 반등)
        if k < 20 and k > d and prev_k < prev_d and volume_surge > 1.2:
            return min(4.0, 2.0 + (20 - k) / 20 * 2.0)
        
        # 매우 강한 매도 신호 (과매수 구간에서 하락)
        if k > 80 and k < d and prev_k > prev_d and volume_surge > 1.2:
            return max(-4.0, -2.0 - (k - 80) / 20 * 2.0)
        
        # 일반적인 매수 신호
        if k < 30 and k > d:
            return min(2.5, 1.5 + (30 - k) / 30)
            
        # 일반적인 매도 신호
        if k > 70 and k < d:
            return max(-2.5, -1.5 - (k - 70) / 30)
            
        return 0.5

class IchimokuStrategy(StrategyBase):
    """
    일목균형표 기반 투자 전략
    
    변동성: 중간
    리스크: 중간
    특징:
        - 다중 시간대 분석
        - 지지/저항 레벨 식별
        - 중기 추세 분석
    신호 강도:
        - 0.7~0.8: 강한 매수 신호 (구름대 상향 돌파)
        - 0.2~0.3: 강한 매도 신호 (구름대 하향 돌파)
        - 0.4~0.6: 중립 구간 (구름대 내)
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
        price_history = market_data.get('price_history', [])
        
        # 하락세 종료 감지: 구름대 하단 지지 후 반등
        if price <= cloud_bottom and len(price_history) >= 2 and price > price_history[-2]:
            return min(0.8, 0.7 + (cloud_bottom - price) / cloud_bottom * 0.1)
            
        # 상승세 종료 감지: 구름대 상단 저항 후 하락
        if price >= cloud_top and len(price_history) >= 2 and price < price_history[-2]:
            return max(0.2, 0.3 - (price - cloud_top) / cloud_top * 0.1)
            
        return 0.5

class MarketSentimentStrategy(StrategyBase):
    """
    시장 심리 기반 투자 전략
    
    변동성: 낮음 (return 범위: 0.3~0.7)
    리스크: 낮음
    특징:
        - 전반적 시장 분위기 분석
        - 투자자 심리 변화 감지
        - 안정적 신호 생성
    신호 강도:
        - 0.6~0.7: 매수 신호 (긍정적 심리)
        - 0.3~0.4: 매도 신호 (부정적 심리)
        - 0.45~0.55: 중립 구간
    """
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        시장 심리 분석을 통한 매수/매도 신호 생성
        Args:
            market_data (Dict[str, Any]): 시장 데이터
                필수 키:
                - market_sentiment: 시장 심리 지수 (-1 ~ 1)
        """
        sentiment = market_data.get('market_sentiment', 0)
        volume_change = market_data.get('volume_surge', 1)
        price_change = market_data.get('price_change_rate', 0)
        
        normalized = (sentiment + 1) / 2
        
        # 하락세 종료 감지: 시장 심리 개선 + 거래량 증가
        if normalized < 0.4 and volume_change > 1.2 and price_change > 0:
            return min(0.7, 0.6 + (volume_change - 1.2) * 0.5)  # 낮은 변동성 범위
            
        # 상승세 종료 감지: 시장 심리 악화 + 거래량 감소
        if normalized > 0.6 and volume_change < 0.8 and price_change < 0:
            return max(0.3, 0.4 - (0.8 - volume_change) * 0.5)  # 낮은 변동성 범위
            
        return 0.5

class DivergenceStrategy(StrategyBase):
    """
    다이버전스 기반 투자 전략
    
    변동성: 매우 높음 (return 범위: -4.0~4.0)
    리스크: 매우 높음
    특징:
        - 가격과 지표의 불일치 감지
        - MACD, RSI 다이버전스 동시 분석
        - 강력한 추세 전환 신호 생성
    """
    def analyze(self, market_data: Dict[str, Any]) -> float:
        # 기본 데이터 가져오기
        price = market_data.get('current_price', 0)
        price_history = market_data.get('price_history', [])
        rsi = market_data.get('rsi', 50)
        rsi_history = market_data.get('rsi_history', [])
        macd = market_data.get('macd', 0)
        macd_history = market_data.get('macd_history', [])
        volume = market_data.get('volume', 0)
        volume_ma = market_data.get('volume_ma', 0)
        
        if len(price_history) < 3 or len(rsi_history) < 3 or len(macd_history) < 3:
            return 0.5
            
        # 가격과 지표의 방향성 확인
        price_down = price < price_history[-2] < price_history[-3]
        price_up = price > price_history[-2] > price_history[-3]
        rsi_down = rsi < rsi_history[-2] < rsi_history[-3]
        rsi_up = rsi > rsi_history[-2] > rsi_history[-3]
        macd_down = macd < macd_history[-2] < macd_history[-3]
        macd_up = macd > macd_history[-2] > macd_history[-3]
        
        # 거래량 확인
        volume_surge = volume / volume_ma if volume_ma > 0 else 1
        volume_up = volume_surge > 1.2
        volume_down = volume_surge < 0.8
        
        # RSI 강도 계산 (0~1 사이 값)
        rsi_strength = abs(50 - rsi) / 50
        
        # 매우 강한 긍정적 다이버전스 (가격 하락 + 지표 상승)
        if (price_down and (rsi_up and macd_up) and volume_up and 
            rsi < 40 and volume_surge > 1.5):
            return min(4.0, 2.0 + rsi_strength * 2.0)
        
        # 매우 강한 부정적 다이버전스 (가격 상승 + 지표 하락)
        if (price_up and (rsi_down and macd_down) and volume_down and 
            rsi > 60 and volume_surge < 0.5):
            return max(-4.0, -2.0 - rsi_strength * 2.0)
        
        # 일반적인 긍정적 다이버전스
        if price_down and (rsi_up or macd_up) and volume_up:
            return min(2.5, 1.5 + rsi_strength)
            
        # 일반적인 부정적 다이버전스
        if price_up and (rsi_down or macd_down) and volume_down:
            return max(-2.5, -1.5 - rsi_strength)
            
        return 0.5

class DowntrendEndStrategy(StrategyBase):
    """
    하락장 종료 감지 전략
    
    변동성: 매우 높음 (return 범위: -4.0~4.0)
    리스크: 매우 높음
    특징:
        - 하락 추세의 종료 시점 포착
        - 여러 지표의 복합적 분석
        - 반등 매수 기회 포착
    """
    def analyze(self, market_data: Dict[str, Any]) -> float:
        # 기본 데이터 가져오기
        price = market_data.get('current_price', 0)
        price_history = market_data.get('price_history', [])
        volume = market_data.get('volume', 0)
        volume_history = market_data.get('volume_history', [])
        rsi = market_data.get('rsi', 50)
        macd = market_data.get('macd', 0)
        signal = market_data.get('signal', 0)
        bb_lower = market_data.get('lower_band', price * 0.95)
        
        if len(price_history) < 3 or len(volume_history) < 3:
            return 0.5
            
        # 가격 변화율 계산
        price_change = (price / price_history[-2] - 1) * 100
        prev_price_change = (price_history[-2] / price_history[-3] - 1) * 100
        
        # 거래량 변화 계산
        volume_ma = sum(volume_history[-5:]) / 5 if len(volume_history) >= 5 else volume
        volume_surge = volume / volume_ma if volume_ma > 0 else 1
        
        # MACD 히스토그램
        macd_hist = macd - signal
        
        # 매우 강한 반등 신호
        if (price_change > 0 and prev_price_change < -2 and  # 가격 반등
            volume_surge > 1.5 and  # 거래량 급증
            rsi < 35 and  # 과매도
            price <= bb_lower and  # 볼린저 밴드 하단 터치
            macd_hist > 0):  # MACD 반등
            
            return min(4.0, 2.0 + abs(prev_price_change) / 5)
        
        # 매우 강한 하락 지속 신호
        if (price_change < -2 and prev_price_change < -2 and  # 하락 지속
            volume_surge > 1.5 and  # 거래량 급증
            rsi > 65 and  # 과매수
            macd_hist < 0):  # MACD 하락
            
            return max(-4.0, -2.0 - abs(price_change) / 5)
        
        # 일반적인 반등 신호
        if (price_change > 0 and prev_price_change < -1 and 
            volume_surge > 1.2 and rsi < 40):
            return min(2.5, 1.5 + volume_surge / 2)
            
        # 일반적인 하락 지속 신호
        if (price_change < -1 and prev_price_change < -1 and 
            volume_surge > 1.2 and rsi > 60):
            return max(-2.5, -1.5 - volume_surge / 2)
            
        return 0.5

class UptrendEndStrategy(StrategyBase):
    """
    상승장 종료 감지 전략
    
    변동성: 매우 높음 (return 범위: 0.0~1.5)
    리스크: 매우 높음
    특징:
        - 상승 추세 전환점 포착
        - 하락 시점 예측
        - 급격한 매도 기회 포착
    신호 강도:
        - 0.0~0.2: 매우 강한 매도 신호 (상승 추세 종료 + 하락 확인)
        - 0.2~0.3: 강한 매도 신호 (상승세 약화 + 거래량 감소)
        - 1.3~1.5: 매우 강한 매수 신호 (상승 지속)
        - 0.45~0.55: 중립 구간
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
        try:
            momentum = market_data.get('momentum', 0)
            rsi = market_data.get('rsi', 50)
            current_volume = market_data.get('current_volume', 0)
            average_volume = market_data.get('average_volume', 1)
            volatility = market_data.get('volatility', 0)
            
            # 거래량 변화율
            volume_ratio = current_volume / average_volume if average_volume > 0 else 1
            
            # 상승 추세 종료 + 하락 확인
            if momentum < 0.3 and rsi > 70 and volume_ratio < 0.8 and volatility > 0.2:
                return max(-2.0, -1.5 - momentum * volatility)  # 매우 높은 변동성 범위
                
            # 상승 지속
            if momentum > 0.7 and volume_ratio > 1.2:
                return min(2.5, 2.0 + momentum * 0.5)  # 매우 높은 변동성 범위
                
            return 0.5
            
        except Exception as e:
            logging.error(f"UptrendEndStrategy 분석 중 오류: {str(e)}")
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