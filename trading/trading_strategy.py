class TradingStrategy:
    def __init__(self, config):
        self.config = config
        self.base_position_size = config.get('base_position_size', 100000)
        self.max_position_size = config.get('max_position_size', 500000)
        self.min_position_size = config.get('min_position_size', 5000)
        
    def calculate_position_size(self, coin: str, market_condition: dict, trends: dict) -> int:
        """포지션 크기 동적 계산"""
        base_size = self.base_position_size
        
        # 시장 위험도에 따른 조정
        risk_factor = self._calculate_risk_factor(market_condition)
        
        # 추세 강도에 따른 조정
        trend_factor = self._calculate_trend_factor(trends)
        
        # 변동성에 따른 조정
        volatility_factor = self._calculate_volatility_factor(trends)
        
        # 최종 포지션 크기 계산
        position_size = base_size * risk_factor * trend_factor * volatility_factor
        
        # 최소/최대 범위 적용
        return max(min(int(position_size), self.max_position_size), self.min_position_size)
        
    def adjust_thresholds(self, market_condition: dict, trends: dict) -> dict:
        """시장 상황에 따른 임계값 동적 조정"""
        base_buy = self.config['strategy']['buy_threshold']
        base_sell = self.config['strategy']['sell_threshold']
        
        # 시장 위험도에 따른 조정
        risk_level = market_condition['risk_level']
        
        # 추세 강도에 따른 조정
        trend_strength = self._get_trend_strength(trends)
        
        # 매수/매도 임계값 조정
        buy_threshold = base_buy * (1 + risk_level) * (1 - trend_strength * 0.2)
        sell_threshold = base_sell * (1 - risk_level) * (1 + trend_strength * 0.2)
        
        return {
            'buy_threshold': buy_threshold,
            'sell_threshold': sell_threshold
        }
        
    def _calculate_risk_factor(self, market_condition: dict) -> float:
        """시장 위험도에 따른 조정 계수 계산"""
        risk_level = market_condition['risk_level']
        fear_greed = market_condition.get('fear_and_greed', 50)
        
        # 위험도가 높을수록 포지션 크기 감소
        risk_factor = 1 - (risk_level * 0.5)
        
        # 극단적인 공포/탐욕 상태 반영
        if fear_greed < 20:  # 극도의 공포
            risk_factor *= 0.7
        elif fear_greed > 80:  # 극도의 탐욕
            risk_factor *= 0.8
            
        return max(min(risk_factor, 1.0), 0.3)
        
    def _calculate_trend_factor(self, trends: dict) -> float:
        """추세 강도에 따른 조정 계수 계산"""
        trend_1m = trends.get('1m', {}).get('trend', 0)
        trend_15m = trends.get('15m', {}).get('trend', 0)
        trend_240m = trends.get('240m', {}).get('trend', 0)
        
        # 각 시간대별 가중치 적용
        weighted_trend = (
            trend_1m * 0.2 +
            trend_15m * 0.5 +
            trend_240m * 0.3
        )
        
        # 강한 추세일수록 포지션 크기 증가
        trend_factor = 1 + (weighted_trend * 0.3)
        return max(min(trend_factor, 1.5), 0.7)
        
    def _calculate_volatility_factor(self, trends: dict) -> float:
        """변동성에 따른 조정 계수 계산"""
        volatility_1m = trends.get('1m', {}).get('volatility', 0)
        volatility_15m = trends.get('15m', {}).get('volatility', 0)
        
        # 변동성이 높을수록 포지션 크기 감소
        weighted_volatility = (
            volatility_1m * 0.3 +
            volatility_15m * 0.7
        )
        
        volatility_factor = 1 - (weighted_volatility * 0.4)
        return max(min(volatility_factor, 1.0), 0.6)
        
    def _get_trend_strength(self, trends: dict) -> float:
        """추세 강도 계산"""
        trend_1m = abs(trends.get('1m', {}).get('trend', 0))
        trend_15m = abs(trends.get('15m', {}).get('trend', 0))
        trend_240m = abs(trends.get('240m', {}).get('trend', 0))
        
        return (trend_1m * 0.2 + trend_15m * 0.5 + trend_240m * 0.3) 