from math import floor

class TradingStrategy:
    def __init__(self, config, total_max_investment):
        self.config = config
        self.total_max_investment = total_max_investment  # 전체 100% 설정
        self.investment_each = floor(self.total_max_investment * 0.8 / 20)  # 기본 분할 투자 단위
        self._cache = {}
        
    def calculate_position_size(self, market: str, market_condition: dict, trends: dict) -> int:
        """포지션 크기 동적 계산"""
        cache_key = f"{market}_{market_condition.get('timestamp', '')}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # 기본 투자금은 investment_each 사용
        base_size = self.investment_each
        
        # 시장 위험도에 따른 조정
        risk_factor = self._calculate_risk_factor(market_condition)
        
        # 추세 강도에 따른 조정
        trend_factor = self._calculate_trend_factor(trends)
        
        # 변동성에 따른 조정
        volatility_factor = self._calculate_volatility_factor(trends)
        
        # 최종 포지션 크기 계산 (시장 상황에 따른 동적 가중치 적용)
        weights = self._get_dynamic_weights(risk_factor, trend_factor, volatility_factor)
        position_size = base_size * (
            risk_factor * weights['risk'] +
            trend_factor * weights['trend'] +
            volatility_factor * weights['volatility']
        )
        
        # 최소/최대 범위 적용 (전체 투자금의 80%를 넘지 않도록)
        result = max(min(int(position_size), floor(self.total_max_investment * 0.8)), floor(self.investment_each))
        self._cache[cache_key] = result
        return result
        
    def _get_dynamic_weights(self, risk: float, trend: float, volatility: float) -> dict:
        """시장 상황에 따른 동적 가중치 계산"""
        weights = {'risk': 0.4, 'trend': 0.3, 'volatility': 0.3}
        
        # 극단적 상황에서 가중치 조정
        if risk < 0.3:  # 고위험 상황
            weights['risk'] = 0.6
            weights['volatility'] = 0.2
            weights['trend'] = 0.2
        elif volatility > 0.8:  # 고변동성 상황
            weights['volatility'] = 0.5
            weights['risk'] = 0.3
            weights['trend'] = 0.2
            
        return weights
        
    def _calculate_risk_factor(self, market_condition: dict) -> float:
        """시장 위험도에 따른 조정 계수 계산"""
        risk_level = market_condition['risk_level']
        fear_greed = market_condition.get('fear_and_greed', 50)
        
        # 위험도가 높을수록 포지션 크기 감소 (비선형 조정)
        risk_factor = 1 - (risk_level * risk_level * 0.5)
        
        # 극단적인 공포/탐욕 상태 반영 (연속적 조정)
        if fear_greed < 20:
            adjustment = 0.7 + (fear_greed / 20) * 0.3  # 10~20 구간에서 연속적 변화
            risk_factor *= adjustment
        elif fear_greed > 80:
            adjustment = 1 - ((fear_greed - 80) / 20) * 0.2  # 80~90 구간에서 연속적 변화
            risk_factor *= adjustment
            
        return max(min(risk_factor, 1.0), 0.3)
        
    def _calculate_trend_factor(self, trends: dict) -> float:
        """추세 강도에 따른 조정 계수 계산"""
        trend_1m = trends.get('1m', {}).get('trend', 0)
        trend_15m = trends.get('15m', {}).get('trend', 0)
        trend_240m = trends.get('240m', {}).get('trend', 0)
        
        # 각 시간대별 가중치 적용 (유지)
        weighted_trend = (
            trend_1m * 0.2 +
            trend_15m * 0.5 +
            trend_240m * 0.3
        )
        
        # 비선형 조정 적용
        trend_factor = 1 + (weighted_trend * abs(weighted_trend) * 0.3)
        return max(min(trend_factor, 1.5), 0.7)
        
    def _calculate_volatility_factor(self, trends: dict) -> float:
        """변동성에 따른 조정 계수 계산"""
        volatility_1m = trends.get('1m', {}).get('volatility', 0)
        volatility_15m = trends.get('15m', {}).get('volatility', 0)
        
        weighted_volatility = (
            volatility_1m * 0.3 +
            volatility_15m * 0.7
        )
        
        # 비선형 변동성 조정
        volatility_factor = 1 - (weighted_volatility * weighted_volatility * 0.4)
        return max(min(volatility_factor, 1.0), 0.6)
        
    def adjust_thresholds(self, market_condition: dict, trends: dict) -> dict:
        """시장 상황에 따른 임계값 동적 조정
        
        매수/매도 임계값을 시장 상황에 따라 미세 조정합니다.
        - 위험도가 높을 때: 매수 임계값 +0.05~0.1 상향, 매도 임계값 -0.05~0.1 하향
        - 추세가 강할 때: 추세 방향에 따라 임계값 미세 조정
        """
        base_buy = self.config['strategy']['buy_threshold']  # 기본값 0.75
        base_sell = self.config['strategy']['sell_threshold']  # 기본값 0.55
        
        # 시장 위험도에 따른 조정 (0~1 사이값)
        risk_level = market_condition['risk_level']
        risk_adjustment = risk_level * 0.1  # 최대 ±0.1 조정
        
        # 추세 강도에 따른 조정 (-1~1 사이값)
        trend_strength = self._get_trend_strength(trends)
        trend_adjustment = abs(trend_strength) * 0.05  # 최대 ±0.05 조정
        
        # 매수 임계값 조정
        if risk_level > 0.5:  # 위험도가 높을 때
            buy_threshold = base_buy + risk_adjustment
        else:  # 위험도가 낮을 때
            buy_threshold = base_buy - trend_adjustment if trend_strength > 0 else base_buy
            
        # 매도 임계값 조정
        if risk_level > 0.5:  # 위험도가 높을 때
            sell_threshold = base_sell - risk_adjustment
        else:  # 위험도가 낮을 때
            sell_threshold = base_sell + trend_adjustment if trend_strength < 0 else base_sell
        
        # 임계값 범위 제한
        buy_threshold = max(min(buy_threshold, base_buy + 0.1), base_buy - 0.05)
        sell_threshold = max(min(sell_threshold, base_sell + 0.05), base_sell - 0.1)
        
        return {
            'buy_threshold': buy_threshold,
            'sell_threshold': sell_threshold
        }
        
    def _get_trend_strength(self, trends: dict) -> float:
        """추세 강도 계산"""
        trend_1m = abs(trends.get('1m', {}).get('trend', 0))
        trend_15m = abs(trends.get('15m', {}).get('trend', 0))
        trend_240m = abs(trends.get('240m', {}).get('trend', 0))
        
        return (trend_1m * 0.2 + trend_15m * 0.5 + trend_240m * 0.3) 