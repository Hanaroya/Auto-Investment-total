from abc import ABC, abstractmethod
from typing import Dict, Any, List  # List 추가

    # ... 나머지 코드는 동일
class StrategyBase(ABC):
    """전략 기본 클래스"""
    
    @abstractmethod
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        시장 데이터 분석 후 매수/매도 신호 반환
        Returns:
            float: 0~1 사이의 값 (1에 가까울수록 매수 신호)
        """
        pass

class StrategyManager:
    def __init__(self, buy_threshold: float = 0.65, sell_threshold: float = 0.35):
        self.strategies: List[StrategyBase] = []
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        
    def add_strategy(self, strategy: StrategyBase) -> None:
        """전략 추가"""
        self.strategies.append(strategy)
        
    def get_decision(self, market_data: Dict[str, Any]) -> str:
        """
        모든 전략의 분석 결과를 취합하여 최종 투자 결정
        Returns:
            str: "buy", "sell", 또는 "hold"
        """
        if not self.strategies:
            return "hold"
            
        signals = [strategy.analyze(market_data) for strategy in self.strategies]
        average_signal = sum(signals) / len(signals)
        
        if average_signal >= self.buy_threshold:
            return "buy"
        elif average_signal <= self.sell_threshold:
            return "sell"
        else:
            return "hold" 