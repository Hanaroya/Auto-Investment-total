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
        self.strategies: List[StrategyBase] = self._load_all_strategies()
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        
    def _load_all_strategies(self) -> List[StrategyBase]:
        """모든 전략 클래스를 자동으로 로드"""
        import inspect
        import sys
        import pkgutil
        import importlib
        import strategy  # strategy 패키지

        strategies = []
        # strategy 패키지 내의 모든 모듈을 순회
        for _, name, _ in pkgutil.iter_modules(strategy.__path__):
            module = importlib.import_module(f'strategy.{name}')
            # 모듈 내의 모든 클래스를 검사
            for _, obj in inspect.getmembers(module, inspect.isclass):
                # StrategyBase를 상속받은 클래스이고 추상 클래스가 아닌 경우
                if (issubclass(obj, StrategyBase) and 
                    obj != StrategyBase and 
                    not inspect.isabstract(obj)):
                    strategies.append(obj())
        
        return strategies
        
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