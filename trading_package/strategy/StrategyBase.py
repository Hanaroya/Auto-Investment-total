"""
투자 전략의 기본 구조를 정의하는 모듈
모든 전략 클래스는 이 기본 클래스를 상속받아 구현됩니다.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List

class StrategyBase(ABC):
    """
    전략 기본 클래스 (추상 클래스)
    
    모든 투자 전략은 이 클래스를 상속받아 analyze 메서드를 구현해야 합니다.
    
    Notes:
        - 각 전략은 독립적으로 동작하며, 시장 데이터를 분석하여 매수/매도 신호를 생성
        - 반환값은 0~1 사이의 값으로, 1에 가까울수록 강한 매수 신호를 의미
    """
    
    @abstractmethod
    def analyze(self, market_data: Dict[str, Any]) -> float:
        """
        시장 데이터를 분석하여 매수/매도 신호를 생성하는 추상 메서드
        
        Args:
            market_data (Dict[str, Any]): 분석할 시장 데이터
                필수 포함 정보:
                - current_price: 현재 가격
                - volume: 거래량
                - price_history: 가격 이력
                - technical_indicators: 기술적 지표들
        
        Returns:
            float: 0~1 사이의 값으로 표현된 매수 신호 강도
                - 1에 가까울수록 강한 매수 신호
                - 0에 가까울수록 강한 매도 신호
                - 0.5 주변은 중립적 신호
        
        Notes:
            - 각 전략 클래스는 이 메서드를 반드시 구현해야 함
            - 시장 데이터가 부족한 경우 기본값 0.5 반환 권장
        """
        pass

class StrategyManager:
    """
    전략 관리자 클래스
    
    여러 투자 전략을 통합 관리하고 최종 투자 결정을 생성합니다.
    
    Attributes:
        strategies (List[StrategyBase]): 등록된 전략 목록
        buy_threshold (float): 매수 결정 임계값
        sell_threshold (float): 매도 결정 임계값
    """

    def __init__(self, buy_threshold: float = 0.65, sell_threshold: float = 0.35):
        """
        StrategyManager 초기화
        
        Args:
            buy_threshold (float, optional): 매수 결정 임계값. Defaults to 0.65.
            sell_threshold (float, optional): 매도 결정 임계값. Defaults to 0.35.
        """
        self.strategies: List[StrategyBase] = self._load_all_strategies()
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        
    def _load_all_strategies(self) -> List[StrategyBase]:
        """
        모든 전략 클래스를 자동으로 로드
        
        Returns:
            List[StrategyBase]: 초기화된 전략 객체 리스트
            
        Notes:
            - strategy 패키지 내의 모든 전략 클래스를 자동으로 검색
            - StrategyBase를 상속받은 실제 구현 클래스만 로드
            - 추상 클래스는 제외
        """
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
        """
        새로운 전략을 추가
        
        Args:
            strategy (StrategyBase): 추가할 전략 객체
            
        Notes:
            - 런타임에 새로운 전략을 동적으로 추가 가능
        """
        self.strategies.append(strategy)
        
    def get_all_strategies(self) -> List[str]:
        """
        모든 전략 목록 가져오기
        
        Returns:
            List[str]: 모든 전략 목록 리스트
        """
        return [strategy.__class__.__name__ for strategy in self.strategies]
        
    def get_decision(self, market_data: Dict[str, Any]) -> str:
        """
        모든 전략의 분석 결과를 취합하여 최종 투자 결정을 생성
        
        Args:
            market_data (Dict[str, Any]): 분석할 시장 데이터
        
        Returns:
            str: 투자 결정
                - "buy": 매수 신호
                - "sell": 매도 신호
                - "hold": 관망 신호
                
        Notes:
            - 각 전략의 결과를 평균하여 최종 신호 강도 계산
            - 임계값을 기준으로 매수/매도/홀딩 결정
            - 전략이 없는 경우 기본값으로 "hold" 반환
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