from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import time
import schedule
from datetime import datetime
import yaml
import logging
from pathlib import Path
from trade_market_api.UpbitCall import UpbitCall
from messenger.Messenger import Messenger
from strategy.StrategyBase import StrategyManager
from strategy.Strategies import *

class MessengerInterface(ABC):
    @abstractmethod
    def send_message(self, message: str) -> bool:
        pass

class ExchangeFactory:
    @staticmethod
    def create_exchange(exchange_name: str, config: Dict) -> Any:
        if exchange_name.lower() == "upbit":
            return UpbitCall(
                access_key=config['api_keys']['upbit']['access_key'],
                secret_key=config['api_keys']['upbit']['secret_key']
            )
        # 다른 거래소들 추가 가능
        else:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")

class InvestmentCenter:
    def __init__(self, exchange_name: str):
        self.config = self._load_config()
        self.exchange = self._initialize_exchange(exchange_name)
        self.messenger = self._initialize_messenger()
        self.logger = self._setup_logger()
        self.is_running = False
        self.scheduled_tasks = []
        self.strategy_manager = self._initialize_strategies()
        
    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        config_path = Path("resource/application.yml")
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise RuntimeError(f"설정 파일 로드 실패: {str(e)}")

    def _setup_logger(self) -> logging.Logger:
        """로깅 설정"""
        logger = logging.getLogger('InvestmentCenter')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('investment.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _initialize_exchange(self, exchange_name: str) -> Any:
        """거래소 초기화"""
        try:
            exchange = ExchangeFactory.create_exchange(exchange_name, self.config)
            self.logger.info(f"{exchange_name} 거래소 초기화 성공")
            return exchange
        except Exception as e:
            self.logger.error(f"거래소 초기화 실패: {str(e)}")
            raise

    def _initialize_messenger(self) -> Messenger:
        """메신저 초기화"""
        try:
            messenger = Messenger(self.config)
            self.logger.info("메신저 초기화 성공")
            return messenger
        except Exception as e:
            self.logger.error(f"메신저 초기화 실패: {str(e)}")
            raise

    def _initialize_strategies(self) -> StrategyManager:
        """전략 초기화"""
        manager = StrategyManager()
        
        # 기본 전략들 추가
        strategies = [
            RSIStrategy(),
            MACDStrategy(),
            BollingerBandStrategy(),
            VolumeStrategy(),
            PriceChangeStrategy(),
            MovingAverageStrategy(),
            MomentumStrategy(),
            StochasticStrategy(),
            IchimokuStrategy(),
            MarketSentimentStrategy()
        ]
        
        for strategy in strategies:
            manager.add_strategy(strategy)
            
        return manager
        
    def buy(self, symbol: str, amount: float, price: Optional[float] = None) -> bool:
        """매수 실행"""
        try:
            if not self._check_api_status():
                return False
                
            result = self.exchange.place_order(
                symbol=symbol,
                side="bid",
                volume=amount,
                price=price
            )
            
            if result and 'uuid' in result:
                message = f"매수 주문 성공: {symbol}, 수량: {amount}"
                self.messenger.send_message(message)
                self.logger.info(message)
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"매수 실패: {str(e)}")
            self.messenger.send_message(f"매수 실패: {symbol}, 오류: {str(e)}")
            return False

    def sell(self, symbol: str, amount: float, price: Optional[float] = None) -> bool:
        """매도 실행"""
        try:
            if not self._check_api_status():
                return False
                
            result = self.exchange.place_order(
                symbol=symbol,
                side="ask",
                volume=amount,
                price=price
            )
            
            if result and 'uuid' in result:
                message = f"매도 주문 성공: {symbol}, 수량: {amount}"
                self.messenger.send_message(message)
                self.logger.info(message)
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"매도 실패: {str(e)}")
            self.messenger.send_message(f"매도 실패: {symbol}, 오류: {str(e)}")
            return False

    def _check_api_status(self) -> bool:
        """API 상태 확인"""
        try:
            # 간단한 API 호출로 상태 확인
            markets = self.exchange.get_krw_markets()
            return bool(markets)
        except Exception:
            return False

    def _handle_emergency(self) -> None:
        """비상 상황 처리"""
        self.logger.warning("비상 상황 발생: API 연결 실패")
        self.messenger.send_message("⚠️ 거래소 API 연결 실패. 시스템 일시 중지.")
        
        while not self._check_api_status():
            self.logger.info("API 재연결 시도 중...")
            time.sleep(60)  # 1분마다 재시도
            
        self.logger.info("API 재연결 성공")
        self.messenger.send_message("✅ API 재연결 성공. 시스템 재개.")

    def analyze_market(self, symbol: str) -> str:
        """시장 분석 및 투자 결정"""
        try:
            # 시장 데이터 수집
            market_data = self._collect_market_data(symbol)
            
            # 전략 분석 결과 획득
            decision = self.strategy_manager.get_decision(market_data)
            
            self.logger.info(f"투자 결정 - {symbol}: {decision}")
            return decision
            
        except Exception as e:
            self.logger.error(f"시장 분석 실패: {str(e)}")
            return "hold"
            
    def _collect_market_data(self, symbol: str) -> Dict[str, Any]:
        """시장 데이터 수집"""
        # 실제 구현에서는 각 전략에 필요한 데이터를 수집
        pass

if __name__ == "__main__":
    # 사용 예시
    try:
        center = InvestmentCenter("upbit")
        print("투자 센터 초기화 성공")
        
        # 스케줄 작업 예시
        def daily_report():
            print("일일 리포트 생성")
        
        center.schedule_task(daily_report, "17:00")
        center.start()
        
    except Exception as e:
        print(f"초기화 실패: {str(e)}")
