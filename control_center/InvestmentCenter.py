from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import time
import schedule
from datetime import datetime
import yaml
import logging
from pathlib import Path
from trade_market_api.UpbitCall import UpbitCall
from messenger.messenger import Messenger
from strategy.StrategyBase import StrategyManager
from strategy.Strategies import *
from trade_market_api.MarketDataConverter import MarketDataConverter

class MessengerInterface(ABC):
    @abstractmethod
    def send_message(self, message: str) -> bool:
        pass

class ExchangeFactory:
    @staticmethod
    def create_exchange(exchange_name: str, config: Dict) -> Any:
        mode = config.get('mode', 'market')  # 기본값은 'market'
        if exchange_name.lower() == "upbit":
            if mode == 'test':
                # 테스트 모드에서는 API 키 검증 스킵
                return UpbitCall(
                    access_key="test_access_key",
                    secret_key="test_secret_key"
                )
            return UpbitCall(
                access_key=config['api_keys']['upbit']['access_key'],
                secret_key=config['api_keys']['upbit']['secret_key']
            )
        else:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")

class InvestmentCenter:
    def __init__(self, exchange_name: str):
        self.config = self._load_config()
        self.mode = self.config.get('mode', 'market')
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
        """로깅 설정
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - 투자 결정은 WARNING 레벨로 기록
            - 시장 데이터 분석은 INFO 레벨로 처리
            - 디버그 모드에서는 모든 로그 저장
        """
        logger = logging.getLogger('InvestmentCenter')
        logger.setLevel(logging.DEBUG if self.mode == 'test' else logging.INFO)
        
        log_dir = Path(self.config.get('logging', {}).get('directory', 'log'))
        log_dir.mkdir(exist_ok=True)
        
        today = datetime.now().strftime('%Y-%m-%d')
        handler = logging.FileHandler(f'{log_dir}/{today}-investment.log')
        
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
                
            if self.mode == 'test':
                # 테스트 모드에서는 로그만 남기고 성공으로 처리
                message = f"[테스트 모드] 매수 주문 시뮬레이션: {symbol}, 수량: {amount}"
                self.logger.info(message)
                self.messenger.send_message(message)
                return True

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
                
            if self.mode == 'test':
                # 테스트 모드에서는 로그만 남기고 성공으로 처리
                message = f"[테스트 모드] 매도 주문 시뮬레이션: {symbol}, 수량: {amount}"
                self.logger.info(message)
                self.messenger.send_message(message)
                return True

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
        """시장 분석 및 투자 결정
        
        Args:
            symbol (str): 분석할 코인 심볼 (예: "KRW-BTC")
            
        Returns:
            str: 투자 결정 ("buy", "sell", "hold")
            
        Notes:
            - 여러 전략의 분석 결과를 종합하여 결정
            - 각 전략의 가중치는 동일하게 적용
            - 임계값을 넘는 경우에만 매수/매도 결정
        """
        try:
            market_data = self._collect_market_data(symbol)
            decision = self.strategy_manager.get_decision(market_data)
            
            # 중요 결정은 WARNING 레벨로 로깅
            if decision in ["buy", "sell"]:
                self.logger.warning(f"투자 결정 - {symbol}: {decision}")
            else:
                self.logger.info(f"투자 결정 - {symbol}: {decision}")
            
            return decision
            
        except Exception as e:
            self.logger.error(f"시장 분석 실패: {str(e)}")
            return "hold"
            
    def _collect_market_data(self, symbol: str) -> Dict[str, Any]:
        """시장 데이터 수집"""
        try:
            # 캔들 데이터 조회
            candle_data = self.exchange.get_candles(symbol, interval="1m", count=200)
            
            # 데이터 변환
            converter = MarketDataConverter()
            market_data = converter.convert_upbit_candle(candle_data)
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"시장 데이터 수집 실패: {str(e)}")
            return {}

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
