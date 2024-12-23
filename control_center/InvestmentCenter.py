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
from trade_market_api.MarketDataConverter import MarketDataConverter

class MessengerInterface(ABC):
    """
    메시지 전송을 위한 인터페이스
    모든 메신저 구현체는 이 인터페이스를 따라야 함
    """
    @abstractmethod
    def send_message(self, message: str) -> bool:
        pass

class ExchangeFactory:
    """
    거래소 객체 생성을 담당하는 팩토리 클래스
    각 거래소별 구현체를 생성하고 설정을 주입
    """
    @staticmethod
    def create_exchange(exchange_name: str, config: Dict) -> Any:
        mode = config.get('mode', 'market')  # 기본 모드는 실제 거래
        if exchange_name.lower() == "upbit":
            if mode == 'test':
                # 테스트 환경: 더미 API 키 사용
                return UpbitCall(
                    access_key="test_access_key",
                    secret_key="test_secret_key"
                )
            # 실제 환경: 설정 파일의 API 키 사용
            return UpbitCall(
                access_key=config['api_keys']['upbit']['access_key'],
                secret_key=config['api_keys']['upbit']['secret_key']
            )
        else:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")

class InvestmentCenter:
    """
    투자 센터 메인 클래스
    거래소 연동, 전략 실행, 주문 처리 등 핵심 기능 담당
    """
    def __init__(self, exchange_name: str):
        """
        초기화 메서드
        Args:
            exchange_name (str): 사용할 거래소 이름 (예: "upbit")
        """
        self.config = self._load_config()  # 설정 파일 로드
        self.mode = self.config.get('mode', 'market')  # 동작 모드 설정
        self.exchange = self._initialize_exchange(exchange_name)  # 거래소 초기화
        self.messenger = self._initialize_messenger()  # 메신저 초기화
        self.logger = self._setup_logger()  # 로거 설정
        self.is_running = False  # 실행 상태 플래그
        self.scheduled_tasks = []  # 예약된 작업 목록
        self.strategy_manager = self._initialize_strategies()  # 전략 관리자 초기화
        
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
        """
        매수 주문 실행
        Args:
            symbol (str): 거래 심볼 (예: "KRW-BTC")
            amount (float): 매수 수량
            price (Optional[float]): 매수 가격 (지정가 주문시)
        Returns:
            bool: 주문 성공 여부
        """
        try:
            # API 상태 확인
            if not self._check_api_status():
                return False
                
            if self.mode == 'test':
                # 테스트 모드: 실제 주문 없이 로그만 기록
                message = f"[테스트 모드] 매수 주문 시뮬레이션: {symbol}, 수량: {amount}"
                self.logger.info(message)
                self.messenger.send_message(message)
                return True

            # 실제 주문 실행
            result = self.exchange.place_order(
                symbol=symbol,
                side="bid",  # bid는 매수
                volume=amount,
                price=price
            )
            
            # 주문 결과 처리
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
        """
        시장 분석 및 투자 결정 수행
        Args:
            symbol (str): 분석할 코인 심볼 (예: "KRW-BTC")
        Returns:
            str: 투자 결정 ("buy", "sell", "hold")
        Notes:
            - 여러 기술적 지표를 종합적으로 분석
            - 리스크 관리를 위한 임계값 적용
            - 이상 징후 발견 시 안전 장치 작동
        """
        try:
            # 시장 데이터 수집
            market_data = self._collect_market_data(symbol)
            # 전략 매니저를 통한 투자 결정
            decision = self.strategy_manager.get_decision(market_data)
            
            # 중요 투자 결정은 경고 레벨로 로깅
            if decision in ["buy", "sell"]:
                self.logger.warning(f"투자 결정 - {symbol}: {decision}")
            else:
                self.logger.info(f"투자 결정 - {symbol}: {decision}")
            
            return decision
            
        except Exception as e:
            self.logger.error(f"시장 분석 실패: {str(e)}")
            return "hold"  # 오류 발생시 안전하게 홀딩
            
    def _collect_market_data(self, symbol: str) -> Dict[str, Any]:
        """시장 데이터 수집
        
        Args:
            symbol (str): 분석할 코인 심볼 (예: "KRW-BTC")
        Returns:
            Dict[str, Any]: 수집된 시장 데이터
        Notes:
            - 캔들 데이터를 조회하여 시장 데이터 수집
            - 데이터 변환 후 반환
        """
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
    """
    투자 센터 초기화 및 스케줄 작업 예시
    """
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
