from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import time
import schedule
from datetime import datetime
import yaml
import logging
from pathlib import Path

class MessengerInterface(ABC):
    @abstractmethod
    def send_message(self, message: str) -> bool:
        pass

class ExchangeInterface(ABC):
    @abstractmethod
    def buy(self, symbol: str, amount: float) -> bool:
        pass
    
    @abstractmethod
    def sell(self, symbol: str, amount: float) -> bool:
        pass
    
    @abstractmethod
    def get_price(self, symbol: str) -> float:
        pass
    
    @abstractmethod
    def check_connection(self) -> bool:
        pass

class InvestmentCenter:
    def __init__(self, exchange_name: str):
        self.config = self._load_config()
        self.exchange = self._initialize_exchange(exchange_name)
        self.messenger = self._initialize_messenger()
        self.logger = self._setup_logger()
        self.is_running = False
        self.scheduled_tasks = []
        
    def _load_config(self) -> Dict:
        """설정 파일 로드"""
        config_path = Path("resource/application.yml")
        try:
            with open(config_path, 'r') as file:
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

    def _initialize_exchange(self, exchange_name: str) -> ExchangeInterface:
        """거래소 초기화"""
        # 실제 거래소 구현체 반환
        pass

    def _initialize_messenger(self) -> MessengerInterface:
        """메신저 초기화"""
        # 실제 메신저 구현체 반환
        pass

    def buy(self, symbol: str, amount: float) -> bool:
        """매수 실행"""
        try:
            if not self._check_api_status():
                return False
                
            result = self.exchange.buy(symbol, amount)
            if result:
                message = f"매수 성공: {symbol}, 수량: {amount}"
                self.messenger.send_message(message)
                self.logger.info(message)
            return result
        except Exception as e:
            self.logger.error(f"매수 실패: {str(e)}")
            self.messenger.send_message(f"매수 실패: {symbol}, 오류: {str(e)}")
            return False

    def sell(self, symbol: str, amount: float) -> bool:
        """매도 실행"""
        try:
            if not self._check_api_status():
                return False
                
            result = self.exchange.sell(symbol, amount)
            if result:
                message = f"매도 성공: {symbol}, 수량: {amount}"
                self.messenger.send_message(message)
                self.logger.info(message)
            return result
        except Exception as e:
            self.logger.error(f"매도 실패: {str(e)}")
            self.messenger.send_message(f"매도 실패: {symbol}, 오류: {str(e)}")
            return False

    def schedule_task(self, task: callable, interval: str) -> None:
        """작업 스케줄링"""
        self.scheduled_tasks.append((task, interval))
        schedule.every().day.at(interval).do(task)

    def start(self) -> None:
        """시스템 시작"""
        self.is_running = True
        self.messenger.send_message("투자 시스템이 시작되었습니다.")
        
        while self.is_running:
            try:
                schedule.run_pending()
                if not self._check_api_status():
                    self._handle_emergency()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"시스템 오류: {str(e)}")
                self._handle_emergency()

    def stop(self) -> None:
        """시스템 중지"""
        self.is_running = False
        self.messenger.send_message("투자 시스템이 중지되었습니다.")

    def _check_api_status(self) -> bool:
        """API 상태 확인"""
        try:
            return self.exchange.check_connection()
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

if __name__ == "__main__":
    # 사용 예시
    center = InvestmentCenter("upbit")
    
    # 스케줄 작업 예시
    def daily_report():
        print("일일 리포트 생성")
    
    center.schedule_task(daily_report, "17:00")
    center.start()
