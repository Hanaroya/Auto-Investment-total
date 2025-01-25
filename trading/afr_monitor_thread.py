import threading
import logging
import asyncio
import time
from datetime import datetime

class AFRMonitorThread(threading.Thread):
    """거래소별 AFR(Aggregated Funding Rate) 모니터링 스레드"""
    
    def __init__(self, investment_center, stop_flag: threading.Event, db_manager):
        super().__init__()
        self.investment_center = investment_center
        self.stop_flag = stop_flag
        self.db = db_manager
        self.logger = logging.getLogger('investment_center')
        self.last_check = {}     # 거래소별 마지막 체크 시간
        self.loop = None         # 비동기 이벤트 루프
        
    def run(self):
        """AFR 모니터링 실행"""
        self.logger.info("AFR 모니터링 스레드 시작")
        
        # 비동기 이벤트 루프 생성
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        while not self.stop_flag.is_set():
            try:
                current_time = datetime.now()
                exchange = self.investment_center.exchange_name
                
                # 마지막 체크 시간 확인
                last_check = self.last_check.get(exchange, datetime.min)
                if (current_time - last_check).total_seconds() < 300:  # 5분 간격
                    time.sleep(1)
                    continue
                
                # 거래소별 AFR 데이터 수집
                market_data = self.loop.run_until_complete(
                    self.investment_center.exchange.get_AFR_value()
                )
                
                if market_data:
                    # market_index 컬렉션에 데이터 업데이트
                    market_data['exchange'] = exchange
                    market_data['timestamp'] = current_time
                    
                    if self.db.update_market_index(market_data):
                        self.last_check[exchange] = current_time
                        self.logger.info(
                            f"{exchange} AFR 데이터 업데이트 - "
                            f"AFR: {market_data.get('AFR', 0):.2f}, "
                            f"변화율: {market_data.get('current_change', 0):.2f}%, "
                            f"공포/탐욕: {market_data.get('fear_and_greed', 50):.1f}"
                        )
                
                time.sleep(60)  # 1분마다 체크
                
            except Exception as e:
                self.logger.error(f"AFR 모니터링 중 오류: {str(e)}")
                time.sleep(60)  # 오류 발생시 1분 대기

        self.logger.info("AFR 모니터링 스레드 종료")
        if self.loop and self.loop.is_running():
            self.loop.close()