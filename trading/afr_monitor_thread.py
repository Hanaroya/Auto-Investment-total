import threading
import logging
import asyncio
import time
from datetime import datetime, timedelta, timezone
from monitoring.memory_monitor import MemoryProfiler, memory_profiler
class AFRMonitorThread(threading.Thread):
    """거래소별 AFR(Aggregated Funding Rate) 모니터링 스레드"""
    
    def __init__(self, investment_center, stop_flag: threading.Event, db_manager, afr_ready: threading.Event):
        super().__init__()
        self.investment_center = investment_center
        self.stop_flag = stop_flag
        self.db = db_manager
        self.afr_ready = afr_ready  # AFR 준비 이벤트 추가
        self.logger = logging.getLogger('investment_center')
        self.last_check = {}     # 거래소별 마지막 체크 시간
        self.loop = None         # 비동기 이벤트 루프
        self.memory_profiler = MemoryProfiler()

    
    def run(self):
        """AFR 모니터링 실행"""
        self.logger.info("AFR 모니터링 스레드 시작")
        
        try:
            # 비동기 이벤트 루프 생성
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            while not self.stop_flag.is_set():
                try:
                    current_time = datetime.now(timezone(timedelta(hours=9)))
                    exchange = self.investment_center.exchange_name
                    
                    # 마지막 체크 시간을 timezone-aware로 변환
                    last_check = self.last_check.get(exchange)
                    if last_check and last_check.tzinfo is None:
                        last_check = last_check.replace(tzinfo=timezone(timedelta(hours=9)))
                    else:
                        last_check = datetime.min.replace(tzinfo=timezone(timedelta(hours=9)))
                    
                    # 5분 간격 체크
                    if last_check and (current_time - last_check).total_seconds() < 300:
                        time.sleep(1)
                        continue
                    
                    # AFR 데이터 수집
                    market_data = self.loop.run_until_complete(
                        self.investment_center.exchange.get_AFR_value()
                    )
                    
                    if market_data:
                        # 기존 데이터 조회
                        existing_data = self.db.market_index.find_one(
                            {'exchange': exchange},
                            sort=[('last_updated', -1)]
                        )
                        
                        # 리스트 데이터 준비
                        afr_list = existing_data.get('AFR', [])[-19:] if existing_data else []
                        change_list = existing_data.get('current_change', [])[-19:] if existing_data else []
                        fear_greed_list = existing_data.get('fear_and_greed', [])[-19:] if existing_data else []
                        
                        # 새 값 추가
                        afr_list.append(market_data['AFR'])
                        change_list.append(market_data['current_change'])
                        fear_greed_list.append(market_data['fear_and_greed'])
                        
                        # 데이터 저장
                        formatted_data = {
                            'exchange': exchange,
                            'AFR': afr_list,
                            'current_change': change_list,
                            'fear_and_greed': fear_greed_list,
                            'market_feargreed': market_data['market_feargreed'],
                            'last_updated': current_time
                        }
                        
                        if self.db.update_market_index(formatted_data):
                            self.last_check[exchange] = current_time
                            self.logger.info(
                                f"{exchange} AFR 데이터 업데이트 - "
                                f"AFR: {market_data['AFR']:.2f}, "
                                f"변화율: {market_data['current_change']:.2f}%, "
                                f"공포/탐욕: {market_data['fear_and_greed']:.1f}"
                            )
                            
                            if not self.afr_ready.is_set():
                                self.afr_ready.set()
                    
                    time.sleep(60)  # 1분마다 체크
                    
                except Exception as e:
                    self.logger.error(f"AFR 모니터링 중 오류: {str(e)}")
                    time.sleep(60)
                    
        except Exception as e:
            self.logger.error(f"AFR 모니터링 스레드 오류: {str(e)}")
        finally:
            if self.loop and self.loop.is_running():
                self.loop.close()