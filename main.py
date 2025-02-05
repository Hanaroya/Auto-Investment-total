import os
import sys
import yaml
import asyncio
import logging
from pathlib import Path
from control_center.InvestmentCenter import InvestmentCenter
from database.mongodb_manager import MongoDBManager
from dotenv import load_dotenv
from utils.scheduler import SimpleScheduler
from utils.logger_config import setup_logger
from utils.time_utils import TimeUtils

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('investment-center')

# .env 파일의 절대 경로 설정
env_path = Path(__file__).parent / '.env'

# .env 파일 로드
load_dotenv(dotenv_path=env_path)

class CryptoTradingBot:
    def __init__(self):
        # 로거 설정
        self.logger = logging.getLogger('investment-center')
        
        # config 로드
        with open('resource/application.yml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 이벤트 루프 생성
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # MongoDB 초기화
        self.db = MongoDBManager()
        
        # InvestmentCenter 초기화 (기본값으로 upbit 사용)
        self.investment_center = InvestmentCenter("upbit")
        
        self.scheduler = SimpleScheduler()
        
    async def initialize(self):
        """초기화"""
        try:
            self.logger.info("초기화 시작...")
                        
            # 초기화 정보 저장
            init_info = {
                'last_start_time': TimeUtils.get_current_kst(),  # KST 시간으로 설정
                'status': 'initializing'
            }
            
            # MongoDB 연결 테스트
            if not self.db.test_connection():
                raise Exception("MongoDB 초기화 실패")
            # 스케줄러 초기화 및 작업 등록
            self.logger.info("스케줄러 초기화 시작...")
            
            # 시간별 리포트 - 매시 정각에 실행
            self.scheduler.schedule_task(
                'hourly_report',
                self.investment_center.trading_manager.generate_hourly_report,
                exchange=self.investment_center.exchange_name,
                minute=0
            )
            
            # 일일 리포트 - 매일 20시에 실행
            self.scheduler.schedule_task(
                'daily_report',
                self.investment_center.trading_manager.generate_daily_report,
                exchange=self.investment_center.exchange_name,
                hour=20,
                minute=0
            )
            
            # 코인 목록 재분배 - 4시간마다 실행 (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
            self.scheduler.schedule_task(
                'market_redistribution',
                self.investment_center.thread_manager.update_market_distribution,
                exchange=self.investment_center.exchange_name,
                hour=-1,  # 매시간 체크
                minute=0  # 정각에 실행
            )

            # 최저가 초기화 스캐쥴 - 매일 아침 9시 혹은 거래 시작시 실행    
            self.scheduler.schedule_task(
                'lowest_price_initialization',
                self.investment_center.trading_manager.initialize_lowest_price,
                exchange=self.investment_center.exchange_name,  
                hour=9,
                minute=0
            )
            
            # 스케줄러 스레드 시작
            self.investment_center.thread_manager.start_scheduler(self.scheduler)
            self.logger.info("스케줄러 작업 등록 완료")
            self.logger.debug("초기화 완료")
            # 메신저로 시작 메시지 전송
            self.investment_center.messenger.send_message(message="자동 거래를 시작합니다.", messenger_type="slack")

        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {str(e)}")
            raise

    async def start(self):
        """봇 시작"""
        try:
            await self.initialize()
            
            # 투자 센터 실행 (비동기로 실행)
            await self.investment_center.start()
            
            # 메인 루프
            while True:
                try:
                    # API 상태 체크
                    if not self.investment_center._check_api_status():
                        self.logger.warning("API 연결 상태 불안정")
                        self.investment_center._handle_emergency()
                    
                    await asyncio.sleep(60)  # 1분 대기
                    
                except Exception as e:
                    self.logger.error(f"메인 루프 실행 중 오류: {str(e)}", exc_info=True)
                    await asyncio.sleep(5)
                    
        except KeyboardInterrupt:
            self.logger.info("프로그램 종료 요청")
            await self.cleanup()
        except Exception as e:
            self.logger.error(f"봇 실행 중 오류 발생: {str(e)}", exc_info=True)
            await self.cleanup()

    async def cleanup(self):
        """종료 시 정리 작업"""
        try:
            # InvestmentCenter 종료
            self.investment_center.stop()
            self.db.close()
            self.logger.info("정리 작업 완료")
        except Exception as e:
            self.logger.error(f"정리 작업 중 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    bot = CryptoTradingBot()
    try:
        # 단일 이벤트 루프에서 실행
        asyncio.set_event_loop(bot.loop)
        bot.loop.run_until_complete(bot.start())
        bot.loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # cleanup을 동기적으로 실행
        bot.loop.run_until_complete(bot.cleanup())
        bot.loop.close() 