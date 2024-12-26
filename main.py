import asyncio
import logging
from datetime import datetime, timedelta
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from trading.thread_manager import ThreadManager
from database.mongodb_manager import MongoDBManager
from messenger.Messenger import Messenger
from utils.scheduler import Scheduler
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# .env 파일 로드
load_dotenv()

class CryptoTradingBot:
    def __init__(self):
        # 로거 설정
        self.logger = logging.getLogger(__name__)
        
        # config 로드
        with open('resource/application.yml', 'r', encoding='utf-8') as f:
            import yaml
            self.config = yaml.safe_load(f)
        
        # 이벤트 루프 생성
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        self.db = MongoDBManager()
        self.messenger = Messenger(self.config)
        self.market_analyzer = MarketAnalyzer(self.config)
        self.trading_manager = TradingManager()
        self.thread_manager = ThreadManager(
            config=self.config
        )
        self.scheduler = Scheduler()
        
    async def initialize(self):
        """초기화"""
        try:
            self.logger.info("초기화 시작...")
            
            # MongoDB 연결 테스트 - await 제거
            if not self.db.test_connection():  # 동기식 호출
                raise Exception("MongoDB 초기화 실패")
            
            # 시스템 설정 업데이트
            self.db.update_system_config({
                'last_start_time': datetime.utcnow(),
                'status': 'initializing'
            })
            
            # 메신저로 시작 메시지 전송
            await self.messenger.send_message("자동 거래를 시작합니다.")
        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {str(e)}")
            raise Exception("MongoDB 초기화 실패")

    async def start(self):
        """봇 시작"""
        try:
            await self.initialize()
            
            # 코인 시장 정보 수집 및 정렬
            markets = await self.market_analyzer.get_sorted_markets()
            if not markets:
                await self.messenger.send_message("마켓 정보를 가져오는데 실패했습니다.")
                return
            
            self.logger.info(f"총 {len(markets)}개의 마켓 분석을 시작합니다.")
            await self.messenger.send_message(f"총 {len(markets)}개의 마켓 분석을 시작합니다.")
            
            # 스레드 매니저 시작
            await self.thread_manager.start_threads(markets)
            
            # 스케줄러 시작
            asyncio.create_task(self.scheduler.start())
            
            # 일일/시간별 리포트 스케줄러 설정
            await self.scheduler.schedule_task(
                'daily_report',
                self.trading_manager.generate_daily_report,
                cron='0 20 * * *',
                immediate=False
            )
            
            await self.scheduler.schedule_task(
                'hourly_report',
                self.trading_manager.generate_hourly_report,
                cron='0 * * * *',
                immediate=False
            )

            # 메인 루프
            while True:
                try:
                    # 스레드 상태 체크
                    thread_health = await self.thread_manager.check_thread_health()
                    if not thread_health:
                        self.logger.warning("마켓 데이터 업데이트 지연 감지")
                        await self.messenger.send_message("마켓 데이터 업데이트가 지연되고 있습니다.")
                    
                    # 활성 거래 상태 체크
                    active_trades = self.trading_manager.get_active_trades()
                    self.logger.info(f"현재 활성 거래: {len(active_trades)}건")
                    
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
            await self.thread_manager.stop_all_threads()
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