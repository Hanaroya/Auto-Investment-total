import asyncio
import logging
from datetime import datetime
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
        # config 로드
        with open('resource/application.yml', 'r', encoding='utf-8') as f:
            import yaml
            self.config = yaml.safe_load(f)
        
        self.db = MongoDBManager()
        self.messenger = Messenger(self.config)  # config 전달
        self.market_analyzer = MarketAnalyzer(self.config)
        self.trading_manager = TradingManager()
        self.thread_manager = ThreadManager(self.config)
        self.scheduler = Scheduler()
        
    async def initialize(self):
        """시스템 초기화"""
        try:
            self.db.update_system_config({
                'last_start_time': datetime.utcnow(),
                'status': 'initializing'
            })
            # 메신저로 시작 메시지 전송
            await self.messenger.send_message("자동 거래를 시작합니다.")
        except Exception as e:
            logging.error(f"초기화 중 오류 발생: {str(e)}")
            raise

    async def start(self):
        """봇 시작"""
        try:
            await self.initialize()
            
            # 코인 시장 정보 수집 및 정렬 (이미 있는 메서드 사용)
            markets = await self.market_analyzer.get_sorted_markets()
            if not markets:
                await self.messenger.send_message("마켓 정보를 가져오는데 실패했습니다.")
                return
            
            await self.messenger.send_message(f"총 {len(markets)}개의 마켓 분석을 시작합니다.")
            
            # 스레드 매니저 시작
            await self.thread_manager.start(markets)
            
            # 일일 리포트 스케줄러 설정
            self.scheduler.schedule_daily_report(self.trading_manager.generate_daily_report)
            
            # 메인 루프
            while True:
                emergency_stop = await self.check_emergency_stop()
                if emergency_stop:
                    await self.emergency_stop()
                    break
                    
                await asyncio.sleep(1)
                
        except Exception as e:
            error_msg = f"Error in main loop: {str(e)}"
            logging.error(error_msg)
            await self.messenger.send_message(error_msg)
            await self.emergency_stop()

    async def check_emergency_stop(self):
        """긴급 정지 확인"""
        try:
            # get_system_config가 동기 함수이므로 await 제거
            config = self.db.get_system_config()
            return config.get('emergency_stop', False)
        except Exception as e:
            logging.error(f"긴급 정지 확인 중 오류: {e}")
            return True

    async def emergency_stop(self):
        """긴급 정지 처리"""
        logging.info("Emergency stop initiated")
        try:
            # 스레드 정지
            await self.thread_manager.stop_all_threads()
            
            # 포지션 정리 (sell_all_positions 대신 올바른 메서드 사용)
            await self.trading_manager.close_all_positions()
            
            # 시스템 상태 업데이트
            self.db.update_system_config({
                'status': 'stopped',
                'last_stop_time': datetime.utcnow()
            })
        except Exception as e:
            logging.error(f"Emergency stop failed: {str(e)}")
            raise

if __name__ == "__main__":
    bot = CryptoTradingBot()
    asyncio.run(bot.start()) 