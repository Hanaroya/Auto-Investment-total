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

# 환경 변수 사용 예시
mongo_username = os.getenv('MONGO_ROOT_USERNAME')
mongo_password = os.getenv('MONGO_ROOT_PASSWORD')

class CryptoTradingBot:
    def __init__(self):
        self.db = MongoDBManager()
        self.messenger = Messenger()
        self.market_analyzer = MarketAnalyzer()
        self.trading_manager = TradingManager()
        self.thread_manager = ThreadManager()
        self.scheduler = Scheduler()
        
    async def initialize(self):
        # 시스템 설정 초기화
        await self.db.update_system_config({
            'initial_investment': 1000000,
            'min_trade_amount': 5000,
            'max_thread_investment': 80000,
            'reserve_amount': 200000,
            'total_max_investment': 800000,
            'emergency_stop': False
        })
        
        # 메신저로 시작 메시지 전송
        await self.messenger.send_message("자동 거래를 시작합니다.")

    async def start(self):
        try:
            await self.initialize()
            
            # 코인 시장 정보 수집 및 정렬
            markets = await self.market_analyzer.get_sorted_markets()
            
            # 스레드 매니저 시작
            await self.thread_manager.start(markets)
            
            # 일일 리포트 스케줄러 설정
            self.scheduler.schedule_daily_report(self.trading_manager.generate_daily_report)
            
            # 메인 루프
            while True:
                if await self.check_emergency_stop():
                    await self.emergency_stop()
                    break
                    
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            await self.emergency_stop()

    async def check_emergency_stop(self):
        config = await self.db.get_system_config()
        return config.get('emergency_stop', False)

    async def emergency_stop(self):
        logger.info("Emergency stop initiated")
        await self.thread_manager.stop_all_threads()
        await self.trading_manager.sell_all_positions()
        await self.messenger.send_message("긴급 정지: 모든 포지션이 정리되었습니다.")

if __name__ == "__main__":
    bot = CryptoTradingBot()
    asyncio.run(bot.start()) 