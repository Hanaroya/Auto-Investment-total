import asyncio
import logging
from datetime import datetime, timezone, timedelta
from control_center.InvestmentCenter import InvestmentCenter
from database.mongodb_manager import MongoDBManager
from dotenv import load_dotenv

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('investment-center')

# .env 파일 로드
load_dotenv()

class CryptoTradingBot:
    def __init__(self):
        # 로거 설정
        self.logger = logging.getLogger('investment-center')
        
        # config 로드
        with open('resource/application.yml', 'r', encoding='utf-8') as f:
            import yaml
            self.config = yaml.safe_load(f)
        
        # 이벤트 루프 생성
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # MongoDB 초기화
        self.db = MongoDBManager()
        
        # InvestmentCenter 초기화 (기본값으로 upbit 사용)
        self.investment_center = InvestmentCenter("upbit")
        
    async def initialize(self):
        """초기화"""
        try:
            self.logger.info("초기화 시작...")
            
            # KST 시간대 설정
            KST = timezone(timedelta(hours=9))
            
            # 초기화 정보 저장
            init_info = {
                'last_start_time': datetime.now(KST),  # 한국 시간으로 설정
                'status': 'initializing'
            }
            
            # MongoDB 연결 테스트
            if not self.db.test_connection():
                raise Exception("MongoDB 초기화 실패")
            
            # 메신저로 시작 메시지 전송
            await self.investment_center.messenger.send_message(message="자동 거래를 시작합니다.", messenger_type="slack", channel="general")
            
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