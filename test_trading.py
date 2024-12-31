import asyncio
import logging
from datetime import datetime
from trading.market_analyzer import MarketAnalyzer
from trading.trading_manager import TradingManager
from trading.thread_manager import ThreadManager
from database.mongodb_manager import MongoDBManager
from messenger.Messenger import Messenger
from trade_market_api.UpbitCall import UpbitCall
import yaml
from pathlib import Path
from logging.handlers import RotatingFileHandler

# 로그 디렉토리 생성
log_dir = Path('log')
log_dir.mkdir(exist_ok=True)

# 로그 파일 설정
log_file = log_dir / f"test_trading_{datetime.now().strftime('%Y%m%d')}.log"

# 로그 핸들러 설정
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)

# 로그 포맷 설정
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)

# 루트 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# 콘솔 출력을 위한 스트림 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class TestTradingBot:
    def __init__(self):
        self.config = self._load_config()
        self.db = MongoDBManager()
        self.messenger = Messenger(self.config)
        self.market_analyzer = MarketAnalyzer()
        self.trading_manager = TradingManager()
        self.thread_manager = ThreadManager(
            config=self.config
        )
        self.upbit = UpbitCall(
            self.config['api_keys']['upbit']['access_key'],
            self.config['api_keys']['upbit']['secret_key'],
            is_test=True
        )

    def _load_config(self):
        """설정 파일 로드"""
        config_path = Path('resource/application.yml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    async def test_single_market(self, market: str):
        """단일 마켓 테스트"""
        try:
            logger.info(f"테스트 시작: {market}")
            
            # 캔들 데이터 조회
            candles = await self.upbit.get_candle(market, '5', 200)
            if not candles:
                logger.warning(f"{market}: 충분한 캔들 데이터 없음")
                return

            # 캔들 데이터 로깅 (디버깅)
            logger.debug(f"캔들 데이터 샘플: {candles[0] if candles else 'None'}")

            # 시장 분석
            analysis = await self.market_analyzer.analyze_market(market, candles)
            logger.info(f"분석 결과: {analysis}")

            # 매수 신호 확인
            if analysis['action'] == 'buy' and analysis['strength'] >= 0.65:
                logger.info(f"매수 신호 감지: {market}")
                message = f"테스트 매수 신호\n"
                message += f"코인: {market}\n"
                message += f"강도: {analysis['strength']}\n"
                message += f"가격: {analysis['price']}\n"
                message += f"전략 데이터: {analysis['strategy_data']}"
                await self.messenger.send_message(
                    message=message,
                    messenger_type="slack"
                )

            # 테스트 결과 저장
            trade_data = {
                'market': market,
                'timestamp': datetime.now(),
                'price': analysis['price'],
                'action': analysis['action'],
                'strength': analysis.get('strength', 0),
                'strategy_data': analysis['strategy_data'],
                'status': 'test'
            }
            
            try:
                # 동기식으로 MongoDB 저장
                self.db.trades.insert_one(trade_data)
                logger.info(f"{market} 거래 데이터 저장 완료")
            except Exception as e:
                logger.error(f"거래 데이터 저장 실패: {str(e)}")

        except Exception as e:
            logger.error(f"{market} 테스트 중 오류: {str(e)}", exc_info=True)

    async def run_test(self):
        """테스트 실행"""
        try:
            logger.info("테스트 모드 시작")
            
            # 원화 마켓 목록 조회
            markets = await self.upbit.get_krw_markets()
            logger.info(f"조회된 마켓: {markets[:5]}")  # 처음 5개만 로깅
            
            if not markets:
                logger.error("마켓 데이터가 비어있습니다")
                return

            # 상위 9개 마켓만 선택 (이미 거래량 순으로 정렬되어 있음)
            test_markets = markets[:9]
            
            logger.info(f"테스트 대상 마켓: {test_markets}")
            
            for market in test_markets:
                try:
                    await self.test_single_market(market)
                except Exception as e:
                    logger.error(f"{market} 테스트 중 오류: {str(e)}")
                await asyncio.sleep(0.1)

            logger.info("테스트 완료")

        except Exception as e:
            logger.error(f"테스트 실행 중 오류: {str(e)}", exc_info=True)
        finally:
            await self.messenger.send_message(message="테스트 완료", messenger_type="slack")
            if hasattr(self, 'db'):
                self.db.close()

if __name__ == "__main__":
    bot = TestTradingBot()
    asyncio.run(bot.run_test()) 