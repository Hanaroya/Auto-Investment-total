import asyncio
import logging
from pathlib import Path
from trade_market_api.UpbitCall import UpbitCall
import aiohttp
from urllib.parse import urlencode

async def test_get_candle():
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("UpbitCandleTest")

    # 업비트 API 인스턴스 생성 (테스트 모드)
    upbit = UpbitCall("your_access_key", "your_secret_key", is_test=True)
    
    # aiohttp 세션 생성
    async with aiohttp.ClientSession() as session:
        upbit.session = session
        
        # 테스트할 마켓 목록
        test_markets = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
        
        for market in test_markets:
            logger.info(f"\n테스트 시작: {market}")
            
            try:
                # URL 생성 및 로깅
                base_url = 'https://crix-api-endpoint.upbit.com/v1/crix/candles'
                params = {
                    'code': f"CRIX.UPBIT.{market}",
                    'count': 200
                }
                url = f"{base_url}/minutes/1?{urlencode(params)}"
                logger.info(f"요청 URL: {url}")
                
                # 캔들 데이터 조회
                candles = await upbit.get_candle(market=market, interval='1', count=200)
                
                if candles is None:
                    logger.error(f"{market}: 캔들 데이터 조회 실패")
                    continue
                    
                logger.info(f"{market}: 캔들 데이터 수신 성공 (개수: {len(candles)})")
                
                # 첫 번째 캔들 데이터 출력
                if candles:
                    logger.info(f"첫 번째 캔들 데이터: {candles[0]}")
                
            except Exception as e:
                logger.error(f"{market} 처리 중 에러 발생: {str(e)}")
            
            # API 호출 간격 유지
            await asyncio.sleep(0.1)

if __name__ == "__main__":
    # 이벤트 루프 생성 및 실행
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_get_candle()) 