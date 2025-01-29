import os
import sys

# 프로젝트 루트 디렉토리를 파이썬 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

import logging
import threading
from trade_market_api.UpbitCall import UpbitCall
import json
from datetime import datetime
import pandas as pd
import asyncio

async def test_feargreed_data():
    # 로깅 설정을 DEBUG로 변경
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("FearGreedTest")
    
    # 설정 로드
    with open('resource/application.yml', 'r', encoding='utf-8') as f:
        import yaml
        config = yaml.safe_load(f)
    
    # UpbitCall 인스턴스 생성
    upbit = UpbitCall(
        config['api_keys']['upbit']['access_key'],
        config['api_keys']['upbit']['secret_key']
    )
    
    logger.info(f"\n{'='*50}\nFear & Greed 데이터 테스트\n{'='*50}")
    
    try:
        # Fear & Greed 데이터 조회
        feargreed_data = await upbit.get_feargreed_data()
        
        if not feargreed_data:
            logger.error("Fear & Greed 데이터 없음")
            return
            
        # 데이터 기본 정보
        logger.info(f"수신된 데이터 개수: {len(feargreed_data)}")
        
        # 데이터 구조 출력
        logger.info("\n전체 Fear & Greed 데이터:")
        for i, data in enumerate(feargreed_data[:5]):  # 처음 5개만 출력
            logger.info(f"\n데이터 {i+1}:")
            logger.info(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        
        # DataFrame으로 변환
        df = pd.DataFrame(feargreed_data)
        
        # 컬럼 정보 출력
        logger.info("\n데이터 컬럼:")
        logger.info(df.columns.tolist())
        
        # 기본 통계 (숫자형 컬럼만)
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        if not numeric_cols.empty:
            logger.info("\n기본 통계:")
            logger.info(df[numeric_cols].describe())
        
        # 데이터 타입 정보
        logger.info("\n데이터 타입 정보:")
        logger.info(df.dtypes)
        
    except Exception as e:
        logger.error(f"Fear & Greed 데이터 처리 중 오류 발생: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("\n" + "="*50 + "\n")

async def test_candle_data():
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("CandleTest")
    
    # 설정 로드
    with open('resource/application.yml', 'r', encoding='utf-8') as f:
        import yaml
        config = yaml.safe_load(f)
    
    # UpbitCall 인스턴스 생성
    upbit = UpbitCall(
        config['api_keys']['upbit']['access_key'],
        config['api_keys']['upbit']['secret_key']
    )
    
    # 테스트할 코인
    test_coins = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']
    
    for coin in test_coins:
        logger.info(f"\n{'='*50}\n테스트 코인: {coin}\n{'='*50}")
        
        try:
            # 캔들 데이터 조회
            candles = upbit.get_candle(market=coin, interval='1', count=300)
            
            if not candles:
                logger.error(f"{coin}: 캔들 데이터 없음")
                continue
                
            # 데이터 기본 정보
            logger.info(f"수신된 캔들 개수: {len(candles) if isinstance(candles, list) else 1}")
            
            # 데이터 구조 출력
            logger.info("\n전체 캔들 데이터:")
            if isinstance(candles, list):
                for i, candle in enumerate(candles):
                    logger.info(f"\n캔들 {i+1}:")
                    logger.info(json.dumps(candle, indent=2, ensure_ascii=False))
            else:
                logger.info(json.dumps(candles, indent=2, ensure_ascii=False))
            
            # DataFrame으로 변환
            df = pd.DataFrame([candles] if not isinstance(candles, list) else candles)
            
            # 컬럼 정보 출력
            logger.info("\n데이터 컬럼:")
            logger.info(df.columns.tolist())
            
            # 기본 통계 (숫자형 컬럼만)
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            if not numeric_cols.empty:
                logger.info("\n기본 통계:")
                logger.info(df[numeric_cols].describe())
            
            # 데이터 타입 정보
            logger.info("\n데이터 타입 정보:")
            logger.info(df.dtypes)
            
        except Exception as e:
            logger.error(f"{coin} 처리 중 오류 발생: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        logger.info("\n" + "="*50 + "\n")

if __name__ == "__main__":
    # 두 테스트 함수 모두 실행
    async def run_tests():
        # await test_candle_data()
        await test_feargreed_data()
    
    asyncio.run(run_tests()) 