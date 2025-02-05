import sys
import os
import logging
from datetime import datetime
import pytz

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trading.trading_manager import TradingManager

def setup_logger():
    """로거 설정"""
    logger = logging.getLogger('investment-center')
    logger.setLevel(logging.DEBUG)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def test_daily_report():
    """일일 리포트 생성 테스트"""
    logger = setup_logger()
    logger.info("일일 리포트 생성 테스트 시작")
    
    try:
        # TradingManager 인스턴스 생성
        trading_manager = TradingManager("upbit")
        
        # 일일 리포트 생성
        trading_manager.generate_daily_report()
        
        logger.info("일일 리포트 생성 테스트 완료")
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
        raise

if __name__ == "__main__":
    test_daily_report() 