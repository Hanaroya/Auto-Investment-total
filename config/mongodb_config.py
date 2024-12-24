import os
from typing import Dict, Any

def get_mongodb_config() -> Dict[str, Any]:
    """MongoDB 설정을 환경 변수에서 가져옵니다."""
    return {
        'host': os.getenv('MONGO_HOST', 'localhost'),
        'port': int(os.getenv('MONGO_PORT', '25000')),
        'db_name': os.getenv('MONGO_DB_NAME', 'trading_db'),
        'username': os.getenv('MONGO_USER_USERNAME'),
        'password': os.getenv('MONGO_USER_PASSWORD'),
        'collections': {
            'trades': 'trades',           # 거래 데이터
            'market_data': 'market_data', # 시장 데이터
            'thread_status': 'thread_status', # 스레드 상태
            'system_config': 'system_config'  # 시스템 설정
        }
    }

def get_initial_system_config() -> Dict[str, Any]:
    """시스템 초기 설정을 환경 변수에서 가져옵니다."""
    return {
        'initial_investment': int(os.getenv('INITIAL_INVESTMENT', '1000000')),
        'min_trade_amount': int(os.getenv('MIN_TRADE_AMOUNT', '5000')),
        'max_thread_investment': int(os.getenv('MAX_THREAD_INVESTMENT', '80000')),
        'reserve_amount': int(os.getenv('RESERVE_AMOUNT', '200000')),
        'total_max_investment': int(os.getenv('TOTAL_MAX_INVESTMENT', '800000')),
        'emergency_stop': False
    }

# 설정 객체 생성
MONGODB_CONFIG = get_mongodb_config()
INITIAL_SYSTEM_CONFIG = get_initial_system_config() 