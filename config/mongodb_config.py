import os
from typing import Dict, Any

def get_mongodb_config() -> Dict[str, Any]:
    """MongoDB 설정을 환경 변수에서 가져옵니다."""
    return {
        'host': os.getenv('MONGO_HOST', 'localhost'),
        'port': int(os.getenv('MONGO_PORT', '25000')),
        'db_name': os.getenv('MONGO_DB_NAME', 'trading_db'),
        'username': os.getenv('MONGO_ROOT_USERNAME'),
        'password': os.getenv('MONGO_ROOT_PASSWORD'),
        'collections': {
            'trades': 'trades',                    # 단기 거래 데이터
            'market_data': 'market_data',          # 시장 데이터
            'thread_status': 'thread_status',      # 스레드 상태
            'system_config': 'system_config',      # 시스템 설정
            'daily_profit': 'daily_profit',        # 일간 수익
            'strategy_data': 'strategy_data',      # 전략 데이터
            'long_term_trades': 'long_term_trades',# 장기 투자 거래 정보
            'trade_conversion': 'trade_conversion', # 거래 전환 기록
            'trading_history': 'trading_history',   # 거래 히스토리
            'portfolio': 'portfolio',               # 포트폴리오 정보
            'scheduled_tasks': 'scheduled_tasks'
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