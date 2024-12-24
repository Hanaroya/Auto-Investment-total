MONGODB_CONFIG = { # 몽고DB 설정
    'host': 'localhost', # 호스트
    'port': 27017, # 포트
    'db_name': 'crypto_trading', # 데이터베이스 이름
    'username': 'autotrade',
    'password': '[REDACTED]',
    'collections': {
        'trades': 'trades', # 거래 데이터
        'market_data': 'market_data', # 시장 데이터
        'thread_status': 'thread_status', # 스레드 상태
        'system_config': 'system_config' # 시스템 설정
    }
}

# 초기 시스템 설정
INITIAL_SYSTEM_CONFIG = {
    'initial_investment': 1000000, # 초기 투자 금액
    'min_trade_amount': 5000, # 최소 거래 금액
    'max_thread_investment': 80000, # 스레드 최대 투자 금액
    'reserve_amount': 200000, # 예비 금액
    'total_max_investment': 800000, # 총 최대 투자 금액
    'emergency_stop': False # 긴급 중지 여부
} 