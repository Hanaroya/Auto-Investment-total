MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'db_name': 'crypto_trading',
    'collections': {
        'trades': 'trades',
        'market_data': 'market_data',
        'thread_status': 'thread_status',
        'system_config': 'system_config'
    }
}

# 초기 시스템 설정
INITIAL_SYSTEM_CONFIG = {
    'initial_investment': 1000000,
    'min_trade_amount': 5000,
    'max_thread_investment': 80000,
    'reserve_amount': 200000,
    'total_max_investment': 800000,
    'emergency_stop': False
} 