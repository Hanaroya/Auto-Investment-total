db.createUser({
    user: process.env.MONGO_ROOT_USERNAME,
    pwd: process.env.MONGO_ROOT_PASSWORD,
    roles: [
        {
            role: "readWrite",
            db: "trading_db"
        }
    ]
});

// 컬렉션 생성
db = db.getSiblingDB('trading_db');

db.createCollection('trades');
db.createCollection('market_data');
db.createCollection('thread_status');
db.createCollection('system_config');
db.createCollection('daily_profit');
db.createCollection('strategy_data');
db.createCollection('long_term_trades');
db.createCollection('trade_conversion');
db.createCollection('trading_history');
db.createCollection('portfolio');
db.createCollection('scheduled_tasks'); 