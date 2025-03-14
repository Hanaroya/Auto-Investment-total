// 환경변수에서 인증 정보 가져오기
const rootUsername = process.env.MONGO_ROOT_USERNAME;
const rootPassword = process.env.MONGO_ROOT_PASSWORD;
const dbName = process.env.MONGO_DB_NAME;
const userUsername = process.env.MONGO_USER_USERNAME;
const userPassword = process.env.MONGO_USER_PASSWORD;

// root 계정으로 인증
db.auth(rootUsername, rootPassword)

// 지정된 데이터베이스로 전환
db = db.getSiblingDB(dbName);

// 일반 사용자 계정 생성
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