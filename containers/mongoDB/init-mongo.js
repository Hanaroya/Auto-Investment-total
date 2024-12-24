db.auth(process.env.MONGO_INITDB_ROOT_USERNAME, process.env.MONGO_INITDB_ROOT_PASSWORD)

db = db.getSiblingDB(process.env.MONGO_DB_NAME);

db.createUser({
    user: process.env.MONGO_USER_USERNAME,
    pwd: process.env.MONGO_USER_PASSWORD,
    roles: [
        {
            role: "readWrite",
            db: process.env.MONGO_DB_NAME
        }
    ]
}); 