from pydantic_settings import BaseSettings
from typing import List
import os

class Settings(BaseSettings):
    # 기본 설정
    PROJECT_NAME: str = "Trading API"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api"
    
    # CORS 설정
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",  # 개발 환경
        "http://localhost:80",    # 프로덕션 환경
    ]
    
    # MongoDB 설정
    MONGO_HOST: str = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT: int = int(os.getenv("MONGO_PORT", "27017"))
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "trading_db")
    MONGO_USER: str = os.getenv("MONGO_ROOT_USERNAME", "")
    MONGO_PASSWORD: str = os.getenv("MONGO_ROOT_PASSWORD", "")
    
    # 환경 설정
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = ENVIRONMENT == "development"

    class Config:
        case_sensitive = True

settings = Settings() 