from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import trading, exchange, analytics

app = FastAPI(title="Trading API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trading.router, prefix="/api/trading", tags=["trading"])
app.include_router(exchange.router, prefix="/api/exchange", tags=["exchange"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["analytics"]) 