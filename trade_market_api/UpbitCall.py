import requests
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import re
import jwt
import uuid
import hashlib
from urllib.parse import urlencode
from pathlib import Path
from threading import Lock
from functools import wraps
import asyncio
import aiohttp

class ThreadLock:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ThreadLock, cls).__new__(cls)
            cls._instance.lock = Lock()
            cls._instance.current_thread = None
            cls._instance.logger = logging.getLogger(__name__)
        return cls._instance

    def acquire_lock(self, thread_id: int, operation: str) -> bool:
        """락 획득 시도"""
        if self.lock.acquire(blocking=False):
            self.current_thread = thread_id
            self.logger.info(f"Thread {thread_id} acquired lock for {operation}")
            return True
        return False

    def release_lock(self, thread_id: int):
        """락 해제"""
        if self.current_thread == thread_id:
            self.lock.release()
            self.current_thread = None
            self.logger.info(f"Thread {thread_id} released lock")

def with_thread_lock(operation: str):
    """데코레이터: 스레드 락 적용"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            thread_id = getattr(self, 'thread_id', 0)
            lock_manager = ThreadLock()
            
            # 락 획득 시도 (최대 3회)
            for attempt in range(3):
                if lock_manager.acquire_lock(thread_id, operation):
                    try:
                        return await func(self, *args, **kwargs)
                    finally:
                        lock_manager.release_lock(thread_id)
                else:
                    await asyncio.sleep(1)
            
            raise RuntimeError(f"Thread {thread_id} failed to acquire lock for {operation}")
        return wrapper
    return decorator

class UpbitCall:
    def __init__(self, access_key: str, secret_key: str, is_test: bool = False):
        self.access_key = access_key
        self.secret_key = secret_key
        self.is_test = is_test
        self.logger = self._setup_logger()
        self.server_url = 'https://api.upbit.com'
        self.user_agent = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }

    def _setup_logger(self) -> logging.Logger:
        """로깅 설정
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - API 호출 결과는 INFO 레벨로 기록
            - 주문 관련 작업은 WARNING 레벨로 처리
        """
        logger = logging.getLogger('UpbitCall')
        logger.setLevel(logging.DEBUG if self.is_test else logging.INFO)
        
        log_dir = Path('log')
        log_dir.mkdir(exist_ok=True)
        
        today = datetime.now().strftime('%Y-%m-%d')
        handler = logging.FileHandler(f'{log_dir}/{today}-trade.log')
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _get_auth_header(self, query: Optional[Dict] = None) -> Dict:
        """인증 헤더 생성"""
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4())
        }
        
        if query:
            query_string = urlencode(query)
            payload['query'] = query_string
            
        jwt_token = jwt.encode(payload, self.secret_key)
        return {'Authorization': f'Bearer {jwt_token}'}

    async def get_krw_markets(self) -> List[str]:
        """원화 마켓 목록 조회 (거래량 순)"""
        try:
            url = "https://crix-api.upbit.com/v1/crix/trends/change_rate"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.user_agent) as response:
                    markets = await response.json()
                    
                    # KRW 마켓만 필터링하고 거래량으로 정렬
                    krw_markets = [
                        market for market in markets 
                        if 'KRW-' in market['code']
                    ]
                    
                    # accTradeVolume 기준으로 정렬
                    sorted_markets = sorted(
                        krw_markets,
                        key=lambda x: float(x.get('accTradeVolume', 0)),
                        reverse=True
                    )
                    
                    # 코인 이름만 추출
                    return [
                        market['code'].replace("CRIX.UPBIT.", '')
                        for market in sorted_markets
                    ]
                    
        except Exception as e:
            self.logger.error(f"원화 마켓 목록 조회 실패: {str(e)}")
            return []

    def _has_sufficient_data(self, candle_data: List[Dict], market: str) -> bool:
        """충분한 캔들 데이터가 있는지 확인"""
        required_candles = 200  # 필요한 최소 캔들 수
        if not candle_data or len(candle_data) < required_candles:
            self.logger.warning(f"불충분한 캔들 데이터 ({market}): {len(candle_data) if candle_data else 0}/{required_candles}")
            return False
        return True

    @with_thread_lock("get_candle")
    async def get_candle(self, market: str, interval: str, count: int = 200) -> List[Dict]:
        """캔들 데이터 조회"""
        try:
            base_url = 'https://crix-api-endpoint.upbit.com/v1/crix/candles'
            
            if interval in ['1', '3', '5', '10', '15', '30', '60', '240']:
                url = f"{base_url}/minutes/{interval}"
            elif interval == 'D':
                url = f"{base_url}/days"
            else:
                raise ValueError("Invalid interval")

            crix_symbol = f"CRIX.UPBIT.{market}"
                
            params = {
                'code': f"CRIX.UPBIT.{market}",
                'count': count
            }
            
            await asyncio.sleep(0.1)  # API 호출 전 0.1초 대기
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=self.user_agent) as response:
                    if response.status != 200:
                        self.logger.error(f"API 요청 실패 ({market}): {response.status}")
                        return []
                        
                    candles = await response.json()
                    
                    # API 응답 필드명 매핑
                    if not self._has_sufficient_data(candles, market):
                        return []

                     # 데이터 처리
                    processed_candles = [{
                        'timestamp': candle['timestamp'],
                        'datetime': candle['candleDateTimeKst'],
                        'opening_price': candle['openingPrice'],
                        'high_price': candle['highPrice'],
                        'low_price': candle['lowPrice'],
                        'trade_price': candle['tradePrice'],
                        'candle_acc_trade_volume': candle['candleAccTradeVolume'],
                        'candle_acc_trade_price': candle['candleAccTradePrice'],
                        'market': market
                    } for candle in candles]

                    return processed_candles
                
        except Exception as e:
            self.logger.error(f"캔들 데이터 조회 실패 ({market}): {str(e)}")
            return []

    def get_current_price(self, symbol: str) -> float:
        """현재가 조회"""
        try:
            url = f"{self.server_url}/v1/ticker"
            query = {'markets': symbol}
            response = requests.get(url, params=query, headers=self.user_agent)
            return float(response.json()[0]['trade_price'])
        except Exception as e:
            self.logger.error(f"현재가 조회 실패: {str(e)}")
            return 0.0

    def place_order(self, symbol: str, side: str, volume: float, price: Optional[float] = None) -> Dict:
        """주문 실행
        side: 'bid'(매수) 또는 'ask'(매도)
        """
        try:
            # 테스트 모드일 경우 모의 주문 응답 반환
            if self.is_test:
                test_uuid = str(uuid.uuid4())
                return {
                    'uuid': test_uuid,
                    'side': side,
                    'market': symbol,
                    'volume': volume,
                    'price': price,
                    'test_mode': True
                }

            # 실제 주문 로직
            url = f"{self.server_url}/v1/orders"
            query = {
                'market': symbol,
                'side': side,
                'volume': str(volume),
                'ord_type': 'limit' if price else 'market'
            }
            
            if price:
                query['price'] = str(price)

            headers = self._get_auth_header(query)
            response = requests.post(url, json=query, headers=headers)
            return response.json()

        except Exception as e:
            self.logger.error(f"주문 실패: {str(e)}")
            return {}

    def cancel_order(self, uuid: str) -> Dict:
        """주문 취소"""
        try:
            # 테스트 모드일 경우 모의 취소 응답 반환
            if self.is_test:
                return {
                    'uuid': uuid,
                    'status': 'cancel',
                    'test_mode': True
                }

            # 실제 취소 로직
            url = f"{self.server_url}/v1/order"
            query = {'uuid': uuid}
            headers = self._get_auth_header(query)
            response = requests.delete(url, params=query, headers=headers)
            return response.json()
        except Exception as e:
            self.logger.error(f"주문 취소 실패: {str(e)}")
            return {}

    def get_order_status(self, uuid: str) -> Dict:
        """주문 상태 조회"""
        try:
            # 테스트 모드일 경우 모의 상태 응답 반환
            if self.is_test:
                return {
                    'uuid': uuid,
                    'status': 'done',
                    'test_mode': True
                }

            # 실제 상태 조회 로직
            url = f"{self.server_url}/v1/order"
            query = {'uuid': uuid}
            headers = self._get_auth_header(query)
            response = requests.get(url, params=query, headers=headers)
            return response.json()
        except Exception as e:
            self.logger.error(f"주문 상태 조회 실패: {str(e)}")
            return {}

    def calculate_rsi(self, data: List[float], period: int = 14) -> float:
        """RSI 계산"""
        try:
            df = pd.DataFrame({'close': data})
            delta = df['close'].diff()
            
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi.iloc[-1])
        except Exception as e:
            self.logger.error(f"RSI 계산 실패: {str(e)}")
            return 0.0

    @with_thread_lock("buy")
    async def buy_market_order(self, market: str, price: float) -> Dict:
        """시장가 매수"""
        try:
            query = {
                'market': market,
                'side': 'bid',
                'price': str(price),
                'ord_type': 'price',
            }
            
            jwt_token = self._create_jwt_token(query)
            headers = {
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                'https://api.upbit.com/v1/orders',
                json=query,
                headers=headers
            )
            
            if response.status_code == 201:
                self.logger.info(f"매수 주문 성공: {market}, 금액: {price}")
                return response.json()
            else:
                self.logger.error(f"매수 주문 실패: {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"매수 주문 중 오류 발생: {str(e)}")
            return {}

    @with_thread_lock("sell")
    async def sell_market_order(self, market: str, volume: float) -> Dict:
        """시장가 매도"""
        try:
            query = {
                'market': market,
                'side': 'ask',
                'volume': str(volume),
                'ord_type': 'market',
            }
            
            jwt_token = self._create_jwt_token(query)
            headers = {
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                'https://api.upbit.com/v1/orders',
                json=query,
                headers=headers
            )
            
            if response.status_code == 201:
                self.logger.info(f"매도 주문 성공: {market}, 수량: {volume}")
                return response.json()
            else:
                self.logger.error(f"매도 주문 실패: {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"매도 주문 중 오류 발생: {str(e)}")
            return {}

    def _create_jwt_token(self, query: Dict) -> str:
        """JWT 토큰 생성"""
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': self._create_query_hash(query),
            'query_hash_alg': 'SHA512',
        }
        
        return jwt.encode(payload, self.secret_key)

    def _create_query_hash(self, query: Dict) -> str:
        """쿼리 해시 생성"""
        query_string = urlencode(query).encode()
        m = hashlib.sha512()
        m.update(query_string)
        return m.hexdigest()

if __name__ == "__main__":
    # 사용 예시
    upbit = UpbitCall("your_access_key", "your_secret_key")
    
    # 원화 마켓 목록 조회
    markets = upbit.get_krw_markets()
    print(f"원화 마켓: {markets[:5]}")
    
    # BTC 현재가 조회
    btc_price = upbit.get_current_price("KRW-BTC")
    print(f"BTC 현재가: {btc_price}")