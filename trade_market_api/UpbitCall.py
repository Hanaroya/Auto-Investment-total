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

class UpbitCall:
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.server_url = 'https://api.upbit.com'
        self.user_agent = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
        self.logger = self._setup_logger()
        # test 모드 여부 확인
        self.is_test = (access_key == "test_access_key" and secret_key == "test_secret_key")

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

    def get_krw_markets(self) -> List[str]:
        """원화 마켓 목록 조회"""
        url = "https://crix-api.upbit.com/v1/crix/trends/change_rate"
        response = requests.get(url=url, headers=self.user_agent).json()
        
        krw_markets = []
        for market in response:
            if 'KRW-' in market['code']:
                krw_markets.append(market['code'].replace("CRIX.UPBIT.",''))
        
        return krw_markets

    def get_candle(self, symbol: str, interval: str, count: int = 200) -> List[Dict]:
        """캔들 데이터 조회
        interval: '1', '3', '5', '10', '15', '30', '60', '240', 'D'
        """
        try:
            base_url = 'https://crix-api-endpoint.upbit.com/v1/crix/candles'
            
            if interval in ['1', '3', '5', '10', '15', '30', '60', '240']:
                url = f"{base_url}/minutes/{interval}"
            elif interval == 'D':
                url = f"{base_url}/days"
            else:
                raise ValueError("Invalid interval")

            # CRIX API 형식에 맞게 심볼 변환
            crix_symbol = f"CRIX.UPBIT.{symbol}"
            
            params = {
                'code': crix_symbol,
                'count': count,
                'to': ''  # 현재 시각 기준
            }
            
            response = requests.get(url, params=params, headers=self.user_agent)
            return response.json()

        except Exception as e:
            self.logger.error(f"캔들 데이터 조회 실패: {str(e)}")
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

if __name__ == "__main__":
    # 사용 예시
    upbit = UpbitCall("your_access_key", "your_secret_key")
    
    # 원화 마켓 목록 조회
    markets = upbit.get_krw_markets()
    print(f"원화 마켓: {markets[:5]}")
    
    # BTC 현재가 조회
    btc_price = upbit.get_current_price("KRW-BTC")
    print(f"BTC 현재가: {btc_price}")