import requests
import pandas as pd
import logging
import time
from typing import Dict, List, Optional, Union, Any, Tuple
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
import threading
from bs4 import BeautifulSoup
from utils.time_utils import TimeUtils
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from trade_market_api.MarketDataConverter import MarketDataConverter

class ThreadLock:
    """싱글톤 패턴으로 구현된 스레드 락 관리자
    
    전역적으로 하나의 인스턴스만 존재하며, API 호출에 대한 동시성을 제어합니다.
    
    Attributes:
        _instance: 싱글톤 인스턴스
        lock: 스레드 간 동기화를 위한 Lock 객체
        current_thread: 현재 락을 보유한 스레드 ID
        logger: 로깅을 위한 logger 인스턴스
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ThreadLock, cls).__new__(cls)
            cls._instance.lock = Lock()
            cls._instance.current_thread = None
            cls._instance.logger = logging.getLogger('investment_center')
        return cls._instance

    def acquire_lock(self, thread_id: int, operation: str) -> bool:
        """락 획득 시도"""
        if self.lock.acquire(blocking=False):
            self.current_thread = thread_id
            self.logger.debug(f"Thread {thread_id} acquired lock for {operation}")
            return True
        return False

    def release_lock(self, thread_id: int):
        """락 해제"""
        if self.current_thread == thread_id:
            self.lock.release()
            self.current_thread = None
            self.logger.debug(f"Thread {thread_id} released lock")

def with_thread_lock(operation: str):
    """API 작업에 대한 스레드 락을 제공하는 데코레이터
    
    Args:
        operation (str): 락을 획득하려는 작업의 이름 (예: "buy", "sell", "get_candle")
    
    Notes:
        - 최대 3회까지 락 획득을 시도
        - 각 시도 사이에 1초 대기
        - ThreadManager의 락과 별개로 작동하는 전역 락
        - API 호출의 동시성을 제어하여 rate limit 준수
    
    Raises:
        RuntimeError: 3회 시도 후에도 락 획득 실패시
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            thread_id = getattr(self, 'thread_id', 0)
            lock_manager = ThreadLock()
            
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
    """업비트 API 호출을 담당하는 클래스
    
    Notes:
        - API 메서드들은 두 단계의 락으로 보호됨:
            1. ThreadManager의 공유 락: 스레드 그룹 간의 동기화
            2. ThreadLock 데코레이터: API 호출의 전역적 동기화
        
        - 주요 보호 대상 메서드:
            - get_candle: 캔들 데이터 조회
            - buy_market_order: 시장가 매수
            - sell_market_order: 시장가 매도
    """
    def __init__(self, access_key: str, secret_key: str, is_test: bool = False):
        self.access_key = access_key
        self.secret_key = secret_key
        self.is_test = is_test
        self.logger = self._setup_logger()
        self.server_url = 'https://api.upbit.com'
        self.user_agent = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
        self.session = None
        self.thread_id = None  # 스레드 식별용
        self.last_ubmi_fetch_time = None

    def _setup_logger(self) -> logging.Logger:
        """로깅 설정
        
        Returns:
            logging.Logger: 설정된 로거 인스턴스
            
        Notes:
            - API 호출 결과는 INFO 레벨로 기록
            - 주문 관련 작업은 WARNING 레벨로 처리
        """
        logger = logging.getLogger('investment_center')
        
        # 이미 핸들러가 설정되어 있다면 추가 설정하지 않음
        if logger.handlers:
            return logger
        
        logger.setLevel(logging.CRITICAL if self.is_test else logging.WARNING)
        
        log_dir = Path('log')
        log_dir.mkdir(exist_ok=True)
        
        today = TimeUtils.get_current_kst().strftime('%Y-%m-%d')
        handler = logging.FileHandler(f'{log_dir}/{today}-trade.log', encoding='utf-8')
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger

    def _get_auth_header(self, query: Optional[Dict] = None) -> Dict:
        """
        인증 헤더 생성
        
        Args:
            query (Optional[Dict]): 쿼리 매개변수 (기본값: None)
        
        Returns:
            Dict: 인증 헤더
        """
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
        """원화 마켓 목록 조회 (거래량 순)
        
        Returns:
            List[str]: 원화 마켓 목록
        """
        try:
            url = "https://crix-api.upbit.com/v1/crix/trends/change_rate"
            response = requests.get(url, headers=self.user_agent)
            markets = response.json()
            
            # KRW 마켓만 필터링하고 거래량으로 정렬
            krw_markets = [
                market for market in markets 
                if 'KRW-' in market['code']
            ]
            
            # accTradeVolume 기준으로 정렬
            sorted_markets = sorted(
                krw_markets,
                key=lambda x: float(x.get('accTradePrice24h', 0)),
                reverse=True
            )
            
            # 마켓 이름만 추출
            return [
                market['code'].replace("CRIX.UPBIT.", '')
                for market in sorted_markets
            ]
                
        except Exception as e:
            self.logger.error(f"원화 마켓 목록 조회 실패: {str(e)}")
            return []

    def _has_sufficient_data(self, candle_data: List[Dict], market: str) -> bool:
        """충분한 캔들 데이터가 있는지 확인"""
        required_candles = 50  # 필요한 최소 캔들 수
        if not candle_data or len(candle_data) < required_candles:
            self.logger.warning(f"Thread {threading.current_thread().name} - {market} - 불충분한 캔들 데이터 (수신: {len(candle_data) if candle_data else 0})")
            return False
        return True

    def get_candle(self, market: str, interval: str = '1', count: int = 300) -> List[Dict]:
        """
        캔들 데이터를 가져옵니다.
        
        Args:
            market (str): 마켓 코드 (예: KRW-BTC)
            interval (str): 시간 간격
                - 분 단위: '1', '3', '5', '10', '15', '30', '60', '240'
                - 일 단위: 'D'
                - 월 단위: 'M'
            count (int): 가져올 캔들 개수 (최대 200)
                
        Returns:
            List[Dict]: 캔들 데이터 리스트. 각 캔들은 다음 정보를 포함:
                - timestamp: 타임스탬프
                - datetime: 캔들 시각 (KST)
                - open: 시가
                - high: 고가
                - low: 저가
                - close: 종가
                - volume: 누적 거래량
                - value: 누적 거래대금
                - market: 마켓 코드
        """
        try:
            # URL 구성
            base_url = "https://crix-api-endpoint.upbit.com/v1/crix/candles"
            
            # market이 딕셔너리인 경우 market 키의 값을 추출
            if isinstance(market, dict):
                market = market.get('market', '')
            
            # 시간 간격에 따른 URL 설정
            if interval in ['1', '3', '5', '10', '15', '30', '60', '240']:
                url = base_url + "/minutes/" + str(interval)
            elif interval == 'D':
                url = base_url + "/days"
            elif interval == 'W':
                url = base_url + "/weeks"
            elif interval == 'M':
                url = base_url + "/months"
            else:
                self.logger.error(f"Thread {threading.current_thread().name} - 잘못된 시간 간격: {interval}")
                return []

            # CRIX.UPBIT. 접두어 추가
            market_code = "CRIX.UPBIT." + str(market)
            
            # 최종 URL 구성
            final_url = url + "?code=" + market_code + "&count=" + str(count) + "&to"
            
            # URL 로깅
            self.logger.debug(f"Thread {threading.current_thread().name} - API 요청 URL: {final_url}")
            
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
            }
            
            response = requests.get(url=final_url, headers=headers)
            
            if response.status_code != 200:
                self.logger.error(f"Thread {threading.current_thread().name} - API 요청 실패 ({market}): {response.status_code}")
                return []
            
            candles = response.json()
            
            # 데이터 유효성 검증
            if not self._has_sufficient_data(candles, market):
                return []
             
            # 데이터 변환
            processed_candles = [{
                'timestamp': candle['timestamp'],
                'datetime': candle['candleDateTimeKst'],
                'open': candle['openingPrice'],
                'high': candle['highPrice'],
                'low': candle['lowPrice'],
                'close': candle['tradePrice'],
                'volume': candle['candleAccTradeVolume'],
                'value': candle['candleAccTradePrice'],
                'market': market
            } for candle in candles]
            converter = MarketDataConverter()
            converted_candles = converter.convert_upbit_candle(processed_candles)

            self.logger.debug(f"Thread {threading.current_thread().name} - {market} 캔들 데이터 수신: {len(candles)}개")
            return converted_candles
            
        except Exception as e:
            self.logger.error(f"Thread {threading.current_thread().name} - 캔들 데이터 조회 중 오류: {str(e)}")
            return []

    def get_current_price(self, symbol: str) -> float:
        """현재가 조회
        
        Args:
            symbol (str): 조회할 마켓 심볼 (예: "KRW-BTC")
        
        Returns:
            float: 현재가
        """
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
        
        Args:
            symbol (str): 주문할 마켓 심볼 (예: "KRW-BTC")
            side (str): 'bid'(매수) 또는 'ask'(매도)
            volume (float): 주문 수량
            price (Optional[float]): 주문 가격 (기본값: None)
        
        Returns:
            Dict: 주문 응답
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
        """주문 취소
        
        Args:
            uuid (str): 취소할 주문의 UUID
        
        Returns:
            Dict: 주문 취소 응답
        """
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
        """주문 상태 조회
        
        Args:
            uuid (str): 조회할 주문의 UUID
        
        Returns:
            Dict: 주문 상태 응답
        """
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
        """RSI 계산
        
        Args:
            data (List[float]): 캔들 데이터 리스트
            period (int): 계산할 기간 (기본값: 14)
        
        Returns:
            float: 계산된 RSI 값
        """
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
        """시장가 매수
        
        Notes:
            - ThreadManager의 buy 락과 함께 동작
            - 주문 실행의 동시성 제어
        """
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
                self.logger.debug(f"매수 주문 성공: {market}, 금액: {price}")
                return response.json()
            else:
                self.logger.error(f"매수 주문 실패: {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"매수 주문 중 오류 발생: {str(e)}")
            return {}

    @with_thread_lock("sell")
    async def sell_market_order(self, market: str, volume: float) -> Dict:
        """시장가 매도
        
        Notes:
            - ThreadManager의 sell 락과 함께 동작
            - 주문 실행의 동시성 제어
        """
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
                self.logger.debug(f"매도 주문 성공: {market}, 수량: {volume}")
                return response.json()
            else:
                self.logger.error(f"매도 주문 실패: {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"매도 주문 중 오류 발생: {str(e)}")
            return {}

    def _create_jwt_token(self, query: Dict) -> str:
        """JWT 토큰 생성
        
        Args:
            query (Dict): 쿼리 매개변수
        
        Returns:
            str: 생성된 JWT 토큰
        """
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': self._create_query_hash(query),
            'query_hash_alg': 'SHA512',
        }
        
        return jwt.encode(payload, self.secret_key)

    def _create_query_hash(self, query: Dict) -> str:
        """쿼리 해시 생성
        
        Args:
            query (Dict): 쿼리 매개변수
        
        Returns:
            str: 생성된 쿼리 해시
        """
        query_string = urlencode(query).encode()
        m = hashlib.sha512()
        m.update(query_string)
        return m.hexdigest()

    async def initialize(self, thread_id: int, loop=None):
        """비동기 초기화"""
        self.thread_id = thread_id
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers=self.user_agent,
                loop=loop
            )

    async def close(self):
        """세션 정리"""
        if self.session:
            await self.session.close()
            self.session = None

    def should_fetch_ubmi(self) -> bool:
        """UBMI 데이터를 가져올 시간인지 확인"""
        current_time = TimeUtils.get_current_kst()
        
        # 마지막 조회 시간 확인
        if self.last_ubmi_fetch_time is None:
            return True
            
        # 5분 간격으로 조회 (0, 5, 10, 15, ..., 55분)
        time_diff = (current_time - self.last_ubmi_fetch_time).total_seconds()
        return (current_time.minute % 5 == 0 and time_diff >= 300)

    @with_thread_lock("ubmi")
    async def fetch_ubmi_data(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """UBMI 데이터 크롤링"""
        url = 'https://ubcindex.com/home'
        options = Options()
        
        # 완전한 백그라운드 실행을 위한 옵션들
        options.add_argument("--headless=new")  # 새로운 헤드리스 모드
        options.add_argument("--disable-gpu")   # GPU 하드웨어 가속 비활성화
        options.add_argument("--no-sandbox")    # 샌드박스 비활성화
        options.add_argument("--disable-dev-shm-usage")  # 공유 메모리 사용 비활성화
        options.add_argument("--disable-extensions")     # 확장 프로그램 비활성화
        options.add_argument("--disable-browser-side-navigation")  # 브라우저 측 탐색 비활성화
        options.add_argument("--disable-infobars")      # 정보 표시줄 비활성화
        options.add_argument("--disable-notifications")  # 알림 비활성화
        options.add_argument("--disable-popup-blocking") # 팝업 차단
        options.add_argument("--window-position=-32000,-32000")  # 화면 밖으로 이동
        options.add_argument("--log-level=3")  # 로그 레벨 최소화
        options.add_argument("--silent")       # 불필요한 출력 억제
        options.add_argument("--window-size=1,1")  # 최소 창 크기
        
        driver = None
        try:
            # 임시 디렉토리 사용 설정
            service = ChromeService(
                ChromeDriverManager().install(),
                log_output=os.devnull  # 로그 출력 억제
            )
            
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_window_position(-32000, -32000)  # 추가적인 창 위치 설정
            
            driver.get(url)
            await asyncio.sleep(10)  # 비동기 대기
            html = BeautifulSoup(driver.page_source, features="html.parser")
            
            p = re.compile('(?<=\">)(.*?)(?=<\/div>)')
            price_ko_divs = html.find_all("div", {"class": "item active"})
            total_ubmi = 0
            change_ubmi = 0
            fear_greed = float(p.findall(str(html.find("div", {"class": "score"})))[0].replace(',',""))
            
            for price_ko_div in price_ko_divs:
                minus_chk = False
                price_status_div = price_ko_div.find("div", class_="price rise") or price_ko_div.find("div", class_="price fall")
                if price_ko_div.find("div", class_="price fall"): 
                    minus_chk = True
                if price_status_div:
                    item_price = price_status_div.find("div", {"class": "item Price"})
                    item_change_rate = price_status_div.find("div", {"class": "item changePrice"})
                    if item_price and item_change_rate:
                        total_ubmi = float(item_price.text.strip().replace(',',""))
                        change_ubmi = float(item_change_rate.text.strip().replace(',',""))
                        if minus_chk:
                            change_ubmi *= -1
            
            self.last_ubmi_fetch_time = TimeUtils.get_current_kst()  
            return total_ubmi, change_ubmi, fear_greed
            
        except Exception as e:
            self.logger.error(f"UBMI 데이터 크롤링 실패: {str(e)}")
            return None, None, None
            
        finally:
            if driver:
                try:
                    driver.quit()  # 드라이버 종료
                    await asyncio.sleep(1)  # 완전한 종료를 위한 대기
                except Exception as e:
                    self.logger.error(f"드라이버 종료 중 오류: {str(e)}")

    async def update_market_data(self, db_manager) -> bool:
        """UBMI 데이터 업데이트 및 DB 저장"""
        try:
            if not self.should_fetch_ubmi():
                return False
                
            total_ubmi, change_ubmi, fear_greed = await self.fetch_ubmi_data()
            if all(v is not None for v in [total_ubmi, change_ubmi, fear_greed]):
                market_data = {
                    'exchange': 'upbit',
                    'timestamp': TimeUtils.get_current_kst(),
                    'AFR': total_ubmi,
                    'current_change': change_ubmi,
                    'fear_and_greed': fear_greed
                }
                
                success = db_manager.update_market_index(market_data)
                if success:
                    self.logger.info(f"UBMI 데이터 업데이트 완료: AFR={total_ubmi}, Change={change_ubmi}, F&G={fear_greed}")
                return success
                
            return False
            
        except Exception as e:
            self.logger.error(f"UBMI 데이터 업데이트 중 오류: {str(e)}")
            return False

    async def get_AFR_value(self) -> Dict:
        """
        거래소별 AFR 데이터 조회
        Returns:
            Dict: {
                'AFR': float,  # 현재 AFR 값
                'current_change': float,  # 현재 변화율
                'fear_and_greed': float,  # 현재 공포/탐욕 지수
                'market_feargreed': List[Dict[str, Any]]  # 마켓별 공포/탐욕 데이터
            }
        """
        try:
            total_ubmi, change_ubmi, fear_greed = await self.fetch_ubmi_data()
            market_feargreed = await self.get_feargreed_data()
            
            if all(v is not None for v in [total_ubmi, change_ubmi, fear_greed, market_feargreed]):
                return {
                    'AFR': total_ubmi,
                    'current_change': change_ubmi,
                    'fear_and_greed': fear_greed,
                    'market_feargreed': market_feargreed
                }
            return None
            
        except Exception as e:
            self.logger.error(f"AFR 데이터 조회 중 오류: {str(e)}")
            return None

    @with_thread_lock("feargreed")
    async def get_feargreed_data(self, url: str = "https://www.ubcindex.com/feargreed") -> List[Dict[str, Any]]:
        """Fear & Greed 데이터 크롤링
        
        Args:
            url (str): 크롤링할 URL
            
        Returns:
            List[Dict[str, Any]]: 크롤링된 데이터 리스트
                [
                    {
                        'market': str,  # 마켓 코드 (예: 'KRW-BTC')
                        'feargreed': float,  # Fear & Greed 값
                        'timestamp': datetime  # 데이터 수집 시각 (KST)
                    },
                    ...
                ]
        """
        options = Options()
        
        # 완전한 백그라운드 실행을 위한 옵션들
        options.add_argument("--headless=new")  # 새로운 헤드리스 모드
        options.add_argument("--disable-gpu")   # GPU 하드웨어 가속 비활성화
        options.add_argument("--no-sandbox")    # 샌드박스 비활성화
        options.add_argument("--disable-dev-shm-usage")  # 공유 메모리 사용 비활성화
        options.add_argument("--disable-extensions")     # 확장 프로그램 비활성화
        options.add_argument("--disable-browser-side-navigation")  # 브라우저 측 탐색 비활성화
        options.add_argument("--disable-infobars")      # 정보 표시줄 비활성화
        options.add_argument("--disable-notifications")  # 알림 비활성화
        options.add_argument("--disable-popup-blocking") # 팝업 차단
        options.add_argument("--window-position=-32000,-32000")  # 화면 밖으로 이동
        options.add_argument("--log-level=3")  # 로그 레벨 최소화
        options.add_argument("--silent")       # 불필요한 출력 억제
        options.add_argument("--window-size=1,1")  # 최소 창 크기
        
        driver = None
        result_data = []
        
        try:
            service = ChromeService(
                ChromeDriverManager().install(),
                log_output=os.devnull
            )
            
            driver = webdriver.Chrome(service=service, options=options)
            driver.get(url)
            await asyncio.sleep(5)  # 페이지 로딩 대기
            
            page_num = 1
            while True:
                try:
                    # 테이블 데이터 처리
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.pairsTbl"))
                    )
                    
                    table = driver.find_element(By.CSS_SELECTOR, "table.pairsTbl")
                    rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                    
                    if not rows:
                        self.logger.debug("테이블 행을 찾을 수 없음")
                        break
                    
                    rows_processed = 0
                    for row in rows:
                        try:
                            # 마켓 코드 (div.code의 부모 div.currency 내부에서 찾기)
                            currency_div = row.find_element(By.CSS_SELECTOR, "div.currency")
                            code_div = currency_div.find_element(By.CSS_SELECTOR, "div.code")
                            name_div = currency_div.find_element(By.CSS_SELECTOR, "div.name")
                            
                            if not code_div or not name_div:
                                continue
                            
                            symbol = code_div.get_attribute('innerHTML').strip().split('/')
                            coin_name = name_div.get_attribute('innerHTML').strip()
                            
                            if len(symbol) != 2 or symbol[1] != 'KRW':
                                continue
                            
                            market_code = f"KRW-{symbol[0]}"
                            
                            # 컬럼 데이터
                            columns = row.find_elements(By.CSS_SELECTOR, "td")
                            if len(columns) < 6:
                                continue
                            
                            # 상태 (td.state)
                            state = columns[1].get_attribute('innerHTML').strip()
                            # Fear & Greed 값
                            feargreed = columns[2].get_attribute('innerHTML').strip()
                            # 날짜
                            date_str = columns[5].get_attribute('innerHTML').strip()
                            
                            result_data.append({
                                'market': market_code,
                                'name': coin_name,
                                'feargreed': float(feargreed),
                                'state': state,
                                'date': date_str,
                                'timestamp': TimeUtils.get_current_kst()
                            })
                            
                            rows_processed += 1
                            
                        except Exception as e:
                            self.logger.error(f"행 처리 중 오류: {str(e)}")
                            continue
                    
                    self.logger.debug(f"페이지 {page_num}: {rows_processed}개 데이터 처리")
                    
                    # 다음 페이지 버튼 처리
                    try:
                        # 다음 페이지 버튼 찾기
                        next_button = driver.find_element(By.CSS_SELECTOR, "span.btn.next")
                        
                        # 버튼이 활성화되어 있는지 확인
                        if next_button.get_attribute("class").find("disabled") == -1:
                            # JavaScript로 클릭 실행
                            driver.execute_script("arguments[0].click();", next_button)
                            page_num += 1
                            await asyncio.sleep(3)  # 페이지 전환 대기
                            
                            # 페이지 전환 확인
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "table.pairsTbl"))
                            )
                            
                            # 데이터 로딩 대기
                            await asyncio.sleep(2)
                        else:
                            self.logger.debug("마지막 페이지 도달")
                            break
                        
                    except NoSuchElementException:
                        self.logger.debug("다음 페이지 버튼을 찾을 수 없음")
                        break
                    except Exception as e:
                        self.logger.error(f"다음 페이지 이동 실패: {str(e)}")
                        break
                    
                except Exception as e:
                    self.logger.error(f"페이지 처리 중 오류: {str(e)}")
                    break
            
            if not result_data:
                self.logger.error("수집된 데이터가 없습니다")
            else:
                self.logger.info(f"총 {len(result_data)}개의 데이터 수집 완료")
            
            return result_data
            
        except Exception as e:
            self.logger.error(f"Fear & Greed 데이터 크롤링 실패: {str(e)}")
            return []
            
        finally:
            if driver:
                try:
                    driver.quit()
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.error(f"드라이버 종료 중 오류: {str(e)}")

if __name__ == "__main__":
    # 사용 예시
    upbit = UpbitCall("your_access_key", "your_secret_key")
    
    # 원화 마켓 목록 조회
    markets = upbit.get_krw_markets()
    print(f"원화 마켓: {markets[:5]}")
    
    # BTC 현재가 조회
    btc_price = upbit.get_current_price("KRW-BTC")
    print(f"BTC 현재가: {btc_price}")