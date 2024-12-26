from asyncio.log import logger
from datetime import datetime
from typing import List, Dict
import logging
from database.mongodb_manager import MongoDBManager
from strategy.Strategies import (
    RSIStrategy,
    MACDStrategy,
    BollingerBandStrategy,
    VolumeStrategy,
    PriceChangeStrategy,
    MovingAverageStrategy,
    MomentumStrategy,
    StochasticStrategy,
    IchimokuStrategy,
    MarketSentimentStrategy,
    DowntrendEndStrategy,
    UptrendEndStrategy,
    DivergenceStrategy
)
import pandas as pd
from trade_market_api.UpbitCall import UpbitCall
import asyncio
import time

class MarketAnalyzer:
    """
    시장 분석을 위한 클래스
    여러 기술적 지표와 전략을 사용하여 거래 신호를 생성합니다.
    """
    def __init__(self, config):
        """
        MarketAnalyzer 초기화
        Args:
            config: 설정 정보가 담긴 딕셔너리
        """
        self.config = config
        self.db = MongoDBManager()
        self.logger = logging.getLogger(__name__)
        self.upbit = UpbitCall(
            self.config['api_keys']['upbit']['access_key'],
            self.config['api_keys']['upbit']['secret_key'],
            is_test=True
        )
        self.strategies = {
            'RSI': RSIStrategy(),
            'MACD': MACDStrategy(),
            'Bollinger': BollingerBandStrategy(),
            'Volume': VolumeStrategy(),
            'PriceChange': PriceChangeStrategy(),
            'Moving_Averages': MovingAverageStrategy(),
            'Momentum': MomentumStrategy(),
            'Stochastic': StochasticStrategy(),
            'Ichimoku': IchimokuStrategy(),
            'Market_Sentiment': MarketSentimentStrategy(),
            'Downtrend_End': DowntrendEndStrategy(),
            'Uptrend_End': UptrendEndStrategy(),
            'Divergence': DivergenceStrategy()
        }

    async def get_sorted_markets(self) -> List[Dict]:
        """
        거래량 기준으로 정렬된 시장 목록을 반환합니다.
        """
        try:
            # 동기 함수로 호출
            markets = self.upbit.get_krw_markets() 
            
            # 마켓 데이터를 딕셔너리 형태로 변환
            market_data = []
            for market in markets:
                data = {
                    'market': market,
                    'timestamp': datetime.utcnow()
                }
                try:
                    # DB 업데이트
                    self.db.market_data.update_one(
                        {'market': market},
                        {'$set': data},
                        upsert=True
                    )
                    market_data.append(data)
                except Exception as e:
                    self.logger.error(f"마켓 데이터 업데이트 실패 - {market}: {str(e)}")
                    continue
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error in get_sorted_markets: {e}")
            return []

    async def get_candle_data(self, market: str, interval: str = "240", max_retries: int = 3):
        """
        특정 시장의 캔들 데이터를 조회합니다.
        
        Args:
            market (str): 시장 코드 (예: KRW-BTC)
            interval (str): 캔들 간격 (기본값: 240분/4시간)
            max_retries (int): 최대 재시도 횟수
        """
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"{market} - 캔들 데이터 조회 시작 (시도: {attempt + 1})")
                candle_data = await self.upbit.get_candle(market, interval)
                
                if not candle_data:
                    self.logger.warning(f"{market} - 캔들 데이터 조회 실패")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)  # 재시도 전 대기
                        continue
                    return None
                    
                converted_data = self.convert_candle_data(candle_data)
                self.logger.debug(f"{market} - 캔들 데이터 조회 완료 (개수: {len(converted_data)})")
                return converted_data
                
            except Exception as e:
                self.logger.error(f"{market} - 캔들 데이터 조회 중 오류: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None

    def convert_candle_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        원시 캔들 데이터를 분석하기 쉬운 형식으로 변환합니다.
        
        Args:
            raw_data (List[Dict]): API에서 받은 원시 캔들 데이터
            
        Returns:
            List[Dict]: 변환된 캔들 데이터
        """
        converted_data = []
        for candle in raw_data:
            converted_data.append({
                'timestamp': candle['timestamp'],
                'open': float(candle['opening_price']),
                'high': float(candle['high_price']),
                'low': float(candle['low_price']),
                'close': float(candle['trade_price']),
                'volume': float(candle['candle_acc_trade_volume'])
            })
        return converted_data 

    def _process_strategy_result(self, result):
        """
        전략 분석 결과를 표준화된 형식으로 변환합니다.
        
        Args:
            result: 전략에서 반환된 원시 결과
            
        Returns:
            Dict: 표준화된 결과 ('signal'과 'strength' 포함)
        """
        if isinstance(result, (int, float)):
            return {'signal': 'hold', 'strength': float(result)}
        return result

    async def analyze_market(self, market: str, candles: List[Dict]) -> Dict:
        """
        주어진 시장에 대해 모든 전략을 실행하여 종합적인 분석을 수행합니다.
        
        Args:
            market (str): 분석할 시장 코드
            candles (List[Dict]): 분석에 사용할 캔들 데이터
            
        Returns:
            Dict: {
                'action': 매수/홀드 신호,
                'strength': 신호 강도 (0-1),
                'price': 현재 가격,
                'strategy_data': 각 전략별 상세 결과
            }
        """
        try:
            self.logger.debug(f"{market} - 시장 분석 시작")
            
            if not candles:
                self.logger.warning(f"{market} - 분석할 캔들 데이터 없음")
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            # 필수 컬럼 확인
            required_columns = ['trade_price', 'candle_acc_trade_volume']
            df = pd.DataFrame(candles)
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"{market} - 필수 컬럼 누락: {missing_columns}")
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            if df.empty:
                self.logger.warning(f"{market} - 캔들 데이터 없음")
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            # 데이터 전처리
            market_data = {
                'df': df,
                'current_price': float(df['trade_price'].iloc[-1]),
                'volume': float(df['candle_acc_trade_volume'].iloc[-1])
            }
            
            # 각 전략 실행 및 결과 수집
            strategy_results = {}
            for name, strategy in self.strategies.items():
                try:
                    start_time = time.time()
                    result = strategy.analyze(market_data)
                    execution_time = time.time() - start_time
                    
                    # 전략 실행 시간 모니터링
                    if execution_time > 1.0:  # 1초 이상 소요되는 전략 감지
                        self.logger.warning(f"{market} - {name} 전략 실행 시간 과다: {execution_time:.2f}초")
                    
                    if isinstance(result, (int, float)):
                        strategy_results[name] = {'signal': 'hold', 'strength': float(result)}
                    else:
                        strategy_results[name] = result
                        
                except Exception as e:
                    self.logger.error(f"{market} - {name} 전략 분석 실패: {str(e)}", exc_info=True)
                    strategy_results[name] = {'signal': 'hold', 'strength': 0}

            # 전략 결과 상세 로깅
            self.logger.info(f"\n[{market}] 전략 분석 결과:")
            for strategy, result in strategy_results.items():
                self.logger.info(f"{strategy}: {result}")

            # 종합 강도 계산 및 검증
            if not strategy_results:
                self.logger.error(f"{market} - 유효한 전략 결과 없음")
                return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}

            total_strength = sum(r['strength'] for r in strategy_results.values()) / len(strategy_results)
            self.logger.info(f"{market} - 종합 강도: {round(total_strength, 2)}")

            return {
                'action': 'buy' if total_strength >= 0.65 else 'hold',
                'strength': round(total_strength, 2),
                'price': market_data['current_price'],
                'strategy_data': strategy_results
            }

        except Exception as e:
            self.logger.error(f"시장 분석 중 오류 발생 ({market}): {str(e)}", exc_info=True)
            return {'action': 'hold', 'strength': 0, 'price': 0, 'strategy_data': {}}