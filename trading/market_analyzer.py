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
from monitoring.memory_monitor import MemoryProfiler, memory_profiler

class MarketAnalyzer:
    """
    시장 분석을 위한 클래스
    여러 기술적 지표와 전략을 사용하여 거래 신호를 생성합니다.
    """
    def __init__(self, config, exchange_name: str):
        """
        MarketAnalyzer 초기화
        Args:
            config: 설정 정보가 담긴 딕셔너리
        """
        self.config = config
        self.exchange_name = exchange_name
        self.db = MongoDBManager(exchange_name=self.exchange_name)
        self.logger = logging.getLogger('investment-center')
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
        self.memory_profiler = MemoryProfiler()

    @memory_profiler.profile_memory
    async def get_sorted_markets(self) -> List:
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
                    'exchange': self.exchange_name,
                    'timestamp': datetime.utcnow()
                }
                try:
                    # DB 업데이트
                    self.db.market_data.update_one(
                        {'market': market, 'exchange': self.exchange_name},
                        {'$set': data},
                        upsert=True
                    )
                    market_data.append(market)
                except Exception as e:
                    self.logger.error(f"마켓 데이터 업데이트 실패 - {market}: {str(e)}")
                    continue
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error in get_sorted_markets: {e}")
            return []

    @memory_profiler.profile_memory
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

    @memory_profiler.profile_memory
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

    @memory_profiler.profile_memory
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

    @memory_profiler.profile_memory 
    def analyze_market(self, market: str, candles: List[Dict]) -> Dict:
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

            # 현재 캔들 데이터
            current_candle = candles[-1]
            
            # 시장 데이터 구성
            market_data = {
                'current_price': float(current_candle['close']),
                'price_history': [float(c['close']) for c in candles[-5:]],  # 최근 5개
                'volume': float(current_candle['volume']),
                'volume_history': [float(c['volume']) for c in candles[-5:]],
                'high': float(current_candle['high']),
                'low': float(current_candle['low']),
                
                # RSI 관련
                'rsi': float(current_candle.get('rsi', 50)),
                'rsi_history': [float(c.get('rsi', 50)) for c in candles[-5:]],
                
                # MACD 관련
                'macd': float(current_candle.get('macd', 0)),
                'signal': float(current_candle.get('signal', 0)),
                'macd_history': [float(c.get('macd', 0)) for c in candles[-5:]],
                
                # 볼린저 밴드
                'upper_band': float(current_candle.get('upper_band', 0)),
                'lower_band': float(current_candle.get('lower_band', 0)),
                'middle_band': float(current_candle.get('middle_band', 0)),
                
                # 스토캐스틱
                'stoch_k': float(current_candle.get('stoch_k', 50)),
                'stoch_d': float(current_candle.get('stoch_d', 50)),
                'stoch_k_history': [float(c.get('stoch_k', 50)) for c in candles[-5:]],
                
                # 이동평균
                'ma5': float(current_candle.get('sma5', 0)),
                'ma20': float(current_candle.get('sma20', 0)),
                
                # 거래량 관련
                'average_volume': float(current_candle.get('average_volume', 0)),
                'current_volume': float(current_candle['volume']),
                
                # 추세/모멘텀 관련
                'momentum': float(current_candle.get('momentum', 0)),
                'trend_strength': float(current_candle.get('trend_strength', 0)),
                'market_sentiment': float(current_candle.get('market_sentiment', 0)),
                
                # 일목균형표
                'ichimoku_cloud_top': float(current_candle.get('ichimoku_cloud_top', 0)),
                'ichimoku_cloud_bottom': float(current_candle.get('ichimoku_cloud_bottom', 0)),
                
                # 가격 변화율
                'price_change_rate': float(current_candle.get('price_change_rate', 0)),
                
                # Divergence 관련 추가
                'price_divergence': float(current_candle.get('price_divergence', 0)),
                'volume_divergence': float(current_candle.get('volume_divergence', 0)),
                'rsi_divergence': float(current_candle.get('rsi_divergence', 0)),
                
                # Bollinger Band 추가 지표
                'bb_width': float(current_candle.get('bb_width', 0)),
                'bb_percent_b': float(current_candle.get('percent_b', 0)),
                
                # Ichimoku 추가 지표
                'tenkan_sen': float(current_candle.get('tenkan_sen', 0)),
                'kijun_sen': float(current_candle.get('kijun_sen', 0)),
                'senkou_span_a': float(current_candle.get('senkou_span_a', 0)),
                'senkou_span_b': float(current_candle.get('senkou_span_b', 0)),
                'chikou_span': float(current_candle.get('chikou_span', 0)),
                
                # Market Sentiment 추가 지표
                'sentiment_score': float(current_candle.get('sentiment_score', 0)),
                'fear_greed_index': float(current_candle.get('fear_greed_index', 50)),
                
                # 추가 이동평균선
                'ma10': float(current_candle.get('sma10', 0)),
                'ma50': float(current_candle.get('sma50', 0)),
                'ma200': float(current_candle.get('sma200', 0))
            }

            # 전략별 결과 수집 및 총합 계산
            strategy_results = {}
            total_strength = 0
            
            for name, strategy in self.strategies.items():
                try:
                    result = strategy.analyze(market_data)
                    strategy_results[name] = {
                        'signal': 'buy' if result >= 0.65 else 'sell' if result <= 0.35 else 'hold',
                        'strength': float(result),
                        'value': float(result),
                        'market_data': market_data  # 전략에 사용된 데이터도 포함
                    }
                    total_strength += float(result)
                except Exception as e:
                    self.logger.error(f"{market} - {name} 전략 분석 실패: {str(e)}", exc_info=True)
                    strategy_results[name] = {'signal': 'hold', 'strength': 0.5, 'value': 0.5}
                    total_strength += 0.5

            # 각 전략의 기여도(percentage) 계산
            strategy_percentages = {}
            for name, result in strategy_results.items():
                percentage = (result['strength'] / total_strength) * 100
                strategy_percentages[f"{name}_percentage"] = round(percentage, 2)
                strategy_results[name]['percentage'] = round(percentage, 2)

            # market_data에 전략별 기여도 추가
            market_data.update(strategy_percentages)

            # 평균 강도 계산
            average_strength = total_strength / len(strategy_results)

            return {
                'action': 'buy' if average_strength >= 0.65 else 'hold',
                'overall_signal': round(average_strength, 2),
                'price': market_data['current_price'],
                'strategy_data': strategy_results,
                'market_data': market_data,  # 전략별 기여도가 포함된 시장 데이터
                'strategy_percentages': strategy_percentages  # 전략별 기여도 별도 제공
            }

        except Exception as e:
            self.logger.error(f"시장 분석 중 오류 발생 ({market}): {str(e)}", exc_info=True)
            return {
                'action': 'hold',
                'overall_signal': 0,
                'price': 0,
                'strategy_data': {}
            }