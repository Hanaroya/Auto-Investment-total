import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import numpy as np
from monitoring.memory_monitor import MemoryProfiler, memory_profiler

class MarketDataConverter:
    """
    시장 데이터를 전략에 맞는 형식으로 변환하는 클래스
    """
    def __init__(self):
        # 필수 기술적 지표 컬럼 목록
        self.memory_profiler = MemoryProfiler()
        self.required_columns = [
            'open', 'high', 'low', 'close', 'volume',  # OHLCV 기본 데이터
            'rsi', 'macd', 'signal', 'oscillator',     # RSI와 MACD 관련 지표
            'sma5', 'sma20', 'sma60', 'sma120',       # 단순이동평균선
            'upper_band', 'lower_band',                # 볼린저 밴드
            'stoch_k', 'stoch_d'                       # 스토캐스틱
        ]

    
    def convert_upbit_candle(self, candle_data: List[Dict]) -> List[Dict]:
        """
        업비트 캔들 데이터를 전략 분석에 적합한 형식으로 변환
        Args:
            candle_data: 업비트 API로부터 받은 원시 캔들 데이터 리스트
        Returns:
            각 캔들의 기술적 지표가 포함된 딕셔너리 리스트
        """
        try:
            if not candle_data:
                print("빈 캔들 데이터")
                return []

            # 시간순으로 정렬 (오래된 데이터부터)
            sorted_candles = sorted(candle_data, key=lambda x: x['timestamp'])
            
            if len(sorted_candles) < 50:
                print(f"불충분한 캔들 데이터: {len(sorted_candles)}개")
                return []

            # DataFrame 생성 (전체 캔들 데이터 사용)
            df = pd.DataFrame({
                'date': [x['datetime'] for x in sorted_candles],
                'open': [x['open'] for x in sorted_candles],
                'high': [x['high'] for x in sorted_candles],
                'low': [x['low'] for x in sorted_candles],
                'close': [x['close'] for x in sorted_candles],
                'volume': [x['volume'] for x in sorted_candles],
                'value': [x['value'] for x in sorted_candles],
                'market': [x['market'] for x in sorted_candles]
            })
            
            # 기술적 지표 계산
            df = self._calculate_indicators(df)
            
            # 각 행을 딕셔너리로 변환
            converted_data = []
            for idx in range(len(df)):
                row_data = df.iloc[idx].to_dict()
                
                # 히스토리 데이터 계산 (각 시점에서 이전 5개 데이터)
                history_columns = ['rsi', 'macd', 'close', 'volume']
                for col in history_columns:
                    history_key = f"{col}_history"
                    start_idx = max(0, idx - 4)  # 최근 5개 데이터
                    row_data[history_key] = df[col].iloc[start_idx:idx + 1].tolist()
                    # 부족한 데이터는 0으로 채움
                    while len(row_data[history_key]) < 5:
                        row_data[history_key].insert(0, 0.0)
                
                # 추가 시장 데이터
                row_data.update({
                    'timestamp': sorted_candles[idx]['timestamp'],
                    'datetime': sorted_candles[idx]['datetime'],
                    'market': sorted_candles[idx]['market'],
                    'market_state': 'active'
                })
                
                converted_data.append(row_data)

            return converted_data

        except Exception as e:
            print(f"데이터 변환 실패: {str(e)}")
            print(f"First candle data: {candle_data[0] if candle_data else 'No data'}")
            return []

    
    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        모든 기술적 지표를 한번에 계산하는 메서드
        Args:
            df: OHLCV 데이터가 포함된 DataFrame
        Returns:
            기술적 지표가 추가된 DataFrame
        """
        try:
            # 숫자형 데이터 확인 및 변환
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'value']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # 기본 기술적 지표 계산
            df['rsi'] = self._calculate_rsi(df['close'], 14)
            df = self._calculate_macd(df)
            
            # 이동평균선
            for period in [5, 20, 60, 120]:
                df[f'sma{period}'] = df['close'].rolling(window=period).mean()
            
            # 볼린저 밴드
            df = self._calculate_bollinger_bands(df)
            
            # 스토캐스틱
            df = self._calculate_stochastic(df)
            
            # 히스토리 데이터 생성 (문자열 형태로 저장)
            history_columns = {
                'rsi_history': 'rsi',
                'macd_history': 'macd',
                'price_history': 'close',
                'volume_history': 'volume'
            }
            
            for hist_col, source_col in history_columns.items():
                # 먼저 숫자형으로 변환
                values = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
                # 롤링 윈도우로 최근 5개 값 가져오기
                history_values = []
                for i in range(len(df)):
                    start_idx = max(0, i - 4)  # 최근 5개 값
                    window_values = values[start_idx:i + 1].tolist()
                    # 부족한 데이터는 0으로 채움
                    while len(window_values) < 5:
                        window_values.insert(0, 0.0)
                    history_values.append(','.join(map(str, window_values)))
                df[hist_col] = history_values
            
            # 추가 지표 계산 (모두 float 형식)
            df['momentum'] = df['close'].pct_change(14).fillna(0).astype(float)
            df['trend_strength'] = self._calculate_trend_strength(df).astype(float)
            df['volatility'] = (df['close'].rolling(window=20).std() / 
                              df['close'].rolling(window=20).mean()).fillna(0).astype(float)
            df['average_volume'] = df['volume'].rolling(window=20).mean().fillna(0).astype(float)
            df['current_volume'] = df['volume'].astype(float)
            
            # 일목균형표
            df = self._calculate_ichimoku(df)
            
            # 시장 심리 지수
            df['market_sentiment'] = self._calculate_market_sentiment(df)
            
            # 가격 변화율
            df['price_change_rate'] = df['close'].pct_change().fillna(0).astype(float) * 100
            
            # 거래량 변화율
            df['volume_change_rate'] = df['volume'].pct_change().fillna(0).astype(float) * 100
            
            # 가격 추세와 변동성 계산 추가
            df['price_trend'] = self._calculate_price_trend(df)
            df['volatility'] = self._calculate_volatility(df)
            
            # NaN 값을 0으로 변환
            df = df.fillna(0)
            
            # 모든 숫자형 컬럼을 float로 변환 (히스토리 데이터 제외)
            for col in df.columns:
                if col not in ['date', 'market'] and not col.endswith('_history'):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
            
            return df

        except Exception as e:
            import traceback
            print(f"지표 계산 중 오류 발생: {str(e)}")
            print("Traceback:")
            print(''.join(traceback.format_tb(e.__traceback__)))
            return df.fillna(0)

    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        상대강도지수(RSI) 계산
        Args:
            prices: 종가 시계열 데이터
            period: RSI 계산 기간 (기본값: 14일)
        Returns:
            RSI 값이 포함된 Series
        """
        try:
            # 가격 변화량 계산
            delta = prices.diff()
            
            # 상승/하락 분리
            gains = delta.where(delta > 0, 0.0)
            losses = -delta.where(delta < 0, 0.0)
            
            # 평균 계산
            avg_gains = gains.rolling(window=period).mean()
            avg_losses = losses.rolling(window=period).mean()
            
            # RS 계산 (0으로 나누기 방지)
            rs = avg_gains / avg_losses.replace(0, float('inf'))
            
            # RSI 계산
            rsi = 100 - (100 / (1 + rs))
            
            # NaN 및 무한값 처리
            rsi = rsi.replace([np.inf, -np.inf], 100)
            return rsi.fillna(50)  # 초기값은 중립적인 50으로 설정

        except Exception as e:
            print(f"RSI 계산 중 오류: {str(e)}")
            return pd.Series([50] * len(prices))  # 오류 시 중립값 반환

    
    def _calculate_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        MACD(이동평균수렴확산) 지표 계산
        - MACD Line: 12일 EMA - 26일 EMA
        - Signal Line: 9일 MACD EMA
        - Oscillator: MACD - Signal
        Args:
            df: 가격 데이터가 포함된 DataFrame
        Returns:
            MACD 관련 지표가 추가된 DataFrame
        """
        try:
            # 지수이동평균 계산
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            
            # MACD 계산
            df['macd'] = exp1 - exp2
            
            # Signal Line 계산
            df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
            
            # Oscillator 계산
            df['oscillator'] = df['macd'] - df['signal']
            
            # NaN 값을 0으로 변환
            df[['macd', 'signal', 'oscillator']] = df[['macd', 'signal', 'oscillator']].fillna(0)
            
            return df

        except Exception as e:
            print(f"MACD 계산 중 오류: {str(e)}")
            # 오류 발생 시 0으로 채움
            df['macd'] = 0
            df['signal'] = 0
            df['oscillator'] = 0
            return df

    
    def _calculate_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        볼린저 밴드 계산
        - 중심선: 20일 이동평균
        - 상단/하단: 중심선 ± (2 * 표준편차)
        Args:
            df: 가격 데이터가 포함된 DataFrame
        Returns:
            볼린저 밴드 지표가 추가된 DataFrame
        """
        df['middle_band'] = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()
        df['upper_band'] = df['middle_band'] + (std * 2)
        df['lower_band'] = df['middle_band'] - (std * 2)
        return df

    
    def _calculate_stochastic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        스토캐스틱 지표 계산
        - %K: (현재가 - n기간 최저가) / (n기간 최고가 - n기간 최저가) * 100
        - %D: %K의 3일 이동평균
        Args:
            df: OHLC 데이터가 포함된 DataFrame
        Returns:
            스토캐스틱 지표가 추가된 DataFrame
        """
        try:
            n = 14  # 기본 기간
            
            # 최저가와 최고가 계산
            df['lowest_low'] = df['low'].rolling(window=n, min_periods=1).min()
            df['highest_high'] = df['high'].rolling(window=n, min_periods=1).max()
            
            # 분모가 0인 경우 방지
            denominator = df['highest_high'] - df['lowest_low']
            denominator = denominator.replace(0, np.inf)
            
            # %K 계산
            df['stoch_k'] = ((df['close'] - df['lowest_low']) / denominator * 100).fillna(0)
            
            # %D 계산
            df['stoch_d'] = df['stoch_k'].rolling(window=3, min_periods=1).mean().fillna(0)
            
            # 불필요한 컬럼 제거
            df = df.drop(['lowest_low', 'highest_high'], axis=1)
            
            # 값 범위 제한 (0-100)
            df['stoch_k'] = df['stoch_k'].clip(0, 100)
            df['stoch_d'] = df['stoch_d'].clip(0, 100)
            
            return df

        except Exception as e:
            print(f"스토캐스틱 계산 중 오류: {str(e)}")
            df['stoch_k'] = 50  # 중립값으로 설정
            df['stoch_d'] = 50
            return df

    
    def _calculate_trend_strength(self, df: pd.DataFrame) -> pd.Series:
        """
        추세 강도 계산 (-1: 강한 하락세, 1: 강한 상승세)
        """
        try:
            ma20 = df['close'].rolling(window=20, min_periods=1).mean()
            price_change = (df['close'] - ma20) / ma20.replace(0, np.inf)
            return price_change.clip(-1, 1).fillna(0)
        except Exception as e:
            print(f"추세 강도 계산 중 오류: {str(e)}")
            return pd.Series([0] * len(df))

    
    def _calculate_ichimoku(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        일목균형표 계산
        """
        try:
            # 전환선 (9일)
            period9_high = df['high'].rolling(window=9, min_periods=1).max()
            period9_low = df['low'].rolling(window=9, min_periods=1).min()
            df['conversion_line'] = (period9_high + period9_low) / 2
            
            # 기준선 (26일)
            period26_high = df['high'].rolling(window=26, min_periods=1).max()
            period26_low = df['low'].rolling(window=26, min_periods=1).min()
            df['base_line'] = (period26_high + period26_low) / 2
            
            # 선행스팬 A
            df['ichimoku_cloud_top'] = ((df['conversion_line'] + df['base_line']) / 2).shift(26)
            
            # 선행스팬 B
            period52_high = df['high'].rolling(window=52, min_periods=1).max()
            period52_low = df['low'].rolling(window=52, min_periods=1).min()
            df['ichimoku_cloud_bottom'] = ((period52_high + period52_low) / 2).shift(26)
            
            # NaN 값 처리 (deprecated method 대체)
            ichimoku_cols = ['conversion_line', 'base_line', 'ichimoku_cloud_top', 'ichimoku_cloud_bottom']
            for col in ichimoku_cols:
                df[col] = df[col].ffill().fillna(df['close'].iloc[0])
            
            # 모든 값을 float으로 변환
            df[ichimoku_cols] = df[ichimoku_cols].astype(float)
            
            return df
        except Exception as e:
            print(f"일목균형표 계산 중 오류: {str(e)}")
            ichimoku_cols = ['conversion_line', 'base_line', 'ichimoku_cloud_top', 'ichimoku_cloud_bottom']
            for col in ichimoku_cols:
                df[col] = float(df['close'].iloc[0])
            return df

    
    def _calculate_market_sentiment(self, df: pd.DataFrame) -> pd.Series:
        """
        시장 심리 지수 계산
        """
        try:
            # RSI 요소
            rsi_factor = ((df['rsi'].fillna(50) - 50) / 50).clip(-1, 1)

            # 거래량 요소
            volume_mean = df['volume'].rolling(window=20, min_periods=1).mean()
            volume_factor = ((df['volume'] - volume_mean) / volume_mean.replace(0, np.inf)).clip(-1, 1)

            # 모멘텀 요소
            momentum = df['close'].pct_change(14).fillna(0)
            momentum_factor = momentum.clip(-1, 1)
            
            # 가중 평균
            sentiment = (rsi_factor * 0.4 + volume_factor * 0.3 + momentum_factor * 0.3)
            return sentiment.clip(-1, 1).fillna(0)
        except Exception as e:
            print(f"시장 심리 지수 계산 중 오류: {str(e)}")
            return pd.Series([0] * len(df))

    
    def _calculate_price_trend(self, df: pd.DataFrame) -> pd.Series:
        """
        가격 추세 계산 (-1 ~ 1)
        - 양수: 상승 추세
        - 음수: 하락 추세
        - 절대값이 클수록 추세가 강함
        """
        try:
            # 단기(5일)와 장기(20일) 이동평균 계산
            ma5 = df['close'].rolling(window=5, min_periods=1).mean()
            ma20 = df['close'].rolling(window=20, min_periods=1).mean()
            
            # 추세 강도 계산
            trend = ((ma5 - ma20) / ma20.replace(0, np.inf)).clip(-1, 1)
            
            # 모멘텀 반영
            momentum = df['close'].pct_change(5).fillna(0).clip(-0.1, 0.1) * 5
            
            # 최종 추세 계산 (추세 + 모멘텀)
            price_trend = ((trend + momentum) / 2).clip(-1, 1)
            
            return price_trend.fillna(0)
            
        except Exception as e:
            print(f"가격 추세 계산 중 오류: {str(e)}")
            return pd.Series([0] * len(df))

     
    def _calculate_volatility(self, df: pd.DataFrame) -> pd.Series:
        """
        변동성 계산 (0 ~ 1)
        - 0: 변동성 낮음
        - 1: 변동성 매우 높음
        """
        try:
            # 일일 변동폭 계산
            daily_volatility = ((df['high'] - df['low']) / df['close']).rolling(window=10).std()
            
            # 거래량 변동성
            volume_volatility = (df['volume'] / df['volume'].rolling(window=20).mean()).clip(0, 5)
            
            # 종합 변동성 (거래량 변동성 반영)
            volatility = (daily_volatility * 0.7 + (volume_volatility / 5) * 0.3).clip(0, 1)
            
            return volatility.fillna(0)
            
        except Exception as e:
            print(f"변동성 계산 중 오류: {str(e)}")
            return pd.Series([0] * len(df))