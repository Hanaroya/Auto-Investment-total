import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

class MarketDataConverter:
    """
    시장 데이터를 전략에 맞는 형식으로 변환하는 클래스
    """
    def __init__(self):
        # 필수 기술적 지표 컬럼 목록
        self.required_columns = [
            'open', 'high', 'low', 'close', 'volume',  # OHLCV 기본 데이터
            'rsi', 'macd', 'signal', 'oscillator',     # RSI와 MACD 관련 지표
            'sma5', 'sma20', 'sma60', 'sma120',       # 단순이동평균선
            'upper_band', 'lower_band',                # 볼린저 밴드
            'stoch_k', 'stoch_d'                       # 스토캐스틱
        ]

    def convert_upbit_candle(self, candle_data: List[Dict]) -> Dict[str, Any]:
        """
        업비트 캔들 데이터를 전략 분석에 적합한 형식으로 변환
        Args:
            candle_data: 업비트 API로부터 받은 원시 캔들 데이터 리스트
        Returns:
            최신 캔들의 기술적 지표가 포함된 딕셔너리
        """
        try:
            df = pd.DataFrame({
                'date': [x['candle_date_time_kst'] for x in candle_data],
                'open': [x['opening_price'] for x in candle_data],
                'high': [x['high_price'] for x in candle_data],
                'low': [x['low_price'] for x in candle_data],
                'close': [x['trade_price'] for x in candle_data],
                'volume': [x['candle_acc_trade_volume'] for x in candle_data]
            })

            # 기술적 지표 계산
            df = self._calculate_indicators(df)

            # 최신 데이터 반환
            latest_data = df.iloc[-1].to_dict()
            
            # 추가 시장 데이터
            latest_data.update({
                'price_change_rate': candle_data[-1].get('change_rate', 0) * 100,
                'market_state': 'active',
                'timestamp': candle_data[-1]['timestamp']
            })

            return latest_data

        except Exception as e:
            print(f"데이터 변환 실패: {str(e)}")
            return {}

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        모든 기술적 지표를 한번에 계산하는 메서드
        Args:
            df: OHLCV 데이터가 포함된 DataFrame
        Returns:
            기술적 지표가 추가된 DataFrame
        """
        # RSI
        df['rsi'] = self._calculate_rsi(df['close'], 14)

        # MACD
        df = self._calculate_macd(df)

        # 이동평균선
        for period in [5, 20, 60, 120]:
            df[f'sma{period}'] = df['close'].rolling(window=period).mean()

        # 볼린저 밴드
        df = self._calculate_bollinger_bands(df)

        # 스토캐스틱
        df = self._calculate_stochastic(df)

        return df

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        상대강도지수(RSI) 계산
        Args:
            prices: 종가 시계열 데이터
            period: RSI 계산 기간 (기본값: 14일)
        Returns:
            RSI 값이 포함된 Series
        """
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

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
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['oscillator'] = df['macd'] - df['signal']
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
        n = 14
        df['lowest_low'] = df['low'].rolling(window=n).min()
        df['highest_high'] = df['high'].rolling(window=n).max()
        df['stoch_k'] = ((df['close'] - df['lowest_low']) / 
                        (df['highest_high'] - df['lowest_low'])) * 100
        df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
        return df