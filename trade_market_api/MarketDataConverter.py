import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

class MarketDataConverter:
    def __init__(self):
        self.required_columns = [
            'open', 'high', 'low', 'close', 'volume',
            'rsi', 'macd', 'signal', 'oscillator',
            'sma5', 'sma20', 'sma60', 'sma120',
            'upper_band', 'lower_band',
            'stoch_k', 'stoch_d'
        ]

    def convert_upbit_candle(self, candle_data: List[Dict]) -> Dict[str, Any]:
        """업비트 캔들 데이터를 전략에 맞는 형식으로 변환"""
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
        """기술적 지표 계산"""
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
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def _calculate_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """MACD 계산"""
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['oscillator'] = df['macd'] - df['signal']
        return df

    def _calculate_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """볼린저 밴드 계산"""
        df['middle_band'] = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()
        df['upper_band'] = df['middle_band'] + (std * 2)
        df['lower_band'] = df['middle_band'] - (std * 2)
        return df

    def _calculate_stochastic(self, df: pd.DataFrame) -> pd.DataFrame:
        """스토캐스틱 계산"""
        n = 14
        df['lowest_low'] = df['low'].rolling(window=n).min()
        df['highest_high'] = df['high'].rolling(window=n).max()
        df['stoch_k'] = ((df['close'] - df['lowest_low']) / 
                        (df['highest_high'] - df['lowest_low'])) * 100
        df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
        return df