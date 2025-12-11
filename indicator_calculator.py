import pandas as pd
import numpy as np


class IndicatorCalculator:
    """Calculate technical indicators on streaming data"""
    
    @staticmethod
    def calculate_rsi(close_prices, period=11):
        """Calculate RSI"""
        delta = close_prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_atr(df, period=14):
        """Calculate ATR"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr
    
    @staticmethod
    def calculate_ema(close_prices, period):
        """Calculate EMA"""
        return close_prices.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def find_pivot_high(highs, index, lookback=3):
        """Check if index is a pivot high"""
        if index < lookback or index >= len(highs) - lookback:
            return False
        
        current = highs.iloc[index]
        for i in range(index - lookback, index + lookback + 1):
            if i != index and highs.iloc[i] >= current:
                return False
        return True
    
    @staticmethod
    def find_pivot_low(lows, index, lookback=3):
        """Check if index is a pivot low"""
        if index < lookback or index >= len(lows) - lookback:
            return False
        
        current = lows.iloc[index]
        for i in range(index - lookback, index + lookback + 1):
            if i != index and lows.iloc[i] <= current:
                return False
        return True