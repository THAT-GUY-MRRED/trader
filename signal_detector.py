import pandas as pd
from indicator_calculator import IndicatorCalculator


class SignalDetector:
    """Detects divergence signals with BZ-CAE validation"""
    
    def __init__(self, config):
        self.config = config
        self.recent_pivots = []
        self.last_signal_time = None
        self.signal_cooldown_bars = 10
    
    def calculate_confidence(self, df, index, is_bullish):
        """Calculate BZ-CAE confidence score"""
        try:
            rsi = df['rsi'].iloc[index]
            close = df['close'].iloc[index]
            ema20 = df['ema20'].iloc[index]
            ema50 = df['ema50'].iloc[index]
            ema100 = df['ema100'].iloc[index]
            atr = df['atr'].iloc[index]
            
            if pd.isna(rsi) or pd.isna(atr) or atr == 0:
                return 0.0
            
            confidence = 0.0
            
            # TCS (Trend Conviction Score)
            tcs = 0.0
            if ema20 > ema50 > ema100 or ema20 < ema50 < ema100:
                tcs = 0.4
            confidence += 0.25 * (1 - tcs)
            
            # Exhaustion score
            exhaustion = 0.0
            if rsi > 75 or rsi < 25:
                exhaustion += 0.4
            elif rsi > 70 or rsi < 30:
                exhaustion += 0.25
            
            distance = abs(close - ema20) / atr
            if distance > 2.5:
                exhaustion += 0.3
            elif distance > 1.5:
                exhaustion += 0.2
            
            confidence += 0.30 * exhaustion
            
            # RSI extreme bonus
            if is_bullish and rsi < 30:
                confidence += 0.25
            elif is_bullish and rsi < 35:
                confidence += 0.20
            elif not is_bullish and rsi > 70:
                confidence += 0.25
            elif not is_bullish and rsi > 65:
                confidence += 0.20
            
            # Pullback quality
            if 0.3 < distance < 2.0:
                confidence += 0.10
            
            # DMA
            dma = (ema20 - ema50) / atr
            if is_bullish and dma < 0:
                confidence += 0.10
            elif not is_bullish and dma > 0:
                confidence += 0.10
            
            return min(confidence, 1.0)
            
        except Exception as e:
            print(f"Error calculating confidence: {e}")
            return 0.0
    
    def detect_signal(self, df):
        """Check for new divergence signals"""
        if len(df) < 100:
            return None
        
        # Cooldown check
        if self.last_signal_time:
            bars_since_signal = len(df) - self.last_signal_time
            if bars_since_signal < self.signal_cooldown_bars:
                return None
        
        lookback = self.config['PIVOT_LOOKBACK']
        check_index = len(df) - lookback - 1
        
        if check_index < lookback:
            return None
        
        # Check pivots
        is_pivot_high = IndicatorCalculator.find_pivot_high(df['high'], check_index, lookback)
        is_pivot_low = IndicatorCalculator.find_pivot_low(df['low'], check_index, lookback)
        
        if not is_pivot_high and not is_pivot_low:
            return None
        
        # Store pivot
        pivot = {
            'index': check_index,
            'type': 'high' if is_pivot_high else 'low',
            'price': df['high'].iloc[check_index] if is_pivot_high else df['low'].iloc[check_index],
            'rsi': df['rsi'].iloc[check_index],
            'time': df.index[check_index]
        }
        
        self.recent_pivots.append(pivot)
        
        # Keep only recent pivots
        max_lookback = self.config['MAX_LOOKBACK_BARS']
        self.recent_pivots = [p for p in self.recent_pivots 
                              if check_index - p['index'] <= max_lookback]
        
        # Look for divergence
        for prev_pivot in reversed(self.recent_pivots[:-1]):
            if prev_pivot['type'] != pivot['type']:
                continue
            
            bars_between = pivot['index'] - prev_pivot['index']
            if bars_between < 8:
                continue
            
            # BULLISH DIVERGENCE
            if (pivot['type'] == 'low' and
                pivot['price'] < prev_pivot['price'] and
                pivot['rsi'] > prev_pivot['rsi'] and
                pivot['rsi'] < 40):
                
                confidence = self.calculate_confidence(df, check_index, is_bullish=True)
                
                if confidence >= self.config['MIN_CONFIDENCE']:
                    self.last_signal_time = check_index
                    return {
                        'type': 'BULLISH',
                        'time': pivot['time'],
                        'price': pivot['price'],
                        'rsi': pivot['rsi'],
                        'confidence': confidence,
                        'atr': df['atr'].iloc[check_index],
                        'bars_between': bars_between
                    }
            
            # BEARISH DIVERGENCE
            elif (pivot['type'] == 'high' and
                  pivot['price'] > prev_pivot['price'] and
                  pivot['rsi'] < prev_pivot['rsi'] and
                  pivot['rsi'] > 60):
                
                confidence = self.calculate_confidence(df, check_index, is_bullish=False)
                
                if confidence >= self.config['MIN_CONFIDENCE']:
                    self.last_signal_time = check_index
                    return {
                        'type': 'BEARISH',
                        'time': pivot['time'],
                        'price': pivot['price'],
                        'rsi': pivot['rsi'],
                        'confidence': confidence,
                        'atr': df['atr'].iloc[check_index],
                        'bars_between': bars_between
                    }
        
        return None