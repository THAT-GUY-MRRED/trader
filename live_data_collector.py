import pandas as pd
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest
from datetime import datetime, timedelta, timezone


class LiveDataCollector:
    """Collect live 5-minute OHLCV candles"""
    
    def __init__(self, api_key, api_secret, symbol='BTC/USD'):
        self.client = CryptoHistoricalDataClient(api_key, api_secret)
        self.symbol = symbol
        self.candles = []
        self.current_candle = None
        self.candle_start = None
        self.interval = timedelta(minutes=5)
        
    def start_collection(self):
        """Start collecting candles"""
        print(f"🚀 Starting live 5-minute candle collection for {self.symbol}")
        self.candle_start = datetime.now(timezone.utc)
        self.current_candle = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': 0
        }
    
    def update(self):
        """Update current candle with latest quote"""
        try:
            request = CryptoLatestQuoteRequest(symbol_or_symbols=self.symbol)
            quote = self.client.get_crypto_latest_quote(request).get(self.symbol)
            
            if not quote:
                return False
            
            price = (quote.bid_price + quote.ask_price) / 2
            vol = (quote.bid_size + quote.ask_size) / 2
            
            now = datetime.now(timezone.utc)
            
            if now - self.candle_start >= self.interval:
                if self.current_candle['open'] is not None:
                    candle_data = {
                        'timestamp': self.candle_start,
                        'open': self.current_candle['open'],
                        'high': self.current_candle['high'],
                        'low': self.current_candle['low'],
                        'close': self.current_candle['close'],
                        'volume': round(self.current_candle['volume'], 6)
                    }
                    self.candles.append(candle_data)
                    
                    print(f"✓ Candle #{len(self.candles)}: "
                          f"O:{candle_data['open']:.2f} H:{candle_data['high']:.2f} "
                          f"L:{candle_data['low']:.2f} C:{candle_data['close']:.2f}")
                    
                    self.candle_start = now
                    self.current_candle = {
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': vol
                    }
                    return True
                else:
                    self.candle_start = now
                    self.current_candle = {
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': vol
                    }
            else:
                if self.current_candle['open'] is None:
                    self.current_candle['open'] = price
                    self.current_candle['high'] = price
                    self.current_candle['low'] = price
                else:
                    self.current_candle['high'] = max(self.current_candle['high'], price)
                    self.current_candle['low'] = min(self.current_candle['low'], price)
                
                self.current_candle['close'] = price
                self.current_candle['volume'] += vol
            
            return False
            
        except Exception as e:
            print(f"⚠ Error fetching quote: {e}")
            return False
    
    def get_dataframe(self):
        """Get candles as DataFrame"""
        if not self.candles:
            return None
        
        df = pd.DataFrame(self.candles)
        df.set_index('timestamp', inplace=True)
        return df
    
    def has_minimum_candles(self, min_count):
        """Check if we have enough candles"""
        return len(self.candles) >= min_count