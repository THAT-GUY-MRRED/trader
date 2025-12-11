import asyncio
import time
from alpaca.trading.client import TradingClient

from config import CONFIG
from utils import load_all_keys
from live_data_collector import LiveDataCollector
from discord_notifier import DiscordNotifier
from signal_detector import SignalDetector
from indicator_calculator import IndicatorCalculator


class IntegratedLiveTrader:
    """Complete trading bot with Discord and live data"""
    
    def __init__(self, config):
        self.config = config
        
        keys = load_all_keys()
        api_key = keys.get('ALPACA_API_KEY')
        api_secret = keys.get('ALPACA_SECRET_KEY')
        
        if not api_key or not api_secret:
            raise ValueError("API keys not found in keys.env")
        
        self.trading_client = TradingClient(api_key, api_secret, paper=True)
        self.data_collector = LiveDataCollector(api_key, api_secret, config['SYMBOL'])
        
        self.discord = None
        if config.get('ENABLE_DISCORD'):
            discord_token = keys.get('DISCORD_TOKEN')
            discord_channel = keys.get('DISCORD_CHANNEL_ID')
            if discord_token and discord_channel:
                self.discord = DiscordNotifier(discord_token, discord_channel)
        
        self.trading_enabled = False
        self.running = False
        self.last_discord_update = time.time()
        self.signal_detector = None
    
    async def initialize(self):
        """Initialize bot"""
        print("\n" + "="*70)
        print("BZ-CAE INTEGRATED LIVE TRADING BOT")
        print("="*70)
        print(f"Symbol: {self.config['SYMBOL']}")
        print(f"Min Candles Required: {self.config['MIN_CANDLES_REQUIRED']}")
        print(f"Discord: {self.config.get('ENABLE_DISCORD', False)}")
        print(f"DRY RUN: {self.config['DRY_RUN']}")
        print(f"LOG ONLY: {self.config['LOG_SIGNALS_ONLY']}")
        print("="*70 + "\n")
        
        if self.discord:
            try:
                await self.discord.start()
            except Exception as e:
                print(f"⚠️  Discord failed: {e}")
                self.discord = None
        
        try:
            account = self.trading_client.get_account()
            print(f"✓ Connected to Alpaca Paper Trading")
            print(f"  Portfolio: ${float(account.portfolio_value):,.2f}\n")
        except Exception as e:
            print(f"❌ Alpaca Error: {e}")
            raise
    
    async def run(self):
        """Main bot loop"""
        self.running = True
        await self.initialize()
        
        self.data_collector.start_collection()
        
        print(f"📊 Collecting {self.config['MIN_CANDLES_REQUIRED']} candles...")
        print(f"   ~{self.config['MIN_CANDLES_REQUIRED'] * 5} minutes")
        print("   Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                new_candle = self.data_collector.update()
                
                if not self.trading_enabled:
                    candle_count = len(self.data_collector.candles)
                    if candle_count > 0 and candle_count % 5 == 0:
                        print(f"   Progress: {candle_count}/{self.config['MIN_CANDLES_REQUIRED']} candles...")
                    
                    if self.data_collector.has_minimum_candles(self.config['MIN_CANDLES_REQUIRED']):
                        print("\n" + "="*70)
                        print("✅ TRADING ENABLED")
                        print("="*70 + "\n")
                        
                        self.trading_enabled = True
                        self.signal_detector = SignalDetector(self.config)
                        
                        if self.discord:
                            await self.discord.send_trading_enabled(len(self.data_collector.candles))
                
                if self.trading_enabled and new_candle:
                    await self.on_new_candle()
                
                if self.discord and self.trading_enabled:
                    if time.time() - self.last_discord_update > self.config.get('DISCORD_UPDATE_INTERVAL', 300):
                        await self.send_status_update()
                        self.last_discord_update = time.time()
                
                await asyncio.sleep(self.config['DATA_CHECK_INTERVAL'])
                
        except KeyboardInterrupt:
            print("\n⚠️  Shutting down...")
            await self.stop()
    
    async def on_new_candle(self):
        """Process new completed candle"""
        df = self.data_collector.get_dataframe()
        
        if df is None or len(df) < 100:
            return
        
        try:
            df['rsi'] = IndicatorCalculator.calculate_rsi(df['close'], self.config['RSI_PERIOD'])
            df['atr'] = IndicatorCalculator.calculate_atr(df, self.config['ATR_PERIOD'])
            df['ema20'] = IndicatorCalculator.calculate_ema(df['close'], self.config['EMA_FAST'])
            df['ema50'] = IndicatorCalculator.calculate_ema(df['close'], self.config['EMA_MID'])
            df['ema100'] = IndicatorCalculator.calculate_ema(df['close'], self.config['EMA_SLOW'])
        except Exception as e:
            print(f"⚠️  Indicator error: {e}")
            return
        
        current_price = df['close'].iloc[-1]
        current_rsi = df['rsi'].iloc[-1]
        current_time = df.index[-1]
        
        print(f"[{current_time}] ${current_price:,.2f} | RSI: {current_rsi:.1f}")
        
        if self.signal_detector:
            try:
                signal = self.signal_detector.detect_signal(df)
                
                if signal:
                    print(f"\n{'='*70}")
                    print(f"🚨 SIGNAL DETECTED!")
                    print(f"{'='*70}")
                    print(f"  Type: {signal['type']}")
                    print(f"  Confidence: {signal['confidence']:.0%}")
                    print(f"  Price: ${signal['price']:,.2f}")
                    print(f"  RSI: {signal['rsi']:.1f}")
                    print(f"{'='*70}\n")
                    
                    if self.discord:
                        try:
                            account = self.trading_client.get_account()
                            await self.discord.send_signal(signal, float(account.equity))
                        except:
                            pass
                    
                    if self.config['LOG_SIGNALS_ONLY']:
                        print("  [LOG ONLY - No trade]\n")
            except Exception as e:
                print(f"⚠️  Signal error: {e}")
    
    async def send_status_update(self):
        """Send periodic status update to Discord"""
        if not self.discord:
            return
        
        try:
            account = self.trading_client.get_account()
            positions = self.trading_client.get_all_positions()
            await self.discord.send_account_update(account, positions)
        except Exception as e:
            print(f"⚠️  Discord update failed: {e}")
    
    async def stop(self):
        """Gracefully stop bot"""
        self.running = False
        
        if self.discord:
            try:
                await self.discord.close()
            except:
                pass
        
        print("\n✓ Bot stopped")


async def main():
    """Main entry point"""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          BZ-CAE LIVE TRADING BOT v3.0 - ALL BUGS FIXED               ║
╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    bot = IntegratedLiveTrader(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()