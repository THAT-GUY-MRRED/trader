import discord
import asyncio
from datetime import datetime


class DiscordNotifier:
    """Send trading updates to Discord"""
    
    def __init__(self, token, channel_id):
        self.token = token
        self.channel_id = int(channel_id)
        self.client = None
        self.channel = None
        self.ready = False
        
    async def start(self):
        """Start Discord bot"""
        intents = discord.Intents.default()
        self.client = discord.Client(intents=intents)
        
        @self.client.event
        async def on_ready():
            print(f"✓ Discord bot connected as {self.client.user}")
            self.channel = self.client.get_channel(self.channel_id)
            if self.channel:
                self.ready = True
                await self.send_startup_message()
            else:
                print(f"❌ Channel {self.channel_id} not found")
        
        asyncio.create_task(self.client.start(self.token))
        
        for _ in range(10):
            if self.ready:
                break
            await asyncio.sleep(1)
    
    async def send_startup_message(self):
        """Send bot startup notification"""
        embed = discord.Embed(
            title="🤖 BZ-CAE Trading Bot Started",
            description="Monitoring BTC/USD for divergence signals",
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="Symbol", value="BTC/USD", inline=True)
        embed.add_field(name="Timeframe", value="5 Minutes", inline=True)
        embed.add_field(name="Status", value="Collecting initial candles...", inline=False)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def send_signal(self, signal, account_balance):
        """Send signal detected notification"""
        if not self.ready:
            return
        
        color = discord.Color.green() if signal['type'] == 'BULLISH' else discord.Color.red()
        emoji = "🟢" if signal['type'] == 'BULLISH' else "🔴"
        
        embed = discord.Embed(
            title=f"{emoji} {signal['type']} DIVERGENCE DETECTED",
            description=f"Confidence: **{signal['confidence']:.0%}**",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="Price", value=f"${signal['price']:,.2f}", inline=True)
        embed.add_field(name="RSI", value=f"{signal['rsi']:.1f}", inline=True)
        embed.add_field(name="ATR", value=f"${signal['atr']:.2f}", inline=True)
        embed.add_field(name="Account Balance", value=f"${account_balance:,.2f}", inline=False)
        
        await self.channel.send(embed=embed)
    
    async def send_trading_enabled(self, num_candles):
        """Send trading enabled notification"""
        if not self.ready:
            return
        
        embed = discord.Embed(
            title="✅ Trading Enabled",
            description=f"Collected {num_candles} candles - Now monitoring for signals",
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        
        await self.channel.send(embed=embed)
    
    async def send_account_update(self, account_info, positions):
        """Send periodic account update"""
        if not self.ready:
            return
        
        embed = discord.Embed(
            title="📊 Account Status Update",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="Portfolio Value", value=f"${float(account_info.portfolio_value):,.2f}", inline=True)
        embed.add_field(name="Cash", value=f"${float(account_info.cash):,.2f}", inline=True)
        embed.add_field(name="Buying Power", value=f"${float(account_info.buying_power):,.2f}", inline=True)
        
        if positions:
            pos_text = ""
            for p in positions[:3]:
                pos_text += f"{p.symbol}: {p.qty} @ ${p.avg_entry_price} (P/L: ${float(p.unrealized_pl):+,.2f})\n"
            embed.add_field(name="Open Positions", value=pos_text or "None", inline=False)
        else:
            embed.add_field(name="Open Positions", value="None", inline=False)
        
        await self.channel.send(embed=embed)
    
    async def close(self):
        """Close Discord connection"""
        if self.client:
            await self.client.close()