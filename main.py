#!/usr/bin/env python3
"""
ULTIMATE STOCK SCANNER BOT - FIXED VERSION WITH UNUSUAL OPTIONS
Version: 3.0
Author: Stock Alert System
"""

import os
import time
import requests
import schedule
import json
from datetime import datetime
import pytz
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
from telegram import Bot
from telegram.error import TelegramError

# ========== CONFIGURATION ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment Variables - WITH DEBUG
MBOUM_API_KEY = os.getenv('MBOUM_API_KEY')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# DEBUG: Log what we got
logger.info("=" * 60)
logger.info("BOT STARTING - DEBUG INFO")
logger.info(f"MBOUM_API_KEY exists: {'YES' if MBOUM_API_KEY else 'NO'}")
if MBOUM_API_KEY:
    logger.info(f"MBOUM_API_KEY first 10 chars: {MBOUM_API_KEY[:10]}...")
logger.info(f"TELEGRAM_BOT_TOKEN exists: {'YES' if TELEGRAM_BOT_TOKEN else 'NO'}")
logger.info(f"TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")
logger.info("=" * 60)

# API Configuration
BASE_URL = "https://api.mboum.com"
HEADERS = {'Authorization': f'Bearer {MBOUM_API_KEY}'} if MBOUM_API_KEY else {}

# Bot Configuration
TIMEZONE = pytz.timezone('America/New_York')
SCAN_INTERVAL = 10              # seconds for main scanner
UNUSUAL_OPTIONS_INTERVAL = 60   # seconds (1 minute)
INSIDER_CHECK_INTERVAL = 20     # seconds
TOP10_INTERVAL = 300            # seconds (5 minutes)

# Alert Thresholds
BID_MATCH_PRICE_1 = 199999
BID_MATCH_SHARES_1 = 100
BID_MATCH_PRICE_2 = 2000
BID_MATCH_SHARES_2 = 20
MIN_PERCENT_MOVE = 5
MIN_INSIDER_SHARES = 10000
VOLUME_MULTIPLIERS = {
    (1, 10): 10,
    (10, 50): 20,
    (50, 100): 30,
    (100, 200): 50,
    (200, float('inf')): 100
}

# ========== DATA CLASSES ==========
@dataclass
class StockAlert:
    symbol: str
    alert_type: str
    message: str
    timestamp: datetime
    price: float = 0.0
    percent_change: float = 0.0
    volume: int = 0
    avg_volume: int = 0

@dataclass
class StockData:
    symbol: str
    price: float
    change_percent: float
    volume: int
    bid: float = 0.0
    bid_size: int = 0
    ask: float = 0.0
    ask_size: int = 0
    previous_close: float = 0.0

# ========== API FUNCTIONS - FIXED ==========
def make_api_call(endpoint: str, params: Dict = None) -> Optional[Dict]:
    """Make API call to MBOUM - WITH DEBUG LOGGING"""
    if not MBOUM_API_KEY:
        logger.error("❌ No MBOUM_API_KEY - Cannot make API call")
        return None

    url = f"{BASE_URL}{endpoint}"

    logger.debug(f"📡 API CALL: {endpoint} with params: {params}")

    try:
        start_time = time.time()
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)
        response_time = time.time() - start_time

        logger.info(f"📡 API Response: {endpoint} - Status: {response.status_code} - Time: {response_time:.2f}s")

        if response.status_code == 200:
            try:
                data = response.json()
                logger.debug(f"✅ API Success: Got {len(data) if isinstance(data, list) else 'dict'} items")
                return data
            except json.JSONDecodeError:
                logger.error(f"❌ JSON decode error for {endpoint}")
                return None
        elif response.status_code == 401:
            logger.error("❌ API Error 401: Unauthorized - Check API Key")
            logger.error(f"Headers sent: {HEADERS}")
            return None
        elif response.status_code == 404:
            logger.error(f"❌ API Error 404: Endpoint not found - {endpoint}")
            return None
        else:
            logger.error(f"❌ API Error {response.status_code}: {response.text[:200]}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"⏰ API Timeout: {endpoint}")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"🔌 API Connection Error: {endpoint}")
        return None
    except Exception as e:
        logger.error(f"⚠️ API Exception for {endpoint}: {str(e)}")
        return None

def get_top_movers(limit: int = 50) -> List[StockData]:
    """Get top moving stocks - FIXED ENDPOINT"""
    logger.info(f"🔍 Getting top {limit} movers...")
    data = make_api_call("/v1/markets/movers", {"type": "STOCKS"})

    if not data:
        logger.warning("❌ No data returned from movers API")
        return []

    if data and len(data) > 0:
        logger.debug(f"Raw first item: {json.dumps(data[0])[:200]}...")

    stocks: List[StockData] = []
    count = 0

    for item in data[:limit]:
        try:
            symbol = item.get('symbol') or item.get('ticker') or item.get('Symbol')
            price = item.get('price') or item.get('lastPrice') or item.get('last') or 0
            change_percent = item.get('changePercent') or item.get('percentChange') or item.get('change') or 0
            volume = item.get('volume') or item.get('Volume') or 0
            prev_close = item.get('previousClose') or item.get('prevClose') or 0

            if not symbol:
                logger.warning(f"Skipping item without symbol: {item}")
                continue

            stock = StockData(
                symbol=str(symbol),
                price=float(price),
                change_percent=float(change_percent),
                volume=int(volume),
                previous_close=float(prev_close)
            )
            stocks.append(stock)
            count += 1

        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing stock data {item}: {e}")
            continue

    logger.info(f"✅ Parsed {count} stocks from API")
    return stocks

def get_real_time_quote(symbol: str) -> Optional[StockData]:
    """Get real-time quote with bid/ask data"""
    logger.info(f"📊 Getting real-time quote for {symbol}...")
    data = make_api_call("/v1/markets/quote", {"ticker": symbol, "type": "STOCKS"})

    if not data:
        logger.warning(f"❌ No quote data for {symbol}")
        return None

    try:
        logger.debug(f"Raw quote data: {json.dumps(data)[:300]}...")

        symbol_val = data.get('symbol') or data.get('ticker') or symbol
        price = data.get('price') or data.get('lastPrice') or data.get('last') or 0
        change_percent = data.get('changePercent') or data.get('percentChange') or data.get('change') or 0
        volume = data.get('volume') or data.get('Volume') or 0
        bid = data.get('bid') or data.get('bidPrice') or data.get('bid') or 0
        bid_size = data.get('bidSize') or data.get('bidQuantity') or data.get('bidSize') or 0
        ask = data.get('ask') or data.get('askPrice') or data.get('ask') or 0
        ask_size = data.get('askSize') or data.get('askQuantity') or data.get('askSize') or 0
        prev_close = data.get('previousClose') or data.get('prevClose') or 0

        stock = StockData(
            symbol=str(symbol_val),
            price=float(price),
            change_percent=float(change_percent),
            volume=int(volume),
            bid=float(bid),
            bid_size=int(bid_size),
            ask=float(ask),
            ask_size=int(ask_size),
            previous_close=float(prev_close)
        )

        logger.info(f"✅ Got quote for {symbol_val}: ${stock.price} ({stock.change_percent}%)")
        return stock

    except (ValueError, TypeError) as e:
        logger.error(f"❌ Error parsing quote for {symbol}: {e}")
        logger.error(f"Raw data: {data}")
        return None

def get_unusual_options() -> List[Dict]:
    """Get unusual options activity"""
    logger.info("🎯 Getting unusual options...")
    data = make_api_call(
        "/v1/markets/options/unusual-options-activity",
        {"type": "STOCKS", "page": 1}
    )

    if data and len(data) > 0:
        logger.info(f"✅ Found {len(data)} unusual options activities")
    else:
        logger.info("ℹ️ No unusual options found")

    return data if data else []

def get_insider_trades(symbol: str = None) -> List[Dict]:
    """Get insider trades, optionally filtered by symbol"""
    params = {"minValue": "10000", "page": 1, "limit": 20}
    if symbol:
        params["ticker"] = symbol
        logger.info(f"👔 Getting insider trades for {symbol}...")
    else:
        logger.info("👔 Getting all insider trades...")

    data = make_api_call("/v1/markets/insider-trades", params)

    if data and len(data) > 0:
        logger.info(f"✅ Found {len(data)} insider trades")
    else:
        logger.info("ℹ️ No insider trades found")

    return data if data else []

def get_market_info(symbol: str = None) -> Optional[Dict]:
    """Get market/halt status (if supported)"""
    params = {}
    if symbol:
        params["symbol"] = symbol
        logger.info(f"⏸️ Getting market info for {symbol}...")
    else:
        logger.info("🏛️ Getting general market info...")

    data = make_api_call("/v2/market-info", params)
    return data

def get_top10_gainers() -> List[StockData]:
    """Get top 10 gainers for scheduled report"""
    return get_top_movers(10)

# ========== ALERT DETECTION ==========
class AlertDetector:
    def __init__(self):
        self.alert_history: Dict[str, datetime] = {}
        self.watchlist = set()
        self.volume_history: Dict[str, Dict] = {}

    def check_bid_match(self, stock: StockData) -> bool:
        """Check for bid match patterns"""
        logger.debug(f"Checking bid match for {stock.symbol}: bid=${stock.bid}, size={stock.bid_size}")

        if stock.bid == BID_MATCH_PRICE_1 and stock.bid_size == BID_MATCH_SHARES_1:
            logger.info(f"🎯 EXACT BID MATCH: {stock.symbol} - ${stock.bid} with {stock.bid_size} shares")
            return True

        elif stock.bid >= BID_MATCH_PRICE_2 and stock.bid_size >= BID_MATCH_SHARES_2:
            logger.info(f"🎯 HIGH VALUE BID: {stock.symbol} - ${stock.bid} with {stock.bid_size} shares")
            return True

        return False

    def check_volume_spike(self, symbol: str, current_volume: int, percent_change: float) -> bool:
        """Check for volume spike using smart thresholds"""
        if symbol not in self.volume_history:
            self.volume_history[symbol] = {'samples': [], 'avg': 10000}

        self.volume_history[symbol]['samples'].append(current_volume)
        if len(self.volume_history[symbol]['samples']) > 30:
            self.volume_history[symbol]['samples'].pop(0)

        if len(self.volume_history[symbol]['samples']) > 5:
            avg = sum(self.volume_history[symbol]['samples']) / len(self.volume_history[symbol]['samples'])
            self.volume_history[symbol]['avg'] = avg

            multiplier = 10
            for (min_p, max_p), mult in VOLUME_MULTIPLIERS.items():
                if min_p <= abs(percent_change) < max_p:
                    multiplier = mult
                    break

            if avg > 0 and current_volume > (avg * multiplier):
                logger.info(f"📊 VOLUME SPIKE: {symbol} - {current_volume:,} vs avg {int(avg):,} ({multiplier}x)")
                return True

        return False

    def check_insider_activity(self, symbol: str) -> Optional[Dict]:
        """Check for large insider trades"""
        insider_trades = get_insider_trades(symbol)
        for trade in insider_trades:
            try:
                shares = int(trade.get('shares', 0))
            except (TypeError, ValueError):
                continue
            if shares >= MIN_INSIDER_SHARES:
                logger.info(f"👔 LARGE INSIDER TRADE: {symbol} - {shares:,} shares")
                return trade
        return None

    def can_send_alert(self, symbol: str, alert_type: str) -> bool:
        """Prevent duplicate alerts"""
        key = f"{symbol}_{alert_type}"
        now = datetime.now()

        if key in self.alert_history:
            last_alert = self.alert_history[key]
            if (now - last_alert).total_seconds() < 300:
                logger.debug(f"⏸️ Alert cooldown: {symbol} - {alert_type}")
                return False

        self.alert_history[key] = now
        return True

# ========== TELEGRAM FUNCTIONS ==========
class TelegramBot:
    def __init__(self):
        if TELEGRAM_BOT_TOKEN:
            self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
            logger.info("✅ Telegram bot initialized")
        else:
            self.bot = None
            logger.warning("❌ Telegram bot token not set")

        self.chat_id = TELEGRAM_CHAT_ID

    def send_message(self, message: str, alert_type: str = "INFO"):
        """Send message to Telegram"""
        if not self.bot or not self.chat_id:
            logger.warning(f"Telegram not configured. Would send: {alert_type} - {message[:50]}...")
            return False

        try:
            emojis = {
                "STARTUP": "☀️",
                "BID_MATCH": "⚡",
                "VOLUME_SPIKE": "📊",
                "UNUSUAL_ACTIVITY": "🚨",
                "UNUSUAL_OPTIONS": "🎯",
                "HALT_ALERT": "⏸️",
                "LARGE_SALE": "📉",
                "TOP10": "🏆",
                "ERROR": "❌",
                "DEBUG": "🔧"
            }

            emoji = emojis.get(alert_type, "ℹ️")
            formatted_msg = f"{emoji} {message}"

            self.bot.send_message(chat_id=self.chat_id, text=formatted_msg)
            logger.info(f"📤 Telegram sent: {alert_type}")
            return True

        except TelegramError as e:
            logger.error(f"❌ Telegram send error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Telegram exception: {str(e)}")
            return False

# ========== MAIN BOT CLASS ==========
class StockScannerBot:
    def __init__(self):
        self.detector = AlertDetector()
        self.telegram = TelegramBot()
        self.is_running = False
        self.market_open = False
        self.startup_sent = False

    def check_market_hours(self) -> bool:
        """Check if within market hours (6 AM - 6 PM EST, Mon-Fri)"""
        now_est = datetime.now(TIMEZONE)

        logger.debug(
            f"Current EST: {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"- Weekday: {now_est.weekday()} - Hour: {now_est.hour}"
        )

        if now_est.weekday() >= 5:
            logger.info("📅 Weekend - Market closed")
            return False

        current_hour = now_est.hour
        if 6 <= current_hour < 18:
            logger.debug(f"✅ Market open: {current_hour}:00 EST")
            return True
        else:
            logger.debug(f"⏰ Market closed: {current_hour}:00 EST (6AM-6PM only)")
            return False

    def startup(self):
        """Send startup message at 6 AM"""
        if not self.startup_sent and self.market_open:
            self.telegram.send_message(
                "Good Morning its 6am Bot is running now",
                "STARTUP"
            )
            self.startup_sent = True
            logger.info("✅ Startup message sent")

            self.telegram.send_message(
                f"Bot initialized successfully\n"
                f"Time: {datetime.now(TIMEZONE).strftime('%I:%M %p EST')}\n"
                f"Scan interval: {SCAN_INTERVAL}s\n"
                f"API Key: {'✅ Set' if MBOUM_API_KEY else '❌ Missing'}",
                "DEBUG"
            )

    def scan_top_movers(self):
        """Main scanner - runs every 10 seconds"""
        if not self.market_open:
            logger.debug("⏸️ Market closed, skipping scan")
            return

        logger.info("🔍 Scanning top 50 movers...")
        stocks = get_top_movers(50)

        if not stocks:
            logger.warning("⚠️ No stocks returned from API")
            return

        logger.info(f"📈 Processing {len(stocks)} stocks...")

        for stock in stocks:
            if stock.change_percent >= MIN_PERCENT_MOVE:
                logger.info(f"📈 {stock.symbol} up {stock.change_percent:.1f}% - Processing...")
                self.process_5percent_mover(stock)
            else:
                logger.debug(f"  {stock.symbol}: {stock.change_percent:.1f}% (below {MIN_PERCENT_MOVE}%)")

            if self.detector.check_volume_spike(stock.symbol, stock.volume, stock.change_percent):
                if self.detector.can_send_alert(stock.symbol, "VOLUME_SPIKE"):
                    self.send_volume_alert(stock)

    def process_5percent_mover(self, stock: StockData):
        """Process stocks that moved 5%+"""
        logger.info(f"🔎 Processing {stock.symbol} at {stock.change_percent:.1f}%")

        quote = get_real_time_quote(stock.symbol)
        if not quote:
            logger.warning(f"⚠️ Could not get quote for {stock.symbol}")
            return

        if self.detector.check_bid_match(quote):
            if self.detector.can_send_alert(stock.symbol, "BID_MATCH"):
                self.send_bid_match_alert(quote)
                self.check_halt_status(quote.symbol)
                self.detector.watchlist.add(quote.symbol)
                logger.info(f"✅ Bid match processed for {stock.symbol}")
                return
        else:
            logger.debug(f"  No bid match for {stock.symbol}")

        insider_trade = self.detector.check_insider_activity(stock.symbol)
        if insider_trade and self.detector.can_send_alert(stock.symbol, "UNUSUAL_ACTIVITY"):
            self.send_unusual_activity_alert(stock, insider_trade)
            logger.info(f"✅ Unusual activity alert for {stock.symbol}")

    def scan_unusual_options(self):
        """Scan unusual options - runs every 1 minute"""
        if not self.market_open:
            return

        logger.info("🎯 Scanning unusual options...")
        options_data = get_unusual_options()

        for option in options_data[:10]:
            symbol = option.get('symbol', '')
            if symbol and self.detector.can_send_alert(symbol, "UNUSUAL_OPTIONS"):
                self.send_unusual_options_alert(option)
                logger.info(f"✅ Unusual options alert for {symbol}")

    def scan_insider_trades(self):
        """Scan insider trades - runs every 20 seconds (per-symbol handled elsewhere)"""
        if not self.market_open:
            return
        # Kept minimal; main insider logic is per 5% mover.

    def check_halt_status(self, symbol: str):
        """Check if stock is halted (if API supports it)"""
        market_info = get_market_info(symbol)
        if market_info and market_info.get('halted', False):
            if self.detector.can_send_alert(symbol, "HALT_ALERT"):
                self.send_halt_alert(symbol)
                logger.info(f"⏸️ Halt alert for {symbol}")

    def check_watchlist_sales(self):
        """Check watchlist stocks for large sales"""
        if not self.market_open:
            return

        if not self.detector.watchlist:
            return

        logger.info(f"👀 Checking {len(self.detector.watchlist)} watchlist stocks for sales...")

        for symbol in list(self.detector.watchlist):
            insider_trades = get_insider_trades(symbol)
            for trade in insider_trades:
                if trade.get('transactionType', '').upper() == 'SELL':
                    try:
                        shares = int(trade.get('shares', 0))
                    except (TypeError, ValueError):
                        continue
                    if shares >= MIN_INSIDER_SHARES:
                        if self.detector.can_send_alert(symbol, "LARGE_SALE"):
                            self.send_large_sale_alert(symbol, trade)
                            logger.info(f"📉 Large sale alert for {symbol}")

    def send_top10_report(self):
        """Send top 10 gainers report - runs every 5 minutes"""
        if not self.market_open:
            return

        logger.info("🏆 Sending top 10 report...")
        gainers = get_top10_gainers()

        if gainers:
            now_est = datetime.now(TIMEZONE).strftime("%I:%M %p EST")
            message = f"TOP 10 GAINERS ({now_est})\n\n"

            for i, stock in enumerate(gainers[:10], 1):
                message += f"{i}. {stock.symbol}: ${stock.price:.2f} (+{stock.change_percent:.1f}%)\n"

            self.telegram.send_message(message, "TOP10")
            logger.info("✅ Top 10 report sent")
        else:
            logger.warning("⚠️ No gainers data for top 10 report")

    # ========== ALERT MESSAGES ==========
    def send_bid_match_alert(self, stock: StockData):
        message = (
            f"BID MATCH ALERT: {stock.symbol}\n"
            f"Price: ${stock.price:.2f} (+{stock.change_percent:.1f}%)\n"
            f"Bid: {stock.bid_size} shares @ ${stock.bid:,.2f}\n"
            f"Total: ${stock.bid * stock.bid_size:,.2f}\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}"
        )
        self.telegram.send_message(message, "BID_MATCH")

    def send_volume_alert(self, stock: StockData):
        avg = self.detector.volume_history[stock.symbol]['avg']
        multiplier = stock.volume / avg if avg > 0 else 0

        message = (
            f"VOLUME SPIKE: {stock.symbol}\n"
            f"Price: ${stock.price:.2f} (+{stock.change_percent:.1f}%)\n"
            f"Volume: {stock.volume:,} shares ({multiplier:.1f}x average)\n"
            f"Average: {int(avg):,} shares\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}"
        )
        self.telegram.send_message(message, "VOLUME_SPIKE")

    def send_unusual_activity_alert(self, stock: StockData, insider_trade: Dict):
        shares = int(insider_trade.get('shares', 0) or 0)
        price = float(insider_trade.get('price', 0) or 0)
        total = shares * price

        message = (
            f"UNUSUAL ACTIVITY: {stock.symbol}\n"
            f"Price: ${stock.price:.2f} (+{stock.change_percent:.1f}%)\n"
            f"Insider: {insider_trade.get('insider', '')}\n"
            f"Transaction: {shares:,} shares @ ${price:.2f}\n"
            f"Total: ${total:,.2f}\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}\n"
            f"Note: Stock up {stock.change_percent:.1f}% but no bid match"
        )
        self.telegram.send_message(message, "UNUSUAL_ACTIVITY")

    def send_unusual_options_alert(self, option: Dict):
        message = (
            f"UNUSUAL OPTIONS: {option.get('symbol', '')}\n"
            f"Contract: {option.get('contractType', '')} ${option.get('strike', 0)}\n"
            f"Expiry: {option.get('expiration', '')}\n"
            f"Volume: {option.get('volume', 0):,} contracts\n"
            f"Open Interest: {option.get('openInterest', 0):,}\n"
            f"Premium: ${option.get('premium', 0):,.2f}\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}"
        )
        self.telegram.send_message(message, "UNUSUAL_OPTIONS")

    def send_halt_alert(self, symbol: str):
        message = (
            f"HALT ALERT: {symbol}\n"
            f"Stock halted after bid match\n"
            f"Halt Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}"
        )
        self.telegram.send_message(message, "HALT_ALERT")

    def send_large_sale_alert(self, symbol: str, trade: Dict):
        shares = int(trade.get('shares', 0) or 0)
        price = float(trade.get('price', 0) or 0)
        total = shares * price

        message = (
            f"LARGE SALE: {symbol}\n"
            f"Sold: {shares:,} shares @ ${price:.2f}\n"
            f"Total: ${total:,.2f}\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M:%S %p EST')}\n"
            f"Note: This stock had bid match earlier"
        )
        self.telegram.send_message(message, "LARGE_SALE")

    # ========== SCHEDULER ==========
    def setup_schedule(self):
        """Setup all scheduled tasks"""
        logger.info("🕐 Setting up scheduler...")

        schedule.every(SCAN_INTERVAL).seconds.do(self.scan_top_movers)
        logger.info(f"  - Main scanner: every {SCAN_INTERVAL}s")

        schedule.every(UNUSUAL_OPTIONS_INTERVAL).seconds.do(self.scan_unusual_options)
        logger.info(f"  - Unusual options: every {UNUSUAL_OPTIONS_INTERVAL}s")

        schedule.every(INSIDER_CHECK_INTERVAL).seconds.do(self.scan_insider_trades)
        logger.info(f"  - Insider trades: every {INSIDER_CHECK_INTERVAL}s")

        schedule.every(TOP10_INTERVAL).seconds.do(self.send_top10_report)
        logger.info(f"  - Top 10 report: every {TOP10_INTERVAL}s")

        schedule.every(30).seconds.do(self.check_watchlist_sales)
        logger.info("  - Watchlist sales: every 30s")

        schedule.every(60).seconds.do(self.check_market_status)
        logger.info("  - Market status: every 60s")

        logger.info("✅ Scheduler setup complete")

    def check_market_status(self):
        """Check if market is open"""
        was_open = self.market_open
        self.market_open = self.check_market_hours()

        if was_open and not self.market_open:
            logger.info("🏁 Market closed. Stopping scans.")
            self.is_running = False
            self.startup_sent = False
            self.telegram.send_message(
                "Market closed. Bot going to sleep until 6 AM EST tomorrow.",
                "DEBUG"
            )
        elif not was_open and self.market_open:
            logger.info("🚀 Market opened. Starting scans.")
            self.is_running = True
            self.startup()
        elif self.market_open and not self.startup_sent:
            self.startup()

    def run(self):
        """Main bot loop"""
        logger.info("=" * 60)
        logger.info("🚀 STARTING STOCK SCANNER BOT")
        logger.info("=" * 60)

        self.check_market_status()
        self.setup_schedule()

        self.telegram.send_message(
            f"Bot started\n"
            f"Time: {datetime.now(TIMEZONE).strftime('%I:%M %p EST')}\n"
            f"Market: {'OPEN' if self.market_open else 'CLOSED'}\n"
            f"Next scan: {'IMMEDIATE' if self.market_open else '6 AM EST'}",
            "DEBUG"
        )

        logger.info("🔄 Entering main loop...")
        while True:
            try:
                if self.is_running:
                    schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("🛑 Bot stopped by user")
                self.telegram.send_message("Bot manually stopped", "DEBUG")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in main loop: {str(e)}")
                self.telegram.send_message(f"Bot error: {str(e)[:100]}", "ERROR")
                time.sleep(10)

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    required_vars = ['MBOUM_API_KEY', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Missing environment variables: {missing_vars}")
        print("ERROR: Missing environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nSet them in Render Environment Variables or .env file")
    else:
        bot = StockScannerBot()
        bot.run()
