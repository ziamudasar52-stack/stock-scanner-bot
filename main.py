import os
import time
import logging
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# ================== CONFIG ==================
MBOUM_API_KEY = os.getenv("MBOUM_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BASE_URL = "https://mboum.com/api/v1"

# Original timing from your base logic
MAIN_SCAN_INTERVAL_SECONDS = 10         # continuous scanner
TOP10_INTERVAL_SECONDS = 300            # 5 minutes
UNUSUAL_OPTIONS_INTERVAL_SECONDS = 120  # 2 minutes
WATCHLIST_INTERVAL_SECONDS = 30         # 30 seconds
MARKET_STATUS_INTERVAL_SECONDS = 1800   # 30 minutes
DARK_POOL_INTERVAL_SECONDS = 120        # 2 minutes (additional feature)
GAPUP_INTERVAL_SECONDS = 120            # 2 minutes (Gap-Up Alerts moved here)

# Thresholds
VOLUME_SPIKE_RATIO = 2.0
PCT_CHANGE_THRESHOLD = 5.0
LARGE_SALE_VOLUME_DELTA = 10000
UNUSUAL_OPTIONS_VOL_OI_MIN = 30.0
UNUSUAL_OPTIONS_VOLUME_MIN = 3000
GAP_UP_THRESHOLD = 5.0
PREMARKET_GAP_THRESHOLD = 3.0
AFTER_HOURS_MOVE_THRESHOLD = 3.0

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================== TELEGRAM ==================
def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram credentials missing; skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            logging.error(f"Telegram send error {resp.status_code}: {resp.text}")
    except Exception as e:
        logging.error(f"Telegram send exception: {e}")

# ================== HTTP HELPERS ==================
def mboum_get(path: str, params: dict | None = None) -> dict | None:
    if params is None:
        params = {}
    params["apikey"] = MBOUM_API_KEY
    url = f"{BASE_URL}{path}"
    try:
        resp = requests.get(url, params=params, timeout=15)
        logging.info(f"📡 API Response: {path} - Status: {resp.status_code}")
        if resp.status_code != 200:
            logging.error(f"❌ API Error {resp.status_code}: {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        logging.error(f"❌ API Exception for {path}: {e}")
        return None

# ================== DATA FETCHERS ==================
def get_screener_day_gainers() -> list:
    logging.info("🔍 Getting day_gainers screener...")
    data = mboum_get("/markets/screener", {"list": "day_gainers"})
    if not data or "body" not in data:
        logging.warning("⚠️ No screener data")
        return []
    return data["body"]

def get_markets_movers() -> dict:
    logging.info("🔍 Getting markets movers...")
    data = mboum_get("/markets/movers")
    if not data or "body" not in data:
        logging.warning("⚠️ No movers data")
        return {}
    return data["body"]

def get_unusual_options_activity() -> list:
    logging.info("🔍 Getting unusual options activity...")
    data = mboum_get("/markets/options/unusual-options-activity")
    if not data or "body" not in data:
        logging.warning("⚠️ No unusual options data")
        return []
    return data["body"]

def get_quotes(symbols: list[str]) -> list:
    if not symbols:
        return []
    sym_str = ",".join(sorted(set(symbols)))
    logging.info(f"🔍 Getting quotes for: {sym_str}")
    data = mboum_get("/qu/quote", {"symbol": sym_str})
    if not data or "body" not in data:
        logging.warning("⚠️ No quote data")
        return []
    return data["body"]

def get_dark_pool_spy() -> list:
    logging.info("🔍 Getting dark pool prints for SPY...")
    data = mboum_get("/markets/dark-pools", {"symbol": "SPY"})
    if not data or "body" not in data:
        logging.warning("⚠️ No dark pool data (SPY) or endpoint not available")
        return []
    return data["body"]

# ================== ANALYTICS HELPERS ==================
def compute_trend_label(quote: dict) -> str:
    price = quote.get("regularMarketPrice")
    fifty = quote.get("fiftyDayAverage")
    twohundred = quote.get("twoHundredDayAverage")
    if price is None or fifty is None or twohundred is None:
        return "Trend: N/A"
    if price > fifty and price > twohundred:
        return "Trend: 📈 Strong Uptrend"
    if price > fifty and price <= twohundred:
        return "Trend: ↗ Short-term Up, Long-term Flat/Down"
    if price < fifty and price > twohundred:
        return "Trend: ↘ Short-term Down, Long-term Up"
    if price < fifty and price < twohundred:
        return "Trend: 📉 Downtrend"
    return "Trend: Mixed"

def compute_sentiment_score(quote: dict, extra_weight: float = 0.0) -> str:
    pct = quote.get("regularMarketChangePercent", 0.0) or 0.0
    vol = quote.get("regularMarketVolume", 0.0) or 0.0
    avg3m = quote.get("averageDailyVolume3Month", 0.0) or 1.0
    vol_ratio = vol / avg3m if avg3m > 0 else 1.0

    score = pct * 0.6 + (vol_ratio - 1.0) * 20 * 0.3 + extra_weight
    if score >= 15:
        label = "🔥 Very Bullish"
    elif score >= 7:
        label = "📊 Bullish"
    elif score <= -10:
        label = "⚠️ Very Bearish"
    elif score <= -4:
        label = "📉 Bearish"
    else:
        label = "😐 Neutral"
    return f"Sentiment: {label} (score {score:.1f})"

def format_price(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)

# ================== FORMATTERS ==================
def format_top_gainers(gainers: list) -> str:
    sorted_g = sorted(
        gainers,
        key=lambda x: x.get("regularMarketChangePercent", 0.0) or 0.0,
        reverse=True
    )[:10]

    lines = ["🏆 *Top 10 Gainers (Screener)*"]
    for g in sorted_g:
        sym = g.get("symbol", "N/A")
        name = g.get("shortName") or g.get("displayName") or g.get("longName") or "N/A"
        price = g.get("regularMarketPrice")
        pct = g.get("regularMarketChangePercent", 0.0) or 0.0
        vol = g.get("regularMarketVolume", 0)
        trend = compute_trend_label(g)
        sentiment = compute_sentiment_score(g)
        lines.append(
            f"\n*{sym}* — {name}\n"
            f"Price: ${price:.2f}  |  Change: {pct:.2f}%  |  Vol: {vol:,}\n"
            f"{trend}\n"
            f"{sentiment}"
        )
    return "\n".join(lines)

def format_unusual_options(options_list: list) -> str:
    filtered = []
    for opt in options_list:
        try:
            vol_oi = float(str(opt.get("volumeOpenInterestRatio", "0")).replace(",", ""))
            vol = float(str(opt.get("volume", "0")).replace(",", ""))
        except Exception:
            continue
        if vol_oi >= UNUSUAL_OPTIONS_VOL_OI_MIN and vol >= UNUSUAL_OPTIONS_VOLUME_MIN:
            filtered.append(opt)

    filtered = filtered[:10]
    if not filtered:
        return "📉 No strong unusual options sweeps found."

    lines = ["⚡ *Unusual Options Activity (Sweeps)*"]
    for opt in filtered:
        base = opt.get("baseSymbol")
        typ = opt.get("symbolType")
        strike = opt.get("strikePrice")
        exp = opt.get("expirationDate")
        dte = opt.get("daysToExpiration")
        bid = opt.get("bidPrice")
        ask = opt.get("askPrice")
        last = opt.get("lastPrice")
        vol = opt.get("volume")
        oi = opt.get("openInterest")
        vol_oi = opt.get("volumeOpenInterestRatio")
        iv = opt.get("volatility")
        delta = opt.get("delta")

        lines.append(
            f"\n*{base}* {typ} {strike} exp {exp} ({dte} DTE)\n"
            f"Bid/Mid/Ask: {bid}/{opt.get('midpoint')}/{ask} | Last: {last}\n"
            f"Vol: {vol} | OI: {oi} | Vol/OI: {vol_oi}\n"
            f"IV: {iv} | Δ: {delta}"
        )
    return "\n".join(lines)

def format_gap_up_alerts(gainers: list) -> str | None:
    lines = ["🚀 *Gap-Up Alerts (Intraday)*"]
    count = 0
    for g in gainers:
        pct = g.get("regularMarketChangePercent", 0.0) or 0.0
        if pct < GAP_UP_THRESHOLD:
            continue
        sym = g.get("symbol", "N/A")
        name = g.get("shortName") or g.get("displayName") or g.get("longName") or "N/A"
        price = g.get("regularMarketPrice")
        prev_close = g.get("regularMarketPreviousClose")
        if price is None or prev_close is None:
            continue
        lines.append(
            f"\n*{sym}* — {name}\n"
            f"Price: ${price:.2f} | Prev Close: ${prev_close:.2f} | Change: {pct:.2f}%"
        )
        count += 1
    if count == 0:
        return None
    return "\n".join(lines)

def format_premarket_and_afterhours(quotes: list) -> str | None:
    pre_lines = ["🌅 *Premarket Gap Scan*"]
    post_lines = ["🌙 *After-hours Movers*"]
    pre_count = 0
    post_count = 0

    for q in quotes:
        sym = q.get("symbol", "N/A")
        name = q.get("shortName") or q.get("displayName") or q.get("longName") or "N/A"
        reg_price = q.get("regularMarketPrice")
        pre_price = q.get("preMarketPrice")
        pre_pct = q.get("preMarketChangePercent")
        post_price = q.get("postMarketPrice")
        post_pct = q.get("postMarketChangePercent")

        if pre_pct is not None and abs(pre_pct) >= PREMARKET_GAP_THRESHOLD:
            pre_lines.append(
                f"\n*{sym}* — {name}\n"
                f"Premkt: ${format_price(pre_price)} | Reg: ${format_price(reg_price)} | Δ: {pre_pct:.2f}%"
            )
            pre_count += 1

        if post_pct is not None and abs(post_pct) >= AFTER_HOURS_MOVE_THRESHOLD:
            post_lines.append(
                f"\n*{sym}* — {name}\n"
                f"After-hours: ${format_price(post_price)} | Reg: ${format_price(reg_price)} | Δ: {post_pct:.2f}%"
            )
            post_count += 1

    parts = []
    if pre_count > 0:
        parts.append("\n".join(pre_lines))
    if post_count > 0:
        parts.append("\n".join(post_lines))

    if not parts:
        return None
    return "\n\n".join(parts)

def format_dark_pool_spy(dark_list: list) -> str | None:
    if not dark_list:
        return None
    lines = ["🕳️ *Dark Pool Prints — SPY*"]
    for dp in dark_list:
        price = dp.get("price") or dp.get("lastPrice") or "N/A"
        size = dp.get("size") or dp.get("volume") or "N/A"
        venue = dp.get("venue") or dp.get("exchange") or "N/A"
        ts = dp.get("time") or dp.get("timestamp") or "N/A"
        lines.append(f"Price: {price} | Size: {size} | Venue: {venue} | Time: {ts}")
    return "\n".join(lines)

# ================== STATE (DEDUP + WATCHLIST) ==================
last_volume_spike_alert: dict[str, bool] = {}
last_bid_exact_alert: dict[str, bool] = {}
last_bid_highvalue_alert: dict[str, bool] = {}
last_unusual_activity_alert: dict[str, bool] = {}
last_halt_alert: dict[str, bool] = {}
watchlist: dict[str, dict] = {}
last_watchlist_volume: dict[str, int] = {}

# ================== CORE LOGIC HELPERS ==================
def is_trading_day(now_est: datetime) -> bool:
    if now_est.weekday() >= 5:
        return False
    return True  # holiday logic could be added here

def is_within_trading_window(now_est: datetime) -> bool:
    hour = now_est.hour
    minute = now_est.minute
    start = 6 * 60  # 6:00
    end = 18 * 60   # 18:00
    current = hour * 60 + minute
    return start <= current <= end

def check_volume_spike(sym: str, stock: dict) -> str | None:
    vol = stock.get("regularMarketVolume")
    avg3m = stock.get("averageDailyVolume3Month")
    if not vol or not avg3m or avg3m <= 0:
        return None
    ratio = vol / avg3m
    if ratio >= VOLUME_SPIKE_RATIO and not last_volume_spike_alert.get(sym, False):
        last_volume_spike_alert[sym] = True
        name = stock.get("shortName") or stock.get("displayName") or stock.get("longName") or "N/A"
        return (
            f"📈 *VOLUME SPIKE* — {sym} ({name})\n"
            f"Volume: {vol:,} vs 3M Avg: {avg3m:,} (x{ratio:.1f})"
        )
    return None

def check_bid_patterns(sym: str, stock: dict) -> list[str]:
    alerts = []
    bid = stock.get("bid")
    bid_size = stock.get("bidSize")
    if bid is None or bid_size is None:
        return alerts

    # Pattern 1: EXACT $199,999 with 100 shares
    if abs(bid - 199999.0) < 0.01 and bid_size == 100 and not last_bid_exact_alert.get(sym, False):
        last_bid_exact_alert[sym] = True
        alerts.append(
            f"🎯 *BID MATCH (EXACT)* — {sym}\n"
            f"Bid: ${bid:,.2f} | Size: {bid_size}"
        )

    # Pattern 2: HIGH VALUE bid ≥ $2,000 with size ≥ 20
    if bid >= 2000.0 and bid_size >= 20 and not last_bid_highvalue_alert.get(sym, False):
        last_bid_highvalue_alert[sym] = True
        alerts.append(
            f"💰 *BID MATCH (HIGH VALUE)* — {sym}\n"
            f"Bid: ${bid:,.2f} | Size: {bid_size}"
        )

    return alerts

def check_unusual_activity(sym: str, stock: dict) -> str | None:
    if last_unusual_activity_alert.get(sym, False):
        return None
    pct = stock.get("regularMarketChangePercent", 0.0) or 0.0
    vol = stock.get("regularMarketVolume", 0) or 0
    avg3m = stock.get("averageDailyVolume3Month", 0) or 1
    ratio = vol / avg3m if avg3m > 0 else 1.0
    if pct >= 10.0 or ratio >= 3.0:
        last_unusual_activity_alert[sym] = True
        name = stock.get("shortName") or stock.get("displayName") or stock.get("longName") or "N/A"
        return (
            f"⚠️ *UNUSUAL ACTIVITY* — {sym} ({name})\n"
            f"Change: {pct:.2f}% | Volume: {vol:,} (x{ratio:.1f} vs 3M Avg)"
        )
    return None

def check_halt(sym: str, stock: dict) -> str | None:
    market_state = stock.get("marketState")
    if market_state == "HALTED" and not last_halt_alert.get(sym, False):
        last_halt_alert[sym] = True
        watchlist.setdefault(sym, {"reason": "halt"})
        name = stock.get("shortName") or stock.get("displayName") or stock.get("longName") or "N/A"
        return f"⛔ *TRADING HALT* — {sym} ({name})\nAdded to watchlist for monitoring."
    return None

def update_watchlist_volume(sym: str, stock: dict) -> str | None:
    vol = stock.get("regularMarketVolume")
    if vol is None:
        return None
    prev = last_watchlist_volume.get(sym)
    last_watchlist_volume[sym] = vol
    if prev is None:
        return None
    delta = vol - prev
    if delta >= LARGE_SALE_VOLUME_DELTA:
        return (
            f"🔻 *LARGE SALE DETECTED* — {sym}\n"
            f"Volume increased by {delta:,} shares since last check."
        )
    return None

# ================== MAIN SCAN (EVERY 10s) ==================
def run_main_scanner() -> None:
    logging.info("🔍 MAIN SCANNER: screener + bid/volume/5%/halts + extras...")
    gainers = get_screener_day_gainers()
    if not gainers:
        logging.warning("⚠️ No screener data in main scanner.")
        return

    # Process each stock in screener
    for stock in gainers:
        sym = stock.get("symbol")
        if not sym:
            continue

        # 1) Volume spike
        vol_msg = check_volume_spike(sym, stock)
        if vol_msg:
            send_telegram_message(vol_msg)

        # 2) 5%+ move → bid pattern check
        pct = stock.get("regularMarketChangePercent", 0.0) or 0.0
        if pct >= PCT_CHANGE_THRESHOLD:
            bid_alerts = check_bid_patterns(sym, stock)
            for msg in bid_alerts:
                send_telegram_message(msg)

        # 3) No pattern → unusual activity check
        unusual_msg = check_unusual_activity(sym, stock)
        if unusual_msg:
            send_telegram_message(unusual_msg)

        # 4) Halt detection
        halt_msg = check_halt(sym, stock)
        if halt_msg:
            send_telegram_message(halt_msg)

        # 5) Add to watchlist if flagged by any condition
        if sym in watchlist or vol_msg or unusual_msg:
            watchlist.setdefault(sym, {"reason": "activity"})

    # Premarket & after-hours (additional feature) using quotes for top symbols
    symbols = [g.get("symbol") for g in gainers[:20] if g.get("symbol")]
    if symbols:
        quotes = get_quotes(symbols)
        pa_msg = format_premarket_and_afterhours(quotes)
        if pa_msg:
            send_telegram_message(pa_msg)

# ================== TOP 10 GAINERS (EVERY 5m) ==================
def run_top10_task() -> None:
    logging.info("🔍 TOP 10 TASK: screener day_gainers...")
    gainers = get_screener_day_gainers()
    if not gainers:
        return
    msg = format_top_gainers(gainers)
    send_telegram_message(msg)

# ================== UNUSUAL OPTIONS (EVERY 2m) ==================
def run_unusual_options_task() -> None:
    logging.info("🔍 UNUSUAL OPTIONS TASK...")
    opts = get_unusual_options_activity()
    if not opts:
        return
    msg = format_unusual_options(opts)
    send_telegram_message(msg)

# ================== WATCHLIST MONITOR (EVERY 30s) ==================
def run_watchlist_task() -> None:
    if not watchlist:
        return
    logging.info("🔍 WATCHLIST TASK: monitoring large sales...")
    symbols = list(watchlist.keys())
    quotes = get_quotes(symbols)
    if not quotes:
        return
    for q in quotes:
        sym = q.get("symbol")
        if not sym:
            continue
        msg = update_watchlist_volume(sym, q)
        if msg:
            send_telegram_message(msg)

# ================== MARKET STATUS (EVERY 30m) ==================
def run_market_status_task(now_est: datetime) -> None:
    logging.info("🔍 MARKET STATUS TASK...")
    status = "OPEN" if is_trading_day(now_est) and is_within_trading_window(now_est) else "CLOSED"
    msg = f"🕒 *Market Status Check*\nStatus: {status}\nTime: {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    send_telegram_message(msg)

# ================== DARK POOL SPY (EVERY 2m, ADDITIONAL) ==================
def run_dark_pool_task() -> None:
    logging.info("🔍 DARK POOL TASK: SPY...")
    dark = get_dark_pool_spy()
    msg = format_dark_pool_spy(dark)
    if msg:
        send_telegram_message(msg)

# ================== GAP-UP TASK (EVERY 2m, MOVED OUT OF 10s LOOP) ==================
def run_gapup_task() -> None:
    logging.info("🔍 GAP-UP TASK: every 2 minutes...")
    gainers = get_screener_day_gainers()
    if not gainers:
        return
    gap_msg = format_gap_up_alerts(gainers)
    if gap_msg:
        send_telegram_message(gap_msg)

# ================== ENTRYPOINT LOOP ==================
if __name__ == "__main__":
    if not MBOUM_API_KEY:
        logging.error("MBOUM_API_KEY is not set. Exiting.")
        raise SystemExit(1)

    logging.info("🤖 Stock scanner bot starting with original timing logic...")

    tz_est = ZoneInfo("America/New_York")

    # Startup message at 6:00 AM EST (if within window, send immediately once)
    now_est = datetime.now(tz_est)
    if is_trading_day(now_est) and is_within_trading_window(now_est) and now_est.hour == 6:
        send_telegram_message("🌅 Good Morning, it's 6am — Bot is running now.")

    last_main_scan = datetime.min.replace(tzinfo=tz_est)
    last_top10 = datetime.min.replace(tzinfo=tz_est)
    last_unusual = datetime.min.replace(tzinfo=tz_est)
    last_watchlist = datetime.min.replace(tzinfo=tz_est)
    last_market_status = datetime.min.replace(tzinfo=tz_est)
    last_dark_pool = datetime.min.replace(tzinfo=tz_est)
    last_gapup = datetime.min.replace(tzinfo=tz_est)

    while True:
        now_est = datetime.now(tz_est)

        # Stop all scanning after 6 PM EST until next trading day
        if not is_trading_day(now_est) or not is_within_trading_window(now_est):
            time.sleep(5)
            continue

        # MAIN SCANNER every 10 seconds
        if (now_est - last_main_scan).total_seconds() >= MAIN_SCAN_INTERVAL_SECONDS:
            last_main_scan = now_est
            run_main_scanner()

        # TOP 10 every 5 minutes
        if (now_est - last_top10).total_seconds() >= TOP10_INTERVAL_SECONDS:
            last_top10 = now_est
            run_top10_task()

        # UNUSUAL OPTIONS every 2 minutes
        if (now_est - last_unusual).total_seconds() >= UNUSUAL_OPTIONS_INTERVAL_SECONDS:
            last_unusual = now_est
            run_unusual_options_task()

        # WATCHLIST every 30 seconds
        if (now_est - last_watchlist).total_seconds() >= WATCHLIST_INTERVAL_SECONDS:
            last_watchlist = now_est
            run_watchlist_task()

        # MARKET STATUS every 30 minutes
        if (now_est - last_market_status).total_seconds() >= MARKET_STATUS_INTERVAL_SECONDS:
            last_market_status = now_est
            run_market_status_task(now_est)

        # DARK POOL SPY every 2 minutes
        if (now_est - last_dark_pool).total_seconds() >= DARK_POOL_INTERVAL_SECONDS:
            last_dark_pool = now_est
            run_dark_pool_task()

        # GAP-UP ALERTS every 2 minutes (moved out of 10s loop)
        if (now_est - last_gapup).total_seconds() >= GAPUP_INTERVAL_SECONDS:
            last_gapup = now_est
            run_gapup_task()

        time.sleep(1)
