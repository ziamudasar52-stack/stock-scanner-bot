import os
import time
import logging
from datetime import datetime, time as dt_time
from collections import defaultdict
from zoneinfo import ZoneInfo

import requests
import schedule
from telegram import Bot, ParseMode
from telegram.error import TelegramError

# =========================
# ENV / CONSTANTS
# =========================

MBOUM_API_KEY = os.getenv("MBOUM_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([MBOUM_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("Missing environment variables")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
EST = ZoneInfo("America/New_York")

GAIN_THRESHOLD = 5.0
VOLUME_SPIKE_MULTIPLIER = 2.0
HISTORY_RANGE = "5d"

last_seen_gainers = {}
last_seen_volume_spikes = {}
last_seen_unusual = set()

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# =========================
# TIME / MARKET
# =========================

def now_est():
    return datetime.now(EST)

def is_us_holiday(d: datetime) -> bool:
    holidays = {
        "2026-01-01",
        "2026-07-03",
        "2026-11-26",
        "2026-12-25",
    }
    return d.strftime("%Y-%m-%d") in holidays

def is_market_open(now=None):
    if now is None:
        now = now_est()
    if now.weekday() >= 5:
        return False
    if is_us_holiday(now):
        return False
    return dt_time(6, 0) <= now.time() <= dt_time(18, 0)

# =========================
# TELEGRAM
# =========================

def send_telegram(msg: str):
    try:
        bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")

def send_startup_message():
    now = now_est()
    status = "OPEN" if is_market_open(now) else "CLOSED"
    msg = (
        "✅ *Bot started*\n"
        f"Time: {now.strftime('%I:%M %p EST')}\n"
        f"Market: {status}"
    )
    send_telegram(msg)

# =========================
# MBOUM API
# =========================

def call_mboum(endpoint: str, params=None):
    url = f"https://mboum.com/api{endpoint}"
    headers = {"Authorization": f"Bearer {MBOUM_API_KEY}"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        logger.info(f"📡 API Response: {endpoint} - Status: {resp.status_code}")

        if not resp.ok:
            logger.error(f"❌ API Error {resp.status_code}: {resp.text}")
            return None

        return resp.json()
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None

# =========================
# MOVERS
# =========================

def get_movers():
    return call_mboum("/v1/markets/movers", params={"type": "STOCKS"})

def parse_most_advanced(body):
    try:
        rows = body["MostAdvanced"]["table"]["rows"]
    except Exception:
        return []

    gainers = []
    for r in rows:
        symbol = r.get("symbol")
        if not symbol:
            continue

        price = float(r["lastSalePrice"].replace("$", "").replace(",", "")) if r.get("lastSalePrice") else None
        pct = r.get("change", "").replace("%", "").replace("+", "").replace(",", "")
        pct = float(pct) if pct else None

        gainers.append({"symbol": symbol, "price": price, "change_percent": pct})
    return gainers

def parse_most_active(body):
    try:
        rows = body["MostActiveByShareVolume"]["table"]["rows"]
    except Exception:
        return []

    actives = []
    for r in rows:
        symbol = r.get("symbol")
        if not symbol:
            continue

        price = float(r["lastSalePrice"].replace("$", "").replace(",", "")) if r.get("lastSalePrice") else None
        vol = int(r["change"].replace(",", "")) if r.get("change") else None

        actives.append({"symbol": symbol, "price": price, "volume": vol})
    return actives

# =========================
# SCREENER (DAY GAINERS)
# =========================

def get_screener_gainers():
    return call_mboum("/v1/screener", params={"description": "day_gainers"})

def parse_screener_gainers(data):
    if not data or "body" not in data:
        return []

    gainers = []
    for q in data["body"]:
        symbol = q.get("symbol")
        pct = q.get("regularMarketChangePercent")
        price = q.get("regularMarketPrice")
        if symbol and pct is not None:
            gainers.append({"symbol": symbol, "price": price, "change_percent": pct})
    return gainers

# =========================
# HISTORY (VOLUME SPIKES)
# =========================

def get_symbol_history(symbol):
    return call_mboum("/v1/hi/history", params={"symbol": symbol, "interval": "5m", "range": HISTORY_RANGE})

def get_today_yesterday_volume(symbol):
    data = get_symbol_history(symbol)
    if not data or "body" not in data:
        return None, None

    volumes = defaultdict(int)
    for bar in data["body"].values():
        date = bar.get("date")
        vol = bar.get("volume")
        if date and vol:
            volumes[date] += int(vol)

    if len(volumes) < 2:
        return None, None

    dates = sorted(volumes.keys())
    return volumes[dates[-1]], volumes[dates[-2]]

# =========================
# UNUSUAL OPTIONS
# =========================

def get_unusual_options():
    data = call_mboum("/v1/options/unusual-activity")
    return data["body"] if data and "body" in data else []

# =========================
# ALERT LOGIC
# =========================

def check_gain_threshold(gainers):
    alerts = []
    for g in gainers:
        pct = g["change_percent"]
        if pct >= GAIN_THRESHOLD:
            if last_seen_gainers.get(g["symbol"]) != pct:
                last_seen_gainers[g["symbol"]] = pct
                alerts.append(g)
    return alerts

def check_volume_spikes(actives):
    alerts = []
    for a in actives:
        today, yesterday = get_today_yesterday_volume(a["symbol"])
        if today and yesterday and yesterday > 0:
            ratio = today / yesterday
            if ratio >= VOLUME_SPIKE_MULTIPLIER:
                if last_seen_volume_spikes.get(a["symbol"]) != ratio:
                    last_seen_volume_spikes[a["symbol"]] = ratio
                    a["ratio"] = ratio
                    alerts.append(a)
    return alerts

def check_unusual(contracts):
    alerts = []
    for c in contracts:
        key = c.get("symbol")
        if key and key not in last_seen_unusual:
            last_seen_unusual.add(key)
            alerts.append(c)
    return alerts

# =========================
# JOBS
# =========================

def scan_main_job():
    now = now_est()
    if not is_market_open(now):
        return

    logger.info("🔍 Running main scan (movers + screener + unusual options)...")

    movers = get_movers()
    screener = get_screener_gainers()
    unusual = get_unusual_options()

    if movers and "body" in movers:
        gainers_movers = parse_most_advanced(movers["body"])
        actives = parse_most_active(movers["body"])
    else:
        gainers_movers = []
        actives = []

    gainers_screener = parse_screener_gainers(screener)

    gain_alerts = check_gain_threshold(gainers_screener)
    if gain_alerts:
        msg = "📈 *Gain Threshold Alerts*\n" + "\n".join(
            f"- *{g['symbol']}* | {g['change_percent']:.2f}% | ${g['price']}"
            for g in gain_alerts
        )
        send_telegram(msg)

    vol_alerts = check_volume_spikes(actives)
    if vol_alerts:
        msg = "📊 *Volume Spike Alerts*\n" + "\n".join(
            f"- *{a['symbol']}* | Vol Spike x{a['ratio']:.1f}"
            for a in vol_alerts
        )
        send_telegram(msg)

    un_alerts = check_unusual(unusual)
    if un_alerts:
        msg = "🧨 *Unusual Options Activity*\n" + "\n".join(
            f"- {c['symbol']} | Vol/OI: {c.get('volumeOpenInterestRatio')}"
            for c in un_alerts[:50]
        )
        send_telegram(msg)

def top_10_gainers_job():
    now = now_est()
    if not is_market_open(now):
        return

    logger.info("🔍 Getting top 10 gainers...")

    screener = get_screener_gainers()
    gainers = parse_screener_gainers(screener)
    gainers = sorted(gainers, key=lambda x: x["change_percent"], reverse=True)[:10]

    msg = "🏆 *Top 10 Gainers (Screener)*\n" + "\n".join(
        f"- *{g['symbol']}* | {g['change_percent']:.2f}% | ${g['price']}"
        for g in gainers
    )
    send_telegram(msg)

# =========================
# MAIN LOOP
# =========================

def setup_scheduler():
    schedule.every(1).minutes.do(scan_main_job)
    schedule.every(5).minutes.do(top_10_gainers_job)

def main_loop():
    send_startup_message()
    setup_scheduler()
    logger.info("🚀 Bot running...")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main_loop()
