import os
import time
import json
import logging
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

import requests
import schedule
from telegram import Bot, ParseMode
from telegram.error import TelegramError

# =========================
# ENVIRONMENT VARIABLES
# =========================

MBOUM_API_KEY = os.getenv("MBOUM_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([MBOUM_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("Missing environment variables")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
EST = ZoneInfo("America/New_York")

GAIN_THRESHOLD = 5.0
VOLUME_SPIKE_MULTIPLIER = 3.0

last_seen_gainers = {}
last_seen_volume_spikes = {}
last_seen_bid_matches = {}

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =========================
# TIME HELPERS
# =========================

def now_est():
    return datetime.now(EST)


def is_us_holiday(d):
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

    start = dt_time(6, 0)
    end = dt_time(18, 0)
    return start <= now.time() <= end


# =========================
# TELEGRAM HELPERS
# =========================

def send_telegram(msg, mode=ParseMode.MARKDOWN):
    try:
        bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode=mode,
            disable_web_page_preview=True,
        )
        logger.info("📤 Telegram sent")
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")


def send_startup_message():
    now = now_est()
    status = "OPEN" if is_market_open(now) else "CLOSED"
    next_scan = "Now" if status == "OPEN" else "6 AM EST"

    msg = (
        "✅ *Bot started*\n"
        f"Time: {now.strftime('%I:%M %p EST')}\n"
        f"Market: {status}\n"
        f"Next scan: {next_scan}"
    )
    send_telegram(msg)


def send_error_message(err):
    send_telegram(f"❌ *ERROR*\n`{err}`")


# =========================
# MBOUM API CALL
# =========================

def call_mboum(endpoint, params=None):
    url = f"https://mboum.com/api{endpoint}"

    headers = {
        "Authorization": f"Bearer {MBOUM_API_KEY}",
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        logger.info(f"📡 API Response: {endpoint} - Status: {resp.status_code}")

        if resp.status_code == 401:
            logger.error(f"❌ API Error 401: {resp.text}")
            return None

        if resp.status_code == 422:
            logger.error(f"❌ API Error 422: {resp.text}")
            return None

        if not resp.ok:
            logger.error(f"❌ API Error {resp.status_code}: {resp.text}")
            return None

        return resp.json()

    except Exception as e:
        logger.error(f"Request error: {e}")
        return None


# =========================
# MOVERS PARSING
# =========================

def extract_movers(data):
    if data is None:
        return []

    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        raw = (
            data.get("results")
            or data.get("movers")
            or data.get("quotes")
            or data.get("data")
            or []
        )
    else:
        return []

    movers = []
    for item in raw:
        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol") or item.get("ticker")
        if not symbol:
            continue

        cp = (
            item.get("change_percent")
            or item.get("changePercent")
            or item.get("percent_change")
        )

        price = (
            item.get("price")
            or item.get("last")
            or item.get("regularMarketPrice")
        )

        vol = (
            item.get("volume")
            or item.get("regularMarketVolume")
        )

        avg = (
            item.get("avg_volume")
            or item.get("averageVolume")
            or item.get("averageDailyVolume3Month")
        )

        movers.append({
            "symbol": symbol,
            "change_percent": float(cp) if cp else None,
            "price": float(price) if price else None,
            "volume": int(vol) if vol else None,
            "avg_volume": int(avg) if avg else None,
        })

    return movers


def get_top_movers(limit=50):
    logger.info("🔍 Getting top movers...")

    data = call_mboum(
        "/v1/markets/movers",
        params={
            "type": "gainers",   # REQUIRED FIX
            "limit": limit
        }
    )

    movers = extract_movers(data)

    if not movers:
        logger.warning("⚠️ No movers returned")
        return []

    movers = sorted(
        movers,
        key=lambda x: x["change_percent"] or 0,
        reverse=True,
    )

    return movers[:limit]


# =========================
# ALERT LOGIC
# =========================

def format_stock(m):
    parts = [f"*{m['symbol']}*"]

    if m["change_percent"] is not None:
        parts.append(f"{m['change_percent']:+.2f}%")

    if m["price"] is not None:
        parts.append(f"${m['price']:.2f}")

    if m["volume"] is not None:
        parts.append(f"Vol: {m['volume']:,}")

    return " | ".join(parts)


def check_gain_threshold(movers):
    alerts = []
    for m in movers:
        cp = m["change_percent"]
        if cp is None:
            continue

        if cp >= GAIN_THRESHOLD:
            key = m["symbol"]
            rounded = round(cp, 2)

            if last_seen_gainers.get(key) != rounded:
                last_seen_gainers[key] = rounded
                alerts.append(m)

    return alerts


def check_volume_spikes(movers):
    alerts = []
    for m in movers:
        vol = m["volume"]
        avg = m["avg_volume"]

        if not vol or not avg or avg == 0:
            continue

        ratio = vol / avg
        if ratio >= VOLUME_SPIKE_MULTIPLIER:
            key = m["symbol"]
            if last_seen_volume_spikes.get(key) != ratio:
                last_seen_volume_spikes[key] = ratio
                m["ratio"] = ratio
                alerts.append(m)

    return alerts


def check_bid_matches(movers):
    alerts = []
    for m in movers:
        cp = m["change_percent"]
        vol = m["volume"]

        if cp and vol and cp > 8 and vol > 500_000:
            key = m["symbol"]
            if key not in last_seen_bid_matches:
                last_seen_bid_matches[key] = True
                alerts.append(m)

    return alerts


# =========================
# SCAN JOBS
# =========================

def scan_top_movers_job():
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping scan")
        return

    movers = get_top_movers()
    if not movers:
        return

    gainers = check_gain_threshold(movers)
    if gainers:
        msg = "📈 *Gain Threshold Alerts*\n" + "\n".join(
            f"- {format_stock(m)}" for m in gainers
        )
        send_telegram(msg)

    spikes = check_volume_spikes(movers)
    if spikes:
        msg = "📊 *Volume Spike Alerts*\n" + "\n".join(
            f"- {format_stock(m)} (x{m['ratio']:.1f})" for m in spikes
        )
        send_telegram(msg)

    bids = check_bid_matches(movers)
    if bids:
        msg = "🎯 *Bid Match Alerts*\n" + "\n".join(
            f"- {format_stock(m)}" for m in bids
        )
        send_telegram(msg)


def top_10_gainers_job():
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping top 10 gainers")
        return

    movers = get_top_movers()
    if not movers:
        return

    top10 = movers[:10]
    msg = "🏆 *Top 10 Gainers*\n" + "\n".join(
        f"- {format_stock(m)}" for m in top10
    )
    send_telegram(msg)


# =========================
# SCHEDULER
# =========================

def setup_scheduler():
    schedule.every(1).minutes.do(scan_top_movers_job)
    schedule.every(5).minutes.do(top_10_gainers_job)


# =========================
# MAIN LOOP
# =========================

def main_loop():
    send_startup_message()
    setup_scheduler()

    logger.info("🚀 Bot running...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            send_error_message(str(e))
            time.sleep(5)


if __name__ == "__main__":
    main_loop()
