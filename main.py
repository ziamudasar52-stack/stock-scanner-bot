import os
import time
import json
import logging
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

import requests
import schedule
from telegram import Bot, ParseMode
from telegram.error import TelegramError

# =========================
# CONFIG & GLOBALS
# =========================

MBOUM_API_KEY = os.getenv("MBOUM_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([MBOUM_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise RuntimeError("Missing one or more required environment variables: MBOUM_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

EST = ZoneInfo("America/New_York")

# Gain threshold for alerts (e.g., 5%+)
GAIN_THRESHOLD = 5.0
# Volume spike multiplier (e.g., 3x average)
VOLUME_SPIKE_MULTIPLIER = 3.0

# Track last seen data to avoid duplicate alerts
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
# TIME & MARKET HELPERS
# =========================

def now_est() -> datetime:
    return datetime.now(EST)


def is_us_holiday(d: datetime) -> bool:
    """
    Very simple placeholder holiday list.
    You can expand this with real US market holidays.
    """
    # Example static dates (YYYY-MM-DD) – adjust per year as needed
    holidays = {
        "2026-01-01",  # New Year's Day
        "2026-07-03",  # Independence Day observed (example)
        "2026-11-26",  # Thanksgiving (example)
        "2026-12-25",  # Christmas
    }
    return d.strftime("%Y-%m-%d") in holidays


def is_market_open(now: datetime | None = None) -> bool:
    if now is None:
        now = now_est()

    # Weekend check
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False

    # Holiday check
    if is_us_holiday(now):
        return False

    # Time window: 6 AM – 6 PM EST
    start = dt_time(6, 0)
    end = dt_time(18, 0)
    current_t = now.time()

    return start <= current_t <= end


# =========================
# TELEGRAM HELPERS
# =========================

def send_telegram(message: str, parse_mode: str | None = None) -> None:
    try:
        bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=parse_mode or ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        logger.info("📤 Telegram sent")
    except TelegramError as e:
        logger.error(f"❌ Telegram error: {e}")


def send_startup_message() -> None:
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


def send_error_message(err: str) -> None:
    msg = f"❌ *ERROR*\n`{err}`"
    send_telegram(msg)


# =========================
# MBOUM API HELPERS
# =========================

def call_mboum(endpoint: str, params: dict | None = None) -> dict | list | None:
    """
    Generic Mboum caller. Adjust base URL/headers if your account uses a different pattern.
    """
    url = f"https://mboum.com/api{endpoint}"
headers = {
    "Authorization": f"Bearer {MBOUM_API_KEY}",
    "Accept": "application/json",
}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        elapsed = resp.elapsed.total_seconds()
        logger.info(f"📡 API Response: {endpoint} - Status: {resp.status_code} - Time: {elapsed:.2f}s")

        if resp.status_code == 403:
            logger.error(f"❌ API Error 403: {resp.text}")
            return None

        if not resp.ok:
            logger.error(f"❌ API Error {resp.status_code}: {resp.text}")
            return None

        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.error("❌ Failed to decode JSON from API response")
            return None

        return data

    except requests.RequestException as e:
        logger.error(f"❌ Request error: {e}")
        return None


# =========================
# MOVERS PARSING
# =========================

def extract_movers(data: dict | list | None) -> list[dict]:
    """
    Normalize Mboum movers response into a list of dicts with at least:
    symbol, change_percent, price, volume, avg_volume (if available).
    This is defensive and handles multiple possible shapes.
    """
    if data is None:
        return []

    # If it's already a list, assume it's the list of movers
    if isinstance(data, list):
        raw_items = data
    elif isinstance(data, dict):
        # Try common keys
        raw_items = (
            data.get("results")
            or data.get("movers")
            or data.get("quotes")
            or data.get("data")
            or []
        )
    else:
        return []

    movers = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue

        symbol = (
            item.get("symbol")
            or item.get("ticker")
            or item.get("code")
        )

        if not symbol:
            continue

        change_percent = (
            item.get("change_percent")
            or item.get("changePercent")
            or item.get("percent_change")
            or item.get("change_pct")
        )

        price = (
            item.get("price")
            or item.get("last")
            or item.get("regularMarketPrice")
        )

        volume = (
            item.get("volume")
            or item.get("regularMarketVolume")
        )

        avg_volume = (
            item.get("avg_volume")
            or item.get("averageVolume")
            or item.get("averageDailyVolume3Month")
        )

        movers.append(
            {
                "symbol": symbol,
                "change_percent": float(change_percent) if change_percent is not None else None,
                "price": float(price) if price is not None else None,
                "volume": int(volume) if volume is not None else None,
                "avg_volume": int(avg_volume) if avg_volume is not None else None,
                "raw": item,
            }
        )

    return movers


def get_top_movers(limit: int = 50) -> list[dict]:
    logger.info("🔍 Getting top movers...")
    data = call_mboum("/v1/markets/movers", params={"limit": limit})
    movers = extract_movers(data)

    if not movers:
        logger.warning("⚠️ No movers returned from API")
        return []

    # Sort by change_percent descending if available
    movers = sorted(
        movers,
        key=lambda x: x["change_percent"] if x["change_percent"] is not None else 0.0,
        reverse=True,
    )

    return movers[:limit]


# =========================
# ALERT LOGIC
# =========================

def format_stock_line(m: dict) -> str:
    symbol = m["symbol"]
    cp = m["change_percent"]
    price = m["price"]
    vol = m["volume"]

    parts = [f"*{symbol}*"]

    if cp is not None:
        parts.append(f"{cp:+.2f}%")

    if price is not None:
        parts.append(f"${price:.2f}")

    if vol is not None:
        parts.append(f"Vol: {vol:,}")

    return " | ".join(parts)


def check_gain_threshold(movers: list[dict]) -> list[dict]:
    triggered = []
    for m in movers:
        cp = m["change_percent"]
        if cp is None:
            continue
        if cp >= GAIN_THRESHOLD:
            key = m["symbol"]
            # Avoid duplicate alerts for same symbol & rounded percent
            rounded = round(cp, 2)
            last_cp = last_seen_gainers.get(key)
            if last_cp is None or rounded > last_cp:
                last_seen_gainers[key] = rounded
                triggered.append(m)
    return triggered


def check_volume_spikes(movers: list[dict]) -> list[dict]:
    triggered = []
    for m in movers:
        vol = m["volume"]
        avg = m["avg_volume"]
        if vol is None or avg is None or avg == 0:
            continue
        ratio = vol / avg
        if ratio >= VOLUME_SPIKE_MULTIPLIER:
            key = m["symbol"]
            last_ratio = last_seen_volume_spikes.get(key)
            if last_ratio is None or ratio > last_ratio:
                last_seen_volume_spikes[key] = ratio
                m["volume_ratio"] = ratio
                triggered.append(m)
    return triggered


def check_bid_matches(movers: list[dict]) -> list[dict]:
    """
    Placeholder: depends on your actual bid/ask data from Mboum.
    For now, we just keep the structure so you can plug in your own condition.
    """
    triggered = []
    for m in movers:
        # Example placeholder condition: big gainer with volume
        cp = m["change_percent"]
        vol = m["volume"]
        if cp is not None and vol is not None and cp > 8 and vol > 500_000:
            key = m["symbol"]
            if key not in last_seen_bid_matches:
                last_seen_bid_matches[key] = True
                triggered.append(m)
    return triggered


# =========================
# SCAN & ALERT FUNCTIONS
# =========================

def send_movers_alerts(movers: list[dict]) -> None:
    if not movers:
        return

    # Gain threshold alerts
    gainers = check_gain_threshold(movers)
    if gainers:
        lines = ["📈 *Gain Threshold Alerts*"]
        for m in gainers:
            lines.append(f"- {format_stock_line(m)}")
        send_telegram("\n".join(lines))

    # Volume spike alerts
    spikes = check_volume_spikes(movers)
    if spikes:
        lines = ["📊 *Volume Spike Alerts*"]
        for m in spikes:
            ratio = m.get("volume_ratio")
            extra = f" (x{ratio:.1f})" if ratio else ""
            lines.append(f"- {format_stock_line(m)}{extra}")
        send_telegram("\n".join(lines))

    # Bid match alerts (placeholder logic)
    bids = check_bid_matches(movers)
    if bids:
        lines = ["🎯 *Bid Match Alerts*"]
        for m in bids:
            lines.append(f"- {format_stock_line(m)}")
        send_telegram("\n".join(lines))


def scan_top_movers_job() -> None:
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping movers scan")
        return

    logger.info("🔍 Scanning top 50 movers...")
    movers = get_top_movers(limit=50)
    if not movers:
        return

    send_movers_alerts(movers)


def top_10_gainers_job() -> None:
    """
    Sends top 10 gainers every 5 minutes during market hours.
    """
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping top 10 gainers")
        return

    logger.info("🏆 Scanning top 10 gainers...")
    movers = get_top_movers(limit=50)
    if not movers:
        return

    top10 = movers[:10]
    lines = ["🏆 *Top 10 Gainers*"]
    for m in top10:
        lines.append(f"- {format_stock_line(m)}")

    send_telegram("\n".join(lines))


# =========================
# COMMAND HANDLERS (STATUS / FORCE SCAN)
# =========================

def handle_status_command() -> None:
    now = now_est()
    status = "OPEN" if is_market_open(now) else "CLOSED"
    msg = (
        "📊 *Bot Status*\n"
        f"Time: {now.strftime('%I:%M %p EST')}\n"
        f"Market: {status}\n"
        f"Next scheduled scan: within 1 minute\n"
        f"Top 10 gainers: every 5 minutes (market hours)"
    )
    send_telegram(msg)


def handle_force_scan_command() -> None:
    send_telegram("⚡ *Force scan triggered*")
    scan_top_movers_job()


# =========================
# SIMPLE POLLING FOR COMMANDS (OPTIONAL)
# =========================

def poll_commands() -> None:
    """
    Very simple polling for /status and /force_scan.
    This is optional and basic; for heavy use, switch to python-telegram-bot's updater.
    """
    # To avoid storing update offsets persistently, we just ignore this for now.
    # You can expand this with real polling if you want.
    pass


# =========================
# SCHEDULER & MAIN LOOP
# =========================

def setup_scheduler() -> None:
    logger.info("🗓 Setting up scheduler...")

    # Main movers scan – every minute
    schedule.every(1).minutes.do(scan_top_movers_job)

    # Top 10 gainers every 5 minutes
    schedule.every(5).minutes.do(top_10_gainers_job)

    # You can add daily summary, etc., here if you want


def main_loop() -> None:
    send_startup_message()
    setup_scheduler()

    logger.info("🚀 Entering main loop...")
    while True:
        try:
            schedule.run_pending()
            # poll_commands()  # if you later implement real command polling
            time.sleep(1)
        except Exception as e:
            logger.error(f"❌ Unexpected error in main loop: {e}")
            send_error_message(str(e))
            time.sleep(10)  # brief backoff to avoid spam


if __name__ == "__main__":
    logger.info("BOT STARTING - DEBUG INFO")
    main_loop()

