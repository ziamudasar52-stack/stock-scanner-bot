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
    raise RuntimeError("Missing environment variables (MBOUM_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
EST = ZoneInfo("America/New_York")

GAIN_THRESHOLD = 5.0                 # % gain threshold for alerts
VOLUME_SPIKE_MULTIPLIER = 2.0        # today > 2x yesterday
HISTORY_RANGE = "5d"                 # safety window for history
MOVER_TYPE = "STOCKS"                # movers universe

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

def now_est() -> datetime:
    return datetime.now(EST)


def is_us_holiday(d: datetime) -> bool:
    # simple fixed list; extend as needed
    holidays = {
        "2026-01-01",
        "2026-07-03",
        "2026-11-26",
        "2026-12-25",
    }
    return d.strftime("%Y-%m-%d") in holidays


def is_market_open(now: datetime | None = None) -> bool:
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
# TELEGRAM
# =========================

def send_telegram(msg: str, mode: str | None = ParseMode.MARKDOWN):
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
    next_scan = "Now" if status == "OPEN" else "6:00 AM EST"

    msg = (
        "✅ *Bot started*\n"
        f"Time: {now.strftime('%I:%M %p EST')}\n"
        f"Market: {status}\n"
        f"Next scan: {next_scan}"
    )
    send_telegram(msg)


def send_error_message(err: str):
    send_telegram(f"❌ *ERROR*\n`{err}`")

# =========================
# MBOUM CORE CALL
# =========================

def call_mboum(endpoint: str, params: dict | None = None):
    url = f"https://mboum.com/api{endpoint}"
    headers = {
        "Authorization": f"Bearer {MBOUM_API_KEY}",
        "Accept": "application/json",
    }

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
# MOVERS (TOP GAINERS / MOST ACTIVE / DECLINERS)
# =========================

def get_movers():
    data = call_mboum("/v1/movers", params={"type": MOVER_TYPE})
    if not data or "body" not in data:
        logger.warning("⚠️ No movers data")
        return None
    return data["body"]


def parse_most_advanced(body: dict) -> list[dict]:
    try:
        rows = body["MostAdvanced"]["table"]["rows"]
    except Exception:
        return []

    gainers = []
    for r in rows:
        symbol = r.get("symbol")
        if not symbol:
            continue

        price_str = r.get("lastSalePrice", "").replace("$", "").replace(",", "")
        change_pct_str = r.get("change", "").replace("%", "").replace("+", "").replace(",", "")

        try:
            price = float(price_str) if price_str else None
        except ValueError:
            price = None

        try:
            change_pct = float(change_pct_str) if change_pct_str else None
        except ValueError:
            change_pct = None

        gainers.append(
            {
                "symbol": symbol,
                "price": price,
                "change_percent": change_pct,
                "raw": r,
            }
        )
    return gainers


def parse_most_active_by_volume(body: dict) -> list[dict]:
    try:
        rows = body["MostActiveByShareVolume"]["table"]["rows"]
    except Exception:
        return []

    actives = []
    for r in rows:
        symbol = r.get("symbol")
        if not symbol:
            continue

        price_str = r.get("lastSalePrice", "").replace("$", "").replace(",", "")
        vol_str = r.get("change", "").replace(",", "")

        try:
            price = float(price_str) if price_str else None
        except ValueError:
            price = None

        try:
            volume = int(vol_str) if vol_str else None
        except ValueError:
            volume = None

        actives.append(
            {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "raw": r,
            }
        )
    return actives

# =========================
# HISTORY (TODAY VS YESTERDAY VOLUME)
# =========================

def get_symbol_history(symbol: str) -> dict | None:
    data = call_mboum("/v1/hi/history", params={"symbol": symbol, "interval": "5m", "range": HISTORY_RANGE})
    if not data or "body" not in data:
        return None
    return data["body"]


def get_today_yesterday_volume(symbol: str) -> tuple[int | None, int | None]:
    body = get_symbol_history(symbol)
    if not body:
        return None, None

    volumes_by_date = defaultdict(int)
    for _, bar in body.items():
        date = bar.get("date")
        vol = bar.get("volume")
        if date is None or vol is None:
            continue
        try:
            v = int(vol)
        except (ValueError, TypeError):
            continue
        volumes_by_date[date] += v

    if not volumes_by_date:
        return None, None

    dates = sorted(volumes_by_date.keys())
    if len(dates) < 2:
        return None, None

    today = dates[-1]
    yesterday = dates[-2]
    return volumes_by_date[today], volumes_by_date[yesterday]

# =========================
# UNUSUAL OPTIONS
# =========================

def get_unusual_options() -> list[dict]:
    data = call_mboum("/v1/options/unusual-activity")
    if not data or "body" not in data:
        logger.warning("⚠️ No unusual options data")
        return []
    body = data["body"]
    if not isinstance(body, list):
        return []
    return body  # alert on ALL contracts

# =========================
# ALERT FORMATTING
# =========================

def format_gainer_line(m: dict) -> str:
    parts = [f"*{m['symbol']}*"]
    if m.get("change_percent") is not None:
        parts.append(f"{m['change_percent']:+.2f}%")
    if m.get("price") is not None:
        parts.append(f"${m['price']:.2f}")
    return " | ".join(parts)


def format_active_line(m: dict, ratio: float | None = None) -> str:
    parts = [f"*{m['symbol']}*"]
    if m.get("price") is not None:
        parts.append(f"${m['price']:.2f}")
    if m.get("volume") is not None:
        parts.append(f"Vol: {m['volume']:,}")
    if ratio is not None:
        parts.append(f"x{ratio:.1f} vs yesterday")
    return " | ".join(parts)


def format_unusual_line(c: dict) -> str:
    symbol = c.get("symbol")
    base = c.get("baseSymbol")
    stype = c.get("symbolType")
    strike = c.get("strikePrice")
    exp = c.get("expirationDate")
    vol = c.get("volume")
    oi = c.get("openInterest")
    vor = c.get("volumeOpenInterestRatio")
    last = c.get("lastPrice")
    delta = c.get("delta")
    return (
        f"*{symbol}* ({base} {stype}) | Strike: {strike} | Exp: {exp} | "
        f"Last: {last} | Vol: {vol} | OI: {oi} | V/OI: {vor} | Δ: {delta}"
    )

# =========================
# ALERT LOGIC
# =========================

def check_gain_threshold(gainers: list[dict]) -> list[dict]:
    alerts = []
    for g in gainers:
        cp = g.get("change_percent")
        if cp is None:
            continue
        if cp >= GAIN_THRESHOLD:
            key = g["symbol"]
            rounded = round(cp, 2)
            if last_seen_gainers.get(key) != rounded:
                last_seen_gainers[key] = rounded
                alerts.append(g)
    return alerts


def check_volume_spikes(actives: list[dict]) -> list[dict]:
    alerts = []
    for a in actives:
        symbol = a["symbol"]
        today_vol, yest_vol = get_today_yesterday_volume(symbol)
        if today_vol is None or yest_vol is None or yest_vol == 0:
            continue
        ratio = today_vol / yest_vol
        if ratio >= VOLUME_SPIKE_MULTIPLIER:
            last_ratio = last_seen_volume_spikes.get(symbol)
            if last_ratio is None or ratio > last_ratio:
                last_seen_volume_spikes[symbol] = ratio
                a["volume_ratio"] = ratio
                alerts.append(a)
    return alerts


def check_unusual_options(contracts: list[dict]) -> list[dict]:
    alerts = []
    for c in contracts:
        key = c.get("symbol")
        if not key:
            continue
        if key in last_seen_unusual:
            continue
        last_seen_unusual.add(key)
        alerts.append(c)
    return alerts

# =========================
# JOBS
# =========================

def scan_main_job():
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping main scan")
        return

    logger.info("🔍 Running main scan (movers + unusual options)...")

    movers_body = get_movers()
    if not movers_body:
        return

    gainers = parse_most_advanced(movers_body)
    actives = parse_most_active_by_volume(movers_body)
    unusual = get_unusual_options()

    # Gain threshold alerts
    gain_alerts = check_gain_threshold(gainers)
    if gain_alerts:
        msg = "📈 *Gain Threshold Alerts*\n" + "\n".join(
            f"- {format_gainer_line(g)}" for g in gain_alerts
        )
        send_telegram(msg)

    # Volume spike alerts
    vol_alerts = check_volume_spikes(actives)
    if vol_alerts:
        msg = "📊 *Volume Spike Alerts*\n" + "\n".join(
            f"- {format_active_line(a, a.get('volume_ratio'))}" for a in vol_alerts
        )
        send_telegram(msg)

    # Unusual options alerts (ALL new contracts)
    un_alerts = check_unusual_options(unusual)
    if un_alerts:
        msg_lines = ["🧨 *Unusual Options Activity*"]
        for c in un_alerts[:50]:  # safety cap
            msg_lines.append(f"- {format_unusual_line(c)}")
        send_telegram("\n".join(msg_lines))


def top_10_gainers_job():
    now = now_est()
    if not is_market_open(now):
        logger.info("⏸ Market closed, skipping top 10 gainers")
        return

    logger.info("🔍 Getting top 10 gainers...")

    movers_body = get_movers()
    if not movers_body:
        return

    gainers = parse_most_advanced(movers_body)
    if not gainers:
        return

    gainers = sorted(
        gainers,
        key=lambda x: x["change_percent"] if x["change_percent"] is not None else 0,
        reverse=True,
    )
    top10 = gainers[:10]

    msg = "🏆 *Top 10 Gainers (STOCKS)*\n" + "\n".join(
        f"- {format_gainer_line(g)}" for g in top10
    )
    send_telegram(msg)

# =========================
# SCHEDULER / MAIN LOOP
# =========================

def setup_scheduler():
    schedule.every(1).minutes.do(scan_main_job)
    schedule.every(5).minutes.do(top_10_gainers_job)


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
