# app/pipeline.py

import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import httpx
from sqlalchemy import text

from .db import get_engine

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# -------------------- КЛЮЧЕВЫЕ СЛОВА ДЛЯ МЕМКОИНОВ -------------------- #

MEME_KEYWORDS = [
    "doge", "shib", "inu", "pepe", "floki", "elon",
    "baby", "kitty", "cat", "dog", "meme", "frog",
    "bonk", "snek", "wojak",
]


def _is_memecoin(token: Dict[str, Any]) -> bool:
    name = (token.get("name") or "").lower()
    symbol = (token.get("symbol") or "").lower()
    text = f"{name} {symbol}"
    return any(k in text for k in MEME_KEYWORDS)


def _is_serious_by_metrics(token: Dict[str, Any]) -> bool:
    """Грубые пороги, чтобы отсечь совсем мусорные монеты."""
    def _to_float(x):
        try:
            return float(x or 0)
        except (TypeError, ValueError):
            return 0.0

    mcap = _to_float(token.get("market_cap"))
    vol = _to_float(token.get("total_volume"))
    price = _to_float(token.get("current_price"))

    if price <= 0:
        return False

    # Достаточно «живые» монеты по ликвидности
    if mcap >= 5_000_000 and vol >= 250_000:
        return True
    if vol >= 1_000_000:
        return True
    return False


def classify_token(token: Dict[str, Any]) -> str:
    """
    Возвращает:
      - "serious"            — серьёзный не-мем
      - "serious_memecoin"   — серьёзный мемкоин
      - "trash_memecoin"     — мемкоин-мусор
      - "trash"              — прочий хлам
    """
    is_meme = _is_memecoin(token)
    is_serious = _is_serious_by_metrics(token)

    if is_serious and is_meme:
        return "serious_memecoin"
    if is_serious:
        return "serious"
    if is_meme:
        return "trash_memecoin"
    return "trash"


# -------------------- ЗАГРУЗКА С COINGECKO -------------------- #


async def fetch_latest_coins(cg_api_key: str | None, max_raw: int) -> List[Dict[str, Any]]:
    """
    Забираем токены с CoinGecko постранично, максимум max_raw штук.
    Ошибки 400/500 не роняют приложение — просто логируем и выходим.
    """
    per_page = 250
    page = 1
    collected: List[Dict[str, Any]] = []

    headers: Dict[str, str] = {}
    # Если когда-нибудь понадобится Pro-ключ:
    # if cg_api_key:
    #     headers["x-cg-pro-api-key"] = cg_api_key

    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        while len(collected) < max_raw:
            params = {
                "vs_currency": "usd",
                "order": "volume_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": "false",
            """

            try:
                resp = await client.get(
                    f"{COINGECKO_BASE_URL}/coins/markets",
                    params=params,
                )
            except Exception as e:
                print(f"[CoinGecko] request error on page={page}: {e}")
                break

            if resp.status_code != 200:
                print(
                    f"[CoinGecko] status {resp.status_code} on page={page}: "
                    f"{resp.text[:300]}"
                )
                break

            try:
                data = resp.json()
            except Exception as e:
                print(f"[CoinGecko] JSON parse error on page={page}: {e}")
                break

            if not data:
                break

            for token in data:
                collected.append(token)
                if len(collected) >= max_raw:
                    break

            if len(data) < per_page or len(collected) >= max_raw:
                break

            page += 1

    print(f"[CoinGecko] fetched {len(collected)} tokens total")
    return collected


# -------------------- ОСНОВНОЙ КОНВЕЙЕР -------------------- #


async def collect_and_filter() -> Dict[str, Any]:
    """
    1) Сбор сырых токенов с CoinGecko.
    2) Классификация: мем / серьёзные / мусор.
    3) В БД сохраняем сырые + только серьёзные.
    4) Возвращаем краткую статистику + список серьёзных для Telegram.
    """
    engine = get_engine()

    max_raw = int(os.getenv("MAX_RAW", "1000"))
    analysis_mode = os.getenv("ANALYSIS_MODE", "previous_day").lower()
    raw_retention_hours = int(os.getenv("RAW_RETENTION_HOURS", "24"))

    if analysis_mode != "previous_day":
        analysis_mode = "previous_day"

    now_utc = datetime.now(timezone.utc)

    start_utc = (now_utc - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_utc = start_utc + timedelta(days=1)

    cg_api_key = os.getenv("COINGECKO_API_KEY", "")
    coins: List[Dict[str, Any]] = await fetch_latest_coins(cg_api_key, max_raw)

    # ---- классификация ----
    memecoins: List[Dict[str, Any]] = []
    serious_tokens_list: List[Dict[str, Any]] = []
    trash_tokens: List[Dict[str, Any]] = []

    for tk in coins:
        cls = classify_token(tk)

        if cls in ("serious", "serious_memecoin"):
            serious_tokens_list.append(tk)

        if "memecoin" in cls:
            memecoins.append(tk)

        if "trash" in cls:
            trash_tokens.append(tk)

    passed_tokens = serious_tokens_list

    # ---- запись в БД ----
    with engine.begin() as conn:
        # сырые
        for tk in coins:
            conn.execute(
                text(
                    """
                    INSERT INTO raw_tokens (source, symbol, address, created_at, raw_json)
                    VALUES (:source, :symbol, :address, :created_at, :raw_json)
                    """
                ),
                {
                    "source": "coingecko",
                    "symbol": tk.get("symbol") or "",
                    "address": tk.get("id") or "",
                    "created_at": now_utc,
                    "raw_json": json.dumps(tk),
                },
            )

        # только серьёзные проекты
        for tk in passed_tokens:
            conn.execute(
                text(
                    """
                    INSERT INTO tokens (symbol, address, source, listed_at, raw_json)
                    VALUES (:symbol, :address, :source, :listed_at, :raw_json)
                    ON CONFLICT (address) DO NOTHING
                    """
                ),
                {
                    "symbol": tk.get("symbol") or "",
                    "address": tk.get("id") or "",
                    "source": "coingecko",
                    "listed_at": now_utc,
                    "raw_json": json.dumps(tk),
                },
            )

        cutoff = now_utc - timedelta(hours=raw_retention_hours)
        conn.execute(
            text("DELETE FROM raw_tokens WHERE created_at < :cutoff"),
            {"cutoff": cutoff},
        )

    # -------- формируем список для Telegram -------- #

    serious_tokens_for_tg: List[Dict[str, Any]] = []
    for tk in passed_tokens:
        name = tk.get("name") or tk.get("symbol") or ""
        symbol = tk.get("symbol") or ""
        coin_id = tk.get("id") or ""

        link = ""
        if coin_id:
            # простая ссылка на страницу монеты на CoinGecko
            link = f"https://www.coingecko.com/en/coins/{coin_id}"

        serious_tokens_for_tg.append(
            {
                "name": name,
                "symbol": symbol,
                "link": link,
            }
        )

    return {
        "collected": len(coins),
        "passed": len(passed_tokens),
        "memecoins": len(memecoins),
        "serious": len(serious_tokens_list),
        "trash": len(trash_tokens),
        "serious_tokens": serious_tokens_for_tg,  # сюда смотрим из Telegram
        "analysis_mode": analysis_mode,
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
    }


async def run_once() -> Dict[str, Any]:
    """Обёртка для единичного запуска пайплайна (используется в main.py)."""
    return await collect_and_filter()


# -------------------- TELEGRAM (нужен для main.py) -------------------- #


async def send_telegram(message: str) -> None:
    """
    Простая отправка текста в Telegram.
    Используется /telegram_test и в будущем для ежедневного отчёта.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("[Telegram] TOKEN or CHAT_ID not set, skip sending")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                print(
                    "[Telegram] send failed:",
                    resp.status_code,
                    resp.text[:200],
                )
    except Exception as e:
        print(f"[Telegram] exception on send: {e}")
