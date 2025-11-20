# app/pipeline.py
# start

import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import httpx
from sqlalchemy import text

# относительный импорт из app/db.py
from .db import get_engine

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
    """Грубые пороги, чтобы отсеять совсем мусор."""
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

    # достаточно «серьёзные» по ликвидности
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


COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


async def fetch_latest_coins(cg_api_key: str | None, max_raw: int) -> List[Dict[str, Any]]:
    """
    Забираем токены с CoinGecko постранично, максимум max_raw штук.
    Ошибки 400/500 НЕ роняют приложение — просто логируем и выходим.
    """
    per_page = 250
    page = 1
    collected: List[Dict[str, Any]] = []

    headers: Dict[str, str] = {}
    # Если когда-нибудь понадобится Pro-ключ CoinGecko:
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
            }

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
                # Монеты закончились
                break

            for token in data:
                collected.append(token)
                if len(collected) >= max_raw:
                    break

            # Если вернули меньше per_page — страниц больше нет
            if len(data) < per_page or len(collected) >= max_raw:
                break

            page += 1

    print(f"[CoinGecko] fetched {len(collected)} tokens total")
    return collected


async def collect_and_filter():
    """
    Ежедневный конвейер:

    1) Сбор сырых токенов (CoinGecko) с ограничением по количеству.
    2) Отбор только за ПРЕДЫДУЩИЕ СУТКИ (окно для отчёта).
    3) Фильтрация по простым правилам (пока заглушка, пропускаем всё).
    4) Сохранение прошедших в tokens.
    5) Очистка старых записей из raw_tokens.
    """

    engine = get_engine()

    # -------- Параметры из переменных окружения --------
    max_raw = int(os.getenv("MAX_RAW", "5000"))
    analysis_mode = os.getenv("ANALYSIS_MODE", "previous_day").lower()
    raw_retention_hours = int(os.getenv("RAW_RETENTION_HOURS", "24"))

    if analysis_mode != "previous_day":
        analysis_mode = "previous_day"

    # Текущее время в UTC
    now_utc = datetime.now(timezone.utc)

    # Окно «вчера» в UTC (для отчёта; CoinGecko по дате мы не режем)
    start_utc = (now_utc - timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    end_utc = start_utc + timedelta(days=1)

    # -------- Шаг 1. Сбор сырых монет с CoinGecko --------
    cg_api_key = os.getenv("COINGECO_API_KEY", "")

    coins: List[Dict[str, Any]] = await fetch_latest_coins(cg_api_key, max_raw)

    # -------- Шаг 2. Запись сырых токенов в raw_tokens --------
    # Ожидаем таблицу raw_tokens:
    # id (serial), source (text), symbol (text), address (text),
    # created_at (timestamptz), raw_json (jsonb)

    with engine.begin() as conn:
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
                    # ВАЖНО: dict -> JSON-строка, чтобы psycopg2 смог это вставить
                    "raw_json": json.dumps(tk, ensure_ascii=False),
                },
            )

        # -------- Шаг 3. Фильтрация --------
    memecoins: List[Dict[str, Any]] = []
    serious_tokens: List[Dict[str, Any]] = []
    trash_tokens: List[Dict[str, Any]] = []

    for tk in coins:
        cls = classify_token(tk)

        if cls in ("serious", "serious_memecoin"):
            serious_tokens.append(tk)

        if "memecoin" in cls:
            memecoins.append(tk)

        if "trash" in cls:
            trash_tokens.append(tk)

    # В БД и в результат пайплайна пойдут только серьёзные проекты
    passed_tokens = serious_tokens


    # -------- Шаг 4. Сохранение прошедших в tokens --------
    # Ожидаем таблицу tokens:
    # symbol (text), address (text PRIMARY KEY), source (text),
    # listed_at (timestamptz), raw_json (jsonb)

    with engine.begin() as conn:
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
                    # снова dict -> JSON-строка
                    "raw_json": json.dumps(tk, ensure_ascii=False),
                },
            )

        # -------- Шаг 5. Очистка старых сырых --------
        cutoff = now_utc - timedelta(hours=raw_retention_hours)
        conn.execute(
            text("DELETE FROM raw_tokens WHERE created_at < :cutoff"),
            {"cutoff": cutoff},
        )

       return {
        "collected": len(coins),
        "passed": len(passed_tokens),
        "memecoins": len(memecoins),
        "serious": len(serious_tokens),
        "trash": len(trash_tokens),
        "analysis_mode": analysis_mode,
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
    }



async def run_once():
    """Обёртка для единичного запуска пайплайна (используется в main.py)."""
    return await collect_and_filter()
