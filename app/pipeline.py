# app/pipeline.py

import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import json

import httpx
from sqlalchemy import text

# относительный импорт из app/db.py
from .db import get_engine

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ МЕМКОИНОВ ----------

def is_memecoin(token: Dict[str, Any]) -> bool:
    """
    Очень грубый фильтр: считаем монету мемкой, если по имени / символу
    видно мем-тематику.
    """
    name = (token.get("name") or "").lower()
    symbol = (token.get("symbol") or "").lower()

    MEME_KEYWORDS = [
        "meme", "pepe", "doge", "shib", "inu",
        "floki", "bonk", "wif", "dog", "cat"
    ]

    haystack = f"{name} {symbol}"
    return any(k in haystack for k in MEME_KEYWORDS)


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def score_token(token: Dict[str, Any]) -> float:
    """
    Очень простой скоринг по данным CoinGecko.

    Чем выше score, тем более «серьёзным» считаем проект:
    - объём торгов
    - капа
    - движение цены за сутки
    """
    market_cap = _safe_float(token.get("market_cap"))
    volume_24h = _safe_float(token.get("total_volume"))
    price_change_24h = _safe_float(token.get("price_change_percentage_24h"))

    score = 0.0

    # Объём торгов
    if volume_24h > 50_000:
        score += 1
    if volume_24h > 200_000:
        score += 1

    # Рыночная капитализация
    if market_cap > 200_000:
        score += 1
    if market_cap > 1_000_000:
        score += 1

    # Изменение цены за сутки — признак активности
    if abs(price_change_24h) > 5:
        score += 0.5
    if abs(price_change_24h) > 20:
        score += 0.5

    return score


# ---------- ЗАГРУЗКА ДАННЫХ ИЗ COINGECKO ----------

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


# ---------- ОСНОВНОЙ ПАЙПЛАЙН ----------

async def collect_and_filter():
    """
    Ежедневный конвейер:

    1) Сбор сырых токенов (CoinGecko) с ограничением по количеству.
    2) Сохранение ВСЕГО в raw_tokens (как "сырой лог").
    3) Отбор только мемкоинов.
    4) Примитивный скоринг: serious / trash.
    5) Сохранение мемкоинов в tokens с полями _score и _tier в raw_json.
    6) Очистка старых записей из raw_tokens.
    """

    engine = get_engine()

    # -------- Параметры из переменных окружения --------
    max_raw = int(os.getenv("MAX_RAW", "5000"))
    analysis_mode = os.getenv("ANALYSIS_MODE", "previous_day").lower()
    raw_retention_hours = int(os.getenv("RAW_RETENTION_HOURS", "24"))

    if analysis_mode != "previous_day":
        analysis_mode = "previous_day"

    # Порог "серьёзности" проекта
    serious_threshold = float(os.getenv("SERIOUS_SCORE_THRESHOLD", "3.0"))

    # Текущее время в UTC
    now_utc = datetime.now(timezone.utc)

    # Окно «вчера» в UTC (для отчёта; CoinGecko по дате мы пока не режем)
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
                    "raw_json": json.dumps(tk),
                },
            )

    # -------- Шаг 3. Фильтрация мемкоинов --------
    memecoins: List[Dict[str, Any]] = [tk for tk in coins if is_memecoin(tk)]

    # -------- Шаг 4. Скоринг и подготовка к записи в tokens --------
    serious_count = 0
    trash_count = 0
    prepared_for_db: List[Dict[str, Any]] = []

    for tk in memecoins:
        score = score_token(tk)
        tier = "serious" if score >= serious_threshold else "trash"

        enriched = dict(tk)
        enriched["_score"] = score
        enriched["_tier"] = tier
        enriched["_source"] = "coingecko"

        if tier == "serious":
            serious_count += 1
        else:
            trash_count += 1

        prepared_for_db.append(enriched)

    # -------- Шаг 5. Сохранение прошедших мемкоинов в tokens --------
    # Ожидаем таблицу tokens:
    # symbol (text), address (text PRIMARY KEY), source (text),
    # listed_at (timestamptz), raw_json (jsonb)

    with engine.begin() as conn:
        for tk in prepared_for_db:
            conn.execute(
                text(
                    """
                    INSERT INTO tokens (symbol, address, source, listed_at, raw_json)
                    VALUES (:symbol, :address, :source, :listed_at, :raw_json)
                    ON CONFLICT (address) DO UPDATE
                    SET raw_json = EXCLUDED.raw_json
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

        # -------- Шаг 6. Очистка старых сырых --------
        cutoff = now_utc - timedelta(hours=raw_retention_hours)
        conn.execute(
            text("DELETE FROM raw_tokens WHERE created_at < :cutoff"),
            {"cutoff": cutoff},
        )

    return {
        "collected": len(coins),
        "memecoins": len(memecoins),
        "serious": serious_count,
        "trash": trash_count,
        "analysis_mode": analysis_mode,
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
    }


async def run_once():
    """Обёртка для единичного запуска пайплайна (используется в main.py)."""
    return await collect_and_filter()
