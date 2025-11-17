# app/pipeline.py

import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import httpx
from sqlalchemy import text

from .db import get_engine

DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

# ---------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------- #


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def make_time_window_previous_day() -> tuple[datetime, datetime]:
    """Окно 'вчера' в UTC (для будущей аналитики/отчётов)."""
    now = now_utc()
    start = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=1)
    return start, end


# ---------------------- DEXSCREENER: СБОР ---------------------- #


async def fetch_from_dexscreener(max_raw: int) -> List[Dict[str, Any]]:
    """
    Грубый сбор “свежих” пар с DexScreener.

    DexScreener не даёт идеального REST под “все новые пары за сутки”,
    поэтому используем /search?q=... как приближение.

    Важно: мы НЕ падаем при ошибках, а просто логируем и идём дальше.
    """

    collected: List[Dict[str, Any]] = []

    # Поисковые запросы, которые часто цепляют мемки
    queries = ["new", "meme", "pepe", "doge", "inu", "shib", "cat", "frog"]

    # Сети, которые нас интересуют
    chains = {"solana", "ethereum", "base", "bsc", "arbitrum"}

    async with httpx.AsyncClient(timeout=30) as client:
        for q in queries:
            if len(collected) >= max_raw:
                break

            try:
                resp = await client.get(
                    f"{DEXSCREENER_BASE_URL}/search",
                    params={"q": q},
                )
            except Exception as e:
                print(f"[DexScreener] request error for q={q}: {e}")
                continue

            if resp.status_code != 200:
                print(
                    f"[DexScreener] status {resp.status_code} q={q}: "
                    f"{resp.text[:200]}"
                )
                continue

            try:
                data = resp.json()
            except Exception as e:
                print(f"[DexScreener] JSON parse error q={q}: {e}")
                continue

            pairs: List[Dict[str, Any]] = data.get("pairs") or []
            for p in pairs:
                if len(collected) >= max_raw:
                    break

                chain_id = (p.get("chainId") or "").lower()
                if chains and chain_id not in chains:
                    continue

                collected.append(p)

    print(f"[DexScreener] fetched ~{len(collected)} pairs")
    return collected


# ---------------------- ФИЛЬТРАЦИЯ МЕМКОИНОВ ---------------------- #

MEME_KEYWORDS = [
    "meme",
    "pepe",
    "wojak",
    "doge",
    "shib",
    "inu",
    "floki",
    "bonk",
    "cat",
    "kitty",
    "frog",
    "elon",
]


def contains_meme_keyword(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in MEME_KEYWORDS)


def is_memecoin_from_dexscreener(p: Dict[str, Any]) -> bool:
    """Грубый детектор мемкоинов по DexScreener-паре."""
    base_token = p.get("baseToken") or {}
    quote_token = p.get("quoteToken") or {}

    name = f"{base_token.get('name') or ''} {quote_token.get('name') or ''}"
    symbol = f"{base_token.get('symbol') or ''} {quote_token.get('symbol') or ''}"

    # 1) По названию/символам
    if contains_meme_keyword(name) or contains_meme_keyword(symbol):
        return True

    # 2) Эвристика по цене и ликвидности
    try:
        price_usd = float(p.get("priceUsd") or 0)
    except Exception:
        price_usd = 0.0

    try:
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
    except Exception:
        liq = 0.0

    # дешёвые монеты со средней ликвидностью — часто мемки
    if price_usd < 0.01 and 2_000 <= liq <= 500_000:
        return True

    return False


def is_serious_memecoin_from_dexscreener(p: Dict[str, Any]) -> bool:
    """
    Отбор более-менее серьёзных мемкоинов:

    - ликвидность > 20k
    - объём за 24ч > 50k
    - пара живёт хотя бы 6 часов
    """
    try:
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
    except Exception:
        liq = 0.0

    try:
        vol_h24 = float((p.get("volume") or {}).get("h24") or 0)
    except Exception:
        vol_h24 = 0.0

    # возраст пары
    age_ok = True
    pair_created_at = p.get("pairCreatedAt")
    try:
        if pair_created_at:
            ts = int(pair_created_at) / 1000.0
            created = datetime.fromtimestamp(ts, tz=timezone.utc)
            age = now_utc() - created
            age_ok = age >= timedelta(hours=6)
    except Exception:
        age_ok = True  # если не смогли посчитать — не режем

    return liq > 20_000 and vol_h24 > 50_000 and age_ok


# ---------------------- ОСНОВНОЙ КОНВЕЙЕР ---------------------- #


async def collect_and_filter():
    """
    Основной ежедневный пайплайн:

    1) Забираем пары с DexScreener (max_raw шт).
    2) Пишем всё сырьё в raw_tokens.
    3) Отбираем мемкоины.
    4) Делим мемкоины на serious / trash.
    5) serious пишем в tokens.
    6) Подчищаем старые raw_tokens.
    """

    engine = get_engine()

    # ----- Параметры из ENV -----
    max_raw_total = int(os.getenv("MAX_RAW", "5000"))
    analysis_mode = os.getenv("ANALYSIS_MODE", "previous_day").lower()
    raw_retention_hours = int(os.getenv("RAW_RETENTION_HOURS", "24"))

    if analysis_mode != "previous_day":
        analysis_mode = "previous_day"

    window_start_utc, window_end_utc = make_time_window_previous_day()

    # ----- 1. Сбор из DexScreener -----
    try:
        pairs_dex = await fetch_from_dexscreener(max_raw_total)
    except Exception as e:
        print(f"[DexScreener] fatal error in fetch: {e}")
        pairs_dex = []

    now = now_utc()

    # ----- 2. Приводим к общему формату сырых -----
    raw_items: List[Dict[str, Any]] = []
    for p in pairs_dex[:max_raw_total]:
        base = p.get("baseToken") or {}
        symbol = base.get("symbol") or ""
        address = base.get("address") or p.get("pairAddress") or ""

        raw_items.append(
            {
                "source": "dexscreener",
                "symbol": symbol,
                "address": address,
                "created_at": now,
                "payload": p,
            }
        )

    # ----- 3. Записываем сырьё в raw_tokens -----
    with engine.begin() as conn:
        for item in raw_items:
            conn.execute(
                text(
                    """
                    INSERT INTO raw_tokens (source, symbol, address, created_at, raw_json)
                    VALUES (:source, :symbol, :address, :created_at, :raw_json)
                    """
                ),
                {
                    "source": item["source"],
                    "symbol": item["symbol"],
                    "address": item["address"],
                    "created_at": item["created_at"],
                    "raw_json": json.dumps(item["payload"]),
                },
            )

    # ----- 4. Фильтрация мемкоинов и разделение serious / trash -----
    memecoins: List[Dict[str, Any]] = []
    serious: List[Dict[str, Any]] = []
    trash: List[Dict[str, Any]] = []

    for item in raw_items:
        payload = item["payload"]

        if not is_memecoin_from_dexscreener(payload):
            continue

        memecoins.append(item)

        if is_serious_memecoin_from_dexscreener(payload):
            serious.append(item)
        else:
            trash.append(item)

    # ----- 5. Запись serious-мемкоинов в tokens -----
    with engine.begin() as conn:
        for item in serious:
            conn.execute(
                text(
                    """
                    INSERT INTO tokens (symbol, address, source, listed_at, raw_json)
                    VALUES (:symbol, :address, :source, :listed_at, :raw_json)
                    ON CONFLICT (address) DO NOTHING
                    """
                ),
                {
                    "symbol": item["symbol"],
                    "address": item["address"],
                    "source": item["source"],
                    "listed_at": now,
                    "raw_json": json.dumps(item["payload"]),
                },
            )

        # очистка старых raw
        cutoff = now - timedelta(hours=raw_retention_hours)
        conn.execute(
            text("DELETE FROM raw_tokens WHERE created_at < :cutoff"),
            {"cutoff": cutoff},
        )

    return {
        "collected": len(raw_items),
        "memecoins": len(memecoins),
        "serious": len(serious),
        "trash": len(trash),
        "analysis_mode": analysis_mode,
        "window_start_utc": window_start_utc.isoformat(),
        "window_end_utc": window_end_utc.isoformat(),
    }


async def run_once():
    """Единичный запуск пайплайна (используется в main.py)."""
    return await collect_and_filter()
