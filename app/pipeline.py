# app/pipeline.py

import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import httpx

DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"


# ---------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------- #

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def make_time_window_previous_day() -> Tuple[datetime, datetime]:
    """
    Окно "прошедшие сутки" в UTC:
    от 00:00 вчера до 00:00 сегодня.
    """
    now = now_utc()
    start = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=1)
    return start, end


# ---------------------- DEXSCREENER: СБОР ---------------------- #

async def fetch_from_dexscreener(max_raw: int) -> List[Dict[str, Any]]:
    """
    Грубый сбор пар с DexScreener.

    DexScreener не даёт идеального REST "все новые пары за сутки",
    поэтому используем /search?q=... как приближение.
    ВАЖНО: мы НЕ падаем при ошибках, а просто логируем и идём дальше.
    """

    collected: List[Dict[str, Any]] = []

    # Поисковые запросы, которые часто цепляют мемки/новые токены
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

    print(f"[DexScreener] fetched ~{len(collected)} pairs (raw)")
    return collected


# ---------------------- КЛАССИФИКАЦИЯ ---------------------- #

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


def is_created_in_window(p: Dict[str, Any], start: datetime, end: datetime) -> bool:
    """
    Проверяем, появилась ли пара в окне [start, end).
    Используем поле pairCreatedAt (мс).
    """
    pair_created_at = p.get("pairCreatedAt")
    if not pair_created_at:
        return False
    try:
        ts = int(pair_created_at) / 1000.0
        created = datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return False
    return start <= created < end


def is_memecoin(p: Dict[str, Any]) -> bool:
    """Мемкоин / не мемкоин — чисто по названию и символу."""
    base = p.get("baseToken") or {}
    quote = p.get("quoteToken") or {}

    name = f"{base.get('name') or ''} {quote.get('name') or ''}"
    symbol = f"{base.get('symbol') or ''} {quote.get('symbol') or ''}"

    if contains_meme_keyword(name) or contains_meme_keyword(symbol):
        return True
    return False


def is_serious(p: Dict[str, Any]) -> bool:
    """
    Отбор серьёзных проектов (и мемов, и обычных):

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


def build_graph_link(p: Dict[str, Any]) -> str:
    """
    Строим ссылку на график на Dexscreener.
    """
    chain = (p.get("chainId") or "").lower()
    pair_address = p.get("pairAddress") or ""
    if not chain or not pair_address:
        return ""
    return f"https://dexscreener.com/{chain}/{pair_address}"


def extract_name_symbol(p: Dict[str, Any]) -> tuple[str, str]:
    base = p.get("baseToken") or {}
    name = base.get("name") or "Unknown"
    symbol = base.get("symbol") or "?"
    return name, symbol


# ---------------------- TELEGRAM ---------------------- #

async def send_telegram(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    print("[TG] token:", token[:10] if token else None)
    print("[TG] chat_id:", chat_id)

    if not token or not chat_id:
        print("[TG] ERROR: no token or chat id")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }

    print("[TG] url:", url)
    print("[TG] payload:", payload)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload)
            print("[TG] status:", resp.status_code)
            print("[TG] response:", resp.text)
    except Exception as e:
        print("[TG] EXCEPTION:", e)


def format_telegram_message(
    total: int,
    serious_tokens: List[Dict[str, Any]],
) -> str:
    """
    Краткий отчёт: сколько всего, сколько прошло фильтр, список имён + ссылки.
    """

    lines: List[str] = []

    lines.append("🔥 *Новые токены за прошедшие сутки*")
    lines.append(f"Всего найдено: *{total}*")
    lines.append(f"Серьёзных проектов: *{len(serious_tokens)}*")
    lines.append("")

    if not serious_tokens:
        lines.append("_Сегодня серьёзных проектов не найдено._")
        return "\n".join(lines)

    lines.append("🟩 *Список серьёзных проектов:*")
    lines.append("")

    for i, t in enumerate(serious_tokens, 1):
        name = t["name"]
        symbol = t["symbol"]
        link = t["link"] or "без ссылки"

        lines.append(f"{i}. *{name} ({symbol})*")
        lines.append(f"📊 {link}")
        lines.append("")

    return "\n".join(lines)


# ---------------------- ОСНОВНОЙ КОНВЕЙЕР ---------------------- #

async def collect_and_filter():
    """
    Ежедневный пайплайн:

    1) Получаем сырые пары с DexScreener (max_raw).
    2) Оставляем только те, что созданы за прошедшие сутки.
    3) Отбираем серьёзные проекты (и мемы, и не-мемы).
    4) Формируем короткий отчёт и отправляем в Telegram.
    """

    max_raw_total = int(os.getenv("MAX_RAW", "5000"))
    analysis_mode = os.getenv("ANALYSIS_MODE", "previous_day").lower()
    if analysis_mode != "previous_day":
        analysis_mode = "previous_day"

    window_start_utc, window_end_utc = make_time_window_previous_day()

    # 1. Сбор
    try:
        pairs_dex = await fetch_from_dexscreener(max_raw_total)
    except Exception as e:
        print(f"[DexScreener] fatal error in fetch: {e}")
        pairs_dex = []

    # 2. Фильтр "за прошедшие сутки"
    pairs_in_window: List[Dict[str, Any]] = [
        p for p in pairs_dex if is_created_in_window(p, window_start_utc, window_end_utc)
    ]

    total_new = len(pairs_in_window)

    # 3. Отбор серьёзных
    serious_tokens: List[Dict[str, Any]] = []

    for p in pairs_in_window:
        if not is_serious(p):
            continue

        name, symbol = extract_name_symbol(p)
        link = build_graph_link(p)

        serious_tokens.append(
            {
                "name": name,
                "symbol": symbol,
                "link": link,
            }
        )

    # 4. Формируем и отправляем сообщение в Telegram
    msg = format_telegram_message(total_new, serious_tokens)
    await send_telegram(msg)

    # Возвращаем краткую статистику для /run_daily
    return {
        "collected": len(pairs_dex),       # всего сырья из DEXScreener
        "new_in_window": total_new,        # реально новых за прошедшие сутки
        "serious": len(serious_tokens),    # сколько прошло фильтр
        "analysis_mode": analysis_mode,
        "window_start_utc": window_start_utc.isoformat(),
        "window_end_utc": window_end_utc.isoformat(),
    }


async def run_once():
    """Единичный запуск пайплайна (используется в main.py)."""
    return await collect_and_filter()
