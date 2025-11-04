from __future__ import annotations

import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

from .db import get_engine
from .config import (
    CG_API_KEY,                     # ключ CoinGecko (может быть пустым)
    PURGE_NON_PASSED,               # True/False — удалять ли непройденные
    RAW_RETENTION_HOURS,            # сколько часов хранить сырой поток
    TIMEZONE,                       # строка таймзоны, напр. "Europe/Amsterdam"
)
from .fetchers.coingecko import fetch_latest_coins
from .fetchers.dexscreener import fetch_new_pairs_last24
from .filter import passes_strict_filter
from .notifier import send_telegram_message


def _previous_day_window(tz_name: str) -> tuple[datetime, datetime]:
    """
    Диапазон предыдущих суток в указанном TZ:
    [вчера 00:00; сегодня 00:00)
    """
    tz = ZoneInfo(tz_name)
    now_tz = datetime.now(tz)
    today_00 = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today_00 - timedelta(days=1)
    end = today_00
    # Переведём в UTC-время БД (PostgreSQL хранит TIMESTAMPTZ)
    return start.astimezone(ZoneInfo("UTC")), end.astimezone(ZoneInfo("UTC"))


async def collect_and_filter():
    """
    Ежедневный конвейер:
    1) Сбор сырых токенов (CoinGecko + DEX Screener) с ограничением MAX_RAW.
    2) Отбор только за ПРЕДЫДУЩИЕ СУТКИ.
    3) Фильтрация по строгим правилам.
    4) Сохранение прошедших.
    5) Очистка непройденных и старых сырых записей.
    6) Уведомление в Telegram (если настроено).
    """
    
    eng = get_engine()

    # -------- Параметры из переменных окружения --------
    # сколько максимум сырых записей собирать за прогон
    MAX_RAW = int(os.getenv("MAX_RAW", "5000"))
    # режим анализа (сейчас поддерживаем только previous_day)
    ANALYSIS_MODE = os.getenv("ANALYSIS_MODE", "previous_day").lower()

    if ANALYSIS_MODE != "previous_day":
        # На будущее можно расширить, но сейчас мы жёстко работаем по "вчера".
        ANALYSIS_MODE = "previous_day"

    # временное окно предыдущих суток
    start_utc, end_utc = _previous_day_window(TIMEZONE)

    total_raw = 0
    total_passed = 0
    passed_ids: list[int] = []

    async def insert_raw(token: dict) -> str | None:
        nonlocal total_raw
        if total_raw >= MAX_RAW:
            return "STOP"
        total_raw += 1
        keys = ",".join(token.keys())
        vals = ",".join([f":{k}" for k in token.keys()])
        q = text(f"INSERT INTO tokens_raw ({keys}) VALUES ({vals})")
        with eng.begin() as conn:
            conn.execute(q, token)
        return None

    # -------- 1) Сбор --------
    # CoinGecko — топ по объёму, несколько страниц
    async for t in fetch_latest_coins(CG_API_KEY):
        res = await insert_raw(t)
        if res == "STOP":
            break

    # DEX Screener — свежие пары/тренды
    if total_raw < MAX_RAW:
        async for t in fetch_new_pairs_last24():
            res = await insert_raw(t)
            if res == "STOP":
                break

    # -------- 2–4) Фильтрация окна "вчера" и сохранение прошедших --------
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM tokens_raw
                WHERE first_seen_at >= :start AND first_seen_at < :end
                """
            ),
            {"start": start_utc, "end": end_utc},
        ).mappings()

        for r in rows:
            token = dict(r)
            if passes_strict_filter(token):
                total_passed += 1
                passed_ids.append(token["id"])

                conn.execute(
                    text(
                        """
                        INSERT INTO tokens_filtered
                        (symbol, name, chain, contract_address, website, whitepaper_url,
                         exchanges, volume_24h, dex_liquidity_usd, audit, github_url,
                         twitter_url, unique_value)
                        VALUES
                        (:symbol, :name, :chain, :contract_address, :website, :whitepaper_url,
                         :exchanges, :volume_24h, :dex_liquidity_usd, :audit, :github_url,
                         :twitter_url, :unique_value)
                        """
                    ),
                    {
                        "symbol": r.get("symbol"),
                        "name": r.get("name"),
                        "chain": r.get("chain"),
                        "contract_address": r.get("contract_address"),
                        "website": r.get("website"),
                        "whitepaper_url": r.get("whitepaper_url"),
                        "exchanges": "CEX" if r.get("tier_exchange") else "DEX",
                        "volume_24h": r.get("volume_24h"),
                        "dex_liquidity_usd": r.get("dex_liquidity_usd"),
                        "audit": r.get("audit"),
                        "github_url": r.get("github_url"),
                        "twitter_url": r.get("twitter_url"),
                        "unique_value": r.get("unique_value") or "",
                    },
                )

        # -------- 5) Очистка --------
        # a) Удаляем непройденные за окно предыдущих суток
        if PURGE_NON_PASSED:
            if passed_ids:
                conn.execute(
                    text(
                        """
                        DELETE FROM tokens_raw
                        WHERE first_seen_at >= :start AND first_seen_at < :end
                          AND id NOT IN :passed
                        """
                    ),
                    {"start": start_utc, "end": end_utc, "passed": tuple(passed_ids)},
                )
            else:
                conn.execute(
                    text(
                        """
                        DELETE FROM tokens_raw
                        WHERE first_seen_at >= :start AND first_seen_at < :end
                        """
                    ),
                    {"start": start_utc, "end": end_utc},
                )

        # b) Чистим всё, что старше окна хранения RAW_RETENTION_HOURS
        conn.execute(
            text(
                f"""
                DELETE FROM tokens_raw
                WHERE first_seen_at < NOW() AT TIME ZONE 'UTC' - INTERVAL '{RAW_RETENTION_HOURS} hours'
                """
            )
        )

        # Лог конвейера
        conn.execute(
            text(
                """
                INSERT INTO sources_log (source, items, status, info)
                VALUES ('pipeline', :items, 'ok', :info)
                """
            ),
            {
                "items": total_raw,
                "info": f"window={start_utc.isoformat()}..{end_utc.isoformat()}, "
                        f"passed={total_passed}, purge_non_passed={PURGE_NON_PASSED}, "
                        f"retention_h={RAW_RETENTION_HOURS}, max_raw={MAX_RAW}",
            },
        )

    # -------- 6) Уведомление --------
    human_start = start_utc.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M")
    human_end = end_utc.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M")
    await send_telegram_message(
        f"Linda: окно {human_start} → {human_end}\n"
        f"Собрано сырых: {total_raw}\n"
        f"Прошли фильтр: {total_passed}\n"
        f"Непрошедшие удалены: {str(PURGE_NON_PASSED)}"
    )


async def run_once():
    await collect_and_filter()
