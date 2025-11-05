# app/pipeline.py

import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import httpx
from sqlalchemy import text

# ВАЖНО: относительный импорт из того же пакета app
from .db import get_engine


COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


async def fetch_latest_coins(api_key: str, max_raw: int) -> List[Dict[str, Any]]:
    """
    Забираем монеты с CoinGecko, пока не наберём max_raw или не кончатся страницы.
    """
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    coins: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=30) as client:
        page = 1
        per_page = 250

        while len(coins) < max_raw:
            params = {
                "vs_currency": "usd",          # ОБЯЗАТЕЛЬНЫЙ параметр — из-за него раньше был 422
                "order": "volume_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": "false",
            }

            resp = await client.get(
                f"{COINGECKO_BASE_URL}/coins/markets",
                params=params,
                headers=headers,
            )
            resp.raise_for_status()
            batch = resp.json()

            if not batch:
                break

            coins.extend(batch)

            if len(batch) < per_page:
                # Страница неполная — дальше данных уже нет
                break

            page += 1

    # Обрезаем до max_raw на всякий случай
    return coins[:max_raw]


async def collect_and_filter():
    """
    Ежедневный конвейер:

    1) Сбор сырых токенов (CoinGecko) с ограничением по количеству.
    2) Отбор только за ПРЕДЫДУЩИЕ СУТКИ.
    3) Фильтрация по простым правилам (пока заглушка).
    4) Сохранение прошедших.
    5) Очистка старых сырых записей.
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

    # Окно "вчера" в UTC (для отчёта; CoinGecko напрямую по дате не режем)
    start_utc = (now_utc - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_utc = start_utc + timedelta(days=1)

    # -------- Шаг 1. Сбор сырых монет с CoinGecko --------
    cg_api_key = os.getenv("COINGECO_API_KEY", "")

    all_coins = await fetch_latest_coins(cg_api_key, max_raw)

    # Пока берём все собранные монеты как "сырые"
    raw_tokens = all_coins

    # -------- Шаг 2. Запись сырых токенов в raw_tokens --------
    # Ожидаем таблицу raw_tokens:
    # id (serial), source (text), symbol (text), address (text),
    # created_at (timestamptz), raw_json (jsonb)

    with engine.begin() as conn:
        for tk in raw_tokens:
            conn.execute(
                text(
                    """
                    INSERT INTO raw_tokens (source, symbol, address, created_at, raw_json)
                    VALUES (:source, :symbol, :address, :created_at, :raw_json)
                    """
                ),
                {
                    "source": "coingecko",
                    "symbol": tk.get("symbol", ""),
                    "address": tk.get("id", ""),
                    "created_at": now_utc,
                    "raw_json": tk,
                },
            )

    # -------- Шаг 3. Фильтрация (пока простая заглушка) --------
    # Здесь потом добавим реальные правила. Сейчас пропускаем всё.
    passed_tokens = raw_tokens

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
                    "symbol": tk.get("symbol", ""),
                    "address": tk.get("id", ""),
                    "source": "coingecko",
                    "listed_at": now_utc,
                    "raw_json": tk,
                },
            )

    # -------- Шаг 5. Очистка старых сырых --------
    cutoff = now_utc - timedelta(hours=raw_retention_hours)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw_tokens WHERE created_at < :cutoff"),
            {"cutoff": cutoff},
        )

    return {
        "collected": len(raw_tokens),
        "passed": len(passed_tokens),
        "analysis_mode": analysis_mode,
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
    }


async def run_once():
    """Обёртка для единичного запуска пайплайна (используется в main.py)."""
    return await collect_and_filter()
