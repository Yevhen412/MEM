from sqlalchemy import text
from .db import get_engine, ensure_schema
from .config import CG_API_KEY, PURGE_NON_PASSED, RAW_RETENTION_HOURS
from .fetchers.coingecko import fetch_latest_coins
from .fetchers.dexscreener import fetch_new_pairs_last24
from .filter import passes_strict_filter
from .notifier import send_telegram_message
import asyncio, time

async def collect_and_filter():
    ensure_schema()
    eng = get_engine()

    total_raw = 0
    total_passed = 0
    passed_ids = []

    async def insert_raw(token: dict):
        nonlocal total_raw
        total_raw += 1
        keys = ",".join(token.keys())
        vals = ",".join([f":{k}" for k in token.keys()])
        q = text(f"INSERT INTO tokens_raw ({keys}) VALUES ({vals}) RETURNING id")
        with eng.begin() as conn:
            rid = conn.execute(q, token).scalar()
        return rid

    # Collect CoinGecko
    async for t in fetch_latest_coins(CG_API_KEY):
        await insert_raw(t)

    # Collect DEX Screener
    async for t in fetch_new_pairs_last24():
        await insert_raw(t)

    # Filter
    with eng.begin() as conn:
        rows = conn.execute(text(f"SELECT * FROM tokens_raw WHERE first_seen_at >= NOW() - INTERVAL '{RAW_RETENTION_HOURS} hours'")).mappings()
        passed = []
        for r in rows:
            token = dict(r)
            if passes_strict_filter(token):
                total_passed += 1
                passed.append(token)
                passed_ids.append(token["id"])

        for r in passed:
            conn.execute(text("""
                INSERT INTO tokens_filtered
                (symbol, name, chain, contract_address, website, whitepaper_url, exchanges, volume_24h, dex_liquidity_usd, audit, github_url, twitter_url, unique_value)
                VALUES
                (:symbol, :name, :chain, :contract_address, :website, :whitepaper_url, :exchanges, :volume_24h, :dex_liquidity_usd, :audit, :github_url, :twitter_url, :unique_value)
            """), {
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
                "unique_value": r.get("unique_value") or ""
            })

        # Purge non-passed rows in the retention window
        if PURGE_NON_PASSED:
            if passed_ids:
                conn.execute(text(f"DELETE FROM tokens_raw WHERE first_seen_at >= NOW() - INTERVAL '{RAW_RETENTION_HOURS} hours' AND id NOT IN :passed"), {"passed": tuple(passed_ids)})
            else:
                conn.execute(text(f"DELETE FROM tokens_raw WHERE first_seen_at >= NOW() - INTERVAL '{RAW_RETENTION_HOURS} hours'"))

        # Purge older than retention anyway
        conn.execute(text(f"DELETE FROM tokens_raw WHERE first_seen_at < NOW() - INTERVAL '{RAW_RETENTION_HOURS} hours'"))

        conn.execute(text("INSERT INTO sources_log (source, items, status, info) VALUES ('pipeline', :items, 'ok', :info)"), {"items": total_raw, "info": f"passed={total_passed}"})

    await send_telegram_message(f"Linda: собрано {total_raw} токенов за {RAW_RETENTION_HOURS}h, прошло фильтр — {total_passed}.")

async def run_once():
    await collect_and_filter()
