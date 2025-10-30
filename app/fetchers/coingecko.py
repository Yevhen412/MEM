import httpx, asyncio, time

BASE = "https://api.coingecko.com/api/v3"

async def fetch_latest_coins(api_key: str | None = None, per_page: int = 250, pages: int = 2):
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    async with httpx.AsyncClient(timeout=20) as client:
        for page in range(1, pages + 1):
            params = {"order": "volume_desc", "per_page": per_page, "page": page, "sparkline": "false"}
            r = await client.get(f"{BASE}/coins/markets", params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
            for d in data:
                yield {
                    "source": "coingecko",
                    "symbol": d.get("symbol"),
                    "name": d.get("name"),
                    "chain": None,
                    "contract_address": None,
                    "website": None,
                    "whitepaper_url": None,
                    "coingecko_id": d.get("id"),
                    "github_url": None,
                    "twitter_url": None,
                    "price": d.get("current_price"),
                    "volume_24h": d.get("total_volume"),
                    "market_cap": d.get("market_cap"),
                    "fdv": d.get("fully_diluted_valuation"),
                    "dex_liquidity_usd": None,
                    "mvp": True,
                    "audit": False,
                    "tier_exchange": True,
                    "top10_holders_pct": None,
                    "single_holder_max_pct": None,
                    "team_public": True,
                    "investors_present": True,
                    "media_mentions": 1,
                    "engagement_quality": "Medium",
                    "roadmap": True,
                    "vesting_present": True
                }
            time.sleep(1)
