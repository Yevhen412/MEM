import httpx

BASE = "https://api.dexscreener.com/latest/dex"

async def fetch_new_pairs_last24():
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{BASE}/search", params={"q": "trending"})
        r.raise_for_status()
        data = r.json()
        for pair in data.get("pairs", []):
            base_token = pair.get("baseToken", {})
            chain = pair.get("chainId")
            contract_address = base_token.get("address")
            volume = (pair.get("volume") or {}).get("h24")
            liquidity = (pair.get("liquidity") or {}).get("usd")
            yield {
                "source": "dexscreener",
                "symbol": base_token.get("symbol"),
                "name": base_token.get("name"),
                "chain": chain,
                "contract_address": contract_address,
                "website": None,
                "whitepaper_url": None,
                "coingecko_id": None,
                "github_url": None,
                "twitter_url": None,
                "price": pair.get("priceUsd"),
                "volume_24h": volume,
                "dex_liquidity_usd": liquidity,
                "market_cap": None,
                "fdv": None,
                "mvp": False,
                "audit": False,
                "tier_exchange": False,
                "top10_holders_pct": None,
                "single_holder_max_pct": None,
                "team_public": False,
                "investors_present": False,
                "media_mentions": 0,
                "engagement_quality": "Low",
                "roadmap": False,
                "vesting_present": False
            }
