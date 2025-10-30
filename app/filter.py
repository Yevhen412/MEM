from .config import (
    MIN_VOLUME_USD, MIN_DEX_LIQ_USD,
    MAX_TOP10_PCT, MAX_SINGLE_HOLDER_PCT,
    REQUIRE_AUDIT, REQUIRE_PUBLIC_TEAM
)

def passes_strict_filter(token: dict) -> bool:
    # Block 1: Legitimacy & Team
    if REQUIRE_PUBLIC_TEAM and not token.get("team_public"):
        return False
    if not (token.get("investors_present") or (token.get("media_mentions", 0) >= 1)):
        return False

    # Block 2: Product & Tech
    if not token.get("mvp"):
        return False
    if REQUIRE_AUDIT and not token.get("audit"):
        return False

    # Block 3: Capital & Infra
    if not (token.get("tier_exchange") or (
        (token.get("volume_24h") or 0) >= MIN_VOLUME_USD and
        (token.get("dex_liquidity_usd") or 0) >= MIN_DEX_LIQ_USD
    )):
        return False

    # Block 4: Tokenomics
    if token.get("vesting_present") is False:
        return False
    if (token.get("top10_holders_pct") or 100) > MAX_TOP10_PCT:
        return False
    if (token.get("single_holder_max_pct") or 100) > MAX_SINGLE_HOLDER_PCT:
        return False

    # Block 5: Publicity & Ecosystem
    if token.get("github_activity_level", "Low") == "Low":
        return False
    if token.get("engagement_quality", "Low") == "Low":
        return False
    if not token.get("roadmap", False):
        return False

    return True
