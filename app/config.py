import os

def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name, str(default)).lower()
    return val in ("1", "true", "yes", "y")

DATABASE_URL = os.getenv("DATABASE_URL", "")
CG_API_KEY = os.getenv("CG_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Amsterdam")

MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "1000000"))
MIN_DEX_LIQ_USD = float(os.getenv("MIN_DEX_LIQ_USD", "200000"))
MAX_TOP10_PCT = float(os.getenv("MAX_TOP10_PCT", "70"))
MAX_SINGLE_HOLDER_PCT = float(os.getenv("MAX_SINGLE_HOLDER_PCT", "20"))
REQUIRE_AUDIT = env_bool("REQUIRE_AUDIT", True)
REQUIRE_PUBLIC_TEAM = env_bool("REQUIRE_PUBLIC_TEAM", True)

PURGE_NON_PASSED = env_bool("PURGE_NON_PASSED", True)
RAW_RETENTION_HOURS = int(os.getenv("RAW_RETENTION_HOURS", "24"))
