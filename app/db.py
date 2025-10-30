from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import DATABASE_URL

_engine = None  # type: Engine | None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set")
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=1800)
    return _engine

SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS tokens_raw (
  id SERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  symbol TEXT,
  name TEXT,
  chain TEXT,
  contract_address TEXT,
  website TEXT,
  whitepaper_url TEXT,
  coingecko_id TEXT,
  cmc_id TEXT,
  github_url TEXT,
  twitter_url TEXT,
  price NUMERIC,
  volume_24h NUMERIC,
  dex_liquidity_usd NUMERIC,
  market_cap NUMERIC,
  fdv NUMERIC,
  mvp BOOLEAN,
  audit BOOLEAN,
  tier_exchange BOOLEAN,
  top10_holders_pct NUMERIC,
  single_holder_max_pct NUMERIC,
  team_public BOOLEAN,
  investors_present BOOLEAN,
  media_mentions INT,
  engagement_quality TEXT,
  roadmap BOOLEAN,
  vesting_present BOOLEAN,
  first_seen_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tokens_filtered (
  id SERIAL PRIMARY KEY,
  symbol TEXT,
  name TEXT,
  chain TEXT,
  contract_address TEXT,
  website TEXT,
  whitepaper_url TEXT,
  exchanges TEXT,
  volume_24h NUMERIC,
  dex_liquidity_usd NUMERIC,
  audit BOOLEAN,
  github_url TEXT,
  twitter_url TEXT,
  unique_value TEXT,
  passed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sources_log (
  id SERIAL PRIMARY KEY,
  source TEXT,
  items INT,
  status TEXT,
  info TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
'''

def ensure_schema():
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
