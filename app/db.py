import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_engine = None


def _build_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    # Принудительно используем psycopg v3 (вместо psycopg2)
    if db_url.startswith("postgresql://"):
        db_url = "postgresql+psycopg://" + db_url[len("postgresql://"):]

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    return engine


def get_engine():
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
