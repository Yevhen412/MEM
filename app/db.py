# app/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Берём строку подключения из переменной окружения DATABASE_URL,
# которая в Railway у MEM ссылается на Postgres.DATABASE_URL.
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# Создаём один общий engine на всё приложение
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# Если где-то потом захочешь использовать сессии ORM:
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_engine():
    """Вернуть общий engine для использования в других модулях."""
    return engine
