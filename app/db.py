# app/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Адрес базы по умолчанию (скопированный из Railway → Postgres → Connect → Public Network)
# Если Railway опять не подставит переменную окружения DATABASE_URL,
# используем этот дефолтный URL.
DEFAULT_DB_URL = "postgresql+psycopg://postgres:GPpfUHEwrQheWGLArcCJ2tPXCURixaxGm@maglev.proxy.rlwy.net:37635/railway"

# Сначала пробуем взять из переменной окружения DATABASE_URL,
# если её нет — падаем назад на DEFAULT_DB_URL.
DATABASE_URL = os.getenv("DATABASE_URL") or DEFAULT_DB_URL

# Создаём один общий engine для всего приложения
_engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def get_engine():
    """Вернуть общий engine для работы с базой."""
    return _engine


def get_session():
    """Вернуть новую сессию SQLAlchemy (если понадобится)."""
    return SessionLocal()
