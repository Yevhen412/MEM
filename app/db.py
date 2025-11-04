# app/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 🔗 Адрес базы по умолчанию.
# 1) Открой на Railway свой сервис Postgres → вкладка Connect → Public Network.
# 2) Скопируй строку, которая начинается на "postgresql://..."
# 3) Вставь её вместо ТЕКУЩЕГО текста в кавычках ниже
# 4) И ДОБАВЬ "+psycopg" после "postgresql"

DEFAULT_DB_URL = "postgresql+psycopg://postgres:GpFPUHewrQheWGLArCJZtPXCURiaxGmN@maglev.proxy.rlwy.net:37635/railway"

# Сначала пробуем взять из переменной окружения DATABASE_URL,
# если Railway снова её не подставит — используем DEFAULT_DB_URL.
DATABASE_URL = os.getenv("DATABASE_URL") or DEFAULT_DB_URL

# Создаём один общий engine для всего приложения
_engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def get_engine():
    """Вернуть общий engine (для raw SQL, если где-то понадобится)."""
    return _engine


def get_session():
    """Создать новую сессию SQLAlchemy (если понадобится в будущем)."""
    return SessionLocal()
