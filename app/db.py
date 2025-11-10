# app/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Сюда вставь данные из Railway → Postgres → Variables
# Пользователь: POSTGRES_USER (обычно postgres)
# Пароль: GpFPUHewrQheWGLArCJZtPXCURiaxGmN
# Хост:   RAILWAY_PRIVATE_DOMAIN или тот, что ты использовал в DBeaver
# Порт:   37635 (у тебя)
# База:   railway
DEFAULT_DB_URL = (
    "postgresql+psycopg://postgres:ТВОЙ_ПАРОЛЬ@maglev.proxy.rlwy.net:37635/railway"
)

# Берём из переменной окружения, если её вдруг когда-нибудь добавишь,
# иначе используем наш дефолт.
DATABASE_URL = os.getenv("DATABASE_URL") or DEFAULT_DB_URL

# На всякий случай: если в окружении опять окажется старый формат
# "postgres://..." или "postgresql://...", мы его исправим на psycopg.
if "psycopg" not in DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = "postgresql+psycopg" + DATABASE_URL[len("postgres") :]
    elif DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = "postgresql+psycopg" + DATABASE_URL[len("postgresql") :]

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
