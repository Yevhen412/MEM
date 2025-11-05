# app/main.py

from fastapi import FastAPI

from .pipeline import run_once  # относительный импорт из app.pipeline

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/run_daily")
async def run_daily():
    """
    Ручной запуск дневного пайплайна.
    """
    result = await run_once()
    return result
