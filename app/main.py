# app/main.py

import os
import asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .pipeline import run_once, send_telegram

app = FastAPI()


@app.get("/telegram_test")
async def telegram_test():
    # Запускаем отправку сообщения в фоне, чтобы не ждать ответа Telegram
    asyncio.create_task(
        send_telegram("Тестовое сообщение из Railway 🚀")
    )
    return {"status": "scheduled"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/run_daily")
async def run_daily():
    try:
        result = await run_once()
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    # ВАЖНО: никаких лишних параметров типа h11_max_incomplete_event_size
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
