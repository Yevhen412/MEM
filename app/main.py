# app/main.py

import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .pipeline import run_once
from .notifier import send_telegram, format_telegram_message

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/run_daily")
async def run_daily():
    try:
        result = await run_once()

        # формируем текст и отправляем в Telegram
        message = format_telegram_message(result)
        await send_telegram(message)

        # а в ответ API по-прежнему отдаём "сырой" JSON
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/telegram_test")
async def telegram_test():
    """
    Простой тест: отправить статическое сообщение.
    Удобно, чтобы проверять токен/чат без запуска пайплайна.
    """
    try:
        await send_telegram("Тестовое сообщение из Railway 🚀")
        return {"status": "sent"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    # ВАЖНО: никаких лишних параметров типа h11_max_incomplete_event_size
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
