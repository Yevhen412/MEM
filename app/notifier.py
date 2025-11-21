# app/notifier.py

import os
from typing import Any, Dict, List

import httpx

TELEGRAM_API_BASE = "https://api.telegram.org"


async def send_telegram(message: str) -> None:
    """
    Отправка текста в Telegram.
    Берём TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID из переменных окружения.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("[Telegram] TOKEN or CHAT_ID не заданы, пропускаю отправку")
        return

    url = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload)

        if resp.status_code != 200:
            print(
                f"[Telegram] send failed: {resp.status_code} "
                f"{resp.text[:200]}"
            )
    except Exception as e:
        print(f"[Telegram] exception on send: {e}")


def format_telegram_message(result: Dict[str, Any]) -> str:
    """
    Формируем короткий отчёт для Телеграма.

    Ожидаем:
      - result["collected"] — сколько всего токенов за сутки
      - result["serious_tokens"] — список отобранных проектов
        (каждый: {name, symbol, link})
    Если serious_tokens нет — считаем, что серьёзных нет.
    """
    total = int(result.get("collected", 0))
    serious_tokens: List[Dict[str, Any]] = result.get("serious_tokens") or []
    serious_count = len(serious_tokens)

    lines: List[str] = []

    lines.append("🧾 *Новые токены за прошедшие сутки*")
    lines.append(f"Всего найдено: *{total}*")
    lines.append(f"Серьёзных проектов: *{serious_count}*")
    lines.append("")

    if not serious_tokens:
        lines.append("_Сегодня серьёзных проектов не найдено._")
        return "\n".join(lines)

    lines.append("🟢 *Список серьёзных проектов:*")
    lines.append("")

    for i, t in enumerate(serious_tokens, 1):
        name = t.get("name") or "без имени"
        symbol = t.get("symbol") or ""
        link = t.get("link") or ""

        header = f"{i}. *{name}* ({symbol})" if symbol else f"{i}. *{name}*"
        lines.append(header)
        if link:
            lines.append(link)
        lines.append("")  # пустая строка между проектами

    return "\n".join(lines)
