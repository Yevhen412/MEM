# Linda — Strict Token Filter (Railway + GitHub)

Готовый каркас проекта для автоматического отбора серьёзных токенов.
Python, deploy на Railway, с включённым автоудалением непройденных токенов и Telegram-уведомлениями.

Файлы:
- app/ — исходники сервиса (FastAPI, fetchers, pipeline, filter, db, notifier)
- requirements.txt, Procfile, .env.example
- В базе PostgreSQL сохраняются `tokens_raw`, `tokens_filtered`. Непрошедшие автоматически удаляются.

Сборка: просто залей в GitHub → Deploy to Railway → прописать переменные окружения из `.env.example`.
