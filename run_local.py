# Simple local runner for testing (requires .env with DATABASE_URL)
from app.pipeline import run_once
import asyncio
asyncio.run(run_once())
