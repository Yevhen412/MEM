from fastapi import FastAPI, Query
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from .pipeline import run_once
from .config import TIMEZONE

app = FastAPI(title="Linda Filter")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/run_daily")
async def run_daily(force: bool = Query(False)):
    await run_once()
    return {"status": "started"}

def start_scheduler():
    tz = pytz.timezone(TIMEZONE)
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(run_once, CronTrigger(hour=9, minute=0))  # daily 09:00
    scheduler.start()

@app.on_event("startup")
async def on_startup():
    start_scheduler()
