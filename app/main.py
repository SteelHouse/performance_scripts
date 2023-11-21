from dotenv import load_dotenv
load_dotenv(".env")    
from fastapi import FastAPI, BackgroundTasks
from prometheus_fastapi_instrumentator import Instrumentator
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger


import asyncio
import os
from .utils.logging_config import logger
from .core.flights import sync_flight_times
from .db.database import create_read_pool, create_write_pool


import uuid



CRON = os.environ.get("CRON")

app = FastAPI()

Instrumentator().instrument(app).expose(app)

scheduler = AsyncIOScheduler()




async def flight_sync_job():
    batch_id = uuid.uuid4()
    logger.info(f"Triggering Flight Time Sync Job | batch_id={batch_id} | {scheduler.get_job('flight_cron')}")
    await sync_flight_times(app.state.read_pool, app.state.write_pool, batch_id)

scheduler.add_job(flight_sync_job, CronTrigger.from_crontab(CRON), id="flight_cron")



@app.on_event("startup")
async def startup():
    logger.info("Starting scheduler")
    scheduler.start()
    for job in scheduler.get_jobs():
        logger.info(job)
    logger.info("Initializing db pools")
    
    app.state.read_pool = await create_read_pool()
    app.state.write_pool = await create_write_pool()
    logger.info("Initialized db pools")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down scheduler")
    scheduler.shutdown()
    logger.info("Closing db pools")
    await app.state.read_pool.close()
    await app.state.write_pool.close()


@app.get("/health")
def health():
    return {"status": "Success"}


@app.get("/env")
def health():
    return dict(os.environ)

@app.get("/sync-flight-times")
async def run_task(background_tasks: BackgroundTasks):
    batch_id = uuid.uuid4()
    background_tasks.add_task(sync_flight_times, app.state.read_pool, app.state.write_pool, batch_id)
    logger.info(f"Triggering Flight Time Sync | batch_id={batch_id}")
    return {"message": "Triggered Flight Time Sync", "batch_id": batch_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
