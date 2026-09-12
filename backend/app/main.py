import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.database import init_db
from app.routers import instances_router, dags_router, dashboard_router

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    from app.services.sync_service import sync_all_instances

    scheduler.add_job(
        sync_all_instances,
        "interval",
        minutes=settings.sync_interval_minutes,
        id="sync_all",
    )
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    lifespan=lifespan,
)

app.include_router(instances_router)
app.include_router(dags_router)
app.include_router(dashboard_router)


@app.get("/")
def root():
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "docs": "/docs",
    }
