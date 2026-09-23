import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.database import init_db
from app.routers import instances_router, dags_router, dashboard_router

logger = logging.getLogger("dagmanager")

scheduler = AsyncIOScheduler()

PUBLIC_PATHS = {
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/docs/oauth2-redirect",
}


def _is_authorized(request: Request) -> bool:
    token = settings.api_token
    if not token:
        return True
    auth = request.headers.get("Authorization", "")
    if auth == f"Bearer {token}":
        return True
    if request.headers.get("X-API-Key", "") == token:
        return True
    return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    await init_db()

    if not settings.api_token:
        logger.warning(
            "API_TOKEN não definido — API aberta. Defina API_TOKEN antes de expor a porta 8000."
        )
    if not settings.secret_key:
        logger.warning(
            "SECRET_KEY não definida — senhas de instância Airflow armazenadas sem criptografia."
        )

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


@app.middleware("http")
async def require_api_token(request: Request, call_next):
    path = request.url.path
    if path in PUBLIC_PATHS or path.startswith("/docs"):
        return await call_next(request)
    if not _is_authorized(request):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


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
