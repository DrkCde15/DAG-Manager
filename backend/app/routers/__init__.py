from app.routers.instances import router as instances_router
from app.routers.dags import router as dags_router
from app.routers.dashboard import router as dashboard_router

__all__ = ["instances_router", "dags_router", "dashboard_router"]
