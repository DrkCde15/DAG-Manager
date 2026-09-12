from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timedelta

from app.database import get_db
from app.models.models import AirflowInstance, DAG, DAGRun, InstanceStatus, DagState
from app.schemas.schemas import DashboardSummary, InstanceHealth

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummary)
async def get_summary(db: AsyncSession = Depends(get_db)):
    total_instances = (await db.execute(select(func.count(AirflowInstance.id)))).scalar()
    active_instances = (await db.execute(
        select(func.count(AirflowInstance.id)).where(AirflowInstance.status == InstanceStatus.ACTIVE)
    )).scalar()

    total_dags = (await db.execute(select(func.count(DAG.id)))).scalar()
    active_dags = (await db.execute(select(func.count(DAG.id)).where(DAG.is_active == True))).scalar()
    paused_dags = (await db.execute(select(func.count(DAG.id)).where(DAG.is_paused == True))).scalar()

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    runs_today = (await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.execution_date >= today)
    )).scalar()

    failed_runs = (await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.state == DagState.FAILED)
    )).scalar()

    running_runs = (await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.state == DagState.RUNNING)
    )).scalar()

    return DashboardSummary(
        total_instances=total_instances or 0,
        active_instances=active_instances or 0,
        total_dags=total_dags or 0,
        active_dags=active_dags or 0,
        paused_dags=paused_dags or 0,
        runs_today=runs_today or 0,
        failed_runs=failed_runs or 0,
        running_runs=running_runs or 0,
    )


@router.get("/health", response_model=list[InstanceHealth])
async def get_instances_health(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance))
    instances = result.scalars().all()
    health_list = []

    for instance in instances:
        dag_count = (await db.execute(
            select(func.count(DAG.id)).where(DAG.instance_id == instance.id)
        )).scalar()

        recent_failures = (await db.execute(
            select(func.count(DAGRun.id))
            .join(DAG)
            .where(
                DAG.instance_id == instance.id,
                DAGRun.state == DagState.FAILED,
                DAGRun.execution_date >= datetime.utcnow() - timedelta(hours=24),
            )
        )).scalar()

        health_list.append(
            InstanceHealth(
                instance_id=instance.id,
                instance_name=instance.name,
                status=instance.status.value,
                dag_count=dag_count or 0,
                last_sync=instance.last_sync,
                recent_failures=recent_failures or 0,
            )
        )

    return health_list
