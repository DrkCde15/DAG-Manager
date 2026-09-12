from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.database import get_db
from app.models.models import DAG, DAGRun, AirflowInstance, DagState
from app.schemas.schemas import DAGResponse, DAGRunResponse

router = APIRouter(prefix="/dags", tags=["dags"])


@router.get("/", response_model=list[DAGResponse])
async def list_dags(
    instance_id: int | None = None,
    is_paused: bool | None = None,
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(DAG).join(AirflowInstance)

    if instance_id:
        query = query.where(DAG.instance_id == instance_id)
    if is_paused is not None:
        query = query.where(DAG.is_paused == is_paused)
    if search:
        query = query.where(DAG.dag_id.ilike(f"%{search}%"))

    result = await db.execute(query)
    dags = result.scalars().all()

    response = []
    for dag in dags:
        instance_result = await db.execute(
            select(AirflowInstance).where(AirflowInstance.id == dag.instance_id)
        )
        instance = instance_result.scalar_one()
        response.append(
            DAGResponse(
                id=dag.id,
                dag_id=dag.dag_id,
                instance_id=dag.instance_id,
                instance_name=instance.name,
                description=dag.description,
                is_active=dag.is_active,
                is_paused=dag.is_paused,
                schedule_interval=dag.schedule_interval,
                last_parsed_time=dag.last_parsed_time,
            )
        )
    return response


@router.get("/{dag_id}/runs", response_model=list[DAGRunResponse])
async def get_dag_runs(
    dag_id: int,
    limit: int = Query(default=20, le=100),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(DAG).where(DAG.id == dag_id))
    dag = result.scalar_one_or_none()
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")

    runs_result = await db.execute(
        select(DAGRun)
        .where(DAGRun.dag_id == dag_id)
        .order_by(DAGRun.execution_date.desc())
        .limit(limit)
    )
    return runs_result.scalars().all()


@router.get("/stats/summary")
async def dag_stats(db: AsyncSession = Depends(get_db)):
    total_result = await db.execute(select(func.count(DAG.id)))
    total = total_result.scalar()

    active_result = await db.execute(select(func.count(DAG.id)).where(DAG.is_active == True))
    active = active_result.scalar()

    paused_result = await db.execute(select(func.count(DAG.id)).where(DAG.is_paused == True))
    paused = paused_result.scalar()

    from datetime import datetime
    today_result = await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.execution_date >= func.current_date())
    )
    today_runs = today_result.scalar()

    failed_result = await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.state == DagState.FAILED)
    )
    failed_runs = failed_result.scalar()

    running_result = await db.execute(
        select(func.count(DAGRun.id)).where(DAGRun.state == DagState.RUNNING)
    )
    running_runs = running_result.scalar()

    return {
        "total_dags": total or 0,
        "active_dags": active or 0,
        "paused_dags": paused or 0,
        "runs_today": today_runs or 0,
        "failed_runs": failed_runs or 0,
        "running_runs": running_runs or 0,
    }
