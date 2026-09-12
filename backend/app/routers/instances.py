from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.models import AirflowInstance, InstanceStatus
from app.schemas.schemas import InstanceCreate, InstanceResponse, InstanceUpdate
from app.services.sync_service import sync_single_instance

router = APIRouter(prefix="/instances", tags=["instances"])


@router.get("/", response_model=list[InstanceResponse])
async def list_instances(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance))
    return result.scalars().all()


@router.get("/{instance_id}", response_model=InstanceResponse)
async def get_instance(instance_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.id == instance_id))
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    return instance


@router.post("/", response_model=InstanceResponse, status_code=201)
async def create_instance(instance_data: InstanceCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.name == instance_data.name))
    existing = result.scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=400, detail="Instance name already exists")

    instance = AirflowInstance(**instance_data.model_dump())
    db.add(instance)
    await db.commit()
    await db.refresh(instance)
    return instance


@router.put("/{instance_id}", response_model=InstanceResponse)
async def update_instance(
    instance_id: int,
    instance_data: InstanceUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.id == instance_id))
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")

    update_data = instance_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(instance, field, value)

    await db.commit()
    await db.refresh(instance)
    return instance


@router.delete("/{instance_id}", status_code=204)
async def delete_instance(instance_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.id == instance_id))
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")

    await db.delete(instance)
    await db.commit()


@router.post("/{instance_id}/sync")
async def sync_instance(instance_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.id == instance_id))
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")

    sync_result = await sync_single_instance(instance_id)
    return sync_result


@router.post("/sync-all")
async def sync_all():
    from app.services.sync_service import sync_all_instances
    results = await sync_all_instances()
    return {"results": results}


@router.get("/{instance_id}/health")
async def instance_health(instance_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AirflowInstance).where(AirflowInstance.id == instance_id))
    instance = result.scalar_one_or_none()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")

    from app.services.airflow_client import AirflowClient
    client = AirflowClient(instance)
    try:
        health = await client.health_check()
        return {"instance_id": instance_id, "health": health, "status": "ok"}
    except Exception as e:
        return {"instance_id": instance_id, "status": "error", "error": str(e)}
