import asyncio
from datetime import datetime

from sqlalchemy import select

from app.database import async_session
from app.models.models import AirflowInstance, DAG, DagState, InstanceStatus
from app.services.airflow_client import AirflowClient


async def sync_all_instances():
    async with async_session() as db:
        result = await db.execute(
            select(AirflowInstance).where(AirflowInstance.status != InstanceStatus.INACTIVE)
        )
        instances = result.scalars().all()

        results = []
        for instance in instances:
            client = AirflowClient(instance)
            sync_result = await client.sync_instance(instance, db)
            results.append(sync_result)

        return results


async def sync_single_instance(instance_id: int) -> dict:
    async with async_session() as db:
        result = await db.execute(
            select(AirflowInstance).where(AirflowInstance.id == instance_id)
        )
        instance = result.scalar_one_or_none()

        if not instance:
            return {"error": "Instance not found"}

        client = AirflowClient(instance)
        return await client.sync_instance(instance, db)
