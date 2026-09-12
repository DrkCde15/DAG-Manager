import httpx
import re
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.models import AirflowInstance, DAG, DagState, InstanceStatus


class AirflowClient:
    def __init__(self, instance: AirflowInstance):
        self.base_url = instance.url.rstrip("/")
        self.username = instance.username
        self.password = instance.password
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client

        self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

        if self.username and self.password:
            login_page = await self._client.get(f"{self.base_url}/login/")
            csrf_match = re.search(
                r'name="csrf_token".*?value="([^"]+)"', login_page.text
            )
            if csrf_match:
                csrf_token = csrf_match.group(1)
                await self._client.post(
                    f"{self.base_url}/login/",
                    data={
                        "username": self.username,
                        "password": self.password,
                        "csrf_token": csrf_token,
                    },
                )

        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{self.base_url}/api/v1/health")
            resp.raise_for_status()
            return resp.json()

    async def get_version(self) -> str:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{self.base_url}/api/v1/version")
            resp.raise_for_status()
            return resp.json().get("version", "unknown")

    async def get_dags(self) -> list[dict]:
        client = await self._get_client()
        dags = []
        offset = 0
        while True:
            resp = await client.get(
                f"{self.base_url}/api/v1/dags",
                params={"offset": offset, "limit": 100},
            )
            resp.raise_for_status()
            data = resp.json()
            dags.extend(data.get("dags", []))
            if len(data.get("dags", [])) < 100:
                break
            offset += 100
        return dags

    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            params={"limit": limit, "order_by": "-execution_date"},
        )
        resp.raise_for_status()
        return resp.json().get("dag_runs", [])

    async def sync_instance(self, instance: AirflowInstance, db: AsyncSession) -> dict:
        try:
            version = await self.get_version()
            instance.airflow_version = version
            instance.status = InstanceStatus.ACTIVE

            remote_dags = await self.get_dags()
            remote_dag_ids = {d["dag_id"] for d in remote_dags}

            result = await db.execute(
                select(DAG).where(DAG.instance_id == instance.id)
            )
            existing_dags = result.scalars().all()
            existing_dag_map = {d.dag_id: d for d in existing_dags}

            synced_count = 0
            for remote_dag in remote_dags:
                dag_id = remote_dag["dag_id"]
                schedule = remote_dag.get("schedule_interval")
                if isinstance(schedule, dict):
                    schedule = schedule.get("value", "")

                if dag_id in existing_dag_map:
                    dag = existing_dag_map[dag_id]
                    dag.is_active = remote_dag.get("is_active", True)
                    dag.is_paused = remote_dag.get("is_paused", False)
                    dag.schedule_interval = schedule
                    dag.description = remote_dag.get("description")
                    dag.last_parsed_time = (
                        datetime.fromisoformat(remote_dag["last_parsed_time"])
                        if remote_dag.get("last_parsed_time")
                        else None
                    )
                else:
                    dag = DAG(
                        dag_id=dag_id,
                        instance_id=instance.id,
                        is_active=remote_dag.get("is_active", True),
                        is_paused=remote_dag.get("is_paused", False),
                        schedule_interval=schedule,
                        description=remote_dag.get("description"),
                        last_parsed_time=(
                            datetime.fromisoformat(remote_dag["last_parsed_time"])
                            if remote_dag.get("last_parsed_time")
                            else None
                        ),
                    )
                    db.add(dag)

                synced_count += 1

            for dag_id, dag in existing_dag_map.items():
                if dag_id not in remote_dag_ids:
                    dag.is_active = False

            await db.commit()
            instance.last_sync = datetime.utcnow()
            await db.commit()

            return {
                "instance": instance.name,
                "version": version,
                "dags_synced": synced_count,
                "status": "success",
            }
        except Exception as e:
            instance.status = InstanceStatus.ERROR
            await db.commit()
            return {
                "instance": instance.name,
                "status": "error",
                "error": str(e),
            }
        finally:
            await self.close()
