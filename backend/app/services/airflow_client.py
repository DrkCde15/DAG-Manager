import httpx
import re
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import settings
from app.models.models import AirflowInstance, DAG, DAGRun, DagState, InstanceStatus


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _map_run_state(raw: str | None) -> DagState | None:
    if not raw:
        return None
    try:
        return DagState(raw)
    except ValueError:
        if raw in ("waiting_for_slot", "deferred", "up_for_reschedule"):
            return DagState.QUEUED
        return None


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

    async def trigger_dag(self, dag_id: str, conf: dict | None = None) -> dict:
        client = await self._get_client()
        payload = {"conf": conf or {}}
        resp = await client.post(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    async def get_dag_logs(
        self, dag_id: str, run_id: str, task_id: str | None = None, try_number: int = 1
    ) -> str:
        client = await self._get_client()

        if task_id:
            resp = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/tasks/{task_id}/logs/{run_id}/{try_number}",
            )
        else:
            resp = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            )
            if resp.status_code == 200:
                tasks = resp.json().get("task_instances", [])
                logs = []
                for t in tasks:
                    log_resp = await client.get(
                        f"{self.base_url}/api/v1/dags/{dag_id}/tasks/{t['task_id']}/logs/{run_id}/{try_number}",
                    )
                    if log_resp.status_code == 200:
                        logs.append(f"=== {t['task_id']} ===\n{log_resp.text}")
                return "\n\n".join(logs) if logs else "No logs found"
            return resp.text

        if resp.status_code == 200:
            return resp.text
        return f"Error fetching logs: {resp.status_code}"

    async def get_task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        )
        resp.raise_for_status()
        return resp.json().get("task_instances", [])

    async def set_dag_paused(self, dag_id: str, is_paused: bool) -> dict:
        client = await self._get_client()
        resp = await client.patch(
            f"{self.base_url}/api/v1/dags/{dag_id}",
            json={"is_paused": is_paused},
        )
        resp.raise_for_status()
        return resp.json()

    async def _upsert_runs(self, dag: DAG, raw_runs: list[dict], db: AsyncSession) -> int:
        if not raw_runs:
            return 0

        run_keys = [
            r.get("dag_run_id") or r.get("run_id")
            for r in raw_runs
            if r.get("dag_run_id") or r.get("run_id")
        ]
        if not run_keys:
            return 0

        result = await db.execute(
            select(DAGRun).where(DAGRun.dag_id == dag.id, DAGRun.run_id.in_(run_keys))
        )
        existing_map = {r.run_id: r for r in result.scalars().all()}

        synced = 0
        for raw in raw_runs:
            run_id = raw.get("dag_run_id") or raw.get("run_id")
            if not run_id:
                continue
            state = _map_run_state(raw.get("state"))
            execution_date = _parse_dt(raw.get("execution_date"))
            if state is None or execution_date is None:
                continue

            start_date = _parse_dt(raw.get("start_date"))
            end_date = _parse_dt(raw.get("end_date"))
            run_type = raw.get("run_type")

            if run_id in existing_map:
                row = existing_map[run_id]
                row.state = state
                row.execution_date = execution_date
                row.start_date = start_date
                row.end_date = end_date
                row.run_type = run_type
            else:
                db.add(
                    DAGRun(
                        dag_id=dag.id,
                        run_id=run_id,
                        state=state,
                        execution_date=execution_date,
                        start_date=start_date,
                        end_date=end_date,
                        run_type=run_type,
                    )
                )
            synced += 1
        return synced

    async def _sync_runs(self, instance_id: int, remote_dag_ids: set[str], db: AsyncSession) -> int:
        result = await db.execute(
            select(DAG).where(DAG.instance_id == instance_id, DAG.is_active == True)
        )
        local_active = {d.dag_id: d for d in result.scalars().all()}

        runs_synced = 0
        for dag_key, local_dag in local_active.items():
            if dag_key not in remote_dag_ids:
                continue
            try:
                raw_runs = await self.get_dag_runs(
                    dag_key, limit=settings.runs_sync_limit
                )
            except Exception:
                continue
            runs_synced += await self._upsert_runs(local_dag, raw_runs, db)
        return runs_synced

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

            await db.flush()
            runs_synced = await self._sync_runs(instance.id, remote_dag_ids, db)

            await db.commit()
            instance.last_sync = datetime.utcnow()
            await db.commit()

            return {
                "instance": instance.name,
                "version": version,
                "dags_synced": synced_count,
                "runs_synced": runs_synced,
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
