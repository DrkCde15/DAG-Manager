import asyncio
import os
import tempfile
from datetime import datetime, timedelta

_TEST_DIR = tempfile.mkdtemp(prefix="dagmanager-tests-")
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_TEST_DIR}/test.db"
os.environ["RUNS_SYNC_LIMIT"] = "5"
os.environ["SYNC_INTERVAL_MINUTES"] = "999"

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, func

from app.database import engine, Base, init_db
from app.main import app
from app.models.models import AirflowInstance, DAG, DAGRun, DagState, InstanceStatus
from app.services.airflow_client import AirflowClient


def run_async(coro):
    """Run a coroutine to completion from sync tests."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_async called while an event loop is running")


@pytest.fixture(autouse=True)
async def clean_db():
    await init_db()
    async with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    yield
    async with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
async def instance():
    from app.database import async_session

    async with async_session() as db:
        inst = AirflowInstance(name="local", url="http://airflow.test:8080")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        return inst


def make_remote_dag(dag_id: str, **overrides) -> dict:
    data = {
        "dag_id": dag_id,
        "is_active": True,
        "is_paused": False,
        "schedule_interval": {"value": "@daily"},
        "description": f"desc {dag_id}",
        "last_parsed_time": "2026-09-23T00:00:00+00:00",
    }
    data.update(overrides)
    return data


def make_remote_run(run_id: str, state: str = "success", execution_date: str | None = None) -> dict:
    return {
        "dag_run_id": run_id,
        "state": state,
        "execution_date": execution_date or datetime.utcnow().isoformat(),
        "start_date": datetime.utcnow().isoformat(),
        "end_date": datetime.utcnow().isoformat(),
        "run_type": "scheduled",
    }


def patch_airflow(monkeypatch, *, version="2.10.0", dags=None, runs_by_dag=None, fail_dags=False):
    dags = dags if dags is not None else []
    runs_by_dag = runs_by_dag if runs_by_dag is not None else {}

    async def fake_version(self):
        if fail_dags:
            raise RuntimeError("connection refused")
        return version

    async def fake_dags(self):
        if fail_dags:
            raise RuntimeError("connection refused")
        return list(dags)

    async def fake_runs(self, dag_id, limit=10):
        if fail_dags:
            raise RuntimeError("connection refused")
        return list(runs_by_dag.get(dag_id, []))

    async def fake_close(self):
        self._client = None

    monkeypatch.setattr(AirflowClient, "get_version", fake_version)
    monkeypatch.setattr(AirflowClient, "get_dags", fake_dags)
    monkeypatch.setattr(AirflowClient, "get_dag_runs", fake_runs)
    monkeypatch.setattr(AirflowClient, "close", fake_close)


async def seed_dashboard_data(instance_name="local", *, failed_age_hours=1, runs_today=True):
    from app.database import async_session

    async with async_session() as db:
        inst = AirflowInstance(
            name=instance_name,
            url="http://airflow.test:8080",
            status=InstanceStatus.ACTIVE,
            last_sync=datetime.utcnow(),
        )
        db.add(inst)
        await db.flush()

        dag = DAG(dag_id="etl", instance_id=inst.id, is_active=True, is_paused=False)
        paused = DAG(dag_id="paused_dag", instance_id=inst.id, is_active=True, is_paused=True)
        db.add_all([dag, paused])
        await db.flush()

        now = datetime.utcnow()
        db.add(
            DAGRun(
                dag_id=dag.id,
                run_id="failed_recent",
                state=DagState.FAILED,
                execution_date=now - timedelta(hours=failed_age_hours),
            )
        )
        if runs_today:
            db.add(
                DAGRun(
                    dag_id=dag.id,
                    run_id="ok_today",
                    state=DagState.SUCCESS,
                    execution_date=now.replace(hour=1, minute=0, second=0, microsecond=0),
                )
            )
        db.add(
            DAGRun(
                dag_id=dag.id,
                run_id="old_success",
                state=DagState.SUCCESS,
                execution_date=now - timedelta(days=3),
            )
        )
        await db.commit()
        return inst.id
