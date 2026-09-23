from sqlalchemy import select, func

from app.database import async_session
from app.models.models import AirflowInstance, DAG, DAGRun, DagState, InstanceStatus
from app.services.airflow_client import AirflowClient
from tests.conftest import make_remote_dag, make_remote_run, patch_airflow


async def _load_instance(instance_id: int) -> AirflowInstance:
    async with async_session() as db:
        inst = await db.get(AirflowInstance, instance_id)
        assert inst is not None
        return inst


async def _sync(instance_id: int) -> dict:
    async with async_session() as db:
        inst = await db.get(AirflowInstance, instance_id)
        return await AirflowClient(inst).sync_instance(inst, db)


async def _count(model) -> int:
    async with async_session() as db:
        return (await db.execute(select(func.count(model.id)))).scalar()


async def _get_dags() -> list[DAG]:
    async with async_session() as db:
        return (await db.execute(select(DAG))).scalars().all()


class TestSyncInstance:
    async def test_first_sync_creates_dags_and_runs(self, instance, monkeypatch):
        patch_airflow(
            monkeypatch,
            version="2.10.1",
            dags=[make_remote_dag("etl_daily"), make_remote_dag("report")],
            runs_by_dag={
                "etl_daily": [
                    make_remote_run("s1", "failed"),
                    make_remote_run("s2", "success"),
                ],
                "report": [make_remote_run("r1", "success")],
            },
        )

        result = await _sync(instance.id)

        assert result["status"] == "success"
        assert result["version"] == "2.10.1"
        assert result["dags_synced"] == 2
        assert result["runs_synced"] == 3
        assert await _count(DAG) == 2
        assert await _count(DAGRun) == 3

        inst = await _load_instance(instance.id)
        assert inst.status == InstanceStatus.ACTIVE
        assert inst.airflow_version == "2.10.1"
        assert inst.last_sync is not None

    async def test_second_sync_is_idempotent(self, instance, monkeypatch):
        dags = [make_remote_dag("etl_daily")]
        runs = {"etl_daily": [make_remote_run("s1", "failed")]}

        patch_airflow(monkeypatch, dags=dags, runs_by_dag=runs)
        await _sync(instance.id)

        patch_airflow(monkeypatch, dags=dags, runs_by_dag=runs)
        result = await _sync(instance.id)

        assert result["status"] == "success"
        assert await _count(DAG) == 1
        assert await _count(DAGRun) == 1

    async def test_updates_existing_dag_fields(self, instance, monkeypatch):
        patch_airflow(monkeypatch, dags=[make_remote_dag("etl", is_paused=True, description="new")])
        await _sync(instance.id)

        patch_airflow(
            monkeypatch,
            dags=[
                make_remote_dag(
                    "etl",
                    is_paused=True,
                    description="new",
                    schedule_interval={"value": "@hourly"},
                )
            ],
        )
        await _sync(instance.id)

        dags = await _get_dags()
        assert len(dags) == 1
        assert dags[0].is_paused is True
        assert dags[0].description == "new"
        assert dags[0].schedule_interval == "@hourly"

    async def test_dag_removed_remote_marked_inactive_not_deleted(self, instance, monkeypatch):
        patch_airflow(monkeypatch, dags=[make_remote_dag("keep"), make_remote_dag("drop")])
        await _sync(instance.id)

        patch_airflow(monkeypatch, dags=[make_remote_dag("keep")])
        result = await _sync(instance.id)

        assert result["dags_synced"] == 1
        dags = {d.dag_id: d for d in await _get_dags()}
        assert dags["keep"].is_active is True
        assert dags["drop"].is_active is False
        assert set(dags) == {"keep", "drop"}

    async def test_failed_remote_sets_error_status(self, instance, monkeypatch):
        patch_airflow(monkeypatch, fail_dags=True)
        result = await _sync(instance.id)

        assert result["status"] == "error"
        assert "connection refused" in result["error"]
        inst = await _load_instance(instance.id)
        assert inst.status == InstanceStatus.ERROR

    async def test_run_state_transition_replaces_without_duplicate(self, instance, monkeypatch):
        patch_airflow(
            monkeypatch,
            dags=[make_remote_dag("etl")],
            runs_by_dag={"etl": [make_remote_run("s1", "running")]},
        )
        await _sync(instance.id)

        patch_airflow(
            monkeypatch,
            dags=[make_remote_dag("etl")],
            runs_by_dag={"etl": [make_remote_run("s1", "failed")]},
        )
        await _sync(instance.id)

        assert await _count(DAGRun) == 1
        async with async_session() as db:
            run = (await db.execute(select(DAGRun))).scalars().one()
            assert run.state == DagState.FAILED
