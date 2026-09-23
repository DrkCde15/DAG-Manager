from datetime import datetime

import pytest
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError

from app.database import async_session
from app.models.models import DAG, DAGRun, DagState
from app.services.airflow_client import AirflowClient, _map_run_state, _parse_dt
from tests.conftest import make_remote_run


class TestParseDt:
    def test_none(self):
        assert _parse_dt(None) is None

    def test_empty(self):
        assert _parse_dt("") is None

    def test_invalid(self):
        assert _parse_dt("not-a-date") is None

    def test_naive_kept(self):
        dt = _parse_dt("2026-09-23T12:00:00")
        assert dt == datetime(2026, 9, 23, 12, 0, 0)
        assert dt.tzinfo is None

    def test_utc_offset_converted_to_naive_utc(self):
        dt = _parse_dt("2026-09-23T12:00:00+03:00")
        assert dt == datetime(2026, 9, 23, 9, 0, 0)
        assert dt.tzinfo is None

    def test_z_suffix(self):
        dt = _parse_dt("2026-09-23T12:00:00Z")
        assert dt == datetime(2026, 9, 23, 12, 0, 0)


class TestMapRunState:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("success", DagState.SUCCESS),
            ("failed", DagState.FAILED),
            ("running", DagState.RUNNING),
            ("queued", DagState.QUEUED),
            ("up_for_retry", DagState.UP_FOR_RETRY),
            ("upstream_failed", DagState.UPSTREAM_FAILED),
            ("waiting_for_slot", DagState.QUEUED),
            ("deferred", DagState.QUEUED),
            ("up_for_reschedule", DagState.QUEUED),
            ("mystery", None),
            (None, None),
            ("", None),
        ],
    )
    def test_mapping(self, raw, expected):
        assert _map_run_state(raw) == expected


class TestUpsertRuns:
    async def _dag(self, instance):
        async with async_session() as db:
            inst = await db.get(type(instance), instance.id)
            dag = DAG(dag_id="dag_a", instance_id=inst.id, is_active=True)
            db.add(dag)
            await db.commit()
            await db.refresh(dag)
            return dag.id

    async def test_inserts_valid_runs_and_skips_invalid(self, instance):
        dag_id = await self._dag(instance)
        async with async_session() as db:
            dag = await db.get(DAG, dag_id)
            client = AirflowClient(instance)
            raw = [
                make_remote_run("run_1", "failed"),
                make_remote_run("run_2", "success"),
                {
                    "dag_run_id": "run_3",
                    "state": "mystery",
                    "execution_date": "2026-09-21T00:00:00+00:00",
                },
                {"dag_run_id": "run_4", "state": "success", "execution_date": "bad-date"},
                {"state": "success", "execution_date": "2026-09-21T00:00:00+00:00"},
            ]
            synced = await client._upsert_runs(dag, raw, db)
            await db.commit()
            assert synced == 2

        async with async_session() as db:
            runs = (await db.execute(select(DAGRun))).scalars().all()
            assert {r.run_id for r in runs} == {"run_1", "run_2"}

    async def test_reupsert_is_idempotent_and_updates_state(self, instance):
        dag_id = await self._dag(instance)
        async with async_session() as db:
            dag = await db.get(DAG, dag_id)
            client = AirflowClient(instance)
            raw = [make_remote_run("run_1", "failed")]
            assert await client._upsert_runs(dag, raw, db) == 1
            await db.commit()

            raw[0]["state"] = "success"
            assert await client._upsert_runs(dag, raw, db) == 1
            await db.commit()

        async with async_session() as db:
            total = (await db.execute(select(func.count(DAGRun.id)))).scalar()
            assert total == 1
            state = (await db.execute(select(DAGRun.state))).scalar_one()
            assert state == DagState.SUCCESS

    async def test_empty_list_returns_zero(self, instance):
        dag_id = await self._dag(instance)
        async with async_session() as db:
            dag = await db.get(DAG, dag_id)
            client = AirflowClient(instance)
            assert await client._upsert_runs(dag, [], db) == 0

    async def test_unique_constraint_on_dag_id_run_id(self, instance):
        dag_id = await self._dag(instance)
        async with async_session() as db:
            db.add(
                DAGRun(
                    dag_id=dag_id,
                    run_id="dup",
                    state=DagState.SUCCESS,
                    execution_date=datetime(2026, 9, 23),
                )
            )
            await db.commit()
            db.add(
                DAGRun(
                    dag_id=dag_id,
                    run_id="dup",
                    state=DagState.FAILED,
                    execution_date=datetime(2026, 9, 24),
                )
            )
            with pytest.raises(IntegrityError):
                await db.commit()
            await db.rollback()


class TestSyncRuns:
    async def test_syncs_runs_only_for_active_remote_dags(self, instance, monkeypatch):
        async with async_session() as db:
            active = DAG(dag_id="active_dag", instance_id=instance.id, is_active=True)
            inactive = DAG(dag_id="inactive_dag", instance_id=instance.id, is_active=False)
            missing = DAG(dag_id="gone_dag", instance_id=instance.id, is_active=True)
            db.add_all([active, inactive, missing])
            await db.commit()
            active_id = active.id

        client = AirflowClient(instance)
        calls = []

        async def fake_runs(self, dag_id, limit=10):
            calls.append(dag_id)
            return [make_remote_run(f"{dag_id}_r1", "failed")]

        monkeypatch.setattr(AirflowClient, "get_dag_runs", fake_runs)

        async with async_session() as db:
            total = await client._sync_runs(instance.id, {"active_dag", "not_local"}, db)
            await db.commit()

        assert calls == ["active_dag"]
        assert total == 1
        async with async_session() as db:
            runs = (await db.execute(select(DAGRun))).scalars().all()
            assert len(runs) == 1
            assert runs[0].run_id == "active_dag_r1"
            assert runs[0].dag_id == active_id

    async def test_run_fetch_error_skips_dag_without_failing_sync(self, instance, monkeypatch):
        async with async_session() as db:
            dag = DAG(dag_id="dag_a", instance_id=instance.id, is_active=True)
            db.add(dag)
            await db.commit()

        client = AirflowClient(instance)

        async def boom(self, dag_id, limit=10):
            raise RuntimeError("500")

        monkeypatch.setattr(AirflowClient, "get_dag_runs", boom)

        async with async_session() as db:
            total = await client._sync_runs(instance.id, {"dag_a"}, db)
            await db.commit()

        assert total == 0
