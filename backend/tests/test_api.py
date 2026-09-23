from tests.conftest import run_async, seed_dashboard_data


class TestRoutes:
    def test_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert resp.json()["docs"] == "/docs"

    def test_stats_summary_removed(self, client):
        assert client.get("/dags/stats/summary").status_code == 404

    def test_dashboard_routes_present(self, client):
        assert client.get("/dashboard/summary").status_code == 200
        assert client.get("/dashboard/health").status_code == 200

    def test_openapi_paths(self, client):
        paths = client.get("/openapi.json").json()["paths"]
        assert "/dags/stats/summary" not in paths
        assert "/dashboard/summary" in paths
        assert "/dags/{dag_id}/runs" in paths
        assert "/instances/" in paths


class TestInstanceAPI:
    def test_create_and_list_hides_password(self, client):
        resp = client.post(
            "/instances/",
            json={"name": "prod", "url": "http://airflow.test:8080", "password": "secret"},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == "prod"
        assert "password" not in body

        listed = client.get("/instances/").json()
        assert len(listed) == 1
        assert "password" not in listed[0]

    def test_duplicate_name_rejected(self, client):
        client.post("/instances/", json={"name": "prod", "url": "http://a"})
        resp = client.post("/instances/", json={"name": "prod", "url": "http://b"})
        assert resp.status_code == 400

    def test_get_missing_404(self, client):
        assert client.get("/instances/999").status_code == 404

    def test_delete(self, client):
        created = client.post("/instances/", json={"name": "tmp", "url": "http://a"}).json()
        assert client.delete(f"/instances/{created['id']}").status_code == 204
        assert client.get(f"/instances/{created['id']}").status_code == 404


class TestDagsAPI:
    def test_list_and_runs_after_seed(self, client):
        run_async(seed_dashboard_data())
        dags = client.get("/dags/").json()
        assert len(dags) == 2
        assert {d["dag_id"] for d in dags} == {"etl", "paused_dag"}
        assert all("instance_name" in d for d in dags)

        etl = next(d for d in dags if d["dag_id"] == "etl")
        runs = client.get(f"/dags/{etl['id']}/runs").json()
        assert len(runs) == 3
        assert runs[0]["execution_date"] >= runs[-1]["execution_date"]
        assert any(r["state"] == "failed" for r in runs)

    def test_search_filter(self, client):
        run_async(seed_dashboard_data())
        resp = client.get("/dags/", params={"search": "etl"})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_missing_dag_runs_404(self, client):
        assert client.get("/dags/999/runs").status_code == 404

    def test_trigger_forwards_conf(self, client, monkeypatch):
        from app.services.airflow_client import AirflowClient

        run_async(seed_dashboard_data())
        dags = client.get("/dags/").json()
        etl = next(d for d in dags if d["dag_id"] == "etl")
        captured = {}

        async def fake_trigger(self, dag_id, conf=None):
            captured["dag_id"] = dag_id
            captured["conf"] = conf
            return {"dag_run_id": "manual__1"}

        async def fake_pause(self, dag_id, is_paused):
            return {"is_paused": is_paused}

        async def fake_close(self):
            self._client = None

        monkeypatch.setattr(AirflowClient, "trigger_dag", fake_trigger)
        monkeypatch.setattr(AirflowClient, "set_dag_paused", fake_pause)
        monkeypatch.setattr(AirflowClient, "close", fake_close)

        conf = {"date": "2026-09-23", "full_refresh": True}
        resp = client.post(f"/dags/{etl['id']}/trigger", json={"conf": conf})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "success"
        assert body["run_id"] == "manual__1"
        assert captured["dag_id"] == "etl"
        assert captured["conf"] == conf

    def test_trigger_without_conf_sends_empty(self, client, monkeypatch):
        from app.services.airflow_client import AirflowClient

        run_async(seed_dashboard_data())
        dags = client.get("/dags/").json()
        etl = next(d for d in dags if d["dag_id"] == "etl")
        captured = {}

        async def fake_trigger(self, dag_id, conf=None):
            captured["conf"] = conf
            return {"dag_run_id": "manual__2"}

        async def fake_close(self):
            self._client = None

        monkeypatch.setattr(AirflowClient, "trigger_dag", fake_trigger)
        monkeypatch.setattr(AirflowClient, "close", fake_close)

        resp = client.post(f"/dags/{etl['id']}/trigger", json={})
        assert resp.status_code == 200
        assert captured["conf"] in (None, {})
