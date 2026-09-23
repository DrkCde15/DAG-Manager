from tests.conftest import run_async, seed_dashboard_data


class TestDashboardSummary:
    def test_empty_returns_zeros(self, client):
        resp = client.get("/dashboard/summary")
        assert resp.status_code == 200
        assert resp.json() == {
            "total_instances": 0,
            "active_instances": 0,
            "total_dags": 0,
            "active_dags": 0,
            "paused_dags": 0,
            "runs_today": 0,
            "failed_runs": 0,
            "running_runs": 0,
        }

    def test_counts_after_seed(self, client):
        run_async(seed_dashboard_data())
        body = client.get("/dashboard/summary").json()
        assert body["total_instances"] == 1
        assert body["active_instances"] == 1
        assert body["total_dags"] == 2
        assert body["active_dags"] == 2
        assert body["paused_dags"] == 1
        assert body["runs_today"] >= 1
        assert body["failed_runs"] == 1
        assert body["running_runs"] == 0


class TestDashboardHealth:
    def test_health_recent_failures(self, client):
        run_async(seed_dashboard_data(failed_age_hours=2))
        body = client.get("/dashboard/health").json()
        assert len(body) == 1
        assert body[0]["instance_name"] == "local"
        assert body[0]["dag_count"] == 2
        assert body[0]["recent_failures"] == 1
        assert body[0]["last_sync"] is not None

    def test_failure_outside_24h_not_counted(self, client):
        run_async(seed_dashboard_data(failed_age_hours=48, runs_today=False))
        body = client.get("/dashboard/health").json()
        assert body[0]["recent_failures"] == 0
