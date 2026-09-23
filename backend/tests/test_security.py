from app.config import settings
from app.main import app, _is_authorized, PUBLIC_PATHS
from app.security import encrypt_secret, decrypt_secret
from app.models.models import AirflowInstance
from tests.conftest import run_async, seed_dashboard_data


class TestApiTokenAuth:
    def test_open_when_no_token_configured(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", None)
        assert client.get("/dashboard/summary").status_code == 200

    def test_401_without_token_when_configured(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        resp = client.get("/dashboard/summary")
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"

    def test_201_with_bearer_token(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        resp = client.get(
            "/dashboard/summary",
            headers={"Authorization": "Bearer sekret"},
        )
        assert resp.status_code == 200

    def test_200_with_x_api_key(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        resp = client.get("/dashboard/summary", headers={"X-API-Key": "sekret"})
        assert resp.status_code == 200

    def test_wrong_token_rejected(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        resp = client.get(
            "/dashboard/summary",
            headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401

    def test_trigger_requires_token(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        run_async(seed_dashboard_data())
        dags = client.get(
            "/dags/", headers={"Authorization": "Bearer sekret"}
        ).json()
        dag_id = dags[0]["id"]
        unauth = client.post(f"/dags/{dag_id}/trigger")
        assert unauth.status_code == 401

    def test_public_paths_stay_open(self, client, monkeypatch):
        monkeypatch.setattr(settings, "api_token", "sekret")
        assert client.get("/").status_code == 200
        assert client.get("/docs").status_code == 200
        assert client.get("/openapi.json").status_code == 200

    def test_is_authorized_helper(self, monkeypatch):
        from fastapi import Request

        monkeypatch.setattr(settings, "api_token", "abc")
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/x",
            "headers": [(b"authorization", b"Bearer abc")],
            "query_string": b"",
        }
        req = Request(scope)
        assert _is_authorized(req) is True
        assert "/" in PUBLIC_PATHS


class TestPasswordEncryption:
    def test_roundtrip_with_secret_key(self, monkeypatch):
        monkeypatch.setattr(settings, "secret_key", "test-secret-key")
        token = encrypt_secret("hunter2")
        assert token != "hunter2"
        assert token.startswith("gAAAA")
        assert decrypt_secret(token) == "hunter2"

    def test_plaintext_when_no_secret_key(self, monkeypatch):
        monkeypatch.setattr(settings, "secret_key", None)
        assert encrypt_secret("plain") == "plain"
        assert decrypt_secret("plain") == "plain"

    def test_legacy_plaintext_decrypt_passthrough(self, monkeypatch):
        monkeypatch.setattr(settings, "secret_key", "test-secret-key")
        assert decrypt_secret("not-encrypted") == "not-encrypted"

    def test_password_not_in_api_response(self, client, monkeypatch):
        monkeypatch.setattr(settings, "secret_key", "test-secret-key")
        resp = client.post(
            "/instances/",
            json={
                "name": "sec",
                "url": "http://airflow.test:8080",
                "password": "s3cret-pw",
            },
        )
        assert resp.status_code == 201
        assert "password" not in resp.json()

        listed = client.get("/instances/", headers={}).json()
        assert "password" not in listed[0]

    def test_password_stored_encrypted_in_db(self, client, monkeypatch):
        monkeypatch.setattr(settings, "secret_key", "test-secret-key")
        client.post(
            "/instances/",
            json={
                "name": "enc",
                "url": "http://airflow.test:8080",
                "password": "s3cret-pw",
            },
        )
        import sqlite3
        import tempfile
        from app.config import settings as s

        db_path = s.database_url.split("///")[-1]
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "select password from airflow_instances where name='enc'"
        ).fetchone()
        conn.close()
        assert row is not None
        stored = row[0]
        assert stored != "s3cret-pw"
        assert stored.startswith("gAAAA")

    def test_airflow_client_reads_decrypted_password(self, monkeypatch):
        from app.database import async_session
        from app.services.airflow_client import AirflowClient

        monkeypatch.setattr(settings, "secret_key", "test-secret-key")

        async def _roundtrip():
            async with async_session() as db:
                inst = AirflowInstance(
                    name="rt",
                    url="http://x",
                    password="plain-pw",
                )
                db.add(inst)
                await db.commit()
                await db.refresh(inst)
                assert inst.password == "plain-pw"
                client = AirflowClient(inst)
                assert client.password == "plain-pw"
                return inst.id

        run_async(_roundtrip())
