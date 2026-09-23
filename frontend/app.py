import os
import time
import httpx
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

API_URL = os.getenv("API_URL", "http://localhost:8000")
API_TOKEN = os.getenv("API_TOKEN", "")

st.set_page_config(page_title="DAG Manager", page_icon=": DAG:", layout="wide")


def _headers() -> dict:
    if API_TOKEN:
        return {"Authorization": f"Bearer {API_TOKEN}"}
    return {}


def api_get(endpoint: str):
    try:
        resp = httpx.get(f"{API_URL}{endpoint}", timeout=10.0, headers=_headers())
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"API Error: {e}")
        return None


def api_post(endpoint: str, data: dict = None):
    try:
        resp = httpx.post(f"{API_URL}{endpoint}", json=data, timeout=10.0, headers=_headers())
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"API Error: {e}")
        return None


def api_delete(endpoint: str):
    try:
        resp = httpx.delete(f"{API_URL}{endpoint}", timeout=10.0, headers=_headers())
        return resp.status_code == 204
    except Exception as e:
        st.error(f"API Error: {e}")
        return False


st.title("DAG Manager Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Instances", "DAGs", "Settings"])

with tab1:
    st.header("Overview")

    summary = api_get("/dashboard/summary")
    health = api_get("/dashboard/health")
    has_sync_data = bool(health) and any(h.get("last_sync") for h in health)

    if summary:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Instances", summary["total_instances"], f"{summary['active_instances']} active")
        col2.metric("Total DAGs", summary["total_dags"], f"{summary['active_dags']} active")
        if has_sync_data:
            col3.metric("Runs Today", summary["runs_today"])
            col4.metric("Failed Runs", summary["failed_runs"], delta_color="inverse")
            if summary["failed_runs"] > 0:
                st.warning(f"⚠ {summary['failed_runs']} failed runs detected!")
        else:
            col3.metric("Runs Today", "—")
            col4.metric("Failed Runs", "—")
            st.info("Run metrics appear after the first sync with runs.")

    st.subheader("Instance Health")
    if health:
        df_health = pd.DataFrame(health)
        if not df_health.empty:
            st.dataframe(
                df_health[["instance_name", "status", "dag_count", "last_sync", "recent_failures"]],
                use_container_width=True,
            )

            if any(h["recent_failures"] > 0 for h in health):
                st.error("Some instances have recent failures!")

with tab2:
    st.header("Airflow Instances")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Sync All"):
            with st.spinner("Syncing all instances..."):
                result = api_post("/instances/sync-all")
                if result:
                    st.success("Sync started!")
                    st.rerun()

    instances = api_get("/instances/")
    if instances:
        for inst in instances:
            with st.expander(f"{inst['name']} - {inst['status']}"):
                st.json(inst)

                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button(f"Sync {inst['name']}", key=f"sync_{inst['id']}"):
                        with st.spinner("Syncing..."):
                            result = api_post(f"/instances/{inst['id']}/sync")
                            if result:
                                st.success("Sync complete!")
                                st.rerun()
                with col2:
                    if st.button(f"Health {inst['name']}", key=f"health_{inst['id']}"):
                        result = api_get(f"/instances/{inst['id']}/health")
                        if result:
                            st.json(result)
                with col3:
                    if st.button(f"Delete {inst['name']}", key=f"del_{inst['id']}"):
                        if api_delete(f"/instances/{inst['id']}"):
                            st.success("Deleted!")
                            st.rerun()

    st.subheader("Quick Add Multiple")
    with st.form("bulk_add"):
        st.info("Add multiple instances at once (one per line: name|url)")
        bulk_input = st.text_area(
            "Instances",
            placeholder="projeto-a|http://localhost:8080\nprojeto-b|https://airflow.company.com",
        )
        
        if st.form_submit_button("Add All"):
            if bulk_input:
                added = 0
                for line in bulk_input.strip().split("\n"):
                    if "|" in line:
                        name, url = line.split("|", 1)
                        result = api_post("/instances/", {
                            "name": name.strip(),
                            "url": url.strip(),
                        })
                        if result:
                            added += 1
                if added:
                    st.success(f"Added {added} instances!")
                    st.rerun()

    st.subheader("Add New Instance")
    with st.form("add_instance"):
        name = st.text_input("Name", placeholder="data-warehouse")
        url = st.text_input("Airflow URL", placeholder="http://localhost:8080 or https://airflow.company.com")
        username = st.text_input("Username (leave empty if no auth)")
        password = st.text_input("Password", type="password")

        if st.form_submit_button("Add Instance"):
            if name and url:
                data = {
                    "name": name,
                    "url": url,
                }
                if username:
                    data["username"] = username
                    data["password"] = password or ""
                
                result = api_post("/instances/", data)
                if result:
                    st.success(f"Instance '{name}' added!")
                    st.rerun()

with tab3:
    st.header("DAGs")

    instances = api_get("/instances/") or []
    instance_options = {inst["name"]: inst["id"] for inst in instances}

    col1, col2 = st.columns(2)
    with col1:
        selected_instance = st.selectbox(
            "Filter by Instance",
            ["All"] + list(instance_options.keys()),
        )
    with col2:
        search = st.text_input("Search DAGs")

    params = {}
    if selected_instance != "All":
        params["instance_id"] = instance_options[selected_instance]
    if search:
        params["search"] = search

    dags = api_get("/dags/") or []
    if dags:
        df = pd.DataFrame(dags)
        if selected_instance != "All":
            df = df[df["instance_name"] == selected_instance]
        if search:
            df = df[df["dag_id"].str.contains(search, case=False, na=False)]

        st.dataframe(
            df[["dag_id", "instance_name", "is_active", "is_paused", "schedule_interval"]],
            use_container_width=True,
        )

        paused_count = len(df[df["is_paused"] == True])
        active_count = len(df[df["is_active"] == True])

        col1, col2 = st.columns(2)
        col1.metric("Active DAGs", active_count)
        col2.metric("Paused DAGs", paused_count)

        st.divider()
        st.subheader("DAG Actions")

        dag_options = {d["dag_id"]: d["id"] for d in dags}
        selected_dag = st.selectbox("Select DAG", list(dag_options.keys()))

        if selected_dag:
            st.markdown("**Trigger DAG**")
            if st.button(f"Run {selected_dag}", key="trigger_dag"):
                result = api_post(f"/dags/{dag_options[selected_dag]}/trigger")
                if result and result.get("status") == "success":
                    st.session_state["live_run"] = {
                        "dag_id": dag_options[selected_dag],
                        "dag_name": selected_dag,
                        "run_id": result.get("run_id"),
                        "start_time": time.time(),
                    }
                    st.rerun()

        if "live_run" in st.session_state:
            live = st.session_state["live_run"]
            elapsed = int(time.time() - live["start_time"])

            st.divider()
            st.subheader(f"Live: {live['dag_name']} - {live['run_id']}")

            progress_bar = st.progress(0)
            status_text = st.empty()
            log_container = st.empty()

            for i in range(60):
                tasks = api_get(f"/dags/{live['dag_id']}/runs/{live['run_id']}/tasks")
                if tasks and tasks.get("tasks"):
                    task_list = tasks["tasks"]
                    total = len(task_list)
                    done = sum(1 for t in task_list if t.get("state") in ("success", "failed", "skipped"))
                    running = sum(1 for t in task_list if t.get("state") == "running")
                    queued = sum(1 for t in task_list if t.get("state") == "queued")

                    progress = done / total if total > 0 else 0
                    progress_bar.progress(min(progress, 0.99))

                    states = {}
                    for t in task_list:
                        s = t.get("state", "unknown")
                        states[s] = states.get(s, 0) + 1
                    status_text.text(f"Tasks: {states} | Elapsed: {elapsed}s")

                    logs = api_get(f"/dags/{live['dag_id']}/runs/{live['run_id']}/logs")
                    if logs and logs.get("logs"):
                        log_container.code(logs["logs"][-3000:], language="text")

                    if running == 0 and queued == 0 and done == total:
                        progress_bar.progress(1.0)
                        has_failed = any(t.get("state") == "failed" for t in task_list)
                        if has_failed:
                            st.error(f"DAG failed after {elapsed}s")
                        else:
                            st.success(f"DAG completed in {elapsed}s")
                        if "live_run" in st.session_state:
                            del st.session_state["live_run"]
                        break

                time.sleep(2)
                elapsed = int(time.time() - live["start_time"])

        elif "runs" in st.session_state:
            del st.session_state["runs"]

with tab4:
    st.header("Settings")
    st.info("Configuration will be available here in future versions.")
    st.write(f"**API URL:** {API_URL}")
    st.write(f"**API Token:** {'configured' if API_TOKEN else 'not set'}")
    st.write(f"**Sync Interval:** 5 minutes")
