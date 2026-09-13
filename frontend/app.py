import os
import httpx
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="DAG Manager", page_icon=": DAG:", layout="wide")


def api_get(endpoint: str):
    try:
        resp = httpx.get(f"{API_URL}{endpoint}", timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"API Error: {e}")
        return None


def api_post(endpoint: str, data: dict = None):
    try:
        resp = httpx.post(f"{API_URL}{endpoint}", json=data, timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"API Error: {e}")
        return None


def api_delete(endpoint: str):
    try:
        resp = httpx.delete(f"{API_URL}{endpoint}", timeout=10.0)
        return resp.status_code == 204
    except Exception as e:
        st.error(f"API Error: {e}")
        return False


st.title("DAG Manager Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Instances", "DAGs", "Settings"])

with tab1:
    st.header("Overview")

    summary = api_get("/dashboard/summary")
    if summary:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Instances", summary["total_instances"], f"{summary['active_instances']} active")
        col2.metric("Total DAGs", summary["total_dags"], f"{summary['active_dags']} active")
        col3.metric("Runs Today", summary["runs_today"])
        col4.metric("Failed Runs", summary["failed_runs"], delta_color="inverse")

        if summary["failed_runs"] > 0:
            st.warning(f"⚠ {summary['failed_runs']} failed runs detected!")

    st.subheader("Instance Health")
    health = api_get("/dashboard/health")
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

with tab4:
    st.header("Settings")
    st.info("Configuration will be available here in future versions.")
    st.write(f"**API URL:** {API_URL}")
    st.write(f"**Sync Interval:** 5 minutes")
