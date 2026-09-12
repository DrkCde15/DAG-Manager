from datetime import datetime
from pydantic import BaseModel

from app.models.models import InstanceStatus, DagState


class InstanceCreate(BaseModel):
    name: str
    url: str
    username: str | None = None
    password: str | None = None


class InstanceResponse(BaseModel):
    id: int
    name: str
    url: str
    username: str | None = None
    status: InstanceStatus
    airflow_version: str | None = None
    last_sync: datetime | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class InstanceUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    username: str | None = None
    password: str | None = None
    status: InstanceStatus | None = None


class DAGResponse(BaseModel):
    id: int
    dag_id: str
    instance_id: int
    instance_name: str
    description: str | None
    is_active: bool
    is_paused: bool
    schedule_interval: str | None
    last_parsed_time: datetime | None

    model_config = {"from_attributes": True}


class DAGRunResponse(BaseModel):
    id: int
    run_id: str
    state: DagState
    execution_date: datetime
    start_date: datetime | None
    end_date: datetime | None
    run_type: str | None

    model_config = {"from_attributes": True}


class DashboardSummary(BaseModel):
    total_instances: int
    active_instances: int
    total_dags: int
    active_dags: int
    paused_dags: int
    runs_today: int
    failed_runs: int
    running_runs: int


class InstanceHealth(BaseModel):
    instance_id: int
    instance_name: str
    status: str
    dag_count: int
    last_sync: datetime | None
    recent_failures: int
