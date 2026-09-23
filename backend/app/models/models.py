import enum
from datetime import datetime

from sqlalchemy import String, Integer, DateTime, Enum, ForeignKey, Text, Boolean, UniqueConstraint, TypeDecorator
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.security import encrypt_secret, decrypt_secret


class EncryptedString(TypeDecorator):
    """Text col that encrypts at rest when SECRET_KEY is set."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return encrypt_secret(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return decrypt_secret(value)


class InstanceStatus(str, enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class DagState(str, enum.Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    QUEUED = "queued"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"


class AirflowInstance(Base):
    __tablename__ = "airflow_instances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    password: Mapped[str | None] = mapped_column(EncryptedString(), nullable=True)
    status: Mapped[InstanceStatus] = mapped_column(
        Enum(InstanceStatus), default=InstanceStatus.ACTIVE
    )
    airflow_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_sync: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    dags: Mapped[list["DAG"]] = relationship("DAG", back_populates="instance", cascade="all, delete-orphan")


class DAG(Base):
    __tablename__ = "dags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    dag_id: Mapped[str] = mapped_column(String(255), nullable=False)
    instance_id: Mapped[int] = mapped_column(Integer, ForeignKey("airflow_instances.id"))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_paused: Mapped[bool] = mapped_column(Boolean, default=False)
    schedule_interval: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_parsed_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    instance: Mapped["AirflowInstance"] = relationship("AirflowInstance", back_populates="dags")
    runs: Mapped[list["DAGRun"]] = relationship("DAGRun", back_populates="dag", cascade="all, delete-orphan")


class DAGRun(Base):
    __tablename__ = "dag_runs"
    __table_args__ = (
        UniqueConstraint("dag_id", "run_id", name="uq_dag_runs_dag_id_run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    dag_id: Mapped[int] = mapped_column(Integer, ForeignKey("dags.id"))
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[DagState] = mapped_column(Enum(DagState), nullable=False)
    execution_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    start_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    run_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    dag: Mapped["DAG"] = relationship("DAG", back_populates="runs")
