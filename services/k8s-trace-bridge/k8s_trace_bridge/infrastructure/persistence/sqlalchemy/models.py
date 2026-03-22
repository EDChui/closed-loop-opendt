from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from k8s_trace_bridge.infrastructure.persistence.sqlalchemy.base import Base


class WorkloadCompletionModel(Base):
    __tablename__ = "workload_completions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    resource_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    namespace: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    uid: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    submission_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    start_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finish_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    cpu_request_count: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cpu_limit_count: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    mem_request_capacity_mb: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mem_limit_capacity_mb: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    success_complete: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    terminal_status: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    node_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_kind: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    owner_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)


class ResourceUsageSnapshotModel(Base):
    __tablename__ = "resource_usage_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uid: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    capture_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    cpu_usage: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    mem_usage_mb: Mapped[Optional[float]] = mapped_column(Float, nullable=True)