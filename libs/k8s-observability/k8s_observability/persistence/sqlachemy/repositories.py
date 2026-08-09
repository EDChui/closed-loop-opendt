from datetime import datetime
from typing import Sequence
from sqlalchemy import select
from sqlalchemy.orm import Session

from k8s_observability.models import K8sTaskRecord, K8sResourceUsageSnapshot, NodePowerReading, NodeUtilizationSnapshot
from k8s_observability.persistence.sqlachemy.mappers import (
    row_to_resource_usage_snapshot,
    resource_usage_snapshot_to_row,
    task_record_to_row,
    node_power_reading_to_row,
    row_to_node_power_reading,
    node_utilization_snapshot_to_row,
)
from k8s_observability.persistence.sqlachemy.tables import K8sResourceUsageSnapshotRow, NodePowerReadingRow

class K8sTaskRecordRepository:
    def __init__(self, session: Session):
        self._session = session

    def add(self, row: K8sTaskRecord) -> int:
        model = task_record_to_row(row)
        self._session.add(model)
        self._session.flush()
        return model.id
    
    def commit(self):
        self._session.commit()


class K8sWorkloadResourceUsageSnapshotRepository:
    def __init__(self, session: Session):
        self._session = session

    def add_many(self, rows: Sequence[K8sResourceUsageSnapshot]) -> Sequence[int]:
        models = [resource_usage_snapshot_to_row(row) for row in rows]
        self._session.add_all(models)
        self._session.flush()
        return [model.id for model in models]

    def list_by_uid(self, uid: str) -> Sequence[K8sResourceUsageSnapshot]:
        query = select(K8sResourceUsageSnapshotRow) \
            .where(K8sResourceUsageSnapshotRow.uid == uid) \
            .order_by(K8sResourceUsageSnapshotRow.capture_time.asc())
        result = self._session.execute(query).scalars().all()
        return [row_to_resource_usage_snapshot(row) for row in result]

    def commit(self):
        self._session.commit()


class NodePowerReadingRepository:
    def __init__(self, session: Session):
        self._session = session

    def add_many(self, rows: Sequence[NodePowerReading]) -> Sequence[int]:
        models = [node_power_reading_to_row(row) for row in rows]
        self._session.add_all(models)
        self._session.flush()
        return [model.id for model in models]

    def list_between(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> Sequence[NodePowerReading]:
        query = select(NodePowerReadingRow).order_by(NodePowerReadingRow.capture_time.asc())

        if start_time is not None:
            query = query.where(NodePowerReadingRow.capture_time >= start_time)
        if end_time is not None:
            query = query.where(NodePowerReadingRow.capture_time <= end_time)

        result = self._session.execute(query).scalars().all()
        return [row_to_node_power_reading(row) for row in result]

    def commit(self):
        self._session.commit()


class NodeResourceUsageSnapshotRepository:
    def __init__(self, session: Session):
        self._session = session

    def add_many(self, rows: Sequence[NodeUtilizationSnapshot]) -> Sequence[int]:
        models = [node_utilization_snapshot_to_row(row) for row in rows]
        self._session.add_all(models)
        self._session.flush()
        return [model.id for model in models]

    def commit(self):
        self._session.commit()
