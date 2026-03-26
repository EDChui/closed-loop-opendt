from typing import Sequence
from sqlalchemy import select
from sqlalchemy.orm import Session

from k8s_observability.models import K8sTaskRecord, K8sResourceUsageSnapshot
from k8s_observability.persistence.sqlachemy.mappers import (
    row_to_resource_usage_snapshot,
    resource_usage_snapshot_to_row,
    task_record_to_row
)
from k8s_observability.persistence.sqlachemy.tables import K8sResourceUsageSnapshotRow

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


class K8sResourceUsageSnapshotRepository:
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
