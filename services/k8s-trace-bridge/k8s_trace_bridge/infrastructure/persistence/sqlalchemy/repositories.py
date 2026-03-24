from collections import defaultdict
from typing import Sequence, Dict
from sqlalchemy import select
from sqlalchemy.orm import Session

from k8s_trace_bridge.models import WorkloadCompletion, ResourceUsageSnapshot
from k8s_trace_bridge.infrastructure.persistence.sqlalchemy.mappers import (
    to_resource_usage_snapshot_entity,
    to_resource_usage_snapshot_model,
    to_workload_completion_entity,
    to_workload_completion_model
)
from k8s_trace_bridge.infrastructure.persistence.sqlalchemy.models import ResourceUsageSnapshotModel, WorkloadCompletionModel
from k8s_trace_bridge.utils import TimeUtils

class WorkloadCompletionRepository:
    def __init__(self, session: Session):
        self._session = session

    def add(self, row: WorkloadCompletion) -> int:
        model = to_workload_completion_model(row)
        self._session.add(model)
        self._session.flush()
        return model.id
    
    def commit(self):
        self._session.commit()


class ResourceUsageSnapshotRepository:
    def __init__(self, session: Session):
        self._session = session

    def add_many(self, rows: Sequence[ResourceUsageSnapshot]) -> Sequence[int]:
        models = [to_resource_usage_snapshot_model(row) for row in rows]
        self._session.add_all(models)
        self._session.flush()
        return [model.id for model in models]

    def list_by_uid(self, uid: str) -> Sequence[ResourceUsageSnapshot]:
        query = select(ResourceUsageSnapshotModel) \
            .where(ResourceUsageSnapshotModel.uid == uid) \
            .order_by(ResourceUsageSnapshotModel.capture_time.asc())
        result = self._session.execute(query).scalars().all()
        return [to_resource_usage_snapshot_entity(row) for row in result]

    def commit(self):
        self._session.commit()
    
    def close(self):
        self._session.close()
