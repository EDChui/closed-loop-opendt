from k8s_trace_bridge.models import WorkloadCompletion, PodCompletion, ResourceUsageSnapshot
from k8s_trace_bridge.infrastructure.persistence.sqlalchemy.models import ResourceUsageSnapshotModel, WorkloadCompletionModel


def to_workload_completion_model(entity: WorkloadCompletion) -> WorkloadCompletionModel:
    return WorkloadCompletionModel(
        resource_type=entity.resource_type,
        namespace=entity.namespace,
        name=entity.name,
        uid=entity.uid,
        submission_time=entity.submission_time,
        start_time=entity.start_time,
        finish_time=entity.finish_time,
        cpu_request_count=entity.cpu_request_count,
        cpu_limit_count=entity.cpu_limit_count,
        mem_request_capacity_mb=entity.mem_request_capacity_mb,
        mem_limit_capacity_mb=entity.mem_limit_capacity_mb,
        success_complete=entity.success_complete,
        terminal_status=entity.terminal_status,
        node_name=getattr(entity, "node_name", None),
        owner_kind=getattr(entity, "owner_kind", None),
        owner_name=getattr(entity, "owner_name", None)
    )

def to_workload_completion_entity(model: WorkloadCompletionModel) -> WorkloadCompletion:
    common_kwargs = {
        "resource_type": model.resource_type,
        "namespace": model.namespace,
        "name": model.name,
        "uid": model.uid,
        "submission_time": model.submission_time,
        "start_time": model.start_time,
        "finish_time": model.finish_time,
        "cpu_request_count": model.cpu_request_count,
        "cpu_limit_count": model.cpu_limit_count,
        "mem_request_capacity_mb": model.mem_request_capacity_mb,
        "mem_limit_capacity_mb": model.mem_limit_capacity_mb,
        "success_complete": model.success_complete,
        "terminal_status": model.terminal_status
    }
    if model.resource_type == "pod" and model.node_name and model.owner_kind and model.owner_name:
        return PodCompletion(
            **common_kwargs,
            node_name=model.node_name,
            owner_kind=model.owner_kind,
            owner_name=model.owner_name
        )
    else:
        return WorkloadCompletion(**common_kwargs)

def to_resource_usage_snapshot_model(entity: ResourceUsageSnapshot) -> ResourceUsageSnapshotModel:
    return ResourceUsageSnapshotModel(
        uid=entity.uid,
        capture_time=entity.capture_time,
        cpu_usage=entity.cpu_usage,
        mem_usage_mb=entity.mem_usage_mb
    )

def to_resource_usage_snapshot_entity(model: ResourceUsageSnapshotModel) -> ResourceUsageSnapshot:
    return ResourceUsageSnapshot(
        resource_type="",
        namespace="",
        name="",
        uid=model.uid,
        capture_time=model.capture_time,    # type: ignore
        cpu_usage=model.cpu_usage,          # type: ignore
        mem_usage_mb=model.mem_usage_mb     # type: ignore
    )
