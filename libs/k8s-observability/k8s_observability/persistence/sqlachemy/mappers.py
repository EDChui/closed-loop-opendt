from k8s_observability.models import K8sTaskRecord, K8sPodRecord, K8sResourceUsageSnapshot, NodePowerReading
from k8s_observability.persistence import K8sTaskRecordRow, K8sResourceUsageSnapshotRow, NodePowerReadingRow


def task_record_to_row(entity: K8sTaskRecord) -> K8sTaskRecordRow:
    return K8sTaskRecordRow(
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

def row_to_task_record(row: K8sTaskRecordRow) -> K8sTaskRecord:
    common_kwargs = {
        "resource_type": row.resource_type,
        "namespace": row.namespace,
        "name": row.name,
        "uid": row.uid,
        "submission_time": row.submission_time,
        "start_time": row.start_time,
        "finish_time": row.finish_time,
        "cpu_request_count": row.cpu_request_count,
        "cpu_limit_count": row.cpu_limit_count,
        "mem_request_capacity_mb": row.mem_request_capacity_mb,
        "mem_limit_capacity_mb": row.mem_limit_capacity_mb,
        "success_complete": row.success_complete,
        "terminal_status": row.terminal_status
    }
    if row.resource_type == "pod" and row.node_name and row.owner_kind and row.owner_name:
        return K8sPodRecord(
            **common_kwargs,
            node_name=row.node_name,
            owner_kind=row.owner_kind,
            owner_name=row.owner_name
        )
    else:
        return K8sTaskRecord(**common_kwargs)

def resource_usage_snapshot_to_row(entity: K8sResourceUsageSnapshot) -> K8sResourceUsageSnapshotRow:
    return K8sResourceUsageSnapshotRow(
        uid=entity.uid,
        capture_time=entity.capture_time,
        cpu_usage=entity.cpu_usage,
        mem_usage_mb=entity.mem_usage_mb
    )

def row_to_resource_usage_snapshot(row: K8sResourceUsageSnapshotRow) -> K8sResourceUsageSnapshot:
    return K8sResourceUsageSnapshot(
        resource_type="",
        namespace="",
        name="",
        uid=row.uid,
        capture_time=row.capture_time,    # type: ignore
        cpu_usage=row.cpu_usage,          # type: ignore
        mem_usage_mb=row.mem_usage_mb     # type: ignore
    )

def node_power_reading_to_row(entity: NodePowerReading) -> NodePowerReadingRow:
    return NodePowerReadingRow(
        node_name=entity.node_name,
        capture_time=entity.capture_time,
        energy_uj=entity.energy_uj,
        energy_usage_j=entity.energy_usage_j,
        power_draw_w=entity.power_draw_w,
    )

def row_to_node_power_reading(row: NodePowerReadingRow) -> NodePowerReading:
    return NodePowerReading(
        node_name=row.node_name or "",
        capture_time=row.capture_time,              # type: ignore[arg-type]
        energy_uj=row.energy_uj or 0,
        energy_usage_j=row.energy_usage_j or 0.0,
        power_draw_w=row.power_draw_w or 0.0,
    )
