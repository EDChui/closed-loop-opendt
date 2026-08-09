from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import datetime
from typing import Any, Optional, Literal

from k8s_observability.models import K8sTaskRecord, K8sPodRecord
from k8s_observability.utils import UnitUtils


class K8sEventObjectExtractor(ABC):
    @property
    @abstractmethod
    def resource_type(self) -> str:
        pass

    @abstractmethod
    def get_terminal_status(self, event_obj: Any) -> Optional[str]:
        raise NotImplementedError("Must be implemented by subclasses")
    
    @abstractmethod
    def is_terminal(self, event_obj: Any) -> bool:
        raise NotImplementedError("Must be implemented by subclasses")
    
    @abstractmethod
    def is_successful(self, event_obj: Any) -> bool:
        raise NotImplementedError("Must be implemented by subclasses")
    
    @abstractmethod
    def get_start_finish_dt(self, event_obj: Any) -> tuple[Optional[datetime], Optional[datetime]]:
        raise NotImplementedError("Must be implemented by subclasses")
    
    @abstractmethod
    def get_template_spec_containers(self, event_obj: Any) -> list[Any]:
        raise NotImplementedError("Must be implemented by subclasses")
    
    def extract_resource_requirements(self, event_obj: Any, resource_kind: Literal["requests", "limits"]="requests") -> tuple[float, int]:
        """
        Extract total CPU and memory resource requests or limits from a list of container specs.

        Returns:
        - cpu_count: Total requested CPU cores (float)
        - mem_capacity_mb: Total requested memory in MB (int)
        """
        containers = self.get_template_spec_containers(event_obj)
        cpu_count = 0.0
        mem_capacity_mb = 0

        for c in containers or []:
            resources_obj = getattr(c, "resources", None)
            if not resources_obj:
                continue

            if resource_kind == "requests":
                resources = getattr(resources_obj, "requests", None) or {}
            if resource_kind == "limits":
                resources = getattr(resources_obj, "limits", None) or {}

            cpu_req = resources.get("cpu")
            mem_req = resources.get("memory")

            if cpu_req is not None:
                cpu_count += UnitUtils.parse_cpu_to_core(cpu_req)
            if mem_req is not None:
                mem_capacity_mb += UnitUtils.parse_mem_to_mb(mem_req)

        return cpu_count, mem_capacity_mb

    def extract_metadata(self, uid: str, event_obj: Any) -> K8sTaskRecord:
        # Time-related fields
        metadata = getattr(event_obj, "metadata", None)
        submission_dt = getattr(metadata, "creation_timestamp", None)
        start_dt, finish_dt = self.get_start_finish_dt(event_obj)

        if submission_dt is None:
            raise ValueError("Could not determine submission time from event object")
        if start_dt is None:
            raise ValueError("Could not determine start time from event object")
        if finish_dt is None:
            raise ValueError("Could not determine finish time from event object")

        cpu_request_count, mem_request_capacity_mb = self.extract_resource_requirements(event_obj, "requests")
        cpu_limit_count, mem_limit_capacity_mb = self.extract_resource_requirements(event_obj, "limits")

        return K8sTaskRecord(
            resource_type=self.resource_type,
            namespace=getattr(metadata, "namespace", "default"),
            name=getattr(metadata, "name", "unknown"),
            uid=uid,
            submission_time=submission_dt,
            start_time=start_dt,
            finish_time=finish_dt,
            cpu_request_count=cpu_request_count,
            cpu_limit_count=cpu_limit_count,
            mem_request_capacity_mb=mem_request_capacity_mb,
            mem_limit_capacity_mb=mem_limit_capacity_mb,
            success_complete=self.is_successful(event_obj),
            terminal_status=self.get_terminal_status(event_obj) or "Unknown"
        )


class K8sPodEventExtractor(K8sEventObjectExtractor):
    @property
    def resource_type(self) -> str:
        return "pod"
    
    def get_terminal_status(self, event_obj: Any) -> Optional[str]:
        status = getattr(event_obj, "status", None)
        phase = getattr(status, "phase", None)
        if phase in ("Succeeded", "Failed"):
            return phase
        return None
    
    def is_terminal(self, event_obj: Any) -> bool:
        return self.get_terminal_status(event_obj) is not None
    
    def is_successful(self, event_obj: Any) -> bool:
        return self.get_terminal_status(event_obj) == "Succeeded"
    
    def get_start_finish_dt(self, event_obj: Any) -> tuple[Optional[datetime], Optional[datetime]]:
        status = getattr(event_obj, "status", None)
        if status is None:
            return None, None
        
        # Old method: Use pod's status.start_time
        # Now use container statuses for more accurate start/finish times
        # start_dt = getattr(status, "start_time", None)

        start_dt = None
        finish_dt = None

        started_ats, finished_ats = [], []

        # Check the pods' container statuses for more accurate start/finish times
        status_groups = [
            # getattr(status, "init_container_statuses", None) or [],
            getattr(status, "container_statuses", None) or [],
            # getattr(status, "ephemeral_container_statuses", None) or []
        ]

        for container_statuses in status_groups:
            for cs in container_statuses:
                for state_attr in ("state", "last_state"):
                    state = getattr(cs, state_attr, None)
                    if state is None:
                        continue

                    running = getattr(state, "running", None)
                    terminated = getattr(state, "terminated", None)

                    if running is not None:
                        running_started_at = getattr(running, "started_at", None)
                        if running_started_at is not None:
                            started_ats.append(running_started_at)
                    
                    if terminated is not None:
                        terminated_started_at = getattr(terminated, "started_at", None)
                        terminated_finished_at = getattr(terminated, "finished_at", None)
                        if terminated_started_at is not None:
                            started_ats.append(terminated_started_at)
                        if terminated_finished_at is not None:
                            finished_ats.append(terminated_finished_at)
        
        if start_dt is None and started_ats:
            start_dt = min(started_ats)
        if finish_dt is None and finished_ats:
            finish_dt = max(finished_ats)

        # Finish time fallback, check the pod conditions
        if finish_dt is None and self.is_terminal(event_obj):
            conditions = getattr(status, "conditions", None) or []
            for c in conditions:
                last_transition_time = getattr(c, "last_transition_time", None)
                if last_transition_time is not None:
                    finished_ats.append(last_transition_time)
            if finished_ats:
                finish_dt = max(finished_ats)
        return start_dt, finish_dt
    
    def get_template_spec_containers(self, event_obj: Any) -> list[Any]:
        spec = getattr(event_obj, "spec", None)
        containers = getattr(spec, "containers", None) or []
        return containers
    
    def extract_metadata(self, uid: str, event_obj: Any) -> K8sPodRecord:
        base_metrics = super().extract_metadata(uid, event_obj)
        owner_kind, owner_name = self._get_owner_reference(event_obj)
        spec = getattr(event_obj, "spec", None)
        node_name = getattr(spec, "node_name", "Unknown")

        if owner_kind is None:
            owner_kind = "Unknown"
        if owner_name is None:
            owner_name = "Unknown"

        return K8sPodRecord(
            **base_metrics.__dict__,
            node_name=node_name,
            owner_kind=owner_kind,
            owner_name=owner_name
        )
    
    def _get_owner_reference(self, event_obj: Any) -> tuple[Optional[str], Optional[str]]:
        metadata = getattr(event_obj, "metadata", None)
        owners = getattr(metadata, "owner_references", None) or []
        if not owners:
            return None, None

        owner = owners[0]
        return getattr(owner, "kind", None), getattr(owner, "name", None)


class K8sJobEventExtractor(K8sEventObjectExtractor):
    @property
    def resource_type(self) -> str:
        return "job"

    def get_terminal_status(self, event_obj: Any) -> Optional[str]:
        status = getattr(event_obj, "status", None)
        conditions = getattr(status, "conditions", None ) or []
        for c in conditions:
            if c.type == "Complete" and c.status == "True":
                return "Complete"
            if c.type == "Failed" and c.status == "True":
                return "Failed"
        return None
    
    def is_terminal(self, event_obj: Any) -> bool:
        return self.get_terminal_status(event_obj) is not None
    
    def is_successful(self, event_obj: Any) -> bool:
        return self.get_terminal_status(event_obj) == "Complete"
    
    def get_start_finish_dt(self, event_obj: Any) -> tuple[Optional[datetime], Optional[datetime]]:
        status = getattr(event_obj, "status", None)
        start_dt = getattr(status, "start_time", None)
        finish_dt = getattr(status, "completion_time", None)

        if finish_dt is None:
            # Fallback: Check conditions for completion/failed times
            conditions = getattr(status, "conditions", None) or []
            for c in conditions:
                if c.type == "Complete" and c.status == "True":
                    finish_dt = c.last_transition_time
                    break
                if c.type == "Failed" and c.status == "True":
                    finish_dt = c.last_transition_time
                    break
        return start_dt, finish_dt
    
    def get_template_spec_containers(self, event_obj: Any) -> list[Any]:
        spec = getattr(event_obj, "spec", None)
        template = getattr(spec, "template", None)
        template_spec = getattr(template, "spec", None)
        return getattr(template_spec, "containers", None) or []
    
    def extract_metadata(self, uid: str, event_obj: Any) -> K8sTaskRecord:
        base_metrics = super().extract_metadata(uid, event_obj)
        
        spec = getattr(event_obj, "spec", None)

        # Multiply CPU and memory by parallelism if specified
        parallelism = getattr(spec, "parallelism", 1) 
        cpu_request_count = base_metrics.cpu_request_count * parallelism
        cpu_limit_count = base_metrics.cpu_limit_count * parallelism
        mem_request_capacity_mb = base_metrics.mem_request_capacity_mb * parallelism
        mem_limit_capacity_mb = base_metrics.mem_limit_capacity_mb * parallelism

        return replace(
            base_metrics,
            cpu_request_count=cpu_request_count,
            cpu_limit_count=cpu_limit_count,
            mem_request_capacity_mb=mem_request_capacity_mb,
            mem_limit_capacity_mb=mem_limit_capacity_mb
        )
