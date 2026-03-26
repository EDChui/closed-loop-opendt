from typing import Dict
from kubernetes import client, config


class K8sJobUidResolver:
    def __init__(self, config_file: str | None = None):
        config.load_kube_config(config_file=config_file)
        self.batch = client.BatchV1Api()

    def get_namespaced_job_uids(self, namespace: str) -> Dict[str, str]:
        """Get a mapping of job names to their UIDs in the specified namespace.
        Assumes that job names are unique within the namespace.

        Args:
            namespace: The Kubernetes namespace to query for jobs.
        Returns:
            A dictionary mapping job names to their UIDs.
        """
        jobs = self.batch.list_namespaced_job(namespace=namespace).items
        result = {}
        for job in jobs:
            name = job.metadata.name
            uid = str(job.metadata.uid)
            result[name] = uid
        return result
