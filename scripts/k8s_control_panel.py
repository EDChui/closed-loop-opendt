"""
Simple K8s control panel script to list nodes and mark a node schedulable/unschedulable.

Requirements:
- click
- kubernetes

Examples:
  python k8s_control_panel.py list
  python k8s_control_panel.py status worker-1
  python k8s_control_panel.py cordon worker-1
  python k8s_control_panel.py uncordon worker-1
  python k8s_control_panel.py settype worker-1 cloud/edge/endpoint

Optional:
  python k8s_control_panel.py list --kubeconfig ~/.kube/config --context my-cluster
"""

import sys
import click
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from kubernetes.client.rest import ApiException


def load_k8s_config(kubeconfig=None, context=None):
    try:
        config.load_incluster_config()
        # click.echo("Loaded in-cluster Kubernetes config")
    except ConfigException:
        config.load_kube_config(config_file=kubeconfig, context=context)
        # click.echo("Loaded kubeconfig")


def get_api():
    return client.CoreV1Api()


def get_unschedulable(node) -> bool:
    return bool(getattr(node.spec, "unschedulable", False))


def set_unschedulable(api, node_name: str, unschedulable: bool):
    patch_body = {
        "spec": {
            "unschedulable": unschedulable
        }
    }
    return api.patch_node(name=node_name, body=patch_body)


def get_avalilability(node) -> bool:
    metadata = getattr(node, "metadata", None)
    labels = getattr(metadata, "labels", {}) or {}
    return labels.get("k8s-observability/availability", "") != "unavailable"


def get_ready_status(node) -> str:
    for condition in node.status.conditions or []:
        if condition.type == "Ready":
            return "Ready" if condition.status == "True" else "NotReady"
    return "Unknown"


def get_roles(node) -> str:
    labels = node.metadata.labels or {}
    roles = []

    for key in labels:
        if key.startswith("node-role.kubernetes.io/"):
            role = key.split("/", 1)[1]
            roles.append(role if role else "default")

    return ",".join(sorted(roles)) if roles else "<none>"


def get_node_type(node) -> str:
    labels = node.metadata.labels or {}
    if "k8s-observability/node-type" in labels:
        if labels["k8s-observability/node-type"] in {"cloud", "edge", "endpoint"}:
            return labels["k8s-observability/node-type"]
    return "unknown"


def set_node_type(api, node_name: str, node_type: str):
    if node_type not in {"cloud", "edge", "endpoint"}:
        raise ValueError("Invalid node type. Must be 'cloud', 'edge', or 'endpoint'.")

    patch_body = {
        "metadata": {
            "labels": {
                "k8s-observability/node-type": node_type
            }
        }
    }
    return api.patch_node(name=node_name, body=patch_body)


def get_internal_ip(node) -> str:
    for addr in node.status.addresses or []:
        if addr.type == "InternalIP":
            return addr.address
    return "-"


def print_status(api, node_name: str):
    node = api.read_node(name=node_name)
    ready = get_ready_status(node)
    schedulable = "unschedulable" if get_unschedulable(node) else "schedulable"
    availability = "available" if get_avalilability(node) else "unavailable"
    roles = get_roles(node)
    node_type = get_node_type(node)
    version = getattr(node.status.node_info, "kubelet_version", "-")
    ip = get_internal_ip(node)

    click.echo(f"Name        : {node.metadata.name}")
    click.echo(f"Ready       : {ready}")
    click.echo(f"Schedulable : {schedulable}")
    click.echo(f"Availability: {availability}")
    click.echo(f"Roles       : {roles}")
    click.echo(f"Node Type   : {node_type}")
    click.echo(f"Kubelet     : {version}")
    click.echo(f"InternalIP  : {ip}")


def list_nodes(api):
    nodes = api.list_node().items

    if not nodes:
        click.echo("No nodes found")
        return

    header = (
        f"{'NAME':30} {'READY':10} {'SCHEDULING':18} "
        f"{'AVAILABILITY':15} {'ROLES':20} "
        f"{'NODE-TYPE':15} {'VERSION':15} {'INTERNAL-IP':15}"
    )
    click.echo(header)
    click.echo("-" * len(header))

    for node in nodes:
        name = node.metadata.name
        ready = get_ready_status(node)
        availability = "available" if get_avalilability(node) else "unavailable"
        scheduling = "unschedulable" if get_unschedulable(node) else "schedulable"
        roles = get_roles(node)
        node_type = get_node_type(node)
        version = getattr(node.status.node_info, "kubelet_version", "-")
        ip = get_internal_ip(node)

        click.echo(
            f"{name:30} {ready:10} {scheduling:18} {availability:15} "
            f"{roles:20} {node_type:15} {version:15} {ip:15}"
        )


def build_api(kubeconfig=None, context=None):
    load_k8s_config(kubeconfig=kubeconfig, context=context)
    return get_api()


def common_options(func):
    func = click.option("--context", help="Kubeconfig context name", default=None)(func)
    func = click.option("--kubeconfig", help="Path to kubeconfig file", default=None)(func)
    return func


@click.group()
def cli():
    """List Kubernetes nodes and mark a node schedulable/unschedulable."""
    pass


@cli.command()
@common_options
def list(kubeconfig, context):
    """List nodes."""
    try:
        api = build_api(kubeconfig=kubeconfig, context=context)
        list_nodes(api)
    except ApiException as e:
        raise click.ClickException(format_api_error(e))
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument("node_name")
@common_options
def status(node_name, kubeconfig, context):
    """Show node status."""
    try:
        api = build_api(kubeconfig=kubeconfig, context=context)
        print_status(api, node_name)
    except ApiException as e:
        raise click.ClickException(format_api_error(e))
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument("node_name")
@common_options
def cordon(node_name, kubeconfig, context):
    """Mark node unschedulable."""
    try:
        api = build_api(kubeconfig=kubeconfig, context=context)
        node = api.read_node(name=node_name)
        current = get_unschedulable(node)

        if current:
            click.echo(f"Node {node_name} is already unschedulable")
            return

        set_unschedulable(api, node_name, True)
        click.echo(f"Node {node_name} has been cordoned (unschedulable)")
    except ApiException as e:
        raise click.ClickException(format_api_error(e))
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument("node_name")
@common_options
def uncordon(node_name, kubeconfig, context):
    """Mark node schedulable."""
    try:
        api = build_api(kubeconfig=kubeconfig, context=context)
        node = api.read_node(name=node_name)
        current = get_unschedulable(node)

        if not current:
            click.echo(f"Node {node_name} is already schedulable")
            return

        set_unschedulable(api, node_name, False)
        click.echo(f"Node {node_name} has been uncordoned (schedulable)")
    except ApiException as e:
        raise click.ClickException(format_api_error(e))
    except Exception as e:
        raise click.ClickException(str(e))
    

@cli.command()
@click.argument("node_name")
@click.argument("node_type", type=click.Choice(["cloud", "edge", "endpoint"]))
@common_options
def settype(node_name, node_type, kubeconfig, context):
    """Set node type label (cloud/edge/endpoint)."""
    try:
        api = build_api(kubeconfig=kubeconfig, context=context)
        set_node_type(api, node_name, node_type)
        click.echo(f"Node {node_name} has been labeled with node type '{node_type}'")
    except ApiException as e:
        raise click.ClickException(format_api_error(e))
    except Exception as e:
        raise click.ClickException(str(e))


def format_api_error(e: ApiException) -> str:
    msg = f"Kubernetes API error: status={e.status}, reason={e.reason}"
    if e.body:
        msg = f"{msg}\n{e.body}"
    return msg


if __name__ == "__main__":
    cli()
