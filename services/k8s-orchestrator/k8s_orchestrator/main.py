import asyncio
import logging
import os
from pathlib import Path

from odt_common import load_config_from_env
from odt_common.models import MetricDirection, RankedDecisionPolicy, RankedObjectiveSpec
from odt_common.utils import get_kafka_bootstrap_servers
from k8s_orchestrator.application.config import DecisionOrchestratorConfig
from k8s_orchestrator.application.orchestrator import DecisionOrchestrator
from k8s_orchestrator.kubernetes.system_adapter import K8sSystemAdapter
from k8s_orchestrator.domain.kubernetes import K8sProposalGenerator, K8sDecisionMaker
from k8s_orchestrator.kafka.state_publisher import KafkaStatePublisher
from k8s_orchestrator.kafka.simulation_gateway import KafkaSimulationGateway
from k8s_orchestrator.kafka.runtime_config_gateway import KafkaRuntimeConfigGateway
from k8s_orchestrator.persistence.jsonl_history_repository import JsonlHistoryRepository


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("aiokafka").setLevel(logging.WARNING)


async def main() -> None:
    logger.info("Starting Kubernetes Orchestrator service")
    # Load configuration from environment
    try:
        config = load_config_from_env()
        logger.info("Loaded configuration")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise

    # Get Kafka configuration from environment variables
    kafka_bootstrap_servers = get_kafka_bootstrap_servers()
    topology_topic = config.kafka.topics["topology"].name
    sim_batch_topic = config.kafka.topics["sim_batch"].name
    sim_batch_report_topic = config.kafka.topics["sim_batch_report"].name
    objectives_topic = config.kafka.topics["objectives"].name
    
    logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
    logger.info(f"Topology topic: {topology_topic}")
    logger.info(f"Simulation batch topic: {sim_batch_topic}")
    logger.info(f"Simulation batch report topic: {sim_batch_report_topic}")

    # Get other configuration from environment variables
    consumer_group = os.getenv("CONSUMER_GROUP", "k8s-orchestrator")
    kubeconfig_path = os.getenv("KUBECONFIG", "/kube/config")
    run_output_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    run_id = os.getenv("RUN_ID")

    if not run_id:
        logger.error("RUN_ID environment variable not set")
        raise ValueError("RUN_ID environment variable is required")

    # Get config settings
    namespace = config.global_config.namespace
    cpu_frequency_mhz = config.global_config.cpu_frequency_mhz
    refresh_interval_seconds = config.services.k8s_orchestrator.refresh_interval_seconds
    backlog_refresh_interval_seconds = config.services.k8s_orchestrator.backlog_refresh_interval_seconds
    backlog_threshold = config.services.k8s_orchestrator.backlog_threshold
    # TODO: Make the initial policy configurable
    initial_policy = RankedDecisionPolicy(
        policy_type="ranked",
        objectives={
            "runtime": RankedObjectiveSpec(name="runtime", direction=MetricDirection.MIN, priority=1, tie_tolerance=30.0),
            "utilization": RankedObjectiveSpec(name="utilization", direction=MetricDirection.MAX, priority=2, tie_tolerance=0.05),
            "power": RankedObjectiveSpec(name="power", direction=MetricDirection.MIN, priority=2, tie_tolerance=0.0)
        }
    )

    logger.info(f"CPU frequency (MHz): {cpu_frequency_mhz}")
    logger.info(f"Refresh interval (seconds): {refresh_interval_seconds}")

    # Orchestrator configuration
    orchestrator_config = DecisionOrchestratorConfig(
        refresh_interval_seconds=refresh_interval_seconds,
        backlog_refresh_interval_seconds=backlog_refresh_interval_seconds
    )

    # Initialize components
    system_adapter = K8sSystemAdapter(
        kubeconfig_path=kubeconfig_path,
        cpu_frequency_mhz=cpu_frequency_mhz,
        namespace=namespace
    )
    proposal_generator = K8sProposalGenerator()
    decision_maker = K8sDecisionMaker(
        policy=initial_policy,
        backlog_threshold=backlog_threshold
    )
    state_publisher = KafkaStatePublisher(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        topology_topic=topology_topic
    )
    simulation_gateway = KafkaSimulationGateway(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        sim_batch_topic=sim_batch_topic,
        sim_batch_report_topic=sim_batch_report_topic,
        consumer_group=consumer_group
    )
    runtime_config_gateway = KafkaRuntimeConfigGateway(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        objectives_topic=objectives_topic,
        consumer_group=consumer_group
    )
    jsonl_history_repository = JsonlHistoryRepository(
        output_dir=Path(run_output_dir) / run_id / "history"
    )

    # Start components that require async startup
    await state_publisher.start()
    await simulation_gateway.start()
    await runtime_config_gateway.start()

    decision_orchestrator = DecisionOrchestrator(
        real_system=system_adapter,
        proposal_generator=proposal_generator,
        decision_maker=decision_maker,
        state_publisher=state_publisher,
        simulation_gateway=simulation_gateway,
        runtime_config_gateway=runtime_config_gateway,
        history_port=jsonl_history_repository,
        config=orchestrator_config
    )

    # Run the orchestrator
    try:
        logger.info("Running Kubernetes Decision Orchestrator")
        await decision_orchestrator.run()
    finally:
        await state_publisher.stop()
        await simulation_gateway.stop()
        await runtime_config_gateway.stop()
        logger.info("Kubernetes Orchestrator service stopped")


if __name__ == "__main__":
    asyncio.run(main())