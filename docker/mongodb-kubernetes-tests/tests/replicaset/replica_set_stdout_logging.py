"""Test that agent and MongoDB logs appear in stdout (kubectl logs)."""

from kubetester.kubetester import KubernetesTester, is_default_architecture_static
from kubetester.kubetester import fixture as yaml_fixture
from kubetester.mongodb import MongoDB
from kubetester.phase import Phase
from pytest import fixture, mark
from tests.pod_logs import get_pod_logs, get_agent_logs, get_mongodb_logs


RESOURCE_NAME = "rs-stdout-test"


@fixture(scope="module")
def replica_set(namespace: str, custom_mdb_version: str, cluster_domain: str) -> MongoDB:
    """Create a basic replica set for stdout logging test."""
    resource = MongoDB.from_yaml(yaml_fixture("replica-set.yaml"), RESOURCE_NAME, namespace)
    resource.set_version(custom_mdb_version)
    resource["spec"]["clusterDomain"] = cluster_domain
    resource["spec"]["members"] = 1
    return resource


@fixture(scope="module")
def deployed_replica_set(replica_set: MongoDB) -> MongoDB:
    """Deploy the replica set and wait for it to become ready."""
    KubernetesTester.create_mdb(replica_set)
    replica_set.assert_reaches_phase(Phase.RUNNING, timeout=600)
    return replica_set


@mark.e2e_replica_set_stdout_logging
def test_agent_logs_in_stdout(deployed_replica_set: MongoDB):
    """Verify automation agent JSON logs appear in kubectl logs output."""
    namespace = deployed_replica_set.namespace
    pod_name = f"{deployed_replica_set.name}-0"
    container_name = "mongodb-agent" if is_default_architecture_static() else "mongodb-enterprise-database"

    logs = get_pod_logs(namespace, pod_name, container_name)
    agent_logs = get_agent_logs(logs)

    assert len(agent_logs) > 0, (
        f"Expected agent logs in stdout for pod {namespace}/{pod_name}, "
        f"but found none. Total log lines: {len(logs)}"
    )


@mark.e2e_replica_set_stdout_logging
def test_mongodb_logs_in_stdout(deployed_replica_set: MongoDB):
    """Verify MongoDB JSON logs appear in kubectl logs output."""
    namespace = deployed_replica_set.namespace
    pod_name = f"{deployed_replica_set.name}-0"
    container_name = "mongodb-agent" if is_default_architecture_static() else "mongodb-enterprise-database"

    logs = get_pod_logs(namespace, pod_name, container_name)
    mongodb_logs = get_mongodb_logs(logs)

    assert len(mongodb_logs) > 0, (
        f"Expected MongoDB logs in stdout for pod {namespace}/{pod_name}, "
        f"but found none. Total log lines: {len(logs)}"
    )


@mark.e2e_replica_set_stdout_logging
def test_no_tail_processes(deployed_replica_set: MongoDB):
    """Verify no tail -F processes are running (no file-based log tailing)."""
    namespace = deployed_replica_set.namespace
    pod_name = f"{deployed_replica_set.name}-0"
    container_name = "mongodb-agent" if is_default_architecture_static() else "mongodb-enterprise-database"

    result = KubernetesTester.run_command_in_pod(
        namespace, pod_name, container_name, ["sh", "-c", "pgrep -c 'tail' || echo 0"]
    )
    tail_count = result.strip()

    assert tail_count == "0", (
        f"Expected no tail processes in pod {namespace}/{pod_name}, "
        f"but found {tail_count} tail process(es)"
    )
