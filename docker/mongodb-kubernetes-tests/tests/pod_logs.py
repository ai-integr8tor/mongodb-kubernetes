import json
import logging
from typing import Any, Optional

import kubernetes
from kubetester.kubetester import KubernetesTester, is_default_architecture_static


def parse_json_pod_logs(pod_logs: str) -> list[dict[str, Any]]:
    """Parses pod logs returned as a string and returns list of lines parsed from structured json."""
    lines = pod_logs.strip().split("\n")
    log_lines = []
    for line in lines:
        try:
            log_lines.append(json.loads(line))
        except json.JSONDecodeError as e:
            logging.warning(f"Ignoring the following log line {line} because of {e}")
    return log_lines


def get_agent_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter for automation agent logs (have 'level' and 'msg' fields)."""
    return [log for log in logs if "level" in log and "msg" in log]


def get_mongodb_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter for mongod logs (have 'c' component field)."""
    return [log for log in logs if "c" in log]


def get_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: str,
    api_client: Optional[kubernetes.client.ApiClient] = None,
) -> list[dict[str, Any]]:
    """Read logs from pod_name and return parsed JSON log lines."""
    pod_logs_str = KubernetesTester.read_pod_logs(namespace, pod_name, container_name, api_client=api_client)
    return parse_json_pod_logs(pod_logs_str)


def assert_logs_present_in_stdout(
    namespace: str,
    pod_name: str,
    api_client: Optional[kubernetes.client.ApiClient] = None,
    container_name: str = "mongodb-agent",
):
    """
    Checks that agent and MongoDB logs appear in pod stdout.
    """

    if not is_default_architecture_static():
        container_name = "mongodb-enterprise-database"

    logs = get_pod_logs(namespace, pod_name, container_name, api_client=api_client)

    agent_logs = get_agent_logs(logs)
    mongodb_logs = get_mongodb_logs(logs)

    assert len(agent_logs) > 0, f"pod {namespace}/{pod_name} should have agent logs in stdout"
    assert len(mongodb_logs) > 0, f"pod {namespace}/{pod_name} should have mongodb logs in stdout"
