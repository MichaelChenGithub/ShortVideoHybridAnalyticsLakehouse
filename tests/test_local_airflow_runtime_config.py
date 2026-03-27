from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKER_COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
LOCAL_RUNTIME_DOC_PATH = (
    REPO_ROOT / "docs" / "architecture" / "batch-analytics" / "local-airflow-dev-runtime.md"
)


class LocalAirflowRuntimeConfigTests(unittest.TestCase):
    def test_airflow_service_uses_docker_socket_without_force_root_user(self) -> None:
        docker_compose = DOCKER_COMPOSE_PATH.read_text()

        self.assertIn('  airflow:\n', docker_compose)
        self.assertNotIn('    user: "0:0"\n', docker_compose)
        self.assertIn("      - /var/run/docker.sock:/var/run/docker.sock\n", docker_compose)
        self.assertIn('    command: >-\n', docker_compose)
        self.assertIn('      airflow db init &&\n', docker_compose)

    def test_local_runtime_doc_mentions_socket_dependency_without_root_runtime(self) -> None:
        runtime_doc = LOCAL_RUNTIME_DOC_PATH.read_text()

        self.assertIn("`/var/run/docker.sock`", runtime_doc)
        self.assertIn("`docker exec`", runtime_doc)
        self.assertNotIn("startup script", runtime_doc)


if __name__ == "__main__":
    unittest.main()
