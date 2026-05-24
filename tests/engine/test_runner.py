from __future__ import annotations

import importlib
import os
import sys
from types import ModuleType
from textwrap import dedent


def test_job_runner_uses_configured_network_and_registry_url(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        dedent(
            """
            engine:
              log_base: /tmp/forge-logs
            registry:
              internal_host: registry-internal
              port: 9001
            isolation:
              network_name: forge-test-net
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FORGE_CONFIG", str(config_path))

    docker_module = ModuleType("docker")
    docker_errors = ModuleType("docker.errors")
    docker_models = ModuleType("docker.models")
    docker_models_containers = ModuleType("docker.models.containers")

    class FakeNotFound(Exception):
        pass

    docker_module.DockerClient = object
    docker_errors.APIError = Exception
    docker_errors.ContainerError = Exception
    docker_errors.ImageNotFound = Exception
    docker_errors.NotFound = FakeNotFound
    docker_models_containers.Container = object

    monkeypatch.setitem(sys.modules, "docker", docker_module)
    monkeypatch.setitem(sys.modules, "docker.errors", docker_errors)
    monkeypatch.setitem(sys.modules, "docker.models", docker_models)
    monkeypatch.setitem(sys.modules, "docker.models.containers", docker_models_containers)

    config_module = importlib.import_module("engine.config")
    importlib.reload(config_module)
    runner_module = importlib.import_module("engine.runner")
    importlib.reload(runner_module)
    config_module.reset_config_cache()

    class FakeNetworks:
        def get(self, name):
            assert name == "forge-test-net"
            return object()

    class FakeDockerClient:
        def __init__(self):
            self.networks = FakeNetworks()

    runner = runner_module.JobRunner(
        docker_client=FakeDockerClient(),
        token_provider=lambda _run_id: "issued-token",
    )
    env = runner._build_env(
        runner_module.JobSpec(run_id="run-1", step_name="build", script="echo hi")
    )

    assert runner.network_name == "forge-test-net"
    assert env["FORGE_URL"] == "http://registry-internal:9001"
    assert env["FORGE_TOKEN"] == "issued-token"


def test_job_runner_mounts_shared_workspace_for_run(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    workspace_base = tmp_path / "workspaces"
    config_path.write_text(
        dedent(
            f"""
            engine:
              log_base: {tmp_path.as_posix()}/logs
              workspace_base: {workspace_base.as_posix()}
            registry:
              internal_host: registry-internal
              port: 9001
            isolation:
              network_name: forge-test-net
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FORGE_CONFIG", str(config_path))

    docker_module = ModuleType("docker")
    docker_errors = ModuleType("docker.errors")
    docker_models = ModuleType("docker.models")
    docker_models_containers = ModuleType("docker.models.containers")

    class FakeNotFound(Exception):
        pass

    class FakeAPIError(Exception):
        pass

    docker_module.DockerClient = object
    docker_errors.APIError = FakeAPIError
    docker_errors.ContainerError = Exception
    docker_errors.ImageNotFound = Exception
    docker_errors.NotFound = FakeNotFound
    docker_models_containers.Container = object

    monkeypatch.setitem(sys.modules, "docker", docker_module)
    monkeypatch.setitem(sys.modules, "docker.errors", docker_errors)
    monkeypatch.setitem(sys.modules, "docker.models", docker_models)
    monkeypatch.setitem(sys.modules, "docker.models.containers", docker_models_containers)

    config_module = importlib.import_module("engine.config")
    importlib.reload(config_module)
    runner_module = importlib.import_module("engine.runner")
    importlib.reload(runner_module)
    config_module.reset_config_cache()

    captured = {}

    class FakeContainer:
        attrs = {"State": {"OOMKilled": False}}

        def logs(self, **kwargs):
            return iter([b"hello from build\n"])

        def wait(self, timeout=None):
            return {"StatusCode": 0}

        def reload(self):
            return None

        def remove(self, force=False):
            return None

    class FakeNetworks:
        def get(self, name):
            assert name == "forge-test-net"
            return object()

    class FakeContainers:
        def run(self, **kwargs):
            captured.update(kwargs)
            return FakeContainer()

    class FakeDockerClient:
        def __init__(self):
            self.networks = FakeNetworks()
            self.containers = FakeContainers()

    runner = runner_module.JobRunner(
        docker_client=FakeDockerClient(),
        token_provider=lambda _run_id: "issued-token",
    )
    result = runner.run(
        runner_module.JobSpec(run_id="run-42", step_name="build", script="echo hi")
    )

    expected_workspace = workspace_base / "run-42"
    assert captured["working_dir"] == "/workspace"
    assert captured["volumes"] == {
        os.path.abspath(expected_workspace): {"bind": "/workspace", "mode": "rw"}
    }
    assert "/workspace" not in captured["tmpfs"]
    assert expected_workspace.exists()
    assert result.exit_code == 0
