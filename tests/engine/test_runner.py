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
              allowed_egress:
                - registry-internal:9001
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

    class FakeContainerError(Exception):
        pass

    docker_module.DockerClient = object
    docker_errors.APIError = Exception
    docker_errors.ContainerError = FakeContainerError
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

    class FakeDockerClient:
        def __init__(self):
            self.networks = object()

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


def test_job_runner_creates_ephemeral_internal_network_with_registry_only(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    workspace_base = tmp_path / "workspaces"
    config_path.write_text(
        dedent(
            f"""
            engine:
              log_base: {tmp_path.as_posix()}/logs
              workspace_base: {workspace_base.as_posix()}
            registry:
              internal_host: registry
              port: 9001
            isolation:
              network_name: forge-test-net
              allowed_egress:
                - registry:9001
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

    class FakeContainerError(Exception):
        pass

    docker_module.DockerClient = object
    docker_errors.APIError = FakeAPIError
    docker_errors.ContainerError = FakeContainerError
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

    created = {}
    connected = []
    removed_networks = []
    captured_run = {}

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

    class FakeRegistryContainer:
        id = "registry-1"

    class FakeNetwork:
        def __init__(self, name):
            self.name = name

        def connect(self, container, aliases=None):
            connected.append((container.id, aliases))

        def remove(self):
            removed_networks.append(self.name)

    class FakeNetworks:
        def create(self, **kwargs):
            created.update(kwargs)
            return FakeNetwork(kwargs["name"])

    class FakeContainers:
        def run(self, **kwargs):
            captured_run.update(kwargs)
            return FakeContainer()

        def list(self, filters=None):
            assert filters == {"label": "com.docker.compose.service=registry"}
            return [FakeRegistryContainer()]

    class FakeDockerClient:
        def __init__(self):
            self.networks = FakeNetworks()
            self.containers = FakeContainers()

    runner = runner_module.JobRunner(
        docker_client=FakeDockerClient(),
        token_provider=lambda _run_id: "issued-token",
    )
    result = runner.run(
        runner_module.JobSpec(run_id="run-99", step_name="build", script="echo hi")
    )

    assert created["internal"] is True
    assert created["driver"] == "bridge"
    assert created["name"].startswith("forge-test-net-")
    assert connected == [("registry-1", ["registry"])]
    assert captured_run["network"] == created["name"]
    assert removed_networks == [created["name"]]
    assert result.exit_code == 0


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
              allowed_egress:
                - registry-internal:9001
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

    class FakeContainerError(Exception):
        pass

    docker_module.DockerClient = object
    docker_errors.APIError = FakeAPIError
    docker_errors.ContainerError = FakeContainerError
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

    class FakeRegistryContainer:
        id = "registry-1"

    class FakeNetworks:
        def create(self, **kwargs):
            class FakeNetwork:
                name = kwargs["name"]

                def connect(self, container, aliases=None):
                    return None

                def remove(self):
                    return None

            return FakeNetwork()

    class FakeContainers:
        def run(self, **kwargs):
            captured.update(kwargs)
            return FakeContainer()

        def list(self, filters=None):
            return [FakeRegistryContainer()]

    class FakeDockerClient:
        def __init__(self):
            self.networks = FakeNetworks()
            self.containers = FakeContainers()

    runner = runner_module.JobRunner(
        docker_client=FakeDockerClient(),
        token_provider=lambda _run_id: "issued-token",
    )
    result = runner.run(
        runner_module.JobSpec(
            run_id="run-42",
            step_name="build",
            script="echo hi",
            cpu_limit=0.5,
            memory_limit="256Mi",
        )
    )

    expected_workspace = workspace_base / "run-42"
    assert captured["working_dir"] == "/workspace"
    assert captured["volumes"] == {
        os.path.abspath(expected_workspace): {"bind": "/workspace", "mode": "rw"}
    }
    assert "/workspace" not in captured["tmpfs"]
    assert captured["cpu_quota"] == 50_000
    assert captured["mem_limit"] == 268_435_456
    assert captured["memswap_limit"] == 268_435_456
    assert expected_workspace.exists()
    assert result.exit_code == 0
