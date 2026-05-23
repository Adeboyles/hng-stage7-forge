from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


@dataclass(frozen=True)
class DependencySpec:
    name: str
    version: str


@dataclass(frozen=True)
class ResourceLimits:
    cpu: float
    memory: str


@dataclass(frozen=True)
class StepDefinition:
    name: str
    run: str


@dataclass(frozen=True)
class JobDefinition:
    name: str
    runtime: str
    resources: ResourceLimits
    steps: tuple[StepDefinition, ...]
    needs: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArtifactDefinition:
    name: str
    version: str
    path: str


@dataclass(frozen=True)
class PipelineDefinition:
    name: str
    version: str
    dependencies: tuple[DependencySpec, ...]
    jobs: dict[str, JobDefinition]
    artifacts: tuple[ArtifactDefinition, ...]


@dataclass(frozen=True)
class PipelineValidationError(ValueError):
    message: str
    line: int
    column: int
    path: str

    def __str__(self) -> str:
        location = f"line {self.line}, column {self.column}"
        if self.path:
            return f"{self.message} at {self.path} ({location})"
        return f"{self.message} ({location})"


def parse_pipeline_file(path: str | Path) -> PipelineDefinition:
    return parse_pipeline_text(Path(path).read_text(encoding="utf-8"))


def parse_pipeline_text(text: str) -> PipelineDefinition:
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        line = (mark.line + 1) if mark else 1
        column = (mark.column + 1) if mark else 1
        problem = getattr(exc, "problem", "invalid YAML")
        raise PipelineValidationError(problem, line, column, "$") from exc

    if root is None:
        raise PipelineValidationError("pipeline definition is empty", 1, 1, "$")

    root_mapping = _require_mapping(root, "$")
    data = _mapping_to_node_map(root_mapping, "$")

    name = _require_string_field(data, "name", "$")
    version = _require_string_field(data, "version", "$")
    dependencies = _parse_dependencies(data.get("dependencies"), "dependencies")
    jobs = _parse_jobs(data, "jobs")
    artifacts = _parse_artifacts(data, "artifacts")
    _validate_job_dependencies(jobs)

    return PipelineDefinition(
        name=name,
        version=version,
        dependencies=dependencies,
        jobs=jobs,
        artifacts=artifacts,
    )


def _parse_dependencies(node: Node | None, path: str) -> tuple[DependencySpec, ...]:
    if node is None:
        return ()

    items: list[DependencySpec] = []
    for index, item_node in enumerate(_require_sequence(node, path).value):
        item_path = f"{path}[{index}]"
        item_map = _mapping_to_node_map(
            _require_mapping(item_node, item_path), item_path
        )
        name = _require_string_field(item_map, "name", item_path)
        version = _require_string_field(item_map, "version", item_path)
        items.append(DependencySpec(name=name, version=version))
    return tuple(items)


def _parse_jobs(
    root_fields: dict[str, Node], field_name: str
) -> dict[str, JobDefinition]:
    jobs_node = _require_field(root_fields, field_name, "$")
    jobs_mapping = _require_mapping(jobs_node, field_name)

    jobs: dict[str, JobDefinition] = {}
    for key_node, value_node in jobs_mapping.value:
        job_name = _require_scalar_string(key_node, field_name)
        job_path = f"{field_name}.{job_name}"
        if job_name in jobs:
            raise _error(key_node, job_path, f"duplicate job name '{job_name}'")

        job_fields = _mapping_to_node_map(
            _require_mapping(value_node, job_path), job_path
        )
        runtime = _require_string_field(job_fields, "runtime", job_path)
        resources = _parse_resources(
            _require_field(job_fields, "resources", job_path), f"{job_path}.resources"
        )
        needs = _parse_needs(job_fields.get("needs"), f"{job_path}.needs")
        steps = _parse_steps(
            _require_field(job_fields, "steps", job_path), f"{job_path}.steps"
        )
        jobs[job_name] = JobDefinition(
            name=job_name,
            runtime=runtime,
            resources=resources,
            steps=steps,
            needs=needs,
        )

    return jobs


def _parse_resources(node: Node, path: str) -> ResourceLimits:
    fields = _mapping_to_node_map(_require_mapping(node, path), path)
    cpu_node = _require_field(fields, "cpu", path)
    memory = _require_string_field(fields, "memory", path)

    cpu_value = _parse_float(cpu_node, f"{path}.cpu")
    return ResourceLimits(cpu=cpu_value, memory=memory)


def _parse_needs(node: Node | None, path: str) -> tuple[str, ...]:
    if node is None:
        return ()

    names: list[str] = []
    seen: set[str] = set()
    for index, item_node in enumerate(_require_sequence(node, path).value):
        item_path = f"{path}[{index}]"
        job_name = _require_scalar_string(item_node, item_path)
        if job_name in seen:
            raise _error(
                item_node, item_path, f"duplicate dependency '{job_name}' in needs"
            )
        seen.add(job_name)
        names.append(job_name)
    return tuple(names)


def _parse_steps(node: Node, path: str) -> tuple[StepDefinition, ...]:
    steps: list[StepDefinition] = []
    for index, item_node in enumerate(_require_sequence(node, path).value):
        item_path = f"{path}[{index}]"
        fields = _mapping_to_node_map(_require_mapping(item_node, item_path), item_path)
        name = _require_string_field(fields, "name", item_path)
        run = _require_string_field(fields, "run", item_path)
        steps.append(StepDefinition(name=name, run=run))
    return tuple(steps)


def _parse_artifacts(
    root_fields: dict[str, Node], field_name: str
) -> tuple[ArtifactDefinition, ...]:
    artifacts_node = _require_field(root_fields, field_name, "$")
    artifacts: list[ArtifactDefinition] = []
    seen: set[tuple[str, str]] = set()

    for index, item_node in enumerate(
        _require_sequence(artifacts_node, field_name).value
    ):
        item_path = f"{field_name}[{index}]"
        fields = _mapping_to_node_map(_require_mapping(item_node, item_path), item_path)
        name = _require_string_field(fields, "name", item_path)
        version = _require_string_field(fields, "version", item_path)
        artifact_path = _require_string_field(fields, "path", item_path)

        coordinate = (name, version)
        if coordinate in seen:
            raise _error(
                item_node,
                item_path,
                f"duplicate artifact coordinate '{name}@{version}'",
            )
        seen.add(coordinate)
        artifacts.append(
            ArtifactDefinition(name=name, version=version, path=artifact_path)
        )

    return tuple(artifacts)


def _validate_job_dependencies(jobs: dict[str, JobDefinition]) -> None:
    known_jobs = set(jobs)
    for job_name, job in jobs.items():
        for dependency in job.needs:
            if dependency == job_name:
                raise PipelineValidationError(
                    f"job '{job_name}' cannot depend on itself",
                    1,
                    1,
                    f"jobs.{job_name}.needs",
                )
            if dependency not in known_jobs:
                raise PipelineValidationError(
                    f"job '{job_name}' references unknown dependency '{dependency}'",
                    1,
                    1,
                    f"jobs.{job_name}.needs",
                )


def _mapping_to_node_map(node: MappingNode, path: str) -> dict[str, Node]:
    result: dict[str, Node] = {}
    allowed = _allowed_fields_for_path(path)

    for key_node, value_node in node.value:
        key = _require_scalar_string(key_node, path)
        if key in result:
            raise _error(key_node, path, f"duplicate field '{key}'")
        if allowed is not None and key not in allowed:
            raise _error(key_node, path, f"unknown field '{key}'")
        result[key] = value_node

    if allowed is not None:
        for required_key in _required_fields_for_path(path):
            if required_key not in result:
                raise _error(
                    node,
                    f"{path}.{required_key}" if path != "$" else required_key,
                    f"missing required field '{required_key}'",
                )

    return result


def _allowed_fields_for_path(path: str) -> set[str] | None:
    if path == "$":
        return {"name", "version", "dependencies", "jobs", "artifacts"}
    if path.startswith("dependencies["):
        return {"name", "version"}
    if path == "jobs":
        return None
    if (
        path.startswith("jobs.")
        and ".steps[" not in path
        and not path.endswith(".resources")
        and not path.endswith(".needs")
    ):
        return {"runtime", "resources", "steps", "needs"}
    if path.endswith(".resources"):
        return {"cpu", "memory"}
    if ".steps[" in path:
        return {"name", "run"}
    if path.startswith("artifacts["):
        return {"name", "version", "path"}
    return None


def _required_fields_for_path(path: str) -> set[str]:
    if path == "$":
        return {"name", "version", "jobs", "artifacts"}
    if path.startswith("dependencies["):
        return {"name", "version"}
    if (
        path.startswith("jobs.")
        and ".steps[" not in path
        and not path.endswith(".resources")
        and not path.endswith(".needs")
    ):
        return {"runtime", "resources", "steps"}
    if path.endswith(".resources"):
        return {"cpu", "memory"}
    if ".steps[" in path:
        return {"name", "run"}
    if path.startswith("artifacts["):
        return {"name", "version", "path"}
    return set()


def _require_field(fields: dict[str, Node], field_name: str, path: str) -> Node:
    if field_name not in fields:
        suffix = f"{path}.{field_name}" if path != "$" else field_name
        raise _error_from_position(
            1, 1, suffix, f"missing required field '{field_name}'"
        )
    return fields[field_name]


def _require_string_field(fields: dict[str, Node], field_name: str, path: str) -> str:
    field_node = _require_field(fields, field_name, path)
    suffix = f"{path}.{field_name}" if path != "$" else field_name
    return _require_scalar_string(field_node, suffix)


def _require_mapping(node: Node, path: str) -> MappingNode:
    if not isinstance(node, MappingNode):
        raise _error(node, path, "expected mapping")
    return node


def _require_sequence(node: Node, path: str) -> SequenceNode:
    if not isinstance(node, SequenceNode):
        raise _error(node, path, "expected sequence")
    return node


def _require_scalar_string(node: Node, path: str) -> str:
    if not isinstance(node, ScalarNode):
        raise _error(node, path, "expected scalar string")
    if node.tag not in (
        "tag:yaml.org,2002:str",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
        "tag:yaml.org,2002:bool",
    ):
        raise _error(node, path, "expected scalar value")
    return node.value


def _parse_float(node: Node, path: str) -> float:
    if not isinstance(node, ScalarNode):
        raise _error(node, path, "expected numeric cpu value")
    try:
        return float(node.value)
    except ValueError as exc:
        raise _error(node, path, "expected numeric cpu value") from exc


def _error(node: Node, path: str, message: str) -> PipelineValidationError:
    return PipelineValidationError(
        message=message,
        line=node.start_mark.line + 1,
        column=node.start_mark.column + 1,
        path=path,
    )


def _error_from_position(
    line: int, column: int, path: str, message: str
) -> PipelineValidationError:
    return PipelineValidationError(message=message, line=line, column=column, path=path)
