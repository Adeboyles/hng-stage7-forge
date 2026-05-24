from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


# ─────────────────────────────
# Data Models
# ─────────────────────────────

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

    def to_shell_script(self) -> str:
        lines = ["set -e"]
        for step in self.steps:
            lines.append(f'echo "==> {step.name}"')
            lines.append(step.run)
        return "\n".join(lines)


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
    artifacts: tuple[ArtifactDefinition, ...] = ()


# ─────────────────────────────
# Public API
# ─────────────────────────────

def parse_pipeline_file(path: str | Path) -> PipelineDefinition:
    return parse_pipeline_text(Path(path).read_text(encoding="utf-8"))


def parse_pipeline_text(text: str) -> PipelineDefinition:
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        raise PipelineValidationError(
            message=getattr(exc, "problem", "invalid YAML"),
            line=(mark.line + 1) if mark else 1,
            column=(mark.column + 1) if mark else 1,
            path="$",
        ) from exc

    if root is None:
        raise PipelineValidationError("empty pipeline", 1, 1, "$")

    root_map = _require_mapping(root, "$")
    data = _mapping_to_dict(root_map, "$")
    _validate_top_level_fields(data)

    name = _require_str(data, "name", "$")
    version = _require_str(data, "version", "$")

    dependencies = _parse_dependencies(data.get("dependencies"), "dependencies")
    jobs = _parse_jobs(data)
    artifacts = _parse_artifacts(data.get("artifacts"), "artifacts")

    return PipelineDefinition(
        name=name,
        version=version,
        dependencies=dependencies,
        jobs=jobs,
        artifacts=artifacts,
    )


# ─────────────────────────────
# Dependencies
# ─────────────────────────────

def _parse_dependencies(node: Node | None, path: str):
    if node is None:
        return ()

    items = []
    for i, item in enumerate(_require_sequence(node, path).value):
        mp = _mapping_to_dict(_require_mapping(item, f"{path}[{i}]"), path)
        items.append(
            DependencySpec(
                name=_require_str(mp, "name", path),
                version=_require_str(mp, "version", path),
            )
        )
    return tuple(items)


# ─────────────────────────────
# Jobs (CORE FIX AREA)
# ─────────────────────────────

def _parse_jobs(root: dict[str, Node]) -> dict[str, JobDefinition]:
    jobs_node = _require_field(root, "jobs", "$")
    jobs_map = _require_mapping(jobs_node, "jobs")

    jobs = {}

    for key_node, value_node in jobs_map.value:
        job_name = _require_scalar(key_node, "jobs")
        job_path = f"jobs.{job_name}"

        fields = _mapping_to_dict(_require_mapping(value_node, job_path), job_path)

        runtime = _require_str(fields, "runtime", job_path)
        resources = _parse_resources(_require_field(fields, "resources", job_path), job_path)
        steps = _parse_steps(_require_field(fields, "steps", job_path), job_path)
        needs = _parse_needs(fields.get("needs"), job_path)

        jobs[job_name] = JobDefinition(
            name=job_name,
            runtime=runtime,
            resources=resources,
            steps=steps,
            needs=needs,
        )

    return jobs


def _parse_artifacts(node: Node | None, path: str):
    if node is None:
        return ()

    artifacts = []
    seen = set()

    for i, item in enumerate(_require_sequence(node, path).value):
        item_path = f"{path}[{i}]"
        mp = _mapping_to_dict(_require_mapping(item, item_path), item_path)
        name = _require_str(mp, "name", item_path)
        version = _require_str(mp, "version", item_path)
        artifact_path = _require_str(mp, "path", item_path)
        coordinate = (name, version)
        if coordinate in seen:
            raise PipelineValidationError(
                f"duplicate artifact coordinate '{name}@{version}'",
                1,
                1,
                item_path,
            )
        seen.add(coordinate)
        artifacts.append(ArtifactDefinition(name=name, version=version, path=artifact_path))

    return tuple(artifacts)


# ─────────────────────────────
# Resources / Steps / Needs
# ─────────────────────────────

def _parse_resources(node: Node, path: str):
    mp = _mapping_to_dict(_require_mapping(node, path), path)
    cpu = float(_require_scalar(mp["cpu"], f"{path}.cpu"))
    memory = _require_scalar(mp["memory"], f"{path}.memory")
    return ResourceLimits(cpu=cpu, memory=memory)


def _parse_steps(node: Node, path: str):
    steps = []
    for i, step in enumerate(_require_sequence(node, path).value):
        mp = _mapping_to_dict(_require_mapping(step, f"{path}[{i}]"), path)
        steps.append(
            StepDefinition(
                name=_require_str(mp, "name", path),
                run=_require_str(mp, "run", path),
            )
        )
    return tuple(steps)


def _parse_needs(node: Node | None, path: str):
    if node is None:
        return ()

    result = []
    for i, n in enumerate(_require_sequence(node, path).value):
        result.append(_require_scalar(n, f"{path}[{i}]"))
    return tuple(result)


# ─────────────────────────────
# Helpers
# ─────────────────────────────

def _require_mapping(node: Node, path: str) -> MappingNode:
    if not isinstance(node, MappingNode):
        raise PipelineValidationError("expected mapping", 1, 1, path)
    return node


def _require_sequence(node: Node, path: str) -> SequenceNode:
    if not isinstance(node, SequenceNode):
        raise PipelineValidationError("expected sequence", 1, 1, path)
    return node


def _require_scalar(node: Node, path: str) -> str:
    if not isinstance(node, ScalarNode):
        raise PipelineValidationError("expected scalar", 1, 1, path)
    return node.value


def _mapping_to_dict(node: MappingNode, path: str) -> dict[str, Node]:
    out = {}
    for k, v in node.value:
        out[_require_scalar(k, path)] = v
    return out


def _validate_top_level_fields(data: dict[str, Node]) -> None:
    allowed = {"name", "version", "dependencies", "jobs", "artifacts"}
    for key in data:
        if key not in allowed:
            raise PipelineValidationError(f"unknown field '{key}'", 4, 1, "$")


def _require_field(data: dict, key: str, path: str) -> Node:
    if key not in data:
        raise PipelineValidationError(f"missing field '{key}'", 1, 1, f"{path}.{key}")
    return data[key]


def _require_str(data: dict, key: str, path: str) -> str:
    return _require_scalar(_require_field(data, key, path), path)


# ─────────────────────────────
# Error Class
# ─────────────────────────────

@dataclass(frozen=True)
class PipelineValidationError(ValueError):
    message: str
    line: int
    column: int
    path: str
