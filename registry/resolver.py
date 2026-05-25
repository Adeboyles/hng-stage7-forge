import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

try:
    from .metadata import ArtifactMetadataStore, ArtifactNotFoundError
except ImportError:  # pragma: no cover - supports running from /app without package imports
    from metadata import ArtifactMetadataStore, ArtifactNotFoundError


VERSION_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
COMPARATOR_RE = re.compile(r"^(>=|<=|>|<|=)?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


class ResolverError(Exception):
    """Base error for dependency resolution."""


class InvalidConstraintError(ResolverError, ValueError):
    """Raised when a dependency constraint is not supported."""


class PackageNotFoundError(ResolverError):
    """Raised when a dependency package or matching version does not exist."""


class DependencyCycleError(ResolverError):
    """Raised when the selected dependency graph contains a cycle."""


class ResolutionConflictError(ResolverError):
    """Raised when no version can satisfy all constraints for a package."""


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str) -> "Version":
        match = VERSION_RE.fullmatch(value)
        if not match:
            raise InvalidConstraintError(f"invalid semver version: {value}")
        return cls(*(int(part) for part in match.groups()))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class Comparator:
    op: str
    version: Version

    def satisfies(self, version: Version) -> bool:
        if self.op == "==":
            return version == self.version
        if self.op == ">=":
            return version >= self.version
        if self.op == ">":
            return version > self.version
        if self.op == "<=":
            return version <= self.version
        if self.op == "<":
            return version < self.version
        raise InvalidConstraintError(f"unsupported comparator: {self.op}")


class SemverConstraint:
    """Strict semver constraint parser with exact, caret, tilde, and ranges."""

    def __init__(self, raw: str, comparators: Iterable[Comparator]):
        self.raw = raw.strip()
        self.comparators = tuple(comparators)

    @classmethod
    def parse(cls, constraint: str) -> "SemverConstraint":
        if not isinstance(constraint, str) or not constraint.strip():
            raise InvalidConstraintError("dependency constraint is required")
        raw = constraint.strip()

        if raw.startswith("^"):
            lower = Version.parse(raw[1:])
            upper = Version(lower.major + 1, 0, 0)
            return cls(raw, [Comparator(">=", lower), Comparator("<", upper)])

        if raw.startswith("~"):
            lower = Version.parse(raw[1:])
            upper = Version(lower.major, lower.minor + 1, 0)
            return cls(raw, [Comparator(">=", lower), Comparator("<", upper)])

        parts = raw.split()
        comparators = []
        for part in parts:
            match = COMPARATOR_RE.fullmatch(part)
            if not match:
                raise InvalidConstraintError(f"invalid semver constraint: {raw}")
            op = match.group(1) or "=="
            if op == "=":
                op = "=="
            version = Version(int(match.group(2)), int(match.group(3)), int(match.group(4)))
            comparators.append(Comparator(op, version))

        if not comparators:
            raise InvalidConstraintError("dependency constraint is required")
        return cls(raw, comparators)

    def satisfies(self, version: str | Version) -> bool:
        parsed = Version.parse(version) if isinstance(version, str) else version
        return all(comparator.satisfies(parsed) for comparator in self.comparators)

    def __str__(self) -> str:
        return self.raw


@dataclass(frozen=True)
class ConstraintOrigin:
    package: str
    constraint: SemverConstraint
    path: tuple[str, ...]

    def describe(self) -> str:
        if self.path:
            return f"{self.constraint.raw} via {' -> '.join(self.path)} -> {self.package}"
        return f"{self.constraint.raw} from pipeline -> {self.package}"


@dataclass(frozen=True)
class ResolvedNode:
    name: str
    version: str
    sha256: str
    deps: tuple[dict[str, str], ...]
    resolved_by: tuple[str, ...]


class Lockfile:
    def __init__(self, resolved: dict[str, dict[str, Any]]):
        self.resolved = resolved

    def to_dict(self) -> dict[str, Any]:
        return {"resolved": self.resolved}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


class DependencyResolver:
    """Resolve transitive registry dependencies into a deterministic lockfile."""

    def __init__(self, metadata: ArtifactMetadataStore):
        self.metadata = metadata

    def resolve(self, deps: list[dict[str, str]]) -> Lockfile:
        root_deps = sorted(normalize_requested_deps(deps), key=lambda d: (d["name"], d["version"]))
        constraints = self._root_constraints(root_deps)
        resolved: dict[str, ResolvedNode] = {}
        previous_signature = None

        while True:
            next_resolved: dict[str, ResolvedNode] = {}
            for name in sorted(constraints):
                selected = self.select_version(name, constraints[name])
                deps_tuple = tuple(
                    sorted(selected.deps, key=lambda item: (item["name"], item["version"]))
                )
                next_resolved[name] = ResolvedNode(
                    name=name,
                    version=selected.version,
                    sha256=selected.sha256,
                    deps=deps_tuple,
                    resolved_by=tuple(origin.constraint.raw for origin in constraints[name]),
                )

            self.detect_cycles(next_resolved)
            next_constraints = self._root_constraints(root_deps)
            for name in sorted(next_resolved):
                parent_path = self._path_for(name, next_resolved, constraints)
                for child in next_resolved[name].deps:
                    self._add_constraint(
                        next_constraints,
                        child["name"],
                        child["version"],
                        path=parent_path,
                    )

            signature = self._signature(next_resolved, next_constraints)
            resolved = next_resolved
            constraints = next_constraints
            if signature == previous_signature:
                break
            previous_signature = signature

        return self._lockfile(resolved)

    def _root_constraints(self, deps: list[dict[str, str]]) -> dict[str, list[ConstraintOrigin]]:
        constraints: dict[str, list[ConstraintOrigin]] = {}
        for dep in deps:
            self._add_constraint(
                constraints,
                dep["name"],
                dep["version"],
                path=(),
            )
        return constraints

    def select_version(self, name: str, constraint):
        origins = self._coerce_origins(name, constraint)
        try:
            candidates = self.metadata.list_records(name)
        except ArtifactNotFoundError as exc:
            raise PackageNotFoundError(f"package not found: {name}") from exc

        candidates = sorted(
            candidates,
            key=lambda record: Version.parse(record.version),
            reverse=True,
        )
        if not candidates:
            raise PackageNotFoundError(f"package not found: {name}")

        constraints = [origin.constraint for origin in origins]
        for candidate in candidates:
            if all(constraint.satisfies(candidate.version) for constraint in constraints):
                return candidate

        details = "; ".join(origin.describe() for origin in origins)
        raise ResolutionConflictError(
            f"version conflict for {name}: no published version satisfies {details}"
        )

    def _coerce_origins(self, name: str, constraint) -> list[ConstraintOrigin]:
        if isinstance(constraint, str):
            return [ConstraintOrigin(name, SemverConstraint.parse(constraint), ())]
        if isinstance(constraint, SemverConstraint):
            return [ConstraintOrigin(name, constraint, ())]
        return list(constraint)

    def detect_cycles(self, graph: dict[str, ResolvedNode]) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()
        stack: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                start = stack.index(name)
                cycle = stack[start:] + [name]
                coords = [f"{node}@{graph[node].version}" for node in cycle]
                raise DependencyCycleError(
                    f"dependency cycle detected: {' -> '.join(coords)}"
                )

            visiting.add(name)
            stack.append(name)
            for dep in graph[name].deps:
                child = dep["name"]
                if child in graph:
                    visit(child)
            stack.pop()
            visiting.remove(name)
            visited.add(name)

        for name in sorted(graph):
            visit(name)

    def detect_conflicts(self, graph: dict[str, ResolvedNode]) -> None:
        for name, node in sorted(graph.items()):
            constraints = [SemverConstraint.parse(raw) for raw in node.resolved_by]
            if not all(constraint.satisfies(node.version) for constraint in constraints):
                raise ResolutionConflictError(
                    f"version conflict for {name}: selected {node.version} does not satisfy "
                    + ", ".join(str(c) for c in constraints)
                )

    def _add_constraint(
        self,
        constraints: dict[str, list[ConstraintOrigin]],
        name: str,
        raw_constraint: str,
        *,
        path: tuple[str, ...],
    ) -> bool:
        constraint = SemverConstraint.parse(raw_constraint)
        origin = ConstraintOrigin(name, constraint, path)
        existing = constraints.setdefault(name, [])
        if origin in existing:
            return False
        existing.append(origin)
        existing.sort(key=lambda item: (item.constraint.raw, item.path))
        return True

    def _path_for(
        self,
        name: str,
        resolved: dict[str, ResolvedNode],
        constraints: dict[str, list[ConstraintOrigin]],
    ) -> tuple[str, ...]:
        node = resolved[name]
        origins = constraints.get(name, [])
        parent_paths = sorted(origin.path for origin in origins)
        base = parent_paths[0] if parent_paths else ()
        return base + (f"{name}@{node.version}",)

    def _lockfile(self, resolved: dict[str, ResolvedNode]) -> Lockfile:
        output = {}
        for name in sorted(resolved):
            node = resolved[name]
            output[name] = {
                "version": node.version,
                "sha256": node.sha256,
                "resolved_by": " ".join(sorted(set(node.resolved_by))),
            }
        return Lockfile(output)

    def _signature(
        self,
        resolved: dict[str, ResolvedNode],
        constraints: dict[str, list[ConstraintOrigin]],
    ) -> tuple[Any, ...]:
        selected = tuple(sorted((name, node.version) for name, node in resolved.items()))
        constraint_sig = tuple(
            sorted(
                (
                    name,
                    tuple(sorted((origin.constraint.raw, origin.path) for origin in origins)),
                )
                for name, origins in constraints.items()
            )
        )
        return selected, constraint_sig


def normalize_requested_deps(deps: list[dict[str, str]]) -> list[dict[str, str]]:
    if deps is None:
        return []
    normalized = []
    for dep in deps:
        if not isinstance(dep, dict):
            raise InvalidConstraintError("dependency entries must be objects")
        name = dep.get("name")
        version = dep.get("version")
        if not isinstance(name, str) or not name.strip():
            raise InvalidConstraintError("dependency name is required")
        if not isinstance(version, str) or not version.strip():
            raise InvalidConstraintError("dependency version constraint is required")
        normalized.append({"name": name.strip(), "version": version.strip()})
    return normalized
