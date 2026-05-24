from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

from parser import JobDefinition, PipelineDefinition


TERMINAL_STATUSES = {"succeeded", "failed", "skipped"}


@dataclass(frozen=True)
class SchedulerError(ValueError):
    """Scheduler planning or execution coordination failure."""

    message: str

    def __str__(self) -> str:
        """Return the scheduler error message."""
        return self.message


@dataclass(frozen=True)
class SchedulerRunResult:
    """Final scheduler state after all runnable jobs have been processed."""

    job_statuses: dict[str, str]
    executor_results: dict[str, Any]


def build_job_graph(pipeline: PipelineDefinition) -> dict[str, set[str]]:
    """Build a dependency graph of ``job -> dependencies`` from a pipeline."""
    graph: dict[str, set[str]] = {}
    known_jobs = set(pipeline.jobs)

    for job_name, job in pipeline.jobs.items():
        dependencies = set(job.needs)
        unknown_dependencies = dependencies - known_jobs
        if unknown_dependencies:
            missing = ", ".join(sorted(unknown_dependencies))
            raise SchedulerError(f"job '{job_name}' references unknown dependency(s): {missing}")
        graph[job_name] = dependencies

    return graph


def find_cycle(pipeline: PipelineDefinition) -> list[str] | None:
    """Return a concrete cycle path if the job graph is cyclic."""
    graph = build_job_graph(pipeline)
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def dfs(job_name: str) -> list[str] | None:
        visiting.add(job_name)
        stack.append(job_name)

        for dependency in sorted(graph[job_name]):
            if dependency in visiting:
                start_index = stack.index(dependency)
                return stack[start_index:] + [dependency]
            if dependency in visited:
                continue
            cycle = dfs(dependency)
            if cycle is not None:
                return cycle

        stack.pop()
        visiting.remove(job_name)
        visited.add(job_name)
        return None

    for job_name in sorted(graph):
        if job_name in visited:
            continue
        cycle = dfs(job_name)
        if cycle is not None:
            return cycle

    return None


def topological_batches(pipeline: PipelineDefinition) -> list[list[str]]:
    """Group jobs into deterministic parallel-ready topological batches."""
    graph = build_job_graph(pipeline)
    cycle = find_cycle(pipeline)
    if cycle is not None:
        cycle_path = " -> ".join(cycle)
        raise SchedulerError(f"jobs cycle detected: {cycle_path}")

    dependents: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {job_name: 0 for job_name in graph}

    for job_name, dependencies in graph.items():
        indegree[job_name] = len(dependencies)
        for dependency in dependencies:
            dependents[dependency].add(job_name)

    ready = sorted(job_name for job_name, count in indegree.items() if count == 0)
    batches: list[list[str]] = []

    while ready:
        current_batch = ready
        batches.append(current_batch)
        next_ready: list[str] = []

        for job_name in current_batch:
            for dependent in sorted(dependents[job_name]):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    next_ready.append(dependent)

        ready = sorted(next_ready)

    return batches


class DAGScheduler:
    """Coordinate job execution over a validated DAG with failure propagation."""

    def __init__(self, pipeline: PipelineDefinition, concurrency_limit: int) -> None:
        """Validate scheduler inputs and prepare a reusable job graph."""
        if concurrency_limit < 1:
            raise SchedulerError("concurrency_limit must be at least 1")

        self.pipeline = pipeline
        self.concurrency_limit = concurrency_limit
        self.graph = build_job_graph(pipeline)
        cycle = find_cycle(pipeline)
        if cycle is not None:
            cycle_path = " -> ".join(cycle)
            raise SchedulerError(f"jobs cycle detected: {cycle_path}")

    def run(self, executor: Callable[[str, JobDefinition], Any]) -> SchedulerRunResult:
        """Run jobs batch by batch using an executor callback.

        The executor may return either:

        - a terminal status string: ``succeeded``, ``failed``, or ``skipped``
        - a runner-style object with ``exit_code`` and optional ``timed_out`` /
          ``oom_killed`` attributes
        """
        batches = topological_batches(self.pipeline)
        statuses = {job_name: "queued" for job_name in self.pipeline.jobs}
        executor_results: dict[str, Any] = {}

        for batch in batches:
            for chunk in _chunked(batch, self.concurrency_limit):
                completed: list[tuple[str, str]] = []

                for job_name in chunk:
                    if any(statuses[dependency] != "succeeded" for dependency in self.graph[job_name]):
                        statuses[job_name] = "skipped"
                        continue

                    statuses[job_name] = "running"
                    raw_result = executor(job_name, self.pipeline.jobs[job_name])
                    result = _coerce_executor_status(raw_result)
                    executor_results[job_name] = raw_result
                    statuses[job_name] = result
                    completed.append((job_name, result))

                for job_name, result in completed:
                    if result == "failed":
                        self._mark_descendants_skipped(job_name, statuses)

        return SchedulerRunResult(
            job_statuses=statuses,
            executor_results=executor_results,
        )

    def _mark_descendants_skipped(self, failed_job: str, statuses: dict[str, str]) -> None:
        """Mark still-queued descendants as skipped after a job failure."""
        queue = [failed_job]
        while queue:
            current = queue.pop(0)
            for job_name, dependencies in self.graph.items():
                if current not in dependencies:
                    continue
                if statuses[job_name] == "queued":
                    statuses[job_name] = "skipped"
                    queue.append(job_name)


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """Split a batch into fixed-size execution chunks."""
    return [items[index : index + size] for index in range(0, len(items), size)]


def _coerce_executor_status(result: Any) -> str:
    """Normalize executor output into a scheduler terminal status."""
    if isinstance(result, str):
        if result not in TERMINAL_STATUSES:
            raise SchedulerError(f"executor returned unsupported job status '{result}'")
        return result

    status = getattr(result, "status", None)
    if isinstance(status, str):
        if status not in TERMINAL_STATUSES:
            raise SchedulerError(f"executor returned unsupported job status '{status}'")
        return status

    if hasattr(result, "exit_code"):
        exit_code = int(getattr(result, "exit_code"))
        timed_out = bool(getattr(result, "timed_out", False))
        oom_killed = bool(getattr(result, "oom_killed", False))
        if exit_code == 0 and not timed_out and not oom_killed:
            return "succeeded"
        return "failed"

    raise SchedulerError("executor must return a terminal status or runner-like result")
