from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

from engine.parser import JobDefinition, PipelineDefinition


TERMINAL_STATUSES = {"succeeded", "failed", "skipped"}


@dataclass(frozen=True)
class SchedulerError(ValueError):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class SchedulerRunResult:
    job_statuses: dict[str, str]


def build_job_graph(pipeline: PipelineDefinition) -> dict[str, set[str]]:
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
    def __init__(self, pipeline: PipelineDefinition, concurrency_limit: int) -> None:
        if concurrency_limit < 1:
            raise SchedulerError("concurrency_limit must be at least 1")

        self.pipeline = pipeline
        self.concurrency_limit = concurrency_limit
        self.graph = build_job_graph(pipeline)
        cycle = find_cycle(pipeline)
        if cycle is not None:
            cycle_path = " -> ".join(cycle)
            raise SchedulerError(f"jobs cycle detected: {cycle_path}")

    def run(self, executor: Callable[[str, JobDefinition], str]) -> SchedulerRunResult:
        batches = topological_batches(self.pipeline)
        statuses = {job_name: "queued" for job_name in self.pipeline.jobs}

        for batch in batches:
            for chunk in _chunked(batch, self.concurrency_limit):
                completed: list[tuple[str, str]] = []

                for job_name in chunk:
                    if any(statuses[dependency] != "succeeded" for dependency in self.graph[job_name]):
                        statuses[job_name] = "skipped"
                        continue

                    statuses[job_name] = "running"
                    result = executor(job_name, self.pipeline.jobs[job_name])
                    if result not in TERMINAL_STATUSES - {"skipped"} | {"succeeded", "failed"}:
                        raise SchedulerError(f"executor returned unsupported job status '{result}'")
                    statuses[job_name] = result
                    completed.append((job_name, result))

                for job_name, result in completed:
                    if result == "failed":
                        self._mark_descendants_skipped(job_name, statuses)

        return SchedulerRunResult(job_statuses=statuses)

    def _mark_descendants_skipped(self, failed_job: str, statuses: dict[str, str]) -> None:
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
    return [items[index : index + size] for index in range(0, len(items), size)]
