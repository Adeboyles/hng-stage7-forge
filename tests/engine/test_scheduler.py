from textwrap import dedent
import threading
from types import SimpleNamespace

from engine.parser import parse_pipeline_text
from engine.scheduler import DAGScheduler, find_cycle, topological_batches


def test_topological_batches_groups_independent_jobs_deterministically():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: demo
            version: 1.0.0
            jobs:
              lint:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: lint, run: echo lint }]
              test:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: test, run: echo test }]
              package:
                runtime: alpine:3.18
                needs: [lint, test]
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: package, run: echo package }]
            artifacts:
              - name: demo
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    assert topological_batches(pipeline) == [["lint", "test"], ["package"]]


def test_find_cycle_reports_job_path():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: demo
            version: 1.0.0
            jobs:
              build:
                runtime: alpine:3.18
                needs: [test]
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: build, run: echo build }]
              test:
                runtime: alpine:3.18
                needs: [build]
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: test, run: echo test }]
            artifacts:
              - name: demo
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    assert find_cycle(pipeline) == ["build", "test", "build"]


def test_scheduler_marks_dependents_skipped_after_failure():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: demo
            version: 1.0.0
            jobs:
              lint:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: lint, run: echo lint }]
              package:
                runtime: alpine:3.18
                needs: [lint]
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: package, run: echo package }]
            artifacts:
              - name: demo
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    scheduler = DAGScheduler(pipeline, concurrency_limit=2)

    def executor(job_name, job_definition):
        return "failed" if job_name == "lint" else "succeeded"

    result = scheduler.run(executor)

    assert result.job_statuses["lint"] == "failed"
    assert result.job_statuses["package"] == "skipped"


def test_scheduler_accepts_runner_style_job_results():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: demo
            version: 1.0.0
            jobs:
              build:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps:
                  - name: test
                    run: echo test
                  - name: package
                    run: echo package
            artifacts:
              - name: demo
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    scheduler = DAGScheduler(pipeline, concurrency_limit=1)

    def executor(job_name, job_definition):
        return SimpleNamespace(
            job_name=job_name,
            script=job_definition.to_shell_script(),
            exit_code=0,
            oom_killed=False,
            timed_out=False,
        )

    result = scheduler.run(executor)

    assert result.job_statuses["build"] == "succeeded"
    assert result.executor_results["build"].exit_code == 0


def test_scheduler_runs_independent_jobs_in_parallel_up_to_limit():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: demo
            version: 1.0.0
            jobs:
              lint:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: lint, run: echo lint }]
              test:
                runtime: alpine:3.18
                resources: { cpu: 1.0, memory: 128Mi }
                steps: [{ name: test, run: echo test }]
            artifacts:
              - name: demo
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    scheduler = DAGScheduler(pipeline, concurrency_limit=2)
    lock = threading.Lock()
    release = threading.Event()
    counts: list[int] = []
    started = 0

    def executor(job_name, job_definition):
        nonlocal started
        with lock:
            started += 1
            counts.append(started)
            if started == 2:
                release.set()
        assert release.wait(0.5), "jobs did not overlap in execution"
        with lock:
            started -= 1
        return "succeeded"

    result = scheduler.run(executor)

    assert max(counts) == 2
    assert result.job_statuses == {"lint": "succeeded", "test": "succeeded"}
