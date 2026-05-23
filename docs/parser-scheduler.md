# Parser and Scheduler

This document describes the current CI engine pipeline parser in [engine/parser.py](../engine/parser.py) and DAG scheduler in [engine/scheduler.py](../engine/scheduler.py).

## Overview

The parser and scheduler are the first reusable layer of the CI engine.

- The parser turns pipeline YAML into validated Python models.
- The scheduler consumes those models and plans job execution from `needs`.
- Neither module knows about HTTP, containers, artifact publishing, or log streaming yet.

That separation keeps validation, graph planning, and future runner integration clean.

## Parser

### Entry Points

The parser currently exposes two public entry points:

- `parse_pipeline_file(path: str | Path) -> PipelineDefinition`
- `parse_pipeline_text(text: str) -> PipelineDefinition`

`parse_pipeline_file(...)` reads the file as UTF-8, then delegates to `parse_pipeline_text(...)`.

`parse_pipeline_text(...)` composes YAML, validates the schema, and returns a `PipelineDefinition`.

The rest of the engine should use the returned models instead of raw YAML dictionaries.

### Models

The parser currently defines these dataclasses:

- `DependencySpec`
- `ResourceLimits`
- `StepDefinition`
- `JobDefinition`
- `ArtifactDefinition`
- `PipelineDefinition`

These represent only validated pipeline state.

### Validation Behavior

The parser is strict by design.

- Unknown fields are rejected.
- Missing required fields are rejected.
- Type shape is enforced for mappings, sequences, and scalar values.
- Duplicate fields within the same mapping are rejected.
- Duplicate job names are rejected.
- Duplicate artifact coordinates in one pipeline are rejected.
- Duplicate entries in `needs` are rejected.
- Unknown job dependencies are rejected before scheduling.
- Self-dependencies are rejected.
- `cpu` is parsed as a float.
- `memory` is currently accepted as a string without unit validation.

Validation errors raise `PipelineValidationError`.

### Error Contract

`PipelineValidationError` includes:

- `message`
- `line`
- `column`
- `path`

This makes parser failures usable by both the CLI and the future HTTP API.

Example error shape:

```text
unknown field 'unexpected' at $ (line 4, column 1)
```

### YAML Strategy

The implementation uses `yaml.compose(...)` instead of loading straight into plain Python data first. That gives access to YAML nodes and source marks, which is how the parser reports line and column information for most schema errors.

### Schema Notes

The current parser supports the pipeline structure used in the task:

```yaml
name: build-lib-http
version: 1.0.0
dependencies:
  - name: lib-core
    version: ^1.0.0
jobs:
  build:
    runtime: alpine:3.18
    needs: []
    resources:
      cpu: 1.0
      memory: 512Mi
    steps:
      - name: test
        run: sh ./test.sh
artifacts:
  - name: lib-http
    version: 1.0.0
    path: ./out.tar.gz
```

Current required fields:

- Top level: `name`, `version`, `jobs`, `artifacts`
- Job: `runtime`, `resources`, `steps`
- Resource limits: `cpu`, `memory`
- Step: `name`, `run`
- Artifact: `name`, `version`, `path`

Current optional fields:

- Top level: `dependencies`
- Job: `needs`

### Internal Parsing Flow

The parser validates in this order:

1. Read the file as UTF-8.
2. Delegate to `parse_pipeline_text(...)`.
3. Compose YAML into a node tree with PyYAML.
4. Require a top-level mapping.
5. Validate allowed and required fields by path.
6. Parse nested sections into dataclasses:
   - dependencies
   - jobs
   - resources
   - needs
   - steps
   - artifacts
7. Validate cross-job dependency references after all jobs are parsed.

This split is why some errors are source-aware from the original YAML node while cross-job dependency errors currently use a generic position.

## Scheduler

### Responsibilities

The scheduler works only with parsed `PipelineDefinition` models.

Its current responsibilities are:

- build a graph from `jobs[*].needs`
- detect cycles
- produce deterministic topological batches
- enforce a concurrency limit during execution
- mark downstream jobs as `skipped` after an upstream failure

### Public Functions and Types

The scheduler currently exposes:

- `build_job_graph(pipeline)`
- `find_cycle(pipeline)`
- `topological_batches(pipeline)`
- `DAGScheduler`
- `SchedulerRunResult`
- `SchedulerError`

### Determinism

Determinism is intentional.

- Runnable jobs are processed in lexical job-name order.
- Cycle traversal also uses sorted dependency order.
- The same validated pipeline produces the same batch plan.

That matters for predictable execution, tests, and later lockfile and run-state integration.

### Graph Model

`build_job_graph(...)` returns a mapping of:

- `job_name -> set(of dependency job names)`

This is a dependency graph, not a dependent graph. `topological_batches(...)` derives reverse edges internally when it needs to release downstream jobs.

### Cycle Detection

`find_cycle(...)` performs DFS and returns a concrete cycle path when one exists.

Example:

```text
["build", "test", "build"]
```

`topological_batches(...)` and `DAGScheduler(...)` both reject cyclic pipelines before execution begins.

### Batch Planning

`topological_batches(...)` groups jobs into parallel-ready levels.

Example:

```python
[["lint", "test"], ["package"]]
```

That means `lint` and `test` can run together, and `package` becomes runnable only after both succeed.

### Runtime Coordination

`DAGScheduler.run(executor)` is a thin coordinator around the DAG plan.

- It initializes all jobs as `queued`.
- It processes each topological batch in chunks of `concurrency_limit`.
- It calls `executor(job_name, job_definition)` for runnable jobs.
- It accepts terminal results of `succeeded`, `failed`, or `skipped`.
- If a job fails, downstream queued dependents are marked `skipped`.

This is still synchronous coordination. It is designed so the future runner can provide the actual execution mechanism.

## Example Usage

```python
from engine.parser import parse_pipeline_file
from engine.scheduler import DAGScheduler, topological_batches

pipeline = parse_pipeline_file("pipeline.yaml")
batches = topological_batches(pipeline)

scheduler = DAGScheduler(pipeline, concurrency_limit=2)

def executor(job_name, job_definition):
    print(f"running {job_name} in {job_definition.runtime}")
    return "succeeded"

result = scheduler.run(executor)
print(result.job_statuses)
```

## Current Limitations

The implementation is intentionally narrow for the first engine slice.

- Pipeline models still live in `engine/parser.py` instead of a shared `engine/models.py`.
- Unknown dependency and self-dependency errors use a generic source position instead of the exact `needs` item location.
- Missing-field errors raised by `_require_field(...)` currently use a generic `line 1, column 1` fallback instead of the parent node's exact location.
- Scalar validation is permissive in one specific way: `_require_scalar_string(...)` accepts YAML scalar tags for strings, ints, floats, and bools, then returns their string value.
- The scheduler coordinates synchronously; it does not launch real concurrent workers yet.
- Status handling is intentionally minimal and only covers the current scheduler tests.

## Tests

Focused coverage lives in:

- [tests/engine/test_parser.py](/d:/Programming%20HNG%2014/DevOps/forge/tests/engine/test_parser.py)
- [tests/engine/test_scheduler.py](/d:/Programming%20HNG%2014/DevOps/forge/tests/engine/test_scheduler.py)

These tests currently verify:

- successful parsing into typed models
- strict validation failures
- deterministic topological batching
- cycle detection
- dependent skipping after failure
