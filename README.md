# Forge — CI/CD Platform with Integrated Artifact Registry

Forge is a self-hosted CI/CD platform with an integrated artifact registry.
It has two cooperating subsystems behind one HTTP surface:

- a CI engine that parses YAML pipelines, resolves dependencies, runs jobs in isolated containers, streams logs over SSE, and reports run status
- an artifact registry and dependency resolver that stores immutable artifacts, verifies checksums, and produces deterministic lockfiles

**Public URL:** `http://YOUR_SERVER_IP`

## Status

The current codebase implements:

- registry HTTP/core alignment
- strict pipeline parsing and DAG scheduling
- run status, lockfile, and SSE log endpoints
- shared per-run workspaces
- dependency pull into `./deps/<name>/` with pull-time SHA-256 verification
- automatic artifact publishing after successful runs
- host-side token creation, listing, and revocation with hashed storage
- per-run internal job networks with registry-only reachable egress

## Fresh VPS Setup

```bash
# 1. Install Docker and Git
sudo apt update -y
sudo apt install -y docker.io docker-compose-plugin git python3 python3-pip
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker "$USER"
newgrp docker

# 2. Clone the repo
git clone https://github.com/YOUR_ORG/forge-platform.git
cd forge-platform

# 3. Create persistent data directories
mkdir -p data/logs data/workspaces data/artifacts

# 4. Install the host CLIs
python3 -m pip install -e .

# 5. Start the platform
docker compose up -d

# 6. Create the first auth token on the host
forge-token create admin

# 7. Login with the user CLI
forge login http://YOUR_SERVER_IP
# paste the token printed by forge-token create
```

Environment variables should only handle environment-specific concerns such as
locating the config file. Platform settings such as ports, workspace paths,
network names, and registry endpoints belong in `config.yaml`.

## Pipeline YAML Schema

Annotated example:

```yaml
# Required: pipeline name
name: build-lib-http

# Required: pipeline version
version: 1.0.0

# Optional: dependency constraints resolved before any build job runs
dependencies:
  - name: lib-core
    version: "^1.0.0"

# Required: jobs map
jobs:
  build:
    # Required: container image
    runtime: alpine:3.18

    # Required: per-job resource limits
    resources:
      cpu: 1.0
      memory: 512Mi

    # Optional: DAG edges
    needs: []

    # Required: ordered shell steps
    steps:
      - name: test
        run: "sh ./test.sh"
      - name: package
        run: "tar czf out.tar.gz src/"

# Optional: top-level artifacts published after the pipeline succeeds
artifacts:
  - name: lib-http
    version: 1.0.0
    path: ./out.tar.gz
```

Validation behavior:

- unknown fields fail validation
- missing required fields fail validation
- YAML parse/shape errors are reported with source location
- duplicate artifact coordinates fail validation
- job cycles fail before any execution starts

Runtime behavior:

- all jobs in the same pipeline share one workspace mounted at `/workspace`
- resolved dependencies are materialized before any job runs at `/workspace/deps/<name>/`
- `FORGE_TOKEN` and `FORGE_URL` are injected into each job container
- top-level declared artifacts are published automatically after a successful run

## Architecture

```text
Internet
   |
   v
nginx :80
   |
   +--> engine   :8000
   |
   +--> registry :8001

engine -> docker socket -> job containers
job containers -> per-run internal network -> registry
```

## DAG Scheduler

Jobs use `needs: [...]` to declare dependencies. The scheduler:

1. Builds a directed graph from job dependencies.
2. Detects cycles before any execution starts.
3. Produces deterministic topological batches with lexical ordering.
4. Runs independent jobs in parallel up to `engine.max_concurrency`.
5. Marks downstream jobs as `skipped` if an upstream dependency fails.

Implementation notes:

- graph validation and cycle detection live in `engine/scheduler.py`
- parallel execution uses the Python standard library, not a workflow engine
- scheduler ordering is deterministic for the same pipeline definition

## Isolation Mechanism

Each job runs in a Docker container with:

| Constraint | Current enforcement |
|---|---|
| Filesystem | read-only root filesystem plus bind-mounted shared `/workspace` |
| Workspace sharing | per-run host-backed workspace bind-mounted at `/workspace` |
| Temporary files | tmpfs at `/tmp` |
| Network | per-run internal Docker network with only the registry service attached |
| CPU | `cpu_period` + `cpu_quota` |
| Memory | `mem_limit` + `memswap_limit` |
| Processes | `pids_limit=100` |
| Privileges | `cap_drop=["ALL"]` + `no-new-privileges` |

## Storage Layer

Registry storage is content-addressed:

- blobs are stored under `/data/artifacts/<sha256[:2]>/<sha256>`
- metadata is stored in SQLite
- `(name, version)` is immutable
- duplicate publishes return `409`
- server-side checksum mismatches return `400`

Two pipelines racing to publish the same `(name, version)` are handled by the
database uniqueness constraint on `(name, version)`. The first publish wins and
the second publish receives a conflict error.

## Dependency Resolver

The resolver is implemented in `registry/resolver.py` and supports:

- exact versions: `1.0.0`
- caret ranges: `^1.0.0`
- tilde ranges: `~1.0.0`
- comparator ranges: `>=1.0.0 <2.0.0`

Resolver behavior:

- walks transitive dependency metadata from the registry
- selects the highest published version satisfying all active constraints
- detects version conflicts with path-aware error messages
- detects dependency cycles with explicit cycle paths
- emits deterministic lockfiles containing exact versions and SHA-256 hashes

Why selection is deterministic:

1. requested dependencies are normalized and sorted
2. package resolution iterates names in sorted order
3. candidate versions are sorted and the highest satisfying version is chosen
4. lockfile JSON is emitted in stable key order

## Log Streaming

Logs are persisted as NDJSON and streamed over SSE.

Behavior:

- each log line is written to disk as a JSON object with timestamp, job, and line
- clients connecting mid-run receive backlog first, then live updates
- the log reader streams line-by-line instead of buffering the whole file
- dependency preparation logs and job logs share the same run log
- the engine appends a single run-level EOF marker when the run reaches a terminal state

This keeps large logs streamable without loading them into memory.

## HTTP API

| Method | Path | Description |
|---|---|---|
| `POST` | `/runs` | submit pipeline |
| `GET` | `/runs/{id}` | get run status |
| `GET` | `/runs/{id}/lockfile` | get resolved lockfile |
| `GET` | `/runs/{id}/logs?follow=true` | stream logs over SSE |
| `POST` | `/artifacts/{name}/{version}` | upload artifact |
| `GET` | `/artifacts/{name}/{version}` | download artifact |
| `GET` | `/artifacts/{name}/{version}/meta` | read artifact metadata |
| `GET` | `/artifacts/{name}` | list versions |

All write operations require:

```text
Authorization: Bearer <token>
```

Run status values:

- `queued`
- `running`
- `succeeded`
- `failed`
- `integrity_failure`
- `conflict_failure`
- `cycle_failure`

## CLI Reference

User CLI:

```bash
forge login <url>
forge run <pipeline.yaml>
forge logs <run-id> [--follow]
forge publish <path> --name <n> --version <v>
forge resolve <pipeline.yaml>
forge ls <package>
```

Host admin CLI:

```bash
forge-token create <name>
forge-token list
forge-token revoke <name>
```

## Slack Alerts

Configured through the `webhook_url` field of `config.yaml`.

Implemented alert types:

- pipeline started
- pipeline succeeded
- pipeline failed
- integrity failure
- resolution failure

*[Slack alerts screenshot here]*

## Current Verification Snapshot

Targeted test suites currently passing:

```bash
python -m pytest tests -q
```

Observed result during the latest update:

- `58 passed`

