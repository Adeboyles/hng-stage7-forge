# Forge — CI/CD Platform with Integrated Artifact Registry

A self-hosted CI/CD platform with an integrated artifact registry.
Two cooperating subsystems with one HTTP API.

**Public URL:** http://YOUR_SERVER_IP

---

## Quick Start

### Fresh VPS Setup

```bash
# 1. Install Docker
sudo apt update -y
sudo apt install -y docker.io docker-compose-plugin git
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ubuntu
newgrp docker

# 2. Clone repo
git clone https://github.com/YOUR_ORG/forge-platform.git
cd forge-platform

# 3. Create data directories
mkdir -p data/logs data/workspaces data/artifacts

# 4. Start the platform
docker compose up -d

# 5. Create first auth token
docker compose exec registry python3 -c "
from registry.auth import AuthManager
import asyncio
async def main():
    auth = AuthManager('data/artifacts/registry.db')
    await auth.init()
    token = await auth.create_token('admin')
    print(f'Token: {token}')
asyncio.run(main())
"

# 6. Login with CLI
pip install -e ./cli
forge login http://YOUR_SERVER_IP
# paste token when prompted
```

---

## Pipeline YAML Schema

```yaml
# Required: pipeline name
name: build-lib-http

# Required: pipeline version
version: 1.0.0

# Optional: dependencies pulled before any job runs
dependencies:
  - name: lib-core          # package name in registry
    version: "^1.0.0"       # semver constraint

# Required: jobs map
jobs:

  # Job name (used in needs: references)
  build:

    # Required: Docker image for this job
    runtime: alpine:3.18

    # Optional: resource limits (defaults shown)
    resources:
      cpu: 1.0              # CPU cores (float)
      memory: 512Mi         # memory limit

    # Optional: wait for other jobs first
    needs: []

    # Required: build steps (run in sequence)
    steps:
      - name: test          # step name (for logs)
        run: "sh ./test.sh" # shell command

      - name: package
        run: "tar czf out.tar.gz src/"

    # Optional: artifacts to publish after job succeeds
    artifacts:
      - name: lib-http      # artifact name in registry
        version: 1.0.0      # semver version
        path: ./out.tar.gz  # path relative to workspace
```

**Validation rules:**
- Unknown fields → error with line number
- Missing required fields → error pointing at field
- Invalid semver version → error
- Cycle in job needs → error naming the cycle
- Version conflict in deps → error showing both paths

---

## Architecture

```
Internet
    │
    ▼
 nginx :80
    │
    ├──► engine :8000   (CI runner, log streaming)
    │
    └──► registry :8001 (artifact storage, resolver)

engine spawns job containers via Docker socket
job containers → forge-internal network → registry only
```

---

## DAG Scheduler

Jobs declare `needs: [other-job]` to express dependencies.
The scheduler:

1. **Builds a directed graph** from needs declarations
2. **Detects cycles** using DFS with a recursion stack before any job runs
3. **Topologically sorts** using Kahn's algorithm
4. **Executes independent jobs in parallel** up to `max_concurrency`
5. **Marks dependents as skipped** (not failed) when a job fails

Example:
```
lint ──┐
       ├──► build ──► publish
test ──┘
```
`lint` and `test` run in parallel.
`build` waits for both.
If `test` fails → `build` is skipped → `publish` is skipped.

---

## Isolation Mechanism

Each job runs in a Docker container with:

| Constraint | How enforced |
|---|---|
| Filesystem | `--read-only` + `--tmpfs /workspace` — host FS invisible |
| Network | `--network forge-internal` — internal only, no internet |
| CPU | `--cpu-quota` via cgroups |
| Memory | `--memory` + `--memory-swap` via cgroups |
| Processes | `--pids-limit 100` — cannot fork-bomb |
| Privileges | `--no-new-privileges` — cannot escalate |

OOM kill produces: `Job killed: memory limit exceeded (512Mi)`

---

## Storage Layer

Content-addressable blob storage:
- Blobs stored at `/artifacts/blobs/<sha256[:2]>/<sha256>`
- Two files with identical content share one blob
- `(name, version)` → `sha256` mapping stored in SQLite
- Second upload to existing `(name, version)` → 409 (immutable)

Race condition handling: SQLite `UNIQUE(name, version)` constraint is atomic.
If two uploads race, the second gets a constraint violation → 409.

---

## Dependency Resolver

Implements semver from scratch:

| Constraint | Meaning |
|---|---|
| `1.0.0` | Exact version |
| `^1.0.0` | `>=1.0.0 <2.0.0` |
| `~1.0.0` | `>=1.0.0 <1.1.0` |
| `>=1.0.0 <2.0.0` | Range |

Resolution is deterministic because:
1. Graph traversal order is alphabetical by package name
2. Version selection always picks highest satisfying version
3. Same registry state + same constraints = identical lockfile

---

## Log Streaming

- Each log line written to disk immediately with `f.flush()`
- Log format: one JSON object per line (`jsonl`)
- SSE endpoint reads line-by-line with file cursor
- Client connecting mid-build receives backlog then new lines
- 50MB logs stream without loading into memory

---

## API Reference

| Method | Path | Description |
|---|---|---|
| POST | `/runs` | Submit pipeline |
| GET | `/runs/{id}` | Get run status |
| GET | `/runs/{id}/lockfile` | Get lockfile |
| GET | `/runs/{id}/logs?follow=true` | Stream logs (SSE) |
| POST | `/artifacts/{name}/{version}` | Upload artifact |
| GET | `/artifacts/{name}/{version}` | Download artifact |
| GET | `/artifacts/{name}/{version}/meta` | Get metadata |
| GET | `/artifacts/{name}` | List versions |

All write operations require `Authorization: Bearer <token>`

---

## CLI Reference

```bash
forge login <url>                              # save credentials
forge run <pipeline.yaml>                      # run pipeline
forge logs <run-id> [--follow]                 # view logs
forge publish <path> --name <n> --version <v>  # publish artifact
forge resolve <pipeline.yaml>                  # print lockfile
forge ls <package>                             # list versions
```

---

## Slack Alerts

All alerts route to the configured Slack webhook.

Events:
- Pipeline started / succeeded / failed
- Integrity failure (with both SHA-256 hashes)
- Resolution failure (with conflict details)

Configure in `config.yaml`:
```yaml
slack:
  webhook_url: "https://hooks.slack.com/services/..."
```

*[Screenshot of Slack alerts here]*

---

## Required Capabilities Test Results

| # | Capability | Result |
|---|---|---|
| 1 | Build + publish lib-core@1.0.0 | ✅ |
| 2 | Resolve ^1.0.0, publish lib-http@1.0.0 | ✅ |
| 3 | Resolve both, publish service-api@0.1.0 | ✅ |
| 4 | Wrong checksum → 400 | ✅ |
| 5 | Duplicate upload → 409 | ✅ |
| 6 | Version conflict → fail before build | ✅ |
| 7 | Filesystem escape, OOM, network egress → all blocked | ✅ |
| 8 | 50MB log stream → live, not buffered | ✅ |