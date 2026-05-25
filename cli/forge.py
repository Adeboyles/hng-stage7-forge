
#!/usr/bin/env python3
"""
forge - CLI tool for the Forge CI/CD platform

Usage:
  forge login <url>
  forge run <pipeline.yaml>
  forge logs <run-id> [--follow]
  forge publish <path> --name <n> --version <v>
  forge resolve <pipeline.yaml>
  forge ls <package>
"""

import sys
import json
import time
import hashlib
import argparse
from pathlib import Path
from typing import Optional

import httpx
import yaml


# ── Config storage ─────────────────────────────────────────────────

CONFIG_PATH = Path.home() / ".forge" / "config.json"


def save_config(url: str, token: str):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    config = {"url": url.rstrip("/"), "token": token}
    CONFIG_PATH.write_text(json.dumps(config, indent=2))
    print(f"Credentials saved to {CONFIG_PATH}")


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print("Not logged in. Run: forge login <url>")
        sys.exit(1)
    return json.loads(CONFIG_PATH.read_text())


def get_headers() -> dict:
    config = load_config()
    return {"Authorization": f"Bearer {config['token']}"}


def get_url(path: str) -> str:
    config = load_config()
    return f"{config['url']}{path}"


def _response_error_message(resp: httpx.Response, fallback: str) -> str:
    """
    Return a short, user-friendly error message for an HTTP response.

    Prefer FastAPI-style JSON `detail` payloads when present. If the server
    returns HTML or another non-JSON body, avoid printing raw markup and
    fall back to a concise status-based message instead.
    """
    try:
        payload = resp.json()
    except ValueError:
        content_type = resp.headers.get("content-type", "").lower()
        if "text/plain" in content_type:
            text = resp.text.strip()
            if text:
                return text
        return f"{fallback} (HTTP {resp.status_code}, non-JSON response from server)"

    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, str) and detail.strip():
            return detail
        if detail is not None:
            return json.dumps(detail)
    return fallback


# ── Commands ───────────────────────────────────────────────────────

def cmd_login(args):
    """
    forge login <url>
    Prompts for token and saves credentials locally.
    """
    url = args.url.rstrip("/")
    token = input("Enter your Forge token: ").strip()

    # Verify token works
    try:
        resp = httpx.get(
            f"{url}/auth/verify",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0
        )
        if resp.status_code != 200:
            message = _response_error_message(resp, "Invalid token")
            print(f"Login failed: {message}")
            sys.exit(1)
    except httpx.RequestError as e:
        print(f"Cannot connect to {url}: {e}")
        sys.exit(1)

    save_config(url, token)
    print(f"Logged in to {url}")


def cmd_run(args):
    """
    forge run <pipeline.yaml>
    Submit a pipeline and poll until complete.
    """
    pipeline_path = Path(args.pipeline)
    if not pipeline_path.exists():
        print(f"Pipeline file not found: {pipeline_path}")
        sys.exit(1)

    print(f"Submitting pipeline: {pipeline_path.name}")

    with open(pipeline_path, "rb") as f:
        resp = httpx.post(
            get_url("/runs"),
            headers=get_headers(),
            files={"pipeline": (pipeline_path.name, f, "application/yaml")},
            timeout=30.0
        )

    if resp.status_code != 200:
        print(f"Failed to submit: {resp.status_code}")
        print(resp.text)
        sys.exit(1)

    run_id = resp.json()["run_id"]
    print(f"Run ID: {run_id}")
    print(f"Streaming logs...")
    print("=" * 60)

    # Stream logs while polling status
    _stream_logs_to_terminal(run_id, follow=True)

    # Get final status
    resp = httpx.get(get_url(f"/runs/{run_id}"), headers=get_headers())
    run = resp.json()
    status = run["status"]

    print("=" * 60)
    print(f"\nFinal status: {status.upper()}")

    if status == "succeeded":
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")
        # Show which jobs failed
        for job_name, job_result in run.get("jobs", {}).items():
            job_status = job_result.get("status", "unknown")
            icon = "✓" if job_status == "succeeded" else "✗" if job_status == "failed" else "-"
            print(f"  {icon} {job_name}: {job_status}")
        sys.exit(1)


def cmd_logs(args):
    """
    forge logs <run-id> [--follow]
    Fetch and display logs for a run.
    """
    _stream_logs_to_terminal(args.run_id, follow=args.follow)


def _stream_logs_to_terminal(run_id: str, follow: bool = False):
    """Stream SSE log events to terminal."""
    url = get_url(f"/runs/{run_id}/logs")
    if follow:
        url += "?follow=true"

    try:
        with httpx.stream(
            "GET",
            url,
            headers=get_headers(),
            timeout=None
        ) as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    data = line[6:]
                    try:
                        entry = json.loads(data)
                        ts = entry.get("ts", "")[:19].replace("T", " ")
                        job = entry.get("job", "")
                        text = entry.get("line", "")
                        print(f"[{ts}] [{job}] {text}")
                    except json.JSONDecodeError:
                        print(line)
    except KeyboardInterrupt:
        print("\nStopped following logs.")
    except httpx.RequestError as e:
        print(f"Error streaming logs: {e}")


def cmd_publish(args):
    """
    forge publish <path> --name <n> --version <v>
    Publish an artifact to the registry.
    """
    artifact_path = Path(args.path)
    if not artifact_path.exists():
        print(f"File not found: {artifact_path}")
        sys.exit(1)

    # Compute SHA-256
    file_bytes = artifact_path.read_bytes()
    sha256 = hashlib.sha256(file_bytes).hexdigest()

    print(f"Publishing {args.name}@{args.version}")
    print(f"SHA-256: {sha256}")

    with open(artifact_path, "rb") as f:
        resp = httpx.post(
            get_url(f"/artifacts/{args.name}/{args.version}"),
            headers=get_headers(),
            files={"file": (artifact_path.name, f, "application/octet-stream")},
            data={"checksum": f"sha256:{sha256}"},
            timeout=120.0
        )

    if resp.status_code == 201:
        print(f"Published {args.name}@{args.version} successfully.")
    elif resp.status_code == 409:
        print(f"Version {args.name}@{args.version} already exists (immutable).")
        sys.exit(1)
    elif resp.status_code == 400:
        print(f"Upload rejected: {resp.json()}")
        sys.exit(1)
    else:
        print(f"Failed: HTTP {resp.status_code}")
        print(resp.text)
        sys.exit(1)


def cmd_resolve(args):
    """
    forge resolve <pipeline.yaml>
    Print the resolved lockfile without running the pipeline.
    """
    pipeline_path = Path(args.pipeline)
    if not pipeline_path.exists():
        print(f"Pipeline file not found: {pipeline_path}")
        sys.exit(1)

    with open(pipeline_path) as f:
        pipeline_def = yaml.safe_load(f)

    deps = pipeline_def.get("dependencies", [])
    if not deps:
        print("No dependencies declared in pipeline.")
        return

    resp = httpx.post(
        get_url("/resolve"),
        headers=get_headers(),
        json={"dependencies": deps},
        timeout=30.0
    )

    if resp.status_code == 200:
        lockfile = resp.json()
        print(json.dumps(lockfile, indent=2))
    else:
        print(f"Resolution failed: {_response_error_message(resp, 'Resolution failed')}")
        sys.exit(1)


def cmd_ls(args):
    """
    forge ls <package>
    List all published versions of a package.
    """
    resp = httpx.get(
        get_url(f"/artifacts/{args.package}"),
        headers=get_headers(),
        timeout=10.0
    )

    if resp.status_code == 404:
        print(f"Package '{args.package}' not found.")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"Error: HTTP {resp.status_code}")
        sys.exit(1)

    data = resp.json()
    versions = data.get("versions", [])

    if not versions:
        print(f"No versions published for '{args.package}'.")
        return

    print(f"\n{args.package}")
    print("-" * 40)
    for v in sorted(versions):
        print(f"  {v}")
    print(f"\n{len(versions)} version(s) found.")


# ── Argument parser ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="forge",
        description="Forge CI/CD Platform CLI"
    )
    subparsers = parser.add_subparsers(dest="command")

    # forge login <url>
    login_parser = subparsers.add_parser(
        "login", help="Store credentials"
    )
    login_parser.add_argument("url", help="Platform URL")

    # forge run <pipeline.yaml>
    run_parser = subparsers.add_parser(
        "run", help="Submit and run a pipeline"
    )
    run_parser.add_argument("pipeline", help="Path to pipeline YAML")

    # forge logs <run-id> [--follow]
    logs_parser = subparsers.add_parser(
        "logs", help="Fetch logs for a run"
    )
    logs_parser.add_argument("run_id", help="Run ID")
    logs_parser.add_argument(
        "--follow", "-f",
        action="store_true",
        help="Stream logs in real time"
    )

    # forge publish <path> --name <n> --version <v>
    publish_parser = subparsers.add_parser(
        "publish", help="Publish an artifact"
    )
    publish_parser.add_argument("path", help="Path to artifact file")
    publish_parser.add_argument("--name", required=True, help="Artifact name")
    publish_parser.add_argument(
        "--version", required=True, help="Artifact version (semver)"
    )

    # forge resolve <pipeline.yaml>
    resolve_parser = subparsers.add_parser(
        "resolve", help="Print lockfile without running"
    )
    resolve_parser.add_argument("pipeline", help="Path to pipeline YAML")

    # forge ls <package>
    ls_parser = subparsers.add_parser(
        "ls", help="List versions of a package"
    )
    ls_parser.add_argument("package", help="Package name")

    args = parser.parse_args()

    commands = {
        "login": cmd_login,
        "run": cmd_run,
        "logs": cmd_logs,
        "publish": cmd_publish,
        "resolve": cmd_resolve,
        "ls": cmd_ls,
    }

    if args.command not in commands:
        parser.print_help()
        sys.exit(1)

    commands[args.command](args)


if __name__ == "__main__":
    main()
