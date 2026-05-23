import httpx
import yaml
from pathlib import Path
from config import SLACK_WEBHOOK_URL
import requests


def load_webhook_url() -> str:
    config_path = Path("config.yaml")
    if not config_path.exists():
        return ""
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config.get("slack", {}).get("webhook_url", "")


async def send_slack(payload: dict):
    """Send a message to Slack webhook."""
    url = load_webhook_url()
    if not url:
        return
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json=payload, timeout=10.0)
        except Exception:
            pass  # never let Slack failure break a pipeline


async def notify_pipeline_started(pipeline_name: str, run_id: str):
    await send_slack({
        "text": f":rocket: *Pipeline Started*",
        "attachments": [
            {
                "color": "#2196F3",
                "fields": [
                    {"title": "Pipeline", "value": pipeline_name, "short": True},
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Status", "value": "started", "short": True},
                ]
            }
        ]
    })


async def notify_pipeline_succeeded(
    pipeline_name: str,
    run_id: str,
    duration: str
):
    await send_slack({
        "text": f":white_check_mark: *Pipeline Succeeded*",
        "attachments": [
            {
                "color": "#4CAF50",
                "fields": [
                    {"title": "Pipeline", "value": pipeline_name, "short": True},
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Duration", "value": duration, "short": True},
                    {"title": "Status", "value": "succeeded", "short": True},
                ]
            }
        ]
    })


async def notify_pipeline_failed(
    pipeline_name: str,
    run_id: str,
    duration: str,
    failing_job: str
):
    await send_slack({
        "text": f":x: *Pipeline Failed* <!here>",
        "attachments": [
            {
                "color": "#F44336",
                "fields": [
                    {"title": "Pipeline", "value": pipeline_name, "short": True},
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Duration", "value": duration, "short": True},
                    {"title": "Failing Job", "value": failing_job, "short": True},
                    {"title": "Status", "value": "failed", "short": True},
                ]
            }
        ]
    })


async def notify_integrity_failure(
    artifact: str,
    expected_sha: str,
    actual_sha: str,
    run_id: str
):
    await send_slack({
        "text": f":warning: *Integrity Failure Detected* <!channel>",
        "attachments": [
            {
                "color": "#FF5722",
                "fields": [
                    {"title": "Artifact", "value": artifact, "short": True},
                    {"title": "Run ID", "value": run_id, "short": True},
                    {
                        "title": "Expected SHA-256",
                        "value": f"`{expected_sha}`",
                        "short": False
                    },
                    {
                        "title": "Actual SHA-256",
                        "value": f"`{actual_sha}`",
                        "short": False
                    },
                ]
            }
        ]
    })


async def notify_resolution_failure(
    pipeline_name: str,
    run_id: str,
    details: str
):
    await send_slack({
        "text": f":no_entry: *Dependency Resolution Failed* <!here>",
        "attachments": [
            {
                "color": "#9C27B0",
                "fields": [
                    {"title": "Pipeline", "value": pipeline_name, "short": True},
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Details", "value": details, "short": False},
                ]
            }
        ]
    })