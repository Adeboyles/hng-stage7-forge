"""Shared configuration loading for the engine package."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load the Forge configuration file from disk."""
    config_path = Path(os.environ.get("FORGE_CONFIG", "config.yaml"))
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def reset_config_cache() -> None:
    """Clear the cached configuration for tests or config reloads."""
    load_config.cache_clear()


def engine_settings() -> dict:
    """Return the engine section of the platform config."""
    return load_config().get("engine", {})


def registry_settings() -> dict:
    """Return the registry section of the platform config."""
    return load_config().get("registry", {})


def isolation_settings() -> dict:
    """Return the isolation section of the platform config."""
    return load_config().get("isolation", {})


def registry_internal_base_url() -> str:
    """Build the internal registry base URL used by engine and jobs."""
    registry = registry_settings()
    host = registry.get("internal_host") or registry.get("host", "registry")
    port = registry.get("port", 8001)
    return f"http://{host}:{port}"
