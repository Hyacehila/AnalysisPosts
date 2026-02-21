"""
Status API for dashboard.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from utils.path_manager import get_report_dir
from utils.status_events import (
    build_empty_status,
    derive_current_location,
    read_status_events,
    write_status_events,
)


def _default_status() -> Dict[str, Any]:
    status = build_empty_status()
    status.update({"current_stage": "", "current_node": ""})
    return status


def read_status(path: str | None = None) -> Dict[str, Any]:
    """Read status.json if exists; otherwise return empty v2 status structure."""
    try:
        status = read_status_events(path=path or str(Path(get_report_dir()) / "status.json"))
    except Exception:
        return _default_status()

    status.update(derive_current_location(status))
    return status


def write_status(status: Dict[str, Any], path: str | None = None) -> str:
    """Write status.json and return path."""
    return write_status_events(status, path=path or str(Path(get_report_dir()) / "status.json"))
