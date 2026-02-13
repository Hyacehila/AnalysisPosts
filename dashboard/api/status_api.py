"""
Status API for dashboard.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from utils.path_manager import get_report_dir
from utils.status_store import atomic_write_json


def _default_status() -> Dict[str, Any]:
    return {
        "start_time": "",
        "current_stage": "",
        "current_node": "",
        "execution_log": [],
        "progress_status": {},
        "error_log": [],
    }


def read_status(path: str | None = None) -> Dict[str, Any]:
    """Read status.json if exists; otherwise return empty status structure."""
    if path is None:
        path = str(Path(get_report_dir()) / "status.json")
    status_path = Path(path)
    if not status_path.exists():
        return _default_status()
    try:
        content = status_path.read_text(encoding="utf-8")
    except OSError:
        return _default_status()
    if not content.strip():
        return _default_status()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return _default_status()


def write_status(status: Dict[str, Any], path: str | None = None) -> str:
    """Write status.json and return path."""
    if path is None:
        path = str(Path(get_report_dir()) / "status.json")
    status_path = Path(path)
    atomic_write_json(status_path, status)
    return str(status_path)
