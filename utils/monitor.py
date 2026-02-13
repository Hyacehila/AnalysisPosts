"""
Execution monitor: write runtime status into report/status.json.
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

from utils.path_manager import get_report_dir
from utils.status_store import atomic_write_json


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def init_monitor(shared: Dict[str, Any]) -> Dict[str, Any]:
    monitor = shared.get("monitor")
    if not isinstance(monitor, dict):
        monitor = {}
        shared["monitor"] = monitor

    monitor.setdefault("start_time", _now())
    monitor.setdefault("current_stage", "")
    monitor.setdefault("current_node", "")
    monitor.setdefault("execution_log", [])
    monitor.setdefault("progress_status", {})
    monitor.setdefault("error_log", [])
    return monitor


def update_status(
    shared: Dict[str, Any],
    *,
    node_name: str,
    status: str,
    stage: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """
    Update shared monitor state and persist to report/status.json.
    """
    monitor = init_monitor(shared)
    if stage is not None:
        monitor["current_stage"] = stage
    monitor["current_node"] = node_name

    entry = {
        "time": _now(),
        "stage": stage or monitor.get("current_stage", ""),
        "node": node_name,
        "status": status,
        "extra": extra or {},
        "error": error or "",
    }
    monitor["execution_log"].append(entry)
    if status == "failed":
        monitor["error_log"].append(entry)

    report_dir = get_report_dir()
    path = os.path.join(report_dir, "status.json")
    atomic_write_json(path, monitor)
