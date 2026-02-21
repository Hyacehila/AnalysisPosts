"""
Status event helpers.

This module stores node enter/exit events in report/status.json.
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from utils.path_manager import get_report_dir
from utils.status_store import atomic_write_json, read_status


STATUS_VERSION = 2
_EVENT_WRITE_LOCK = threading.Lock()
_VALID_EVENTS = {"enter", "exit"}
_VALID_EXIT_STATUS = {"", "completed", "failed"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _new_run_id() -> str:
    return uuid.uuid4().hex


def _resolve_path(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path)
    return Path(get_report_dir()) / "status.json"


def build_empty_status(run_id: str | None = None) -> Dict[str, Any]:
    return {
        "version": STATUS_VERSION,
        "run_id": str(run_id or "").strip() or _new_run_id(),
        "events": [],
    }


def _normalize_status(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    events = data.get("events")
    if not isinstance(events, list):
        events = []

    normalized_events = []
    for item in events:
        if not isinstance(item, dict):
            continue
        seq_raw = item.get("seq", 0)
        try:
            seq = int(seq_raw)
        except Exception:
            seq = 0
        normalized_events.append(
            {
                "seq": seq,
                "ts": str(item.get("ts", "")).strip(),
                "event": str(item.get("event", "")).strip(),
                "stage": str(item.get("stage", "")).strip(),
                "node": str(item.get("node", "")).strip(),
                "branch_id": str(item.get("branch_id", "main")).strip() or "main",
                "status": str(item.get("status", "")).strip(),
                "error": str(item.get("error", "")).strip(),
            }
        )

    normalized_events.sort(key=lambda x: (x.get("seq", 0), x.get("ts", "")))
    for idx, event in enumerate(normalized_events, start=1):
        event["seq"] = idx

    version_raw = data.get("version", STATUS_VERSION)
    try:
        version = int(version_raw)
    except Exception:
        version = STATUS_VERSION

    run_id = str(data.get("run_id", "")).strip() or _new_run_id()

    return {
        "version": STATUS_VERSION if version != STATUS_VERSION else version,
        "run_id": run_id,
        "events": normalized_events,
    }


def read_status_events(*, path: str | Path | None = None) -> Dict[str, Any]:
    target = _resolve_path(path)
    payload = read_status(target, default={})
    return _normalize_status(payload)


def write_status_events(status: Dict[str, Any], *, path: str | Path | None = None) -> str:
    target = _resolve_path(path)
    normalized = _normalize_status(status)
    atomic_write_json(target, normalized)
    return str(target)


def start_status_run(*, path: str | Path | None = None, run_id: str | None = None) -> Dict[str, Any]:
    status = build_empty_status(run_id=run_id)
    write_status_events(status, path=path)
    return status


def append_status_event(
    *,
    node_name: str,
    stage: str,
    event: str,
    status: str = "",
    error: str = "",
    branch_id: str = "main",
    run_id: str | None = None,
    path: str | Path | None = None,
) -> Dict[str, Any]:
    event_name = str(event or "").strip().lower()
    if event_name not in _VALID_EVENTS:
        raise ValueError(f"event must be one of {_VALID_EVENTS}, got {event!r}")

    exit_status = str(status or "").strip().lower()
    if event_name == "enter":
        exit_status = ""
    elif exit_status not in _VALID_EXIT_STATUS:
        raise ValueError(f"status must be one of {_VALID_EXIT_STATUS}, got {status!r}")

    target = _resolve_path(path)

    with _EVENT_WRITE_LOCK:
        payload = read_status_events(path=target)
        if run_id and str(run_id).strip() and payload.get("run_id") != str(run_id).strip():
            payload = build_empty_status(run_id=run_id)

        events = payload.setdefault("events", [])
        entry = {
            "seq": len(events) + 1,
            "ts": _now(),
            "event": event_name,
            "stage": str(stage or "").strip(),
            "node": str(node_name or "").strip(),
            "branch_id": str(branch_id or "main").strip() or "main",
            "status": exit_status,
            "error": str(error or "").strip(),
        }
        events.append(entry)
        write_status_events(payload, path=target)
    return entry


def derive_current_location(status: Dict[str, Any]) -> Dict[str, str]:
    payload = _normalize_status(status)
    open_by_branch: Dict[str, Dict[str, Any]] = {}
    for event in payload.get("events", []):
        branch = event.get("branch_id", "main")
        if event.get("event") == "enter":
            open_by_branch[branch] = event
            continue
        if event.get("event") == "exit":
            open_event = open_by_branch.get(branch)
            if open_event and open_event.get("node") == event.get("node"):
                open_by_branch.pop(branch, None)
            elif branch in open_by_branch:
                open_by_branch.pop(branch, None)

    if not open_by_branch:
        return {"current_stage": "", "current_node": ""}

    latest = max(open_by_branch.values(), key=lambda x: int(x.get("seq", 0)))
    return {
        "current_stage": str(latest.get("stage", "")),
        "current_node": str(latest.get("node", "")),
    }

