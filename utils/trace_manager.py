"""Trace helpers for Stage2 provenance logging."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def _next_id(prefix: str, items: Iterable[Dict[str, Any]]) -> str:
    count = 0
    for _ in items:
        count += 1
    return f"{prefix}_{count + 1:04d}"


def init_trace(shared: Dict[str, Any]) -> Dict[str, Any]:
    trace = shared.setdefault("trace", {})
    if not isinstance(trace.get("decisions"), list):
        trace["decisions"] = []
    if not isinstance(trace.get("executions"), list):
        trace["executions"] = []
    if not isinstance(trace.get("reflections"), list):
        trace["reflections"] = []
    if not isinstance(trace.get("insight_provenance"), dict):
        trace["insight_provenance"] = {}
    return trace


def append_decision(
    shared: Dict[str, Any],
    *,
    action: str,
    tool_name: str,
    reason: str,
    iteration: int,
) -> str:
    trace = init_trace(shared)
    decision_id = _next_id("d", trace["decisions"])
    trace["decisions"].append(
        {
            "id": decision_id,
            "iteration": int(iteration),
            "action": action,
            "tool_name": tool_name,
            "reason": reason,
            "timestamp": _timestamp(),
        }
    )
    return decision_id


def append_execution(
    shared: Dict[str, Any],
    *,
    tool_name: str,
    iteration: int,
    status: str,
    summary: str,
    has_chart: bool,
    has_data: bool,
    error: bool,
    decision_ref: str | None = None,
) -> str:
    trace = init_trace(shared)
    execution_id = _next_id("e", trace["executions"])
    trace["executions"].append(
        {
            "id": execution_id,
            "decision_ref": decision_ref,
            "iteration": int(iteration),
            "tool_name": tool_name,
            "status": status,
            "summary": summary,
            "has_chart": bool(has_chart),
            "has_data": bool(has_data),
            "error": bool(error),
            "timestamp": _timestamp(),
        }
    )
    return execution_id


def append_reflection(
    shared: Dict[str, Any],
    *,
    iteration: int,
    result: Dict[str, Any],
) -> str:
    trace = init_trace(shared)
    reflection_id = _next_id("r", trace["reflections"])
    trace["reflections"].append(
        {
            "id": reflection_id,
            "iteration": int(iteration),
            "result": dict(result or {}),
            "timestamp": _timestamp(),
        }
    )
    return reflection_id


def set_insight_provenance(shared: Dict[str, Any], provenance: Dict[str, Any]) -> None:
    trace = init_trace(shared)
    trace["insight_provenance"] = dict(provenance or {})


def build_lite_confidence(evidence: Iterable[Dict[str, Any]]) -> Tuple[str, str]:
    count = sum(1 for _ in evidence)
    if count >= 2:
        return "high", "multiple matched execution records"
    if count == 1:
        return "medium", "single matched execution record"
    return "low", "no direct execution evidence matched"


def dump_trace_json(trace: Dict[str, Any], output_path: str) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(trace, f, ensure_ascii=False, indent=2)
    return str(path)


__all__ = [
    "init_trace",
    "append_decision",
    "append_execution",
    "append_reflection",
    "set_insight_provenance",
    "build_lite_confidence",
    "dump_trace_json",
]
