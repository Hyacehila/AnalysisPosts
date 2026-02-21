"""
Pure helpers for Results Viewer page data loading and shaping.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


CORE_JSON_FILES = (
    "analysis_data.json",
    "chart_analyses.json",
    "insights.json",
    "trace.json",
    "status.json",
)


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "exists": False,
            "path": str(path),
            "size_bytes": 0,
            "updated_at": "",
            "parse_ok": False,
            "error": "",
            "text": "",
            "data": {},
        }

    text = path.read_text(encoding="utf-8")
    stat = path.stat()
    updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")

    if not text.strip():
        return {
            "exists": True,
            "path": str(path),
            "size_bytes": stat.st_size,
            "updated_at": updated_at,
            "parse_ok": False,
            "error": "empty file",
            "text": text,
            "data": {},
        }

    try:
        data = json.loads(text)
    except Exception as exc:  # pragma: no cover - exact message varies by runtime
        return {
            "exists": True,
            "path": str(path),
            "size_bytes": stat.st_size,
            "updated_at": updated_at,
            "parse_ok": False,
            "error": str(exc),
            "text": text,
            "data": {},
        }

    return {
        "exists": True,
        "path": str(path),
        "size_bytes": stat.st_size,
        "updated_at": updated_at,
        "parse_ok": True,
        "error": "",
        "text": text,
        "data": data,
    }


def _resolve_chart_path(raw_path: str, report_dir: Path) -> str:
    path_text = str(raw_path or "").strip()
    if not path_text:
        return ""
    candidate = Path(path_text)
    if candidate.is_absolute():
        return str(candidate)

    normalized = path_text.replace("\\", "/").strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]

    normalized_path = Path(normalized)
    if normalized_path.parts and normalized_path.parts[0].lower() == "report":
        return str((report_dir.parent / normalized_path).resolve())

    return str((report_dir / normalized_path).resolve())


def _chart_analysis_lookup(chart_analyses: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(chart_analyses, dict):
        return {
            str(key): _as_dict(value)
            for key, value in chart_analyses.items()
            if str(key).strip()
        }

    lookup: Dict[str, Dict[str, Any]] = {}
    for item in _as_list(chart_analyses):
        payload = _as_dict(item)
        chart_id = str(payload.get("chart_id") or payload.get("id") or "").strip()
        if chart_id:
            lookup[chart_id] = payload
    return lookup


def _build_images_section(
    report_dir: Path,
    analysis_data: Dict[str, Any],
    chart_analyses: Any,
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    charts = _as_list(analysis_data.get("charts"))
    analysis_lookup = _chart_analysis_lookup(chart_analyses)

    for index, chart in enumerate(charts, 1):
        payload = _as_dict(chart)
        chart_id = str(payload.get("id") or payload.get("chart_id") or f"chart_{index}").strip()
        file_path = _resolve_chart_path(
            str(payload.get("file_path") or payload.get("path") or "").strip(),
            report_dir,
        )
        analysis_item = analysis_lookup.get(chart_id, {})
        items.append(
            {
                "id": chart_id,
                "title": str(payload.get("title") or chart_id),
                "type": str(payload.get("type") or ""),
                "source_tool": str(payload.get("source_tool") or ""),
                "description": str(payload.get("description") or ""),
                "file_path": file_path,
                "chart_analysis": str(
                    analysis_item.get("analysis_content")
                    or analysis_item.get("analysis")
                    or ""
                ),
                "analysis_status": str(analysis_item.get("analysis_status") or ""),
                "analysis_timestamp": str(analysis_item.get("analysis_timestamp") or ""),
            }
        )

    if items:
        return {
            "items": items,
            "count": len(items),
            "source": "analysis_data",
        }

    images_dir = report_dir / "images"
    fallback_items: List[Dict[str, Any]] = []
    if images_dir.exists() and images_dir.is_dir():
        for image_path in sorted(images_dir.glob("*.png")):
            fallback_items.append(
                {
                    "id": image_path.stem,
                    "title": image_path.name,
                    "type": "png",
                    "source_tool": "filesystem_fallback",
                    "description": "",
                    "file_path": str(image_path.resolve()),
                    "chart_analysis": "",
                    "analysis_status": "",
                    "analysis_timestamp": "",
                }
            )

    return {
        "items": fallback_items,
        "count": len(fallback_items),
        "source": "images_directory_fallback",
    }


def _build_tables_section(analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []

    for index, table in enumerate(_as_list(analysis_data.get("tables")), 1):
        payload = _as_dict(table)
        items.append(
            {
                "id": str(payload.get("id") or f"table_{index}"),
                "title": str(payload.get("title") or f"Table {index}"),
                "source_tool": str(payload.get("source_tool") or ""),
                "source_type": str(payload.get("source_type") or ""),
                "data": payload.get("data"),
                "raw": payload,
            }
        )

    execution_log = _as_dict(analysis_data.get("execution_log"))
    return {
        "items": items,
        "count": len(items),
        "execution_log": execution_log,
    }


def _build_forum_section(trace: Dict[str, Any]) -> Dict[str, Any]:
    rounds: List[Dict[str, Any]] = []
    for item in _as_list(trace.get("forum_rounds")):
        payload = _as_dict(item)
        rounds.append(
            {
                "round": int(payload.get("round", 0) or 0),
                "decision": str(payload.get("decision") or ""),
                "directive": _as_dict(payload.get("directive")),
                "gaps": _as_list(payload.get("gaps")),
                "synthesized_conclusions": _as_list(payload.get("synthesized_conclusions")),
            }
        )

    return {
        "rounds": rounds,
        "count": len(rounds),
        "loop_status": _as_dict(_as_dict(trace.get("loop_status")).get("forum")),
    }


def _build_search_section(analysis_data: Dict[str, Any], trace: Dict[str, Any]) -> Dict[str, Any]:
    search_context = _as_dict(analysis_data.get("search_context"))

    return {
        "search_context": search_context,
        "agent_analyses": _as_list(trace.get("search_agent_analysis")),
        "search_reflections": _as_list(trace.get("search_reflections")),
        "search_supplements": _as_list(trace.get("search_supplements")),
    }


def _find_matched_executions(
    evidence_items: Iterable[Dict[str, Any]],
    executions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    execution_ids = {
        str(item.get("execution_id") or "").strip()
        for item in evidence_items
        if str(item.get("execution_id") or "").strip()
    }
    tool_names = {
        str(item.get("tool_name") or "").strip()
        for item in evidence_items
        if str(item.get("tool_name") or "").strip()
    }

    matched: List[Dict[str, Any]] = []
    for execution in executions:
        execution_id = str(execution.get("id") or "").strip()
        tool_name = str(execution.get("tool_name") or "").strip()

        if execution_id and execution_id in execution_ids:
            matched.append(execution)
            continue

        if not execution_ids and tool_names and tool_name in tool_names:
            matched.append(execution)

    return matched


def _find_matched_decisions(
    evidence_items: Iterable[Dict[str, Any]],
    matched_executions: List[Dict[str, Any]],
    decisions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    decision_lookup = {
        str(item.get("id") or "").strip(): item
        for item in decisions
        if str(item.get("id") or "").strip()
    }

    decision_ids = {
        str(item.get("decision_id") or "").strip()
        for item in evidence_items
        if str(item.get("decision_id") or "").strip()
    }
    tool_names = {
        str(item.get("tool_name") or "").strip()
        for item in evidence_items
        if str(item.get("tool_name") or "").strip()
    }

    matched: List[Dict[str, Any]] = []
    seen_ids = set()

    for execution in matched_executions:
        decision_ref = str(execution.get("decision_ref") or "").strip()
        decision = decision_lookup.get(decision_ref)
        if decision and decision_ref not in seen_ids:
            matched.append(decision)
            seen_ids.add(decision_ref)

    for decision_id in decision_ids:
        decision = decision_lookup.get(decision_id)
        if decision and decision_id not in seen_ids:
            matched.append(decision)
            seen_ids.add(decision_id)

    if matched or not tool_names:
        return matched

    for decision in decisions:
        tool_name = str(decision.get("tool_name") or "").strip()
        decision_id = str(decision.get("id") or "").strip()
        if tool_name in tool_names and decision_id and decision_id not in seen_ids:
            matched.append(decision)
            seen_ids.add(decision_id)

    return matched


def build_insight_evidence_chain(insights: Dict[str, Any], trace: Dict[str, Any]) -> List[Dict[str, Any]]:
    insight_items = _as_dict(insights)
    provenance_lookup = _as_dict(trace.get("insight_provenance"))
    executions = [_as_dict(item) for item in _as_list(trace.get("executions"))]
    decisions = [_as_dict(item) for item in _as_list(trace.get("decisions"))]

    chains: List[Dict[str, Any]] = []
    for key, insight_value in insight_items.items():
        provenance_key = f"insight_{key}"
        provenance = _as_dict(provenance_lookup.get(provenance_key))
        evidence_items = [_as_dict(item) for item in _as_list(provenance.get("supporting_evidence"))]

        matched_executions = _find_matched_executions(evidence_items, executions)
        matched_decisions = _find_matched_decisions(evidence_items, matched_executions, decisions)

        chains.append(
            {
                "insight_key": str(key),
                "insight_text": str(insight_value),
                "confidence": str(provenance.get("confidence") or ""),
                "confidence_reasoning": str(provenance.get("confidence_reasoning") or ""),
                "supporting_evidence": evidence_items,
                "matched_executions": matched_executions,
                "matched_decisions": matched_decisions,
                "provenance_text": str(provenance.get("text") or ""),
            }
        )

    return chains


def _build_summary(
    images_section: Dict[str, Any],
    tables_section: Dict[str, Any],
    insights: Dict[str, Any],
    forum_section: Dict[str, Any],
    trace: Dict[str, Any],
    status: Dict[str, Any],
    json_files_section: Dict[str, Any],
) -> Dict[str, int]:
    return {
        "charts": len(_as_list(images_section.get("items"))),
        "tables": len(_as_list(tables_section.get("items"))),
        "insights": len(_as_dict(insights)),
        "forum_rounds": len(_as_list(forum_section.get("rounds"))),
        "executions": len(_as_list(trace.get("executions"))),
        "decisions": len(_as_list(trace.get("decisions"))),
        "status_events": len(_as_list(status.get("events"))),
        "available_json_files": sum(
            1
            for meta in json_files_section.values()
            if isinstance(meta, dict) and meta.get("exists")
        ),
    }


def load_results_viewer_bundle(report_dir: Path | str = Path("report")) -> Dict[str, Any]:
    base_dir = Path(report_dir)

    json_files_section = {
        filename: _read_json_file(base_dir / filename)
        for filename in CORE_JSON_FILES
    }

    analysis_data = _as_dict(json_files_section["analysis_data.json"].get("data"))
    chart_analyses = json_files_section["chart_analyses.json"].get("data")
    insights = _as_dict(json_files_section["insights.json"].get("data"))
    trace = _as_dict(json_files_section["trace.json"].get("data"))
    status = _as_dict(json_files_section["status.json"].get("data"))

    images_section = _build_images_section(base_dir, analysis_data, chart_analyses)
    tables_section = _build_tables_section(analysis_data)
    forum_section = _build_forum_section(trace)
    search_section = _build_search_section(analysis_data, trace)
    evidence_section = {
        "chains": build_insight_evidence_chain(insights, trace),
    }

    summary = _build_summary(
        images_section,
        tables_section,
        insights,
        forum_section,
        trace,
        status,
        json_files_section,
    )

    return {
        "summary": summary,
        "images_section": images_section,
        "tables_section": tables_section,
        "forum_section": forum_section,
        "search_section": search_section,
        "evidence_section": evidence_section,
        "json_files_section": json_files_section,
    }


__all__ = [
    "CORE_JSON_FILES",
    "build_insight_evidence_chain",
    "load_results_viewer_bundle",
]
