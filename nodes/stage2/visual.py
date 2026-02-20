"""
Stage2 visual supplement node (B7).
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List

from nodes.base import MonitoredNode
from utils.call_llm import call_glm45v_thinking


def _collect_all_charts(shared: Dict[str, Any]) -> List[Dict[str, Any]]:
    data_agent = shared.get("agent_results", {}).get("data_agent", {}) or {}
    return list(data_agent.get("charts", []) or [])


def _select_charts(all_charts: List[Dict[str, Any]], requested: List[str], existing_ids: List[str]) -> List[Dict[str, Any]]:
    chart_map = {}
    for chart in all_charts:
        cid = str(chart.get("id", "")).strip()
        if cid:
            chart_map[cid] = chart

    picked: List[Dict[str, Any]] = []
    if requested:
        for cid in requested:
            if cid in chart_map:
                picked.append(chart_map[cid])
    else:
        for chart in all_charts:
            cid = str(chart.get("id", "")).strip()
            if not cid:
                continue
            if cid in existing_ids:
                continue
            picked.append(chart)
            if len(picked) >= 2:
                break
    return picked[:3]


class VisualAnalysisNode(MonitoredNode):
    """Analyze selected charts with GLM4.5V and append visual findings."""

    def prep(self, shared):
        forum = shared.get("forum", {}) or {}
        directive = forum.get("current_directive", {}) or {}
        requested_ids = []
        for item in list(directive.get("charts", []) or []):
            requested_ids.append(str(item).strip())
        requested_ids = [cid for cid in requested_ids if cid]

        all_charts = _collect_all_charts(shared)
        existing = [str(item.get("chart_id", "")).strip() for item in list(forum.get("visual_analyses", []) or [])]
        selected = _select_charts(all_charts, requested_ids, existing)

        return {
            "question": str(directive.get("question", "") or "请分析图表中的关键趋势与异常点。"),
            "selected_charts": selected,
        }

    def exec(self, prep_res):
        question = prep_res.get("question", "")
        selected = list(prep_res.get("selected_charts", []) or [])
        results = []

        for chart in selected:
            chart_id = str(chart.get("id", "")).strip() or "unknown_chart"
            chart_title = str(chart.get("title", "")).strip() or chart_id
            chart_path = (
                str(chart.get("path", "")).strip()
                or str(chart.get("file_path", "")).strip()
                or str(chart.get("chart_path", "")).strip()
                or str(chart.get("image_path", "")).strip()
            )
            image_paths = [chart_path] if chart_path and os.path.exists(chart_path) else None

            try:
                response = call_glm45v_thinking(
                    prompt=f"{question}\n图表标题：{chart_title}",
                    image_paths=image_paths,
                    temperature=0.6,
                    max_tokens=1200,
                    enable_thinking=True,
                )
                results.append(
                    {
                        "chart_id": chart_id,
                        "chart_title": chart_title,
                        "chart_path": chart_path,
                        "analysis": str(response).strip(),
                        "analysis_status": "success",
                        "analysis_timestamp": time.time(),
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "chart_id": chart_id,
                        "chart_title": chart_title,
                        "chart_path": chart_path,
                        "analysis": f"视觉分析失败: {exc}",
                        "analysis_status": "failed",
                        "analysis_timestamp": time.time(),
                    }
                )
        return {"visual_results": results}

    def post(self, shared, prep_res, exec_res):
        forum = shared.setdefault("forum", {})
        forum.setdefault("visual_analyses", []).extend(list(exec_res.get("visual_results", []) or []))

        trace = shared.setdefault("trace", {})
        trace.setdefault("visual_analyses", []).extend(list(exec_res.get("visual_results", []) or []))
        return "default"


__all__ = ["VisualAnalysisNode"]
