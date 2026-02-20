"""
Stage2 merge node (B8).
"""
from __future__ import annotations

import copy
from typing import Any, Dict, List

from nodes.base import MonitoredNode


def _extract_forum_conclusions(rounds: List[Dict[str, Any]]) -> List[str]:
    if not rounds:
        return []
    latest_summary = dict(rounds[-1].get("summary", {}) or {})
    conclusions = list(latest_summary.get("synthesized_conclusions", []) or [])
    return [str(item).strip() for item in conclusions if str(item).strip()]


class MergeResultsNode(MonitoredNode):
    """Merge dual-source + forum loop outputs into stage2_results."""

    def prep(self, shared):
        return {
            "data_agent": copy.deepcopy(shared.get("agent_results", {}).get("data_agent", {})),
            "search_agent": copy.deepcopy(shared.get("agent_results", {}).get("search_agent", {})),
            "forum": copy.deepcopy(shared.get("forum", {})),
        }

    def exec(self, prep_res):
        data_agent = prep_res.get("data_agent", {}) or {}
        search_agent = prep_res.get("search_agent", {}) or {}
        forum = prep_res.get("forum", {}) or {}
        rounds = list(forum.get("rounds", []) or [])
        visual_analyses = list(forum.get("visual_analyses", []) or [])

        execution_log = dict(data_agent.get("execution_log", {}) or {})
        execution_log.setdefault("tools_executed", [])
        execution_log["total_charts"] = len(data_agent.get("charts", []) or [])
        execution_log["total_tables"] = len(data_agent.get("tables", []) or [])
        execution_log["forum_rounds"] = int(forum.get("current_round", 0))

        search_context = dict(search_agent)
        search_context["forum_conclusions"] = _extract_forum_conclusions(rounds)
        search_context["forum_rounds"] = int(forum.get("current_round", 0))
        search_context["visual_analyses"] = visual_analyses

        return {
            "charts": list(data_agent.get("charts", []) or []),
            "tables": list(data_agent.get("tables", []) or []),
            "insights": {},
            "execution_log": execution_log,
            "search_context": search_context,
        }

    def post(self, shared, prep_res, exec_res):
        shared["stage2_results"] = dict(exec_res)
        shared["stage2_results"].setdefault(
            "output_files",
            {
                "charts_dir": "report/images/",
                "analysis_data": "report/analysis_data.json",
                "insights_file": "report/insights.json",
            },
        )
        return "default"


__all__ = ["MergeResultsNode"]
