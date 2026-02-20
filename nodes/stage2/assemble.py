"""
Stage2 compatibility assembly node (B4 bridge).
"""
from __future__ import annotations

import copy

from nodes.base import MonitoredNode


class AssembleStage2ResultsNode(MonitoredNode):
    """
    Merge dual-source agent outputs into stage2_results (backward compatible).
    """

    def prep(self, shared):
        return {
            "data_agent": copy.deepcopy(shared.get("agent_results", {}).get("data_agent", {})),
            "search_agent": copy.deepcopy(shared.get("agent_results", {}).get("search_agent", {})),
        }

    def exec(self, prep_res):
        data_agent = prep_res.get("data_agent", {})
        search_agent = prep_res.get("search_agent", {})
        return {
            "charts": list(data_agent.get("charts", []) or []),
            "tables": list(data_agent.get("tables", []) or []),
            "insights": {},
            "execution_log": dict(data_agent.get("execution_log", {}) or {}),
            "search_context": dict(search_agent or {}),
        }

    def post(self, shared, prep_res, exec_res):
        shared["stage2_results"] = exec_res
        shared.setdefault("stage2_results", {}).setdefault(
            "output_files",
            {
                "charts_dir": "report/images/",
                "analysis_data": "report/analysis_data.json",
                "insights_file": "report/insights.json",
            },
        )
        return "default"


__all__ = ["AssembleStage2ResultsNode"]
