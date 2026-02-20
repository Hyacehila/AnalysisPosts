"""
MergeResults node tests (B8).
"""
from __future__ import annotations

from nodes.stage2.merge import MergeResultsNode


def test_merge_results_node_builds_stage2_compatible_output():
    shared = {
        "agent_results": {
            "data_agent": {
                "charts": [{"id": "c1"}],
                "tables": [{"id": "t1"}],
                "execution_log": {"tools_executed": ["tool_a"], "total_charts": 1},
                "supplements": [
                    {
                        "tools": ["tool_b"],
                        "reason": "补充分析",
                    }
                ],
            },
            "search_agent": {
                "background_context": "外部背景",
                "consistency_points": ["一致点A"],
                "conflict_points": [],
                "blind_spots": [],
                "recommended_followups": [],
                "supplements": [
                    {
                        "queries": ["官方回应"],
                        "background_context": "补充背景",
                    }
                ],
            },
        },
        "forum": {
            "current_round": 2,
            "rounds": [
                {"round": 1, "summary": {"decision": "supplement_search"}},
                {
                    "round": 2,
                    "summary": {
                        "decision": "sufficient",
                        "synthesized_conclusions": ["结论1", "结论2"],
                    },
                },
            ],
            "visual_analyses": [{"chart_id": "c1", "analysis": "视觉补充"}],
        },
    }

    node = MergeResultsNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert shared["stage2_results"]["charts"] == [{"id": "c1"}]
    assert shared["stage2_results"]["tables"] == [{"id": "t1"}]
    assert shared["stage2_results"]["execution_log"]["tools_executed"] == ["tool_a"]
    assert shared["stage2_results"]["search_context"]["background_context"] == "外部背景"
    assert shared["stage2_results"]["search_context"]["forum_conclusions"] == ["结论1", "结论2"]
