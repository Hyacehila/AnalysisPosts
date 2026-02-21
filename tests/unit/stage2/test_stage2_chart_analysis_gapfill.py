"""
ChartAnalysis gap-fill behavior tests (B8).
"""
from __future__ import annotations

from unittest.mock import patch

from nodes.stage2.chart_analysis import ChartAnalysisNode


def test_chart_analysis_only_processes_uncovered_charts():
    shared = {
        "stage2_results": {
            "charts": [
                {"id": "c1", "title": "图1", "path": "report/images/c1.png"},
                {"id": "c2", "title": "图2", "path": "report/images/c2.png"},
            ],
            "execution_log": {"tools_executed": []},
        },
        "forum": {
            "visual_analyses": [
                {
                    "chart_id": "c1",
                    "chart_title": "图1",
                    "analysis": "论坛视觉分析图1",
                    "analysis_status": "success",
                }
            ]
        },
    }

    node = ChartAnalysisNode()
    with patch("nodes.stage2.chart_analysis.call_glm45v_thinking", return_value="批量分析图2") as mocked_call:
        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

    assert mocked_call.call_count == 1
    assert "c1" in shared["stage2_results"]["chart_analyses"]
    assert "c2" in shared["stage2_results"]["chart_analyses"]
    assert shared["stage2_results"]["chart_analyses"]["c1"]["analysis_content"] == "论坛视觉分析图1"
    assert shared["stage2_results"]["chart_analyses"]["c2"]["analysis_status"] == "success"


def test_chart_analysis_respects_vision_thinking_switch():
    shared = {
        "config": {"llm": {"vision_thinking_enabled": False}},
        "stage2_results": {
            "charts": [{"id": "c1", "title": "图1", "path": "report/images/c1.png"}],
            "execution_log": {"tools_executed": []},
        },
        "forum": {"visual_analyses": []},
    }
    captured = {}

    def _fake_call(*args, **kwargs):
        captured["kwargs"] = kwargs
        return "分析结果"

    node = ChartAnalysisNode()
    with patch("nodes.stage2.chart_analysis.call_glm45v_thinking", side_effect=_fake_call):
        prep_res = node.prep(shared)
        node.exec(prep_res)

    assert captured["kwargs"]["enable_thinking"] is False
