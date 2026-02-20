"""
VisualAnalysis node tests (B7).
"""
from __future__ import annotations

from pathlib import Path

import nodes.stage2.visual as visual_module
from nodes.stage2.visual import VisualAnalysisNode


def _build_shared(chart_path: str) -> dict:
    return {
        "forum": {
            "current_directive": {
                "charts": ["c1"],
                "question": "请解释图表中的关键趋势",
            },
            "visual_analyses": [],
            "current_round": 1,
            "rounds": [],
        },
        "agent_results": {
            "data_agent": {
                "charts": [{"id": "c1", "title": "趋势图", "path": chart_path}],
            }
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_visual_analysis_node_analyzes_selected_chart(monkeypatch, tmp_path):
    chart_path = tmp_path / "c1.png"
    chart_path.write_bytes(b"fake")
    shared = _build_shared(str(chart_path))

    monkeypatch.setattr(
        visual_module,
        "call_glm45v_thinking",
        lambda *args, **kwargs: "图中显示情绪在第二阶段明显上升。",
    )

    node = VisualAnalysisNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert len(shared["forum"]["visual_analyses"]) == 1
    assert shared["forum"]["visual_analyses"][0]["chart_id"] == "c1"
    assert "明显上升" in shared["forum"]["visual_analyses"][0]["analysis"]
    assert len(shared["trace"]["visual_analyses"]) == 1


def test_visual_analysis_node_handles_missing_chart_path(monkeypatch):
    shared = _build_shared(str(Path("report/images/missing.png")))

    monkeypatch.setattr(
        visual_module,
        "call_glm45v_thinking",
        lambda *args, **kwargs: "无法访问图片，给出文本级分析。",
    )

    node = VisualAnalysisNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert shared["forum"]["visual_analyses"][0]["analysis_status"] == "success"
    assert shared["forum"]["visual_analyses"][0]["chart_id"] == "c1"
