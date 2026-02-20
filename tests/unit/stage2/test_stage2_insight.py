"""
LLMInsightNode semantic compatibility tests.
"""

from __future__ import annotations

import nodes.stage2.insight as insight_module
from nodes.stage2.insight import LLMInsightNode


def _success_payload() -> str:
    return (
        '{"sentiment_summary":"情感总结","topic_distribution":"主题总结",'
        '"geographic_distribution":"地域总结","publisher_behavior":"发布者总结",'
        '"overall_summary":"综合总结"}'
    )


def test_insight_exec_prefers_analysis_content(monkeypatch):
    shared = {
        "agent": {"data_summary": "样本摘要"},
        "stage2_results": {
            "chart_analyses": {
                "c1": {
                    "chart_title": "趋势图",
                    "analysis_status": "success",
                    "analysis_content": "来自analysis_content的图表洞察",
                }
            },
            "tables": [],
        },
    }
    captured = {}

    def _fake_call(prompt, *args, **kwargs):
        captured["prompt"] = prompt
        return _success_payload()

    monkeypatch.setattr(insight_module, "call_glm46", _fake_call)

    node = LLMInsightNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert "来自analysis_content的图表洞察" in captured["prompt"]
    assert exec_res["overall_summary"] == "综合总结"


def test_insight_exec_keeps_backward_compat_with_analysis(monkeypatch):
    shared = {
        "agent": {"data_summary": "样本摘要"},
        "stage2_results": {
            "chart_analyses": {
                "c1": {
                    "chart_title": "趋势图",
                    "analysis_status": "success",
                    "analysis": "来自analysis字段的旧版洞察",
                }
            },
            "tables": [],
        },
    }
    captured = {}

    def _fake_call(prompt, *args, **kwargs):
        captured["prompt"] = prompt
        return _success_payload()

    monkeypatch.setattr(insight_module, "call_glm46", _fake_call)

    node = LLMInsightNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert "来自analysis字段的旧版洞察" in captured["prompt"]
    assert exec_res["sentiment_summary"] == "情感总结"
