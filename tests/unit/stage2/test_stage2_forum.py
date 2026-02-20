"""
ForumHost stage2 node tests (B5).
"""
from __future__ import annotations

import json

import nodes.stage2.forum as forum_module
from nodes.stage2.forum import ForumHostNode


def _build_shared() -> dict:
    return {
        "config": {
            "stage2_loops": {
                "forum_max_rounds": 3,
                "forum_min_rounds_for_sufficient": 2,
            }
        },
        "agent": {"data_summary": "样本事件摘要"},
        "agent_results": {
            "data_agent": {
                "charts": [{"id": "c1", "title": "图1"}],
                "tables": [{"id": "t1"}],
                "execution_log": {"tools_executed": ["sentiment_distribution_stats"]},
            },
            "search_agent": {
                "background_context": "搜索背景",
                "blind_spots": ["官方回应细节不足"],
            },
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_forum_host_routes_to_supplement_data(monkeypatch):
    shared = _build_shared()

    monkeypatch.setattr(
        forum_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "cross_analysis": {"agreement": ["主叙事一致"]},
                "gaps": ["缺少细分人群分布"],
                "decision": "supplement_data",
                "directive": {"tools": ["publisher_type_distribution"]},
                "synthesized_conclusions": [],
            },
            ensure_ascii=False,
        ),
    )

    node = ForumHostNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "supplement_data"
    assert shared["forum"]["current_round"] == 1
    assert shared["forum"]["current_directive"]["tools"] == ["publisher_type_distribution"]
    assert len(shared["trace"]["forum_rounds"]) == 1
    assert shared["trace"]["loop_status"]["forum"]["current"] == 1
    assert shared["trace"]["loop_status"]["forum"]["max"] == 3


def test_forum_host_enforces_min_round_before_sufficient(monkeypatch):
    shared = _build_shared()
    shared["forum"] = {
        "current_round": 0,
        "rounds": [],
        "current_directive": {},
        "visual_analyses": [],
    }

    monkeypatch.setattr(
        forum_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "cross_analysis": {"agreement": []},
                "gaps": [],
                "decision": "sufficient",
                "directive": {},
                "synthesized_conclusions": ["首轮即可收敛"],
            },
            ensure_ascii=False,
        ),
    )

    node = ForumHostNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    # min_rounds_for_sufficient=2, 首轮不能直接 sufficient
    assert action == "supplement_search"
    assert shared["trace"]["loop_status"]["forum"]["termination_reason"] == "continue"


def test_forum_host_force_sufficient_when_max_round_reached(monkeypatch):
    shared = _build_shared()
    shared["forum"] = {
        "current_round": 2,
        "rounds": [{"round": 1, "summary": {}}, {"round": 2, "summary": {}}],
        "current_directive": {},
        "visual_analyses": [],
    }

    monkeypatch.setattr(
        forum_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "cross_analysis": {"agreement": []},
                "gaps": ["继续补充"],
                "decision": "supplement_search",
                "directive": {"queries": ["事件 官方回应"]},
                "synthesized_conclusions": [],
            },
            ensure_ascii=False,
        ),
    )

    node = ForumHostNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "sufficient"
    assert shared["forum"]["current_round"] == 3
    assert shared["trace"]["loop_status"]["forum"]["termination_reason"] == "max_rounds_reached"


def test_forum_host_routes_to_supplement_visual(monkeypatch):
    shared = _build_shared()
    shared["agent_results"]["data_agent"]["charts"] = [
        {"id": "c1", "title": "图1", "path": "report/images/c1.png"}
    ]

    monkeypatch.setattr(
        forum_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "cross_analysis": {"agreement": ["图表趋势可进一步解释"]},
                "gaps": ["需要视觉趋势解释"],
                "decision": "supplement_visual",
                "directive": {
                    "charts": ["c1"],
                    "question": "请解释该图的峰值变化。",
                    "reason": "补齐视觉证据。",
                },
                "synthesized_conclusions": [],
            },
            ensure_ascii=False,
        ),
    )

    node = ForumHostNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "supplement_visual"
    assert shared["forum"]["current_directive"]["charts"] == ["c1"]
    assert "峰值变化" in shared["forum"]["current_directive"]["question"]
