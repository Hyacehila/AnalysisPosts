"""
Supplement nodes tests (B6).
"""
from __future__ import annotations

import json

import nodes.stage2.supplement as supplement_module
from nodes.stage2.supplement import SupplementDataNode, SupplementSearchNode


def _build_shared() -> dict:
    return {
        "config": {
            "data_source": {"enhanced_data_path": "data/enhanced_posts_sample_30.json"},
            "web_search": {
                "provider": "tavily",
                "max_results": 2,
                "timeout_seconds": 10,
                "api_key": "test-key",
            },
        },
        "agent_results": {
            "data_agent": {
                "charts": [],
                "tables": [],
                "execution_log": {"tools_executed": []},
            },
            "search_agent": {
                "background_context": "已有背景",
                "consistency_points": [],
                "conflict_points": [],
                "blind_spots": [],
                "recommended_followups": [],
            },
        },
        "forum": {
            "current_directive": {},
            "rounds": [],
            "current_round": 1,
            "visual_analyses": [],
        },
        "agent": {"data_summary": "样本摘要"},
        "search_results": {
            "event_timeline": [],
            "key_actors": [],
            "official_responses": [],
            "public_reactions_summary": "",
            "related_events": [],
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_supplement_data_node_appends_tools_result(monkeypatch):
    shared = _build_shared()
    shared["forum"]["current_directive"] = {
        "tools": ["sentiment_distribution_stats"],
        "reason": "补齐情感分布细节",
    }

    monkeypatch.setattr(
        supplement_module,
        "list_tools",
        lambda *_args, **_kwargs: [
            {
                "name": "sentiment_distribution_stats",
                "canonical_name": "sentiment_distribution_stats",
                "category": "情感分析",
                "generates_chart": True,
            }
        ],
    )
    monkeypatch.setattr(
        supplement_module,
        "call_tool",
        lambda *_args, **_kwargs: {
            "charts": [{"id": "c_s1", "title": "补充图", "path": "report/images/c_s1.png"}],
            "data": {"summary": "补充统计"},
            "summary": "补充执行成功",
        },
    )

    node = SupplementDataNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    data_agent = shared["agent_results"]["data_agent"]
    assert data_agent["execution_log"]["tools_executed"] == ["sentiment_distribution_stats"]
    assert data_agent["supplements"][0]["reason"] == "补齐情感分布细节"
    assert data_agent["charts"][0]["id"] == "c_s1"
    assert len(shared["trace"]["data_agent_supplements"]) == 1


def test_supplement_search_node_appends_followup_search(monkeypatch):
    shared = _build_shared()
    shared["forum"]["current_directive"] = {
        "queries": ["事件 官方回应"],
        "reason": "补齐官方口径",
    }

    monkeypatch.setattr(
        supplement_module,
        "batch_search",
        lambda queries, **kwargs: {
            "queries": list(queries),
            "provider": "tavily",
            "results_by_query": [
                {
                    "query": queries[0],
                    "provider": "tavily",
                    "results": [
                        {
                            "title": "官方通报",
                            "url": "https://example.com/notice",
                            "snippet": "已发布权威信息",
                            "date": "2026-02-19",
                            "source": "example",
                        }
                    ],
                    "error": "",
                }
            ],
            "total_results": 1,
        },
    )
    monkeypatch.setattr(
        supplement_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "background_context": "新增官方回应背景",
                "consistency_points": ["口径一致"],
                "conflict_points": [],
                "blind_spots": ["缺少地市级细节"],
                "recommended_followups": ["持续跟踪"],
            },
            ensure_ascii=False,
        ),
    )

    node = SupplementSearchNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    search_agent = shared["agent_results"]["search_agent"]
    assert search_agent["background_context"] == "新增官方回应背景"
    assert search_agent["supplements"][0]["queries"] == ["事件 官方回应"]
    assert len(shared["trace"]["search_supplements"]) == 1


def test_supplement_search_respects_stage2_reasoning_switch(monkeypatch):
    shared = _build_shared()
    shared["config"]["llm"] = {"reasoning_enabled_stage2": False}
    shared["forum"]["current_directive"] = {
        "queries": ["事件 官方回应"],
        "reason": "补齐官方口径",
    }
    captured = {}

    monkeypatch.setattr(
        supplement_module,
        "batch_search",
        lambda queries, **kwargs: {
            "queries": list(queries),
            "provider": "tavily",
            "results_by_query": [],
            "total_results": 0,
        },
    )

    def _fake_llm(*args, **kwargs):
        captured["kwargs"] = kwargs
        return json.dumps(
            {
                "background_context": "新增官方回应背景",
                "consistency_points": [],
                "conflict_points": [],
                "blind_spots": [],
                "recommended_followups": [],
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(supplement_module, "call_glm46", _fake_llm)

    node = SupplementSearchNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert captured["kwargs"]["enable_reasoning"] is False
