"""
Stage2 forum loop integration (B5-B8).
"""
from __future__ import annotations

import json

import pytest

import nodes.stage2.forum as forum_module
import nodes.stage2.supplement as supplement_module
import nodes.stage2.visual as visual_module
from nodes.stage2.forum import ForumHostNode
from nodes.stage2.merge import MergeResultsNode
from nodes.stage2.supplement import SupplementSearchNode
from nodes.stage2.visual import VisualAnalysisNode


pytestmark = pytest.mark.integration


def _build_shared() -> dict:
    return {
        "config": {
            "stage2_loops": {
                "forum_max_rounds": 3,
                "forum_min_rounds_for_sufficient": 2,
            },
            "web_search": {
                "provider": "tavily",
                "max_results": 2,
                "timeout_seconds": 10,
                "api_key": "test-key",
            },
        },
        "agent": {"data_summary": "样本事件摘要"},
        "forum": {
            "current_round": 0,
            "rounds": [],
            "current_directive": {},
            "visual_analyses": [],
        },
        "search_results": {
            "event_timeline": [],
            "key_actors": [],
            "official_responses": [],
            "public_reactions_summary": "",
            "related_events": [],
        },
        "agent_results": {
            "data_agent": {
                "charts": [],
                "tables": [],
                "execution_log": {"tools_executed": []},
            },
            "search_agent": {
                "background_context": "初始背景",
                "consistency_points": [],
                "conflict_points": [],
                "blind_spots": ["官方回应不足"],
                "recommended_followups": [],
            },
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_stage2_forum_loop_then_merge(monkeypatch):
    shared = _build_shared()

    forum_outputs = iter(
        [
            json.dumps(
                {
                    "cross_analysis": {"agreement": []},
                    "gaps": ["官方信息缺口"],
                    "decision": "supplement_search",
                    "directive": {"queries": ["事件 官方回应"]},
                    "synthesized_conclusions": [],
                },
                ensure_ascii=False,
            ),
            json.dumps(
                {
                    "cross_analysis": {"agreement": ["主叙事一致"]},
                    "gaps": [],
                    "decision": "sufficient",
                    "directive": {},
                    "synthesized_conclusions": ["结论A"],
                },
                ensure_ascii=False,
            ),
        ]
    )
    monkeypatch.setattr(forum_module, "call_glm46", lambda *args, **kwargs: next(forum_outputs))
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
                            "snippet": "发布权威回应",
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
                "background_context": "补充后背景",
                "consistency_points": ["一致"],
                "conflict_points": [],
                "blind_spots": [],
                "recommended_followups": ["继续跟踪"],
            },
            ensure_ascii=False,
        ),
    )

    forum_node = ForumHostNode()
    prep_1 = forum_node.prep(shared)
    exec_1 = forum_node.exec(prep_1)
    action_1 = forum_node.post(shared, prep_1, exec_1)
    assert action_1 == "supplement_search"

    supplement_node = SupplementSearchNode()
    sup_prep = supplement_node.prep(shared)
    sup_exec = supplement_node.exec(sup_prep)
    sup_action = supplement_node.post(shared, sup_prep, sup_exec)
    assert sup_action == "default"

    prep_2 = forum_node.prep(shared)
    exec_2 = forum_node.exec(prep_2)
    action_2 = forum_node.post(shared, prep_2, exec_2)
    assert action_2 == "sufficient"

    merge_node = MergeResultsNode()
    m_prep = merge_node.prep(shared)
    m_exec = merge_node.exec(m_prep)
    merge_node.post(shared, m_prep, m_exec)

    assert shared["stage2_results"]["search_context"]["background_context"] == "补充后背景"
    assert shared["stage2_results"]["search_context"]["forum_conclusions"] == ["结论A"]
    assert shared["trace"]["loop_status"]["forum"]["termination_reason"] == "forum_host_sufficient"


def test_stage2_forum_visual_loop_then_merge(monkeypatch):
    shared = _build_shared()
    shared["agent_results"]["data_agent"]["charts"] = [
        {"id": "c1", "title": "趋势图", "path": "report/images/c1.png"}
    ]

    forum_outputs = iter(
        [
            json.dumps(
                {
                    "cross_analysis": {"agreement": []},
                    "gaps": ["视觉证据不足"],
                    "decision": "supplement_visual",
                    "directive": {
                        "charts": ["c1"],
                        "question": "请分析趋势变化与异常点。",
                    },
                    "synthesized_conclusions": [],
                },
                ensure_ascii=False,
            ),
            json.dumps(
                {
                    "cross_analysis": {"agreement": ["视觉证据与数据一致"]},
                    "gaps": [],
                    "decision": "sufficient",
                    "directive": {},
                    "synthesized_conclusions": ["结论V"],
                },
                ensure_ascii=False,
            ),
        ]
    )
    monkeypatch.setattr(forum_module, "call_glm46", lambda *args, **kwargs: next(forum_outputs))
    monkeypatch.setattr(
        visual_module,
        "call_glm45v_thinking",
        lambda *args, **kwargs: "图表在中段出现明显峰值，后续回落。",
    )

    forum_node = ForumHostNode()
    prep_1 = forum_node.prep(shared)
    exec_1 = forum_node.exec(prep_1)
    action_1 = forum_node.post(shared, prep_1, exec_1)
    assert action_1 == "supplement_visual"

    visual_node = VisualAnalysisNode()
    vis_prep = visual_node.prep(shared)
    vis_exec = visual_node.exec(vis_prep)
    vis_action = visual_node.post(shared, vis_prep, vis_exec)
    assert vis_action == "default"

    prep_2 = forum_node.prep(shared)
    exec_2 = forum_node.exec(prep_2)
    action_2 = forum_node.post(shared, prep_2, exec_2)
    assert action_2 == "sufficient"

    merge_node = MergeResultsNode()
    m_prep = merge_node.prep(shared)
    m_exec = merge_node.exec(m_prep)
    merge_node.post(shared, m_prep, m_exec)

    visual_analyses = shared["stage2_results"]["search_context"]["visual_analyses"]
    assert len(visual_analyses) == 1
    assert visual_analyses[0]["chart_id"] == "c1"
    assert shared["stage2_results"]["search_context"]["forum_conclusions"] == ["结论V"]
