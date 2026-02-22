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
        "analysis_context": {
            "time_range": {
                "start": "2024-08-16 10:00:00",
                "end": "2024-08-31 22:00:00",
                "span_hours": 373.0,
            },
            "time_range_text": "2024-08-16 10:00:00 至 2024-08-31 22:00:00",
            "user_analysis_instruction": "重点分析官方部门回应是否充分",
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
                "host_narrative": "主持人认为当前人群结构证据不足，应补充发布者类型分析。",
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
    assert shared["forum"]["debate_logs"] == ["主持人认为当前人群结构证据不足，应补充发布者类型分析。"]
    assert len(shared["trace"]["forum_rounds"]) == 1
    assert shared["trace"]["forum_rounds"][0]["host_narrative"] == "主持人认为当前人群结构证据不足，应补充发布者类型分析。"
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
                "host_narrative": "主持人认为第一轮结论尚需验证。",
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
                "host_narrative": "主持人建议继续追踪官方回应。",
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
                "host_narrative": "主持人建议补齐图表视觉解释。",
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


def test_forum_host_respects_stage2_reasoning_switch(monkeypatch):
    shared = _build_shared()
    shared["config"]["llm"] = {"reasoning_enabled_stage2": False}
    captured = {}

    def _fake_call(*args, **kwargs):
        captured["kwargs"] = kwargs
        return json.dumps(
            {
                "cross_analysis": {"agreement": []},
                "gaps": [],
                "decision": "sufficient",
                "directive": {},
                "host_narrative": "主持人确认当前证据可收敛。",
                "synthesized_conclusions": [],
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(forum_module, "call_glm46", _fake_call)

    node = ForumHostNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert captured["kwargs"]["enable_reasoning"] is False


def test_forum_prompt_contains_explicit_event_keyword_context(monkeypatch):
    shared = _build_shared()
    shared["agent"]["data_summary"] = (
        "首都骑游文明公约发布后，张艺兴夜骑话题在共享单车讨论中升温。"
    )
    captured = {}

    def _fake_call(*args, **kwargs):
        captured["prompt"] = args[0]
        return json.dumps(
            {
                "cross_analysis": {"agreement": []},
                "gaps": [],
                "decision": "sufficient",
                "directive": {},
                "host_narrative": "主持人综合观点并准备收敛。",
                "synthesized_conclusions": [],
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(forum_module, "call_glm46", _fake_call)

    node = ForumHostNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert "事件关键词" in captured["prompt"]
    assert "分析时间范围" in captured["prompt"]
    assert "2024-08-16 10:00:00 至 2024-08-31 22:00:00" in captured["prompt"]
    assert "用户分析指令" in captured["prompt"]
    assert "重点分析官方部门回应是否充分" in captured["prompt"]
    assert "host_narrative" in captured["prompt"]


def test_forum_exec_supports_structured_host_narrative(monkeypatch):
    shared = _build_shared()

    monkeypatch.setattr(
        forum_module,
        "call_glm46",
        lambda *args, **kwargs: json.dumps(
            {
                "cross_analysis": {"agreement": ["主叙事一致"], "conflicts": ["时效认知存在差异"]},
                "gaps": ["官方回应细节不足"],
                "decision": "supplement_search",
                "directive": {"queries": ["北京暴雨 官方发布会"], "reason": "补齐用户关注的官方回应细节"},
                "host_narrative": {
                    "timeline_analysis": "8月16日至8月31日期间讨论持续升温。",
                    "viewpoint_synthesis": "INSIGHT与MEDIA对情绪走向达成一致。",
                    "deep_analysis": "争议焦点集中于信息发布时效。",
                    "guided_questions": ["回应是否覆盖核心质疑？", "后续政策是否跟进？"],
                },
                "synthesized_conclusions": ["需补充官方发布会原文"],
            },
            ensure_ascii=False,
        ),
    )

    node = ForumHostNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "supplement_search"
    narrative = exec_res["host_narrative"]
    assert "【事件脉络】" in narrative
    assert "【观点综合】" in narrative
    assert "【深层分析】" in narrative
    assert "【引导问题】" in narrative
