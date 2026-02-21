"""
SearchAgent node unit tests.
"""
import nodes.stage2.search_agent as search_agent_module
from nodes.stage2.search_agent import SearchAgentNode


def _build_shared():
    return {
        "agent": {
            "data_summary": "样本数据集中讨论焦点集中在城市内涝和应急响应。",
        },
        "search_results": {
            "event_timeline": ["7月31日发生强降雨"],
            "key_actors": ["市应急管理局"],
            "official_responses": ["发布橙色预警"],
            "public_reactions_summary": "公众关注交通与排水",
            "related_events": [],
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_search_agent_writes_agent_results(monkeypatch):
    shared = _build_shared()
    node = SearchAgentNode()

    monkeypatch.setattr(
        search_agent_module,
        "call_glm46",
        lambda *args, **kwargs: (
            '{"background_context":"背景补充",'
            '"consistency_points":["舆情高峰与降雨时段一致"],'
            '"conflict_points":["样本中官方回应覆盖不足"],'
            '"blind_spots":["跨城传播链"],'
            '"recommended_followups":["继续跟踪发布会信息"]}'
        ),
    )

    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert shared["agent_results"]["search_agent"]["background_context"] == "背景补充"
    assert shared["agent_results"]["search_agent"]["blind_spots"] == ["跨城传播链"]
    assert len(shared["trace"]["search_agent_analysis"]) == 1


def test_search_agent_fallback_when_llm_json_invalid(monkeypatch):
    shared = _build_shared()
    node = SearchAgentNode()

    monkeypatch.setattr(search_agent_module, "call_glm46", lambda *args, **kwargs: "not json")

    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert shared["agent_results"]["search_agent"]["background_context"] != ""
    assert isinstance(shared["agent_results"]["search_agent"]["blind_spots"], list)


def test_search_agent_respects_stage2_reasoning_switch(monkeypatch):
    shared = _build_shared()
    shared["config"] = {"llm": {"reasoning_enabled_stage2": False}}
    captured = {}

    def _fake_llm(*args, **kwargs):
        captured["kwargs"] = kwargs
        return (
            '{"background_context":"背景补充",'
            '"consistency_points":[],"conflict_points":[],"blind_spots":[],"recommended_followups":[]}'
        )

    monkeypatch.setattr(search_agent_module, "call_glm46", _fake_llm)

    node = SearchAgentNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert captured["kwargs"]["enable_reasoning"] is False
