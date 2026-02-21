"""
PlanOutlineNode unit tests.
"""
from unittest.mock import patch

from nodes import PlanOutlineNode


def _shared_for_outline():
    return {
        "agent": {"data_summary": {"total_posts": 30}},
        "analysis_context": {
            "time_range": {
                "start": "2024-08-16 10:00:00",
                "end": "2024-08-31 22:00:00",
                "span_hours": 373.0,
            },
            "time_range_text": "2024-08-16 10:00:00 至 2024-08-31 22:00:00",
            "user_analysis_instruction": "重点分析官方回应时效性",
        },
        "stage3_data": {
            "analysis_data": {
                "charts": [{"id": "c1", "title": "情感趋势"}],
                "tables": [],
            },
            "insights": {"summary": "整体情绪趋稳"},
            "trace": {"forum_rounds": [{"round": 1, "decision": "sufficient"}]},
        },
        "stage3_results": {},
    }


@patch("nodes.stage3.outline.call_glm46")
def test_outline_parses_llm_json(mock_llm):
    mock_llm.return_value = (
        '{"title":"测试报告","chapters":[{"id":"ch01","title":"执行摘要","target_words":300}]}'
    )
    shared = _shared_for_outline()

    node = PlanOutlineNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert shared["stage3_results"]["outline"]["title"] == "测试报告"
    assert shared["stage3_results"]["outline"]["chapters"][0]["id"] == "ch01"


@patch("nodes.stage3.outline.call_glm46", return_value="not-json")
def test_outline_uses_fallback_when_llm_invalid(_mock_llm):
    shared = _shared_for_outline()

    node = PlanOutlineNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    outline = shared["stage3_results"]["outline"]
    assert outline["title"]
    assert len(outline["chapters"]) >= 3
    assert outline["chapters"][0]["id"]


@patch("nodes.stage3.outline.call_glm46")
def test_outline_respects_stage3_reasoning_switch(mock_llm):
    mock_llm.return_value = (
        '{"title":"测试报告","chapters":[{"id":"ch01","title":"执行摘要","target_words":300}]}'
    )
    shared = _shared_for_outline()
    shared["config"] = {"llm": {"reasoning_enabled_stage3": False}}

    node = PlanOutlineNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert mock_llm.call_args.kwargs["enable_reasoning"] is False


@patch("nodes.stage3.outline.call_glm46")
def test_outline_prompt_contains_real_insight_context_and_placeholder_guard(mock_llm):
    mock_llm.return_value = (
        '{"title":"测试报告","chapters":[{"id":"ch01","title":"执行摘要","target_words":300}]}'
    )
    shared = _shared_for_outline()
    shared["stage3_data"]["insights"] = {
        "overall_summary": "首都骑游文明公约与张艺兴夜骑在8月21日出现峰值。"
    }

    node = PlanOutlineNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    prompt = mock_llm.call_args.args[0]
    assert "首都骑游文明公约与张艺兴夜骑在8月21日出现峰值" in prompt
    assert "占位符" in prompt
    assert "分析时间范围" in prompt
    assert "2024-08-16 10:00:00 至 2024-08-31 22:00:00" in prompt
    assert "用户分析指令" in prompt
    assert "重点分析官方回应时效性" in prompt
