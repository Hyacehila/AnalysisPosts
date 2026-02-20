"""
PlanOutlineNode unit tests.
"""
from unittest.mock import patch

from nodes import PlanOutlineNode


def _shared_for_outline():
    return {
        "agent": {"data_summary": {"total_posts": 30}},
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
