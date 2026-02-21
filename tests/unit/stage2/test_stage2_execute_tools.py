"""
ExecuteToolsNode behavior for chart tools with empty outputs.
"""
from unittest.mock import patch

from nodes.stage2.agent import ExecuteToolsNode


def test_execute_tools_warn_on_empty_charts():
    shared = {
        "data": {"blog_data": []},
        "agent": {
            "next_tool": "sentiment_trend_chart",
            "tool_source": "mcp",
            "available_tools": [{
                "name": "sentiment_trend_chart",
                "canonical_name": "sentiment_trend_chart",
                "category": "情感趋势分析",
                "description": "情感趋势图",
                "generates_chart": True,
            }],
        },
        "config": {
            "stage2_chart": {"missing_policy": "fail"},
            "data_source": {"enhanced_data_path": ""},
        },
        "stage2_results": {
            "charts": [],
            "tables": [],
            "execution_log": {"tools_executed": []},
        },
    }

    node = ExecuteToolsNode()
    prep_res = node.prep(shared)

    with patch("utils.mcp_client.mcp_client.call_tool", return_value={"charts": [], "summary": "no data"}):
        exec_res = node.exec(prep_res)

    node.post(shared, prep_res, exec_res)

    assert shared["agent"]["last_tool_result"]["error"] is True
