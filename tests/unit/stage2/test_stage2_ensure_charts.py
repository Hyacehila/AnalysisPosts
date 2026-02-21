"""
EnsureChartsNode fallback behavior.
"""
import pytest
from unittest.mock import patch

from nodes.stage2.agent import EnsureChartsNode


def test_ensure_charts_adds_missing_category():
    shared = {
        "stage2_results": {
            "charts": [],
            "tables": [],
            "execution_log": {"tools_executed": []},
        },
        "agent": {
            "available_tools": [{
                "name": "sentiment_trend_chart",
                "canonical_name": "sentiment_trend_chart",
                "category": "情感趋势分析",
                "description": "情感趋势图",
                "generates_chart": True,
            }],
            "execution_history": [],
        },
        "config": {
            "stage2_chart": {
                "min_per_category": {"sentiment": 1},
                "tool_policy": "coverage_first",
                "tool_allowlist": [],
            },
            "data_source": {"enhanced_data_path": ""},
        },
    }

    node = EnsureChartsNode()
    prep_res = node.prep(shared)

    fake_result = {
        "charts": [{
            "id": "sentiment_trend_1",
            "title": "情感趋势图",
            "file_path": "report/images/sentiment_trend_1.png",
            "source_tool": "sentiment_trend_chart",
        }],
        "summary": "ok",
    }

    with patch("utils.mcp_client.mcp_client.call_tool", return_value=fake_result):
        exec_res = node.exec(prep_res)

    node.post(shared, prep_res, exec_res)

    assert len(shared["stage2_results"]["charts"]) == 1
    assert shared["stage2_results"]["execution_log"]["total_charts"] == 1
    assert shared["stage2_results"]["execution_log"]["charts_by_category"]["sentiment"] == 1


def test_ensure_charts_fail_fast_when_missing():
    shared = {
        "stage2_results": {
            "charts": [],
            "tables": [],
            "execution_log": {"tools_executed": []},
        },
        "agent": {
            "available_tools": [{
                "name": "sentiment_trend_chart",
                "canonical_name": "sentiment_trend_chart",
                "category": "情感趋势分析",
                "description": "情感趋势图",
                "generates_chart": True,
            }],
            "execution_history": [],
        },
        "config": {
            "stage2_chart": {
                "min_per_category": {"sentiment": 1},
                "tool_policy": "coverage_first",
                "tool_allowlist": [],
                "missing_policy": "fail",
            },
            "data_source": {"enhanced_data_path": ""},
        },
    }

    node = EnsureChartsNode()
    prep_res = node.prep(shared)

    fake_result = {"charts": [], "summary": "no data"}

    with patch("utils.mcp_client.mcp_client.call_tool", return_value=fake_result):
        exec_res = node.exec(prep_res)

    with pytest.raises(RuntimeError):
        node.post(shared, prep_res, exec_res)
