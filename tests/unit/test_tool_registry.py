"""
Tests for tool registry helpers.
"""
from utils.analysis_tools.tool_registry import (
    TOOL_REGISTRY,
    get_all_tools,
    get_tool_by_name,
    execute_tool,
    get_tools_by_category,
    get_chart_tools,
    get_data_tools,
)


def test_get_all_tools_contains_registry():
    tools = get_all_tools()
    names = {t["name"] for t in tools}
    assert set(TOOL_REGISTRY.keys()).issubset(names)


def test_get_tool_by_name_returns_tool():
    tool = get_tool_by_name("sentiment_distribution_stats")
    assert tool is not None
    assert tool["name"] == "sentiment_distribution_stats"


def test_execute_tool_returns_result(sample_enhanced_data):
    result = execute_tool("sentiment_distribution_stats", sample_enhanced_data)
    assert "data" in result
    assert result["tool_name"] == "sentiment_distribution_stats"


def test_get_tools_by_category():
    tools = get_tools_by_category("情感趋势分析")
    assert tools


def test_chart_and_data_tools_split():
    chart_tools = set(get_chart_tools())
    data_tools = set(get_data_tools())
    assert chart_tools
    assert data_tools
    assert chart_tools.isdisjoint(data_tools)
