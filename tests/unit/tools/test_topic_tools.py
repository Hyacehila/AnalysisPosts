"""
Characterization tests for topic_tools.
"""
from pathlib import Path

import pytest

from utils.analysis_tools import TOOL_REGISTRY


def _run_tool(tool_info, blog_data, tmp_path):
    func = tool_info["function"]
    params = {"blog_data": blog_data}
    for name, meta in tool_info.get("parameters", {}).items():
        if name == "blog_data":
            continue
        if name in {"output_dir", "data_dir"}:
            params[name] = str(tmp_path)
        elif "default" in meta:
            params[name] = meta["default"]
    return func(**params)


@pytest.mark.parametrize(
    "tool_name",
    [
        name for name, info in TOOL_REGISTRY.items()
        if info["category"] == "主题演化分析"
    ]
)
def test_topic_tools_run(sample_enhanced_data, tmp_path, tool_name):
    tool_info = TOOL_REGISTRY[tool_name]
    result = _run_tool(tool_info, sample_enhanced_data, tmp_path)
    assert isinstance(result, dict)
    if tool_info["generates_chart"]:
        assert "charts" in result
        charts = result.get("charts") or []
        for ch in charts:
            path = ch.get("file_path") or ch.get("path") or ch.get("chart_path")
            if path:
                assert Path(path).exists()
    else:
        assert "data" in result
