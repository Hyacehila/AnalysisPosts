"""
Characterization tests for belief_tools.
"""
from pathlib import Path

from utils.analysis_tools import TOOL_REGISTRY


def test_belief_network_chart(sample_enhanced_data, tmp_path):
    tool_info = TOOL_REGISTRY["belief_network_chart"]
    func = tool_info["function"]
    result = func(
        blog_data=sample_enhanced_data,
        output_dir=str(tmp_path),
        data_dir=str(tmp_path),
        event_name="test_event",
    )
    assert isinstance(result, dict)
    assert "charts" in result
    charts = result.get("charts") or []
    for ch in charts:
        path = ch.get("file_path") or ch.get("path") or ch.get("chart_path")
        if path:
            assert Path(path).exists()
