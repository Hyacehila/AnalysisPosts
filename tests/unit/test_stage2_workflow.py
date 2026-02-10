"""
test_stage2_workflow.py — Stage 2 Workflow 路径节点单元测试

覆盖范围:
  - LoadEnhancedDataNode: 增强数据加载 & 增强完整率计算
  - DataSummaryNode: 多维统计摘要生成
  - ExecuteAnalysisScriptNode.post: 结果存储到 shared
  - ChartAnalysisNode: 图表分析流程（LLM mocked）
  - SaveAnalysisResultsNode: 分析结果持久化
  - LLMInsightNode: 洞察生成（LLM mocked）
  - Stage2CompletionNode: 阶段完成标记
"""
import sys
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from nodes import (
    LoadEnhancedDataNode,
    DataSummaryNode,
    ExecuteAnalysisScriptNode,
    ChartAnalysisNode,
    SaveAnalysisResultsNode,
    LLMInsightNode,
    Stage2CompletionNode,
)


# =============================================================================
# LoadEnhancedDataNode
# =============================================================================

class TestLoadEnhancedDataNode:

    @patch("os.path.exists", return_value=False)
    def test_prep_raises_if_file_missing(self, mock_exists, minimal_shared):
        node = LoadEnhancedDataNode()
        with pytest.raises(FileNotFoundError, match="阶段1输出文件不存在"):
            node.prep(minimal_shared)

    @patch("os.path.exists", return_value=True)
    def test_prep_returns_data_path(self, mock_exists, minimal_shared):
        node = LoadEnhancedDataNode()
        result = node.prep(minimal_shared)
        assert "data_path" in result

    @patch("nodes.stage2.load_enhanced_blog_data")
    def test_exec_counts_valid_posts(self, mock_load, sample_enhanced_data):
        mock_load.return_value = sample_enhanced_data
        node = LoadEnhancedDataNode()
        result = node.exec({"data_path": "data/enh.json"})
        assert result["total_count"] == 3
        assert result["valid_count"] == 3
        assert result["enhancement_rate"] == 100.0

    @patch("nodes.stage2.load_enhanced_blog_data")
    def test_exec_partial_enhancement(self, mock_load, sample_blog_data):
        """部分增强 → valid_count < total_count"""
        mock_load.return_value = sample_blog_data  # 无增强字段
        node = LoadEnhancedDataNode()
        result = node.exec({"data_path": "data/enh.json"})
        assert result["valid_count"] == 0
        assert result["enhancement_rate"] == 0

    def test_post_stores_blog_data(self, minimal_shared, sample_enhanced_data):
        node = LoadEnhancedDataNode()
        exec_res = {
            "blog_data": sample_enhanced_data,
            "total_count": 3,
            "valid_count": 3,
            "enhancement_rate": 100.0,
        }
        action = node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["data"]["blog_data"] == sample_enhanced_data
        assert action == "default"


# =============================================================================
# DataSummaryNode
# =============================================================================

class TestDataSummaryNode:

    def test_prep_extracts_blog_data(self, enhanced_shared):
        node = DataSummaryNode()
        result = node.prep(enhanced_shared)
        assert len(result) == 3

    def test_exec_generates_statistics(self, sample_enhanced_data):
        node = DataSummaryNode()
        result = node.exec(sample_enhanced_data)
        stats = result["statistics"]
        assert stats["total_posts"] == 3
        assert "sentiment_distribution" in stats
        assert "publisher_distribution" in stats
        assert "topic_distribution" in stats
        assert "location_distribution" in stats
        assert stats["engagement"]["total_likes"] == 210

    def test_exec_empty_data(self):
        node = DataSummaryNode()
        result = node.exec([])
        assert result["summary"] == "无数据"

    def test_exec_time_range(self, sample_enhanced_data):
        node = DataSummaryNode()
        result = node.exec(sample_enhanced_data)
        tr = result["statistics"]["time_range"]
        assert tr["start"] == "2024-11-09 10:00:00"
        assert tr["end"] == "2024-11-10 08:00:00"
        assert tr["span_hours"] > 0

    def test_post_stores_summary(self, enhanced_shared, sample_enhanced_data):
        node = DataSummaryNode()
        exec_res = node.exec(sample_enhanced_data)
        action = node.post(enhanced_shared, sample_enhanced_data, exec_res)
        assert "data_summary" in enhanced_shared["agent"]
        assert "data_statistics" in enhanced_shared["agent"]
        assert action == "default"


# =============================================================================
# ExecuteAnalysisScriptNode.post （exec 过于复杂且直接调用分析工具，只测 post）
# =============================================================================

class TestExecuteAnalysisScriptNodePost:

    def test_post_stores_results(self, enhanced_shared):
        node = ExecuteAnalysisScriptNode()
        exec_res = {
            "charts": [{"id": "c1", "title": "Chart1"}],
            "tables": [{"id": "t1", "title": "Table1"}],
            "tools_executed": ["sentiment_distribution_stats"],
            "execution_time": 1.5,
        }
        action = node.post(enhanced_shared, [], exec_res)
        s2 = enhanced_shared["stage2_results"]
        assert len(s2["charts"]) == 1
        assert len(s2["tables"]) == 1
        assert s2["execution_log"]["total_charts"] == 1
        assert action == "default"


# =============================================================================
# ChartAnalysisNode
# =============================================================================

class TestChartAnalysisNode:

    def test_prep_reads_charts(self, enhanced_shared):
        enhanced_shared["stage2_results"] = {
            "charts": [{"id": "c1"}, {"id": "c2"}]
        }
        node = ChartAnalysisNode()
        result = node.prep(enhanced_shared)
        assert len(result) == 2

    @patch("nodes.stage2.call_glm45v_thinking", return_value="分析结果文本")
    def test_exec_analyzes_charts(self, mock_llm):
        node = ChartAnalysisNode()
        charts = [
            {"id": "chart_1", "title": "情感趋势", "type": "line", "path": "img.png"},
        ]
        result = node.exec(charts)
        assert result["total_charts"] == 1
        assert result["success_count"] == 1
        assert "chart_1" in result["chart_analyses"]
        assert result["chart_analyses"]["chart_1"]["analysis_status"] == "success"

    @patch("nodes.stage2.call_glm45v_thinking", side_effect=Exception("LLM Error"))
    def test_exec_handles_failure(self, mock_llm):
        node = ChartAnalysisNode()
        charts = [{"id": "chart_1", "title": "X"}]
        result = node.exec(charts)
        assert result["success_count"] == 0
        assert result["chart_analyses"]["chart_1"]["analysis_status"] == "failed"

    def test_exec_empty_charts(self):
        node = ChartAnalysisNode()
        result = node.exec([])
        assert result["total_charts"] == 0
        assert result["success_rate"] == 0

    def test_post_stores_analyses(self, enhanced_shared):
        node = ChartAnalysisNode()
        exec_res = {
            "chart_analyses": {"c1": {"analysis_status": "success"}},
            "success_count": 1,
            "total_charts": 1,
            "success_rate": 1.0,
            "execution_time": 0.1,
        }
        action = node.post(enhanced_shared, [], exec_res)
        assert enhanced_shared["stage2_results"]["chart_analyses"] == exec_res["chart_analyses"]
        assert action == "default"


# =============================================================================
# SaveAnalysisResultsNode
# =============================================================================

class TestSaveAnalysisResultsNode:

    def test_prep_extracts_results(self, enhanced_shared):
        enhanced_shared["stage2_results"] = {
            "charts": [{"id": "c1"}],
            "tables": [{"id": "t1"}],
            "chart_analyses": {"c1": {}},
            "insights": {"sentiment_summary": "ok"},
            "execution_log": {},
        }
        node = SaveAnalysisResultsNode()
        result = node.prep(enhanced_shared)
        assert len(result["charts"]) == 1
        assert len(result["tables"]) == 1

    def test_exec_saves_files(self, tmp_path):
        """使用 tmp_path 避免写入真实项目目录"""
        node = SaveAnalysisResultsNode()
        prep_res = {
            "charts": [{"id": "c1", "title": "Chart1"}],
            "tables": [],
            "chart_analyses": {"c1": {"status": "ok"}},
            "insights": {"summary": "test"},
            "execution_log": {"tools": ["tool1"]},
        }
        # Mock os.makedirs and open to use tmp_path
        report_dir = tmp_path / "report"
        report_dir.mkdir()
        with patch("nodes.stage2.os.makedirs"), \
             patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__ = lambda s: s
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mock_open.return_value.write = MagicMock()

            result = node.exec(prep_res)
            assert result["success"] is True
            assert result["charts_count"] == 1

    def test_post_records_output_files(self, enhanced_shared):
        node = SaveAnalysisResultsNode()
        exec_res = {
            "success": True,
            "analysis_data_path": "report/analysis_data.json",
            "chart_analyses_path": "report/chart_analyses.json",
            "insights_path": "report/insights.json",
            "charts_count": 2,
            "tables_count": 1,
            "chart_analyses_count": 2,
        }
        action = node.post(enhanced_shared, {}, exec_res)
        assert "output_files" in enhanced_shared["stage2_results"]
        assert action == "default"


# =============================================================================
# LLMInsightNode
# =============================================================================

class TestLLMInsightNode:

    def test_prep_reads_chart_analyses(self, enhanced_shared):
        enhanced_shared["stage2_results"]["chart_analyses"] = {"c1": {"status": "ok"}}
        enhanced_shared["stage2_results"]["tables"] = [{"id": "t1"}]
        enhanced_shared["agent"] = {"data_summary": "summary text"}
        node = LLMInsightNode()
        result = node.prep(enhanced_shared)
        assert "chart_analyses" in result
        assert "tables" in result
        assert result["data_summary"] == "summary text"

    @patch("nodes.stage2.call_glm46", return_value='{"sentiment_summary":"ok","topic_distribution":"ok","geographic_distribution":"ok","publisher_behavior":"ok","overall_summary":"ok"}')
    def test_exec_generates_insights(self, mock_llm):
        node = LLMInsightNode()
        prep_res = {
            "chart_analyses": {},
            "tables": [],
            "data_summary": "test",
        }
        result = node.exec(prep_res)
        assert "sentiment_summary" in result
        assert result["sentiment_summary"] == "ok"

    def test_post_stores_insights(self, enhanced_shared):
        node = LLMInsightNode()
        exec_res = {"sentiment_summary": "好", "overall_summary": "全"}
        action = node.post(enhanced_shared, {}, exec_res)
        assert enhanced_shared["stage2_results"]["insights"] == exec_res
        assert action == "default"
