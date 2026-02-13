"""
test_flow_integration.py — P4 端到端流程集成测试

验证多个节点在管道中的串联行为:
  - 完整 Stage 1 数据加载 → 验证 → 保存流程
  - Stage 2 数据摘要 → 分析 → 保存 → 洞察流程
  - Stage 3 加载 → 格式化 → 保存流程
  - Dispatcher 多阶段调度完整流程
"""
import sys
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from nodes import (
    DataLoadNode,
    DataValidationAndOverviewNode,
    SaveEnhancedDataNode,
    DataSummaryNode,
    ChartAnalysisNode,
    SaveAnalysisResultsNode,
    LLMInsightNode,
    FormatReportNode,
    SaveReportNode,
    LoadAnalysisResultsNode,
    GenerateFullReportNode,
    InitReportStateNode,
    ReviewReportNode,
    DispatcherNode,
    Stage1CompletionNode,
    Stage2CompletionNode,
    Stage3CompletionNode,
    TerminalNode,
)


def _complete_stage(shared, CompletionNodeClass):
    """辅助函数：完整执行 CompletionNode 的 prep→exec→post"""
    node = CompletionNodeClass()
    p = node.prep(shared)
    e = node.exec(p)
    return node.post(shared, p, e)


# =============================================================================
# Stage 1 Pipeline Flow
# =============================================================================

class TestStage1PipelineFlow:
    """DataLoad → Validation → Save 串联流程"""

    @patch("nodes.stage1.save.save_enhanced_blog_data", return_value=True)
    @patch("nodes.stage1.data_load.load_publisher_decisions", return_value=["type1"])
    @patch("nodes.stage1.data_load.load_belief_system", return_value=[])
    @patch("nodes.stage1.data_load.load_publisher_objects")
    @patch("nodes.stage1.data_load.load_sentiment_attributes")
    @patch("nodes.stage1.data_load.load_topics")
    @patch("nodes.stage1.data_load.load_blog_data")
    def test_full_stage1_pipeline(
        self, mock_blog, mock_topics, mock_sa, mock_po,
        mock_bs, mock_pd, mock_save,
        sample_blog_data, sample_topics, sample_sentiment_attrs,
        sample_publishers, minimal_shared,
    ):
        mock_blog.return_value = sample_blog_data
        mock_topics.return_value = sample_topics
        mock_sa.return_value = sample_sentiment_attrs
        mock_po.return_value = sample_publishers

        # Step 1: DataLoadNode
        load_node = DataLoadNode()
        prep = load_node.prep(minimal_shared)
        exec_res = load_node.exec(prep)
        load_node.post(minimal_shared, prep, exec_res)

        assert len(minimal_shared["data"]["blog_data"]) == 3
        assert minimal_shared["stage1_results"]["statistics"]["total_blogs"] == 3

        # Step 2: DataValidationAndOverviewNode
        val_node = DataValidationAndOverviewNode()
        val_prep = val_node.prep(minimal_shared)
        val_exec = val_node.exec(val_prep)
        val_node.post(minimal_shared, val_prep, val_exec)

        stats = minimal_shared["stage1_results"]["statistics"]
        assert stats["total_blogs"] == 3
        assert stats["processed_blogs"] == 0  # raw data, no enhancement

        # Step 3: SaveEnhancedDataNode
        save_node = SaveEnhancedDataNode()
        save_prep = save_node.prep(minimal_shared)
        save_exec = save_node.exec(save_prep)
        save_node.post(minimal_shared, save_prep, save_exec)

        assert minimal_shared["stage1_results"]["data_save"]["saved"] is True

    @patch("nodes.stage1.save.save_enhanced_blog_data", return_value=True)
    @patch("nodes.stage1.data_load.load_publisher_decisions", return_value=[])
    @patch("nodes.stage1.data_load.load_belief_system", return_value=[])
    @patch("nodes.stage1.data_load.load_publisher_objects")
    @patch("nodes.stage1.data_load.load_sentiment_attributes")
    @patch("nodes.stage1.data_load.load_topics")
    @patch("nodes.stage1.data_load.load_blog_data")
    def test_stage1_with_enhanced_data(
        self, mock_blog, mock_topics, mock_sa, mock_po,
        mock_bs, mock_pd, mock_save,
        sample_enhanced_data, sample_topics, sample_sentiment_attrs,
        sample_publishers, minimal_shared,
    ):
        """增强后的数据通过验证可看到 processed_blogs > 0"""
        mock_blog.return_value = sample_enhanced_data
        mock_topics.return_value = sample_topics
        mock_sa.return_value = sample_sentiment_attrs
        mock_po.return_value = sample_publishers

        load_node = DataLoadNode()
        prep = load_node.prep(minimal_shared)
        exec_res = load_node.exec(prep)
        load_node.post(minimal_shared, prep, exec_res)

        val_node = DataValidationAndOverviewNode()
        val_prep = val_node.prep(minimal_shared)
        val_exec = val_node.exec(val_prep)

        assert val_exec["processed_blogs"] == 3
        assert val_exec["empty_fields"]["sentiment_polarity_empty"] == 0


# =============================================================================
# Stage 2 Summary → Analysis Flow
# =============================================================================

class TestStage2AnalysisFlow:
    """DataSummary → ChartAnalysis → SaveResults → Insight 串联"""

    @patch("nodes.stage2.insight.call_glm46", return_value='{"sentiment_summary":"s","topic_distribution":"t","geographic_distribution":"g","publisher_behavior":"p","overall_summary":"o"}')
    @patch("nodes.stage2.chart_analysis.call_glm45v_thinking", return_value="分析结果")
    def test_summary_to_insight_pipeline(self, mock_chart_llm, mock_insight_llm, enhanced_shared, sample_enhanced_data):
        # Ensure stage2_results exists
        enhanced_shared["stage2_results"] = {}

        # Step 1: DataSummaryNode
        summary_node = DataSummaryNode()
        sum_prep = summary_node.prep(enhanced_shared)
        sum_exec = summary_node.exec(sum_prep)
        summary_node.post(enhanced_shared, sum_prep, sum_exec)

        assert "data_summary" in enhanced_shared["agent"]
        assert enhanced_shared["agent"]["data_statistics"]["total_posts"] == 3

        # Step 2: 模拟已有 charts (ExecuteAnalysisScript 太复杂，直接注入)
        enhanced_shared["stage2_results"]["charts"] = [
            {"id": "c1", "title": "情感趋势", "type": "line"},
        ]
        enhanced_shared["stage2_results"]["tables"] = [
            {"id": "t1", "title": "情感分布", "data": {"summary": "测试"}},
        ]

        # Step 3: ChartAnalysisNode
        chart_node = ChartAnalysisNode()
        chart_prep = chart_node.prep(enhanced_shared)
        chart_exec = chart_node.exec(chart_prep)
        chart_node.post(enhanced_shared, chart_prep, chart_exec)

        assert "chart_analyses" in enhanced_shared["stage2_results"]
        assert enhanced_shared["stage2_results"]["chart_analyses"]["c1"]["analysis_status"] == "success"

        # Step 4: LLMInsightNode
        insight_node = LLMInsightNode()
        insight_prep = insight_node.prep(enhanced_shared)
        insight_exec = insight_node.exec(insight_prep)
        insight_node.post(enhanced_shared, insight_prep, insight_exec)

        assert "insights" in enhanced_shared["stage2_results"]
        assert enhanced_shared["stage2_results"]["insights"]["sentiment_summary"] == "s"


# =============================================================================
# Stage 3 Report Flow
# =============================================================================

class TestStage3ReportFlow:
    """LoadAnalysisResults → FormatReport → SaveReport"""

    @patch("nodes.stage3.format._load_analysis_charts", return_value=[])
    def test_format_and_save_pipeline(self, mock_charts, minimal_shared):
        # 准备数据
        report_content = "# 舆情分析报告\n\n## 引言\n内容\n\n## 分析\n![图](report/images/c1.png)"
        minimal_shared["report"] = {"full_content": report_content}
        minimal_shared["stage3_results"] = {}

        # Step 1: FormatReportNode
        fmt_node = FormatReportNode()
        fmt_prep = fmt_node.prep(minimal_shared)
        fmt_exec = fmt_node.exec(fmt_prep)
        fmt_node.post(minimal_shared, fmt_prep, fmt_exec)

        formatted = minimal_shared["stage3_results"]["final_report_text"]
        # 路径已标准化
        assert "./images/c1.png" in formatted
        # 目录已添加
        assert "## 目录" in formatted

        # Step 2: SaveReportNode.prep
        save_node = SaveReportNode()
        save_prep = save_node.prep(minimal_shared)
        assert save_prep == formatted


# =============================================================================
# Dispatcher Multi-Stage Flow
# =============================================================================

class TestDispatcherMultiStageFlow:

    def test_three_stage_sequential_dispatch(self, minimal_shared):
        """模拟三阶段顺序完成的 Dispatcher 调度"""
        minimal_shared["dispatcher"]["run_stages"] = [1, 2, 3]
        minimal_shared["config"]["enhancement_mode"] = "async"
        minimal_shared["config"]["analysis_mode"] = "agent"
        minimal_shared["config"]["tool_source"] = "mcp"
        minimal_shared["config"]["report_mode"] = "template"

        dispatcher = DispatcherNode()

        # --- 第 1 轮: 进入 Stage 1 ---
        p1 = dispatcher.prep(minimal_shared)
        e1 = dispatcher.exec(p1)
        a1 = dispatcher.post(minimal_shared, p1, e1)
        assert a1 == "stage1_async"
        assert minimal_shared["dispatcher"]["current_stage"] == 1

        # 模拟 Stage 1 完成 (完整 prep→exec→post)
        _complete_stage(minimal_shared, Stage1CompletionNode)
        assert 1 in minimal_shared["dispatcher"]["completed_stages"]

        # --- 第 2 轮: 进入 Stage 2 ---
        p2 = dispatcher.prep(minimal_shared)
        e2 = dispatcher.exec(p2)
        a2 = dispatcher.post(minimal_shared, p2, e2)
        assert a2 == "stage2_agent"
        assert minimal_shared["dispatcher"]["current_stage"] == 2

        # 模拟 Stage 2 完成
        _complete_stage(minimal_shared, Stage2CompletionNode)
        assert 2 in minimal_shared["dispatcher"]["completed_stages"]

        # --- 第 3 轮: 进入 Stage 3 ---
        p3 = dispatcher.prep(minimal_shared)
        e3 = dispatcher.exec(p3)
        a3 = dispatcher.post(minimal_shared, p3, e3)
        assert a3 == "stage3_template"
        assert minimal_shared["dispatcher"]["current_stage"] == 3

        # 模拟 Stage 3 完成
        _complete_stage(minimal_shared, Stage3CompletionNode)
        assert 3 in minimal_shared["dispatcher"]["completed_stages"]

        # --- 第 4 轮: 全部完成 → terminal ---
        p4 = dispatcher.prep(minimal_shared)
        e4 = dispatcher.exec(p4)
        a4 = dispatcher.post(minimal_shared, p4, e4)
        assert a4 == "done"

    def test_single_stage_dispatch(self, minimal_shared):
        """只运行 Stage 2"""
        minimal_shared["dispatcher"]["run_stages"] = [2]
        minimal_shared["dispatcher"]["start_stage"] = 2
        minimal_shared["config"]["analysis_mode"] = "agent"

        dispatcher = DispatcherNode()
        p1 = dispatcher.prep(minimal_shared)
        e1 = dispatcher.exec(p1)
        a1 = dispatcher.post(minimal_shared, p1, e1)
        assert a1 == "stage2_agent"

        # 完成
        _complete_stage(minimal_shared, Stage2CompletionNode)

        p2 = dispatcher.prep(minimal_shared)
        e2 = dispatcher.exec(p2)
        a2 = dispatcher.post(minimal_shared, p2, e2)
        assert a2 == "done"

    def test_terminal_generates_summary(self, minimal_shared):
        """TerminalNode 生成最终摘要"""
        minimal_shared["dispatcher"]["completed_stages"] = [1, 2, 3]
        minimal_shared["stage1_results"]["statistics"]["total_blogs"] = 100
        minimal_shared["stage1_results"]["data_save"]["saved"] = True
        minimal_shared["stage3_results"] = {"report_file": "report/report.md"}

        terminal = TerminalNode()
        prep = terminal.prep(minimal_shared)
        exec_res = terminal.exec(prep)
        action = terminal.post(minimal_shared, prep, exec_res)

        assert "final_summary" in minimal_shared
        assert action == "default"  # TerminalNode 返回 "default"
