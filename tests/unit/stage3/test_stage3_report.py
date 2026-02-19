"""
test_stage3_report.py — Stage 3 报告生成节点单元测试

覆盖范围:
  - LoadAnalysisResultsNode: 分析结果加载（内存/文件双路径）
  - FormatReportNode: 报告格式化（路径标准化 & 目录生成）
  - SaveReportNode: 报告文件持久化
  - GenerateFullReportNode.post: 一次性报告存储
  - InitReportStateNode: 迭代状态初始化
  - ReviewReportNode: LLM 评审 & 路由决策
  - Stage3CompletionNode: 阶段完成标记
"""
import sys
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    LoadAnalysisResultsNode,
    FormatReportNode,
    SaveReportNode,
    GenerateFullReportNode,
    InitReportStateNode,
    ReviewReportNode,
    Stage3CompletionNode,
)


# =============================================================================
# LoadAnalysisResultsNode
# =============================================================================

class TestLoadAnalysisResultsNode:

    def test_prep_memory_data_available(self, enhanced_shared):
        """内存中有 stage2 数据 → has_memory_data=True"""
        enhanced_shared["dispatcher"]["completed_stages"] = [1, 2]
        enhanced_shared["stage2_results"] = {
            "charts": [{"id": "c1"}],
            "chart_analyses": {},
            "tables": [],
            "insights": {},
        }
        with patch("os.path.exists", return_value=True):
            node = LoadAnalysisResultsNode()
            result = node.prep(enhanced_shared)
            assert result["has_memory_data"] is True

    @patch("os.path.exists")
    def test_prep_file_mode_missing_files(self, mock_exists, minimal_shared):
        """文件模式，缺少必要文件 → FileNotFoundError"""
        mock_exists.return_value = False
        node = LoadAnalysisResultsNode()
        with pytest.raises(FileNotFoundError):
            node.prep(minimal_shared)

    def test_exec_from_memory(self, sample_enhanced_data):
        """从内存加载 stage2 结果"""
        node = LoadAnalysisResultsNode()
        stage2_results = {
            "charts": [{"id": "c1"}],
            "tables": [{"id": "t1"}],
            "chart_analyses": {"c1": {"status": "ok"}},
            "insights": {"summary": "ok"},
            "execution_log": {},
        }
        prep_res = {
            "has_memory_data": True,
            "stage2_results": stage2_results,
            "enhanced_data_path": "",
            "images_dir": "report/images/",
            "analysis_data_path": "",
            "chart_analyses_path": "",
            "insights_path": "",
        }
        result = node.exec(prep_res)
        assert len(result["analysis_data"]["charts"]) == 1
        assert result["chart_analyses"] == {"c1": {"status": "ok"}}
        assert result["insights"] == {"summary": "ok"}

    def test_post_stores_to_shared(self, minimal_shared):
        node = LoadAnalysisResultsNode()
        exec_res = {
            "analysis_data": {},
            "chart_analyses": {},
            "insights": {},
            "sample_blogs": [],
            "images_dir": "report/images/",
        }
        action = node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["stage3_data"] == exec_res
        assert action == "default"


# =============================================================================
# FormatReportNode
# =============================================================================

class TestFormatReportNode:

    def test_prep_prefers_full_content(self, minimal_shared):
        """优先读取 report.full_content"""
        minimal_shared["report"] = {"full_content": "完整报告"}
        node = FormatReportNode()
        result = node.prep(minimal_shared)
        assert result == "完整报告"

    def test_prep_falls_back_to_draft(self, minimal_shared):
        """无 full_content → 读取 current_draft"""
        minimal_shared["report"] = {}
        minimal_shared["stage3_results"] = {"current_draft": "草稿报告"}
        node = FormatReportNode()
        result = node.prep(minimal_shared)
        assert result == "草稿报告"

    def test_exec_empty_content(self):
        node = FormatReportNode()
        result = node.exec("")
        assert result == ""

    @patch("nodes.stage3.format._load_analysis_charts", return_value=[])
    def test_exec_fixes_image_paths(self, mock_charts):
        """Windows 路径 → 标准化为 ./images/"""
        node = FormatReportNode()
        content = "![图](report/images/chart.png)"
        result = node.exec(content)
        assert "./images/chart.png" in result

    @patch("nodes.stage3.format._load_analysis_charts", return_value=[])
    def test_exec_adds_toc(self, mock_charts):
        """无目录时自动添加"""
        node = FormatReportNode()
        content = "# 引言\n正文\n## 数据分析\n内容"
        result = node.exec(content)
        assert "## 目录" in result

    @patch("nodes.stage3.format._load_analysis_charts", return_value=[])
    def test_exec_no_duplicate_toc(self, mock_charts):
        """已有目录时不重复添加"""
        node = FormatReportNode()
        content = "## 目录\n- 链接\n\n# 引言\n正文"
        result = node.exec(content)
        assert result.count("## 目录") == 1

    @patch("nodes.stage3.format._load_analysis_charts", return_value=[])
    def test_exec_ensures_trailing_newline(self, mock_charts):
        """确保结尾有换行"""
        node = FormatReportNode()
        result = node.exec("no newline")
        assert result.endswith("\n")

    def test_post_stores_formatted(self, minimal_shared):
        minimal_shared["stage3_results"] = {}
        node = FormatReportNode()
        action = node.post(minimal_shared, "raw", "formatted report\n")
        assert minimal_shared["stage3_results"]["final_report_text"] == "formatted report\n"
        assert action == "default"


# =============================================================================
# SaveReportNode
# =============================================================================

class TestSaveReportNode:

    def test_prep_reads_final_text(self, minimal_shared):
        minimal_shared["stage3_results"] = {"final_report_text": "报告全文"}
        node = SaveReportNode()
        result = node.prep(minimal_shared)
        assert result == "报告全文"

    @patch("nodes.stage3.save.get_report_dir")
    def test_exec_writes_file(self, mock_dir, tmp_path):
        node = SaveReportNode()
        report_text = "# 舆情分析报告\n\n## 引言\n"
        mock_dir.return_value = str(tmp_path)
        result = node.exec(report_text)
        assert result.endswith("report.md")

    def test_post_records_path(self, minimal_shared):
        minimal_shared["stage3_results"] = {}
        node = SaveReportNode()
        action = node.post(minimal_shared, "", "report/report.md")
        assert minimal_shared["stage3_results"]["report_file"] == "report/report.md"
        assert action == "default"


# =============================================================================
# GenerateFullReportNode.post
# =============================================================================

class TestGenerateFullReportNodePost:

    def test_post_stores_full_report(self, minimal_shared):
        minimal_shared["report"] = {}
        minimal_shared["stage3_results"] = {}
        node = GenerateFullReportNode()
        report = "# 完整报告\n## 引言\n报告内容"
        action = node.post(minimal_shared, {}, report)
        assert minimal_shared["report"]["full_content"] == report
        assert minimal_shared["report"]["generation_mode"] == "template"
        assert minimal_shared["stage3_results"]["current_draft"] == report
        assert action == "default"


# =============================================================================
# InitReportStateNode
# =============================================================================

class TestInitReportStateNode:

    def test_prep_reads_config(self, minimal_shared):
        minimal_shared["config"]["iterative_report_config"] = {"max_iterations": 3}
        node = InitReportStateNode()
        result = node.prep(minimal_shared)
        assert result["max_iterations"] == 3

    def test_exec_initializes_state(self):
        node = InitReportStateNode()
        result = node.exec({"max_iterations": 5, "stage3_data": {}})
        assert result["max_iterations"] == 5
        assert result["current_iteration"] == 0
        assert result["review_history"] == []
        assert result["current_draft"] == ""

    def test_post_updates_shared(self, minimal_shared):
        node = InitReportStateNode()
        exec_res = {
            "max_iterations": 5,
            "current_iteration": 0,
            "review_history": [],
            "revision_feedback": "",
            "current_draft": "",
        }
        action = node.post(minimal_shared, {}, exec_res)
        assert minimal_shared["report"]["max_iterations"] == 5
        assert action == "default"


# =============================================================================
# ReviewReportNode
# =============================================================================

class TestReviewReportNode:

    def _setup_shared_for_review(self, minimal_shared):
        minimal_shared["report"] = {
            "current_draft": "# 报告\n## 引言\n内容",
            "iteration": 0,
            "max_iterations": 5,
            "review_history": [],
        }
        minimal_shared["stage3_data"] = {"analysis_data": {}}
        minimal_shared["config"]["iterative_report_config"] = {
            "satisfaction_threshold": 80,
        }
        return minimal_shared

    def test_prep_extracts_review_data(self, minimal_shared):
        shared = self._setup_shared_for_review(minimal_shared)
        node = ReviewReportNode()
        result = node.prep(shared)
        assert result["current_draft"] == "# 报告\n## 引言\n内容"
        assert result["satisfaction_threshold"] == 80
        assert result["max_iterations"] == 5

    @patch("nodes.stage3.iterative.call_glm46")
    def test_exec_parses_review_result(self, mock_llm):
        mock_llm.return_value = json.dumps({
            "structure_score": 18,
            "data_support_score": 16,
            "chart_reference_score": 17,
            "logic_score": 18,
            "suggestion_score": 15,
            "total_score": 84,
            "unsupported_conclusions": [],
            "chart_reference_issues": [],
            "revision_feedback": "结构良好",
            "needs_revision": False,
            "overall_assessment": "优秀",
        })
        node = ReviewReportNode()
        prep_res = {
            "current_draft": "报告内容",
            "stage3_data": {"analysis_data": {}},
            "iteration": 0,
            "satisfaction_threshold": 80,
            "max_iterations": 5,
        }
        result = node.exec(prep_res)
        assert result["total_score"] == 84
        assert result["needs_revision"] is False

    @patch("nodes.stage3.iterative.call_glm46", return_value="invalid json!!!")
    def test_exec_fallback_on_json_error(self, mock_llm):
        """JSON 解析失败 → 使用默认评分"""
        node = ReviewReportNode()
        prep_res = {
            "current_draft": "报告",
            "stage3_data": {},
            "iteration": 0,
            "satisfaction_threshold": 80,
            "max_iterations": 5,
        }
        result = node.exec(prep_res)
        assert result["total_score"] == 75
        assert result["needs_revision"] is True

    def test_post_satisfied(self, minimal_shared):
        """评分 >= 阈值 → 返回 satisfied"""
        shared = self._setup_shared_for_review(minimal_shared)
        node = ReviewReportNode()
        prep_res = {"iteration": 0, "satisfaction_threshold": 80, "max_iterations": 5}
        exec_res = {
            "total_score": 85,
            "unsupported_conclusions": [],
            "chart_reference_issues": [],
            "revision_feedback": "",
            "needs_revision": False,
        }
        action = node.post(shared, prep_res, exec_res)
        assert action == "satisfied"
        assert shared["report"]["last_review"]["total_score"] == 85

    def test_post_needs_revision(self, minimal_shared):
        """评分 < 阈值且未达最大迭代 → 返回 needs_revision"""
        shared = self._setup_shared_for_review(minimal_shared)
        node = ReviewReportNode()
        prep_res = {"iteration": 0, "satisfaction_threshold": 80, "max_iterations": 5}
        exec_res = {
            "total_score": 60,
            "unsupported_conclusions": ["问题1"],
            "revision_feedback": "需要改进",
            "needs_revision": True,
        }
        action = node.post(shared, prep_res, exec_res)
        assert action == "needs_revision"

    def test_post_max_iterations_reached(self, minimal_shared):
        """达到最大迭代 → 强制返回 satisfied"""
        shared = self._setup_shared_for_review(minimal_shared)
        node = ReviewReportNode()
        prep_res = {
            "iteration": 4,  # 0-indexed, max=5, so this is the 5th
            "satisfaction_threshold": 80,
            "max_iterations": 5,
        }
        exec_res = {
            "total_score": 50,
            "unsupported_conclusions": [],
            "revision_feedback": "",
            "needs_revision": True,
        }
        action = node.post(shared, prep_res, exec_res)
        assert action == "satisfied"


# =============================================================================
# Stage3CompletionNode (已在 test_dispatcher.py 测试 post)
# =============================================================================

class TestStage3CompletionNodeExtended:

    def test_marks_stage3_completed(self, minimal_shared):
        node = Stage3CompletionNode()
        prep_res = {"generation_mode": "template", "iterations": 2, "final_score": 85}
        exec_res = {"stage": 3}
        action = node.post(minimal_shared, prep_res, exec_res)
        assert 3 in minimal_shared["dispatcher"]["completed_stages"]
        assert minimal_shared["dispatcher"]["current_stage"] == 3
        assert action == "dispatch"
