"""
test_utils.py — 辅助函数单元测试

测试 nodes.py 中无外部依赖的纯函数：
  - normalize_path
  - _strip_timestamp_suffix
  - _build_chart_path_index
  - _remap_report_images
  - get_project_relative_path
  - ensure_dir_exists
"""
import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from nodes import (
    normalize_path,
    _strip_timestamp_suffix,
    _build_chart_path_index,
    _remap_report_images,
    get_project_relative_path,
    ensure_dir_exists,
)


# =============================================================================
# normalize_path
# =============================================================================

class TestNormalizePath:
    def test_empty_string(self):
        assert normalize_path("") == ""

    def test_none_input(self):
        assert normalize_path(None) is None

    def test_forward_slash_relative(self):
        result = normalize_path("data/posts.json")
        assert "\\" not in result
        assert result == "data/posts.json"

    def test_backslash_to_forward(self):
        result = normalize_path("data\\posts.json")
        assert "\\" not in result
        assert "posts.json" in result

    def test_dot_slash_prefix_removed(self):
        result = normalize_path("./data/posts.json")
        assert result == "data/posts.json"

    def test_dot_slash_alone(self):
        result = normalize_path("./")
        # "./" 或 "." 都是合理结果
        assert result in ("./", ".")

    def test_already_normalized(self):
        path = "report/images/chart.png"
        assert normalize_path(path) == path


# =============================================================================
# _strip_timestamp_suffix
# =============================================================================

class TestStripTimestampSuffix:
    def test_with_digit_timestamp_is_stripped(self):
        """数字时间戳应被剥离"""
        result = _strip_timestamp_suffix("sentiment_trend_20240101_120000")
        assert result == "sentiment_trend"

    def test_without_timestamp(self):
        assert _strip_timestamp_suffix("sentiment_trend") == "sentiment_trend"

    def test_partial_timestamp(self):
        result = _strip_timestamp_suffix("chart_2024")
        assert result == "chart_2024"

    def test_empty_string(self):
        assert _strip_timestamp_suffix("") == ""


# =============================================================================
# _build_chart_path_index
# =============================================================================

class TestBuildChartPathIndex:
    def test_empty_charts(self):
        allowed, alias = _build_chart_path_index([])
        assert allowed == set()
        assert alias == {}

    def test_none_charts(self):
        allowed, alias = _build_chart_path_index(None)
        assert allowed == set()
        assert alias == {}

    def test_single_chart(self):
        charts = [{"file_path": "report/images/sentiment_trend.png"}]
        allowed, alias = _build_chart_path_index(charts)
        assert "./images/sentiment_trend.png" in allowed
        assert "sentiment_trend" in alias

    def test_multiple_path_keys(self):
        """支持 file_path / path / chart_path / image_path 键"""
        for key in ["file_path", "path", "chart_path", "image_path"]:
            charts = [{key: f"report/images/test_{key}.png"}]
            allowed, _ = _build_chart_path_index(charts)
            assert f"./images/test_{key}.png" in allowed

    def test_timestamp_alias_full_stem(self):
        """带时间戳的 stem 会被精简为基础名"""
        charts = [{"file_path": "report/images/topic_ranking_20240101_120000.png"}]
        _, alias = _build_chart_path_index(charts)
        assert "topic_ranking" in alias

    def test_source_tool_alias(self):
        charts = [{
            "file_path": "report/images/chart.png",
            "source_tool": "sentiment_trend_chart",
        }]
        _, alias = _build_chart_path_index(charts)
        assert "sentiment_trend" in alias

    def test_chart_without_path_ignored(self):
        charts = [{"title": "Some chart"}]
        allowed, alias = _build_chart_path_index(charts)
        assert allowed == set()


# =============================================================================
# _remap_report_images
# =============================================================================

class TestRemapReportImages:
    def test_empty_content(self):
        assert _remap_report_images("", []) == ""
        assert _remap_report_images(None, []) is None

    def test_empty_charts(self):
        content = "![图表](./images/foo.png)"
        assert _remap_report_images(content, []) == content
        assert _remap_report_images(content, None) == content

    def test_known_chart_path_preserved(self):
        charts = [{"file_path": "report/images/sentiment_trend.png"}]
        content = "![情感趋势](./images/sentiment_trend.png)"
        result = _remap_report_images(content, charts)
        assert "![情感趋势](./images/sentiment_trend.png)" in result

    def test_absolute_path_remapped(self):
        charts = [{"file_path": "report/images/sentiment_trend.png"}]
        content = "![图](D:\\Project\\report\\images\\sentiment_trend.png)"
        result = _remap_report_images(content, charts)
        assert "./images/sentiment_trend.png" in result

    def test_no_markdown_image(self):
        """普通文本不受影响"""
        charts = [{"file_path": "report/images/chart.png"}]
        content = "这是一段普通文本，没有图片引用。"
        result = _remap_report_images(content, charts)
        assert result == content


# =============================================================================
# get_project_relative_path
# =============================================================================

class TestGetProjectRelativePath:
    def test_relative_output(self):
        """结果应使用正斜杠"""
        cwd = Path.cwd()
        abs_path = str(cwd / "data" / "posts.json")
        result = get_project_relative_path(abs_path)
        assert "\\" not in result
        assert "posts.json" in result


# =============================================================================
# ensure_dir_exists
# =============================================================================

class TestEnsureDirExists:
    def test_creates_directory(self, tmp_path):
        new_dir = tmp_path / "a" / "b" / "c"
        assert not new_dir.exists()
        ensure_dir_exists(str(new_dir))
        assert new_dir.exists()

    def test_existing_directory_no_error(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()
        ensure_dir_exists(str(existing))  # 不应抛出异常
        assert existing.exists()
