"""
conftest.py — pytest 共享 Fixtures

为所有测试提供统一的测试数据和 Mock 对象。
"""
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# Windows + Python 3.12 in some sandboxed environments may create unreadable
# directories when os.mkdir receives mode=0o700 (pytest tmpdir default).
# Patch it early so tmp_path fixtures stay usable in CI/agent runs.
if os.name == "nt" and not getattr(os.mkdir, "__analysisposts_mode_patch__", False):
    _orig_os_mkdir = os.mkdir

    def _mkdir_mode_compat(path, mode=0o777, *, dir_fd=None):
        effective_mode = 0o777 if mode == 0o700 else mode
        if dir_fd is None:
            return _orig_os_mkdir(path, effective_mode)
        return _orig_os_mkdir(path, effective_mode, dir_fd=dir_fd)

    _mkdir_mode_compat.__analysisposts_mode_patch__ = True
    os.mkdir = _mkdir_mode_compat


# =============================================================================
# 数据 Fixtures
# =============================================================================

@pytest.fixture
def sample_blog_data():
    """3条最小完整博文（原始数据，不含增强字段）"""
    with open(FIXTURES_DIR / "sample_posts.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_enhanced_data():
    """3条带六维增强字段的博文"""
    with open(FIXTURES_DIR / "sample_enhanced.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_topics():
    """精简主题层次结构"""
    with open(FIXTURES_DIR / "sample_topics.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_sentiment_attrs():
    """情感属性列表"""
    with open(FIXTURES_DIR / "sample_sentiment_attrs.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_publishers():
    """发布者类型列表"""
    with open(FIXTURES_DIR / "sample_publishers.json", "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# shared 字典 Fixtures
# =============================================================================

@pytest.fixture
def minimal_shared(sample_blog_data, sample_topics, sample_sentiment_attrs, sample_publishers):
    """
    最小化的 shared 字典，足够驱动 Stage 1 节点。
    模拟 main.py 中 init_shared() 产生的结构。
    """
    return {
        "data": {
            "blog_data": sample_blog_data,
        },
        "config": {
            "enhancement_mode": "async",
            "analysis_mode": "agent",
            "tool_source": "mcp",
            "stage3_review": {
                "chapter_review_max_rounds": 2,
                "min_score": 80,
            },
            "data_source": {
                "type": "original",
                "blog_data_path": "data/posts.json",
                "topics_path": "data/topics.json",
                "sentiment_attributes_path": "data/sentiment_attributes.json",
                "publisher_objects_path": "data/publisher_objects.json",
                "enhanced_data_path": "data/enhanced_posts.json",
            },
            "stage1": {
                "concurrency_limit": 5,
                "max_retries": 2,
                "retry_wait": 1,
            },
            "stage2": {},
            "stage3": {
                "report_template_path": "data/report_template.md",
                "output_path": "report/report.md",
                "max_iterations": 3,
                "quality_threshold": 80,
            },
        },
        "reference_data": {
            "topics_hierarchy": sample_topics,
            "sentiment_attributes": sample_sentiment_attrs,
            "publisher_objects": sample_publishers,
        },
        "pipeline_state": {
            "start_stage": 1,
            "current_stage": 0,
            "completed_stages": [],
        },
        "stage1_results": {
            "statistics": {},
            "data_save": {},
        },
    }


@pytest.fixture
def enhanced_shared(minimal_shared, sample_enhanced_data):
    """
    包含增强数据的 shared 字典，足够驱动 Stage 2/3 节点。
    """
    shared = minimal_shared.copy()
    shared["data"]["blog_data"] = sample_enhanced_data
    shared["stage2_results"] = {
        "charts": [],
        "tables": [],
        "chart_analyses": {},
        "insights": {},
    }
    shared["stage3_results"] = {}
    shared["agent"] = {}
    shared["report"] = {
        "content": "",
        "sections": {},
    }
    return shared


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_llm_calls():
    """
    Mock 所有 LLM 调用函数，返回可控的字符串。
    使用方法:
        def test_something(mock_llm_calls):
            mock_llm_calls["air"].return_value = "3"
            # ... 执行节点 ...
    """
    with patch("nodes.stage1.sentiment.call_glm_45_air") as m_air, \
         patch("nodes.stage1.sentiment.call_glm4v_plus") as m_4v, \
         patch("nodes.stage2.chart_analysis.call_glm45v_thinking") as m_thinking, \
         patch("nodes.stage2.agent.call_glm46") as m_46_agent, \
         patch("nodes.stage2.insight.call_glm46") as m_46_insight:
        m_air.return_value = "mocked_air_response"
        m_4v.return_value = "mocked_4v_response"
        m_thinking.return_value = "mocked_thinking_response"
        m_46_agent.return_value = "mocked_46_response"
        m_46_insight.return_value = "mocked_46_response"
        yield {
            "air": m_air,
            "4v": m_4v,
            "thinking": m_thinking,
            "46_agent": m_46_agent,
            "46_insight": m_46_insight,
        }

# =============================================================================
# Windows 文件系统解除残留锁
# =============================================================================

import gc
import shutil
import stat

def handle_remove_readonly(func, path, exc_info):
    """
    Error handler for ``shutil.rmtree``.
    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.
    If the error is due to a Windows file lock (WinError 32), we skip it gracefully
    to prevent pytest from crashing.
    """
    excvalue = exc_info[1]
    if isinstance(excvalue, OSError):
        # Ignore "The process cannot access the file because it is being used by another process"
        if getattr(excvalue, "winerror", None) == 32:
            return

    # Attempt to clear read-only flag
    if func in (os.rmdir, os.remove, os.unlink):
        try:
            os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            func(path)
        except OSError:
            pass


@pytest.fixture(scope="session", autouse=True)
def cleanup_windows_locks():
    """
    Session-level fixture to forcefully garbage collect before teardown,
    and patch pytest's temporary directory removal to ignore Windows locks.
    """
    yield
    # Force garbage collection to release unreferenced file handles
    gc.collect()

    # Patches rmtree behavior globally for the suite cleanup process
    original_rmtree = shutil.rmtree

    def safe_rmtree(path, ignore_errors=False, onerror=None, **kwargs):
        if onerror is None:
            onerror = handle_remove_readonly
        try:
            original_rmtree(path, ignore_errors=ignore_errors, onerror=onerror, **kwargs)
        except Exception:
            pass

    shutil.rmtree = safe_rmtree
