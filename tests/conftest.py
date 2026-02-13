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
            "report_mode": "template",
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
        "dispatcher": {
            "start_stage": 1,
            "run_stages": [1, 2, 3],
            "current_stage": 0,
            "completed_stages": [],
            "next_action": None,
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
