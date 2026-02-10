"""
pytest-based tests for utils.data_loader.
"""
from __future__ import annotations

from pathlib import Path
import json

import pytest

from utils.data_loader import (
    load_blog_data,
    load_topics,
    load_sentiment_attributes,
    load_publisher_objects,
    save_enhanced_blog_data,
    load_enhanced_blog_data,
)


def _data_path(filename: str) -> Path:
    return Path(__file__).resolve().parent.parent / "data" / filename


def test_load_blog_data():
    data_path = _data_path("posts.json")
    if not data_path.exists():
        pytest.skip(f"data file missing: {data_path}")
    data = load_blog_data(str(data_path))
    assert isinstance(data, list)
    assert data
    assert isinstance(data[0], dict)


def test_load_topics():
    data_path = _data_path("topics.json")
    if not data_path.exists():
        pytest.skip(f"data file missing: {data_path}")
    topics = load_topics(str(data_path))
    assert isinstance(topics, list)
    assert topics
    assert isinstance(topics[0], dict)
    assert "parent_topic" in topics[0]
    assert "sub_topics" in topics[0]


def test_load_sentiment_attributes():
    data_path = _data_path("sentiment_attributes.json")
    if not data_path.exists():
        pytest.skip(f"data file missing: {data_path}")
    attrs = load_sentiment_attributes(str(data_path))
    assert isinstance(attrs, list)
    assert attrs
    assert isinstance(attrs[0], str)


def test_load_publisher_objects():
    data_path = _data_path("publisher_objects.json")
    if not data_path.exists():
        pytest.skip(f"data file missing: {data_path}")
    objs = load_publisher_objects(str(data_path))
    assert isinstance(objs, list)
    assert objs
    assert isinstance(objs[0], str)


def test_save_and_load_enhanced_data(tmp_path):
    output_path = tmp_path / "enhanced.json"
    test_data = [
        {
            "username": "测试用户",
            "user_id": "test_user",
            "content": "测试内容",
            "publish_time": "2024-07-30 12:00:00",
            "location": "测试地点",
            "repost_count": 10,
            "comment_count": 5,
            "like_count": 20,
            "image_urls": [],
            "sentiment_polarity": 3,
            "sentiment_attribute": "担忧",
            "topics": "自然灾害",
            "publisher": "个人用户",
        }
    ]

    ok = save_enhanced_blog_data(test_data, str(output_path))
    assert ok is True
    assert output_path.exists()

    loaded = load_enhanced_blog_data(str(output_path))
    assert loaded == test_data
