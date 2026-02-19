"""
Validate JSON reference files format.
"""
from __future__ import annotations

from pathlib import Path
import json

import pytest

pytestmark = pytest.mark.integration


def _data_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[3] / "data" / filename


def test_sentiment_attributes_json():
    path = _data_path("sentiment_attributes.json")
    if not path.exists():
        pytest.skip(f"data file missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data
    assert all(isinstance(item, str) for item in data)


def test_publisher_objects_json():
    path = _data_path("publisher_objects.json")
    if not path.exists():
        pytest.skip(f"data file missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data
    assert all(isinstance(item, str) for item in data)


def test_topics_json():
    path = _data_path("topics.json")
    if not path.exists():
        pytest.skip(f"data file missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data
    assert all(isinstance(item, dict) for item in data)
    for item in data:
        assert "parent_topic" in item
        assert "sub_topics" in item
        assert isinstance(item["sub_topics"], list)
