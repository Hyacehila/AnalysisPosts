"""
Validate raw post data format against required schema.
"""
from __future__ import annotations

from pathlib import Path
import re

import pytest


REQUIRED_FIELDS = [
    "username",
    "user_id",
    "content",
    "publish_time",
    "location",
    "repost_count",
    "comment_count",
    "like_count",
    "image_urls",
]


def test_posts_schema_and_types():
    data_file = Path(__file__).resolve().parent.parent / "data" / "posts.json"
    if not data_file.exists():
        pytest.skip(f"data file missing: {data_file}")

    import json

    posts = json.loads(data_file.read_text(encoding="utf-8"))
    assert isinstance(posts, list)
    assert posts, "posts.json should contain at least one record"

    for idx, record in enumerate(posts):
        assert isinstance(record, dict), f"record {idx} should be dict"
        for field in REQUIRED_FIELDS:
            assert field in record, f"record {idx} missing field: {field}"

        assert isinstance(record["username"], str)
        assert isinstance(record["user_id"], str)
        assert isinstance(record["content"], str)
        assert isinstance(record["publish_time"], str)
        assert isinstance(record["location"], str)
        assert isinstance(record["repost_count"], int)
        assert isinstance(record["comment_count"], int)
        assert isinstance(record["like_count"], int)
        assert isinstance(record["image_urls"], list)

        assert record["repost_count"] >= 0
        assert record["comment_count"] >= 0
        assert record["like_count"] >= 0


def test_publish_time_format():
    data_file = Path(__file__).resolve().parent.parent / "data" / "posts.json"
    if not data_file.exists():
        pytest.skip(f"data file missing: {data_file}")

    import json

    posts = json.loads(data_file.read_text(encoding="utf-8"))
    time_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

    for idx, record in enumerate(posts):
        publish_time = record.get("publish_time", "")
        assert time_pattern.match(publish_time), f"record {idx} invalid publish_time: {publish_time}"
