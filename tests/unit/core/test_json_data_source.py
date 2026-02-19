"""
Tests for JSON data source adapter.
"""
from pathlib import Path

from utils.data_sources.json_source import JsonDataSource


FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"


def test_json_source_loads_blog_data():
    source = JsonDataSource()
    data = source.load_blog_data(str(FIXTURES_DIR / "sample_posts.json"))
    assert isinstance(data, list)
    assert len(data) > 0


def test_json_source_loads_auxiliary_files():
    source = JsonDataSource()
    topics = source.load_topics(str(FIXTURES_DIR / "sample_topics.json"))
    attrs = source.load_sentiment_attributes(str(FIXTURES_DIR / "sample_sentiment_attrs.json"))
    pubs = source.load_publisher_objects(str(FIXTURES_DIR / "sample_publishers.json"))
    assert isinstance(topics, list) and topics
    assert isinstance(attrs, list) and attrs
    assert isinstance(pubs, list) and pubs
