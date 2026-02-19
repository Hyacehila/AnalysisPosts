"""
Unit tests for utils.web_search.
"""
from __future__ import annotations

import pytest
import requests

from utils.web_search import batch_search, search_web


def test_search_web_requires_api_key(monkeypatch):
    monkeypatch.delenv("TAVILY_API_KEY", raising=False)

    with pytest.raises(EnvironmentError):
        search_web("郑州 夜骑", api_key="")


def test_search_web_tavily_success_normalizes_response(monkeypatch):
    payload = {
        "results": [
            {
                "title": "T1",
                "url": "https://example.com/1",
                "content": "snippet-1",
                "published_date": "2026-01-01",
                "source": "example",
            },
            {
                "title": "T2",
                "url": "https://example.com/2",
                "content": "snippet-2",
                "published_date": "2026-01-02",
                "source": "example",
            },
        ]
    }

    class _DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return payload

    def _fake_post(url, json=None, timeout=20):  # noqa: ANN001
        assert "tavily.com" in url
        assert json["query"] == "郑州 夜骑"
        assert json["max_results"] == 2
        assert json["api_key"] == "test-tavily-key"
        return _DummyResponse()

    monkeypatch.setenv("TAVILY_API_KEY", "test-tavily-key")
    monkeypatch.setattr("utils.web_search.requests.post", _fake_post)

    result = search_web("郑州 夜骑", max_results=2, timeout_seconds=10)

    assert result["provider"] == "tavily"
    assert result["query"] == "郑州 夜骑"
    assert result["error"] == ""
    assert len(result["results"]) == 2
    assert result["results"][0] == {
        "title": "T1",
        "url": "https://example.com/1",
        "snippet": "snippet-1",
        "date": "2026-01-01",
        "source": "example",
    }


def test_search_web_tavily_timeout_returns_error(monkeypatch):
    def _fake_post(url, json=None, timeout=20):  # noqa: ANN001
        raise requests.Timeout("timeout")

    monkeypatch.setenv("TAVILY_API_KEY", "test-tavily-key")
    monkeypatch.setattr("utils.web_search.requests.post", _fake_post)

    result = search_web("郑州 夜骑")

    assert result["results"] == []
    assert result["error"] != ""
    assert result["provider"] == "tavily"


def test_batch_search_aggregates_results(monkeypatch):
    def _fake_search_web(query, **kwargs):  # noqa: ANN001
        return {
            "query": query,
            "provider": "tavily",
            "results": [{"title": query, "url": "", "snippet": "", "date": "", "source": ""}],
            "error": "",
        }

    monkeypatch.setattr("utils.web_search.search_web", _fake_search_web)

    out = batch_search(["q1", "q2"])

    assert out["provider"] == "tavily"
    assert out["queries"] == ["q1", "q2"]
    assert len(out["results_by_query"]) == 2
    assert out["results_by_query"][0]["query"] == "q1"
    assert out["total_results"] == 2
