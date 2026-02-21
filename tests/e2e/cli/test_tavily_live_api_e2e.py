"""
Live E2E tests for Tavily search wrapper.
"""
from __future__ import annotations

import sys
from urllib.parse import urlparse
from pathlib import Path

import pytest

from utils.web_search import search_web

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _e2e_config_runtime import (  # noqa: E402
    load_reserved_config,
    require_tavily_api_key,
)

pytestmark = [pytest.mark.e2e, pytest.mark.live_api]


def _assert_result_shape(result: dict) -> None:
    assert set(result.keys()) >= {"title", "url", "snippet", "date", "source"}
    assert isinstance(result["title"], str)
    assert isinstance(result["url"], str)
    assert isinstance(result["snippet"], str)
    parsed = urlparse(result["url"])
    assert parsed.scheme in {"http", "https"}
    assert bool(parsed.netloc)


def test_tavily_live_single_query_from_reserved_config():
    config_dict = load_reserved_config()
    key = require_tavily_api_key(config_dict)

    output = search_web(
        "郑州 夜骑 开封 舆情",
        provider="tavily",
        api_key=key,
        max_results=3,
        timeout_seconds=20,
    )

    assert output["provider"] == "tavily"
    assert output["error"] == ""
    assert len(output["results"]) >= 1
    _assert_result_shape(output["results"][0])
