"""
Tests for llm_retry decorator.
"""
from unittest.mock import patch

import pytest

from utils.llm_retry import llm_retry


def test_retry_on_429():
    calls = {"count": 0}

    @llm_retry(max_retries=3, retry_delay=0.01)
    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise Exception("429 Too Many Requests")
        return "ok"

    assert flaky() == "ok"
    assert calls["count"] == 3


def test_no_retry_on_400():
    calls = {"count": 0}

    @llm_retry(max_retries=3, retry_delay=0.01)
    def bad_request():
        calls["count"] += 1
        raise Exception("400 Bad Request")

    with pytest.raises(Exception, match="400"):
        bad_request()
    assert calls["count"] == 1


def test_exponential_backoff():
    calls = {"count": 0}

    @llm_retry(max_retries=3, retry_delay=1, backoff="exponential")
    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise Exception("429 rate limit")
        return "ok"

    with patch("utils.llm_retry.time.sleep") as mock_sleep:
        assert flaky() == "ok"
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)


def test_max_retries_exceeded():
    @llm_retry(max_retries=2, retry_delay=0.01)
    def always_fail():
        raise Exception("429 rate limit")

    with pytest.raises(Exception, match="failed after 2 retries"):
        always_fail()
