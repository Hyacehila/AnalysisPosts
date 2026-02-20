"""
Stage3 unified loop integration tests.
"""
from unittest.mock import patch

import pytest

from nodes import ReviewChaptersNode

pytestmark = pytest.mark.integration


def _shared(max_rounds=2, min_score=80):
    return {
        "config": {
            "stage3_review": {
                "chapter_review_max_rounds": max_rounds,
                "min_score": min_score,
            }
        },
        "stage3_results": {
            "chapters": [
                {"id": "ch01", "title": "执行摘要", "content": "A"},
                {"id": "ch02", "title": "趋势分析", "content": "B"},
            ],
            "review_round": 0,
        },
        "trace": {"loop_status": {}},
    }


@patch("nodes.stage3.review.call_glm46")
def test_review_loop_two_rounds_then_converges(mock_llm):
    mock_llm.side_effect = [
        '{"score": 70, "needs_revision": true, "feedback": "补证据"}',
        '{"score": 82, "needs_revision": false, "feedback": "ok"}',
        '{"score": 88, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared(max_rounds=2, min_score=80)

    node = ReviewChaptersNode()

    p1 = node.prep(shared)
    e1 = node.exec(p1)
    a1 = node.post(shared, p1, e1)

    assert a1 == "needs_revision"

    p2 = node.prep(shared)
    e2 = node.exec(p2)
    a2 = node.post(shared, p2, e2)

    assert a2 == "satisfied"
    status = shared["trace"]["loop_status"]["stage3_chapter_review"]
    assert status["termination_reason"] == "sufficient"
