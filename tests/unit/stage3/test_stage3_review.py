"""
ReviewChaptersNode unit tests.
"""
from unittest.mock import patch

from nodes import ReviewChaptersNode


def _shared_for_review(round_no=0, max_rounds=2, min_score=80):
    return {
        "config": {
            "stage3_review": {
                "chapter_review_max_rounds": max_rounds,
                "min_score": min_score,
            }
        },
        "stage3_results": {
            "chapters": [
                {"id": "ch01", "title": "执行摘要", "content": "内容 A"},
                {"id": "ch02", "title": "趋势分析", "content": "内容 B"},
            ],
            "review_round": round_no,
        },
        "trace": {"loop_status": {}},
    }


@patch("nodes.stage3.review.call_glm46")
def test_review_needs_revision_when_score_below_threshold(mock_llm):
    mock_llm.side_effect = [
        '{"score": 75, "needs_revision": true, "feedback": "补充证据"}',
        '{"score": 86, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2, min_score=80)

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "needs_revision"
    assert shared["stage3_results"]["review_round"] == 1
    loop_status = shared["trace"]["loop_status"]["stage3_chapter_review"]
    assert loop_status["current"] == 1
    assert loop_status["max"] == 2


@patch("nodes.stage3.review.call_glm46")
def test_review_satisfied_when_all_scores_pass(mock_llm):
    mock_llm.side_effect = [
        '{"score": 88, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2, min_score=80)

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "satisfied"
    loop_status = shared["trace"]["loop_status"]["stage3_chapter_review"]
    assert loop_status["termination_reason"] == "sufficient"


@patch("nodes.stage3.review.call_glm46")
def test_review_forces_satisfied_at_max_rounds(mock_llm):
    mock_llm.side_effect = [
        '{"score": 60, "needs_revision": true, "feedback": "继续"}',
        '{"score": 65, "needs_revision": true, "feedback": "继续"}',
    ]
    shared = _shared_for_review(round_no=2, max_rounds=2, min_score=80)

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "satisfied"
    loop_status = shared["trace"]["loop_status"]["stage3_chapter_review"]
    assert loop_status["termination_reason"] == "max_iterations_reached"


@patch("nodes.stage3.review.call_glm46")
def test_review_respects_stage3_reasoning_switch(mock_llm):
    mock_llm.side_effect = [
        '{"score": 88, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2, min_score=80)
    shared["config"]["llm"] = {"reasoning_enabled_stage3": False}

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert mock_llm.call_args.kwargs["enable_reasoning"] is False
