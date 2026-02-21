"""
ReviewChaptersNode unit tests.
"""
from unittest.mock import patch

from nodes import ReviewChaptersNode


def _shared_for_review(round_no=0, max_rounds=2):
    return {
        "config": {
            "stage3_review": {
                "chapter_review_max_rounds": max_rounds,
            }
        },
        "stage3_results": {
            "chapters": [
                {
                    "id": "ch01",
                    "title": "执行摘要",
                    "content": "内容 A。\n\n证据说明：该结论由[E1]支持。来源为情感分布统计。置信度：高。理由：样本覆盖完整。",
                },
                {
                    "id": "ch02",
                    "title": "趋势分析",
                    "content": "内容 B。\n\n证据说明：该结论由[E2]支持。来源为趋势图。置信度：中。理由：部分时间段数据缺失。",
                },
            ],
            "review_round": round_no,
        },
        "trace": {"loop_status": {}},
    }


@patch("nodes.stage3.review.call_glm46")
def test_review_needs_revision_when_model_flags_revision(mock_llm):
    mock_llm.side_effect = [
        '{"score": 75, "needs_revision": true, "feedback": "补充证据"}',
        '{"score": 86, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)

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
    shared = _shared_for_review(round_no=0, max_rounds=2)

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
    shared = _shared_for_review(round_no=2, max_rounds=2)

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
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["config"]["llm"] = {"reasoning_enabled_stage3": False}

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    assert mock_llm.call_args.kwargs["enable_reasoning"] is False


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_placeholder_as_hard_failure(mock_llm):
    mock_llm.side_effect = [
        '{"score": 95, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["content"] = "核心观点：[议题A] 继续发酵。"

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "占位符" in first_review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_missing_paragraph_evidence_note_as_hard_failure(mock_llm):
    mock_llm.side_effect = [
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["content"] = "这是一个没有证据说明的段落。"

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "证据说明" in first_review["feedback"]
