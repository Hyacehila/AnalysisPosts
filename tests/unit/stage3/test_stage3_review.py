"""
ReviewChaptersNode unit tests.
"""
from unittest.mock import patch

from nodes import ReviewChaptersNode
from nodes.stage3.review import _blocks_to_text


def _shared_for_review(round_no=0, max_rounds=2):
    return {
        "config": {
            "stage3_review": {
                "chapter_review_max_rounds": max_rounds,
            }
        },
        "stage3_results": {
            "outline": {
                "title": "测试报告",
                "chapters": [
                    {"id": "ch01", "allowSwot": False, "allowPest": False},
                    {"id": "ch02", "allowSwot": True, "allowPest": False},
                ],
            },
            "chapters": [
                {
                    "id": "ch01",
                    "title": "执行摘要",
                    "blocks": [
                        {
                            "type": "paragraph",
                            "inlines": [{"text": "内容 A，由情感分布统计支持[E1]。", "marks": []}],
                        }
                    ],
                    "content": "",
                },
                {
                    "id": "ch02",
                    "title": "趋势分析",
                    "blocks": [
                        {
                            "type": "paragraph",
                            "inlines": [{"text": "内容 B，由趋势图分析支持[E2]。", "marks": []}],
                        }
                    ],
                    "content": "",
                },
            ],
            "review_round": round_no,
        },
        "trace": {"loop_status": {}},
    }


def test_blocks_to_text_paragraph():
    blocks = [
        {
            "type": "paragraph",
            "inlines": [
                {"text": "测试文本 ", "marks": []},
                {"text": "[E1]", "marks": [{"type": "bold"}]},
            ],
        },
    ]
    result = _blocks_to_text(blocks)
    assert "测试文本" in result
    assert "[E1]" in result


def test_blocks_to_text_engine_quote():
    blocks = [
        {
            "type": "engineQuote",
            "engine": "insight",
            "title": "Insight Agent",
            "blocks": [{"type": "paragraph", "inlines": [{"text": "观点内容", "marks": []}]}],
        },
    ]
    result = _blocks_to_text(blocks)
    assert "Insight Agent" in result
    assert "观点内容" in result


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
def test_review_prompt_includes_entity_hallucination_guard(mock_llm):
    mock_llm.side_effect = [
        '{"score": 88, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    node.exec(prep_res)

    prompt = mock_llm.call_args.args[0]
    assert "不得引入输入未出现的新专有名词" in prompt


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_placeholder_as_hard_failure(mock_llm):
    mock_llm.side_effect = [
        '{"score": 95, "needs_revision": false, "feedback": "ok"}',
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {"type": "paragraph", "inlines": [{"text": "核心观点：[议题A] 继续发酵。", "marks": []}]}
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "占位符" in first_review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_missing_inline_evidence_citation_as_hard_failure(mock_llm):
    mock_llm.side_effect = [
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {"type": "paragraph", "inlines": [{"text": "这是一个没有证据角标的段落。", "marks": []}]}
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "证据角标" in first_review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_duplicate_heading_as_hard_failure(mock_llm):
    mock_llm.side_effect = [
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {"type": "heading", "text": "执行摘要"},
        {"type": "paragraph", "inlines": [{"text": "关键结论由统计支持[E1]。", "marks": []}]},
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "重复标题" in first_review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_review_flags_invalid_non_evidence_bracket_citation(mock_llm):
    mock_llm.side_effect = [
        '{"score": 90, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {
            "type": "paragraph",
            "inlines": [{"text": "该结论需要持续观察[topic_distribution]，当前样本保持中性[E1]。", "marks": []}],
        }
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    assert exec_res["needs_revision"] is True
    first_review = exec_res["reviews"][0]
    assert first_review["needs_revision"] is True
    assert "非法引用" in first_review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_swot_violation_detected(mock_llm):
    mock_llm.side_effect = [
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {
            "type": "swotTable",
            "strengths": [{"point": "品牌", "detail": "知名度高"}],
            "weaknesses": [],
            "opportunities": [],
            "threats": [],
        }
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    review = exec_res["reviews"][0]
    assert review["needs_revision"] is True
    assert "swotTable" in review["feedback"]


@patch("nodes.stage3.review.call_glm46")
def test_pest_violation_detected(mock_llm):
    mock_llm.side_effect = [
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
        '{"score": 92, "needs_revision": false, "feedback": "ok"}',
    ]
    shared = _shared_for_review(round_no=0, max_rounds=2)
    shared["stage3_results"]["chapters"][0]["blocks"] = [
        {
            "type": "pestTable",
            "political": [{"factor": "监管", "detail": "规则趋严"}],
            "economic": [],
            "social": [],
            "technological": [],
        }
    ]

    node = ReviewChaptersNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    review = exec_res["reviews"][0]
    assert review["needs_revision"] is True
    assert "pestTable" in review["feedback"]
