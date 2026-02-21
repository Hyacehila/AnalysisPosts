"""
GenerateChaptersBatchNode unit tests.
"""
import asyncio
from unittest.mock import patch

from nodes import GenerateChaptersBatchNode


def run_async(coro):
    return asyncio.run(coro)


def _shared_for_chapters():
    return {
        "analysis_context": {
            "time_range": {
                "start": "2024-08-16 10:00:00",
                "end": "2024-08-31 22:00:00",
                "span_hours": 373.0,
            },
            "time_range_text": "2024-08-16 10:00:00 至 2024-08-31 22:00:00",
            "user_analysis_instruction": "重点分析官方回应时效性",
        },
        "stage3_results": {
            "outline": {
                "title": "测试报告",
                "chapters": [
                    {
                        "id": "ch01",
                        "title": "执行摘要",
                        "target_words": 300,
                        "key_data": ["total_posts"],
                        "relevant_charts": ["c1"],
                    },
                    {
                        "id": "ch02",
                        "title": "趋势分析",
                        "target_words": 500,
                        "key_data": ["sentiment"],
                        "relevant_charts": ["c2"],
                    },
                ],
            }
        },
        "stage3_data": {
            "analysis_data": {
                "charts": [
                    {"id": "c1", "title": "图1", "file_path": "./images/c1.png"},
                    {"id": "c2", "title": "图2", "file_path": "./images/c2.png"},
                ]
            },
            "insights": {"summary": "ok"},
        },
    }


def test_prep_async_returns_outline_chapters():
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    prep_res = run_async(node.prep_async(shared))

    assert len(prep_res) == 2
    assert prep_res[0]["id"] == "ch01"


@patch("nodes.stage3.chapters.call_glm46", return_value="章节内容")
def test_exec_async_generates_single_chapter(_mock_llm):
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = run_async(node.prep_async(shared))[0]

    result = run_async(node.exec_async(chapter_input))

    assert result["id"] == "ch01"
    assert result["title"] == "执行摘要"
    assert "章节内容" in result["content"]


def test_apply_item_result_writes_content_in_place():
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_item = {"id": "ch01", "title": "执行摘要"}

    node.apply_item_result(chapter_item, {"id": "ch01", "title": "执行摘要", "content": "abc"})

    assert chapter_item["content"] == "abc"


def test_post_async_stores_generated_chapters():
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    prep_res = [
        {"id": "ch01", "title": "执行摘要"},
        {"id": "ch02", "title": "趋势分析"},
    ]
    exec_res = [
        {"id": "ch01", "title": "执行摘要", "content": "A"},
        {"id": "ch02", "title": "趋势分析", "content": "B"},
    ]

    action = run_async(node.post_async(shared, prep_res, exec_res))

    assert action == "default"
    assert len(shared["stage3_results"]["chapters"]) == 2
    assert shared["stage3_results"]["chapters"][1]["content"] == "B"


@patch("nodes.stage3.chapters.call_glm46", return_value="章节内容")
def test_exec_async_accepts_non_numeric_target_words(_mock_llm):
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch03",
        "title": "风险评估",
        "target_words": "约500字",
        "key_data": [],
    }

    result = run_async(node.exec_async(chapter_input))

    assert result["id"] == "ch03"
    assert "章节内容" in result["content"]


@patch("nodes.stage3.chapters.call_glm46", return_value="章节内容")
def test_chapter_generation_respects_stage3_reasoning_switch(mock_llm):
    shared = _shared_for_chapters()
    shared["config"] = {"llm": {"reasoning_enabled_stage3": False}}
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = run_async(node.prep_async(shared))[0]

    run_async(node.exec_async(chapter_input))

    assert mock_llm.call_args.kwargs["enable_reasoning"] is False


@patch("nodes.stage3.chapters.call_glm46", return_value="章节内容")
def test_chapter_prompt_includes_chart_analysis_insights_and_placeholder_guard(mock_llm):
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch03",
        "title": "关键趋势",
        "target_words": 400,
        "key_data": ["sentiment_summary"],
        "_relevant_charts": [
            {
                "id": "c9",
                "title": "情感分布饼图",
                "analysis_content": "中性占比70.0%，乐观占比26.7%。",
            }
        ],
        "_insights": {
            "overall_summary": "8月16日至8月31日讨论集中于共享单车夜骑治理。",
        },
        "_search_context": {
            "event_timeline": ["2024-08-21 声量峰值"],
        },
        "_analysis_time_range_text": "2024-08-16 10:00:00 至 2024-08-31 22:00:00",
        "_user_analysis_instruction": "重点分析官方回应时效性",
        "_evidence_cards": [
            {
                "id": "E1",
                "source": "sentiment_trend_chart",
                "evidence": "中性占比70.0%，乐观占比26.7%。",
                "confidence": "高",
                "reason": "图表与统计口径一致",
            }
        ],
    }

    run_async(node.exec_async(chapter_input))

    prompt = mock_llm.call_args.args[0]
    assert "中性占比70.0%，乐观占比26.7%" in prompt
    assert "8月16日至8月31日讨论集中于共享单车夜骑治理" in prompt
    assert "占位符" in prompt
    assert "证据说明" in prompt
    assert "置信度" in prompt
    assert "2024-08-16 10:00:00 至 2024-08-31 22:00:00" in prompt
    assert "重点分析官方回应时效性" in prompt
