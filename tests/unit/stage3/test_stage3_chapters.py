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
                        "allowSwot": False,
                        "allowPest": False,
                        "description": "全局概览",
                    },
                    {
                        "id": "ch02",
                        "title": "趋势分析",
                        "target_words": 500,
                        "key_data": ["sentiment"],
                        "relevant_charts": ["c2"],
                        "allowSwot": True,
                        "allowPest": False,
                        "description": "详细分析",
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


def test_prep_async_returns_outline_chapters_and_component_flags():
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    prep_res = run_async(node.prep_async(shared))

    assert len(prep_res) == 2
    assert prep_res[0]["id"] == "ch01"
    assert prep_res[0]["_allowSwot"] is False
    assert prep_res[1]["_allowSwot"] is True
    assert prep_res[1]["_allowPest"] is False
    assert prep_res[1]["_chapter_description"] == "详细分析"


@patch("nodes.stage3.chapters.call_glm46")
def test_exec_async_generates_single_chapter_blocks(mock_llm):
    mock_llm.return_value = (
        '[{"type":"paragraph","inlines":[{"text":"章节内容 [E1]。","marks":[]}]}]'
    )
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = run_async(node.prep_async(shared))[0]

    result = run_async(node.exec_async(chapter_input))

    assert result["id"] == "ch01"
    assert result["title"] == "执行摘要"
    assert isinstance(result["blocks"], list)
    assert result["blocks"][0]["type"] == "paragraph"
    assert result["content"] == ""


@patch("nodes.stage3.chapters.call_glm46")
def test_exec_async_filters_disallowed_swot_pest_blocks(mock_llm):
    mock_llm.return_value = (
        '[{"type":"swotTable","strengths":[],"weaknesses":[],"opportunities":[],"threats":[]},'
        '{"type":"pestTable","political":[],"economic":[],"social":[],"technological":[]},'
        '{"type":"paragraph","inlines":[{"text":"保留段落[E1]","marks":[]}]}]'
    )
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch01",
        "title": "执行摘要",
        "target_words": 200,
        "key_data": [],
        "_allowSwot": False,
        "_allowPest": False,
    }

    result = run_async(node.exec_async(chapter_input))
    block_types = [block.get("type") for block in result["blocks"]]
    assert "swotTable" not in block_types
    assert "pestTable" not in block_types
    assert "paragraph" in block_types


@patch("nodes.stage3.chapters.call_glm46")
def test_exec_async_removes_duplicate_or_empty_heading_blocks(mock_llm):
    mock_llm.return_value = (
        '[{"type":"heading","level":3,"text":"执行摘要"},'
        '{"type":"heading","level":3,"text":"  "},'
        '{"type":"paragraph","inlines":[{"text":"保留段落[E1]","marks":[]}]}]'
    )
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch01",
        "title": "执行摘要",
        "target_words": 200,
        "key_data": [],
        "_allowSwot": False,
        "_allowPest": False,
    }

    result = run_async(node.exec_async(chapter_input))
    block_types = [block.get("type") for block in result["blocks"]]
    assert block_types == ["paragraph"]


@patch("nodes.stage3.chapters.call_glm46")
def test_exec_async_normalizes_inline_invalid_bracket_citations(mock_llm):
    mock_llm.return_value = (
        '[{"type":"paragraph","inlines":[{"text":"该段由[E1, E2]支撑，同时提及[topic_distribution]。","marks":[]}]}]'
    )
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch06",
        "title": "证据规范",
        "target_words": 200,
        "key_data": [],
        "_allowSwot": False,
        "_allowPest": False,
    }

    result = run_async(node.exec_async(chapter_input))
    text = result["blocks"][0]["inlines"][0]["text"]
    assert "[E1][E2]" in text
    assert "[topic_distribution]" not in text


def test_apply_item_result_writes_blocks_and_content_in_place():
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_item = {"id": "ch01", "title": "执行摘要"}

    node.apply_item_result(
        chapter_item,
        {
            "id": "ch01",
            "title": "执行摘要",
            "blocks": [{"type": "paragraph", "inlines": [{"text": "abc", "marks": []}]}],
            "content": "",
        },
    )

    assert chapter_item["blocks"][0]["type"] == "paragraph"
    assert chapter_item["content"] == ""


def test_post_async_stores_generated_chapters():
    shared = _shared_for_chapters()
    node = GenerateChaptersBatchNode(max_concurrent=2)
    prep_res = [
        {"id": "ch01", "title": "执行摘要"},
        {"id": "ch02", "title": "趋势分析"},
    ]
    exec_res = [
        {
            "id": "ch01",
            "title": "执行摘要",
            "blocks": [{"type": "paragraph", "inlines": [{"text": "A[E1]", "marks": []}]}],
            "content": "",
        },
        {
            "id": "ch02",
            "title": "趋势分析",
            "blocks": [{"type": "paragraph", "inlines": [{"text": "B[E2]", "marks": []}]}],
            "content": "",
        },
    ]

    action = run_async(node.post_async(shared, prep_res, exec_res))

    assert action == "default"
    assert len(shared["stage3_results"]["chapters"]) == 2
    assert shared["stage3_results"]["chapters"][1]["blocks"][0]["type"] == "paragraph"
    assert shared["stage3_results"]["generation_mode"] == "unified_json"


@patch("nodes.stage3.chapters.call_glm46")
def test_exec_async_accepts_non_numeric_target_words(mock_llm):
    mock_llm.return_value = (
        '[{"type":"paragraph","inlines":[{"text":"章节内容 [E1]","marks":[]}]}]'
    )
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch03",
        "title": "风险评估",
        "target_words": "约500字",
        "key_data": [],
        "_allowSwot": False,
        "_allowPest": False,
    }

    result = run_async(node.exec_async(chapter_input))

    assert result["id"] == "ch03"
    assert result["blocks"][0]["type"] == "paragraph"


@patch("nodes.stage3.chapters.call_glm46")
def test_chapter_generation_respects_stage3_reasoning_switch(mock_llm):
    mock_llm.return_value = (
        '[{"type":"paragraph","inlines":[{"text":"章节内容 [E1]","marks":[]}]}]'
    )
    shared = _shared_for_chapters()
    shared["config"] = {"llm": {"reasoning_enabled_stage3": False}}
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = run_async(node.prep_async(shared))[0]

    run_async(node.exec_async(chapter_input))

    assert mock_llm.call_args.kwargs["enable_reasoning"] is False


@patch("nodes.stage3.chapters.call_glm46")
def test_chapter_prompt_includes_json_contract_and_component_permissions(mock_llm):
    mock_llm.return_value = (
        '[{"type":"paragraph","inlines":[{"text":"章节内容 [E1]","marks":[]}]}]'
    )
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch03",
        "title": "关键趋势",
        "target_words": 400,
        "key_data": ["sentiment_summary"],
        "_allowSwot": True,
        "_allowPest": False,
        "_chapter_description": "详细分析",
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
        "_debate_logs": [
            "ForumHost: 数据侧认为情绪偏中性，搜索侧指出官方回应时效存在争议。"
        ],
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
    assert "JSON 契约" in prompt
    assert "engineQuote" in prompt
    assert "allowSwot=True" in prompt
    assert "allowPest=False" in prompt
    assert "占位符" in prompt
    assert "只返回 JSON 数组" in prompt


@patch("nodes.stage3.chapters.call_glm46", side_effect=RuntimeError("llm failed"))
def test_exec_async_fallback_returns_paragraph_block(_mock_llm):
    node = GenerateChaptersBatchNode(max_concurrent=2)
    chapter_input = {
        "id": "ch05",
        "title": "异常降级",
        "target_words": 200,
        "key_data": [],
        "_allowSwot": False,
        "_allowPest": False,
    }

    result = run_async(node.exec_async(chapter_input))

    assert result["blocks"][0]["type"] == "paragraph"
    text = result["blocks"][0]["inlines"][0]["text"]
    assert "章节生成失败" in text
