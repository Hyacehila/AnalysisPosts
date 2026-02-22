"""
Stage2 QuerySearchFlow unit tests.
"""
import asyncio

from nodes.stage2.search import create_query_search_flow
import nodes.stage2.search as search_module
from nodes.stage2.search import SearchReflectionNode, SearchSummaryNode


def _build_shared():
    return {
        "agent": {
            "data_summary": "北京暴雨事件舆情快速升温，公众关注官方回应和救援进展。",
        },
        "config": {
            "web_search": {
                "provider": "tavily",
                "max_results": 3,
                "timeout_seconds": 10,
                "api_key": "demo-key",
            },
            "stage2_loops": {
                "search_reflection_max_rounds": 2,
            },
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_query_search_flow_generates_structured_summary(monkeypatch):
    shared = _build_shared()

    llm_outputs = [
        '{"queries": ["北京暴雨 官方回应", "北京暴雨 救援进展"]}',
        '{"is_sufficient": true, "missing": []}',
        (
            '{"event_timeline":["7月31日开始强降雨"],'
            '"key_actors":["北京市应急管理局"],'
            '"official_responses":["已启动应急响应"],'
            '"public_reactions_summary":"关注排水和交通恢复",'
            '"related_events":["历史同期暴雨"]}'
        ),
    ]

    def fake_llm(*args, **kwargs):
        return llm_outputs.pop(0)

    def fake_batch_search(queries, **kwargs):
        return {
            "queries": list(queries),
            "provider": "tavily",
            "results_by_query": [
                {
                    "query": queries[0],
                    "provider": "tavily",
                    "results": [
                        {
                            "title": "官方通报",
                            "url": "https://example.com/notice",
                            "snippet": "启动应急响应",
                            "date": "2026-02-18",
                            "source": "example",
                        }
                    ],
                    "error": "",
                }
            ],
            "total_results": 1,
        }

    monkeypatch.setattr(search_module, "call_glm46", fake_llm)
    monkeypatch.setattr(search_module, "batch_search", fake_batch_search)

    flow = create_query_search_flow()
    asyncio.run(flow.run_async(shared))

    assert shared["search"]["queries"] == ["北京暴雨 官方回应", "北京暴雨 救援进展"]
    assert shared["search_results"]["key_actors"] == ["北京市应急管理局"]
    assert len(shared["trace"]["search_reflections"]) == 1
    assert shared["trace"]["search_reflections"][0]["is_sufficient"] is True
    assert shared["trace"]["loop_status"]["search_reflection"]["current"] == 1
    assert shared["trace"]["loop_status"]["search_reflection"]["termination_reason"] == "sufficient"


def test_query_search_flow_supports_reflection_loop(monkeypatch):
    shared = _build_shared()

    llm_outputs = [
        '{"queries": ["北京暴雨 进展"]}',
        '{"is_sufficient": false, "missing": ["official_responses"], "query_hints": ["北京暴雨 官方发布会"]}',
        '{"queries": ["北京暴雨 官方发布会"]}',
        '{"is_sufficient": true, "missing": []}',
        (
            '{"event_timeline":[],"key_actors":[],"official_responses":["召开发布会"],'
            '"public_reactions_summary":"关注救援效率","related_events":[]}'
        ),
    ]

    def fake_llm(*args, **kwargs):
        return llm_outputs.pop(0)

    def fake_batch_search(queries, **kwargs):
        query = queries[0] if queries else "unknown"
        return {
            "queries": list(queries),
            "provider": "tavily",
            "results_by_query": [
                {
                    "query": query,
                    "provider": "tavily",
                    "results": [
                        {
                            "title": f"{query} 新闻",
                            "url": f"https://example.com/{query}",
                            "snippet": "结果内容",
                            "date": "2026-02-18",
                            "source": "example",
                        }
                    ],
                    "error": "",
                }
            ],
            "total_results": 1,
        }

    monkeypatch.setattr(search_module, "call_glm46", fake_llm)
    monkeypatch.setattr(search_module, "batch_search", fake_batch_search)

    flow = create_query_search_flow()
    asyncio.run(flow.run_async(shared))

    assert len(shared["trace"]["search_reflections"]) == 2
    assert shared["search"]["round"] == 2
    assert shared["search_results"]["official_responses"] == ["召开发布会"]
    assert shared["trace"]["loop_status"]["search_reflection"]["current"] == 2
    assert shared["trace"]["loop_status"]["search_reflection"]["max"] == 2


def test_query_search_flow_summary_timeout_falls_back(monkeypatch):
    shared = _build_shared()
    shared["config"]["llm"] = {"request_timeout_seconds": 120}

    calls = []
    llm_outputs = [
        '{"queries": ["北京暴雨 官方回应"]}',
        '{"is_sufficient": true, "missing": []}',
    ]

    def fake_llm(*args, **kwargs):
        calls.append(dict(kwargs))
        if len(calls) <= len(llm_outputs):
            return llm_outputs[len(calls) - 1]
        raise TimeoutError("summary timeout")

    def fake_batch_search(queries, **kwargs):
        return {
            "queries": list(queries),
            "provider": "tavily",
            "results_by_query": [
                {
                    "query": queries[0],
                    "provider": "tavily",
                    "results": [
                        {
                            "title": "官方通报",
                            "url": "https://example.com/notice",
                            "snippet": "启动应急响应",
                            "date": "2026-02-18",
                            "source": "example",
                        }
                    ],
                    "error": "",
                }
            ],
            "total_results": 1,
        }

    monkeypatch.setattr(search_module, "call_glm46", fake_llm)
    monkeypatch.setattr(search_module, "batch_search", fake_batch_search)

    flow = create_query_search_flow()
    asyncio.run(flow.run_async(shared))

    # timeout still produces a fallback summary instead of stopping the flow.
    assert shared["search_results"]["event_timeline"]
    assert shared["search_results"]["key_actors"]

    # every search-stage LLM call should honor the configured timeout.
    assert all(call.get("timeout") == 120 for call in calls)


def test_search_reflection_prompt_includes_document_snippets(monkeypatch):
    captured = {}

    def fake_llm(prompt, *args, **kwargs):
        captured["prompt"] = prompt
        return '{"is_sufficient": true, "missing": []}'

    monkeypatch.setattr(search_module, "call_glm46", fake_llm)

    node = SearchReflectionNode()
    prep_res = {
        "round": 1,
        "documents": [
            {
                "title": "官方回应",
                "snippet": "启动应急响应并公布救援进展",
                "url": "https://example.com/notice",
            }
        ],
        "queries": ["北京暴雨 官方回应"],
        "max_rounds": 2,
        "request_timeout_seconds": 120,
    }

    node.exec(prep_res)

    assert "启动应急响应并公布救援进展" in captured["prompt"]


def test_search_summary_prompt_contains_density_requirements(monkeypatch):
    captured = {}

    def fake_llm(prompt, *args, **kwargs):
        captured["prompt"] = prompt
        return (
            '{"event_timeline":["2026-02-18: 官方通报发布"],'
            '"key_actors":["北京市应急管理局"],'
            '"official_responses":["北京市应急管理局：已启动应急响应"],'
            '"public_reactions_summary":"公众关注排水与交通恢复。",'
            '"related_events":["2023年同期暴雨事件"]}'
        )

    monkeypatch.setattr(search_module, "call_glm46", fake_llm)

    node = SearchSummaryNode()
    prep_res = {
        "documents": [
            {
                "title": "官方通报",
                "snippet": "启动应急响应",
                "url": "https://example.com/notice",
            }
        ],
        "data_summary": "北京暴雨事件舆情快速升温",
        "request_timeout_seconds": 120,
    }
    result = node.exec(prep_res)

    assert result["key_actors"] == ["北京市应急管理局"]
    assert "事件时间线" in captured["prompt"]
    assert "信息密度要求" in captured["prompt"]
    assert "官方回应汇总" in captured["prompt"]
