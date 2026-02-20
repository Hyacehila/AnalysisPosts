"""
Stage2 QuerySearchFlow unit tests.
"""
import asyncio

from nodes.stage2.search import create_query_search_flow
import nodes.stage2.search as search_module


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
