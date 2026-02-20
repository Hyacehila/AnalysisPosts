"""
Parallel dual-source flow unit tests (B4).
"""
import asyncio

import nodes.stage2.parallel as parallel_module
from nodes.stage2.assemble import AssembleStage2ResultsNode


def _build_shared():
    return {
        "data": {
            "blog_data": [{"id": "p1", "content": "sample"}],
        },
        "config": {
            "tool_source": "mcp",
            "agent_config": {"max_iterations": 2},
            "stage2_chart": {
                "min_per_category": {
                    "sentiment": 0,
                    "topic": 0,
                    "geographic": 0,
                    "interaction": 0,
                    "nlp": 0,
                },
                "tool_policy": "coverage_first",
                "tool_allowlist": [],
                "missing_policy": "warn",
            },
            "data_source": {"enhanced_data_path": "data/enhanced_posts.json"},
        },
        "agent": {
            "data_summary": "样本摘要",
            "data_statistics": {"total_posts": 1},
        },
        "search_results": {
            "event_timeline": [],
            "key_actors": [],
            "official_responses": [],
            "public_reactions_summary": "",
            "related_events": [],
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_parallel_agent_flow_collects_dual_results(monkeypatch):
    shared = _build_shared()

    monkeypatch.setattr(
        parallel_module,
        "_run_data_agent_branch_sync",
        lambda snapshot: {
            "charts": [{"id": "c1", "title": "chart"}],
            "tables": [{"id": "t1", "title": "table"}],
            "execution_log": {"tools_executed": ["sentiment_distribution_stats"]},
            "trace": {
                "decisions": [{"id": "d_0001"}],
                "executions": [{"id": "e_0001"}],
                "reflections": [{"id": "r_0001"}],
                "insight_provenance": {},
                "loop_status": {
                    "data_agent": {"current": 2, "max": 2, "termination_reason": "max_iterations_reached"},
                },
            },
            "monitor": {"error_log": []},
        },
    )
    monkeypatch.setattr(
        parallel_module,
        "_run_search_agent_branch_sync",
        lambda snapshot: {
            "background_context": "补充背景",
            "consistency_points": ["一致点"],
            "conflict_points": [],
            "blind_spots": [],
            "recommended_followups": [],
        },
    )

    flow = parallel_module.create_parallel_agent_flow()
    asyncio.run(flow.run_async(shared))

    assert shared["agent_results"]["data_agent"]["charts"][0]["id"] == "c1"
    assert shared["agent_results"]["search_agent"]["background_context"] == "补充背景"
    assert shared["trace"]["decisions"][0]["id"] == "d_0001"
    assert len(shared["trace"]["search_agent_analysis"]) == 1
    assert shared["trace"]["loop_status"]["data_agent"]["termination_reason"] == "max_iterations_reached"


def test_assemble_stage2_results_node_merges_dual_sources():
    shared = {
        "agent_results": {
            "data_agent": {
                "charts": [{"id": "c1"}],
                "tables": [{"id": "t1"}],
                "execution_log": {"tools_executed": ["x"]},
            },
            "search_agent": {
                "background_context": "ctx",
                "consistency_points": ["p1"],
                "conflict_points": [],
                "blind_spots": [],
                "recommended_followups": [],
            },
        }
    }

    node = AssembleStage2ResultsNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "default"
    assert shared["stage2_results"]["charts"] == [{"id": "c1"}]
    assert shared["stage2_results"]["search_context"]["background_context"] == "ctx"
