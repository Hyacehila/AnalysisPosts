"""
DataAgent trace reflection tests (B3).
"""
from nodes.stage2.agent import ProcessResultNode


def test_process_result_post_appends_reflection_trace():
    shared = {
        "agent": {
            "execution_history": [],
            "current_iteration": 0,
        },
        "stage2_results": {
            "charts": [
                {"id": "c1", "source_tool": "sentiment_distribution_chart"},
            ]
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
    }

    node = ProcessResultNode()
    exec_res = {
        "execution_history": [
            {
                "tool_name": "sentiment_distribution_stats",
                "summary": "ok",
                "has_chart": True,
                "has_data": True,
                "error": False,
            }
        ],
        "new_iteration": 1,
        "should_continue": True,
        "reason": "继续分析",
    }

    action = node.post(shared, {}, exec_res)

    assert action == "continue"
    assert len(shared["trace"]["reflections"]) == 1
    reflection = shared["trace"]["reflections"][0]
    assert reflection["iteration"] == 1
    assert reflection["result"]["last_tool"]["tool_name"] == "sentiment_distribution_stats"
    assert "gaps" in reflection["result"]
    assert shared["trace"]["loop_status"]["data_agent"]["current"] == 1
    assert shared["trace"]["loop_status"]["data_agent"]["max"] == 10
