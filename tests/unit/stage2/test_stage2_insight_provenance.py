"""LLMInsightNode trace provenance tests."""

from nodes.stage2.insight import LLMInsightNode


def _make_shared_with_trace(executions):
    return {
        "stage2_results": {},
        "trace": {
            "decisions": [],
            "executions": executions,
            "reflections": [],
            "insight_provenance": {},
        },
    }


def test_post_builds_provenance_for_each_insight():
    shared = _make_shared_with_trace(
        [
            {
                "id": "e_0001",
                "tool_name": "sentiment_trend_chart",
                "summary": "sentiment trend generated",
                "status": "success",
            },
            {
                "id": "e_0002",
                "tool_name": "topic_frequency_stats",
                "summary": "topic stats generated",
                "status": "success",
            },
        ]
    )
    exec_res = {
        "sentiment_summary": "sentiment_trend_chart shows a drop",
        "overall_summary": "topic_frequency_stats confirms concentration",
    }

    node = LLMInsightNode()
    node.post(shared, {}, exec_res)

    provenance = shared["trace"]["insight_provenance"]
    assert len(provenance) == 2
    texts = {item["text"] for item in provenance.values()}
    assert "sentiment_trend_chart shows a drop" in texts
    assert "topic_frequency_stats confirms concentration" in texts


def test_post_sets_high_confidence_with_multi_evidence():
    shared = _make_shared_with_trace(
        [
            {
                "id": "e_0001",
                "tool_name": "sentiment_trend_chart",
                "summary": "sentiment trend generated",
                "status": "success",
            },
            {
                "id": "e_0002",
                "tool_name": "topic_frequency_stats",
                "summary": "topic stats generated",
                "status": "success",
            },
        ]
    )
    exec_res = {
        "summary_insight": "sentiment_trend_chart and topic_frequency_stats both indicate risk",
    }

    node = LLMInsightNode()
    node.post(shared, {}, exec_res)

    provenance = shared["trace"]["insight_provenance"]
    insight = next(iter(provenance.values()))
    assert insight["confidence"] == "high"
    assert len(insight["supporting_evidence"]) >= 2


def test_post_sets_low_confidence_when_no_evidence():
    shared = _make_shared_with_trace([])
    exec_res = {
        "summary_insight": "this statement has no linked execution",
    }

    node = LLMInsightNode()
    node.post(shared, {}, exec_res)

    provenance = shared["trace"]["insight_provenance"]
    insight = next(iter(provenance.values()))
    assert insight["confidence"] == "low"
    assert insight["supporting_evidence"] == []
