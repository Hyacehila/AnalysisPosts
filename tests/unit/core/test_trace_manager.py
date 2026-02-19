"""
Trace manager unit tests.
"""
import json

from utils.trace_manager import (
    init_trace,
    append_decision,
    append_execution,
    set_insight_provenance,
    dump_trace_json,
)


def test_init_trace_creates_required_keys():
    shared = {}
    trace = init_trace(shared)

    assert trace is shared["trace"]
    assert trace["decisions"] == []
    assert trace["executions"] == []
    assert trace["reflections"] == []
    assert trace["insight_provenance"] == {}


def test_append_decision_generates_id_and_fields():
    shared = {}
    decision_id = append_decision(
        shared,
        action="execute",
        tool_name="sentiment_distribution_stats",
        reason="collect baseline",
        iteration=1,
    )

    assert decision_id.startswith("d_")
    item = shared["trace"]["decisions"][0]
    assert item["id"] == decision_id
    assert item["iteration"] == 1
    assert item["action"] == "execute"
    assert item["tool_name"] == "sentiment_distribution_stats"
    assert item["reason"] == "collect baseline"
    assert item["timestamp"]


def test_append_execution_generates_id_and_fields():
    shared = {}
    execution_id = append_execution(
        shared,
        tool_name="sentiment_distribution_stats",
        iteration=1,
        status="success",
        summary="done",
        has_chart=False,
        has_data=True,
        error=False,
        decision_ref="d_0001",
    )

    assert execution_id.startswith("e_")
    item = shared["trace"]["executions"][0]
    assert item["id"] == execution_id
    assert item["decision_ref"] == "d_0001"
    assert item["tool_name"] == "sentiment_distribution_stats"
    assert item["status"] == "success"
    assert item["summary"] == "done"
    assert item["has_chart"] is False
    assert item["has_data"] is True
    assert item["error"] is False
    assert item["timestamp"]


def test_set_insight_provenance_replaces_map():
    shared = {}
    set_insight_provenance(shared, {"insight_001": {"text": "x"}})

    assert shared["trace"]["insight_provenance"] == {"insight_001": {"text": "x"}}


def test_dump_trace_json_writes_file(tmp_path):
    trace = {
        "decisions": [{"id": "d_0001"}],
        "executions": [{"id": "e_0001"}],
        "reflections": [],
        "insight_provenance": {"insight_001": {"text": "x"}},
    }
    out = tmp_path / "report" / "trace.json"

    written_path = dump_trace_json(trace, str(out))

    assert written_path == str(out)
    assert out.exists()
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded == trace
