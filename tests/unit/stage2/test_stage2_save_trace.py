"""SaveAnalysisResultsNode trace output tests."""
import json
from pathlib import Path

from nodes.stage2.save import SaveAnalysisResultsNode


def _build_shared():
    return {
        "stage2_results": {
            "charts": [{"id": "c1", "title": "chart", "path": "report/images/c1.png"}],
            "tables": [{"id": "t1", "title": "table", "data": {"k": 1}}],
            "chart_analyses": {"c1": {"analysis_status": "success"}},
            "insights": {"summary_insight": "ok"},
            "execution_log": {"tools_executed": ["sentiment_distribution_stats"]},
        },
        "trace": {
            "decisions": [{"id": "d_0001"}],
            "executions": [{"id": "e_0001"}],
            "reflections": [],
            "insight_provenance": {"insight_summary_insight": {"text": "ok"}},
        },
    }


def test_save_analysis_results_writes_trace_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shared = _build_shared()

    node = SaveAnalysisResultsNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)

    trace_file = Path(exec_res["trace_path"])
    assert trace_file.exists()
    loaded = json.loads(trace_file.read_text(encoding="utf-8"))
    assert loaded["decisions"][0]["id"] == "d_0001"


def test_output_files_contains_trace_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shared = _build_shared()

    node = SaveAnalysisResultsNode()
    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    node.post(shared, prep_res, exec_res)

    assert "trace_file" in shared["stage2_results"]["output_files"]
    trace_file = shared["stage2_results"]["output_files"]["trace_file"].replace("\\", "/")
    assert trace_file.endswith("report/trace.json")
