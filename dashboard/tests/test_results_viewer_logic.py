"""
Tests for Results Viewer data shaping logic.
"""

from __future__ import annotations

import json
from pathlib import Path

from dashboard.logic.results_viewer_logic import build_insight_evidence_chain, load_results_viewer_bundle


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_report_payloads() -> dict:
    return {
        "analysis_data.json": {
            "charts": [
                {
                    "id": "chart_1",
                    "title": "Chart 1",
                    "source_tool": "topic_chart",
                    "file_path": "report/images/chart_1.png",
                    "description": "demo chart",
                }
            ],
            "tables": [
                {
                    "id": "table_1",
                    "title": "Table 1",
                    "source_tool": "topic_stats",
                    "source_type": "mcp",
                    "data": {"value": 42},
                }
            ],
            "execution_log": {
                "tools_executed": ["topic_stats", "topic_chart"],
                "total_charts": 1,
                "total_tables": 1,
                "forum_rounds": 1,
            },
            "search_context": {
                "background_context": "background summary",
                "consistency_points": ["point-1"],
                "conflict_points": ["conflict-1"],
                "blind_spots": ["blind-1"],
                "recommended_followups": ["followup-1"],
                "forum_conclusions": ["conclusion-1"],
                "forum_rounds": 1,
            },
            "analysis_context": {
                "user_analysis_instruction": "focus on skepticism",
                "time_range_text": "2024-08-01 ~ 2024-08-31",
            },
        },
        "chart_analyses.json": {
            "chart_1": {
                "chart_id": "chart_1",
                "analysis_content": "trend went up",
                "analysis_status": "completed",
                "analysis_timestamp": "2026-02-21T18:40:00",
            }
        },
        "insights.json": {
            "sentiment_summary": "Sentiment is mostly positive.",
            "overall_summary": "Overall stable discussion.",
        },
        "trace.json": {
            "decisions": [
                {
                    "id": "d_0001",
                    "iteration": 1,
                    "action": "execute",
                    "tool_name": "topic_stats",
                    "reason": "Need baseline table",
                    "timestamp": "2026-02-21T18:18:01",
                }
            ],
            "executions": [
                {
                    "id": "e_0001",
                    "decision_ref": "d_0001",
                    "iteration": 1,
                    "tool_name": "topic_stats",
                    "status": "success",
                    "summary": "table generated",
                    "has_chart": False,
                    "has_data": True,
                    "error": False,
                    "timestamp": "2026-02-21T18:18:03",
                }
            ],
            "insight_provenance": {
                "insight_sentiment_summary": {
                    "text": "Sentiment is mostly positive.",
                    "supporting_evidence": [
                        {
                            "execution_id": "e_0001",
                            "tool_name": "topic_stats",
                            "detail": "distribution supports claim",
                        }
                    ],
                    "confidence": "high",
                    "confidence_reasoning": "multiple evidence lines",
                }
            },
            "forum_rounds": [
                {
                    "round": 1,
                    "decision": "supplement_search",
                    "directive": {"queries": ["why skepticism"]},
                    "gaps": ["missing direct trigger"],
                    "synthesized_conclusions": ["need more evidence"],
                }
            ],
            "search_agent_analysis": [
                {
                    "background_context": "search background",
                    "consistency_points": ["point-s"],
                    "conflict_points": ["conflict-s"],
                    "blind_spots": ["blind-s"],
                    "recommended_followups": ["followup-s"],
                }
            ],
            "search_reflections": [
                {
                    "round": 1,
                    "is_sufficient": False,
                    "missing": ["official statement"],
                }
            ],
            "search_supplements": [
                {
                    "queries": ["official statement"],
                    "documents_count": 3,
                }
            ],
            "loop_status": {
                "forum": {
                    "current": 1,
                    "max": 2,
                    "termination_reason": "continue",
                }
            },
        },
        "status.json": {
            "version": 2,
            "run_id": "run-001",
            "events": [
                {
                    "seq": 1,
                    "ts": "2026-02-21T18:18:00Z",
                    "event": "enter",
                    "stage": "stage2",
                    "node": "ExecuteTools",
                    "branch_id": "main",
                },
                {
                    "seq": 2,
                    "ts": "2026-02-21T18:18:03Z",
                    "event": "exit",
                    "stage": "stage2",
                    "node": "ExecuteTools",
                    "branch_id": "main",
                    "status": "completed",
                },
            ],
        },
    }


def _create_report_dir(tmp_path: Path) -> Path:
    report_dir = tmp_path / "report"
    payloads = _build_report_payloads()
    for filename, payload in payloads.items():
        _write_json(report_dir / filename, payload)
    images_dir = report_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "chart_1.png").write_bytes(b"\x89PNG\r\n\x1a\n")
    return report_dir


def test_load_results_viewer_bundle_builds_source_sections(tmp_path):
    report_dir = _create_report_dir(tmp_path)

    bundle = load_results_viewer_bundle(report_dir)

    assert bundle["summary"]["charts"] == 1
    assert bundle["summary"]["tables"] == 1
    assert bundle["summary"]["insights"] == 2
    assert bundle["summary"]["forum_rounds"] == 1
    assert bundle["summary"]["executions"] == 1
    assert bundle["summary"]["decisions"] == 1
    assert bundle["summary"]["status_events"] == 2
    assert bundle["summary"]["available_json_files"] == 5

    assert len(bundle["images_section"]["items"]) == 1
    assert len(bundle["tables_section"]["items"]) == 1
    assert len(bundle["forum_section"]["rounds"]) == 1
    assert len(bundle["search_section"]["agent_analyses"]) == 1
    assert len(bundle["evidence_section"]["chains"]) == 2


def test_load_results_viewer_bundle_handles_missing_and_invalid_json(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "trace.json").write_text("{not-valid-json", encoding="utf-8")

    bundle = load_results_viewer_bundle(report_dir)

    trace_meta = bundle["json_files_section"]["trace.json"]
    assert trace_meta["exists"] is True
    assert trace_meta["parse_ok"] is False
    assert trace_meta["error"]

    assert bundle["summary"]["available_json_files"] == 1
    assert bundle["images_section"]["items"] == []
    assert bundle["tables_section"]["items"] == []
    assert bundle["forum_section"]["rounds"] == []
    assert bundle["search_section"]["agent_analyses"] == []


def test_build_insight_evidence_chain_matches_execution_and_decision_refs():
    insights = {
        "sentiment_summary": "Sentiment is mostly positive.",
    }
    trace = {
        "executions": [
            {
                "id": "e_0001",
                "decision_ref": "d_0001",
                "tool_name": "topic_stats",
                "status": "success",
            }
        ],
        "decisions": [
            {
                "id": "d_0001",
                "tool_name": "topic_stats",
                "action": "execute",
            }
        ],
        "insight_provenance": {
            "insight_sentiment_summary": {
                "text": "Sentiment is mostly positive.",
                "supporting_evidence": [
                    {
                        "execution_id": "e_0001",
                        "tool_name": "topic_stats",
                    }
                ],
                "confidence": "high",
                "confidence_reasoning": "matched execution",
            }
        },
    }

    chains = build_insight_evidence_chain(insights, trace)

    assert len(chains) == 1
    item = chains[0]
    assert item["insight_key"] == "sentiment_summary"
    assert item["confidence"] == "high"
    assert len(item["supporting_evidence"]) == 1
    assert [entry["id"] for entry in item["matched_executions"]] == ["e_0001"]
    assert [entry["id"] for entry in item["matched_decisions"]] == ["d_0001"]


def test_load_results_viewer_bundle_uses_images_directory_fallback(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    _write_json(report_dir / "analysis_data.json", {"charts": [], "tables": []})
    images_dir = report_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (images_dir / "fallback.png").write_bytes(b"\x89PNG\r\n\x1a\n")

    bundle = load_results_viewer_bundle(report_dir)

    assert len(bundle["images_section"]["items"]) == 1
    assert bundle["images_section"]["items"][0]["source_tool"] == "filesystem_fallback"


def test_load_results_viewer_bundle_resolves_report_prefixed_chart_path(tmp_path):
    report_dir = _create_report_dir(tmp_path)

    bundle = load_results_viewer_bundle(report_dir)

    image_path = Path(bundle["images_section"]["items"][0]["file_path"])
    assert image_path.exists()
