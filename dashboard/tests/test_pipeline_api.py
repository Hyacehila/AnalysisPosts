"""
Tests for dashboard pipeline API.
"""
import json
from pathlib import Path

from dashboard.api.pipeline_api import (
    build_config_dict_from_form,
    build_shared_from_config,
    list_data_candidates,
    load_config_dict,
    run_pipeline,
    save_config_dict,
)


def test_load_and_save_config(tmp_path):
    cfg = {"pipeline": {"start_stage": 1, "run_stages": [1]}}
    path = tmp_path / "config.yaml"
    save_config_dict(cfg, str(path))
    loaded = load_config_dict(str(path))
    assert loaded["pipeline"]["start_stage"] == 1


def test_build_shared_from_config(tmp_path, monkeypatch):
    cfg = {
        "data": {
            "input_path": "tests/fixtures/sample_posts.json",
            "output_path": "data/enhanced_posts.json",
            "topics_path": "tests/fixtures/sample_topics.json",
            "sentiment_attributes_path": "tests/fixtures/sample_sentiment_attrs.json",
            "publisher_objects_path": "tests/fixtures/sample_publishers.json",
            "belief_system_path": "data/believe_system_common.json",
            "publisher_decision_path": "data/publisher_decision.json",
        },
        "pipeline": {"start_stage": 1, "run_stages": [1]},
        "stage1": {"mode": "async", "checkpoint": {"enabled": False}},
        "stage2": {"mode": "agent", "tool_source": "mcp", "agent_max_iterations": 1},
        "stage3": {"mode": "template", "max_iterations": 1, "min_score": 80},
        "runtime": {"concurrent_num": 1, "max_retries": 1, "wait_time": 1},
    }
    path = tmp_path / "config.yaml"
    save_config_dict(cfg, str(path))
    monkeypatch.setenv("ENHANCED_DATA_PATH", cfg["data"]["output_path"])

    shared = build_shared_from_config(str(path))
    assert "config" in shared
    assert shared["dispatcher"]["start_stage"] == 1


def test_run_pipeline_dry_run(tmp_path):
    cfg = {
        "data": {
            "input_path": "tests/fixtures/sample_posts.json",
            "output_path": "data/enhanced_posts.json",
            "topics_path": "tests/fixtures/sample_topics.json",
            "sentiment_attributes_path": "tests/fixtures/sample_sentiment_attrs.json",
            "publisher_objects_path": "tests/fixtures/sample_publishers.json",
            "belief_system_path": "data/believe_system_common.json",
            "publisher_decision_path": "data/publisher_decision.json",
        },
        "pipeline": {"start_stage": 1, "run_stages": [1]},
        "stage1": {"mode": "async", "checkpoint": {"enabled": False}},
        "stage2": {"mode": "agent", "tool_source": "mcp", "agent_max_iterations": 1},
        "stage3": {"mode": "template", "max_iterations": 1, "min_score": 80},
        "runtime": {"concurrent_num": 1, "max_retries": 1, "wait_time": 1},
    }
    path = tmp_path / "config.yaml"
    save_config_dict(cfg, str(path))

    shared = run_pipeline(str(path), dry_run=True)
    assert "config" in shared


def test_list_data_candidates(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "a.json").write_text("[]", encoding="utf-8")
    (data_dir / "b.json").write_text("[]", encoding="utf-8")
    results = list_data_candidates(str(data_dir))
    assert results == [(data_dir / "a.json").as_posix(), (data_dir / "b.json").as_posix()]


def test_build_config_dict_from_form():
    flat = {
        "data.input_path": "data/sample.json",
        "data.resume_if_exists": False,
        "pipeline.start_stage": 2,
        "pipeline.run_stages": [2, 3],
        "stage1.checkpoint.enabled": False,
    }
    cfg = build_config_dict_from_form(flat)
    assert cfg["data"]["input_path"] == "data/sample.json"
    assert cfg["data"]["resume_if_exists"] is False
    assert cfg["pipeline"]["start_stage"] == 2
    assert cfg["pipeline"]["run_stages"] == [2, 3]
    assert cfg["stage1"]["checkpoint"]["enabled"] is False
