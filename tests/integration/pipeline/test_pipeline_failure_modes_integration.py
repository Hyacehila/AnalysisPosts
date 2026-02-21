"""
Failure-mode checks for config-driven pipeline runtime.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dashboard.api.pipeline_api import run_pipeline
from tests.e2e.cli._e2e_config_runtime import (
    build_runtime_config,
    build_runtime_env,
    load_yaml,
    patched_environ,
    working_directory,
)

pytestmark = pytest.mark.integration


def test_run_pipeline_fails_without_glm_key(tmp_path, monkeypatch):
    monkeypatch.setenv("GLM_API_KEY", "test-key")
    config_path = build_runtime_config(tmp_path, override_runtime=True, start_stage_override=1)
    config_dict = load_yaml(config_path)
    config_dict.setdefault("llm", {})["glm_api_key"] = ""
    config_path.write_text(
        yaml.safe_dump(config_dict, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    monkeypatch.delenv("GLM_API_KEY", raising=False)

    with working_directory(tmp_path):
        with pytest.raises(EnvironmentError):
            run_pipeline(path=str(config_path), dry_run=False)


def test_run_pipeline_stage3_fails_when_stage3_inputs_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("GLM_API_KEY", "test-key")
    config_path = build_runtime_config(tmp_path, override_runtime=True, start_stage_override=1)
    env = build_runtime_env(config_path, tmp_path)
    cfg = load_yaml(config_path)

    # Ensure enhanced data precondition passes while Stage3 artifacts are absent.
    enhanced_path = Path(cfg["data"]["output_path"])
    enhanced_path.parent.mkdir(parents=True, exist_ok=True)
    enhanced_path.write_text("[]", encoding="utf-8")
    cfg.setdefault("pipeline", {})["start_stage"] = 3
    config_path.write_text(
        yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    report_dir = tmp_path / "report"
    if report_dir.exists():
        for child in report_dir.iterdir():
            if child.is_file():
                child.unlink()

    with patched_environ(env), working_directory(tmp_path):
        with pytest.raises(FileNotFoundError, match="Stage3 requires analysis outputs"):
            run_pipeline(path=str(config_path), dry_run=False)


def test_run_pipeline_fails_fast_on_invalid_start_stage(tmp_path, monkeypatch):
    monkeypatch.setenv("GLM_API_KEY", "test-key")
    config_path = build_runtime_config(tmp_path, override_runtime=True, start_stage_override=1)
    env = build_runtime_env(config_path, tmp_path)
    cfg = load_yaml(config_path)
    cfg.setdefault("pipeline", {})["start_stage"] = 4
    config_path.write_text(
        yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with patched_environ(env), working_directory(tmp_path):
        with pytest.raises(ValueError, match="start_stage must be one of"):
            run_pipeline(path=str(config_path), dry_run=False)
