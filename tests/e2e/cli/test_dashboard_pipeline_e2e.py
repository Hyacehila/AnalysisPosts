"""
Config-driven E2E test via dashboard backend API.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from dashboard.api.pipeline_api import run_pipeline

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _e2e_config_runtime import (  # noqa: E402
    assert_full_pipeline_artifacts,
    build_runtime_config,
    build_runtime_env,
    load_yaml,
    patched_environ,
    working_directory,
)

pytestmark = [pytest.mark.e2e, pytest.mark.live_api]


def test_dashboard_api_runs_full_pipeline_from_reserved_config(tmp_path):
    config_path = build_runtime_config(tmp_path, override_runtime=True)
    env = build_runtime_env(config_path, tmp_path)

    with patched_environ(env), working_directory(tmp_path):
        shared = run_pipeline(path=str(config_path), dry_run=False)

    completed = shared.get("dispatcher", {}).get("completed_stages", [])
    assert 1 in completed and 2 in completed and 3 in completed
    assert_full_pipeline_artifacts(config_path, tmp_path)


def test_yaml_api_key_is_required_for_e2e(tmp_path):
    config_path = build_runtime_config(tmp_path, override_runtime=True)
    config_dict = load_yaml(config_path)
    config_dict.setdefault("llm", {})["glm_api_key"] = ""
    config_path.write_text(
        yaml.safe_dump(config_dict, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(pytest.fail.Exception):
        build_runtime_env(config_path, tmp_path)
