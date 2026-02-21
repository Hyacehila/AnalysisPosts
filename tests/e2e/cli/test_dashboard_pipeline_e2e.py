"""
Config-driven E2E test via dashboard backend API.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from dashboard.api.pipeline_api import run_pipeline

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _e2e_config_runtime import (  # noqa: E402
    assert_full_pipeline_artifacts,
    assert_stage1_contract,
    assert_stage2_contract,
    assert_stage3_contract,
    build_runtime_config,
    build_runtime_env,
    patched_environ,
    working_directory,
)

pytestmark = [pytest.mark.e2e, pytest.mark.live_api]


def test_dashboard_api_stage3_entry_runs_stage3_only_and_keeps_artifact_contract(tmp_path):
    config_path = build_runtime_config(
        tmp_path,
        override_runtime=True,
        start_stage_override=3,
    )
    env = build_runtime_env(config_path, tmp_path)

    with patched_environ(env), working_directory(tmp_path):
        shared = run_pipeline(path=str(config_path), dry_run=False)

    completed = shared.get("pipeline_state", {}).get("completed_stages", [])
    assert completed == [3]

    assert_stage1_contract(config_path, tmp_path)
    assert_stage2_contract(tmp_path)
    assert_stage3_contract(tmp_path)
    assert_full_pipeline_artifacts(config_path, tmp_path)
