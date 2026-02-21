"""
Config-driven E2E test via CLI entrypoint.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _e2e_config_runtime import (  # noqa: E402
    assert_full_pipeline_artifacts,
    assert_stage1_contract,
    assert_stage2_contract,
    assert_stage3_contract,
    build_runtime_config,
    build_runtime_env,
)

pytestmark = [pytest.mark.e2e, pytest.mark.live_api]


def test_cli_runs_pipeline_from_stage1_and_produces_full_artifacts(tmp_path):
    config_path = build_runtime_config(
        tmp_path,
        override_runtime=True,
        start_stage_override=1,
    )
    env = build_runtime_env(config_path, tmp_path)

    proc = subprocess.run(
        [sys.executable, "-m", "main"],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )

    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"
    assert "[X] 执行出错" not in proc.stdout

    assert_stage1_contract(config_path, tmp_path)
    assert_stage2_contract(tmp_path)
    assert_stage3_contract(tmp_path)
    assert_full_pipeline_artifacts(config_path, tmp_path)
