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
    build_runtime_config,
    build_runtime_env,
)

pytestmark = [pytest.mark.e2e, pytest.mark.live_api]


def test_cli_runs_full_pipeline_from_reserved_config(tmp_path):
    config_path = build_runtime_config(tmp_path, override_runtime=True)
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
    assert_full_pipeline_artifacts(config_path, tmp_path)
