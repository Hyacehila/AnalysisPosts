"""
Live UI E2E: simulate a human starting the pipeline from Streamlit dashboard.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ui_runtime import (  # noqa: E402
    assert_report_artifacts,
    launch_dashboard,
    start_pipeline_from_ui,
    wait_pipeline_finished,
)
from tests.e2e.cli._e2e_config_runtime import build_runtime_config  # noqa: E402

pytestmark = [pytest.mark.e2e, pytest.mark.live_api, pytest.mark.ui_e2e]


def test_dashboard_ui_can_start_pipeline_and_produce_full_artifacts(tmp_path: Path) -> None:
    config_path = build_runtime_config(
        tmp_path,
        override_runtime=True,
        start_stage_override=2,
        overrides={
            "data": {
                "output_path": "tests/fixtures/sample_enhanced.json",
            },
            "stage2": {
                "chart_missing_policy": "warn",
                "chart_min_per_category": {
                    "sentiment": 0,
                    "topic": 0,
                    "geographic": 0,
                    "interaction": 0,
                    "nlp": 0,
                },
            },
        },
    )

    with launch_dashboard(tmp_path=tmp_path, config_path=config_path) as app:
        start_pipeline_from_ui(base_url=app.base_url, timeout_seconds=120)
        wait_pipeline_finished(tmp_path=tmp_path, timeout_seconds=1200)

    assert_report_artifacts(tmp_path=tmp_path, config_path=config_path)
