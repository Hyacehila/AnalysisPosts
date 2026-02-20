"""
Runtime profile checks for live E2E config builder.
"""
from __future__ import annotations

from _e2e_config_runtime import build_runtime_config, load_yaml


def test_build_runtime_config_applies_balanced_stage2_loop_caps(tmp_path):
    config_path = build_runtime_config(tmp_path, override_runtime=True)
    cfg = load_yaml(config_path)
    stage2 = cfg.get("stage2", {})

    assert stage2.get("agent_max_iterations") == 3
    assert stage2.get("search_reflection_max_rounds") == 2
    assert stage2.get("forum_max_rounds") == 3
