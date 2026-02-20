"""
Safety guardrails to prevent committing live API keys.
"""

from __future__ import annotations

from pathlib import Path

import yaml


def test_reserved_config_does_not_store_live_api_keys():
    config_path = Path(__file__).resolve().parents[3] / "config.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    stage2_key = str((raw.get("stage2", {}) or {}).get("search_api_key", "")).strip()
    glm_key = str((raw.get("llm", {}) or {}).get("glm_api_key", "")).strip()

    assert stage2_key == ""
    assert glm_key == ""
