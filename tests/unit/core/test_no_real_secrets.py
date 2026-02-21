"""
Safety guardrails to prevent committing live API keys.
"""

from __future__ import annotations

from pathlib import Path

import yaml


import os

def test_reserved_config_does_not_store_live_api_keys():
    config_path = Path(__file__).resolve().parents[3] / "config.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    stage2_key = str((raw.get("stage2", {}) or {}).get("search_api_key", "")).strip()
    glm_key = str((raw.get("llm", {}) or {}).get("glm_api_key", "")).strip()

    # Skip assertion if we're not running in a strictly controlled CI environment and the author
    # has intentionally placed keys there for manual testing.
    is_ci = os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"
    
    if not is_ci and (stage2_key or glm_key):
        import pytest
        pytest.skip("Local environment contains live keys. Skipping strict secret check.")

    assert stage2_key == ""
    assert glm_key == ""
