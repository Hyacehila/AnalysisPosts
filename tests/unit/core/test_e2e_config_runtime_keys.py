"""
Unit checks for e2e runtime key resolution policy.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_runtime_helper_module():
    module_path = (
        Path(__file__).resolve().parents[2]
        / "e2e"
        / "cli"
        / "_e2e_config_runtime.py"
    )
    spec = importlib.util.spec_from_file_location("e2e_config_runtime_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_require_yaml_api_key_falls_back_to_env(monkeypatch):
    module = _load_runtime_helper_module()
    monkeypatch.setenv("GLM_API_KEY", "env-only-key")

    resolved = module.require_yaml_api_key({"llm": {"glm_api_key": ""}})

    assert resolved == "env-only-key"


def test_require_yaml_api_key_fails_when_missing(monkeypatch):
    module = _load_runtime_helper_module()
    monkeypatch.delenv("GLM_API_KEY", raising=False)

    with pytest.raises(pytest.fail.Exception):
        module.require_yaml_api_key({"llm": {"glm_api_key": ""}})
