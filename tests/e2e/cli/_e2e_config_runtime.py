"""
Shared helpers for config-driven live E2E tests.
"""
from __future__ import annotations

import copy
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
RESERVED_CONFIG_PATH = REPO_ROOT / "config.yaml"
DATA_PATH_KEYS = (
    "input_path",
    "topics_path",
    "sentiment_attributes_path",
    "publisher_objects_path",
    "belief_system_path",
    "publisher_decision_path",
)


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_reserved_config() -> dict[str, Any]:
    if not RESERVED_CONFIG_PATH.exists():
        pytest.fail(f"Reserved config is missing: {RESERVED_CONFIG_PATH}")
    return load_yaml(RESERVED_CONFIG_PATH)


def require_yaml_api_key(config_dict: dict[str, Any]) -> str:
    key = str(config_dict.get("llm", {}).get("glm_api_key", "")).strip()
    if not key:
        pytest.fail("config.yaml.llm.glm_api_key is required for e2e live API tests.")
    return key


def _to_absolute_path(raw_path: str) -> str:
    path_obj = Path(raw_path)
    if path_obj.is_absolute():
        return str(path_obj)
    return str((REPO_ROOT / path_obj).resolve())


def build_runtime_config(
    tmp_path: Path,
    *,
    override_runtime: bool = True,
) -> Path:
    """
    Build a temporary config derived from reserved config.yaml.
    Keeps stage/pipeline settings, redirects outputs, and optionally overrides runtime.
    """
    config_dict = copy.deepcopy(load_reserved_config())
    require_yaml_api_key(config_dict)

    data_cfg = config_dict.setdefault("data", {})
    for key in DATA_PATH_KEYS:
        raw = data_cfg.get(key)
        if raw:
            data_cfg[key] = _to_absolute_path(str(raw))

    output_name = Path(str(data_cfg.get("output_path", "enhanced_posts_sample_30.json"))).name
    output_path = (tmp_path / "data" / output_name).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data_cfg["output_path"] = str(output_path)
    data_cfg["resume_if_exists"] = False

    if override_runtime:
        runtime_cfg = config_dict.setdefault("runtime", {})
        runtime_cfg["concurrent_num"] = 4
        runtime_cfg["max_retries"] = 1
        runtime_cfg["wait_time"] = 1

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(config_dict, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    _prepare_runtime_workspace(tmp_path)
    return config_path


def _prepare_runtime_workspace(tmp_path: Path) -> None:
    """
    Prepare runtime files expected by subprocess workers when cwd is tmp_path.
    Stage2 MCP currently resolves server path from cwd (utils/mcp_server.py).
    """
    target_utils = tmp_path / "utils"
    if target_utils.exists():
        return
    shutil.copytree(REPO_ROOT / "utils", target_utils)


def build_runtime_env(config_path: Path, tmp_path: Path) -> dict[str, str]:
    config_dict = load_yaml(config_path)
    key = require_yaml_api_key(config_dict)

    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = (
        str(REPO_ROOT)
        if not existing_pythonpath
        else str(REPO_ROOT) + os.pathsep + existing_pythonpath
    )
    env["GLM_API_KEY"] = key
    env["PROJECT_ROOT"] = str(REPO_ROOT)
    env["REPORT_DIR"] = str((tmp_path / "report").resolve())
    return env


@contextmanager
def patched_environ(overrides: dict[str, str]) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in overrides}
    for key, value in overrides.items():
        os.environ[key] = value
    try:
        yield
    finally:
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


@contextmanager
def working_directory(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def assert_full_pipeline_artifacts(config_path: Path, tmp_path: Path) -> None:
    config_dict = load_yaml(config_path)
    enhanced_data_path = Path(config_dict["data"]["output_path"])
    assert enhanced_data_path.exists(), f"Missing enhanced data output: {enhanced_data_path}"
    assert enhanced_data_path.stat().st_size > 0, f"Enhanced output is empty: {enhanced_data_path}"

    report_dir = tmp_path / "report"
    expected_files = (
        report_dir / "analysis_data.json",
        report_dir / "chart_analyses.json",
        report_dir / "insights.json",
        report_dir / "report.md",
    )
    for artifact in expected_files:
        assert artifact.exists(), f"Missing pipeline artifact: {artifact}"
        assert artifact.stat().st_size > 0, f"Pipeline artifact is empty: {artifact}"
