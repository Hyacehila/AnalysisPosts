"""
Pipeline API for dashboard usage.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List
import copy

import yaml

from config import (
    AppConfig,
    DataConfig,
    PipelineConfig,
    Stage1CheckpointConfig,
    Stage1NlpConfig,
    Stage1Config,
    Stage2Config,
    Stage3Config,
    RuntimeConfig,
    LLMConfig,
    load_config,
    validate_config,
    config_to_shared,
    apply_glm_api_key,
)
from flow import create_main_flow
from utils.mcp_client import set_mcp_mode


def load_config_dict(path: str = "config.yaml") -> Dict[str, Any]:
    """Load raw YAML config into a dict."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        return {}
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def save_config_dict(data: Dict[str, Any], path: str = "config.yaml") -> str:
    """Save raw config dict to YAML file."""
    cfg_path = Path(path)
    cfg_path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return str(cfg_path)


def build_shared_from_config(path: str = "config.yaml") -> Dict[str, Any]:
    """Load config -> validate -> build shared store."""
    cfg = load_config(path)
    validate_config(cfg)
    return config_to_shared(cfg)


def default_config_dict() -> Dict[str, Any]:
    """Return default config as a dict."""
    return asdict(AppConfig())


def apply_defaults(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Merge raw config dict with defaults (raw wins)."""
    defaults = default_config_dict()
    merged = copy.deepcopy(raw or {})

    def _merge(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
        for key, value in src.items():
            if key not in dst or dst[key] is None:
                dst[key] = copy.deepcopy(value)
                continue
            if isinstance(value, dict) and isinstance(dst[key], dict):
                _merge(dst[key], value)

    _merge(merged, defaults)
    return merged


def list_data_candidates(data_dir: str = "data") -> List[str]:
    """List JSON files under data_dir."""
    base = Path(data_dir)
    if not base.exists():
        return []
    files = sorted(base.glob("*.json"))
    return [p.as_posix() for p in files]


def build_config_dict_from_form(flat: Dict[str, Any]) -> Dict[str, Any]:
    """Build nested config dict from flat dot-key dict."""
    config: Dict[str, Any] = {}
    for key, value in flat.items():
        if value is None:
            continue
        parts = key.split(".")
        cursor = config
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = value
    return config


def _config_from_dict(raw: Dict[str, Any]) -> AppConfig:
    data = DataConfig(**(raw.get("data", {}) or {}))
    pipeline = PipelineConfig(**(raw.get("pipeline", {}) or {}))

    stage1_raw = raw.get("stage1", {}) or {}
    checkpoint = Stage1CheckpointConfig(**(stage1_raw.get("checkpoint", {}) or {}))
    nlp_cfg = Stage1NlpConfig(**(stage1_raw.get("nlp", {}) or {}))
    stage1 = Stage1Config(
        mode=stage1_raw.get("mode", Stage1Config().mode),
        checkpoint=checkpoint,
        nlp=nlp_cfg,
    )

    stage2 = Stage2Config(**(raw.get("stage2", {}) or {}))
    stage3 = Stage3Config(**(raw.get("stage3", {}) or {}))
    runtime = RuntimeConfig(**(raw.get("runtime", {}) or {}))
    llm = LLMConfig(**(raw.get("llm", {}) or {}))

    return AppConfig(
        data=data,
        pipeline=pipeline,
        stage1=stage1,
        stage2=stage2,
        stage3=stage3,
        runtime=runtime,
        llm=llm,
    )


def validate_config_dict(raw: Dict[str, Any]) -> AppConfig:
    """Validate config dict using AppConfig validators."""
    cfg = _config_from_dict(raw)
    validate_config(cfg)
    return cfg


def run_pipeline(path: str = "config.yaml", dry_run: bool = False) -> Dict[str, Any]:
    """
    Run the full pipeline. Use dry_run to only build shared store.
    """
    cfg = load_config(path)
    apply_glm_api_key(cfg)
    if cfg.stage2.mode == "agent" and cfg.stage2.tool_source == "mcp":
        set_mcp_mode(True)
    if cfg.stage2.mode == "agent" and cfg.stage2.tool_source == "mcp":
        if cfg.data.output_path:
            os.environ["ENHANCED_DATA_PATH"] = os.path.abspath(cfg.data.output_path)
    validate_config(cfg)
    shared = config_to_shared(cfg)
    if dry_run:
        return shared

    flow = create_main_flow(
        concurrent_num=cfg.runtime.concurrent_num,
        max_retries=cfg.runtime.max_retries,
        wait_time=cfg.runtime.wait_time,
    )
    asyncio.run(flow.run_async(shared))
    return shared
