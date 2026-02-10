"""
test_config.py — YAML config loading and shared conversion tests
"""
from pathlib import Path

import pytest

from config import (
    AppConfig,
    DataConfig,
    PipelineConfig,
    Stage1Config,
    Stage2Config,
    Stage3Config,
    RuntimeConfig,
    load_config,
    validate_config,
    config_to_shared,
)


def test_load_config_from_yaml(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join([
            "data:",
            "  input_path: \"data/posts.json\"",
            "pipeline:",
            "  start_stage: 1",
            "  run_stages: [1, 2]",
            "stage1:",
            "  mode: \"async\"",
            "stage2:",
            "  mode: \"workflow\"",
            "runtime:",
            "  concurrent_num: 10",
        ]),
        encoding="utf-8",
    )

    config = load_config(str(config_path))
    assert config.data.input_path == "data/posts.json"
    assert config.pipeline.run_stages == [1, 2]
    assert config.runtime.concurrent_num == 10


def test_validate_config_rejects_bad_modes():
    config = AppConfig(
        data=DataConfig(),
        pipeline=PipelineConfig(start_stage=1, run_stages=[1]),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="bad_mode"),
        stage3=Stage3Config(mode="template"),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_requires_stage2_output(tmp_path):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")

    config = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2, run_stages=[2]),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="workflow"),
        stage3=Stage3Config(mode="template"),
        runtime=RuntimeConfig(),
    )
    validate_config(config)


def test_config_to_shared_contains_required_keys():
    config = AppConfig()
    shared = config_to_shared(config)
    assert "data" in shared
    assert "config" in shared
    assert "dispatcher" in shared
    assert "stage1_results" in shared
    assert "stage2_results" in shared
    assert "stage3_results" in shared
