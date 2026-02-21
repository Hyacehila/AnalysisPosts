"""
test_config.py — YAML config loading and shared conversion tests
"""
import os

import pytest

from config import (
    AppConfig,
    DataConfig,
    LLMConfig,
    PipelineConfig,
    RuntimeConfig,
    Stage1Config,
    Stage2Config,
    Stage3Config,
    apply_glm_api_key,
    config_to_shared,
    load_config,
    resolve_glm_api_key,
    validate_config,
)


def test_load_config_from_yaml(tmp_path, monkeypatch):
    # Ensure external acceptance profile override does not leak into YAML-load assertions.
    monkeypatch.delenv("ACCEPTANCE_PROFILE", raising=False)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "data:",
                "  input_path: \"data/posts.json\"",
                "  resume_if_exists: false",
                "llm:",
                "  glm_api_key: \"yaml-key\"",
                "  acceptance_profile: \"quality\"",
                "  reasoning_enabled_stage2: true",
                "  reasoning_enabled_stage3: false",
                "  vision_thinking_enabled: true",
                "  request_timeout_seconds: 120",
                "pipeline:",
                "  start_stage: 2",
                "stage1:",
                "  mode: \"async\"",
                "stage2:",
                "  mode: \"agent\"",
                "  tool_source: \"mcp\"",
                "  search_provider: \"tavily\"",
                "  search_max_results: 7",
                "  search_timeout_seconds: 30",
                "  search_api_key: \"tavily-yaml-key\"",
                "stage3:",
                "  max_iterations: 3",
                "  min_score: 85",
                "  chapter_review_max_rounds: 2",
                "runtime:",
                "  concurrent_num: 10",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(str(config_path))
    assert config.data.input_path == "data/posts.json"
    assert config.data.resume_if_exists is False
    assert config.pipeline.start_stage == 2
    assert config.runtime.concurrent_num == 10
    assert config.llm.glm_api_key == "yaml-key"
    assert config.llm.acceptance_profile == "quality"
    assert config.llm.reasoning_enabled_stage2 is True
    assert config.llm.reasoning_enabled_stage3 is False
    assert config.llm.vision_thinking_enabled is True
    assert config.llm.request_timeout_seconds == 120
    assert config.stage2.search_provider == "tavily"
    assert config.stage2.search_max_results == 7
    assert config.stage2.search_timeout_seconds == 30
    assert config.stage2.search_api_key == "tavily-yaml-key"
    assert config.stage3.max_iterations == 3
    assert config.stage3.min_score == 85
    assert config.stage3.chapter_review_max_rounds == 2


def test_load_config_rejects_legacy_run_stages(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "pipeline:",
                "  start_stage: 1",
                "  run_stages: [1, 2, 3]",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(TypeError):
        load_config(str(config_path))


def test_load_config_rejects_legacy_stage3_mode(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "stage3:",
                "  mode: template",
                "  max_iterations: 3",
                "  min_score: 80",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(TypeError):
        load_config(str(config_path))


def test_validate_config_rejects_bad_stage2_mode():
    config = AppConfig(
        data=DataConfig(),
        pipeline=PipelineConfig(start_stage=1),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="bad_mode"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
        llm=LLMConfig(glm_api_key="test-key"),
    )
    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_rejects_invalid_start_stage(monkeypatch):
    monkeypatch.setenv("GLM_API_KEY", "test-key")
    config = AppConfig(
        data=DataConfig(),
        pipeline=PipelineConfig(start_stage=4),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_requires_stage2_output(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    config = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    validate_config(config)


def test_validate_config_requires_stage3_outputs(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    config = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=3),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(FileNotFoundError):
        validate_config(config)


def test_validate_config_rejects_bad_search_provider(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    config = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp", search_provider="bad"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )

    with pytest.raises(ValueError):
        validate_config(config)


def test_validate_config_rejects_non_positive_search_limits(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    bad_results_cfg = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(
            mode="agent",
            tool_source="mcp",
            search_provider="tavily",
            search_max_results=0,
            search_timeout_seconds=20,
        ),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(bad_results_cfg)

    bad_timeout_cfg = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(
            mode="agent",
            tool_source="mcp",
            search_provider="tavily",
            search_max_results=5,
            search_timeout_seconds=0,
        ),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(bad_timeout_cfg)


def test_validate_config_rejects_non_positive_stage2_loop_limits(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    bad_search_loop = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(
            mode="agent",
            tool_source="mcp",
            search_provider="tavily",
            search_max_results=5,
            search_timeout_seconds=20,
            search_reflection_max_rounds=0,
        ),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(bad_search_loop)

    bad_forum_loop = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(
            mode="agent",
            tool_source="mcp",
            search_provider="tavily",
            search_max_results=5,
            search_timeout_seconds=20,
            forum_max_rounds=0,
        ),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )
    with pytest.raises(ValueError):
        validate_config(bad_forum_loop)


def test_validate_config_rejects_forum_min_rounds_over_max(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    bad_relation_cfg = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(
            mode="agent",
            tool_source="mcp",
            search_provider="tavily",
            search_max_results=5,
            search_timeout_seconds=20,
            forum_max_rounds=2,
            forum_min_rounds_for_sufficient=3,
        ),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
    )

    with pytest.raises(ValueError):
        validate_config(bad_relation_cfg)


def test_validate_config_rejects_invalid_stage3_review_rounds(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    bad_cfg = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp"),
        stage3=Stage3Config(chapter_review_max_rounds=0),
        runtime=RuntimeConfig(),
    )

    with pytest.raises(ValueError):
        validate_config(bad_cfg)


def test_validate_config_rejects_invalid_llm_acceptance_profile(tmp_path, monkeypatch):
    output_path = tmp_path / "enhanced.json"
    output_path.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("ENHANCED_DATA_PATH", str(output_path))
    monkeypatch.setenv("GLM_API_KEY", "test-key")

    bad_cfg = AppConfig(
        data=DataConfig(output_path=str(output_path)),
        pipeline=PipelineConfig(start_stage=2),
        stage1=Stage1Config(mode="async"),
        stage2=Stage2Config(mode="agent", tool_source="mcp"),
        stage3=Stage3Config(),
        runtime=RuntimeConfig(),
        llm=LLMConfig(acceptance_profile="fastest"),
    )

    with pytest.raises(ValueError):
        validate_config(bad_cfg)


def test_resolve_glm_api_key_yaml_over_env(monkeypatch):
    monkeypatch.setenv("GLM_API_KEY", "env-key")
    config = AppConfig(llm=LLMConfig(glm_api_key="yaml-key"))

    assert resolve_glm_api_key(config) == "yaml-key"
    apply_glm_api_key(config)
    assert os.environ.get("GLM_API_KEY") == "yaml-key"


def test_config_to_shared_contains_required_keys():
    config = AppConfig()
    shared = config_to_shared(config)

    assert "data" in shared
    assert "config" in shared
    assert "pipeline_state" in shared
    assert "stage1_results" in shared
    assert "stage2_results" in shared
    assert "stage3_results" in shared
    assert "trace" in shared
    assert "search" in shared
    assert "search_results" in shared
    assert "agent_results" in shared
    assert shared["pipeline_state"] == {
        "start_stage": 1,
        "current_stage": 0,
        "completed_stages": [],
    }
    assert shared["config"]["data_source"]["resume_if_exists"] is True
    assert shared["config"]["web_search"] == {
        "provider": "tavily",
        "max_results": 5,
        "timeout_seconds": 20,
        "api_key": "",
    }
    assert shared["config"]["stage2_loops"] == {
        "agent_max_iterations": 10,
        "search_reflection_max_rounds": 2,
        "forum_max_rounds": 5,
        "forum_min_rounds_for_sufficient": 2,
    }
    assert shared["config"]["stage3_review"] == {
        "chapter_review_max_rounds": 2,
        "min_score": 80,
    }
    assert shared["forum"] == {
        "current_round": 0,
        "rounds": [],
        "current_directive": {},
        "visual_analyses": [],
    }
    assert set(shared["trace"].keys()) == {
        "decisions",
        "executions",
        "reflections",
        "insight_provenance",
        "loop_status",
    }
    assert shared["agent_results"] == {
        "data_agent": {},
        "search_agent": {},
    }


def test_config_to_shared_defaults_llm_controls_to_fast_profile():
    config = AppConfig(llm=LLMConfig(glm_api_key="test-key"))
    shared = config_to_shared(config)

    assert shared["config"]["llm"] == {
        "acceptance_profile": "fast",
        "reasoning_enabled_stage2": False,
        "reasoning_enabled_stage3": False,
        "vision_thinking_enabled": False,
        "request_timeout_seconds": 120,
    }


def test_config_to_shared_quality_profile_enables_reasoning_and_thinking():
    config = AppConfig(
        llm=LLMConfig(
            glm_api_key="test-key",
            acceptance_profile="quality",
        )
    )
    shared = config_to_shared(config)

    assert shared["config"]["llm"] == {
        "acceptance_profile": "quality",
        "reasoning_enabled_stage2": True,
        "reasoning_enabled_stage3": True,
        "vision_thinking_enabled": True,
        "request_timeout_seconds": 120,
    }
