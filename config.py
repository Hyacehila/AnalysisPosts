"""
Configuration loader and shared-store builder.

Uses a YAML file as the single source of truth for runtime settings.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import os

import yaml


@dataclass
class DataConfig:
    input_path: str = "data/posts.json"
    output_path: str = "data/enhanced_posts.json"
    resume_if_exists: bool = True
    topics_path: str = "data/topics.json"
    sentiment_attributes_path: str = "data/sentiment_attributes.json"
    publisher_objects_path: str = "data/publisher_objects.json"
    belief_system_path: str = "data/believe_system_common.json"
    publisher_decision_path: str = "data/publisher_decision.json"


@dataclass
class PipelineConfig:
    start_stage: int = 1
    run_stages: List[int] = field(default_factory=lambda: [1, 2, 3])


@dataclass
class Stage1CheckpointConfig:
    enabled: bool = True
    save_every: int = 100
    min_interval_seconds: float = 20.0


@dataclass
class Stage1NlpConfig:
    enabled: bool = True
    keyword_top_n: int = 8
    similarity_threshold: float = 0.85
    min_cluster_size: int = 2


@dataclass
class Stage1Config:
    mode: str = "async"
    checkpoint: Stage1CheckpointConfig = field(default_factory=Stage1CheckpointConfig)
    nlp: Stage1NlpConfig = field(default_factory=Stage1NlpConfig)


@dataclass
class Stage2Config:
    mode: str = "agent"
    tool_source: str = "mcp"
    agent_max_iterations: int = 10
    search_provider: str = "tavily"
    search_max_results: int = 5
    search_timeout_seconds: int = 20
    search_api_key: str = ""
    chart_min_per_category: Dict[str, int] = field(default_factory=lambda: {
        "sentiment": 1,
        "topic": 1,
        "geographic": 1,
        "interaction": 1,
        "nlp": 1,
    })
    chart_tool_policy: str = "coverage_first"
    chart_tool_allowlist: List[str] = field(default_factory=list)
    chart_missing_policy: str = "warn"


@dataclass
class Stage3Config:
    mode: str = "template"
    max_iterations: int = 5
    min_score: int = 80


@dataclass
class RuntimeConfig:
    concurrent_num: int = 60
    max_retries: int = 3
    wait_time: int = 8


@dataclass
class LLMConfig:
    glm_api_key: str = ""


@dataclass
class AppConfig:
    data: DataConfig = field(default_factory=DataConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    stage1: Stage1Config = field(default_factory=Stage1Config)
    stage2: Stage2Config = field(default_factory=Stage2Config)
    stage3: Stage3Config = field(default_factory=Stage3Config)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)


def load_config(path: str) -> AppConfig:
    """Load YAML configuration into AppConfig."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

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


def _derive_data_source_type(pipeline: PipelineConfig) -> str:
    if 1 in pipeline.run_stages and pipeline.start_stage == 1:
        return "original"
    return "enhanced"


def validate_config(config: AppConfig) -> None:
    """Validate configuration constraints and prerequisites."""
    valid_stage1_modes = {"async"}
    valid_stage2_modes = {"agent"}
    valid_stage3_modes = {"template", "iterative"}
    valid_tool_sources = {"mcp"}

    if config.stage1.mode not in valid_stage1_modes:
        raise ValueError(f"Invalid stage1 mode: {config.stage1.mode}")
    if config.stage2.mode not in valid_stage2_modes:
        raise ValueError(
            f"Invalid stage2 mode: {config.stage2.mode}. Stage2 only supports agent mode."
        )
    if config.stage3.mode not in valid_stage3_modes:
        raise ValueError(f"Invalid stage3 mode: {config.stage3.mode}")
    if config.stage2.tool_source not in valid_tool_sources:
        raise ValueError(
            f"Invalid stage2 tool_source: {config.stage2.tool_source}. Stage2 only supports mcp tools."
        )

    effective_key = resolve_glm_api_key(config)
    if not effective_key:
        raise EnvironmentError(
            "GLM API key is required (set env GLM_API_KEY or llm.glm_api_key in YAML)."
        )

    run_stages = config.pipeline.run_stages
    start_stage = config.pipeline.start_stage
    if not set(run_stages).issubset({1, 2, 3}):
        raise ValueError(f"run_stages must be subset of [1,2,3], got {run_stages}")
    if start_stage not in run_stages:
        raise ValueError(f"start_stage must be in run_stages, got {start_stage} not in {run_stages}")

    needs_stage2_output = 2 in run_stages and (1 not in run_stages or start_stage > 1)
    if needs_stage2_output and not os.path.exists(config.data.output_path):
        raise FileNotFoundError(f"Stage2 requires enhanced data file: {config.data.output_path}")

    needs_stage3_output = 3 in run_stages and (2 not in run_stages or start_stage > 2)
    if needs_stage3_output:
        required_files = [
            "report/analysis_data.json",
            "report/chart_analyses.json",
            "report/insights.json",
        ]
        missing_files = [p for p in required_files if not os.path.exists(p)]
        if missing_files:
            raise FileNotFoundError(f"Stage3 requires analysis outputs: {missing_files}")

    if config.stage2.mode == "agent" and config.stage2.tool_source == "mcp":
        if not os.environ.get("ENHANCED_DATA_PATH"):
            raise EnvironmentError("ENHANCED_DATA_PATH must be set for agent+mcp mode")

    if config.stage2.chart_tool_policy not in {"coverage_first"}:
        raise ValueError(f"Invalid stage2 chart_tool_policy: {config.stage2.chart_tool_policy}")
    if config.stage2.chart_missing_policy not in {"warn", "fail"}:
        raise ValueError(f"Invalid stage2 chart_missing_policy: {config.stage2.chart_missing_policy}")
    if config.stage2.search_provider not in {"tavily"}:
        raise ValueError(f"Invalid stage2 search_provider: {config.stage2.search_provider}")
    if int(config.stage2.search_max_results) <= 0:
        raise ValueError("stage2.search_max_results must be >= 1")
    if int(config.stage2.search_timeout_seconds) <= 0:
        raise ValueError("stage2.search_timeout_seconds must be >= 1")

    if not isinstance(config.stage2.chart_min_per_category, dict):
        raise ValueError("stage2.chart_min_per_category must be a dict")
    for key, value in config.stage2.chart_min_per_category.items():
        try:
            value_int = int(value)
        except Exception:
            raise ValueError(f"stage2.chart_min_per_category[{key}] must be int")
        if value_int < 0:
            raise ValueError(f"stage2.chart_min_per_category[{key}] must be >= 0")


def config_to_shared(config: AppConfig) -> dict:
    """Convert AppConfig into the shared store structure used by nodes."""
    data_source_type = _derive_data_source_type(config.pipeline)

    shared = {
        "data": {
            "blog_data": [],
            "topics_hierarchy": [],
            "sentiment_attributes": [],
            "publisher_objects": [],
            "data_paths": {
                "blog_data_path": config.data.input_path,
                "topics_path": config.data.topics_path,
                "sentiment_attributes_path": config.data.sentiment_attributes_path,
                "publisher_objects_path": config.data.publisher_objects_path,
                "belief_system_path": config.data.belief_system_path,
                "publisher_decision_path": config.data.publisher_decision_path,
            },
        },
        "dispatcher": {
            "start_stage": config.pipeline.start_stage,
            "run_stages": config.pipeline.run_stages,
            "current_stage": 0,
            "completed_stages": [],
            "next_action": None,
        },
        "config": {
            "enhancement_mode": config.stage1.mode,
            "stage1_checkpoint": {
                "enabled": config.stage1.checkpoint.enabled,
                "save_every": config.stage1.checkpoint.save_every,
                "min_interval_seconds": config.stage1.checkpoint.min_interval_seconds,
            },
            "stage1_nlp": {
                "enabled": config.stage1.nlp.enabled,
                "keyword_top_n": config.stage1.nlp.keyword_top_n,
                "similarity_threshold": config.stage1.nlp.similarity_threshold,
                "min_cluster_size": config.stage1.nlp.min_cluster_size,
            },
            "analysis_mode": config.stage2.mode,
            "tool_source": config.stage2.tool_source,
            "stage2_chart": {
                "min_per_category": (config.stage2.chart_min_per_category or {
                    "sentiment": 1,
                    "topic": 1,
                    "geographic": 1,
                    "interaction": 1,
                    "nlp": 1,
                }),
                "tool_policy": config.stage2.chart_tool_policy,
                "tool_allowlist": list(config.stage2.chart_tool_allowlist or []),
                "missing_policy": config.stage2.chart_missing_policy,
            },
            "web_search": {
                "provider": config.stage2.search_provider,
                "max_results": int(config.stage2.search_max_results),
                "timeout_seconds": int(config.stage2.search_timeout_seconds),
                "api_key": config.stage2.search_api_key,
            },
            "report_mode": config.stage3.mode,
            "agent_config": {"max_iterations": config.stage2.agent_max_iterations},
            "iterative_report_config": {
                "max_iterations": config.stage3.max_iterations,
                "satisfaction_threshold": config.stage3.min_score,
                "enable_review": True,
                "quality_check": True,
            },
            "data_source": {
                "type": data_source_type,
                "resume_if_exists": config.data.resume_if_exists,
                "enhanced_data_path": config.data.output_path,
            },
        },
        "agent": {
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": config.stage2.agent_max_iterations,
            "is_finished": False,
        },
        "report": {
            "iteration": 0,
            "current_draft": "",
            "revision_feedback": "",
            "review_history": [],
        },
        "stage1_results": {
            "statistics": {
                "total_blogs": 0,
                "processed_blogs": 0,
                "empty_fields": {
                    "sentiment_polarity_empty": 0,
                    "sentiment_attribute_empty": 0,
                    "topics_empty": 0,
                    "publisher_empty": 0,
                },
                "engagement_statistics": {
                    "total_reposts": 0,
                    "total_comments": 0,
                    "total_likes": 0,
                    "avg_reposts": 0.0,
                    "avg_comments": 0.0,
                    "avg_likes": 0.0,
                },
                "user_statistics": {
                    "unique_users": 0,
                    "top_active_users": [],
                    "user_type_distribution": {},
                },
                "content_statistics": {
                    "total_images": 0,
                    "blogs_with_images": 0,
                    "avg_content_length": 0.0,
                    "time_distribution": {},
                },
                "geographic_distribution": {},
            },
            "data_save": {
                "saved": False,
                "output_path": "",
                "data_count": 0,
            },
        },
        "stage2_results": {
            "charts": [],
            "tables": [],
            "insights": {
                "sentiment_insight": "",
                "topic_insight": "",
                "geographic_insight": "",
                "cross_dimension_insight": "",
                "summary_insight": "",
            },
            "execution_log": {
                "tools_executed": [],
                "total_charts": 0,
                "total_tables": 0,
                "execution_time": 0.0,
                "charts_by_category": {},
            },
            "output_files": {
                "charts_dir": "report/images/",
                "analysis_data": "report/analysis_data.json",
                "insights_file": "report/insights.json",
            },
        },
        "stage3_results": {
            "report_file": "report/report.md",
            "generation_mode": "",
            "iterations": 0,
            "final_score": 0,
            "report_reasoning": "",
            "data_citations": {},
            "hallucination_check": {},
        },
        "trace": {
            "decisions": [],
            "executions": [],
            "reflections": [],
            "insight_provenance": {},
        },
        "monitor": {
            "start_time": "",
            "current_stage": "",
            "current_node": "",
            "execution_log": [],
            "progress_status": {},
            "error_log": [],
        },
        "thinking": {
            "stage2_tool_decisions": [],
            "stage3_report_planning": [],
            "stage3_section_planning": {},
            "thinking_timestamps": [],
        },
    }

    return shared


def resolve_glm_api_key(config: AppConfig) -> str:
    """Resolve GLM API key with YAML taking precedence over environment."""
    yaml_key = (config.llm.glm_api_key or "").strip()
    if yaml_key:
        return yaml_key
    return os.environ.get("GLM_API_KEY", "").strip()


def apply_glm_api_key(config: AppConfig) -> None:
    """Apply GLM API key from config to environment when provided."""
    yaml_key = (config.llm.glm_api_key or "").strip()
    if yaml_key:
        os.environ["GLM_API_KEY"] = yaml_key
