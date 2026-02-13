"""
Pipeline console page: configure, validate, and run the pipeline.
"""
from __future__ import annotations

import threading
import os
import time
from datetime import datetime
from typing import List

import streamlit as st

from dashboard.api.pipeline_api import (
    apply_defaults,
    build_config_dict_from_form,
    list_data_candidates,
    load_config_dict,
    save_config_dict,
    validate_config_dict,
    run_pipeline,
)
from dashboard.api.status_api import read_status, write_status
from utils.run_state import is_running, set_running


def _is_running() -> bool:
    return is_running()


def _set_running(flag: bool) -> None:
    set_running(flag)


def _run_pipeline_safe(config_path: str) -> None:
    try:
        run_pipeline(config_path, dry_run=False)
    except Exception as exc:
        status = read_status()
        entry = {
            "time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "stage": status.get("current_stage", ""),
            "node": "PipelineConsole",
            "status": "failed",
            "extra": {},
            "error": str(exc),
        }
        status.setdefault("execution_log", []).append(entry)
        status.setdefault("error_log", []).append(entry)
        try:
            write_status(status)
        except Exception as write_exc:
            print(f"[PipelineConsole] 写入 status.json 失败: {write_exc}")
    finally:
        _set_running(False)


def _path_selector(label: str, current: str, options: List[str], key: str) -> str:
    custom_label = "Custom..."
    options = list(dict.fromkeys(options))  # stable unique
    if current and current not in options:
        options.append(current)

    select_options = options + [custom_label]
    if current and current in options:
        default_index = select_options.index(current)
    else:
        default_index = select_options.index(custom_label)

    selected = st.selectbox(label, select_options, index=default_index, key=key)
    if selected == custom_label:
        return st.text_input(f"{label} (custom)", value=current or "", key=f"{key}_custom").strip()
    return selected


def _safe_index(options: List[str] | List[int], value: str | int, fallback: int = 0) -> int:
    try:
        return options.index(value)
    except ValueError:
        return fallback


st.title("Pipeline Console")

config_path = st.text_input("Config Path", value="config.yaml", key="config_path")
raw_config = load_config_dict(config_path)
config = apply_defaults(raw_config)

data_candidates = list_data_candidates("data")

with st.form("config_form"):
    st.subheader("Key Settings")
    left, right = st.columns(2)

    with left:
        input_path = _path_selector(
            "Input data path",
            config["data"]["input_path"],
            data_candidates,
            key="input_path",
        )
        topics_path = _path_selector(
            "Topics path",
            config["data"]["topics_path"],
            data_candidates,
            key="topics_path",
        )
        sentiment_attributes_path = _path_selector(
            "Sentiment attributes path",
            config["data"]["sentiment_attributes_path"],
            data_candidates,
            key="sentiment_attributes_path",
        )
        publisher_objects_path = _path_selector(
            "Publisher objects path",
            config["data"]["publisher_objects_path"],
            data_candidates,
            key="publisher_objects_path",
        )
        belief_system_path = _path_selector(
            "Belief system path",
            config["data"]["belief_system_path"],
            data_candidates,
            key="belief_system_path",
        )

    with right:
        output_path = _path_selector(
            "Enhanced data output path",
            config["data"]["output_path"],
            data_candidates,
            key="output_path",
        )
        publisher_decision_path = _path_selector(
            "Publisher decision path",
            config["data"]["publisher_decision_path"],
            data_candidates,
            key="publisher_decision_path",
        )

        start_stage = st.selectbox(
            "Start stage",
            [1, 2, 3],
            index=_safe_index([1, 2, 3], config["pipeline"]["start_stage"], fallback=0),
        )
        run_stages = st.multiselect(
            "Run stages",
            [1, 2, 3],
            default=[s for s in config["pipeline"]["run_stages"] if s in [1, 2, 3]],
        )

        st.markdown("**Stage2**: `agent + mcp` (fixed)")
        stage3_mode = st.selectbox(
            "Stage3 mode",
            ["template", "iterative"],
            index=_safe_index(["template", "iterative"], config["stage3"]["mode"], fallback=0),
        )

    with st.expander("Advanced settings", expanded=False):
        st.markdown("**LLM settings**")
        glm_api_key = st.text_input(
            "GLM API Key",
            value=config.get("llm", {}).get("glm_api_key", ""),
            type="password",
            help="Saved to config.yaml in plaintext. YAML value overrides GLM_API_KEY env.",
        )

        st.markdown("**Data source**")
        resume_if_exists = st.checkbox(
            "Resume if enhanced data exists",
            value=bool(config["data"].get("resume_if_exists", True)),
        )

        st.markdown("**Runtime settings**")
        concurrent_num = st.number_input(
            "Runtime concurrent num",
            min_value=1,
            step=1,
            value=int(config["runtime"]["concurrent_num"]),
        )
        max_retries = st.number_input(
            "Runtime max retries",
            min_value=0,
            step=1,
            value=int(config["runtime"]["max_retries"]),
        )
        wait_time = st.number_input(
            "Runtime wait time (seconds)",
            min_value=0,
            step=1,
            value=int(config["runtime"]["wait_time"]),
        )

        st.markdown("**Stage 1**")
        stage1_mode = st.selectbox(
            "Stage1 mode",
            ["async"],
            index=0,
        )
        checkpoint_enabled = st.checkbox(
            "Stage1 checkpoint enabled",
            value=bool(config["stage1"]["checkpoint"]["enabled"]),
        )
        checkpoint_save_every = st.number_input(
            "Stage1 checkpoint save every",
            min_value=1,
            step=1,
            value=int(config["stage1"]["checkpoint"]["save_every"]),
        )
        checkpoint_min_interval = st.number_input(
            "Stage1 checkpoint min interval (seconds)",
            min_value=0.0,
            step=1.0,
            value=float(config["stage1"]["checkpoint"]["min_interval_seconds"]),
        )
        nlp_enabled = st.checkbox(
            "Stage1 NLP enabled",
            value=bool(config["stage1"]["nlp"]["enabled"]),
        )
        nlp_keyword_top_n = st.number_input(
            "Stage1 NLP keyword top N",
            min_value=1,
            step=1,
            value=int(config["stage1"]["nlp"]["keyword_top_n"]),
        )
        nlp_similarity = st.number_input(
            "Stage1 NLP similarity threshold",
            min_value=0.0,
            max_value=1.0,
            step=0.01,
            value=float(config["stage1"]["nlp"]["similarity_threshold"]),
        )
        nlp_min_cluster = st.number_input(
            "Stage1 NLP min cluster size",
            min_value=1,
            step=1,
            value=int(config["stage1"]["nlp"]["min_cluster_size"]),
        )

        st.markdown("**Stage 2**")
        agent_max_iterations = st.number_input(
            "Stage2 agent max iterations",
            min_value=1,
            step=1,
            value=int(config["stage2"]["agent_max_iterations"]),
        )

        st.markdown("**Stage 3**")
        report_max_iterations = st.number_input(
            "Stage3 report max iterations",
            min_value=1,
            step=1,
            value=int(config["stage3"]["max_iterations"]),
        )
        report_min_score = st.number_input(
            "Stage3 report min score",
            min_value=0,
            max_value=100,
            step=1,
            value=int(config["stage3"]["min_score"]),
        )

    col1, col2, col3 = st.columns(3)
    save_clicked = col1.form_submit_button("Save Config")
    dry_run_clicked = col2.form_submit_button("Dry Run")
    run_clicked = col3.form_submit_button("Start Analysis")


flat_config = {
    "data.input_path": input_path,
    "data.output_path": output_path,
    "data.resume_if_exists": bool(resume_if_exists),
    "data.topics_path": topics_path,
    "data.sentiment_attributes_path": sentiment_attributes_path,
    "data.publisher_objects_path": publisher_objects_path,
    "data.belief_system_path": belief_system_path,
    "data.publisher_decision_path": publisher_decision_path,
    "pipeline.start_stage": int(start_stage),
    "pipeline.run_stages": sorted(run_stages),
    "stage1.mode": stage1_mode,
    "stage1.checkpoint.enabled": bool(checkpoint_enabled),
    "stage1.checkpoint.save_every": int(checkpoint_save_every),
    "stage1.checkpoint.min_interval_seconds": float(checkpoint_min_interval),
    "stage1.nlp.enabled": bool(nlp_enabled),
    "stage1.nlp.keyword_top_n": int(nlp_keyword_top_n),
    "stage1.nlp.similarity_threshold": float(nlp_similarity),
    "stage1.nlp.min_cluster_size": int(nlp_min_cluster),
    "stage2.mode": "agent",
    "stage2.tool_source": "mcp",
    "stage2.agent_max_iterations": int(agent_max_iterations),
    "stage3.mode": stage3_mode,
    "stage3.max_iterations": int(report_max_iterations),
    "stage3.min_score": int(report_min_score),
    "runtime.concurrent_num": int(concurrent_num),
    "runtime.max_retries": int(max_retries),
    "runtime.wait_time": int(wait_time),
    "llm.glm_api_key": glm_api_key,
}

config_dict = build_config_dict_from_form(flat_config)

validation_errors: List[str] = []
if not run_stages:
    validation_errors.append("Run stages cannot be empty.")
if run_stages and start_stage not in run_stages:
    validation_errors.append("Start stage must be included in run stages.")
for path_label, value in [
    ("Input data path", input_path),
    ("Enhanced data output path", output_path),
    ("Topics path", topics_path),
    ("Sentiment attributes path", sentiment_attributes_path),
    ("Publisher objects path", publisher_objects_path),
    ("Belief system path", belief_system_path),
    ("Publisher decision path", publisher_decision_path),
]:
    if not value:
        validation_errors.append(f"{path_label} cannot be empty.")

if save_clicked or dry_run_clicked or run_clicked:
    if validation_errors:
        for err in validation_errors:
            st.error(err)
    else:
        if save_clicked:
            save_config_dict(config_dict, config_path)
            st.success("Config saved.")

        if dry_run_clicked:
            try:
                if output_path:
                    os.environ["ENHANCED_DATA_PATH"] = os.path.abspath(output_path)
                validate_config_dict(config_dict)
                st.success("Dry run validation passed.")
            except Exception as exc:
                st.error(f"Dry run failed: {exc}")

        if run_clicked:
            if _is_running():
                st.warning("Pipeline is already running.")
            else:
                try:
                    if output_path:
                        os.environ["ENHANCED_DATA_PATH"] = os.path.abspath(output_path)
                    validate_config_dict(config_dict)
                    save_config_dict(config_dict, config_path)
                except Exception as exc:
                    st.error(f"Cannot start: {exc}")
                else:
                    _set_running(True)
                    thread = threading.Thread(
                        target=_run_pipeline_safe,
                        args=(config_path,),
                        daemon=True,
                    )
                    thread.start()
                    st.success("Pipeline started.")

st.subheader("Status")
status = read_status()
stage_col, node_col = st.columns(2)
with stage_col:
    st.metric("Current Stage", status.get("current_stage", ""))
with node_col:
    st.metric("Current Node", status.get("current_node", ""))

if status.get("execution_log"):
    st.subheader("Recent Logs")
    st.dataframe(status.get("execution_log", [])[-20:], width="stretch")

if status.get("error_log"):
    st.subheader("Errors")
    st.dataframe(status.get("error_log", [])[-20:], width="stretch")

auto_refresh = st.toggle("Auto refresh", value=True, key="pipeline_auto_refresh")
if auto_refresh:
    time.sleep(2)
    st.rerun()
