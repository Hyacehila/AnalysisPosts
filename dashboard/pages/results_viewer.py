"""
Results viewer page for full Stage2/Stage3 run traceability.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st

from dashboard.logic.results_viewer_logic import CORE_JSON_FILES, load_results_viewer_bundle


st.title("Results Viewer")

refresh_col, _ = st.columns([1, 5])
if refresh_col.button("Refresh now", use_container_width=True):
    st.rerun()

bundle = load_results_viewer_bundle("report")
summary = bundle["summary"]
json_files_section = bundle["json_files_section"]

parse_errors = sum(
    1
    for item in json_files_section.values()
    if isinstance(item, dict) and item.get("exists") and not item.get("parse_ok")
)

(
    overview_tab,
    images_tab,
    tables_tab,
    forum_tab,
    search_tab,
    evidence_tab,
    json_tab,
) = st.tabs(
    [
        "Overview",
        "Image Results",
        "Table Results",
        "Forum Debate",
        "Search Summary",
        "Evidence Chain",
        "JSON Files",
    ]
)


def _normalize_table_data_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        if not data:
            return []
        if all(isinstance(item, dict) for item in data):
            return [dict(item) for item in data]
        return [{"index": idx, "value": json.dumps(item, ensure_ascii=False)} for idx, item in enumerate(data)]

    if isinstance(data, dict):
        if not data:
            return []
        if all(not isinstance(value, (dict, list)) for value in data.values()):
            return [data]
        return [
            {
                "key": str(key),
                "value": json.dumps(value, ensure_ascii=False),
            }
            for key, value in data.items()
        ]

    if data in (None, ""):
        return []

    return [{"value": str(data)}]


with overview_tab:
    metrics_row_1 = st.columns(4)
    metrics_row_1[0].metric("Charts", summary.get("charts", 0))
    metrics_row_1[1].metric("Tables", summary.get("tables", 0))
    metrics_row_1[2].metric("Insights", summary.get("insights", 0))
    metrics_row_1[3].metric("Forum Rounds", summary.get("forum_rounds", 0))

    metrics_row_2 = st.columns(4)
    metrics_row_2[0].metric("Executions", summary.get("executions", 0))
    metrics_row_2[1].metric("Decisions", summary.get("decisions", 0))
    metrics_row_2[2].metric("Status Events", summary.get("status_events", 0))
    metrics_row_2[3].metric("JSON Parse Errors", parse_errors)

    st.subheader("Core JSON file status")
    status_rows = []
    for file_name in CORE_JSON_FILES:
        meta = json_files_section.get(file_name, {})
        status_rows.append(
            {
                "file": file_name,
                "exists": bool(meta.get("exists")),
                "parse_ok": bool(meta.get("parse_ok")),
                "size_bytes": int(meta.get("size_bytes", 0) or 0),
                "updated_at": str(meta.get("updated_at") or ""),
                "error": str(meta.get("error") or ""),
            }
        )
    st.dataframe(status_rows, width="stretch")

with images_tab:
    images_section = bundle["images_section"]
    image_items = images_section.get("items", [])
    if not image_items:
        st.info("No chart images available.")
    else:
        st.caption(
            f"Total {len(image_items)} images. Source: {images_section.get('source', '')}"
        )
        for item in image_items:
            title = str(item.get("title") or item.get("id") or "chart")
            source_tool = str(item.get("source_tool") or "unknown")
            image_path = str(item.get("file_path") or "")
            st.subheader(title)
            st.caption(f"source_tool: {source_tool}")
            if image_path and Path(image_path).exists():
                st.image(image_path, caption=Path(image_path).name, width="stretch")
            else:
                st.warning(f"Image file not found: {image_path}")

            chart_analysis = str(item.get("chart_analysis") or "").strip()
            if chart_analysis:
                st.markdown("**Chart analysis**")
                st.write(chart_analysis)

            with st.expander("Chart metadata"):
                st.json(item)

with tables_tab:
    tables_section = bundle["tables_section"]
    table_items = tables_section.get("items", [])

    if not table_items:
        st.info("No table results available.")
    else:
        st.caption(f"Total {len(table_items)} tables")
        for table in table_items:
            title = str(table.get("title") or table.get("id") or "table")
            st.subheader(title)
            st.caption(
                f"source_tool: {table.get('source_tool') or 'unknown'} | source_type: {table.get('source_type') or 'unknown'}"
            )

            rows = _normalize_table_data_rows(table.get("data"))
            if rows:
                st.dataframe(rows, width="stretch")
            else:
                st.info("This table has no structured rows to display.")

            with st.expander("Raw table JSON"):
                st.json(table.get("raw") or table)

    execution_log = tables_section.get("execution_log", {})
    if execution_log:
        st.subheader("Execution log")
        st.json(execution_log)

with forum_tab:
    forum_section = bundle["forum_section"]
    rounds = forum_section.get("rounds", [])

    if not rounds:
        st.info("No forum rounds found in trace.json")
    else:
        for item in rounds:
            round_no = int(item.get("round", 0) or 0)
            decision = str(item.get("decision") or "")
            with st.expander(f"Round {round_no} | decision: {decision}", expanded=(round_no == len(rounds))):
                st.markdown("**Directive**")
                st.json(item.get("directive") or {})

                st.markdown("**Gaps**")
                gaps = item.get("gaps") or []
                if gaps:
                    for gap in gaps:
                        st.write(f"- {gap}")
                else:
                    st.write("- None")

                st.markdown("**Synthesized conclusions**")
                conclusions = item.get("synthesized_conclusions") or []
                if conclusions:
                    for conclusion in conclusions:
                        st.write(f"- {conclusion}")
                else:
                    st.write("- None")

    loop_status = forum_section.get("loop_status", {})
    if loop_status:
        st.subheader("Forum loop status")
        st.json(loop_status)

with search_tab:
    search_section = bundle["search_section"]

    st.subheader("Merged search context")
    search_context = search_section.get("search_context", {})
    if search_context:
        st.json(search_context)
    else:
        st.info("search_context is empty.")

    st.subheader("Search agent analyses")
    analyses = search_section.get("agent_analyses", [])
    if analyses:
        for idx, item in enumerate(analyses, 1):
            with st.expander(f"Analysis #{idx}"):
                st.json(item)
    else:
        st.info("No search_agent_analysis records found.")

    st.subheader("Search reflections")
    reflections = search_section.get("search_reflections", [])
    if reflections:
        st.dataframe(reflections, width="stretch")
    else:
        st.info("No search_reflections records found.")

    st.subheader("Search supplements")
    supplements = search_section.get("search_supplements", [])
    if supplements:
        st.dataframe(supplements, width="stretch")
    else:
        st.info("No search_supplements records found.")

with evidence_tab:
    chains = bundle["evidence_section"].get("chains", [])
    if not chains:
        st.info("No insight evidence chain is available.")
    else:
        for item in chains:
            insight_key = str(item.get("insight_key") or "")
            confidence = str(item.get("confidence") or "")
            header = f"{insight_key or 'insight'} | confidence: {confidence or 'n/a'}"
            with st.expander(header):
                st.markdown("**Insight**")
                st.write(str(item.get("insight_text") or ""))

                confidence_reasoning = str(item.get("confidence_reasoning") or "")
                if confidence_reasoning:
                    st.caption(f"confidence_reasoning: {confidence_reasoning}")

                st.markdown("**Supporting evidence**")
                supporting = item.get("supporting_evidence") or []
                if supporting:
                    st.json(supporting)
                else:
                    st.write("No supporting_evidence recorded.")

                st.markdown("**Matched executions**")
                matched_executions = item.get("matched_executions") or []
                if matched_executions:
                    st.dataframe(matched_executions, width="stretch")
                else:
                    st.write("No matched execution records.")

                st.markdown("**Matched decisions**")
                matched_decisions = item.get("matched_decisions") or []
                if matched_decisions:
                    st.dataframe(matched_decisions, width="stretch")
                else:
                    st.write("No matched decision records.")

with json_tab:
    selected = st.selectbox("Select JSON file", CORE_JSON_FILES, index=0)
    meta = json_files_section.get(selected, {})

    st.caption(
        f"exists={meta.get('exists', False)} | parse_ok={meta.get('parse_ok', False)} | "
        f"size={meta.get('size_bytes', 0)} bytes | updated_at={meta.get('updated_at', '')}"
    )

    if meta.get("error"):
        st.error(str(meta.get("error")))

    if meta.get("exists"):
        raw_text = str(meta.get("text") or "")
        st.download_button(
            f"Download {selected}",
            data=raw_text,
            file_name=selected,
            mime="application/json",
            use_container_width=True,
        )

    if meta.get("parse_ok"):
        st.json(meta.get("data") or {})
    elif meta.get("exists"):
        st.code(str(meta.get("text") or ""), language="json")
    else:
        st.info(f"{selected} not found.")
