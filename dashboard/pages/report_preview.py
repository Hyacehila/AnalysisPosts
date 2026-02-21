"""
Report preview page.
"""
import streamlit as st
import streamlit.components.v1 as components

from dashboard.logic.report_preview_logic import (
    build_pdf_error_payload,
    load_report_preview_artifacts,
    write_pdf_error_log,
)

try:
    from dashboard.utils.pdf_generator import (
        PDF_RUNTIME_VERSION,
        diagnose_pdf_runtime,
        generate_pdf_from_html_content,
    )
except ImportError:
    PDF_RUNTIME_VERSION = "unavailable"
    diagnose_pdf_runtime = None
    generate_pdf_from_html_content = None

st.title("Report Preview")

artifacts = load_report_preview_artifacts("report")
preview_html = str(artifacts.get("preview_html", "") or "")
preview_source = str(artifacts.get("preview_source", "") or "")

if not preview_html:
    st.info("No report found. Please run the pipeline first.")
    st.stop()

if generate_pdf_from_html_content is None:
    st.error("PDF export dependency is unavailable. Please run `uv sync` and retry.")
else:
    preflight_col, generate_col = st.columns(2)

    if preflight_col.button("Run PDF Preflight", use_container_width=True):
        if diagnose_pdf_runtime is None:
            st.session_state["report_preview_pdf_preflight"] = {
                "ok": False,
                "stage": "import",
                "error_type": "ImportError",
                "error_message": "diagnose_pdf_runtime is unavailable.",
                "traceback": "",
                "diagnostics": {},
            }
        else:
            st.session_state["report_preview_pdf_preflight"] = diagnose_pdf_runtime("report")

    if generate_col.button("Generate PDF", use_container_width=True):
        with st.spinner("Preparing report PDF..."):
            try:
                pdf_bytes = generate_pdf_from_html_content(str(artifacts.get("pdf_html", "") or ""), report_dir="report")
            except Exception as exc:
                st.session_state["report_preview_pdf_bytes"] = b""
                payload = build_pdf_error_payload(exc)
                payload["log_path"] = write_pdf_error_log(payload, report_dir="report")
                st.session_state["report_preview_pdf_error_payload"] = payload
            else:
                st.session_state["report_preview_pdf_bytes"] = pdf_bytes
                st.session_state["report_preview_pdf_error_payload"] = {}
                if diagnose_pdf_runtime is not None:
                    st.session_state["report_preview_pdf_preflight"] = diagnose_pdf_runtime("report")

    preflight = st.session_state.get("report_preview_pdf_preflight", {})
    if isinstance(preflight, dict) and preflight:
        if preflight.get("ok"):
            st.success("PDF runtime check passed.")
        else:
            stage = str(preflight.get("stage", "") or "").strip()
            error_type = str(preflight.get("error_type", "") or "").strip()
            error_message = str(preflight.get("error_message", "") or "").strip()
            st.error(f"PDF preflight failed: [{stage}] {error_type} {error_message}".strip())
        with st.expander("PDF Runtime Details", expanded=False):
            st.json(preflight)

    payload = st.session_state.get("report_preview_pdf_error_payload", {})
    if isinstance(payload, dict) and payload:
        stage = str(payload.get("stage", "") or "").strip()
        error_type = str(payload.get("error_type", "") or "").strip()
        error_message = str(payload.get("error_message", "") or "").strip()
        st.error(f"Failed to generate PDF: [{stage}] {error_type} {error_message}".strip())
        log_path = str(payload.get("log_path", "") or "").strip()
        if log_path:
            st.caption(f"Detailed log: `{log_path}`")
        with st.expander("PDF Failure Details", expanded=False):
            st.json(payload)

    pdf_bytes = st.session_state.get("report_preview_pdf_bytes", b"")
    if isinstance(pdf_bytes, (bytes, bytearray)) and pdf_bytes:
        st.download_button(
            label="Download Report PDF (.pdf)",
            data=bytes(pdf_bytes),
            file_name="report.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

st.divider()

components.html(preview_html, height=900, scrolling=True)
if preview_source:
    st.caption(f"Preview source: {preview_source}")
st.caption(f"PDF runtime version: {PDF_RUNTIME_VERSION}")
