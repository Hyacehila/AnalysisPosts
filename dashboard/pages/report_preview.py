"""
Report preview page.
"""
import streamlit as st
import streamlit.components.v1 as components

from dashboard.pages.report_preview_logic import load_report_preview_artifacts

st.title("Report Preview")

artifacts = load_report_preview_artifacts("report")
report_text = str(artifacts.get("markdown_text", "") or "")
report_html = str(artifacts.get("html_text", "") or "")

st.markdown(report_text or "_No report.md found._")

if report_text:
    st.download_button(
        "Download report.md",
        data=report_text,
        file_name="report.md",
        mime="text/markdown",
    )

if report_html:
    st.subheader("HTML Preview")
    components.html(report_html, height=700, scrolling=True)
    st.download_button(
        "Download report.html",
        data=report_html,
        file_name="report.html",
        mime="text/html",
    )
else:
    st.caption("No report.html found.")
