"""
Report preview page.
"""
from pathlib import Path

import streamlit as st

st.title("Report Preview")

report_path = Path("report/report.md")
if report_path.exists():
    report_text = report_path.read_text(encoding="utf-8")
else:
    report_text = ""

st.markdown(report_text or "_No report.md found._")

if report_text:
    st.download_button(
        "Download report.md",
        data=report_text,
        file_name="report.md",
        mime="text/markdown",
    )
