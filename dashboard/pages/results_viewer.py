"""
Results viewer page (charts and data).
"""
from pathlib import Path
import json

import streamlit as st


st.title("Results Viewer")

charts_tab, data_tab = st.tabs(["Charts", "Analysis Data"])

with charts_tab:
    images_dir = Path("report/images")
    if images_dir.exists():
        images = sorted(images_dir.glob("*.png"))
        st.write(f"Found {len(images)} images")
        for img in images:
            st.image(str(img), caption=img.name, width="stretch")
    else:
        st.info("No report/images directory found.")

with data_tab:
    analysis_path = Path("report/analysis_data.json")
    if analysis_path.exists():
        st.subheader("analysis_data.json")
        analysis_text = analysis_path.read_text(encoding="utf-8")
        st.json(json.loads(analysis_text))
        st.download_button(
            "Download analysis_data.json",
            data=analysis_text,
            file_name="analysis_data.json",
            mime="application/json",
        )
    else:
        st.info("analysis_data.json not found.")

    insights_path = Path("report/insights.json")
    if insights_path.exists():
        st.subheader("insights.json")
        insights_text = insights_path.read_text(encoding="utf-8")
        st.json(json.loads(insights_text))
        st.download_button(
            "Download insights.json",
            data=insights_text,
            file_name="insights.json",
            mime="application/json",
        )
    else:
        st.info("insights.json not found.")
