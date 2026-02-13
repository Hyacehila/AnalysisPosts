"""
Streamlit entry for AnalysisPosts dashboard.
"""
import streamlit as st


st.set_page_config(page_title="AnalysisPosts Dashboard", layout="wide")
st.title("AnalysisPosts Dashboard")
st.write("Use the sidebar to navigate: Pipeline Console, Progress, Results, Report Preview.")

st.markdown(
    """
This dashboard provides:
- Pipeline console (configure + run)
- Progress monitor (status.json)
- Results viewer (charts/data)
- Report preview
"""
)
