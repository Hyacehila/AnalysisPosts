"""
Progress monitor page.
"""
import time

import streamlit as st

from dashboard.api.status_api import read_status


st.title("Progress Monitor")

status = read_status()

current_stage = status.get("current_stage", "")
current_node = status.get("current_node", "")
st.metric("Current Stage", current_stage)
st.metric("Current Node", current_node)

st.subheader("Execution Log")
st.dataframe(status.get("execution_log", [])[-20:], width="stretch")

errors = status.get("error_log", [])
if errors:
    st.subheader("Errors")
    st.dataframe(errors[-20:], width="stretch")

auto_refresh = st.toggle("Auto refresh", value=True, key="progress_auto_refresh")
if auto_refresh:
    time.sleep(2)
    st.rerun()
