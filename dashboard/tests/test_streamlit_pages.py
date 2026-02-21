"""
Tests for Streamlit page discovery constraints.
"""

from pathlib import Path


def test_streamlit_pages_do_not_expose_logic_helpers():
    page_dir = Path("dashboard/pages")
    logic_modules = sorted(item.name for item in page_dir.glob("*_logic.py"))

    assert logic_modules == []
