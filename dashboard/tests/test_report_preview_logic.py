"""
Tests for report preview page helper logic.
"""

from pathlib import Path

from dashboard.pages.report_preview_logic import load_report_preview_artifacts


def test_load_report_preview_artifacts_reads_md_and_html(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "report.md").write_text("# 报告", encoding="utf-8")
    (report_dir / "report.html").write_text("<html><body>报告</body></html>", encoding="utf-8")

    artifacts = load_report_preview_artifacts(report_dir)

    assert artifacts["has_markdown"] is True
    assert artifacts["has_html"] is True
    assert artifacts["markdown_text"] == "# 报告"
    assert artifacts["html_text"].startswith("<html>")


def test_load_report_preview_artifacts_handles_missing_files(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)

    artifacts = load_report_preview_artifacts(Path(report_dir))

    assert artifacts["has_markdown"] is False
    assert artifacts["has_html"] is False
    assert artifacts["markdown_text"] == ""
    assert artifacts["html_text"] == ""
