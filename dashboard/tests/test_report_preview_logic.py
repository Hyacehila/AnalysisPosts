"""
Tests for report preview page helper logic.
"""

from pathlib import Path

from dashboard.logic.report_preview_logic import (
    build_pdf_error_payload,
    format_error_message,
    load_report_preview_artifacts,
    write_pdf_error_log,
)


_PNG_1PX = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00"
    b"\x00\x00\x0cIDAT\x08\xd7c\xf8\xff\xff?\x00\x05\xfe"
    b"\x02\xfeA\xd9R\xde\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _write_test_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_PNG_1PX)


def test_load_report_preview_artifacts_prefers_html_and_inlines_images(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "report.md").write_text("# 兜底报告", encoding="utf-8")
    (report_dir / "report.html").write_text(
        "<html><body><h1 id='top'>报告</h1>"
        "<img src='./images/chart.png' alt='chart' />"
        "<a href='#top'>回到顶部</a></body></html>",
        encoding="utf-8",
    )
    _write_test_png(report_dir / "images" / "chart.png")

    artifacts = load_report_preview_artifacts(report_dir)

    assert artifacts["has_preview"] is True
    assert artifacts["preview_source"] == "html"
    assert artifacts["has_markdown"] is True
    assert artifacts["has_html"] is True
    preview_html = str(artifacts["preview_html"])
    assert "data:image/png;base64," in preview_html
    assert "href='#top'" in preview_html or 'href="#top"' in preview_html
    pdf_html = str(artifacts["pdf_html"])
    assert "data:image/png;base64," not in pdf_html
    assert "./images/chart.png" in pdf_html


def test_load_report_preview_artifacts_falls_back_to_markdown_preview(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "report.md").write_text(
        "# 标题\n\n## 目录\n- [跳转](#标题)\n\n![图](./images/chart.png)\n\n[外链](https://example.com)",
        encoding="utf-8",
    )
    _write_test_png(report_dir / "images" / "chart.png")

    artifacts = load_report_preview_artifacts(report_dir)

    assert artifacts["has_preview"] is True
    assert artifacts["preview_source"] == "markdown_fallback"
    assert artifacts["has_markdown"] is True
    assert artifacts["has_html"] is False
    preview_html = str(artifacts["preview_html"])
    assert '<h1 id="标题">标题</h1>' in preview_html
    assert 'href="#标题"' in preview_html
    assert 'href="https://example.com"' in preview_html
    assert "data:image/png;base64," in preview_html
    assert "data:image/png;base64," not in str(artifacts["pdf_html"])


def test_load_report_preview_artifacts_handles_missing_files(tmp_path):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)

    artifacts = load_report_preview_artifacts(Path(report_dir))

    assert artifacts["has_preview"] is False
    assert artifacts["preview_source"] == ""
    assert artifacts["has_markdown"] is False
    assert artifacts["has_html"] is False
    assert artifacts["markdown_text"] == ""
    assert artifacts["html_text"] == ""
    assert artifacts["preview_html"] == ""
    assert artifacts["pdf_html"] == ""


def test_format_error_message_prefers_message_text():
    assert format_error_message(RuntimeError("boom")) == "boom"


def test_format_error_message_handles_empty_message():
    class _EmptyError(Exception):
        def __str__(self) -> str:
            return ""

    msg = format_error_message(_EmptyError())
    assert "_EmptyError" in msg


def test_build_pdf_error_payload_reads_structured_exception():
    class _StructuredError(Exception):
        stage = "goto"
        error_type = "NotImplementedError"
        traceback_text = "traceback text"
        diagnostics = {"python_executable": "python.exe"}

        def __str__(self) -> str:
            return "goto failed"

    payload = build_pdf_error_payload(_StructuredError())

    assert payload["stage"] == "goto"
    assert payload["error_type"] == "NotImplementedError"
    assert payload["error_message"] == "goto failed"
    assert payload["traceback"] == "traceback text"
    assert payload["diagnostics"]["python_executable"] == "python.exe"


def test_write_pdf_error_log_creates_log_file(tmp_path):
    payload = {
        "stage": "goto",
        "error_type": "NotImplementedError",
        "error_message": "goto failed",
        "traceback": "traceback text",
        "diagnostics": {"platform": "Windows"},
    }

    log_path = write_pdf_error_log(payload, report_dir=tmp_path / "report")

    content = Path(log_path).read_text(encoding="utf-8")
    assert "NotImplementedError" in content
    assert "goto failed" in content
    assert "traceback text" in content
