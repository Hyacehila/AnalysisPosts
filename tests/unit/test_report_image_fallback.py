"""
Ensure report formatting appends chart appendix when no image references exist.
"""
import json
from pathlib import Path

from nodes.stage3.format import FormatReportNode


def test_format_appends_chart_appendix(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    images_dir.mkdir(parents=True)

    chart_path = images_dir / "chart1.png"
    chart_path.write_text("x", encoding="utf-8")

    analysis_data = {
        "charts": [{
            "id": "chart1",
            "title": "测试图表",
            "file_path": str(chart_path),
        }],
        "tables": [],
        "execution_log": {},
    }
    (report_dir / "analysis_data.json").write_text(
        json.dumps(analysis_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    node = FormatReportNode()
    output = node.exec("## 测试报告\n\n无图片内容\n")

    assert "图表附录" in output
    assert "![测试图表](./images/chart1.png)" in output
