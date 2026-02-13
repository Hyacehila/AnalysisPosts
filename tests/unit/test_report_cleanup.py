"""
test_report_cleanup.py — report 目录清理节点测试
"""
from pathlib import Path

from nodes import ClearReportDirNode, ClearStage3OutputsNode
from utils.path_manager import PathManager


def test_clear_report_dir_node_recreates_images(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    report_dir.mkdir()
    images_dir.mkdir()
    old_file = report_dir / "old.txt"
    old_file.write_text("stale", encoding="utf-8")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)

    node = ClearReportDirNode()
    node.exec(None)

    assert report_dir.exists()
    assert images_dir.exists()
    assert not old_file.exists()


def test_clear_stage3_outputs_node_preserves_stage2_outputs(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    report_dir.mkdir()
    images_dir.mkdir()

    report_md = report_dir / "report.md"
    status_json = report_dir / "status.json"
    analysis_data = report_dir / "analysis_data.json"

    report_md.write_text("report", encoding="utf-8")
    status_json.write_text("status", encoding="utf-8")
    analysis_data.write_text("analysis", encoding="utf-8")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)

    node = ClearStage3OutputsNode()
    node.exec(None)

    assert not report_md.exists()
    assert not status_json.exists()
    assert analysis_data.exists()
    assert images_dir.exists()
