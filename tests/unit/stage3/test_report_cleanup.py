"""
test_report_cleanup.py — report 目录清理节点测试
"""
from pathlib import Path

from nodes import ClearReportDirNode, ClearStage3OutputsNode
from utils.path_manager import PathManager
from utils.status_events import read_status_events, start_status_run


def test_clear_report_dir_node_recreates_images(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    report_dir.mkdir()
    images_dir.mkdir()
    old_file = report_dir / "old.txt"
    old_file.write_text("stale", encoding="utf-8")
    old_image = images_dir / "stale.png"
    old_image.write_bytes(b"image")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)

    node = ClearReportDirNode()
    node.exec(None)

    assert report_dir.exists()
    assert images_dir.exists()
    assert not old_file.exists()
    assert not old_image.exists()


def test_clear_report_dir_node_preserves_status_and_acceptance(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    acceptance_dir = report_dir / "acceptance"
    report_dir.mkdir()
    images_dir.mkdir()
    acceptance_dir.mkdir()
    status_path = report_dir / "status.json"
    status_path.write_text('{"version":2,"events":[]}', encoding="utf-8")
    acceptance_log = acceptance_dir / "full_acceptance_demo.md"
    acceptance_log.write_text("# acceptance", encoding="utf-8")
    stage2_json = report_dir / "analysis_data.json"
    stage2_json.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)

    node = ClearReportDirNode()
    node.exec(None)

    assert report_dir.exists()
    assert images_dir.exists()
    assert status_path.exists()
    assert acceptance_log.exists()
    assert not stage2_json.exists()


def test_clear_stage3_outputs_node_preserves_stage2_outputs(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    images_dir = report_dir / "images"
    report_dir.mkdir()
    images_dir.mkdir()

    report_md = report_dir / "report.md"
    report_html = report_dir / "report.html"
    status_json = report_dir / "status.json"
    analysis_data = report_dir / "analysis_data.json"

    report_md.write_text("report", encoding="utf-8")
    report_html.write_text("<html></html>", encoding="utf-8")
    status_json.write_text("status", encoding="utf-8")
    analysis_data.write_text("analysis", encoding="utf-8")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)

    node = ClearStage3OutputsNode()
    node.exec(None)

    assert not report_md.exists()
    assert not report_html.exists()
    assert status_json.exists()
    assert analysis_data.exists()
    assert images_dir.exists()


def test_clear_stage3_outputs_node_records_enter_exit_events(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    report_dir.mkdir()
    (report_dir / "report.md").write_text("report", encoding="utf-8")
    status_path = report_dir / "status.json"
    start_status_run(path=status_path, run_id="run-stage3-cleanup")

    monkeypatch.setattr(PathManager, "report_dir", lambda self: report_dir)
    monkeypatch.chdir(tmp_path)

    shared = {"status_file": str(status_path)}

    node = ClearStage3OutputsNode()
    node._run(shared)

    status = read_status_events(path=status_path)
    node_events = [item for item in status["events"] if item.get("node") == "ClearStage3OutputsNode"]
    assert len(node_events) == 2
    assert node_events[0]["event"] == "enter"
    assert node_events[1]["event"] == "exit"
    assert node_events[1]["status"] == "completed"
