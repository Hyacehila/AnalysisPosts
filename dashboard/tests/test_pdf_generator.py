"""
Tests for PDF generator helper.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from dashboard.utils import pdf_generator


def _completed_process(*, returncode: int, stdout: str, stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["python", "-m", "dashboard.utils.pdf_worker"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_run_pdf_worker_uses_current_interpreter_and_worker_module(tmp_path, monkeypatch):
    calls: list[dict[str, object]] = []

    def _fake_run(cmd, check, capture_output, text, env, cwd, timeout):
        calls.append(
            {
                "cmd": cmd,
                "check": check,
                "capture_output": capture_output,
                "text": text,
                "env": env,
                "cwd": cwd,
                "timeout": timeout,
            }
        )
        payload = {"ok": True, "stage": "ready", "diagnostics": {"worker_mode": "preflight"}}
        return _completed_process(returncode=0, stdout=json.dumps(payload))

    monkeypatch.setattr(pdf_generator.subprocess, "run", _fake_run)

    result = pdf_generator._run_pdf_worker(
        mode="preflight",
        worker_args={"report_dir": str(tmp_path / "report")},
        browsers_path=tmp_path / "browsers",
    )

    assert result["ok"] is True
    assert calls
    cmd = list(calls[0]["cmd"])
    assert cmd[0] == sys.executable
    assert cmd[1:4] == ["-m", "dashboard.utils.pdf_worker", "--mode"]
    assert "preflight" in cmd
    assert "--report-dir" in cmd
    assert calls[0]["check"] is False
    assert calls[0]["capture_output"] is True
    assert calls[0]["text"] is True
    assert isinstance(calls[0]["env"], dict)
    assert calls[0]["env"]["PLAYWRIGHT_BROWSERS_PATH"] == str((tmp_path / "browsers").resolve())
    assert calls[0]["cwd"] == str(pdf_generator._project_root())


def test_run_pdf_worker_raises_structured_error_on_nonzero_exit(tmp_path, monkeypatch):
    def _fake_run(*_args, **_kwargs):
        payload = {
            "ok": False,
            "stage": "launch_check",
            "error_type": "NotImplementedError",
            "error_message": "NotImplementedError()",
            "traceback": "tb",
            "diagnostics": {"worker_flag": "x"},
        }
        return _completed_process(returncode=1, stdout=json.dumps(payload), stderr="worker stderr")

    monkeypatch.setattr(pdf_generator.subprocess, "run", _fake_run)

    try:
        pdf_generator._run_pdf_worker(
            mode="preflight",
            worker_args={"report_dir": str(tmp_path / "report")},
            browsers_path=tmp_path / "browsers",
        )
    except pdf_generator.PdfGenerationError as exc:
        assert exc.stage == "launch_check"
        assert exc.error_type == "NotImplementedError"
        assert exc.message == "NotImplementedError()"
        assert exc.traceback_text == "tb"
        assert exc.diagnostics["worker_returncode"] == 1
        assert "worker stderr" in exc.diagnostics["worker_stderr_tail"]
    else:
        raise AssertionError("Expected PdfGenerationError")


def test_run_pdf_worker_raises_worker_protocol_error_on_invalid_json(tmp_path, monkeypatch):
    def _fake_run(*_args, **_kwargs):
        return _completed_process(returncode=0, stdout="not-json", stderr="")

    monkeypatch.setattr(pdf_generator.subprocess, "run", _fake_run)

    try:
        pdf_generator._run_pdf_worker(
            mode="preflight",
            worker_args={"report_dir": str(tmp_path / "report")},
            browsers_path=tmp_path / "browsers",
        )
    except pdf_generator.PdfGenerationError as exc:
        assert exc.stage == "worker"
        assert exc.error_type == "WorkerProtocolError"
        assert "Invalid worker JSON output" in exc.message
    else:
        raise AssertionError("Expected PdfGenerationError")


def test_diagnose_pdf_runtime_reports_ready(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "report.html").write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(
        pdf_generator,
        "_run_pdf_worker",
        lambda **_kwargs: {
            "ok": True,
            "stage": "ready",
            "diagnostics": {"browser_install_attempted": False},
        },
    )

    diagnostics = pdf_generator.diagnose_pdf_runtime(report_dir)

    assert diagnostics["ok"] is True
    assert diagnostics["stage"] == "ready"
    assert diagnostics["playwright_import_ok"] is True
    assert diagnostics["target_file_url"].startswith("file:///")


def test_diagnose_pdf_runtime_reports_structured_failure(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)

    def _fail(**_kwargs):
        raise pdf_generator.PdfGenerationError(
            stage="launch_check",
            error_type="NotImplementedError",
            message="NotImplementedError()",
            traceback_text="tb",
            diagnostics={"worker_returncode": 1},
        )

    monkeypatch.setattr(pdf_generator, "_run_pdf_worker", _fail)

    diagnostics = pdf_generator.diagnose_pdf_runtime(report_dir)

    assert diagnostics["ok"] is False
    assert diagnostics["stage"] == "launch_check"
    assert diagnostics["error_type"] == "NotImplementedError"
    assert diagnostics["error_message"] == "NotImplementedError()"
    assert diagnostics["worker_returncode"] == 1


def test_generate_pdf_from_html_reads_worker_generated_pdf(tmp_path, monkeypatch):
    html_path = tmp_path / "report.html"
    html_path.write_text("<html><body>report</body></html>", encoding="utf-8")

    observed: dict[str, str] = {}

    def _fake_run_pdf_worker(*, mode, worker_args, browsers_path):
        observed["mode"] = mode
        observed["html_path"] = str(worker_args["html_path"])
        observed["pdf_path"] = str(worker_args["pdf_path"])
        observed["browsers_path"] = str(browsers_path)
        pdf_path = Path(worker_args["pdf_path"])
        pdf_path.write_bytes(b"pdf")
        return {"ok": True, "stage": "pdf", "pdf_path": str(pdf_path)}

    monkeypatch.setattr(pdf_generator, "_run_pdf_worker", _fake_run_pdf_worker)

    result = pdf_generator.generate_pdf_from_html(html_path)

    assert result == b"pdf"
    assert observed["mode"] == "render"
    assert observed["html_path"] == str(html_path.resolve())
    assert "\\" not in pdf_generator._path_to_file_url(html_path)
    assert not Path(observed["pdf_path"]).exists()


def test_generate_pdf_from_html_raises_when_worker_does_not_output_pdf(tmp_path, monkeypatch):
    html_path = tmp_path / "report.html"
    html_path.write_text("<html><body>report</body></html>", encoding="utf-8")

    monkeypatch.setattr(
        pdf_generator,
        "_run_pdf_worker",
        lambda **_kwargs: {"ok": True, "stage": "pdf", "pdf_path": str(tmp_path / "missing.pdf")},
    )

    try:
        pdf_generator.generate_pdf_from_html(html_path)
    except pdf_generator.PdfGenerationError as exc:
        assert exc.stage == "pdf"
        assert exc.error_type == "FileNotFoundError"
    else:
        raise AssertionError("Expected PdfGenerationError")


def test_generate_pdf_from_html_content_writes_temp_file_and_cleans_up(tmp_path, monkeypatch):
    report_dir = tmp_path / "report"
    report_dir.mkdir(parents=True, exist_ok=True)

    observed: dict[str, Path | str] = {}

    def _fake_generate_pdf_from_html(path: Path | str) -> bytes:
        html_path = Path(path)
        observed["path"] = html_path
        observed["text"] = html_path.read_text(encoding="utf-8")
        return b"pdf-bytes"

    monkeypatch.setattr(pdf_generator, "generate_pdf_from_html", _fake_generate_pdf_from_html)

    result = pdf_generator.generate_pdf_from_html_content(
        "<html><body><h1>Report</h1></body></html>",
        report_dir=report_dir,
    )

    assert result == b"pdf-bytes"
    assert observed["path"].parent == report_dir
    assert "Report" in str(observed["text"])
    assert not observed["path"].exists()
