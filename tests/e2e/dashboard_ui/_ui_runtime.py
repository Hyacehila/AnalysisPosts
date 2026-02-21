"""
Helpers for dashboard UI live E2E tests.
"""
from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from urllib.error import URLError
from urllib.request import urlopen

from tests.e2e.cli._e2e_config_runtime import (
    REPO_ROOT,
    assert_full_pipeline_artifacts,
    build_runtime_env,
)


@dataclass
class DashboardApp:
    process: subprocess.Popen[str]
    base_url: str
    log_path: Path
    stdout_path: Path
    stderr_path: Path


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ready(url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as resp:  # noqa: S310 - local test URL
                if resp.status == 200:
                    return
        except URLError as exc:
            last_error = str(exc)
        except TimeoutError as exc:
            last_error = str(exc)
        time.sleep(1)
    raise RuntimeError(f"Dashboard failed to become ready within {timeout_seconds}s: {last_error}")


def _write_process_logs(app: DashboardApp) -> None:
    if app.process.poll() is None:
        try:
            app.process.terminate()
            app.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            app.process.kill()
            app.process.wait(timeout=5)

    stdout_text = app.stdout_path.read_text(encoding="utf-8", errors="replace") if app.stdout_path.exists() else ""
    stderr_text = app.stderr_path.read_text(encoding="utf-8", errors="replace") if app.stderr_path.exists() else ""

    payload = {
        "base_url": app.base_url,
        "returncode": app.process.returncode,
        "stdout_tail": (stdout_text or "")[-5000:],
        "stderr_tail": (stderr_text or "")[-5000:],
    }
    app.log_path.parent.mkdir(parents=True, exist_ok=True)
    app.log_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@contextmanager
def launch_dashboard(tmp_path: Path, config_path: Path, timeout_seconds: int = 120) -> Iterator[DashboardApp]:
    """
    Start Streamlit dashboard for UI E2E and stop it after test.
    """
    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_dir = tmp_path / ".ui_e2e_logs"
    log_path = log_dir / "dashboard_process.json"
    stdout_path = log_dir / "dashboard_stdout.log"
    stderr_path = log_dir / "dashboard_stderr.log"
    env = build_runtime_env(config_path, tmp_path)
    env["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str((REPO_ROOT / "dashboard" / "app.py").resolve()),
        "--server.address=127.0.0.1",
        f"--server.port={port}",
        "--server.headless=true",
        "--logger.level=error",
    ]
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_file = stdout_path.open("w", encoding="utf-8", errors="replace")
    stderr_file = stderr_path.open("w", encoding="utf-8", errors="replace")
    process = subprocess.Popen(
        command,
        cwd=str(tmp_path),
        env=env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    app = DashboardApp(
        process=process,
        base_url=base_url,
        log_path=log_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    try:
        _wait_http_ready(f"{base_url}/_stcore/health", timeout_seconds=timeout_seconds)
        yield app
    finally:
        if app.process.poll() is None:
            try:
                app.process.terminate()
                app.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                app.process.kill()
                app.process.wait(timeout=5)
                
        stdout_file.close()
        stderr_file.close()
        _write_process_logs(app)


def start_pipeline_from_ui(base_url: str, timeout_seconds: int) -> None:
    """
    Drive browser interaction to start pipeline from Pipeline Console page.
    """
    playwright = __import__("playwright.sync_api", fromlist=["sync_playwright"])
    sync_playwright = playwright.sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(base_url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)

            sidebar = page.locator('[data-testid="stSidebarNav"]')
            sidebar.get_by_text("Pipeline Console", exact=False).first.click(timeout=timeout_seconds * 1000)

            page.get_by_text("Pipeline Console", exact=False).first.wait_for(timeout=timeout_seconds * 1000)
            page.get_by_role("button", name="Start Analysis").click(timeout=timeout_seconds * 1000)
            page.get_by_text("Pipeline started.", exact=False).first.wait_for(timeout=timeout_seconds * 1000)
        finally:
            browser.close()


def wait_pipeline_finished(tmp_path: Path, timeout_seconds: int) -> None:
    """
    Wait until terminal completion is observed in status.json.
    """
    status_path = tmp_path / "report" / "status.json"
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if status_path.exists():
            status = json.loads(status_path.read_text(encoding="utf-8") or "{}")
            events = status.get("events", [])
            failed = [
                item for item in events
                if item.get("event") == "exit" and item.get("status") == "failed"
            ]
            if failed:
                raise AssertionError(f"Pipeline finished with failed exit event: {failed[-1]}")

            for item in events:
                if (
                    item.get("node") == "TerminalNode"
                    and item.get("event") == "exit"
                    and item.get("status") == "completed"
                ):
                    return
        time.sleep(2)

    status_text = status_path.read_text(encoding="utf-8") if status_path.exists() else ""
    raise TimeoutError(
        f"Pipeline did not finish within {timeout_seconds}s. "
        f"status_tail={status_text[-1000:]}"
    )


def assert_report_artifacts(tmp_path: Path, config_path: Path) -> None:
    assert_full_pipeline_artifacts(config_path, tmp_path)
