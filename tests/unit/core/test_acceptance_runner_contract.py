"""
Contract checks for full-acceptance runner and pytest marker registration.
"""
from __future__ import annotations

from pathlib import Path
import tomllib


def test_run_full_acceptance_script_exists() -> None:
    script_path = Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1"
    assert script_path.exists(), "scripts/run_full_acceptance.ps1 must exist"


def test_ui_e2e_marker_registered_in_pyproject() -> None:
    pyproject = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
    assert "ui_e2e:" in pyproject


def test_acceptance_runner_supports_profile_switch() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert "[ValidateSet(\"fast\", \"quality\")]" in script
    assert "$Profile" in script


def test_acceptance_runner_passes_pytest_arguments() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert "[string[]]$PytestArgs" in script
    assert "& uv run pytest @PytestArgs" in script


def test_acceptance_runner_non_live_step_excludes_whitelist_secret_check() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert '-k", "not test_reserved_config_does_not_store_live_api_keys"' in script


def test_acceptance_runner_marks_blocking_failures_in_script_scope() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert "$script:blockingFailed = $true" in script


def test_pytest_default_addopts_skip_live_api_and_ui_e2e() -> None:
    pyproject_text = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
    addopts = tomllib.loads(pyproject_text)["tool"]["pytest"]["ini_options"]["addopts"]
    assert '-m "not live_api and not ui_e2e"' in addopts


def test_acceptance_runner_uses_try_finally_for_cleanup() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert "try {" in script
    assert "finally {" in script
    assert "Get-CimInstance Win32_Process" in script


def test_acceptance_runner_reports_aborted_and_failed_labels() -> None:
    script = (Path(__file__).resolve().parents[3] / "scripts" / "run_full_acceptance.ps1").read_text(
        encoding="utf-8"
    )
    assert "ABORTED" in script
    assert "FAILED" in script
