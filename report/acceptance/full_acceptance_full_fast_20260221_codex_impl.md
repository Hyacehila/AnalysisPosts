# Full Acceptance Report

- Date: 2026-02-21 02:03:58 +08:00
- Run ID: full_fast_20260221_codex_impl
- Acceptance Profile: fast
- Final Outcome: FAILED
- Final Gate: all steps pass except approved whitelist exemption.
- Whitelist exemption: tests/unit/core/test_no_real_secrets.py

## Step Results
- non_live_regression | PASSED | exit=0
- live_cli_and_dashboard_api_e2e | FAILED | exit=1
- cleanup_residual_processes | CLEAN | exit=0

## Commands
- 'uv run pytest tests dashboard/tests -v -p no:cacheprovider -m not live_api and not ui_e2e -k not test_reserved_config_does_not_store_live_api_keys --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_codex_impl\\non_live'
- 'uv run pytest tests/e2e/cli -v -p no:cacheprovider -m live_api --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_codex_impl\\live_cli_api'
- 'Stop-RunProcessesFromCurrentRun -CurrentRunId full_fast_20260221_codex_impl'
