# Full Acceptance Report

- Date: 2026-02-21 15:37:02 +08:00
- Run ID: full_fast_20260221_fix2
- Acceptance Profile: fast
- Final Outcome: PASSED
- Final Gate: all blocking steps must pass; whitelist key check is informational.
- Whitelist exemption: tests/unit/core/test_no_real_secrets.py

## Step Results
- non_live_regression | PASSED | exit=0
- live_cli_and_dashboard_api_e2e | PASSED | exit=0
- live_dashboard_ui_e2e | PASSED | exit=0
- whitelist_security_key_check | PASSED | exit=0
- cleanup_residual_processes | CLEAN | exit=0

## Commands
- 'uv run pytest tests dashboard/tests -v -p no:cacheprovider -m not live_api and not ui_e2e -k not test_reserved_config_does_not_store_live_api_keys --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_fix2\\non_live'
- 'uv run pytest tests/e2e/cli -v -p no:cacheprovider -m live_api --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_fix2\\live_cli_api'
- 'uv run pytest tests/e2e/dashboard_ui -v -p no:cacheprovider -m ui_e2e and live_api --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_fix2\\live_ui'
- 'uv run pytest tests/unit/core/test_no_real_secrets.py -v -p no:cacheprovider --basetemp=C:\Users\hyace\AppData\Local\Temp\analysisposts_pytest\acceptance_full_fast_20260221_fix2\\whitelist'
- 'Stop-RunProcessesFromCurrentRun -CurrentRunId full_fast_20260221_fix2'
