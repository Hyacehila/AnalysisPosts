# Full Acceptance Report

- Date: 2026-02-20 23:02:46 +08:00
- Run ID: full_fast_20260220_a
- Acceptance Profile: fast
- Final Gate: all steps pass except approved whitelist exemption.
- Whitelist exemption: tests/unit/core/test_no_real_secrets.py

## Step Results
- non_live_regression | PASSED | exit=0
- live_cli_and_dashboard_api_e2e | FAILED | exit=-1
- live_dashboard_ui_e2e | FAILED | exit=-1
- whitelist_security_key_check | EXPECTED_FAIL | exit=1

## Commands
- 'uv run pytest tests dashboard/tests -v -m not live_api and not ui_e2e -k not test_reserved_config_does_not_store_live_api_keys --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_full_fast_20260220_a\\non_live'
- 'uv run pytest tests/e2e/cli -v -m live_api --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_full_fast_20260220_a\\live_cli_api'
- 'uv run pytest tests/e2e/dashboard_ui -v -m ui_e2e and live_api --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_full_fast_20260220_a\\live_ui'
- 'uv run pytest tests/unit/core/test_no_real_secrets.py -v --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_full_fast_20260220_a\\whitelist'
