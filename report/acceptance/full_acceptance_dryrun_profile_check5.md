# Full Acceptance Report

- Date: 2026-02-20 21:34:40 +08:00
- Run ID: dryrun_profile_check5
- Acceptance Profile: fast
- Final Gate: all steps pass except approved whitelist exemption.
- Whitelist exemption: tests/unit/core/test_no_real_secrets.py

## Step Results
- non_live_regression | PASSED | exit=0
- whitelist_security_key_check | EXPECTED_FAIL | exit=1

## Commands
- 'uv run pytest tests dashboard/tests -v -m not live_api and not ui_e2e -k not test_reserved_config_does_not_store_live_api_keys --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_dryrun_profile_check5\\non_live'
- 'uv run pytest tests/unit/core/test_no_real_secrets.py -v --basetemp=D:\PythonProject\AnalysisPosts\.pytest_tmp_local\acceptance_dryrun_profile_check5\\whitelist'
