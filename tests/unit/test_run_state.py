"""
Tests for shared run state lock.
"""
from utils.run_state import is_running, lock_path, set_running


def test_run_state_lock(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    set_running(False)
    assert not is_running()
    assert not lock_path().exists()

    set_running(True)
    assert is_running()
    assert lock_path().exists()

    set_running(False)
    assert not is_running()
    assert not lock_path().exists()
