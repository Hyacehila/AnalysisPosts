import gc
import os
import shutil
import stat
import pytest

def handle_remove_readonly(func, path, exc_info):
    """
    Error handler for ``shutil.rmtree``.
    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.
    If the error is due to a Windows file lock (WinError 32), we skip it gracefully
    to prevent pytest from crashing.
    """
    excvalue = exc_info[1]
    if isinstance(excvalue, OSError):
        # Ignore "The process cannot access the file because it is being used by another process"
        if getattr(excvalue, "winerror", None) == 32:
            return

    # Attempt to clear read-only flag
    if func in (os.rmdir, os.remove, os.unlink):
        try:
            os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            func(path)
        except OSError:
            pass


@pytest.fixture(scope="session", autouse=True)
def cleanup_windows_locks():
    """
    Session-level fixture to forcefully garbage collect before teardown,
    and patch pytest's temporary directory removal to ignore Windows locks.
    """
    yield
    # Force garbage collection to release unreferenced file handles
    gc.collect()

    # Patches rmtree behavior globally for the suite cleanup process
    original_rmtree = shutil.rmtree

    def safe_rmtree(path, ignore_errors=False, onerror=None, **kwargs):
        if onerror is None:
            onerror = handle_remove_readonly
        try:
            original_rmtree(path, ignore_errors=ignore_errors, onerror=onerror, **kwargs)
        except Exception:
            pass

    shutil.rmtree = safe_rmtree
