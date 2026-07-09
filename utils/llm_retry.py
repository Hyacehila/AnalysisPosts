"""
LLM retry decorator with configurable backoff.
"""
from __future__ import annotations

import time
from typing import Callable


def _should_retry(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "rate" in msg or "concurrency" in msg


def llm_retry(max_retries: int = 3, retry_delay: float = 5.0, backoff: str = "exponential") -> Callable:
    """
    Decorator for retrying LLM calls on transient errors (e.g. 429).

    Args:
        max_retries: maximum attempts (including the first call)
        retry_delay: base delay in seconds
        backoff: "exponential" | "linear" | "fixed"
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: B902 - need to retry on broad exceptions
                    last_exc = exc
                    if not _should_retry(exc):
                        raise
                    if attempt >= max_retries - 1:
                        break
                    if backoff == "exponential":
                        delay = retry_delay * (2 ** attempt)
                    elif backoff == "linear":
                        delay = retry_delay * (attempt + 1)
                    else:
                        delay = retry_delay
                    time.sleep(delay)
            raise Exception(f"LLM call failed after {max_retries} retries: {last_exc}")

        return wrapper

    return decorator
