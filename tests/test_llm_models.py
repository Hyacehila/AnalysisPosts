"""
Integration test for GLM model calls.
Run only when RUN_LLM_TESTS=1 is set in the environment.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from utils.call_llm import (
    call_glm_45_air,
    call_glm4v_plus,
    call_glm45v_thinking,
    call_glm46,
)

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "posts_sample_30.json"
MAX_SAMPLE_POSTS = 30


def _load_sample_posts(max_posts: int = MAX_SAMPLE_POSTS) -> list[dict]:
    if not DATA_PATH.exists():
        pytest.skip(f"data file missing: {DATA_PATH}")
    posts = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    if not posts:
        pytest.skip("posts.json is empty")
    return posts[: min(len(posts), max_posts)]


@pytest.mark.integration
def test_llm_models_integration():
    if os.environ.get("RUN_LLM_TESTS") != "1":
        pytest.skip("Set RUN_LLM_TESTS=1 to run integration LLM tests.")
    if not os.environ.get("GLM_API_KEY"):
        pytest.skip("Set GLM_API_KEY to run integration LLM tests.")

    sample_posts = _load_sample_posts()
    assert len(sample_posts) <= MAX_SAMPLE_POSTS
    snippet_lines = []
    for post in sample_posts[: min(len(sample_posts), 10)]:
        content = str(post.get("content", "")).replace("\n", " ").strip()
        snippet_lines.append(f"- {content[:120]}")
    sample_prompt = "以下是博文样本，请用一句话概括整体舆情主题：\n" + "\n".join(snippet_lines)

    text_response = call_glm_45_air(sample_prompt)
    assert isinstance(text_response, str) and text_response

    multimodal_response = call_glm4v_plus(sample_prompt)
    assert isinstance(multimodal_response, str) and multimodal_response

    thinking_response = call_glm45v_thinking(
        sample_prompt, enable_thinking=True
    )
    assert isinstance(thinking_response, str) and thinking_response

    reasoning_response = call_glm46(sample_prompt, enable_reasoning=True)
    assert isinstance(reasoning_response, str) and reasoning_response
