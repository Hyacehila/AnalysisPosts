"""
test_call_llm.py — LLM call utilities

Focus: call_glm4v_plus multimodal message construction.
"""
import base64
from types import SimpleNamespace

import pytest

from utils import call_llm


class _DummyCompletions:
    def __init__(self):
        self.last_params = None

    def create(self, **params):
        self.last_params = params
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])


class _DummyChat:
    def __init__(self, completions):
        self.completions = completions


class _DummyClient:
    def __init__(self):
        self.completions = _DummyCompletions()
        self.chat = _DummyChat(self.completions)


def _patch_client(monkeypatch):
    dummy = _DummyClient()
    monkeypatch.setattr(call_llm, "get_client", lambda: dummy)
    return dummy


def test_call_glm4v_plus_with_image_paths_includes_base64(tmp_path, monkeypatch):
    dummy = _patch_client(monkeypatch)

    img_path = tmp_path / "x.png"
    img_bytes = b"img-bytes"
    img_path.write_bytes(img_bytes)

    response = call_llm.call_glm4v_plus("prompt", image_paths=[str(img_path)])
    assert response == "ok"

    params = dummy.completions.last_params
    assert params["model"] == "glm-4v-plus"
    messages = params["messages"]
    content = messages[0]["content"]

    # Expect text + image_url
    assert content[0]["type"] == "text"
    assert "image_url" in content[1]
    data_url = content[1]["image_url"]["url"]
    assert data_url.startswith("data:image/png;base64,")
    assert base64.b64encode(img_bytes).decode("utf-8") in data_url


def test_call_glm4v_plus_with_image_data_includes_base64(monkeypatch):
    dummy = _patch_client(monkeypatch)
    img_bytes = b"raw-bytes"

    response = call_llm.call_glm4v_plus("prompt", image_data=[img_bytes])
    assert response == "ok"

    params = dummy.completions.last_params
    messages = params["messages"]
    content = messages[0]["content"]
    data_url = content[1]["image_url"]["url"]
    assert data_url.startswith("data:image/jpeg;base64,")
    assert base64.b64encode(img_bytes).decode("utf-8") in data_url


def test_call_glm4v_plus_text_only(monkeypatch):
    dummy = _patch_client(monkeypatch)
    response = call_llm.call_glm4v_plus("prompt only")
    assert response == "ok"

    params = dummy.completions.last_params
    messages = params["messages"]
    content = messages[0]["content"]
    assert content == [{"type": "text", "text": "prompt only"}]


def test_call_glm46_disables_reasoning_when_flag_false(monkeypatch):
    dummy = _patch_client(monkeypatch)

    response = call_llm.call_glm46("prompt", enable_reasoning=False)
    assert response == "ok"

    params = dummy.completions.last_params
    assert params["model"] == "glm-4.6"
    assert "thinking" not in params


def test_call_glm46_passes_timeout(monkeypatch):
    dummy = _patch_client(monkeypatch)

    response = call_llm.call_glm46("prompt", timeout=120)
    assert response == "ok"

    params = dummy.completions.last_params
    assert params["timeout"] == 120


def test_call_glm45v_thinking_disables_thinking_when_flag_false(monkeypatch):
    dummy = _patch_client(monkeypatch)

    response = call_llm.call_glm45v_thinking("prompt", enable_thinking=False)
    assert response == "ok"

    params = dummy.completions.last_params
    assert params["model"] == "glm-4.5v"
    assert "thinking" not in params


def test_call_glm45v_thinking_passes_timeout(monkeypatch):
    dummy = _patch_client(monkeypatch)

    response = call_llm.call_glm45v_thinking("prompt", timeout=120)
    assert response == "ok"

    params = dummy.completions.last_params
    assert params["timeout"] == 120
