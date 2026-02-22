"""
MCP client result parsing.
"""
import asyncio
import sys
from types import SimpleNamespace

from utils.mcp_client import mcp_client as mc


class _DummyItem:
    def __init__(self, *, text=None, data=None):
        self.text = text
        self.data = data


class _DummyResult:
    def __init__(self, content):
        self.content = content


def test_parse_prefers_data():
    result = _DummyResult([_DummyItem(data={"charts": [{"id": "c1"}]})])
    assert mc._parse_mcp_result(result) == {"charts": [{"id": "c1"}]}


def test_parse_text_json():
    result = _DummyResult([_DummyItem(text='{"ok": 1}')])
    assert mc._parse_mcp_result(result) == {"ok": 1}


def test_parse_text_invalid_returns_error():
    result = _DummyResult([_DummyItem(text="not-json")])
    parsed = mc._parse_mcp_result(result)
    assert parsed.get("error")
    assert parsed.get("raw_text") == "not-json"


class _DummyStdioContext:
    async def __aenter__(self):
        return object(), object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_mcp_get_tools_uses_current_python_executable(monkeypatch):
    captured = {}

    class _DummyParams:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class _DummySession:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def initialize(self):
            return None

        async def list_tools(self):
            return SimpleNamespace(tools=[])

    monkeypatch.setattr(mc, "StdioServerParameters", _DummyParams)
    monkeypatch.setattr(mc, "stdio_client", lambda _params: _DummyStdioContext())
    monkeypatch.setattr(mc, "ClientSession", _DummySession)
    monkeypatch.setattr(mc, "_build_mcp_env", lambda: {})

    asyncio.run(mc.mcp_get_tools("utils/mcp_server"))
    assert captured["command"] == sys.executable


def test_mcp_call_tool_uses_current_python_executable(monkeypatch):
    captured = {}

    class _DummyParams:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class _DummySession:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def initialize(self):
            return None

        async def call_tool(self, *_args, **_kwargs):
            return _DummyResult([_DummyItem(data={"ok": True})])

    monkeypatch.setattr(mc, "StdioServerParameters", _DummyParams)
    monkeypatch.setattr(mc, "stdio_client", lambda _params: _DummyStdioContext())
    monkeypatch.setattr(mc, "ClientSession", _DummySession)
    monkeypatch.setattr(mc, "_build_mcp_env", lambda: {})

    result = asyncio.run(mc.mcp_call_tool("utils/mcp_server", "sentiment_distribution_stats", {}))
    assert result == {"ok": True}
    assert captured["command"] == sys.executable
