"""
MCP client result parsing.
"""
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
