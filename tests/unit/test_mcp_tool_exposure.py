"""
Verify MCP server exposes all tool_registry names.
"""
import re
from pathlib import Path


def _extract_registry_names() -> set[str]:
    text = Path("utils/analysis_tools/tool_registry.py").read_text(encoding="utf-8")
    return set(re.findall(r'^ {4}"([a-zA-Z0-9_]+)"\\s*:\\s*\\{', text, flags=re.MULTILINE))


def _extract_mcp_tool_names() -> set[str]:
    text = Path("utils/mcp_server.py").read_text(encoding="utf-8")
    return set(re.findall(r"^@mcp\\.tool\\(\\)\\n(?:def\\s+)?([a-zA-Z0-9_]+)\\s*\\(",
                          text, flags=re.MULTILINE))


def test_mcp_exposes_registry_tools():
    registry_names = _extract_registry_names()
    mcp_names = _extract_mcp_tool_names()

    missing = sorted(name for name in registry_names if name not in mcp_names)
    assert not missing, f"MCP missing tools: {missing}"
