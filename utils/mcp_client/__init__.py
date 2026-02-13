"""
MCP (Model Context Protocol) 工具模块

提供MCP客户端和服务器功能，用于动态发现和调用分析工具。
"""

from .mcp_client import get_tools, call_tool, list_tools, set_mcp_mode, is_mcp_enabled

# Try to import mcp_server, but it may fail due to dependencies
__all__ = [
    'get_tools',
    'call_tool',
    'list_tools',
    'set_mcp_mode',
    'is_mcp_enabled'
]

try:
    from ..mcp_server import mcp
    __all__.append('mcp')
except ImportError as exc:
    print(f"[MCP] Warning: mcp_server not available, some features may be limited. {exc}")
    mcp = None
