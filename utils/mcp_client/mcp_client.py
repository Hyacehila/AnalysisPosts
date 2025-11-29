"""
舆情分析智能体 - MCP客户端集成
提供MCP和本地工具的统一接口
"""

import asyncio
import os
from typing import List, Dict, Any, Optional
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import json

# 全局标志控制是否使用MCP
USE_MCP = False

def set_mcp_mode(use_mcp: bool):
    """设置MCP模式"""
    global USE_MCP
    USE_MCP = use_mcp

def is_mcp_enabled() -> bool:
    """检查是否启用MCP"""
    return USE_MCP

async def mcp_get_tools(server_script_path: str = "utils.mcp_server") -> List[Dict[str, Any]]:
    """从MCP服务器获取可用工具列表"""
    try:
        # 使用绝对路径来确保能找到服务器脚本
        import os
        if not os.path.isabs(server_script_path):
            # 如果是相对路径，转换为相对于项目根目录的绝对路径
            server_script_path = os.path.abspath(server_script_path)
            if not server_script_path.endswith('.py'):
                server_script_path += '.py'

        server_params = StdioServerParameters(
            command="python",
            args=[server_script_path]
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_response = await session.list_tools()

                # 转换MCP工具格式为统一格式
                tools = []
                for tool in tools_response.tools:
                    tool_info = {
                        "name": tool.name,
                        "description": tool.description,
                        "category": _get_tool_category(tool.name),
                        "input_schema": tool.inputSchema,
                        "mcp_tool": tool  # 保留原始MCP工具对象
                    }
                    tools.append(tool_info)

                return tools

    except Exception as e:
        print(f"[MCP Client] 获取MCP工具失败: {str(e)}")
        return []

async def mcp_call_tool(server_script_path: str, tool_name: str, arguments: Dict[str, Any]) -> Any:
    """调用MCP服务器上的工具"""
    try:
        # 使用绝对路径来确保能找到服务器脚本
        import os
        if not os.path.isabs(server_script_path):
            # 如果是相对路径，转换为相对于项目根目录的绝对路径
            server_script_path = os.path.abspath(server_script_path)
            if not server_script_path.endswith('.py'):
                server_script_path += '.py'

        server_params = StdioServerParameters(
            command="python",
            args=[server_script_path]
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)

                # 解析MCP返回结果
                if hasattr(result, 'content') and result.content:
                    # 处理文本内容
                    if hasattr(result.content[0], 'text'):
                        try:
                            return json.loads(result.content[0].text)
                        except json.JSONDecodeError:
                            return {"result": result.content[0].text}
                    # 处理其他内容类型
                    elif hasattr(result.content[0], 'data'):
                        return result.content[0].data

                return {"result": result}

    except Exception as e:
        print(f"[MCP Client] 调用MCP工具失败: {str(e)}")
        return {"error": str(e)}

def _get_tool_category(tool_name: str) -> str:
    """根据工具名称推断工具类别"""
    name_lower = tool_name.lower()

    if "sentiment" in name_lower:
        return "情感分析"
    elif "topic" in name_lower:
        return "主题分析"
    elif "geographic" in name_lower or "geo" in name_lower:
        return "地理分析"
    elif "publisher" in name_lower or "interaction" in name_lower or "cross" in name_lower or "influence" in name_lower or "correlation" in name_lower:
        return "多维交互分析"
    elif "comprehensive" in name_lower:
        return "综合分析"
    else:
        return "其他"

def get_tools(server_script_path: str = "utils.mcp_server") -> List[Dict[str, Any]]:
    """获取可用工具列表，根据配置选择MCP或本地"""
    if USE_MCP:
        try:
            # 检查是否已经在事件循环中
            try:
                loop = asyncio.get_running_loop()
                # 如果已经在运行事件循环，创建任务
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, mcp_get_tools(server_script_path))
                    return future.result()
            except RuntimeError:
                # 没有运行的事件循环，可以直接使用asyncio.run
                return asyncio.run(mcp_get_tools(server_script_path))
        except Exception as e:
            print(f"[MCP Client] MCP工具获取失败: {str(e)}")
            return []
    else:
        print(f"[MCP Client] MCP模式未启用")
        return []

def call_tool(server_script_path: str, tool_name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """调用工具，根据配置选择MCP或本地"""
    if arguments is None:
        arguments = {}

    if USE_MCP:
        try:
            # 检查是否已经在事件循环中
            try:
                loop = asyncio.get_running_loop()
                # 如果已经在运行事件循环，创建任务
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, mcp_call_tool(server_script_path, tool_name, arguments))
                    return future.result()
            except RuntimeError:
                # 没有运行的事件循环，可以直接使用asyncio.run
                return asyncio.run(mcp_call_tool(server_script_path, tool_name, arguments))
        except Exception as e:
            print(f"[MCP Client] MCP工具调用失败: {str(e)}")
            return {"error": f"MCP工具调用失败: {str(e)}"}
    else:
        # MCP模式未启用
        print(f"[MCP Client] MCP模式未启用")
        return {"error": "MCP模式未启用"}

def _get_local_tools() -> List[Dict[str, Any]]:
    """获取本地工具列表"""
    try:
        from .analysis_tools import get_all_tools
        return get_all_tools()
    except ImportError as e:
        print(f"[MCP Client] 无法导入本地工具: {str(e)}")
        return []

def _call_local_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """调用本地工具"""
    try:
        from .analysis_tools import execute_tool
        result = execute_tool(tool_name, [])
        return result
    except Exception as e:
        print(f"[Local Tools] 本地工具调用失败: {str(e)}")
        return {"error": str(e)}

# 测试函数
async def test_mcp_connection():
    """测试MCP连接"""
    if not USE_MCP:
        print("[MCP Client] MCP未启用")
        return

    print("[MCP Client] 测试MCP连接...")
    tools = await mcp_get_tools()
    print(f"[MCP Client] 发现 {len(tools)} 个工具")

    # 测试一个简单的工具
    if tools:
        first_tool = tools[0]
        print(f"[MCP Client] 测试工具: {first_tool['name']}")
        try:
            result = await mcp_call_tool("mcp_server.py", first_tool['name'], {})
            print(f"[MCP Client] 工具调用成功")
        except Exception as e:
            print(f"[MCP Client] 工具调用失败: {str(e)}")

if __name__ == "__main__":
    # 测试本地工具
    print("=== 测试本地工具 ===")
    tools = get_tools()
    print(f"发现 {len(tools)} 个本地工具")

    # 显示工具信息
    for tool in tools:
        print(f"- {tool['name']} ({tool['category']}): {tool['description'][:50]}...")

    # 测试MCP
    print("\n=== 测试MCP连接 ===")
    set_mcp_mode(True)
    asyncio.run(test_mcp_connection())