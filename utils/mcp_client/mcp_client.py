"""
舆情分析智能体 - MCP客户端集成
提供MCP和本地工具的统一接口
"""

import asyncio
import ast
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client, get_default_environment
import json

# 全局标志控制是否使用MCP
USE_MCP = False

_ALIAS_TO_CANONICAL = {
    "sentiment_distribution": "sentiment_distribution_stats",
    "sentiment_timeseries": "sentiment_time_series",
    "sentiment_anomaly": "sentiment_anomaly_detection",
    "topic_frequency": "topic_frequency_stats",
    "topic_evolution": "topic_time_evolution",
    "topic_cooccurrence": "topic_cooccurrence_analysis",
    "topic_focus_evolution": "topic_focus_evolution_chart",
    "topic_keyword_trend": "topic_keyword_trend_chart",
    "topic_focus_distribution": "topic_focus_distribution_chart",
    "geographic_distribution": "geographic_distribution_stats",
    "geographic_hotspot": "geographic_hotspot_detection",
    "geographic_sentiment": "geographic_sentiment_analysis",
    "geographic_sentiment_bar": "geographic_sentiment_bar_chart",
    "geographic_topic_heatmap_tool": "geographic_topic_heatmap",
    "geographic_temporal_heatmap_tool": "geographic_temporal_heatmap",
    "publisher_distribution": "publisher_distribution_stats",
    "cross_matrix": "cross_dimension_matrix",
    "publisher_sentiment_bucket": "publisher_sentiment_bucket_chart",
    "publisher_topic_distribution": "publisher_topic_distribution_chart",
    "participant_trend": "participant_trend_chart",
    "publisher_focus_distribution": "publisher_focus_distribution_chart",
    "generate_sentiment_bucket_trend_chart": "sentiment_bucket_trend_chart",
    "generate_sentiment_attribute_trend_chart": "sentiment_attribute_trend_chart",
    "generate_sentiment_focus_window_chart": "sentiment_focus_window_chart",
    "generate_sentiment_focus_publisher_chart": "sentiment_focus_publisher_chart",
    "generate_sentiment_trend_chart": "sentiment_trend_chart",
    "generate_sentiment_pie_chart": "sentiment_pie_chart",
    "generate_topic_ranking_chart": "topic_ranking_chart",
    "generate_topic_evolution_chart": "topic_evolution_chart",
    "generate_topic_network_chart": "topic_network_chart",
    "generate_geographic_heatmap": "geographic_heatmap",
    "generate_geographic_bar_chart": "geographic_bar_chart",
    "generate_interaction_heatmap": "interaction_heatmap",
    "generate_publisher_bar_chart": "publisher_bar_chart",
}


def _load_tool_registry() -> Dict[str, Dict[str, Any]]:
    try:
        from utils.analysis_tools.tool_registry import TOOL_REGISTRY
        return TOOL_REGISTRY
    except Exception:
        return {}


def _resolve_canonical_name(tool_name: str, registry: Dict[str, Dict[str, Any]]) -> str:
    if tool_name in registry:
        return tool_name
    return _ALIAS_TO_CANONICAL.get(tool_name, tool_name)


def _infer_generates_chart(tool_name: str) -> bool:
    lower = tool_name.lower()
    return any(token in lower for token in [
        "chart", "heatmap", "network", "wordcloud", "trend", "bar", "pie"
    ])

def _build_mcp_env() -> dict:
    """Extend MCP's default safe env with data path needed by mcp_server."""
    env = get_default_environment()
    enhanced_path = os.environ.get("ENHANCED_DATA_PATH")
    if enhanced_path:
        env["ENHANCED_DATA_PATH"] = enhanced_path
        print(f"[MCP Client] _build_mcp_env: 设置 ENHANCED_DATA_PATH={enhanced_path}")
    else:
        print(f"[MCP Client] _build_mcp_env: 警告 - ENHANCED_DATA_PATH 环境变量未设置")
    project_root = Path(__file__).resolve().parents[2]
    env.setdefault("PROJECT_ROOT", str(project_root))
    env.setdefault("REPORT_DIR", str(project_root / "report"))
    return env

def set_mcp_mode(use_mcp: bool):
    """设置MCP模式"""
    global USE_MCP
    USE_MCP = use_mcp

def ensure_mcp_enabled() -> bool:
    """确保MCP模式开启（工具来源为MCP时自动启用）"""
    global USE_MCP
    if not USE_MCP:
        set_mcp_mode(True)
    return USE_MCP

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
            args=[server_script_path],
            env=_build_mcp_env()
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_response = await session.list_tools()

                # 转换MCP工具格式为统一格式
                tools = []
                registry = _load_tool_registry()
                for tool in tools_response.tools:
                    canonical = _resolve_canonical_name(tool.name, registry)
                    tool_info_registry = registry.get(canonical, {})
                    tool_info = {
                        "name": tool.name,
                        "description": tool.description,
                        "category": tool_info_registry.get("category") or _get_tool_category(tool.name),
                        "input_schema": tool.inputSchema,
                        "canonical_name": canonical,
                        "generates_chart": tool_info_registry.get(
                            "generates_chart", _infer_generates_chart(canonical)
                        ),
                        "mcp_tool": tool  # 保留原始MCP工具对象
                    }
                    tools.append(tool_info)

                return tools

    except Exception as e:
        print(f"[MCP Client] 获取MCP工具失败: {str(e)}")
        return []

def _parse_text_payload(text: Any) -> Dict[str, Any]:
    """解析MCP返回的文本内容为结构化数据"""
    if not isinstance(text, str):
        return {"result": text}
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        return ast.literal_eval(text)
    except Exception:
        return {"error": "MCP返回文本无法解析为结构化数据", "raw_text": text}

def _parse_mcp_result(result: Any) -> Any:
    """统一解析MCP返回结果（优先 data，其次 text）"""
    if result is None:
        return {"error": "MCP返回空结果"}

    content = getattr(result, "content", None)
    if content:
        # 优先解析 data
        for item in content:
            data = getattr(item, "data", None)
            if data is not None:
                if isinstance(data, (dict, list)):
                    return data
                return {"result": data}
        # 其次解析 text
        for item in content:
            text = getattr(item, "text", None)
            if text is not None:
                return _parse_text_payload(text)

    return {"result": result}

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
            args=[server_script_path],
            env=_build_mcp_env()
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)

                return _parse_mcp_result(result)

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
    elif "keyword" in name_lower or "entity" in name_lower or "lexicon" in name_lower or "cluster" in name_lower:
        return "NLP增强分析"
    elif "comprehensive" in name_lower:
        return "综合分析"
    else:
        return "其他"

def list_tools(server_script_path: str = "utils.mcp_server") -> List[Dict[str, Any]]:
    """兼容接口：强制启用MCP并获取工具列表"""
    ensure_mcp_enabled()
    return get_tools(server_script_path)

def get_tools(server_script_path: str = "utils.mcp_server") -> List[Dict[str, Any]]:
    """获取可用工具列表，根据配置选择MCP或本地"""
    ensure_mcp_enabled()
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
    print(f"[MCP Client] MCP模式未启用")
    return []

def call_tool(server_script_path: str, tool_name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """调用工具，根据配置选择MCP或本地"""
    if arguments is None:
        arguments = {}

    ensure_mcp_enabled()
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
    # MCP模式未启用
    print(f"[MCP Client] MCP模式未启用")
    return {"error": "MCP模式未启用"}

def _get_local_tools() -> List[Dict[str, Any]]:
    """获取本地工具列表"""
    try:
        from utils.analysis_tools import get_all_tools
        return get_all_tools()
    except ImportError as e:
        print(f"[MCP Client] 无法导入本地工具: {str(e)}")
        return []

def _call_local_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """调用本地工具"""
    try:
        from utils.analysis_tools import execute_tool
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
