"""
舆情分析智能体 - 主入口文件

本文件是整个系统的入口点，遵循PocketFlow设计原则。

================================================================================
使用说明
================================================================================

1. 完整流程运行（三阶段）：
   python main.py
   
2. 仅运行阶段1（数据增强）：
   python main.py --stage1-only
   
3. 指定处理模式：
   python main.py --enhancement-mode async      # 异步并行处理
   python main.py --enhancement-mode batch_api  # Batch API处理

4. 配置并发参数：
   python main.py --concurrent 100 --retries 5 --wait 10

================================================================================
"""

import asyncio
import argparse
import concurrent.futures
import time
from typing import Dict, Any

from flow import (
    create_main_flow,
    create_stage1_only_flow,
    create_async_enhancement_flow,
    DEFAULT_CONCURRENT_NUM,
    DEFAULT_MAX_RETRIES,
    DEFAULT_WAIT_TIME,
)


def create_default_shared() -> Dict[str, Any]:
    """
    创建默认的shared字典
    
    根据设计文档，shared字典包含：
    - dispatcher: 调度控制配置
    - config: 三阶段路径控制配置
    - data: 数据管理
    - results: 结果存储
    
    Returns:
        Dict: 初始化的shared字典
    """
    return {
        # === 调度控制（DispatcherNode使用） ===
        "dispatcher": {
            "start_stage": 1,              # 起始阶段：1 | 2 | 3
            "run_stages": [1, 2, 3],       # 需要执行的阶段列表
            "current_stage": 0,            # 当前执行到的阶段（0表示未开始）
            "completed_stages": [],        # 已完成的阶段列表
            "next_action": None            # 下一步动作
        },
        
        # === 三阶段路径控制 ===
        "config": {
            # 阶段1: 增强处理方式
            "enhancement_mode": "async",   # "async" | "batch_api"
            
            # 阶段2: 分析执行方式（待实现）
            "analysis_mode": "workflow",   # "workflow" | "agent"
            "tool_source": "local",        # "local" | "mcp"
            
            # 阶段3: 报告生成方式（待实现）
            "report_mode": "template",     # "template" | "iterative"
            
            # 数据源配置
            "data_source": {
                "type": "original",
                "enhanced_data_path": "data/enhanced_blogs.json"
            },
            
            # Batch API配置
            "batch_api_config": {
                "script_path": "batch/batch_run.py",
                "input_path": "data/beijing_rainstorm_posts.json",
                "output_path": "data/enhanced_blogs.json",
                "wait_for_completion": True
            },
            
            # Agent配置（待实现）
            "agent_config": {
                "max_iterations": 10
            },
            
            # 迭代报告配置（待实现）
            "iterative_report_config": {
                "max_iterations": 5,
                "min_score_threshold": 80
            }
        },
        
        # === 数据管理 ===
        "data": {
            "blog_data": [],
            "topics_hierarchy": [],
            "sentiment_attributes": [],
            "publisher_objects": [],
            "data_paths": {
                "blog_data_path": "data/beijing_rainstorm_posts.json",
                "topics_path": "data/topics.json",
                "sentiment_attributes_path": "data/sentiment_attributes.json",
                "publisher_objects_path": "data/publisher_objects.json"
            }
        },
        
        # === 结果存储 ===
        "results": {
            "statistics": {}
        },
        
        # === Agent运行时状态（待实现） ===
        "agent": {
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": 10,
            "is_finished": False
        },
        
        # === 报告生成状态（待实现） ===
        "report": {
            "iteration": 0,
            "current_draft": "",
            "revision_feedback": "",
            "review_history": []
        }
    }


async def run_main_flow_async(
    shared: Dict[str, Any],
    concurrent_num: int = DEFAULT_CONCURRENT_NUM,
    max_retries: int = DEFAULT_MAX_RETRIES,
    wait_time: int = DEFAULT_WAIT_TIME
) -> Dict[str, Any]:
    """
    异步运行主Flow
    
    Args:
        shared: 共享数据字典
        concurrent_num: 最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间
    
    Returns:
        Dict: 执行后的shared字典
    """
    # 设置线程池（用于异步调用同步LLM函数）
    thread_pool_size = concurrent_num + 20
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    
    print(f"\n[Main] 线程池配置: max_workers={thread_pool_size}")
    
    # 创建主Flow
    main_flow = create_main_flow(
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time
    )
    
    # 运行Flow
    await main_flow.run_async(shared)
    
    return shared


async def run_stage1_only_async(
    shared: Dict[str, Any],
    mode: str = "async",
    concurrent_num: int = DEFAULT_CONCURRENT_NUM,
    max_retries: int = DEFAULT_MAX_RETRIES,
    wait_time: int = DEFAULT_WAIT_TIME
) -> Dict[str, Any]:
    """
    异步运行仅阶段1的Flow
    
    Args:
        shared: 共享数据字典
        mode: 处理模式，"async" 或 "batch_api"
        concurrent_num: 最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间
    
    Returns:
        Dict: 执行后的shared字典
    """
    # 设置处理模式
    shared["config"]["enhancement_mode"] = mode
    
    if mode == "async":
        # 设置线程池
        thread_pool_size = concurrent_num + 20
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
        loop = asyncio.get_running_loop()
        loop.set_default_executor(executor)
        
        print(f"\n[Main] 线程池配置: max_workers={thread_pool_size}")
    
    # 创建阶段1 Flow
    stage1_flow = create_stage1_only_flow(
        mode=mode,
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time
    )
    
    # 运行Flow
    await stage1_flow.run_async(shared)
    
    return shared


def print_banner():
    """打印程序启动横幅"""
    print("\n" + "=" * 60)
    print("舆情分析智能体系统".center(56))
    print("=" * 60)
    print("基于PocketFlow框架 | 三阶段解耦架构")
    print("=" * 60 + "\n")


def print_config(shared: Dict[str, Any], args: argparse.Namespace):
    """打印配置信息"""
    print("配置信息:")
    print(f"  ├─ 起始阶段: {shared['dispatcher']['start_stage']}")
    print(f"  ├─ 执行阶段: {shared['dispatcher']['run_stages']}")
    print(f"  ├─ 增强模式: {shared['config']['enhancement_mode']}")
    print(f"  ├─ 分析模式: {shared['config']['analysis_mode']}")
    print(f"  ├─ 报告模式: {shared['config']['report_mode']}")
    print(f"  ├─ 输入路径: {args.data_path}")
    print(f"  ├─ 输出路径: {args.output_path}")
    print(f"  ├─ 并发数: {args.concurrent}")
    print(f"  ├─ 重试次数: {args.retries}")
    print(f"  └─ 重试等待: {args.wait}秒")
    print()


def print_results(shared: Dict[str, Any], elapsed_time: float):
    """打印执行结果"""
    print("\n" + "=" * 60)
    print("执行结果".center(56))
    print("=" * 60)
    
    # 完成的阶段
    completed_stages = shared.get("dispatcher", {}).get("completed_stages", [])
    print(f"\n已完成阶段: {completed_stages}")
    
    # 统计信息
    stats = shared.get("results", {}).get("statistics", {})
    if stats:
        print(f"\n📊 数据统计:")
        print(f"  ├─ 总博文数: {stats.get('total_blogs', 0)}")
        print(f"  └─ 已处理数: {stats.get('processed_blogs', 0)}")
        
        # 空字段统计
        empty_fields = stats.get("empty_fields", {})
        if empty_fields:
            print(f"\n⚠️  空字段统计:")
            print(f"  ├─ 情感极性为空: {empty_fields.get('sentiment_polarity_empty', 0)}")
            print(f"  ├─ 情感属性为空: {empty_fields.get('sentiment_attribute_empty', 0)}")
            print(f"  ├─ 主题为空: {empty_fields.get('topics_empty', 0)}")
            print(f"  └─ 发布者为空: {empty_fields.get('publisher_empty', 0)}")
    
    # 保存状态
    data_save = shared.get("results", {}).get("data_save", {})
    if data_save.get("saved"):
        print(f"\n💾 数据保存:")
        print(f"  ├─ 保存路径: {data_save.get('output_path', 'N/A')}")
        print(f"  └─ 保存数量: {data_save.get('data_count', 0)} 条")
    
    # 时间统计
    print(f"\n⏱️  总耗时: {elapsed_time:.2f} 秒")
    
    processed_blogs = stats.get('processed_blogs', 0)
    if processed_blogs > 0:
        throughput = processed_blogs / elapsed_time
        print(f"📈 处理效率: {throughput:.2f} 条/秒")
    
    print("\n" + "=" * 60 + "\n")


async def main_async(args: argparse.Namespace):
    """异步主函数"""
    print_banner()
    
    # 创建shared字典
    shared = create_default_shared()
    
    # 根据命令行参数更新配置
    shared["config"]["enhancement_mode"] = args.enhancement_mode
    
    # 更新数据路径配置
    shared["data"]["data_paths"]["blog_data_path"] = args.data_path
    shared["config"]["data_source"]["enhanced_data_path"] = args.output_path
    shared["config"]["batch_api_config"]["input_path"] = args.data_path
    shared["config"]["batch_api_config"]["output_path"] = args.output_path
    
    if args.stage1_only:
        # 仅运行阶段1
        shared["dispatcher"]["run_stages"] = [1]
        print("[Main] 模式: 仅运行阶段1（数据增强）\n")
    else:
        # 完整流程（目前只有阶段1可用）
        # TODO: 阶段2和阶段3实现后，取消此限制
        shared["dispatcher"]["run_stages"] = [1]
        print("[Main] 模式: 完整流程（目前仅阶段1可用）\n")
    
    # 打印配置
    print_config(shared, args)
    
    # 记录开始时间
    start_time = time.time()
    print(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}\n")
    
    try:
        if args.stage1_only:
            await run_stage1_only_async(
                shared=shared,
                mode=args.enhancement_mode,
                concurrent_num=args.concurrent,
                max_retries=args.retries,
                wait_time=args.wait
            )
        else:
            await run_main_flow_async(
                shared=shared,
                concurrent_num=args.concurrent,
                max_retries=args.retries,
                wait_time=args.wait
            )
        
        # 记录结束时间
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 打印结果
        print_results(shared, elapsed_time)
        
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n❌ 执行出错: {str(e)}")
        print(f"⏱️  运行时间: {elapsed_time:.2f} 秒")
        
        import traceback
        traceback.print_exc()


def main():
    """主函数入口"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="舆情分析智能体系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                          # 运行完整流程
  python main.py --stage1-only            # 仅运行阶段1
  python main.py --enhancement-mode async # 使用异步模式
  python main.py --concurrent 100         # 设置并发数为100
        """
    )
    
    parser.add_argument(
        "--stage1-only",
        action="store_true",
        help="仅运行阶段1（数据增强）"
    )
    
    parser.add_argument(
        "--enhancement-mode",
        choices=["async", "batch_api"],
        default="async",
        help="阶段1处理模式: async（异步并行）或 batch_api（Batch API）"
    )
    
    parser.add_argument(
        "--concurrent",
        type=int,
        default=DEFAULT_CONCURRENT_NUM,
        help=f"最大并发数（默认: {DEFAULT_CONCURRENT_NUM}）"
    )
    
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"最大重试次数（默认: {DEFAULT_MAX_RETRIES}）"
    )
    
    parser.add_argument(
        "--wait",
        type=int,
        default=DEFAULT_WAIT_TIME,
        help=f"重试等待时间/秒（默认: {DEFAULT_WAIT_TIME}）"
    )
    
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/beijing_rainstorm_posts.json",
        help="输入数据文件路径"
    )
    
    parser.add_argument(
        "--output-path",
        type=str,
        default="data/enhanced_blogs.json",
        help="输出数据文件路径"
    )
    
    args = parser.parse_args()
    
    # 运行异步主函数
    asyncio.run(main_async(args))


if __name__ == "__main__":
    # =========================================================================
    # 快速配置区域 - 修改以下参数可快速调整运行配置
    # =========================================================================
    
    # 是否使用快速配置（True: 使用下方配置，False: 使用命令行参数）
    USE_QUICK_CONFIG = True
    
    if USE_QUICK_CONFIG:
        # ----- 常用配置参数 -----
        
        # 数据路径配置
        INPUT_DATA_PATH = "data/test_posts.json"           # 输入数据文件路径
        OUTPUT_DATA_PATH = "data/test_enhanced_blogs.json" # 输出增强数据路径
        
        # 运行模式配置
        STAGE1_ONLY = True                  # True: 仅运行阶段1, False: 运行全部阶段
        ENHANCEMENT_MODE = "async"          # "async": 异步并行, "batch_api": Batch API
        
        # 性能配置
        CONCURRENT_NUM = 60                 # 最大并发数
        MAX_RETRIES = 3                     # 最大重试次数
        WAIT_TIME = 8                       # 重试等待时间（秒）
        
        # ----- 构建参数 -----
        import sys
        sys.argv = [
            "main.py",
            "--data-path", INPUT_DATA_PATH,
            "--output-path", OUTPUT_DATA_PATH,
            "--enhancement-mode", ENHANCEMENT_MODE,
            "--concurrent", str(CONCURRENT_NUM),
            "--retries", str(MAX_RETRIES),
            "--wait", str(WAIT_TIME),
        ]
        if STAGE1_ONLY:
            sys.argv.append("--stage1-only")
    
    # 运行主函数
    main()
