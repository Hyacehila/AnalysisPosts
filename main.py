"""
舆情分析智能体 - 主入口文件

系统采用中央调度模式，只需配置shared字典并启动主Flow即可。
DispatcherNode会根据配置自动在各节点间流转。

================================================================================
使用说明
================================================================================

1. 在main()函数的配置区域修改参数
2. 运行 python main.py 启动系统
3. 系统会自动根据run_stages配置执行对应阶段

================================================================================
"""

import asyncio
import concurrent.futures
import time
from typing import Dict, Any, List

from flow import create_main_flow


def init_shared(
    # 数据路径配置
    input_data_path: str = "data/beijing_rainstorm_posts.json",
    output_data_path: str = "data/enhanced_blogs.json",
    topics_path: str = "data/topics.json",
    sentiment_attributes_path: str = "data/sentiment_attributes.json",
    publisher_objects_path: str = "data/publisher_objects.json",
    # 调度配置
    start_stage: int = 1,
    run_stages: List[int] = None,
    # 阶段1配置
    enhancement_mode: str = "async",
    # 阶段2配置
    analysis_mode: str = "workflow",
    tool_source: str = "local",
    agent_max_iterations: int = 10,
    # 阶段3配置
    report_mode: str = "template",
    report_max_iterations: int = 5,
    report_min_score: int = 80,
    # Batch API配置
    batch_script_path: str = "batch/batch_run.py",
) -> Dict[str, Any]:
    """
    初始化shared字典
    
    shared字典是节点间通信的核心数据结构，包含所有配置和运行时状态。
    DispatcherNode会根据此配置决定执行路径。
    
    Args:
        input_data_path: 输入博文数据文件路径
        output_data_path: 输出增强数据文件路径
        topics_path: 主题层次结构文件路径
        sentiment_attributes_path: 情感属性列表文件路径
        publisher_objects_path: 发布者类型列表文件路径
        start_stage: 起始阶段 (1/2/3)
        run_stages: 需要执行的阶段列表，默认[1, 2]
        enhancement_mode: 阶段1处理模式 ("async" | "batch_api")
        analysis_mode: 阶段2分析模式 ("workflow" | "agent")
        tool_source: Agent工具来源 ("local" | "mcp")
        agent_max_iterations: Agent最大迭代次数
        report_mode: 阶段3报告模式 ("template" | "iterative")
        report_max_iterations: 报告最大迭代次数
        report_min_score: 报告满意度阈值
        batch_script_path: Batch API脚本路径
    
    Returns:
        Dict: 初始化完成的shared字典
    """
    if run_stages is None:
        run_stages = [1, 2]  # 默认执行阶段1和2
    
    return {
        # === 调度控制（DispatcherNode使用） ===
        "dispatcher": {
            "start_stage": start_stage,
            "run_stages": run_stages,
            "current_stage": 0,
            "completed_stages": [],
            "next_action": None
        },
        
        # === 三阶段路径控制 ===
        "config": {
            "enhancement_mode": enhancement_mode,
            "analysis_mode": analysis_mode,
            "tool_source": tool_source,
            "report_mode": report_mode,
            "data_source": {
                "type": "original",
                "enhanced_data_path": output_data_path
            },
            "batch_api_config": {
                "script_path": batch_script_path,
                "input_path": input_data_path,
                "output_path": output_data_path,
                "wait_for_completion": True
            },
            "agent_config": {
                "max_iterations": agent_max_iterations
            },
            "iterative_report_config": {
                "max_iterations": report_max_iterations,
                "min_score_threshold": report_min_score
            }
        },
        
        # === 数据管理 ===
        "data": {
            "blog_data": [],
            "topics_hierarchy": [],
            "sentiment_attributes": [],
            "publisher_objects": [],
            "load_type": "original",
            "data_paths": {
                "blog_data_path": input_data_path,
                "topics_path": topics_path,
                "sentiment_attributes_path": sentiment_attributes_path,
                "publisher_objects_path": publisher_objects_path
            }
        },
        
        # === 结果存储 ===
        "results": {
            "statistics": {},
            "data_save": {},
            "batch_api": {}
        },
        
        # === Agent运行时状态 ===
        "agent": {
            "available_tools": [],
            "execution_history": [],
            "current_iteration": 0,
            "max_iterations": agent_max_iterations,
            "is_finished": False
        },
        
        # === 报告生成状态 ===
        "report": {
            "iteration": 0,
            "current_draft": "",
            "revision_feedback": "",
            "review_history": [],
            "template": "",
            "sections": {}
        },
        
        # === 最终输出 ===
        "final_summary": {}
    }


def print_banner():
    """打印程序启动横幅"""
    print("\n" + "=" * 60)
    print("舆情分析智能体系统".center(56))
    print("=" * 60)
    print("基于PocketFlow框架 | 中央调度模式")
    print("=" * 60 + "\n")


def print_config(shared: Dict[str, Any], concurrent_num: int, max_retries: int, wait_time: int):
    """打印配置信息"""
    print("配置信息:")
    print(f"  ├─ 执行阶段: {shared['dispatcher']['run_stages']}")
    print(f"  ├─ 增强模式: {shared['config']['enhancement_mode']}")
    print(f"  ├─ 分析模式: {shared['config']['analysis_mode']}")
    print(f"  ├─ 报告模式: {shared['config']['report_mode']}")
    print(f"  ├─ 输入路径: {shared['data']['data_paths']['blog_data_path']}")
    print(f"  ├─ 输出路径: {shared['config']['data_source']['enhanced_data_path']}")
    print(f"  ├─ 并发数: {concurrent_num}")
    print(f"  ├─ 重试次数: {max_retries}")
    print(f"  └─ 重试等待: {wait_time}秒")
    print()


def print_results(shared: Dict[str, Any], elapsed_time: float):
    """
    打印最终执行摘要
    
    注意：详细的数据统计信息已由 DataValidationAndOverviewNode 在阶段1完成时打印，
    此函数仅打印最终摘要信息（耗时、效率、保存状态）
    """
    print("\n" + "=" * 60)
    print("执行摘要".center(56))
    print("=" * 60)
    
    completed_stages = shared.get("dispatcher", {}).get("completed_stages", [])
    print(f"\n[OK] 已完成阶段: {completed_stages}")
    
    # 数据保存状态（阶段1结果）
    data_save = shared.get("stage1_results", {}).get("data_save", {})
    if data_save.get("saved"):
        print(f"\n[DATA] 数据保存:")
        print(f"  ├─ 保存路径: {data_save.get('output_path', 'N/A')}")
        print(f"  └─ 保存数量: {data_save.get('data_count', 0)} 条")
    
    # 耗时和效率
    print(f"\n[TIME] 总耗时: {elapsed_time:.2f} 秒")
    
    stats = shared.get("stage1_results", {}).get("statistics", {})
    processed_blogs = stats.get('processed_blogs', 0)
    if processed_blogs > 0 and elapsed_time > 0:
        print(f"[RATE] 处理效率: {processed_blogs / elapsed_time:.2f} 条/秒")
    
    print("\n" + "=" * 60 + "\n")


async def run(
    shared: Dict[str, Any],
    concurrent_num: int = 60,
    max_retries: int = 3,
    wait_time: int = 8
):
    """
    运行主Flow - 系统唯一入口
    
    创建主Flow并运行，DispatcherNode会根据shared配置自动流转。
    
    Args:
        shared: 初始化后的shared字典
        concurrent_num: 最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间（秒）
    """
    print_banner()
    print_config(shared, concurrent_num, max_retries, wait_time)
    
    # 设置线程池（用于异步调用同步LLM函数）
    thread_pool_size = concurrent_num + 20
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    print(f"[Main] 线程池配置: max_workers={thread_pool_size}")
    
    start_time = time.time()
    print(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}\n")
    
    try:
        # 创建并运行主Flow - DispatcherNode会根据配置自动调度
        main_flow = create_main_flow(
            concurrent_num=concurrent_num,
            max_retries=max_retries,
            wait_time=wait_time
        )
        await main_flow.run_async(shared)
        
        elapsed_time = time.time() - start_time
        print_results(shared, elapsed_time)
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"\n[X] 执行出错: {str(e)}")
        print(f"[T] 运行时间: {elapsed_time:.2f} 秒")
        import traceback
        traceback.print_exc()


def main():
    """
    主函数入口
    
    所有配置参数直接在此函数中修改。
    系统会根据run_stages配置自动执行对应阶段。
    """
    # =========================================================================
    # 配置区域 - 修改以下参数调整运行配置
    # =========================================================================
    #
    # 快速切换运行模式：
    # 1. 仅运行阶段2 (当前配置): RUN_STAGES = [2]
    # 2. 运行完整流程: RUN_STAGES = [1, 2]
    # 3. 仅运行阶段1: RUN_STAGES = [1]
    #
    # 阶段2模式切换：
    # - workflow: 预定义分析脚本 (推荐)
    # - agent: LLM自主决策分析
    #
    
    # ----- 数据路径配置 -----
    INPUT_DATA_PATH = "data/test_posts.json"
    OUTPUT_DATA_PATH = "data/test_enhanced_blogs.json"
    TOPICS_PATH = "data/topics.json"
    SENTIMENT_ATTRS_PATH = "data/sentiment_attributes.json"
    PUBLISHER_OBJS_PATH = "data/publisher_objects.json"

    # 阶段2需要读取的增强数据文件路径（确保阶段1已生成）
    ENHANCED_DATA_PATH = OUTPUT_DATA_PATH
    
    # ----- 执行阶段配置 -----
    # 设置需要执行的阶段列表
    # [1] = 仅阶段1, [2] = 仅阶段2, [1,2] = 阶段1和2, [1,2,3] = 全部阶段
    RUN_STAGES = [2]  # 仅执行阶段2
    
    # ----- 阶段1配置 -----
    ENHANCEMENT_MODE = "async"  # "async" | "batch_api"
    
    # ----- 阶段2配置 -----
    ANALYSIS_MODE = "workflow"  # "workflow" | "agent"
    TOOL_SOURCE = "local"       # "local" | "mcp"
    AGENT_MAX_ITERATIONS = 10
    
    # ----- 阶段3配置（待实现） -----
    REPORT_MODE = "template"    # "template" | "iterative"
    REPORT_MAX_ITERATIONS = 5
    REPORT_MIN_SCORE = 80
    
    # ----- Batch API配置 -----
    BATCH_SCRIPT_PATH = "batch/batch_run.py"
    
    # =========================================================================
    # 检查前置条件并初始化shared字典
    # =========================================================================

    # 如果只运行阶段2，检查增强数据文件是否存在
    if RUN_STAGES == [2]:
        import os
        if not os.path.exists(OUTPUT_DATA_PATH):
            print(f"[X] 错误: 增强数据文件不存在: {OUTPUT_DATA_PATH}")
            print(f"请先运行阶段1生成增强数据，或设置 RUN_STAGES = [1, 2] 运行完整流程")
            return

    # 根据运行阶段设置数据源类型
    data_source_type = "enhanced" if RUN_STAGES == [2] else "original"

    shared = init_shared(
        input_data_path=INPUT_DATA_PATH,
        output_data_path=OUTPUT_DATA_PATH,
        topics_path=TOPICS_PATH,
        sentiment_attributes_path=SENTIMENT_ATTRS_PATH,
        publisher_objects_path=PUBLISHER_OBJS_PATH,
        run_stages=RUN_STAGES,
        enhancement_mode=ENHANCEMENT_MODE,
        analysis_mode=ANALYSIS_MODE,
        tool_source=TOOL_SOURCE,
        agent_max_iterations=AGENT_MAX_ITERATIONS,
        report_mode=REPORT_MODE,
        report_max_iterations=REPORT_MAX_ITERATIONS,
        report_min_score=REPORT_MIN_SCORE,
        batch_script_path=BATCH_SCRIPT_PATH,
        start_stage=2 if RUN_STAGES == [2] else 1,  # 如果只运行阶段2，设置起始阶段为2
    )

    # 如果只运行阶段2，设置数据源为增强数据
    if RUN_STAGES == [2]:
        shared["config"]["data_source"]["type"] = "enhanced"
    
    # 运行系统 - DispatcherNode会根据配置自动调度仅执行阶段2的workflow模式
    # 性能参数使用run函数的默认值：concurrent_num=60, max_retries=3, wait_time=8
    asyncio.run(run(shared=shared))


if __name__ == "__main__":
    main()
