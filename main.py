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
    belief_system_path: str = "data/believe_system_common.json",
    publisher_decision_path: str = "data/publisher_decision.json",
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
    # 数据源配置
    data_source_type: str = "original",
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
        run_stages = [1]  # 默认执行阶段1，2，3
    
    return {
        # === 数据管理（贯穿三阶段） ===
        "data": {
            "blog_data": [],              # 博文数据（原始或增强后）
            "topics_hierarchy": [],        # 主题层次结构（从data/topics.json加载）
            "sentiment_attributes": [],    # 情感属性列表（从data/sentiment_attributes.json加载）
            "publisher_objects": [],       # 发布者类型列表（从data/publisher_objects.json加载）
            "data_paths": {
                "blog_data_path": input_data_path,
                "topics_path": topics_path,
                "sentiment_attributes_path": sentiment_attributes_path,
                "publisher_objects_path": publisher_objects_path,
                "belief_system_path": belief_system_path,
                "publisher_decision_path": publisher_decision_path
            }
        },

        # === 调度控制（DispatcherNode使用） ===
        "dispatcher": {
            "start_stage": start_stage,              # 起始阶段：1 | 2 | 3
            "run_stages": run_stages,       # 需要执行的阶段列表
            "current_stage": 0,            # 当前执行到的阶段（0表示未开始）
            "completed_stages": [],        # 已完成的阶段列表
            "next_action": "stage1"        # 下一步动作：stage1 | stage2 | stage3 | done
        },

        # === 三阶段路径控制（对应需求分析中的三阶段架构） ===
        "config": {
            # 阶段1: 增强处理方式（对应需求：四维度分析）
            "enhancement_mode": enhancement_mode,   # "async" | "batch_api"
            # 阶段1: 断点续跑/防中断丢失（默认开启）
            # - enabled: 是否开启
            # - save_every: 每完成 N 条就保存一次（设为 1 即“每条都保存”）
            # - min_interval_seconds: 最小保存间隔（秒），避免过于频繁写盘
            "stage1_checkpoint": {
                "enabled": True,
                "save_every": 100,
                "min_interval_seconds": 20
            },

            # 阶段2: 分析执行方式（对应需求：分析工具集）
            "analysis_mode": analysis_mode,   # "workflow" | "agent"
            "tool_source": tool_source,          # "mcp" (Agent模式下的唯一工具来源)

            # 阶段3: 报告生成方式（对应需求：报告输出）
            "report_mode": report_mode,     # "template" | "iterative"

            # 阶段2 Agent配置
            "agent_config": {
                "max_iterations": agent_max_iterations
            },

            # 阶段3 迭代报告配置
            "iterative_report_config": {
                "max_iterations": report_max_iterations,
                "satisfaction_threshold": 80,      # 满意度阈值（默认80分）
                "enable_review": True,            # 启用评审机制
                "quality_check": True             # 启用质量检查
            },

            # 数据源配置
            "data_source": {
                "type": data_source_type,
                "resume_if_exists": True,
                "enhanced_data_path": output_data_path
            },

            # Batch API配置
            "batch_api_config": {
                "script_path": batch_script_path,
                "input_path": input_data_path,
                "output_path": output_data_path,
                "wait_for_completion": True
            }
        },

        # === 阶段2运行时状态（Agent Loop模式） ===
        "agent": {
            "available_tools": [],         # 工具收集节点获取的可用工具列表
            "execution_history": [],       # 工具执行历史（每次循环记录）
            "current_iteration": 0,        # 当前循环迭代次数
            "max_iterations": agent_max_iterations,          # 最大迭代次数（防止无限循环）
            "is_finished": False           # Agent是否判断分析已充分
        },

        # === 阶段3报告生成状态 ===
        "report": {
            "iteration": 0,
            "current_draft": "",
            "revision_feedback": "",
            "review_history": []
        },

        # === 阶段1执行结果（由阶段1节点填充） ===
        "stage1_results": {
            # 数据统计信息（DataValidationAndOverviewNode填充）
            "statistics": {
                "total_blogs": 0,               # 总博文数
                "processed_blogs": 0,           # 已处理博文数（含增强字段）
                "empty_fields": {               # 增强字段空值统计
                    "sentiment_polarity_empty": 0,
                    "sentiment_attribute_empty": 0,
                    "topics_empty": 0,
                    "publisher_empty": 0
                },
                "engagement_statistics": {      # 参与度统计
                    "total_reposts": 0,
                    "total_comments": 0,
                    "total_likes": 0,
                    "avg_reposts": 0.0,
                    "avg_comments": 0.0,
                    "avg_likes": 0.0
                },
                "user_statistics": {            # 用户统计
                    "unique_users": 0,          # 独立用户数
                    "top_active_users": [],     # 活跃用户Top10
                    "user_type_distribution": {} # 发布者类型分布
                },
                "content_statistics": {         # 内容统计
                    "total_images": 0,
                    "blogs_with_images": 0,
                    "avg_content_length": 0.0,
                    "time_distribution": {}     # 按小时的发布时间分布
                },
                "geographic_distribution": {}   # 地理位置分布
            },
            # 数据保存状态（SaveEnhancedDataNode填充）
            "data_save": {
                "saved": False,
                "output_path": "",
                "data_count": 0
            },
            # Batch API处理状态（BatchAPIEnhancementNode填充）
            "batch_api": {
                "success": False,
                "data_count": 0,
                "error": ""
            }
        },

        # === 阶段2执行结果（由阶段2节点填充，存储到report/目录） ===
        "stage2_results": {
            # 生成的可视化图表列表
            "charts": [],
            # 生成的数据表格列表
            "tables": [],
            # LLM生成的深度洞察分析
            "insights": {
                "sentiment_insight": "",     # 情感趋势洞察
                "topic_insight": "",         # 主题演化洞察
                "geographic_insight": "",    # 地理分布洞察
                "cross_dimension_insight": "", # 多维交互洞察
                "summary_insight": ""        # 综合洞察摘要
            },
            # 分析执行记录
            "execution_log": {
                "tools_executed": [],        # 已执行的工具列表
                "total_charts": 0,           # 生成的图表总数
                "total_tables": 0,           # 生成的表格总数
                "execution_time": 0.0        # 执行耗时（秒）
            },
            # 阶段2输出文件路径（供阶段3加载）
            "output_files": {
                "charts_dir": "report/images/",          # 图表存储目录
                "analysis_data": "report/analysis_data.json",  # 分析数据文件
                "insights_file": "report/insights.json"  # 洞察描述文件
            }
        },

        # === 阶段3执行结果（由阶段3节点填充） ===
        "stage3_results": {
            "report_file": "report/report.md",  # 最终报告文件路径
            "generation_mode": "",              # 生成模式：template | iterative
            "iterations": 0,                    # 迭代次数（iterative模式）
            "final_score": 0,                   # 最终评分（iterative模式）
            "report_reasoning": "",             # 报告编排的原因和逻辑说明
            "data_citations": {},               # 数据引用映射，确保结论有数据支撑
            "hallucination_check": {}           # 幻觉检测结果
        },

        # === 系统运行监测（贯穿三阶段） ===
        "monitor": {
            "start_time": "",                   # 系统启动时间
            "current_stage": "",                # 当前执行阶段
            "current_node": "",                 # 当前执行节点
            "execution_log": [],                # 执行日志列表
            "progress_status": {},              # 进度状态信息
            "error_log": []                     # 错误日志列表
        },

        # === LLM思考过程记录（Stage2和Stage3） ===
        "thinking": {
            "stage2_tool_decisions": [],        # Stage2工具调用决策思考
            "stage3_report_planning": [],       # Stage3报告编排思考
            "stage3_section_planning": {},      # 各章节具体编排思考
            "thinking_timestamps": []           # 思考过程时间戳
        }
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
    # - workflow: 预定义分析脚本 (推荐，稳定)
    # - agent: LLM自主决策分析 (通过MCP协议动态调用工具)
    #
    # Agent工具源：
    # - mcp: 使用MCP协议动态发现和调用工具 (唯一实现方式)
    #
    
    # ----- 数据路径配置 -----
    INPUT_DATA_PATH = "data/HongKongFireSEL.json"   # 设置为香港大火事件单关键词版本数据
    OUTPUT_DATA_PATH = "data/HongKongFireSEL_enhenced.json"
    TOPICS_PATH = "data/topics.json"
    SENTIMENT_ATTRS_PATH = "data/sentiment_attributes.json"
    PUBLISHER_OBJS_PATH = "data/publisher_objects.json"
    BELIEF_SYSTEM_PATH = "data/believe_system_common.json"
    PUBLISHER_DECISION_PATH = "data/publisher_decision.json"

    # 阶段2需要读取的增强数据文件路径（确保阶段1已生成）
    ENHANCED_DATA_PATH = OUTPUT_DATA_PATH
    
    # ----- 执行阶段配置 -----
    # 设置需要执行的阶段列表
    # [1] = 仅阶段1, [2] = 仅阶段2, [3] = 仅阶段3, [1,2] = 阶段1和2, [1,2,3] = 全部阶段
    RUN_STAGES = [2,3]    # 先进行阶段1的增强工作

    # ----- 阶段1配置 -----
    ENHANCEMENT_MODE = "async"  # "async" | "batch_api"

    # ----- 阶段2配置 -----
    ANALYSIS_MODE = "agent"     # "workflow" | "agent"
    TOOL_SOURCE = "mcp"            # Agent模式下的唯一工具来源 (MCP协议)
    AGENT_MAX_ITERATIONS = 40

    # ----- 阶段3配置 -----
    REPORT_MODE = "template"    # "template" | "iterative"
    REPORT_MAX_ITERATIONS = 10
    REPORT_MIN_SCORE = 80
    
    # ----- Batch API配置 -----
    BATCH_SCRIPT_PATH = "batch/batch_run.py"
    
    # =========================================================================
    # 检查前置条件并初始化shared字典
    # =========================================================================

    import os

    # Agent(MCP)模式下：MCP server 是独立子进程，需通过环境变量告知增强数据路径
    if ANALYSIS_MODE == "agent" and TOOL_SOURCE == "mcp" and ENHANCED_DATA_PATH:
        os.environ["ENHANCED_DATA_PATH"] = os.path.abspath(ENHANCED_DATA_PATH)

    # 检查不同阶段的文件前置条件
    if RUN_STAGES == [2]:
        # 只运行阶段2：检查增强数据文件是否存在
        if not os.path.exists(OUTPUT_DATA_PATH):
            print(f"[X] 错误: 增强数据文件不存在: {OUTPUT_DATA_PATH}")
            print(f"请先运行阶段1生成增强数据，或设置 RUN_STAGES = [1, 2] 运行完整流程")
            return
    elif RUN_STAGES == [3]:
        # 只运行阶段3：检查阶段2输出文件是否存在
        required_files = [
            "report/analysis_data.json",
            "report/chart_analyses.json",
            "report/insights.json"
        ]
        missing_files = [f for f in required_files if not os.path.exists(f)]
        if missing_files:
            print(f"[X] 错误: 阶段2输出文件不存在: {missing_files}")
            print(f"请先运行阶段2生成分析结果，或设置 RUN_STAGES = [1, 2, 3] 运行完整流程")
            return
    elif RUN_STAGES == [2, 3]:
        # 运行阶段2和3：检查增强数据文件是否存在
        if not os.path.exists(OUTPUT_DATA_PATH):
            print(f"[X] 错误: 增强数据文件不存在: {OUTPUT_DATA_PATH}")
            print(f"请先运行阶段1生成增强数据，或设置 RUN_STAGES = [1, 2, 3] 运行完整流程")
            return
    elif RUN_STAGES == [1, 2, 3]:
        # 运行完整流程：无需额外检查
        pass

    # 根据运行阶段设置数据源类型
    if RUN_STAGES == [2]:
        # 只运行阶段2：需要加载已增强的数据
        data_source_type = "enhanced"
    elif RUN_STAGES == [3]:
        # 只运行阶段3：需要加载已增强的数据
        data_source_type = "enhanced"
    elif RUN_STAGES == [2, 3]:
        # 运行阶段2和3：需要加载已增强的数据
        data_source_type = "enhanced"
    elif RUN_STAGES == [1, 2, 3] or RUN_STAGES == [1]:
        # 运行阶段1或完整流程：从原始数据开始
        data_source_type = "original"
    else:
        data_source_type = "original"

    # 确定起始阶段
    if RUN_STAGES == [2]:
        start_stage = 2
    elif RUN_STAGES == [3]:
        start_stage = 3
    elif RUN_STAGES == [2, 3]:
        start_stage = 2  # 从阶段2开始
    else:
        start_stage = 1

    shared = init_shared(
        input_data_path=INPUT_DATA_PATH,
        output_data_path=OUTPUT_DATA_PATH,
        topics_path=TOPICS_PATH,
        sentiment_attributes_path=SENTIMENT_ATTRS_PATH,
        publisher_objects_path=PUBLISHER_OBJS_PATH,
        belief_system_path=BELIEF_SYSTEM_PATH,
        publisher_decision_path=PUBLISHER_DECISION_PATH,
        run_stages=RUN_STAGES,
        start_stage=start_stage,
        enhancement_mode=ENHANCEMENT_MODE,
        analysis_mode=ANALYSIS_MODE,
        tool_source=TOOL_SOURCE,
        agent_max_iterations=AGENT_MAX_ITERATIONS,
        report_mode=REPORT_MODE,
        report_max_iterations=REPORT_MAX_ITERATIONS,
        report_min_score=REPORT_MIN_SCORE,
        batch_script_path=BATCH_SCRIPT_PATH,
        data_source_type=data_source_type,  # 添加数据源类型参数
    )

        
    # 运行系统 - DispatcherNode会根据配置自动调度执行相应阶段
    # 当前配置：仅执行阶段3的模板模式
    # 性能参数使用run函数的默认值：concurrent_num=60, max_retries=3, wait_time=8
    asyncio.run(run(shared=shared))


if __name__ == "__main__":
    main()
