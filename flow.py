"""
舆情分析智能体 - Flow编排定义

系统采用中央调度模式，DispatcherNode作为唯一入口。
所有阶段的执行路径由shared["dispatcher"]和shared["config"]配置决定。

================================================================================
架构说明
================================================================================

DispatcherNode（中央调度入口）
    ├─ stage1_async → AsyncEnhancementFlow → dispatch（返回）
    ├─ stage1_batch_api → BatchAPIEnhancementFlow → dispatch（返回）
    ├─ stage2_workflow → WorkflowAnalysisFlow → dispatch（返回）
    ├─ stage2_agent → AgentAnalysisFlow → dispatch（返回）
    ├─ stage3_template → TemplateReportFlow → dispatch（返回）（待实现）
    ├─ stage3_iterative → IterativeReportFlow → dispatch（返回）（待实现）
    └─ done → TerminalNode（结束）

================================================================================
"""

from pocketflow import Flow, AsyncFlow

from nodes import (
    # 系统调度节点
    DispatcherNode,
    TerminalNode,
    # 阶段1完成节点
    Stage1CompletionNode,
    # 阶段1通用节点
    DataLoadNode,
    SaveEnhancedDataNode,
    DataValidationAndOverviewNode,
    # 阶段1异步分析节点
    AsyncSentimentPolarityAnalysisBatchNode,
    AsyncSentimentAttributeAnalysisBatchNode,
    AsyncTwoLevelTopicAnalysisBatchNode,
    AsyncPublisherObjectAnalysisBatchNode,
    # 阶段1 Batch API节点
    BatchAPIEnhancementNode,
    # 阶段2通用节点
    LoadEnhancedDataNode,
    DataSummaryNode,
    SaveAnalysisResultsNode,
    Stage2CompletionNode,
    # 阶段2 Workflow路径节点
    ExecuteAnalysisScriptNode,
    ChartAnalysisNode,
    LLMInsightNode,
    # 阶段2 Agent路径节点
    CollectToolsNode,
    DecisionToolsNode,
    ExecuteToolsNode,
    ChartAnalysisNode,
    ProcessResultNode,
)


def _create_async_enhancement_flow(
    concurrent_num: int,
    max_retries: int,
    wait_time: int
) -> AsyncFlow:
    """
    创建异步批量并行处理Flow (enhancement_mode="async")
    
    内部函数，由create_main_flow调用。
    """
    data_load_node = DataLoadNode()
    
    sentiment_polarity_node = AsyncSentimentPolarityAnalysisBatchNode(
        max_retries=max_retries, 
        wait=wait_time, 
        max_concurrent=concurrent_num
    )
    sentiment_attribute_node = AsyncSentimentAttributeAnalysisBatchNode(
        max_retries=max_retries, 
        wait=wait_time, 
        max_concurrent=concurrent_num
    )
    topic_analysis_node = AsyncTwoLevelTopicAnalysisBatchNode(
        max_retries=max_retries, 
        wait=wait_time, 
        max_concurrent=concurrent_num
    )
    publisher_analysis_node = AsyncPublisherObjectAnalysisBatchNode(
        max_retries=max_retries, 
        wait=wait_time, 
        max_concurrent=concurrent_num
    )
    
    save_data_node = SaveEnhancedDataNode()
    validation_node = DataValidationAndOverviewNode()
    completion_node = Stage1CompletionNode()
    
    # 连接节点
    data_load_node >> sentiment_polarity_node
    sentiment_polarity_node >> sentiment_attribute_node
    sentiment_attribute_node >> topic_analysis_node
    topic_analysis_node >> publisher_analysis_node
    publisher_analysis_node >> save_data_node
    save_data_node >> validation_node
    validation_node >> completion_node
    
    return AsyncFlow(start=data_load_node)


def _create_batch_api_enhancement_flow() -> Flow:
    """
    创建Batch API处理Flow (enhancement_mode="batch_api")
    
    内部函数，由create_main_flow调用。
    """
    data_load_node = DataLoadNode()
    batch_api_node = BatchAPIEnhancementNode()
    validation_node = DataValidationAndOverviewNode()
    completion_node = Stage1CompletionNode()
    
    data_load_node >> batch_api_node
    batch_api_node >> validation_node
    validation_node >> completion_node
    
    return Flow(start=data_load_node)


def _create_workflow_analysis_flow() -> Flow:
    """
    创建预定义Workflow分析Flow (analysis_mode="workflow")
    
    执行流程：
    1. 加载增强数据
    2. 生成数据概况
    3. 执行固定分析脚本生成全部图形
    4. GLM4.5V分析每个图表
    5. LLM补充洞察信息
    6. 保存分析结果
    7. 返回调度器
    
    内部函数，由create_main_flow调用。
    """
    # 创建节点
    load_data_node = LoadEnhancedDataNode()
    data_summary_node = DataSummaryNode()
    execute_script_node = ExecuteAnalysisScriptNode()
    chart_analysis_node = ChartAnalysisNode(max_retries=2, wait=3)
    llm_insight_node = LLMInsightNode()
    save_results_node = SaveAnalysisResultsNode()
    completion_node = Stage2CompletionNode()

    # 连接节点
    load_data_node >> data_summary_node
    data_summary_node >> execute_script_node
    execute_script_node >> chart_analysis_node
    chart_analysis_node >> llm_insight_node
    llm_insight_node >> save_results_node
    save_results_node >> completion_node

    return Flow(start=load_data_node)


def _create_agent_analysis_flow() -> AsyncFlow:
    """
    创建Agent自主调度分析Flow (analysis_mode="agent")
    
    执行流程：
    1. 加载增强数据
    2. 生成数据概况
    3. 收集可用工具
    4. Agent决策循环：决策 → 执行 → 处理 → 决策...
    5. GLM4.5V分析生成的图表
    6. 生成洞察分析
    7. 保存分析结果
    8. 返回调度器
    
    内部函数，由create_main_flow调用。
    """
    # 创建节点
    load_data_node = LoadEnhancedDataNode()
    data_summary_node = DataSummaryNode()
    collect_tools_node = CollectToolsNode()
    decision_node = DecisionToolsNode()
    execute_node = ExecuteToolsNode()
    process_result_node = ProcessResultNode()
    chart_analysis_node = ChartAnalysisNode(max_retries=2, wait=3)
    llm_insight_node = LLMInsightNode()
    save_results_node = SaveAnalysisResultsNode()
    completion_node = Stage2CompletionNode()

    # 连接节点 - 线性部分
    load_data_node >> data_summary_node
    data_summary_node >> collect_tools_node
    collect_tools_node >> decision_node

    # Agent循环部分
    decision_node - "execute" >> execute_node
    execute_node >> process_result_node
    process_result_node - "continue" >> decision_node  # 继续循环

    # 结束循环的路径
    decision_node - "finish" >> chart_analysis_node  # Agent判断分析充分，先分析图表
    process_result_node - "finish" >> chart_analysis_node  # 达到最大迭代次数，先分析图表

    chart_analysis_node >> llm_insight_node  # 图表分析完成后生成洞察
    llm_insight_node >> save_results_node
    save_results_node >> completion_node

    return AsyncFlow(start=load_data_node)


def create_main_flow(
    concurrent_num: int = 60,
    max_retries: int = 3,
    wait_time: int = 8
) -> AsyncFlow:
    """
    创建中央调度主Flow - 系统唯一入口
    
    系统采用中央调度模式：
    1. DispatcherNode根据shared["dispatcher"]["run_stages"]决定执行哪些阶段
    2. 根据shared["config"]中的模式配置选择具体执行路径
    3. 每个阶段完成后返回DispatcherNode，由其决定下一步
    4. 当所有配置的阶段完成后，自动导向TerminalNode结束
    
    Args:
        concurrent_num: 异步处理最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间（秒）
    
    Returns:
        AsyncFlow: 配置好的中央调度主Flow
    """
    # 中央调度节点
    dispatcher = DispatcherNode()
    terminal = TerminalNode()
    
    # 阶段1 Flow
    async_enhancement_flow = _create_async_enhancement_flow(
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time
    )
    batch_api_enhancement_flow = _create_batch_api_enhancement_flow()
    
    # 阶段2 Flow
    workflow_analysis_flow = _create_workflow_analysis_flow()
    agent_analysis_flow = _create_agent_analysis_flow()
    
    # === 阶段1路径 ===
    dispatcher - "stage1_async" >> async_enhancement_flow
    dispatcher - "stage1_batch_api" >> batch_api_enhancement_flow
    async_enhancement_flow - "dispatch" >> dispatcher
    batch_api_enhancement_flow - "dispatch" >> dispatcher
    
    # === 阶段2路径 ===
    dispatcher - "stage2_workflow" >> workflow_analysis_flow
    dispatcher - "stage2_agent" >> agent_analysis_flow
    workflow_analysis_flow - "dispatch" >> dispatcher
    agent_analysis_flow - "dispatch" >> dispatcher
    
    # === 阶段3路径（待实现） ===
    # dispatcher - "stage3_template" >> template_report_flow
    # dispatcher - "stage3_iterative" >> iterative_report_flow
    # template_report_flow - "dispatch" >> dispatcher
    # iterative_report_flow - "dispatch" >> dispatcher
    
    # === 结束路径 ===
    dispatcher - "done" >> terminal
    
    return AsyncFlow(start=dispatcher)


def create_stage2_only_flow(analysis_mode: str = "workflow") -> Flow:
    """
    创建仅阶段2的Flow - 用于独立运行阶段2
    
    前置条件：阶段1已完成，增强数据已保存到 data/enhanced_blogs.json
    
    Args:
        analysis_mode: 分析模式，"workflow" 或 "agent"
    
    Returns:
        Flow: 配置好的阶段2 Flow
    """
    if analysis_mode == "workflow":
        return _create_workflow_analysis_flow()
    else:
        return _create_agent_analysis_flow()
