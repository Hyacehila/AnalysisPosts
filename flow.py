"""
舆情分析智能体 - Flow编排定义

系统采用中央调度模式，DispatcherNode作为唯一入口。
所有阶段的执行路径由shared["dispatcher"]和shared["config"]配置决定。

================================================================================
架构说明
================================================================================

DispatcherNode（中央调度入口）
    ├─ stage1_async → AsyncEnhancementFlow → dispatch（返回）
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
    NLPEnrichmentNode,
    # 阶段1异步分析节点
    AsyncSentimentPolarityAnalysisBatchNode,
    AsyncSentimentAttributeAnalysisBatchNode,
    AsyncTwoLevelTopicAnalysisBatchNode,
    AsyncPublisherObjectAnalysisBatchNode,
    AsyncBeliefSystemAnalysisBatchNode,
    AsyncPublisherDecisionAnalysisBatchNode,

    # 阶段2通用节点
    LoadEnhancedDataNode,
    DataSummaryNode,
    ClearReportDirNode,
    SaveAnalysisResultsNode,
    Stage2CompletionNode,
    ChartAnalysisNode,
    LLMInsightNode,
    EnsureChartsNode,
    # 阶段2 Agent路径节点
    CollectToolsNode,
    DecisionToolsNode,
    ExecuteToolsNode,
    ProcessResultNode,
    # 阶段3通用节点
    LoadAnalysisResultsNode,
    FormatReportNode,
    SaveReportNode,
    ClearStage3OutputsNode,
    Stage3CompletionNode,
    # 阶段3模板填充路径节点
    GenerateFullReportNode,
    LoadTemplateNode,
    FillSectionNode,
    AssembleReportNode,
    # 阶段3多轮迭代路径节点
    InitReportStateNode,
    GenerateReportNode,
    ReviewReportNode,
    ApplyFeedbackNode,
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
    nlp_enrichment_node = NLPEnrichmentNode()
    
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
    belief_analysis_node = AsyncBeliefSystemAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num
    )
    publisher_decision_node = AsyncPublisherDecisionAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num
    )
    
    save_data_node = SaveEnhancedDataNode()
    validation_node = DataValidationAndOverviewNode()
    completion_node = Stage1CompletionNode()
    
    # 连接节点
    data_load_node >> nlp_enrichment_node
    nlp_enrichment_node >> sentiment_polarity_node
    sentiment_polarity_node >> sentiment_attribute_node
    sentiment_attribute_node >> topic_analysis_node
    topic_analysis_node >> publisher_analysis_node
    publisher_analysis_node >> belief_analysis_node
    belief_analysis_node >> publisher_decision_node
    publisher_decision_node >> save_data_node
    save_data_node >> validation_node
    validation_node >> completion_node
    
    return AsyncFlow(start=data_load_node)



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
    clear_report_node = ClearReportDirNode()
    load_data_node = LoadEnhancedDataNode()
    data_summary_node = DataSummaryNode()
    collect_tools_node = CollectToolsNode()
    decision_node = DecisionToolsNode()
    execute_node = ExecuteToolsNode()
    process_result_node = ProcessResultNode()
    chart_analysis_node = ChartAnalysisNode(max_retries=2, wait=3)
    ensure_charts_node = EnsureChartsNode()
    llm_insight_node = LLMInsightNode()
    save_results_node = SaveAnalysisResultsNode()
    completion_node = Stage2CompletionNode()

    # 连接节点 - 线性部分
    clear_report_node >> load_data_node
    load_data_node >> data_summary_node
    data_summary_node >> collect_tools_node
    collect_tools_node >> decision_node

    # Agent循环部分
    decision_node - "execute" >> execute_node
    execute_node >> process_result_node
    process_result_node - "continue" >> decision_node  # 继续循环

    # 结束循环的路径
    decision_node - "finish" >> ensure_charts_node  # Agent判断分析充分，先补齐图表
    process_result_node - "finish" >> ensure_charts_node  # 达到最大迭代次数，先补齐图表

    ensure_charts_node >> chart_analysis_node
    chart_analysis_node >> llm_insight_node  # 图表分析完成后生成洞察
    llm_insight_node >> save_results_node
    save_results_node >> completion_node

    return AsyncFlow(start=clear_report_node)


def create_main_flow(
    concurrent_num: int = 60,
    max_retries: int = 3,
    wait_time: int = 8
) -> AsyncFlow:
    """
    创建中央调度主Flow - 系统唯一入口
    
    系统采用中央调度模式：
    1. DispatcherNode根据shared["dispatcher"]["run_stages"]决定执行哪些阶段
    2. 根据shared["config"]中的模式配置选择具体执行路径（Stage2 固定为 Agent）
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
    
    # 阶段2 Flow
    agent_analysis_flow = _create_agent_analysis_flow()

    # 阶段3 Flow
    template_report_flow = _create_template_report_flow()
    iterative_report_flow = _create_iterative_report_flow()

    # === 阶段1路径 ===
    dispatcher - "stage1_async" >> async_enhancement_flow
    async_enhancement_flow - "dispatch" >> dispatcher

    # === 阶段2路径 ===
    dispatcher - "stage2_agent" >> agent_analysis_flow
    agent_analysis_flow - "dispatch" >> dispatcher

    # === 阶段3路径 ===
    dispatcher - "stage3_template" >> template_report_flow
    dispatcher - "stage3_iterative" >> iterative_report_flow
    template_report_flow - "dispatch" >> dispatcher
    iterative_report_flow - "dispatch" >> dispatcher
    
    # === 结束路径 ===
    dispatcher - "done" >> terminal
    
    return AsyncFlow(start=dispatcher)


def _create_template_report_flow() -> Flow:
    """
    创建模板填充报告Flow (report_mode="template")

    执行流程：
    1. 加载分析结果
    2. 一次性生成完整报告（替代分章节生成）
    3. 格式化报告
    4. 保存报告文件
    5. 返回调度器

    内部函数，由create_main_flow调用。
    """
    # 创建节点
    clear_outputs_node = ClearStage3OutputsNode()
    load_results_node = LoadAnalysisResultsNode()
    generate_report_node = GenerateFullReportNode()
    format_node = FormatReportNode()
    save_node = SaveReportNode()
    completion_node = Stage3CompletionNode()

    # 连接节点 - 简化为一次性生成流程
    clear_outputs_node >> load_results_node
    load_results_node >> generate_report_node
    generate_report_node >> format_node
    format_node >> save_node
    save_node >> completion_node

    return Flow(start=clear_outputs_node)


def _create_iterative_report_flow() -> AsyncFlow:
    """
    创建多轮迭代报告Flow (report_mode="iterative")

    执行流程：
    1. 加载分析结果
    2. 初始化报告状态
    3. 生成初始报告
    4. LLM评审报告质量
    5. 根据评审决定是否修改
    6. 如果需要修改：应用反馈 -> 重新生成 -> 评审
    7. 重复直到满足条件或达到最大迭代次数
    8. 格式化并保存最终报告
    9. 返回调度器

    内部函数，由create_main_flow调用。
    """
    # 创建节点
    clear_outputs_node = ClearStage3OutputsNode()
    load_results_node = LoadAnalysisResultsNode()
    init_state_node = InitReportStateNode()
    generate_node = GenerateReportNode()
    review_node = ReviewReportNode()
    apply_feedback_node = ApplyFeedbackNode()
    format_node = FormatReportNode()
    save_node = SaveReportNode()
    completion_node = Stage3CompletionNode()

    # 连接节点
    clear_outputs_node >> load_results_node
    load_results_node >> init_state_node
    init_state_node >> generate_node
    generate_node >> review_node

    # 迭代循环部分
    review_node - "needs_revision" >> apply_feedback_node
    apply_feedback_node - "continue_iteration" >> generate_node

    # 结束循环的路径
    review_node - "satisfied" >> format_node
    apply_feedback_node - "max_iterations_reached" >> format_node

    format_node >> save_node
    save_node >> completion_node

    return AsyncFlow(start=clear_outputs_node)


def create_stage2_only_flow() -> AsyncFlow:
    """
    创建仅阶段2的Flow - 用于独立运行阶段2

    前置条件：阶段1已完成，增强数据已保存到 data/enhanced_blogs.json

    Returns:
        Flow: 配置好的阶段2 Flow
    """
    return _create_agent_analysis_flow()


def create_stage3_only_flow(report_mode: str = "template") -> Flow:
    """
    创建仅阶段3的Flow - 用于独立运行阶段3

    前置条件：阶段2已完成，分析结果已保存到 report/ 目录

    Args:
        report_mode: 报告模式，"template" 或 "iterative"

    Returns:
        Flow: 配置好的阶段3 Flow
    """
    if report_mode == "template":
        return _create_template_report_flow()
    else:
        return _create_iterative_report_flow()
