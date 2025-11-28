"""
舆情分析智能体 - Flow编排定义

根据设计文档，系统采用中央调度+三阶段顺序依赖架构。
本文件包含所有Flow的创建函数，按以下结构组织：

================================================================================
目录结构
================================================================================

1. 阶段1 Flow: 原始博文增强处理
   - create_async_enhancement_flow(): 异步批量并行处理Flow
   - create_batch_api_enhancement_flow(): Batch API处理Flow

2. 阶段2 Flow: 分析执行（待实现）
   - create_workflow_analysis_flow(): 预定义Workflow分析Flow
   - create_agent_analysis_flow(): Agent自主调度分析Flow

3. 阶段3 Flow: 报告生成（待实现）
   - create_template_report_flow(): 模板填充报告Flow
   - create_iterative_report_flow(): 多轮迭代报告Flow

4. 主Flow: 系统入口
   - create_main_flow(): 中央调度主Flow

================================================================================
"""

from pocketflow import Flow, AsyncFlow

# 导入阶段1节点
from nodes import (
    # 系统调度节点
    DispatcherNode,
    TerminalNode,
    # 阶段1入口和完成节点
    Stage1EntryNode,
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
)


# =============================================================================
# 1. 阶段1 Flow: 原始博文增强处理
# =============================================================================

# 默认并发配置
DEFAULT_CONCURRENT_NUM = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_WAIT_TIME = 8


def create_async_enhancement_flow(
    concurrent_num: int = DEFAULT_CONCURRENT_NUM,
    max_retries: int = DEFAULT_MAX_RETRIES,
    wait_time: int = DEFAULT_WAIT_TIME
) -> AsyncFlow:
    """
    创建异步批量并行处理Flow (enhancement_mode="async")
    
    用于阶段1的异步增强处理路径，通过AsyncParallelBatchNode
    并发调用LLM API对博文进行四维度分析。
    
    流程：
    DataLoadNode → AsyncSentimentPolarityBatchNode → AsyncSentimentAttributeBatchNode
    → AsyncTwoLevelTopicBatchNode → AsyncPublisherObjectBatchNode 
    → SaveEnhancedDataNode → DataValidationAndOverviewNode → Stage1CompletionNode
    
    Args:
        concurrent_num: 最大并发数，默认60
        max_retries: 最大重试次数，默认3
        wait_time: 重试等待时间（秒），默认8
    
    Returns:
        AsyncFlow: 配置好的异步增强处理Flow
    """
    # 创建节点实例
    data_load_node = DataLoadNode()
    
    # 异步批处理节点，设置并发限制
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
    
    # 保存和验证节点
    save_data_node = SaveEnhancedDataNode()
    validation_node = DataValidationAndOverviewNode()
    
    # 阶段完成节点
    completion_node = Stage1CompletionNode()
    
    # 连接节点形成流程
    # DataLoad → 四维度分析（串行，每个维度内部并行） → Save → Validation → Completion
    data_load_node >> sentiment_polarity_node
    sentiment_polarity_node >> sentiment_attribute_node
    sentiment_attribute_node >> topic_analysis_node
    topic_analysis_node >> publisher_analysis_node
    publisher_analysis_node >> save_data_node
    save_data_node >> validation_node
    validation_node >> completion_node
    
    # 创建异步流程
    async_flow = AsyncFlow(start=data_load_node)
    
    return async_flow


def create_batch_api_enhancement_flow() -> Flow:
    """
    创建Batch API处理Flow (enhancement_mode="batch_api")
    
    用于阶段1的Batch API增强处理路径，通过调用batch/目录下的脚本
    进行批量处理。
    
    流程：
    DataLoadNode → BatchAPIEnhancementNode → DataValidationAndOverviewNode 
    → Stage1CompletionNode
    
    Returns:
        Flow: 配置好的Batch API处理Flow
    """
    # 创建节点实例
    data_load_node = DataLoadNode()
    batch_api_node = BatchAPIEnhancementNode()
    validation_node = DataValidationAndOverviewNode()
    completion_node = Stage1CompletionNode()
    
    # 连接节点形成流程
    data_load_node >> batch_api_node
    batch_api_node >> validation_node
    validation_node >> completion_node
    
    # 创建流程
    flow = Flow(start=data_load_node)
    
    return flow


# =============================================================================
# 2. 阶段2 Flow: 分析执行（待实现）
# =============================================================================

def create_workflow_analysis_flow() -> Flow:
    """
    创建预定义Workflow分析Flow (analysis_mode="workflow")
    
    用于阶段2的固定脚本分析路径，执行预定义的分析脚本生成全部图形，
    然后调用LLM补充洞察信息。
    
    流程（待实现）：
    LoadEnhancedDataNode → ExecuteAnalysisScriptNode → LLMInsightNode 
    → SaveAnalysisResultsNode → Stage2CompletionNode
    
    Returns:
        Flow: 配置好的Workflow分析Flow
    """
    # TODO: 实现阶段2 Workflow分析Flow
    # 需要实现以下节点：
    # - LoadEnhancedDataNode
    # - ExecuteAnalysisScriptNode
    # - LLMInsightNode
    # - SaveAnalysisResultsNode
    # - Stage2CompletionNode
    raise NotImplementedError("阶段2 Workflow分析Flow尚未实现")


def create_agent_analysis_flow() -> Flow:
    """
    创建Agent自主调度分析Flow (analysis_mode="agent")
    
    用于阶段2的智能体调度路径，Single Agent通过循环自主决策
    执行哪些分析工具。
    
    流程（待实现）：
    LoadEnhancedDataNode → DataSummaryNode → CollectToolsNode 
    → [DecisionToolsNode → ExecuteToolsNode → ProcessResultNode]（循环）
    → SaveAnalysisResultsNode → Stage2CompletionNode
    
    Returns:
        Flow: 配置好的Agent分析Flow
    """
    # TODO: 实现阶段2 Agent分析Flow
    # 需要实现以下节点：
    # - LoadEnhancedDataNode
    # - DataSummaryNode
    # - CollectToolsNode
    # - DecisionToolsNode
    # - ExecuteToolsNode
    # - ProcessResultNode
    # - SaveAnalysisResultsNode
    # - Stage2CompletionNode
    raise NotImplementedError("阶段2 Agent分析Flow尚未实现")


# =============================================================================
# 3. 阶段3 Flow: 报告生成（待实现）
# =============================================================================

def create_template_report_flow() -> Flow:
    """
    创建模板填充报告Flow (report_mode="template")
    
    用于阶段3的模板填充报告路径，使用预定义模板，LLM填充各章节内容。
    
    流程（待实现）：
    LoadAnalysisResultsNode → LoadTemplateNode → FillSectionNode（多个）
    → AssembleReportNode → FormatReportNode → SaveReportNode 
    → Stage3CompletionNode
    
    Returns:
        Flow: 配置好的模板填充报告Flow
    """
    # TODO: 实现阶段3 模板填充报告Flow
    raise NotImplementedError("阶段3 模板填充报告Flow尚未实现")


def create_iterative_report_flow() -> Flow:
    """
    创建多轮迭代报告Flow (report_mode="iterative")
    
    用于阶段3的多轮迭代报告路径，通过生成-评审-修改循环
    直到满意或达到最大迭代次数。
    
    流程（待实现）：
    LoadAnalysisResultsNode → InitReportStateNode 
    → [GenerateReportNode → ReviewReportNode → ApplyFeedbackNode]（循环）
    → FormatReportNode → SaveReportNode → Stage3CompletionNode
    
    Returns:
        Flow: 配置好的多轮迭代报告Flow
    """
    # TODO: 实现阶段3 多轮迭代报告Flow
    raise NotImplementedError("阶段3 多轮迭代报告Flow尚未实现")


# =============================================================================
# 4. 主Flow: 系统入口（中央调度模式）
# =============================================================================

def create_main_flow(
    concurrent_num: int = DEFAULT_CONCURRENT_NUM,
    max_retries: int = DEFAULT_MAX_RETRIES,
    wait_time: int = DEFAULT_WAIT_TIME
) -> AsyncFlow:
    """
    创建中央调度主Flow
    
    系统采用中央调度模式，DispatcherNode作为入口和控制中心，
    根据配置决定执行路径，每个阶段完成后返回调度节点。
    
    注意：由于内部包含AsyncFlow，必须返回AsyncFlow并使用run_async()运行
    
    架构：
    DispatcherNode（入口）
        ├─ stage1_async → AsyncEnhancementFlow → dispatch（返回）
        ├─ stage1_batch_api → BatchAPIEnhancementFlow → dispatch（返回）
        ├─ stage2_workflow → WorkflowAnalysisFlow → dispatch（返回）（待实现）
        ├─ stage2_agent → AgentAnalysisFlow → dispatch（返回）（待实现）
        ├─ stage3_template → TemplateReportFlow → dispatch（返回）（待实现）
        ├─ stage3_iterative → IterativeReportFlow → dispatch（返回）（待实现）
        └─ done → TerminalNode（结束）
    
    Args:
        concurrent_num: 异步处理最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间（秒）
    
    Returns:
        AsyncFlow: 配置好的中央调度主Flow
    """
    # 创建调度节点
    dispatcher = DispatcherNode()
    terminal = TerminalNode()
    
    # 创建阶段1 Flow
    async_enhancement_flow = create_async_enhancement_flow(
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time
    )
    batch_api_enhancement_flow = create_batch_api_enhancement_flow()
    
    # 连接调度节点到各阶段Flow
    # 阶段1路径
    dispatcher - "stage1_async" >> async_enhancement_flow
    dispatcher - "stage1_batch_api" >> batch_api_enhancement_flow
    
    # 阶段1完成后返回调度器
    async_enhancement_flow - "dispatch" >> dispatcher
    batch_api_enhancement_flow - "dispatch" >> dispatcher
    
    # TODO: 阶段2路径（待实现）
    # dispatcher - "stage2_workflow" >> workflow_analysis_flow
    # dispatcher - "stage2_agent" >> agent_analysis_flow
    # workflow_analysis_flow - "dispatch" >> dispatcher
    # agent_analysis_flow - "dispatch" >> dispatcher
    
    # TODO: 阶段3路径（待实现）
    # dispatcher - "stage3_template" >> template_report_flow
    # dispatcher - "stage3_iterative" >> iterative_report_flow
    # template_report_flow - "dispatch" >> dispatcher
    # iterative_report_flow - "dispatch" >> dispatcher
    
    # 结束路径
    dispatcher - "done" >> terminal
    
    # 创建主Flow（使用AsyncFlow因为内部包含异步节点）
    main_flow = AsyncFlow(start=dispatcher)
    
    return main_flow


def create_stage1_only_flow(
    mode: str = "async",
    concurrent_num: int = DEFAULT_CONCURRENT_NUM,
    max_retries: int = DEFAULT_MAX_RETRIES,
    wait_time: int = DEFAULT_WAIT_TIME
) -> AsyncFlow:
    """
    创建仅阶段1的独立Flow（便于单独测试）
    
    注意：由于内部包含AsyncFlow，必须返回AsyncFlow并使用run_async()运行
    
    Args:
        mode: 处理模式，"async" 或 "batch_api"
        concurrent_num: 异步处理最大并发数
        max_retries: 最大重试次数
        wait_time: 重试等待时间（秒）
    
    Returns:
        AsyncFlow: 阶段1独立Flow
    """
    # 创建入口节点
    entry_node = Stage1EntryNode()
    terminal = TerminalNode()
    
    # 创建两个路径的Flow
    async_flow = create_async_enhancement_flow(
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time
    )
    batch_api_flow = create_batch_api_enhancement_flow()
    
    # 连接入口节点到两个路径
    entry_node - "async" >> async_flow
    entry_node - "batch_api" >> batch_api_flow
    
    # 两个路径都连接到终止节点
    async_flow - "dispatch" >> terminal
    batch_api_flow - "dispatch" >> terminal
    
    # 创建AsyncFlow（因为内部包含异步节点）
    flow = AsyncFlow(start=entry_node)
    
    return flow

