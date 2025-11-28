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
    ├─ stage2_workflow → WorkflowAnalysisFlow → dispatch（返回）（待实现）
    ├─ stage2_agent → AgentAnalysisFlow → dispatch（返回）（待实现）
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
    
    # === 阶段1路径 ===
    dispatcher - "stage1_async" >> async_enhancement_flow
    dispatcher - "stage1_batch_api" >> batch_api_enhancement_flow
    async_enhancement_flow - "dispatch" >> dispatcher
    batch_api_enhancement_flow - "dispatch" >> dispatcher
    
    # === 阶段2路径（待实现） ===
    # dispatcher - "stage2_workflow" >> workflow_analysis_flow
    # dispatcher - "stage2_agent" >> agent_analysis_flow
    # workflow_analysis_flow - "dispatch" >> dispatcher
    # agent_analysis_flow - "dispatch" >> dispatcher
    
    # === 阶段3路径（待实现） ===
    # dispatcher - "stage3_template" >> template_report_flow
    # dispatcher - "stage3_iterative" >> iterative_report_flow
    # template_report_flow - "dispatch" >> dispatcher
    # iterative_report_flow - "dispatch" >> dispatcher
    
    # === 结束路径 ===
    dispatcher - "done" >> terminal
    
    return AsyncFlow(start=dispatcher)
