"""
舆情分析智能体 - Flow 编排定义

系统采用中央调度模式，DispatcherNode 作为唯一入口。
"""

from pocketflow import AsyncFlow

from nodes import (
    # 调度节点
    DispatcherNode,
    TerminalNode,
    Stage1CompletionNode,
    Stage2CompletionNode,
    Stage3CompletionNode,
    # Stage1 节点
    DataLoadNode,
    SaveEnhancedDataNode,
    DataValidationAndOverviewNode,
    NLPEnrichmentNode,
    AsyncSentimentPolarityAnalysisBatchNode,
    AsyncSentimentAttributeAnalysisBatchNode,
    AsyncTwoLevelTopicAnalysisBatchNode,
    AsyncPublisherObjectAnalysisBatchNode,
    AsyncBeliefSystemAnalysisBatchNode,
    AsyncPublisherDecisionAnalysisBatchNode,
    # Stage2 节点
    LoadEnhancedDataNode,
    DataSummaryNode,
    ClearReportDirNode,
    SaveAnalysisResultsNode,
    ChartAnalysisNode,
    LLMInsightNode,
    create_query_search_flow,
    create_parallel_agent_flow,
    ForumHostNode,
    SupplementDataNode,
    SupplementSearchNode,
    VisualAnalysisNode,
    MergeResultsNode,
    # Stage3 节点
    ClearStage3OutputsNode,
    LoadAnalysisResultsNode,
    PlanOutlineNode,
    GenerateChaptersBatchNode,
    ReviewChaptersNode,
    InjectTraceNode,
    MethodologyAppendixNode,
    FormatReportNode,
    RenderHTMLNode,
    SaveReportNode,
)


def _create_async_enhancement_flow(
    concurrent_num: int,
    max_retries: int,
    wait_time: int,
) -> AsyncFlow:
    """Create Stage1 async enhancement flow."""
    data_load_node = DataLoadNode()
    nlp_enrichment_node = NLPEnrichmentNode()

    sentiment_polarity_node = AsyncSentimentPolarityAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )
    sentiment_attribute_node = AsyncSentimentAttributeAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )
    topic_analysis_node = AsyncTwoLevelTopicAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )
    publisher_analysis_node = AsyncPublisherObjectAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )
    belief_analysis_node = AsyncBeliefSystemAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )
    publisher_decision_node = AsyncPublisherDecisionAnalysisBatchNode(
        max_retries=max_retries,
        wait=wait_time,
        max_concurrent=concurrent_num,
    )

    save_data_node = SaveEnhancedDataNode()
    validation_node = DataValidationAndOverviewNode()
    completion_node = Stage1CompletionNode()

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
    """Create Stage2 dual-source analysis flow."""
    clear_report_node = ClearReportDirNode()
    load_data_node = LoadEnhancedDataNode()
    data_summary_node = DataSummaryNode()
    query_search_flow = create_query_search_flow()
    parallel_agent_flow = create_parallel_agent_flow()
    forum_host_node = ForumHostNode()
    supplement_data_node = SupplementDataNode()
    supplement_search_node = SupplementSearchNode()
    visual_analysis_node = VisualAnalysisNode()
    merge_node = MergeResultsNode()
    chart_analysis_node = ChartAnalysisNode(max_retries=2, wait=3)
    llm_insight_node = LLMInsightNode()
    save_results_node = SaveAnalysisResultsNode()
    completion_node = Stage2CompletionNode()

    clear_report_node >> load_data_node
    load_data_node >> data_summary_node
    data_summary_node >> query_search_flow
    query_search_flow >> parallel_agent_flow
    parallel_agent_flow >> forum_host_node

    forum_host_node - "supplement_data" >> supplement_data_node
    forum_host_node - "supplement_search" >> supplement_search_node
    forum_host_node - "supplement_visual" >> visual_analysis_node
    forum_host_node - "sufficient" >> merge_node

    supplement_data_node >> forum_host_node
    supplement_search_node >> forum_host_node
    visual_analysis_node >> forum_host_node

    merge_node >> chart_analysis_node
    chart_analysis_node >> llm_insight_node
    llm_insight_node >> save_results_node
    save_results_node >> completion_node

    return AsyncFlow(start=clear_report_node)


def _create_unified_report_flow() -> AsyncFlow:
    """Create unified Stage3 report flow."""
    clear_outputs_node = ClearStage3OutputsNode()
    load_results_node = LoadAnalysisResultsNode()
    outline_node = PlanOutlineNode()
    generate_chapters_node = GenerateChaptersBatchNode(max_concurrent=3)
    review_chapters_node = ReviewChaptersNode()
    inject_trace_node = InjectTraceNode()
    methodology_node = MethodologyAppendixNode()
    format_node = FormatReportNode()
    render_html_node = RenderHTMLNode()
    save_node = SaveReportNode()
    completion_node = Stage3CompletionNode()

    clear_outputs_node >> load_results_node
    load_results_node >> outline_node
    outline_node >> generate_chapters_node
    generate_chapters_node >> review_chapters_node

    review_chapters_node - "needs_revision" >> generate_chapters_node
    review_chapters_node - "satisfied" >> inject_trace_node

    inject_trace_node >> methodology_node
    methodology_node >> format_node
    format_node >> render_html_node
    render_html_node >> save_node
    save_node >> completion_node

    return AsyncFlow(start=clear_outputs_node)


def create_main_flow(
    concurrent_num: int = 60,
    max_retries: int = 3,
    wait_time: int = 8,
) -> AsyncFlow:
    """Create central dispatcher flow."""
    dispatcher = DispatcherNode()
    terminal = TerminalNode()

    async_enhancement_flow = _create_async_enhancement_flow(
        concurrent_num=concurrent_num,
        max_retries=max_retries,
        wait_time=wait_time,
    )
    agent_analysis_flow = _create_agent_analysis_flow()
    unified_report_flow = _create_unified_report_flow()

    dispatcher - "stage1_async" >> async_enhancement_flow
    async_enhancement_flow - "dispatch" >> dispatcher

    dispatcher - "stage2_agent" >> agent_analysis_flow
    agent_analysis_flow - "dispatch" >> dispatcher

    dispatcher - "stage3_report" >> unified_report_flow
    unified_report_flow - "dispatch" >> dispatcher

    dispatcher - "done" >> terminal

    return AsyncFlow(start=dispatcher)


def create_stage2_only_flow() -> AsyncFlow:
    """Create Stage2-only flow."""
    return _create_agent_analysis_flow()


def create_stage3_only_flow() -> AsyncFlow:
    """Create Stage3-only unified flow."""
    return _create_unified_report_flow()
