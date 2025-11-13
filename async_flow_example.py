"""
异步流程示例

展示如何使用新的 AsyncParallelBatchNode 节点创建 AsyncFlow 来提高处理速度
"""

import asyncio
from pocketflow import AsyncFlow
from nodes import (
    AsyncSentimentPolarityAnalysisBatchNode,
    AsyncSentimentAttributeAnalysisBatchNode, 
    AsyncTwoLevelTopicAnalysisBatchNode,
    AsyncPublisherObjectAnalysisBatchNode,
    DataLoadNode,
    SaveEnhancedDataNode,
    DataValidationAndOverviewNode
)


async def create_async_analysis_flow():
    """
    创建异步分析流程
    使用 AsyncParallelBatchNode 节点来并发处理数据，提高处理速度
    """
    
    # 创建节点实例，设置并发限制
    data_load_node = DataLoadNode()
    
    # 异步批处理节点，设置并发限制为3（避免API调用过于频繁）
    sentiment_polarity_node = AsyncSentimentPolarityAnalysisBatchNode(max_retries=3, wait=10,max_concurrent=5)
    sentiment_attribute_node = AsyncSentimentAttributeAnalysisBatchNode(max_retries=3, wait=10,max_concurrent=5)
    topic_analysis_node = AsyncTwoLevelTopicAnalysisBatchNode(max_retries=3, wait=10,max_concurrent=5)  # 主题分析较复杂，降低并发
    publisher_analysis_node = AsyncPublisherObjectAnalysisBatchNode(max_retries=3, wait=10,max_concurrent=5)  # 发布者分析较简单，可以提高并发
    
    # 同步节点
    save_data_node = SaveEnhancedDataNode()
    validation_node = DataValidationAndOverviewNode()
    
    # 连接节点形成流程
    data_load_node >> sentiment_polarity_node
    sentiment_polarity_node >> sentiment_attribute_node
    sentiment_attribute_node >> topic_analysis_node
    topic_analysis_node >> publisher_analysis_node
    publisher_analysis_node >> save_data_node
    save_data_node >> validation_node
    
    # 创建异步流程
    async_flow = AsyncFlow(start=data_load_node)
    
    return async_flow


async def main():
    """主函数演示异步流程的使用"""
    print("=== 异步数据处理流程示例 ===\n")
    
    # 配置数据源
    config = {
        "data_source": {
            "type": "original",
            "enhanced_data_path": "data/test_enhanced_blogs.json"
        }
    }
    
    # 初始化共享数据
    shared_data = {
        "config": config,
        "data": {
            "data_paths": {
                "blog_data_path": "data/test_posts.json",  # 使用测试数据
                "topics_path": "data/topics.json",
                "sentiment_attributes_path": "data/sentiment_attributes.json",
                "publisher_objects_path": "data/publisher_objects.json"
            }
        },
        "results": {
            "statistics": {}  # 预先初始化statistics结构，避免KeyError
        }
    }
    
    # 创建异步流程
    async_flow = await create_async_analysis_flow()
    
    print("开始异步数据处理流程...")
    print("配置信息:")
    print(f"  - 情感极性分析并发数: 5")
    print(f"  - 情感属性分析并发数: 5") 
    print(f"  - 主题分析并发数: 5")
    print(f"  - 发布者分析并发数: 5")
    print()
    
    # 运行异步流程
    import time
    start_time = time.time()
    
    try:
        await async_flow.run_async(shared_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"\n=== 流程完成 ===")
        print(f"总处理时间: {processing_time:.2f} 秒")
        
        # 显示统计信息
        if "statistics" in shared_data.get("results", {}):
            stats = shared_data["results"]["statistics"]
            print(f"\n=== 数据统计 ===")
            print(f"总博文数: {stats.get('total_blogs', 0)}")
            print(f"已处理博文数: {stats.get('processed_blogs', 0)}")
            
            if "engagement_statistics" in stats:
                eng_stats = stats["engagement_statistics"]
                print(f"平均转发数: {eng_stats.get('avg_reposts', 0):.2f}")
                print(f"平均评论数: {eng_stats.get('avg_comments', 0):.2f}")
                print(f"平均点赞数: {eng_stats.get('avg_likes', 0):.2f}")
            
            if "empty_fields" in stats:
                empty_stats = stats["empty_fields"]
                print(f"情感极性为空: {empty_stats.get('sentiment_polarity_empty', 0)}")
                print(f"情感属性为空: {empty_stats.get('sentiment_attribute_empty', 0)}")
                print(f"主题为空: {empty_stats.get('topics_empty', 0)}")
                print(f"发布者为空: {empty_stats.get('publisher_empty', 0)}")
        
        # 显示保存状态
        if "data_save" in shared_data.get("results", {}):
            save_stats = shared_data["results"]["data_save"]
            if save_stats.get("saved", False):
                print(f"\n数据已保存到: {save_stats.get('output_path', 'N/A')}")
                print(f"保存数据量: {save_stats.get('data_count', 0)} 条")
            else:
                print(f"\n数据保存失败: {save_stats.get('error', '未知错误')}")
    
    except Exception as e:
        print(f"\n流程执行出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
