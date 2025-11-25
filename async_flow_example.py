"""
异步流程示例

展示如何使用新的 AsyncParallelBatchNode 节点创建 AsyncFlow 来提高处理速度
"""

import asyncio
import concurrent
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

import concurrent.futures

# 并发配置
CONCURRENT_NUM = 60

async def create_async_analysis_flow():
    """
    创建异步分析流程
    使用 AsyncParallelBatchNode 节点来并发处理数据，提高处理速度
    """
    
    # 创建节点实例，设置并发限制
    data_load_node = DataLoadNode()
    
    # 异步批处理节点，设置并发限制为100（避免API调用过于频繁）
    # wait 参数降低为8 秒，减少重试等待时间
    sentiment_polarity_node = AsyncSentimentPolarityAnalysisBatchNode(max_retries=3, wait=8, max_concurrent=CONCURRENT_NUM)
    sentiment_attribute_node = AsyncSentimentAttributeAnalysisBatchNode(max_retries=3, wait=8, max_concurrent=CONCURRENT_NUM)
    topic_analysis_node = AsyncTwoLevelTopicAnalysisBatchNode(max_retries=3, wait=8, max_concurrent=CONCURRENT_NUM)  # 主题分析较复杂，降低并发
    publisher_analysis_node = AsyncPublisherObjectAnalysisBatchNode(max_retries=3, wait=8, max_concurrent=CONCURRENT_NUM)  # 发布者分析较简单，可以提高并发
    
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
    import time
    
    # 记录程序启动时间
    program_start_time = time.time()
    
    # 在异步上下文中设置线程池
    # 创建一个更大的线程池，允许 100 个并发
    # max_workers 根据你的 API 速率限制(Rate Limit)和机器内存来定
    # 线程池大小设置：应该略大于最大并发数，以避免线程不够用
    # 每个并发任务需要一个线程来执行同步的LLM调用
    thread_pool_size = CONCURRENT_NUM + 20  # 预留一些额外线程
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    
    print("=== 异步数据处理流程示例 ===\n")
    print(f"线程池配置: max_workers={thread_pool_size}")
    print(f"程序启动时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(program_start_time))}\n")
    
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
    print(f"  - 情感极性分析并发数: {CONCURRENT_NUM}")
    print(f"  - 情感属性分析并发数: {CONCURRENT_NUM}") 
    print(f"  - 主题分析并发数: {CONCURRENT_NUM}")
    print(f"  - 发布者分析并发数: {CONCURRENT_NUM}")
    print()
    
    # 记录分析开始时间
    analysis_start_time = time.time()
    print(f"分析开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(analysis_start_time))}\n")
    
    try:
        await async_flow.run_async(shared_data)
        
        # 记录分析结束时间
        analysis_end_time = time.time()
        analysis_time = analysis_end_time - analysis_start_time
        
        # 计算程序总运行时间
        program_end_time = time.time()
        total_program_time = program_end_time - program_start_time
        
        # 获取博文数量用于计算平均时间
        total_blogs = 0
        processed_blogs = 0
        if "statistics" in shared_data.get("results", {}):
            stats = shared_data["results"]["statistics"]
            total_blogs = stats.get('total_blogs', 0)
            processed_blogs = stats.get('processed_blogs', 0)
        
        # 计算每条博文的平均处理时间
        avg_time_per_blog = analysis_time / processed_blogs if processed_blogs > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"{'流程完成':^48}")
        print(f"{'='*50}")
        print(f"\n分析结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(analysis_end_time))}")
        print(f"\n⏱️  时间统计:")
        print(f"  ├─ 程序总运行时间: {total_program_time:.2f} 秒")
        print(f"  ├─ 数据分析时间: {analysis_time:.2f} 秒")
        print(f"  ├─ 初始化时间: {(analysis_start_time - program_start_time):.2f} 秒")
        print(f"  ├─ 处理博文数量: {processed_blogs} 条")
        print(f"  └─ 每条博文平均耗时: {avg_time_per_blog:.3f} 秒")
        
        if processed_blogs > 0:
            throughput = processed_blogs / analysis_time
            print(f"\n📊 处理效率: {throughput:.2f} 条/秒")
        
        # 显示统计信息
        if "statistics" in shared_data.get("results", {}):
            stats = shared_data["results"]["statistics"]
            print(f"\n{'='*50}")
            print(f"{'数据统计':^48}")
            print(f"{'='*50}")
            print(f"\n📝 博文统计:")
            print(f"  ├─ 总博文数: {stats.get('total_blogs', 0)}")
            print(f"  └─ 已处理博文数: {stats.get('processed_blogs', 0)}")
            
            if "engagement_statistics" in stats:
                eng_stats = stats["engagement_statistics"]
                print(f"\n💬 互动统计:")
                print(f"  ├─ 平均转发数: {eng_stats.get('avg_reposts', 0):.2f}")
                print(f"  ├─ 平均评论数: {eng_stats.get('avg_comments', 0):.2f}")
                print(f"  └─ 平均点赞数: {eng_stats.get('avg_likes', 0):.2f}")
            
            if "empty_fields" in stats:
                empty_stats = stats["empty_fields"]
                print(f"\n⚠️  空字段统计:")
                print(f"  ├─ 情感极性为空: {empty_stats.get('sentiment_polarity_empty', 0)}")
                print(f"  ├─ 情感属性为空: {empty_stats.get('sentiment_attribute_empty', 0)}")
                print(f"  ├─ 主题为空: {empty_stats.get('topics_empty', 0)}")
                print(f"  └─ 发布者为空: {empty_stats.get('publisher_empty', 0)}")
        
        # 显示保存状态
        if "data_save" in shared_data.get("results", {}):
            save_stats = shared_data["results"]["data_save"]
            print(f"\n{'='*50}")
            print(f"{'数据保存':^48}")
            print(f"{'='*50}")
            if save_stats.get("saved", False):
                print(f"\n✅ 数据已成功保存")
                print(f"  ├─ 保存路径: {save_stats.get('output_path', 'N/A')}")
                print(f"  └─ 保存数量: {save_stats.get('data_count', 0)} 条")
            else:
                print(f"\n❌ 数据保存失败: {save_stats.get('error', '未知错误')}")
    
    except Exception as e:
        # 即使出错也显示时间统计
        error_time = time.time()
        elapsed_time = error_time - analysis_start_time
        total_elapsed = error_time - program_start_time
        
        print(f"\n{'='*50}")
        print(f"{'流程执行出错':^48}")
        print(f"{'='*50}")
        print(f"\n❌ 错误信息: {str(e)}")
        print(f"\n⏱️  时间统计:")
        print(f"  ├─ 程序运行时间: {total_elapsed:.2f} 秒")
        print(f"  └─ 分析运行时间: {elapsed_time:.2f} 秒")
        print(f"\n详细错误堆栈:")
        import traceback
        traceback.print_exc()
    
    finally:
        # 最终清理和总结
        print(f"\n{'='*50}")
        print(f"程序结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
