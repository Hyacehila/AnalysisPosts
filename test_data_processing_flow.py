"""
数据处理流程测试

使用test_three_posts.json中的三条博文作为测试数据，
创建测试所有node能否正常运行的flow，包括：
1. 数据加载
2. 情感极性分析
3. 情感属性分析
4. 主题分析
5. 发布者分析
6. 数据保存
7. 数据验证与概况分析

以顺序的形式运行，每个创建的node示例都要使用max_retries参数允许exec步中出错
"""

from pocketflow import Flow
from nodes import (
    DataLoadNode, 
    SentimentPolarityAnalysisBatchNode,
    SentimentAttributeAnalysisBatchNode, 
    TwoLevelTopicAnalysisBatchNode,
    PublisherObjectAnalysisBatchNode,
    SaveEnhancedDataNode,
    DataValidationAndOverviewNode
)


def TestDataProcessingFlow():
    """
    测试数据处理流程
    顺序运行所有节点，验证功能完整性
    """
    
    # 创建节点实例，设置max_retries参数允许exec步中出错重试
    data_load_node = DataLoadNode()
    
    # 按顺序创建四个分析BatchNode，设置重试次数和等待时间
    sentiment_polarity_node = SentimentPolarityAnalysisBatchNode(max_retries=3, wait=10)
    sentiment_attribute_node = SentimentAttributeAnalysisBatchNode(max_retries=3, wait=10)
    topic_analysis_node = TwoLevelTopicAnalysisBatchNode(max_retries=3, wait=10)
    publisher_analysis_node = PublisherObjectAnalysisBatchNode(max_retries=3, wait=10)
    
    # 数据保存节点
    save_enhanced_data_node = SaveEnhancedDataNode(max_retries=3, wait=10)
    
    # 数据验证与概况分析节点
    data_validation_node = DataValidationAndOverviewNode(max_retries=3, wait=10)
    
    # 按顺序连接节点：
    # 数据加载 -> 情感极性分析 -> 情感属性分析 -> 主题分析 -> 发布者分析 -> 数据保存 -> 数据验证
    data_load_node >> sentiment_polarity_node
    sentiment_polarity_node >> sentiment_attribute_node
    sentiment_attribute_node >> topic_analysis_node
    topic_analysis_node >> publisher_analysis_node
    publisher_analysis_node >> save_enhanced_data_node
    save_enhanced_data_node >> data_validation_node
    
    # 创建流程，从数据加载开始
    flow = Flow(start=data_load_node)
    
    return flow


def main():
    """
    主函数：运行测试流程
    """
    
    # 配置shared数据结构
    shared = {
        "data": {
            "data_paths": {
                "blog_data_path": "data/test_three_posts.json",
                "topics_path": "data/topics.json",
                "sentiment_attributes_path": "data/sentiment_attributes.json",
                "publisher_objects_path": "data/publisher_objects.json"
            }
        },
        "config": {
            "data_source": {
                "type": "original",
                "enhanced_data_path": "data/test_enhanced_blogs.json"
            }
        },
        "results": {
            "statistics": {}
        }
    }
    
    print("开始测试数据处理流程...")
    print("=" * 50)
    
    try:
        # 创建并运行测试流程
        test_flow = TestDataProcessingFlow()
        
        print("流程创建成功，开始执行...")
        print("节点执行顺序：")
        print("1. DataLoadNode (数据加载)")
        print("2. SentimentPolarityAnalysisBatchNode (情感极性分析)")
        print("3. SentimentAttributeAnalysisBatchNode (情感属性分析)")
        print("4. TwoLevelTopicAnalysisBatchNode (主题分析)")
        print("5. PublisherObjectAnalysisBatchNode (发布者分析)")
        print("6. SaveEnhancedDataNode (数据保存)")
        print("7. DataValidationAndOverviewNode (数据验证与概况分析)")
        print("=" * 50)
        
        test_flow.run(shared)
        
        print("=" * 50)
        print("数据处理流程测试完成！")
        
        # 输出统计信息
        if "results" in shared and "statistics" in shared["results"]:
            stats = shared["results"]["statistics"]
            print(f"\n=== 数据统计概况 ===")
            print(f"总博文数: {stats.get('total_blogs', 0)}")
            print(f"处理成功数: {stats.get('processed_blogs', 0)}")
            
            # 输出空字段统计
            empty_fields = stats.get('empty_fields', {})
            print(f"\n=== 空字段统计 ===")
            print(f"情感极性为空: {empty_fields.get('sentiment_polarity_empty', 0)}")
            print(f"情感属性为空: {empty_fields.get('sentiment_attribute_empty', 0)}")
            print(f"主题分析为空: {empty_fields.get('topics_empty', 0)}")
            print(f"发布者分析为空: {empty_fields.get('publisher_empty', 0)}")
            
            # 输出保存状态
            if "data_save" in shared["results"]:
                save_info = shared["results"]["data_save"]
                print(f"\n=== 数据保存状态 ===")
                if save_info.get("saved", False):
                    print(f"保存成功: {save_info.get('data_count', 0)} 条数据")
                    print(f"保存路径: {save_info.get('output_path', '')}")
                else:
                    print("保存失败")
        
        # 输出增强后的数据样例
        if "data" in shared and "blog_data" in shared["data"]:
            blog_data = shared["data"]["blog_data"]
            print(f"\n=== 增强数据样例 ===")
            for i, blog in enumerate(blog_data[:3]):  # 只显示前3条
                print(f"\n博文 {i+1}:")
                print(f"用户名: {blog.get('username', '')}")
                print(f"情感极性: {blog.get('sentiment_polarity', 'None')}")
                print(f"情感属性: {blog.get('sentiment_attribute', 'None')}")
                print(f"主题分析: {blog.get('topics', 'None')}")
                print(f"发布者类型: {blog.get('publisher', 'None')}")
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
