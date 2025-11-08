"""
解析并整合结果脚本

解析批处理结果文件，将结果与原始博文数据整合，生成增强后的博文数据
"""

import os
import sys
import json
from utils.result_parser import load_reference_data
from utils.data_integration import (
    load_and_integrate_all_results,
    save_enhanced_posts,
    generate_integration_report,
    validate_enhanced_posts
)


def get_api_key():
    """获取API密钥"""
    api_key = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"
    
    return api_key


def check_result_files():
    """检查结果文件是否存在"""
    temp_dir = "batch/temp"
    result_files = {
        "sentiment_polarity": os.path.join(temp_dir, "sentiment_polarity_results.jsonl"),
        "sentiment_attribute": os.path.join(temp_dir, "sentiment_attribute_results.jsonl"),
        "topic_analysis": os.path.join(temp_dir, "topic_analysis_results.jsonl"),
        "publisher_analysis": os.path.join(temp_dir, "publisher_analysis_results.jsonl")
    }
    
    missing_files = []
    for analysis_type, file_path in result_files.items():
        if not os.path.exists(file_path):
            missing_files.append(f"{analysis_type}: {file_path}")
    
    if missing_files:
        print("以下结果文件不存在:")
        for file_info in missing_files:
            print(f"  - {file_info}")
        print("\n请先运行 download_results.py 下载结果文件")
        return None
    
    return result_files


def load_reference_data_safe():
    """安全加载参考数据"""
    data_dir = "data"
    
    try:
        # 加载博文数据
        with open(os.path.join(data_dir, "posts.json"), 'r', encoding='utf-8') as f:
            posts = json.load(f)
        
        # 加载主题层次结构
        with open(os.path.join(data_dir, "topics.json"), 'r', encoding='utf-8') as f:
            topics_hierarchy = json.load(f)
        
        # 加载情感属性
        with open(os.path.join(data_dir, "sentiment_attributes.json"), 'r', encoding='utf-8') as f:
            sentiment_attributes = json.load(f)
        
        # 加载发布者对象
        with open(os.path.join(data_dir, "publisher_objects.json"), 'r', encoding='utf-8') as f:
            publisher_objects = json.load(f)
        
        return posts, topics_hierarchy, sentiment_attributes, publisher_objects
        
    except Exception as e:
        print(f"加载参考数据失败: {e}")
        return None, None, None, None


def load_batch_info():
    """加载批处理信息"""
    temp_dir = "batch/temp"
    info_file = os.path.join(temp_dir, "batch_info.json")
    
    if not os.path.exists(info_file):
        print(f"批处理信息文件不存在: {info_file}")
        print("请先运行 upload_and_start.py 创建批处理任务")
        return None
    
    try:
        with open(info_file, 'r', encoding='utf-8') as f:
            batch_info = json.load(f)
        
        return batch_info
        
    except Exception as e:
        print(f"加载批处理信息失败: {e}")
        return None


def parse_and_integrate():
    """解析并整合结果"""
    print("加载参考数据...")
    
    # 加载参考数据
    posts, topics_hierarchy, sentiment_attributes, publisher_objects = load_reference_data_safe()
    
    if posts is None:
        print("参考数据加载失败，程序退出")
        return None
    
    print(f"加载博文数据: {len(posts)} 条")
    print(f"加载主题层次结构: {len(topics_hierarchy)} 个父主题")
    print(f"加载情感属性: {len(sentiment_attributes)} 个")
    print(f"加载发布者对象: {len(publisher_objects)} 个")
    print()
    
    # 检查结果文件
    result_files = check_result_files()
    if not result_files:
        return None
    
    print(f"找到 {len(result_files)} 个结果文件:")
    for analysis_type, file_path in result_files.items():
        file_size = os.path.getsize(file_path)
        print(f"  {analysis_type}: {file_path} ({file_size} bytes)")
    print()
    
    # 加载批处理信息用于报告
    batch_info = load_batch_info()
    if batch_info:
        print(f"批处理任务信息:")
        for analysis_type, info in batch_info.items():
            batch_id = info.get("batch_id")
            status = info.get("status", "unknown")
            print(f"  {analysis_type}: {batch_id} ({status})")
        print()
    
    # 加载并整合所有结果
    print("开始解析并整合结果...")
    
    integration_result = load_and_integrate_all_results(
        posts_file="data/posts.json",
        sentiment_polarity_file=result_files["sentiment_polarity"],
        sentiment_attribute_file=result_files["sentiment_attribute"],
        topic_analysis_file=result_files["topic_analysis"],
        publisher_analysis_file=result_files["publisher_analysis"],
        topics_hierarchy=topics_hierarchy,
        sentiment_attributes=sentiment_attributes,
        publisher_objects=publisher_objects
    )
    
    if "error" in integration_result:
        print(f"整合失败: {integration_result['error']}")
        return None
    
    enhanced_posts = integration_result.get("enhanced_posts", [])
    integration_stats = integration_result.get("integration_stats", {})
    
    print("结果整合完成!")
    print()
    
    # 生成整合报告
    report = generate_integration_report(integration_stats)
    print(report)
    print()
    
    # 验证增强数据
    print("验证增强数据...")
    validation_result = validate_enhanced_posts(enhanced_posts)
    
    print("数据验证结果:")
    print(f"  总博文数: {validation_result['total_posts']}")
    
    field_completeness = validation_result.get("field_completeness", {})
    for field, stats in field_completeness.items():
        count = stats.get("count", 0)
        completeness = stats.get("completeness", 0)
        print(f"  {field}: {count} 条 ({completeness}%)")
    
    data_quality = validation_result.get("data_quality", {})
    for field, count in data_quality.items():
        print(f"  {field}_valid: {count}")
    
    issues = validation_result.get("issues", [])
    if issues:
        print("  发现的问题:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  未发现明显问题")
    print()
    
    # 保存增强数据
    output_path = "batch/enhanced_posts.json"
    print(f"保存增强数据到: {output_path}")
    
    if save_enhanced_posts(enhanced_posts, output_path):
        print("增强数据保存成功!")
        
        # 保存整合报告
        report_file = "batch/integration_report.txt"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"整合报告已保存到: {report_file}")
        except Exception as e:
            print(f"保存整合报告失败: {e}")
        
        # 验证结果仅在控制台显示，不保存到文件
        print("验证结果仅在控制台显示，未保存到文件")
        
        return enhanced_posts
    else:
        print("增强数据保存失败!")
        return None


def main():
    """主函数"""
    print("=" * 50)
    print("解析并整合批处理结果")
    print("=" * 50)
    
    # 解析并整合结果
    enhanced_posts = parse_and_integrate()
    
    if enhanced_posts is not None:
        print("\n" + "=" * 50)
        print("数据整合完成")
        print("=" * 50)
        
        print(f"最终增强博文数据: {len(enhanced_posts)} 条")
        print("输出文件: batch/enhanced_posts.json")
        print("报告文件: batch/integration_report.txt")
        
        print("\n数据整合流程已完成!")
        print("可以使用 batch/enhanced_posts.json 进行后续分析")
    else:
        print("\n数据整合失败，请检查错误信息")


if __name__ == "__main__":
    main()
