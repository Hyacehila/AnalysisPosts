"""
解析和整合批处理结果脚本

解析下载的批处理结果文件，将结果与原始博文数据整合
支持多文件结果合并
"""

import os
import sys
import json
from utils.data_integration import (
    load_and_integrate_all_results,
    save_enhanced_posts,
    validate_enhanced_posts,
    save_integration_report
)


def load_reference_data():
    """加载参考数据"""
    data_dir = "data"
    
    try:
        # 加载主题层次结构
        with open(os.path.join(data_dir, "topics.json"), 'r', encoding='utf-8') as f:
            topics_hierarchy = json.load(f)
        
        # 加载情感属性
        with open(os.path.join(data_dir, "sentiment_attributes.json"), 'r', encoding='utf-8') as f:
            sentiment_attributes = json.load(f)
        
        # 加载发布者对象
        with open(os.path.join(data_dir, "publisher_objects.json"), 'r', encoding='utf-8') as f:
            publisher_objects = json.load(f)
        
        return topics_hierarchy, sentiment_attributes, publisher_objects
        
    except Exception as e:
        print(f"加载参考数据失败: {e}")
        return [], [], []


def check_result_files(temp_dir: str):
    """检查结果文件是否存在"""
    required_patterns = [
        "sentiment_polarity_results*.jsonl",
        "sentiment_attribute_results*.jsonl", 
        "topic_analysis_results*.jsonl",
        "publisher_analysis_results*.jsonl"
    ]
    
    import glob
    found_files = {}
    
    for pattern in required_patterns:
        full_pattern = os.path.join(temp_dir, pattern)
        files = glob.glob(full_pattern)
        found_files[pattern] = files
    
    print("结果文件检查:")
    for pattern, files in found_files.items():
        if files:
            print(f"  ✓ {pattern}: {len(files)} 个文件")
            for file in files:
                print(f"    - {os.path.basename(file)}")
        else:
            print(f"  ✗ {pattern}: 未找到")
    
    return found_files


def main():
    """主函数"""
    print("=" * 60)
    print("解析和整合批处理结果（支持多文件合并）")
    print("=" * 60)
    
    # 检查结果文件
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    found_files = check_result_files(temp_dir)
    
    # 检查是否有任何结果文件
    has_any_results = any(len(files) > 0 for files in found_files.values())
    if not has_any_results:
        print("\n未找到任何结果文件")
        print("请先运行 upload_and_start.py 完成批处理任务")
        return
    
    # 加载参考数据
    topics_hierarchy, sentiment_attributes, publisher_objects = load_reference_data()
    
    if not topics_hierarchy:
        print("参考数据加载失败，程序退出")
        return
    
    print(f"\n加载参考数据:")
    print(f"  主题层次结构: {len(topics_hierarchy)} 个父主题")
    print(f"  情感属性: {len(sentiment_attributes)} 个")
    print(f"  发布者对象: {len(publisher_objects)} 个")
    
    # 整合结果
    print(f"\n开始整合结果...")
    
    try:
        integration_result = load_and_integrate_all_results(
            posts_file="data/posts.json",
            temp_dir=temp_dir,
            topics_hierarchy=topics_hierarchy,
            sentiment_attributes=sentiment_attributes,
            publisher_objects=publisher_objects
        )
        
        if "error" in integration_result:
            print(f"整合失败: {integration_result['error']}")
            return
        
        enhanced_posts = integration_result["enhanced_posts"]
        integration_stats = integration_result["integration_stats"]
        source_files = integration_result["source_files"]
        
        print(f"\n" + "=" * 60)
        print("结果整合完成")
        print("=" * 60)
        
        # 显示整合统计
        print(f"原始博文数量: {integration_stats['total_posts']}")
        print(f"增强博文数量: {len(enhanced_posts)}")
        print()
        
        # 显示各分析类型的统计
        analysis_types = [
            ("sentiment_polarity", "情感极性分析"),
            ("sentiment_attribute", "情感属性分析"),
            ("topic_analysis", "主题分析"),
            ("publisher_analysis", "发布者对象分析")
        ]
        
        for analysis_key, analysis_name in analysis_types:
            stats = integration_stats.get(analysis_key, {})
            if stats.get("status") == "not_processed":
                print(f"{analysis_name}: 未处理")
            else:
                processed = stats.get("processed_posts", 0)
                completeness = stats.get("completeness_rate", 0)
                print(f"{analysis_name}: {processed}/{integration_stats['total_posts']} ({completeness}%)")
                
                # 显示源文件信息
                files = source_files.get(analysis_key, [])
                if files:
                    print(f"  源文件: {len(files)} 个")
                    for file in files:
                        print(f"    - {os.path.basename(file)}")
        
        # 验证增强数据
        print(f"\n验证增强数据...")
        validation_result = validate_enhanced_posts(enhanced_posts)
        
        print(f"验证结果:")
        print(f"  总博文数: {validation_result['total_posts']}")
        
        for field in ["sentiment_polarity", "sentiment_attribute", "topics", "publisher"]:
            field_stats = validation_result["field_completeness"].get(field, {})
            count = field_stats.get("count", 0)
            completeness = field_stats.get("completeness", 0)
            print(f"  {field}: {count} 个 ({completeness}%)")
        
        if validation_result["issues"]:
            print(f"  发现问题: {len(validation_result['issues'])} 个")
            for issue in validation_result["issues"]:
                print(f"    - {issue}")
        else:
            print("  未发现问题")
        
        # 保存增强数据
        output_dir = "data"
        enhanced_posts_path = os.path.join(output_dir, "enhanced_posts.json")
        
        if save_enhanced_posts(enhanced_posts, enhanced_posts_path):
            print(f"\n✓ 增强博文数据已保存到: {enhanced_posts_path}")
        else:
            print(f"\n✗ 保存增强博文数据失败")
            return
        
        # 保存整合报告
        report_path = os.path.join(output_dir, "integration_report.json")
        if save_integration_report(integration_stats, validation_result, report_path):
            print(f"✓ 整合报告已保存到: {report_path}")
        else:
            print(f"\n✗ 保存整合报告失败")
        
        # 显示最终统计
        print(f"\n" + "=" * 60)
        print("处理完成统计")
        print("=" * 60)
        
        total_processed = 0
        total_possible = len(enhanced_posts) * 4  # 4种分析类型
        
        for analysis_key, _ in analysis_types:
            stats = integration_stats.get(analysis_key, {})
            if stats.get("status") != "not_processed":
                total_processed += stats.get("processed_posts", 0)
        
        overall_success_rate = (total_processed / total_possible * 100) if total_possible > 0 else 0
        
        print(f"总体处理成功率: {overall_success_rate:.2f}%")
        print(f"成功处理的分析结果: {total_processed}/{total_possible}")
        
        if overall_success_rate >= 90:
            print("✓ 数据整合质量良好")
        elif overall_success_rate >= 70:
            print("⚠ 数据整合质量一般，建议检查缺失数据")
        else:
            print("✗ 数据整合质量较差，建议重新处理")
        
    except Exception as e:
        print(f"整合过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()
