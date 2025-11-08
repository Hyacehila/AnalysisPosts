"""
数据整合工具

将批处理结果与原始博文数据整合，生成增强后的博文数据
支持多文件结果合并
"""

import json
import os
from typing import List, Dict, Any, Optional
from .result_parser import parse_multiple_result_files, find_result_files, validate_result_completeness


def integrate_analysis_results(posts: List[Dict[str, Any]],
                         sentiment_polarity_results: Optional[Dict[int, Any]] = None,
                         sentiment_attribute_results: Optional[Dict[int, Any]] = None,
                         topic_analysis_results: Optional[Dict[int, Any]] = None,
                         publisher_analysis_results: Optional[Dict[int, Any]] = None) -> List[Dict[str, Any]]:
    """
    整合所有分析结果到原始博文数据
    
    Args:
        posts: 原始博文数据
        sentiment_polarity_results: 情感极性分析结果映射
        sentiment_attribute_results: 情感属性分析结果映射
        topic_analysis_results: 主题分析结果映射
        publisher_analysis_results: 发布者对象分析结果映射
        
    Returns:
        增强后的博文数据
    """
    enhanced_posts = []
    
    for i, post in enumerate(posts):
        # 复制原始博文数据
        enhanced_post = post.copy()
        
        # 添加情感极性分析结果
        if sentiment_polarity_results and i in sentiment_polarity_results:
            enhanced_post['sentiment_polarity'] = sentiment_polarity_results[i]
        else:
            enhanced_post['sentiment_polarity'] = None
        
        # 添加情感属性分析结果
        if sentiment_attribute_results and i in sentiment_attribute_results:
            enhanced_post['sentiment_attribute'] = sentiment_attribute_results[i]
        else:
            enhanced_post['sentiment_attribute'] = None
        
        # 添加主题分析结果
        if topic_analysis_results and i in topic_analysis_results:
            enhanced_post['topics'] = topic_analysis_results[i]
        else:
            enhanced_post['topics'] = None
        
        # 添加发布者对象分析结果
        if publisher_analysis_results and i in publisher_analysis_results:
            enhanced_post['publisher'] = publisher_analysis_results[i]
        else:
            enhanced_post['publisher'] = None
        
        enhanced_posts.append(enhanced_post)
    
    return enhanced_posts


def load_and_integrate_all_results(posts_file: str,
                                 temp_dir: str,
                                 topics_hierarchy: List[Dict[str, Any]],
                                 sentiment_attributes: List[str],
                                 publisher_objects: List[str]) -> Dict[str, Any]:
    """
    加载并整合所有批处理结果（支持多文件）
    
    Args:
        posts_file: 原始博文数据文件路径
        temp_dir: 临时文件目录（包含结果文件）
        topics_hierarchy: 主题层次结构
        sentiment_attributes: 情感属性列表
        publisher_objects: 发布者对象列表
        
    Returns:
        整合结果和统计信息
    """
    # 加载原始博文数据
    try:
        with open(posts_file, 'r', encoding='utf-8') as f:
            posts = json.load(f)
    except Exception as e:
        print(f"加载博文数据失败: {e}")
        return {"error": str(e)}
    
    print(f"加载博文数据: {len(posts)} 条")
    
    # 查找并解析各种分析结果文件
    sentiment_polarity_results = None
    sentiment_attribute_results = None
    topic_analysis_results = None
    publisher_analysis_results = None
    
    # 情感极性分析结果
    sentiment_polarity_files = find_result_files(temp_dir, "sentiment_polarity")
    if sentiment_polarity_files:
        print(f"找到情感极性结果文件: {len(sentiment_polarity_files)} 个")
        sentiment_polarity_results = parse_multiple_result_files(
            sentiment_polarity_files, "sentiment_polarity"
        )
    
    # 情感属性分析结果
    sentiment_attribute_files = find_result_files(temp_dir, "sentiment_attribute")
    if sentiment_attribute_files:
        print(f"找到情感属性结果文件: {len(sentiment_attribute_files)} 个")
        sentiment_attribute_results = parse_multiple_result_files(
            sentiment_attribute_files, "sentiment_attribute",
            sentiment_attributes=sentiment_attributes
        )
    
    # 主题分析结果
    topic_analysis_files = find_result_files(temp_dir, "topic_analysis")
    if topic_analysis_files:
        print(f"找到主题分析结果文件: {len(topic_analysis_files)} 个")
        topic_analysis_results = parse_multiple_result_files(
            topic_analysis_files, "topic_analysis",
            topics_hierarchy=topics_hierarchy
        )
    
    # 发布者对象分析结果
    publisher_analysis_files = find_result_files(temp_dir, "publisher_analysis")
    if publisher_analysis_files:
        print(f"找到发布者对象结果文件: {len(publisher_analysis_files)} 个")
        publisher_analysis_results = parse_multiple_result_files(
            publisher_analysis_files, "publisher_analysis",
            publisher_objects=publisher_objects
        )
    
    # 整合结果
    enhanced_posts = integrate_analysis_results(
        posts,
        sentiment_polarity_results,
        sentiment_attribute_results,
        topic_analysis_results,
        publisher_analysis_results
    )
    
    # 生成统计信息
    integration_stats = {
        "total_posts": len(posts),
        "sentiment_polarity": validate_result_completeness(
            posts, sentiment_polarity_results or {}, "sentiment_polarity"
        ) if sentiment_polarity_results else {"status": "not_processed"},
        "sentiment_attribute": validate_result_completeness(
            posts, sentiment_attribute_results or {}, "sentiment_attribute"
        ) if sentiment_attribute_results else {"status": "not_processed"},
        "topic_analysis": validate_result_completeness(
            posts, topic_analysis_results or {}, "topic_analysis"
        ) if topic_analysis_results else {"status": "not_processed"},
        "publisher_analysis": validate_result_completeness(
            posts, publisher_analysis_results or {}, "publisher_analysis"
        ) if publisher_analysis_results else {"status": "not_processed"}
    }
    
    return {
        "enhanced_posts": enhanced_posts,
        "integration_stats": integration_stats,
        "result_maps": {
            "sentiment_polarity": sentiment_polarity_results,
            "sentiment_attribute": sentiment_attribute_results,
            "topic_analysis": topic_analysis_results,
            "publisher_analysis": publisher_analysis_results
        },
        "source_files": {
            "sentiment_polarity": sentiment_polarity_files,
            "sentiment_attribute": sentiment_attribute_files,
            "topic_analysis": topic_analysis_files,
            "publisher_analysis": publisher_analysis_files
        }
    }


def save_enhanced_posts(enhanced_posts: List[Dict[str, Any]], output_path: str) -> bool:
    """
    保存增强后的博文数据
    
    Args:
        enhanced_posts: 增强后的博文数据
        output_path: 输出文件路径
        
    Returns:
        保存是否成功
    """
    try:
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(enhanced_posts, f, ensure_ascii=False, indent=2)
        
        print(f"成功保存 {len(enhanced_posts)} 条增强博文数据到: {output_path}")
        return True
    except Exception as e:
        print(f"保存增强博文数据失败: {e}")
        return False


def generate_integration_report(integration_stats: Dict[str, Any]) -> str:
    """
    生成整合报告
    
    Args:
        integration_stats: 整合统计信息
        
    Returns:
        报告文本
    """
    report = []
    report.append("=" * 60)
    report.append("数据整合报告（多文件合并）")
    report.append("=" * 60)
    
    total_posts = integration_stats.get("total_posts", 0)
    report.append(f"总博文数量: {total_posts}")
    report.append("")
    
    # 各分析类型的统计
    analysis_types = [
        ("sentiment_polarity", "情感极性分析"),
        ("sentiment_attribute", "情感属性分析"),
        ("topic_analysis", "主题分析"),
        ("publisher_analysis", "发布者对象分析")
    ]
    
    for analysis_key, analysis_name in analysis_types:
        stats = integration_stats.get(analysis_key, {})
        if stats.get("status") == "not_processed":
            report.append(f"{analysis_name}: 未处理")
        else:
            processed = stats.get("processed_posts", 0)
            completeness = stats.get("completeness_rate", 0)
            report.append(f"{analysis_name}: {processed}/{total_posts} ({completeness}%)")
            
            # 显示缺失的索引（如果有）
            missing_indices = stats.get("missing_indices", [])
            if missing_indices and len(missing_indices) <= 10:
                report.append(f"  缺失索引: {missing_indices}")
            elif missing_indices:
                report.append(f"  缺失索引: {missing_indices[:10]}... (共{len(missing_indices)}个)")
    
    report.append("")
    
    # 计算整体完整性
    processed_types = 0
    total_completeness = 0
    
    for analysis_key, _ in analysis_types:
        stats = integration_stats.get(analysis_key, {})
        if stats.get("status") != "not_processed":
            processed_types += 1
            total_completeness += stats.get("completeness_rate", 0)
    
    if processed_types > 0:
        overall_completeness = total_completeness / processed_types
        report.append(f"整体完整性: {overall_completeness:.2f}%")
    
    return "\n".join(report)


def validate_enhanced_posts(enhanced_posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    验证增强数据的完整性和正确性
    
    Args:
        enhanced_posts: 增强后的博文数据
        
    Returns:
        验证结果
    """
    validation_result = {
        "total_posts": len(enhanced_posts),
        "field_completeness": {},
        "data_quality": {},
        "issues": []
    }
    
    if not enhanced_posts:
        validation_result["issues"].append("增强数据为空")
        return validation_result
    
    # 检查各字段的完整性
    fields = ["sentiment_polarity", "sentiment_attribute", "topics", "publisher"]
    
    for field in fields:
        count = sum(1 for post in enhanced_posts if post.get(field) is not None)
        completeness = (count / len(enhanced_posts)) * 100
        validation_result["field_completeness"][field] = {
            "count": count,
            "completeness": round(completeness, 2)
        }
    
    # 检查数据质量
    sentiment_polarity_valid = 0
    sentiment_attribute_valid = 0
    topics_valid = 0
    publisher_valid = 0
    
    for post in enhanced_posts:
        # 检查情感极性
        sentiment_polarity = post.get("sentiment_polarity")
        if sentiment_polarity is not None and isinstance(sentiment_polarity, int) and 1 <= sentiment_polarity <= 5:
            sentiment_polarity_valid += 1
        
        # 检查情感属性
        sentiment_attribute = post.get("sentiment_attribute")
        if sentiment_attribute is not None and isinstance(sentiment_attribute, list) and len(sentiment_attribute) > 0:
            sentiment_attribute_valid += 1
        
        # 检查主题
        topics = post.get("topics")
        if topics is not None and isinstance(topics, list):
            topics_valid += 1
        
        # 检查发布者
        publisher = post.get("publisher")
        if publisher is not None and isinstance(publisher, str) and publisher.strip():
            publisher_valid += 1
    
    validation_result["data_quality"] = {
        "sentiment_polarity_valid": sentiment_polarity_valid,
        "sentiment_attribute_valid": sentiment_attribute_valid,
        "topics_valid": topics_valid,
        "publisher_valid": publisher_valid
    }
    
    # 检查问题
    for field in fields:
        completeness = validation_result["field_completeness"][field]["completeness"]
        if completeness < 50:
            validation_result["issues"].append(f"{field}分析完整性过低")
    
    return validation_result


def save_integration_report(integration_stats: Dict[str, Any], validation_result: Dict[str, Any], output_path: str) -> bool:
    """
    保存整合报告
    
    Args:
        integration_stats: 整合统计信息
        validation_result: 验证结果
        output_path: 报告输出路径
        
    Returns:
        保存是否成功
    """
    try:
        report_data = {
            "integration_stats": integration_stats,
            "validation_result": validation_result,
            "report_text": generate_integration_report(integration_stats)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        print(f"整合报告已保存到: {output_path}")
        return True
    except Exception as e:
        print(f"保存整合报告失败: {e}")
        return False


# 保持向后兼容的旧函数
def load_and_integrate_all_results_legacy(posts_file: str,
                                     sentiment_polarity_file: str,
                                     sentiment_attribute_file: str,
                                     topic_analysis_file: str,
                                     publisher_analysis_file: str,
                                     topics_hierarchy: List[Dict[str, Any]],
                                     sentiment_attributes: List[str],
                                     publisher_objects: List[str]) -> Dict[str, Any]:
    """加载并整合所有批处理结果（向后兼容）"""
    from .result_parser import parse_results_with_mapping
    
    # 加载原始博文数据
    try:
        with open(posts_file, 'r', encoding='utf-8') as f:
            posts = json.load(f)
    except Exception as e:
        print(f"加载博文数据失败: {e}")
        return {"error": str(e)}
    
    # 加载各种分析结果
    sentiment_polarity_results = None
    sentiment_attribute_results = None
    topic_analysis_results = None
    publisher_analysis_results = None
    
    # 情感极性分析结果
    if os.path.exists(sentiment_polarity_file):
        sentiment_polarity_results = parse_results_with_mapping(
            sentiment_polarity_file, "sentiment_polarity"
        )
    
    # 情感属性分析结果
    if os.path.exists(sentiment_attribute_file):
        sentiment_attribute_results = parse_results_with_mapping(
            sentiment_attribute_file, "sentiment_attribute",
            sentiment_attributes=sentiment_attributes
        )
    
    # 主题分析结果
    if os.path.exists(topic_analysis_file):
        topic_analysis_results = parse_results_with_mapping(
            topic_analysis_file, "topic_analysis",
            topics_hierarchy=topics_hierarchy
        )
    
    # 发布者对象分析结果
    if os.path.exists(publisher_analysis_file):
        publisher_analysis_results = parse_results_with_mapping(
            publisher_analysis_file, "publisher_analysis",
            publisher_objects=publisher_objects
        )
    
    # 整合结果
    enhanced_posts = integrate_analysis_results(
        posts,
        sentiment_polarity_results,
        sentiment_attribute_results,
        topic_analysis_results,
        publisher_analysis_results
    )
    
    # 生成统计信息
    integration_stats = {
        "total_posts": len(posts),
        "sentiment_polarity": validate_result_completeness(
            posts, sentiment_polarity_results or {}, "sentiment_polarity"
        ) if sentiment_polarity_results else {"status": "not_processed"},
        "sentiment_attribute": validate_result_completeness(
            posts, sentiment_attribute_results or {}, "sentiment_attribute"
        ) if sentiment_attribute_results else {"status": "not_processed"},
        "topic_analysis": validate_result_completeness(
            posts, topic_analysis_results or {}, "topic_analysis"
        ) if topic_analysis_results else {"status": "not_processed"},
        "publisher_analysis": validate_result_completeness(
            posts, publisher_analysis_results or {}, "publisher_analysis"
        ) if publisher_analysis_results else {"status": "not_processed"}
    }
    
    return {
        "enhanced_posts": enhanced_posts,
        "integration_stats": integration_stats,
        "result_maps": {
            "sentiment_polarity": sentiment_polarity_results,
            "sentiment_attribute": sentiment_attribute_results,
            "topic_analysis": topic_analysis_results,
            "publisher_analysis": publisher_analysis_results
        }
    }


if __name__ == "__main__":
    print("数据整合工具模块加载成功")
