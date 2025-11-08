"""
批处理结果解析工具

解析智谱Batch API返回的JSONL格式结果文件，将结果映射回原始博文数据
支持四种分析类型的结果解析：情感极性、情感属性、主题分析、发布者对象
"""

import json
import os
import re
from typing import Dict, Any, List, Optional


def extract_index_from_custom_id(custom_id: str, analysis_type: str) -> Optional[int]:
    """从custom_id中提取原始博文索引"""
    try:
        # custom_id格式: {analysis_type}_{index:06d}
        pattern = f"{analysis_type}_(\\d+)"
        match = re.search(pattern, custom_id)
        
        if match:
            return int(match.group(1))
        
        return None
        
    except (ValueError, AttributeError):
        return None


def parse_sentiment_polarity_result(result_line: str) -> Optional[int]:
    """解析情感极性分析结果"""
    try:
        data = json.loads(result_line)
        
        # 检查响应状态
        if data.get("response", {}).get("status_code") != 200:
            return None
        
        # 提取内容
        content = data["response"]["body"]["choices"][0]["message"]["content"].strip()
        
        # 验证是否为单一数字
        if not content.isdigit():
            return None
        
        score = int(content)
        if not 1 <= score <= 5:
            return None
        
        return score
        
    except (json.JSONDecodeError, KeyError, IndexError, ValueError):
        return None


def parse_sentiment_attribute_result(result_line: str, sentiment_attributes: List[str]) -> Optional[List[str]]:
    """解析情感属性分析结果"""
    try:
        data = json.loads(result_line)
        
        # 检查响应状态
        if data.get("response", {}).get("status_code") != 200:
            return None
        
        # 提取内容
        content = data["response"]["body"]["choices"][0]["message"]["content"].strip()
        
        # 尝试解析JSON数组
        try:
            attributes = json.loads(content)
            if not isinstance(attributes, list):
                return None
        except json.JSONDecodeError:
            return None
        
        # 验证结果是否在预定义列表中
        valid_attributes = []
        for attr in attributes:
            if isinstance(attr, str) and attr in sentiment_attributes:
                valid_attributes.append(attr)
        
        return valid_attributes if valid_attributes else None
        
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def parse_topic_analysis_result(result_line: str, topics_hierarchy: List[Dict[str, Any]]) -> Optional[List[Dict[str, str]]]:
    """解析主题分析结果"""
    try:
        data = json.loads(result_line)
        
        # 检查响应状态
        if data.get("response", {}).get("status_code") != 200:
            return None
        
        # 提取内容
        content = data["response"]["body"]["choices"][0]["message"]["content"].strip()
        
        # 尝试解析JSON数组
        try:
            topics = json.loads(content)
            if not isinstance(topics, list):
                return None
        except json.JSONDecodeError:
            return None
        
        # 验证主题结构
        valid_topics = []
        valid_parent_topics = {topic["parent_topic"] for topic in topics_hierarchy}
        valid_sub_topics = {}
        for topic_group in topics_hierarchy:
            valid_sub_topics[topic_group["parent_topic"]] = set(topic_group["sub_topics"])
        
        for topic in topics:
            if not isinstance(topic, dict):
                continue
            
            parent_topic = topic.get("parent_topic")
            sub_topic = topic.get("sub_topic")
            
            if (parent_topic in valid_parent_topics and 
                sub_topic in valid_sub_topics.get(parent_topic, set())):
                valid_topics.append({
                    "parent_topic": parent_topic,
                    "sub_topic": sub_topic
                })
        
        return valid_topics if valid_topics else None
        
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def parse_publisher_analysis_result(result_line: str, publisher_objects: List[str]) -> Optional[str]:
    """解析发布者对象分析结果"""
    try:
        data = json.loads(result_line)
        
        # 检查响应状态
        if data.get("response", {}).get("status_code") != 200:
            return None
        
        # 提取内容
        content = data["response"]["body"]["choices"][0]["message"]["content"].strip()
        
        # 验证结果是否在预定义列表中
        if content in publisher_objects:
            return content
        
        return None
        
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def parse_results_with_mapping(result_file_path: str, analysis_type: str, **kwargs) -> Dict[int, Any]:
    """解析结果文件并生成索引映射"""
    result_mapping = {}
    
    if not os.path.exists(result_file_path):
        print(f"结果文件不存在: {result_file_path}")
        return result_mapping
    
    try:
        with open(result_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    custom_id = data.get("custom_id", "")
                    
                    # 提取索引
                    index = extract_index_from_custom_id(custom_id, analysis_type)
                    if index is None:
                        print(f"第{line_num}行：无法提取索引，custom_id: {custom_id}")
                        continue
                    
                    # 根据分析类型解析结果
                    if analysis_type == "sentiment_polarity":
                        result = parse_sentiment_polarity_result(line)
                    elif analysis_type == "sentiment_attribute":
                        sentiment_attributes = kwargs.get("sentiment_attributes", [])
                        result = parse_sentiment_attribute_result(line, sentiment_attributes)
                    elif analysis_type == "topic_analysis":
                        topics_hierarchy = kwargs.get("topics_hierarchy", [])
                        result = parse_topic_analysis_result(line, topics_hierarchy)
                    elif analysis_type == "publisher_analysis":
                        publisher_objects = kwargs.get("publisher_objects", [])
                        result = parse_publisher_analysis_result(line, publisher_objects)
                    else:
                        print(f"第{line_num}行：未知的分析类型 {analysis_type}")
                        continue
                    
                    if result is not None:
                        result_mapping[index] = result
                    else:
                        print(f"第{line_num}行：解析失败，索引: {index}")
                
                except json.JSONDecodeError as e:
                    print(f"第{line_num}行：JSON解析失败: {e}")
                    continue
    
    except Exception as e:
        print(f"读取结果文件失败: {e}")
    
    return result_mapping


def validate_result_completeness(posts: List[Dict[str, Any]], result_mapping: Dict[int, Any], analysis_type: str) -> Dict[str, Any]:
    """验证结果完整性"""
    total_posts = len(posts)
    processed_posts = len(result_mapping)
    completeness_rate = (processed_posts / total_posts * 100) if total_posts > 0 else 0
    
    # 找出缺失的索引
    missing_indices = []
    for i in range(total_posts):
        if i not in result_mapping:
            missing_indices.append(i)
    
    return {
        "analysis_type": analysis_type,
        "total_posts": total_posts,
        "processed_posts": processed_posts,
        "missing_posts": len(missing_indices),
        "completeness_rate": round(completeness_rate, 2),
        "missing_indices": missing_indices[:20],  # 只显示前20个缺失索引
        "status": "completed" if completeness_rate >= 90 else "incomplete"
    }


def load_reference_data(posts_file: str, topics_file: str, sentiment_attributes_file: str, publisher_objects_file: str) -> tuple:
    """加载参考数据"""
    try:
        # 加载博文数据
        with open(posts_file, 'r', encoding='utf-8') as f:
            posts = json.load(f)
        
        # 加载主题层次结构
        with open(topics_file, 'r', encoding='utf-8') as f:
            topics_hierarchy = json.load(f)
        
        # 加载情感属性
        with open(sentiment_attributes_file, 'r', encoding='utf-8') as f:
            sentiment_attributes = json.load(f)
        
        # 加载发布者对象
        with open(publisher_objects_file, 'r', encoding='utf-8') as f:
            publisher_objects = json.load(f)
        
        return posts, topics_hierarchy, sentiment_attributes, publisher_objects
        
    except Exception as e:
        print(f"加载参考数据失败: {e}")
        return [], [], [], []


if __name__ == "__main__":
    print("结果解析工具模块加载成功")
