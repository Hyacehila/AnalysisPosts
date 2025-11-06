import json
import os
from typing import List, Dict, Any
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_blog_data(data_file_path: str) -> List[Dict[str, Any]]:
    """
    加载博文数据
    
    Args:
        data_file_path: 博文数据文件路径
        
    Returns:
        List[Dict[str, Any]]: 博文数据列表
    """
    try:
        with open(data_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"成功加载博文数据，共 {len(data)} 条记录")
        return data
        
    except Exception as e:
        logger.error(f"加载博文数据失败: {e}")
        raise

def load_topics(topics_file_path: str) -> List[Dict[str, Any]]:
    """
    加载主题数据
    
    Args:
        topics_file_path: 主题数据文件路径
        
    Returns:
        List[Dict[str, Any]]: 主题数据列表，每个元素包含parent_topic和sub_topics
    """
    try:
        with open(topics_file_path, 'r', encoding='utf-8') as f:
            topics = json.load(f)
        
        logger.info(f"成功加载主题数据，共 {len(topics)} 个父主题")
        return topics
        
    except Exception as e:
        logger.error(f"加载主题数据失败: {e}")
        raise

def load_sentiment_attributes(sentiment_attributes_file_path: str) -> List[str]:
    """
    加载情感属性数据
    
    Args:
        sentiment_attributes_file_path: 情感属性文件路径
        
    Returns:
        List[str]: 情感属性列表
    """
    try:
        with open(sentiment_attributes_file_path, 'r', encoding='utf-8') as f:
            attributes = json.load(f)
        
        logger.info(f"成功加载情感属性数据，共 {len(attributes)} 个")
        return attributes
        
    except Exception as e:
        logger.error(f"加载情感属性数据失败: {e}")
        raise

def load_publisher_objects(publisher_objects_file_path: str) -> List[str]:
    """
    加载发布者对象数据
    
    Args:
        publisher_objects_file_path: 发布者对象文件路径
        
    Returns:
        List[str]: 发布者对象列表
    """
    try:
        with open(publisher_objects_file_path, 'r', encoding='utf-8') as f:
            objects = json.load(f)
        
        logger.info(f"成功加载发布者对象数据，共 {len(objects)} 个")
        return objects
        
    except Exception as e:
        logger.error(f"加载发布者对象数据失败: {e}")
        raise

def save_enhanced_blog_data(enhanced_blog_data: List[Dict[str, Any]], output_path: str) -> bool:
    """
    保存增强后的博文数据
    
    Args:
        enhanced_blog_data: 增强后的博文数据
        output_path: 输出文件路径
        
    Returns:
        bool: 保存是否成功
    """
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 保存数据
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(enhanced_blog_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"成功保存增强博文数据到 {output_path}，共 {len(enhanced_blog_data)} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"保存增强博文数据失败: {e}")
        return False

def load_enhanced_blog_data(enhanced_data_path: str) -> List[Dict[str, Any]]:
    """
    加载增强后的博文数据
    
    Args:
        enhanced_data_path: 增强数据文件路径
        
    Returns:
        List[Dict[str, Any]]: 增强博文数据列表
    """
    try:
        with open(enhanced_data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"成功加载增强博文数据，共 {len(data)} 条记录")
        return data
        
    except Exception as e:
        logger.error(f"加载增强博文数据失败: {e}")
        raise

