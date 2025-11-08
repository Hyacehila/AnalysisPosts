"""
生成JSONL文件脚本

从data文件夹加载博文数据和配置文件，生成四个对应的JSONL文件
"""

import os
import sys
import json
from utils.jsonl_generator import create_all_jsonl_files


def load_data_files():
    """加载所有必要的数据文件"""
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
        print(f"加载数据文件失败: {e}")
        return None, None, None, None


def main():
    """主函数"""
    print("=" * 50)
    print("生成JSONL文件")
    print("=" * 50)
    
    # 加载数据
    posts, topics_hierarchy, sentiment_attributes, publisher_objects = load_data_files()
    
    if posts is None:
        print("数据加载失败，程序退出")
        return
    
    print(f"加载博文数据: {len(posts)} 条")
    print(f"加载主题层次结构: {len(topics_hierarchy)} 个父主题")
    print(f"加载情感属性: {len(sentiment_attributes)} 个")
    print(f"加载发布者对象: {len(publisher_objects)} 个")
    print()
    
    # 创建临时目录
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    os.makedirs(temp_dir, exist_ok=True)
    
    # 生成JSONL文件
    try:
        jsonl_files = create_all_jsonl_files(
            posts=posts,
            topics=topics_hierarchy,
            sentiment_attributes=sentiment_attributes,
            publishers=publisher_objects,
            data_dir="data",
            temp_dir=temp_dir
        )
        
        print("\n" + "=" * 50)
        print("JSONL文件生成完成")
        print("=" * 50)
        
        for analysis_type, file_path in jsonl_files.items():
            file_size = os.path.getsize(file_path)
            print(f"{analysis_type}: {file_path} ({file_size} bytes)")
        
        print(f"\n所有JSONL文件已保存到: {temp_dir}/")
        print("可以继续执行 upload_and_start.py")
        
    except Exception as e:
        print(f"生成JSONL文件失败: {e}")
        return


if __name__ == "__main__":
    main()
