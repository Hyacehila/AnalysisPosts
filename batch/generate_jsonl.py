"""
生成JSONL文件脚本

从data文件夹加载博文数据和配置文件，生成四个对应的JSONL文件
支持自动拆分以符合API限制（1万条/500MB）
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
        
        # 加载信念系统
        belief_system = None
        belief_system_path = os.path.join(data_dir, "believe_system_common.json")
        if os.path.exists(belief_system_path):
            with open(belief_system_path, 'r', encoding='utf-8') as f:
                belief_system = json.load(f)
        
        # 加载发布者事件关联身份分类
        publisher_decisions = None
        publisher_decision_path = os.path.join(data_dir, "publisher_decision.json")
        if os.path.exists(publisher_decision_path):
            with open(publisher_decision_path, 'r', encoding='utf-8') as f:
                publisher_decisions = json.load(f)
        
        return posts, topics_hierarchy, sentiment_attributes, publisher_objects, belief_system, publisher_decisions
        
    except Exception as e:
        print(f"加载数据文件失败: {e}")
        return None, None, None, None, None, None


def save_batch_info(jsonl_files: dict, temp_dir: str):
    """保存批处理文件信息"""
    info_file = os.path.join(temp_dir, "jsonl_files_info.json")
    
    try:
        # 计算文件统计信息
        file_stats = {}
        total_files = 0
        total_size = 0
        
        for analysis_type, file_paths in jsonl_files.items():
            file_stats[analysis_type] = {
                "file_count": len(file_paths),
                "files": []
            }
            
            for file_path in file_paths:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    file_stats[analysis_type]["files"].append({
                        "path": os.path.basename(file_path),
                        "size_bytes": size,
                        "size_mb": size / 1024 / 1024
                    })
                    total_files += 1
                    total_size += size
        
        # 保存信息
        batch_info = {
            "generation_time": os.path.getmtime(info_file) if os.path.exists(info_file) else None,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / 1024 / 1024,
            "analysis_types": file_stats
        }
        
        with open(info_file, 'w', encoding='utf-8') as f:
            json.dump(batch_info, f, ensure_ascii=False, indent=2)
        
        print(f"批处理文件信息已保存到: {info_file}")
        
    except Exception as e:
        print(f"保存批处理信息失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("生成JSONL文件（支持自动拆分）")
    print("=" * 60)
    
    # 加载数据
    posts, topics_hierarchy, sentiment_attributes, publisher_objects, belief_system, publisher_decisions = load_data_files()
    
    if posts is None:
        print("数据加载失败，程序退出")
        return
    
    print(f"加载博文数据: {len(posts)} 条")
    print(f"加载主题层次结构: {len(topics_hierarchy)} 个父主题")
    print(f"加载情感属性: {len(sentiment_attributes)} 个")
    print(f"加载发布者对象: {len(publisher_objects)} 个")
    if belief_system:
        print(f"加载信念系统: {len(belief_system)} 个类别")
    if publisher_decisions:
        print(f"加载关联身份分类: {len(publisher_decisions)} 个类别")
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
            temp_dir=temp_dir,
            belief_system=belief_system,
            publisher_decisions=publisher_decisions
        )
        
        print("\n" + "=" * 60)
        print("JSONL文件生成完成")
        print("=" * 60)
        
        # 显示文件信息
        total_files = 0
        total_size = 0
        
        for analysis_type, file_paths in jsonl_files.items():
            print(f"\n{analysis_type}: {len(file_paths)} 个文件")
            for file_path in file_paths:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    total_files += 1
                    total_size += size
                    print(f"  - {os.path.basename(file_path)} ({size/1024/1024:.2f} MB)")
        
        print(f"\n总计: {total_files} 个文件, {total_size/1024/1024:.2f} MB")
        
        # 保存批处理信息
        save_batch_info(jsonl_files, temp_dir)
        
        print(f"\n所有JSONL文件已保存到: {temp_dir}/")
        print("可以继续执行 upload_and_start.py")
        
    except Exception as e:
        print(f"生成JSONL文件失败: {e}")
        return


if __name__ == "__main__":
    main()
