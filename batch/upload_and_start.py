"""
上传并启动批处理任务脚本

上传JSONL文件到智谱服务器并创建批处理任务
"""

import os
import sys
import json
from utils.batch_client import upload_file, create_batch, process_all_analysis_types


def get_api_key():
    """获取API密钥"""
    api_key = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"
    return api_key


def check_jsonl_files():
    """检查JSONL文件是否存在"""
    temp_dir = "batch/temp"
    jsonl_files = {
        "sentiment_polarity": os.path.join(temp_dir, "sentiment_polarity_batch.jsonl"),
        "sentiment_attribute": os.path.join(temp_dir, "sentiment_attribute_batch.jsonl"),
        "topic_analysis": os.path.join(temp_dir, "topic_analysis_batch.jsonl"),
        "publisher_analysis": os.path.join(temp_dir, "publisher_analysis_batch.jsonl")
    }
    
    missing_files = []
    for analysis_type, file_path in jsonl_files.items():
        if not os.path.exists(file_path):
            missing_files.append(f"{analysis_type}: {file_path}")
    
    if missing_files:
        print("以下JSONL文件不存在:")
        for file_info in missing_files:
            print(f"  - {file_info}")
        print("\n请先运行 generate_jsonl.py 生成JSONL文件")
        return None
    
    return jsonl_files


def upload_and_create_tasks(api_key: str, jsonl_files: dict) -> dict:
    """上传文件并创建批处理任务"""
    batch_info = {}
    
    print("开始上传文件并创建批处理任务...")
    print()
    
    for analysis_type, file_path in jsonl_files.items():
        print(f"处理 {analysis_type}...")
        
        # 上传文件
        file_id = upload_file(api_key, file_path)
        if not file_id:
            print(f"{analysis_type} 文件上传失败，跳过")
            continue
        
        # 创建批处理任务
        batch_id = create_batch(api_key, file_id, f"{analysis_type} analysis")
        if not batch_id:
            print(f"{analysis_type} 批处理任务创建失败，跳过")
            continue
        
        batch_info[analysis_type] = {
            "file_path": file_path,
            "file_id": file_id,
            "batch_id": batch_id,
            "status": "created"
        }
        
        print(f"{analysis_type} 任务创建成功: {batch_id}")
        print()
    
    return batch_info


def save_batch_info(batch_info: dict):
    """保存批处理任务信息"""
    temp_dir = "batch/temp"
    os.makedirs(temp_dir, exist_ok=True)
    info_file = os.path.join(temp_dir, "batch_info.json")
    
    try:
        with open(info_file, 'w', encoding='utf-8') as f:
            json.dump(batch_info, f, ensure_ascii=False, indent=2)
        
        print(f"批处理任务信息已保存到: {info_file}")
        
    except Exception as e:
        print(f"保存批处理信息失败: {e}")


def main():
    """主函数"""
    print("=" * 50)
    print("上传文件并启动批处理任务")
    print("=" * 50)
    
    # 获取API密钥
    api_key = get_api_key()
    if not api_key:
        return
    
    # 检查JSONL文件
    jsonl_files = check_jsonl_files()
    if not jsonl_files:
        return
    
    print(f"找到 {len(jsonl_files)} 个JSONL文件:")
    for analysis_type, file_path in jsonl_files.items():
        file_size = os.path.getsize(file_path)
        print(f"  {analysis_type}: {file_path} ({file_size} bytes)")
    print()
    
    # 上传文件并创建任务
    batch_info = upload_and_create_tasks(api_key, jsonl_files)
    
    if not batch_info:
        print("没有成功创建任何批处理任务")
        return
    
    # 保存批处理信息
    save_batch_info(batch_info)
    
    print("\n" + "=" * 50)
    print("批处理任务创建完成")
    print("=" * 50)
    
    print("任务摘要:")
    for analysis_type, info in batch_info.items():
        print(f"  {analysis_type}: {info['batch_id']}")
    
    print(f"\n已创建 {len(batch_info)} 个批处理任务")
    print("可以继续执行 download_results.py 下载结果")
    print("\n注意: 批处理任务可能需要几分钟到几小时完成")
    print("可以使用智谱控制台查看任务进度")


if __name__ == "__main__":
    main()
