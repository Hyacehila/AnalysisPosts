"""
上传并启动批处理任务脚本

上传JSONL文件到智谱服务器并创建批处理任务
支持多文件处理以符合API限制
"""

import os
import sys
import json
from utils.batch_client import upload_and_create_all_batches


def get_api_key():
    """获取API密钥"""
    api_key = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"
    return api_key


def load_jsonl_files_info():
    """加载JSONL文件信息"""
    temp_dir = "batch/temp"
    info_file = os.path.join(temp_dir, "jsonl_files_info.json")
    
    if not os.path.exists(info_file):
        print(f"JSONL文件信息不存在: {info_file}")
        print("请先运行 generate_jsonl.py 生成JSONL文件")
        return None
    
    try:
        with open(info_file, 'r', encoding='utf-8') as f:
            files_info = json.load(f)
        
        # 构建文件路径映射
        jsonl_files = {}
        for analysis_type, type_info in files_info["analysis_types"].items():
            file_paths = []
            for file_info in type_info["files"]:
                file_path = os.path.join(temp_dir, file_info["path"])
                if os.path.exists(file_path):
                    file_paths.append(file_path)
                else:
                    print(f"警告: 文件不存在 {file_path}")
            jsonl_files[analysis_type] = file_paths
        
        return jsonl_files, files_info
        
    except Exception as e:
        print(f"加载JSONL文件信息失败: {e}")
        return None, None


def check_jsonl_files(jsonl_files: dict):
    """检查JSONL文件是否存在"""
    if not jsonl_files:
        return False
    
    missing_files = []
    for analysis_type, file_paths in jsonl_files.items():
        if not file_paths:
            missing_files.append(f"{analysis_type}: 无文件")
            continue
            
        for file_path in file_paths:
            if not os.path.exists(file_path):
                missing_files.append(f"{analysis_type}: {file_path}")
    
    if missing_files:
        print("以下JSONL文件不存在:")
        for file_info in missing_files:
            print(f"  - {file_info}")
        print("\n请先运行 generate_jsonl.py 生成JSONL文件")
        return False
    
    return True


def display_file_summary(jsonl_files: dict, files_info: dict):
    """显示文件摘要"""
    print("文件摘要:")
    print(f"  总文件数: {files_info['total_files']}")
    print(f"  总大小: {files_info['total_size_mb']:.2f} MB")
    print()
    
    for analysis_type, file_paths in jsonl_files.items():
        type_info = files_info["analysis_types"][analysis_type]
        print(f"{analysis_type}: {len(file_paths)} 个文件, {type_info['file_count']} 个预期")
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                print(f"  - {os.path.basename(file_path)} ({size/1024/1024:.2f} MB)")
        print()


def save_batch_results(batch_results: dict, temp_dir: str):
    """保存批处理结果"""
    results_file = os.path.join(temp_dir, "batch_results.json")
    
    try:
        # 清理数据以便序列化
        def clean_data(data):
            if isinstance(data, dict):
                return {k: clean_data(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [clean_data(item) for item in data]
            elif hasattr(data, '__dict__'):
                return clean_data(data.__dict__)
            else:
                return data
        
        cleaned_results = clean_data(batch_results)
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_results, f, ensure_ascii=False, indent=2)
        
        print(f"批处理结果已保存到: {results_file}")
        
    except Exception as e:
        print(f"保存批处理结果失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("上传文件并启动批处理任务（支持多文件）")
    print("=" * 60)
    
    # 获取API密钥
    api_key = get_api_key()
    if not api_key:
        return
    
    # 加载JSONL文件信息
    result = load_jsonl_files_info()
    if not result:
        return
    
    jsonl_files, files_info = result
    
    # 检查文件
    if not check_jsonl_files(jsonl_files):
        return
    
    # 显示文件摘要
    display_file_summary(jsonl_files, files_info)
    
    print("开始上传文件并创建批处理任务...")
    print()
    
    # 处理所有分析类型
    try:
        batch_results = upload_and_create_all_batches(
            api_key=api_key,
            jsonl_files=jsonl_files
        )
        
        print("\n" + "=" * 60)
        print("批处理任务创建完成")
        print("=" * 60)
        
        # 显示结果摘要
        total_batches = 0
        total_failed = 0
        
        for analysis_type, result in batch_results.items():
            print(f"\n{analysis_type}:")
            
            if result["status"] == "batches_created":
                successful_batches = len(result["successful_batches"])
                total_batches += successful_batches
                
                print(f"  状态: 批处理任务已创建")
                print(f"  成功创建任务: {successful_batches}")
                
                # 显示创建的任务信息
                for batch_info in result["successful_batches"]:
                    print(f"    - {batch_info['description']}: {batch_info['batch_id']}")
                
            else:
                print(f"  状态: {result['status']}")
                total_failed += 1
        
        print(f"\n总计: {total_batches} 个批处理任务已创建")
        
        # 保存结果
        save_batch_results(batch_results, "batch/temp")
        
        if total_batches > 0:
            print(f"\n已成功创建 {total_batches} 个批处理任务")
            print("可以使用智谱控制台查看任务进度")
            print("完成后可以执行 download_results.py 下载结果")
        else:
            print("\n没有成功创建任何批处理任务")
            print("请检查错误信息并重试")
        
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        return


if __name__ == "__main__":
    main()
