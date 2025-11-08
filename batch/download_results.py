"""
下载批处理结果脚本

监控批处理任务状态，完成后下载结果文件
"""

import os
import sys
import json
import time
from utils.batch_client import get_batch_status, download_file, wait_for_completion


def get_api_key():
    """获取API密钥"""
    api_key = "fecda0f3e009473a88c9bcfe711c3248.D35PCYssGvjLqObH"

    
    return api_key


def load_batch_info():
    """加载批处理任务信息"""
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


def check_all_batch_status(api_key: str, batch_info: dict) -> dict:
    """检查所有批处理任务状态"""
    status_info = {}
    
    print("检查批处理任务状态...")
    print()
    
    for analysis_type, info in batch_info.items():
        batch_id = info.get("batch_id")
        if not batch_id:
            print(f"{analysis_type}: 无batch_id")
            continue
        
        print(f"检查 {analysis_type} (ID: {batch_id})...")
        
        status = get_batch_status(api_key, batch_id)
        if status:
            status_info[analysis_type] = {
                "batch_id": batch_id,
                "status": status.get("status"),
                "created_at": status.get("created_at"),
                "completed_at": status.get("completed_at"),
                "request_counts": status.get("request_counts", {}),
                "output_file_id": status.get("output_file_id"),
                "error_file_id": status.get("error_file_id")
            }
            
            print(f"  状态: {status.get('status')}")
            
            # 显示请求统计
            counts = status.get("request_counts", {})
            if counts:
                total = counts.get("total", 0)
                completed = counts.get("completed", 0)
                failed = counts.get("failed", 0)
                print(f"  进度: {completed}/{total} 完成, {failed} 失败")
        else:
            print(f"  状态查询失败")
        
        print()
    
    return status_info


def wait_for_all_completion(api_key: str, batch_info: dict, poll_interval: int = 60) -> dict:
    """等待所有批处理任务完成"""
    print("等待所有批处理任务完成...")
    print(f"检查间隔: {poll_interval} 秒")
    print("按 Ctrl+C 可以停止等待")
    print()
    
    completed_tasks = {}
    failed_tasks = {}
    
    while True:
        try:
            # 检查所有任务状态
            status_info = check_all_batch_status(api_key, batch_info)
            
            # 检查是否所有任务都完成
            all_completed = True
            for analysis_type, status in status_info.items():
                task_status = status.get("status")
                if task_status in ["validating", "in_progress", "cancelling"]:
                    all_completed = False
                elif task_status == "completed":
                    completed_tasks[analysis_type] = status
                elif task_status in ["failed", "cancelled", "expired"]:
                    failed_tasks[analysis_type] = status
            
            if all_completed:
                print("\n所有批处理任务已完成!")
                break
            
            # 显示完成进度
            total_tasks = len(batch_info)
            completed_count = len(completed_tasks) + len(failed_tasks)
            print(f"进度: {completed_count}/{total_tasks} 任务完成")
            
            # 等待下次检查
            time.sleep(poll_interval)
            
        except KeyboardInterrupt:
            print("\n\n用户中断等待")
            break
        except Exception as e:
            print(f"检查状态时出错: {e}")
            time.sleep(poll_interval)
    
    return {
        "completed": completed_tasks,
        "failed": failed_tasks,
        "status_info": status_info
    }


def download_result_files(api_key: str, completed_tasks: dict) -> dict:
    """下载结果文件"""
    temp_dir = "batch/temp"
    os.makedirs(temp_dir, exist_ok=True)
    
    downloaded_files = {}
    
    print("下载结果文件...")
    print()
    
    for analysis_type, status in completed_tasks.items():
        output_file_id = status.get("output_file_id")
        error_file_id = status.get("error_file_id")
        
        # 下载结果文件
        if output_file_id:
            result_path = os.path.join(temp_dir, f"{analysis_type}_results.jsonl")
            print(f"下载 {analysis_type} 结果文件...")
            
            if download_file(api_key, output_file_id, result_path):
                downloaded_files[analysis_type] = result_path
                print(f"  结果文件: {result_path}")
            else:
                print(f"  结果文件下载失败")
        
        # 下载错误文件
        if error_file_id:
            error_path = os.path.join(temp_dir, f"{analysis_type}_errors.jsonl")
            print(f"下载 {analysis_type} 错误文件...")
            
            if download_file(api_key, error_file_id, error_path):
                print(f"  错误文件: {error_path}")
            else:
                print(f"  错误文件下载失败")
        
        print()
    
    return downloaded_files


def save_download_info(completed_tasks: dict, failed_tasks: dict, downloaded_files: dict):
    """保存下载信息"""
    # 清理数据，确保可以正确序列化
    def clean_data(data):
        if isinstance(data, dict):
            return {k: clean_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_data(item) for item in data]
        elif hasattr(data, '__dict__'):
            return clean_data(data.__dict__)
        else:
            return data
    
    download_info = {
        "completed_tasks": clean_data(completed_tasks),
        "failed_tasks": clean_data(failed_tasks),
        "downloaded_files": clean_data(downloaded_files),
        "download_time": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    temp_dir = "batch/temp"
    os.makedirs(temp_dir, exist_ok=True)
    info_file = os.path.join(temp_dir, "download_info.json")
    
    try:
        # 先写入临时文件，确保完整性
        temp_file = info_file + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(download_info, f, ensure_ascii=False, indent=2)
        
        # 验证JSON格式
        with open(temp_file, 'r', encoding='utf-8') as f:
            json.load(f)  # 验证JSON格式正确
        
        # 重命名为最终文件
        os.rename(temp_file, info_file)
        
        print(f"下载信息已保存到: {info_file}")
        
    except Exception as e:
        print(f"保存下载信息失败: {e}")
        # 如果保存失败，尝试保存简化版本
        try:
            simplified_info = {
                "completed_count": len(completed_tasks),
                "failed_count": len(failed_tasks),
                "downloaded_count": len(downloaded_files),
                "download_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(simplified_info, f, ensure_ascii=False, indent=2)
            print(f"已保存简化版本的下载信息到: {info_file}")
        except Exception as e2:
            print(f"保存简化版本也失败: {e2}")


def main():
    """主函数"""
    print("=" * 50)
    print("下载批处理结果")
    print("=" * 50)
    
    # 获取API密钥
    api_key = get_api_key()
    if not api_key:
        return
    
    # 加载批处理信息
    batch_info = load_batch_info()
    if not batch_info:
        return
    
    print(f"找到 {len(batch_info)} 个批处理任务:")
    for analysis_type, info in batch_info.items():
        batch_id = info.get("batch_id")
        print(f"  {analysis_type}: {batch_id}")
    print()
    
    # 等待任务完成
    task_results = wait_for_all_completion(api_key, batch_info)
    
    completed_tasks = task_results.get("completed", {})
    failed_tasks = task_results.get("failed", {})
    
    print("\n" + "=" * 50)
    print("任务完成情况")
    print("=" * 50)
    
    if completed_tasks:
        print(f"成功完成的任务 ({len(completed_tasks)}):")
        for analysis_type, status in completed_tasks.items():
            batch_id = status.get("batch_id")
            counts = status.get("request_counts", {})
            completed = counts.get("completed", 0)
            total = counts.get("total", 0)
            print(f"  {analysis_type}: {batch_id} ({completed}/{total})")
    
    if failed_tasks:
        print(f"\n失败的任务 ({len(failed_tasks)}):")
        for analysis_type, status in failed_tasks.items():
            batch_id = status.get("batch_id")
            task_status = status.get("status")
            print(f"  {analysis_type}: {batch_id} ({task_status})")
    
    # 下载结果文件
    if completed_tasks:
        print(f"\n开始下载 {len(completed_tasks)} 个任务的结果...")
        downloaded_files = download_result_files(api_key, completed_tasks)
        
        # 保存下载信息
        save_download_info(completed_tasks, failed_tasks, downloaded_files)
        
        print("\n" + "=" * 50)
        print("结果下载完成")
        print("=" * 50)
        
        print("下载的文件:")
        for analysis_type, file_path in downloaded_files.items():
            file_size = os.path.getsize(file_path)
            print(f"  {analysis_type}: {file_path} ({file_size} bytes)")
        
        print(f"\n所有结果文件已保存到: batch/temp/")
        print("可以继续执行 parse_and_integrate.py 解析并整合结果")
    else:
        print("\n没有成功完成的任务，无法下载结果")


if __name__ == "__main__":
    main()
