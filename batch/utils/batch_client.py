"""
智谱Batch API客户端工具

提供简化的函数式API，用于文件上传、批处理任务创建、状态查询和结果下载
支持多文件处理以符合API限制
"""

import os
import json
import time
from typing import Dict, Any, Optional, List
from zai import ZhipuAiClient


def upload_file(api_key: str, file_path: str, purpose: str = "batch") -> Optional[str]:
    """
    上传文件到智谱服务器
    
    Args:
        api_key: 智谱API密钥
        file_path: 文件路径
        purpose: 文件用途，默认为"batch"
        
    Returns:
        文件ID，失败时返回None
    """
    try:
        client = ZhipuAiClient(api_key=api_key)
        
        file_object = client.files.create(
            file=open(file_path, "rb"),
            purpose=purpose
        )
        
        print(f"文件上传成功: {os.path.basename(file_path)} -> {file_object.id}")
        return file_object.id
        
    except Exception as e:
        print(f"文件上传失败 {file_path}: {str(e)}")
        return None


def create_batch(api_key: str, input_file_id: str, description: str = "") -> Optional[str]:
    """
    创建批处理任务
    
    Args:
        api_key: 智谱API密钥
        input_file_id: 输入文件的ID
        description: 任务描述
        
    Returns:
        批处理任务ID，失败时返回None
    """
    try:
        client = ZhipuAiClient(api_key=api_key)
        
        batch_job = client.batches.create(
            input_file_id=input_file_id,
            endpoint="/v4/chat/completions",
            auto_delete_input_file=True,
            metadata={
                "description": description,
                "project": "blog_analysis"
            }
        )
        
        print(f"批处理任务创建成功: {description} -> {batch_job.id}")
        return batch_job.id
        
    except Exception as e:
        print(f"创建批处理任务失败: {str(e)}")
        return None


def get_batch_status(api_key: str, batch_id: str) -> Optional[Dict[str, Any]]:
    """
    查询批处理任务状态
    
    Args:
        api_key: 智谱API密钥
        batch_id: 批处理任务ID
        
    Returns:
        任务状态信息，失败时返回None
    """
    try:
        client = ZhipuAiClient(api_key=api_key)
        batch_job = client.batches.retrieve(batch_id)
        
        return {
            "id": batch_job.id,
            "status": batch_job.status,
            "created_at": batch_job.created_at,
            "completed_at": getattr(batch_job, 'completed_at', None),
            "failed_at": getattr(batch_job, 'failed_at', None),
            "request_counts": getattr(batch_job, 'request_counts', {}),
            "output_file_id": getattr(batch_job, 'output_file_id', None),
            "error_file_id": getattr(batch_job, 'error_file_id', None),
            "metadata": getattr(batch_job, 'metadata', {})
        }
        
    except Exception as e:
        print(f"查询任务状态失败 {batch_id}: {str(e)}")
        return None


def wait_for_completion(api_key: str, batch_id: str, poll_interval: int = 60, timeout: int = 14400) -> Optional[Dict[str, Any]]:
    """
    等待批处理任务完成
    
    Args:
        api_key: 智谱API密钥
        batch_id: 批处理任务ID
        poll_interval: 轮询间隔（秒）
        timeout: 超时时间（秒）
        
    Returns:
        最终任务状态，失败时返回None
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            status = get_batch_status(api_key, batch_id)
            
            if status is None:
                return None
            
            print(f"任务 {batch_id} 状态: {status.get('status', 'unknown')}")
            
            if status.get('status') in ['completed', 'failed', 'cancelled', 'expired']:
                return status
            
            time.sleep(poll_interval)
            
        except Exception as e:
            print(f"查询状态时出错: {str(e)}")
            time.sleep(poll_interval)
    
    print(f"任务 {batch_id} 在 {timeout} 秒内未完成")
    return None


def download_file(api_key: str, file_id: str, output_path: str) -> bool:
    """
    下载文件
    
    Args:
        api_key: 智谱API密钥
        file_id: 文件ID
        output_path: 输出文件路径
        
    Returns:
        下载是否成功
    """
    try:
        client = ZhipuAiClient(api_key=api_key)
        
        # 获取文件内容
        file_content = client.files.content(file_id)
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 写入文件 - 处理不同类型的返回值
        if hasattr(file_content, 'write_to_file'):
            # 如果对象有write_to_file方法
            file_content.write_to_file(output_path)
        elif hasattr(file_content, 'content'):
            # 如果对象有content属性
            with open(output_path, 'wb') as f:
                f.write(file_content.content)
        elif isinstance(file_content, bytes):
            # 如果直接返回字节数据
            with open(output_path, 'wb') as f:
                f.write(file_content)
        elif hasattr(file_content, 'read'):
            # 如果是文件类对象
            with open(output_path, 'wb') as f:
                f.write(file_content.read())
        else:
            # 尝试直接写入
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(str(file_content))
        
        print(f"文件下载成功: {file_id} -> {os.path.basename(output_path)}")
        return True
        
    except Exception as e:
        print(f"下载文件失败 {file_id}: {str(e)}")
        # 添加更详细的错误信息
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
        return False


def get_file_info(api_key: str, file_id: str) -> Optional[Dict[str, Any]]:
    """
    获取文件信息
    
    Args:
        api_key: 智谱API密钥
        file_id: 文件ID
        
    Returns:
        文件信息，失败时返回None
    """
    try:
        client = ZhipuAiClient(api_key=api_key)
        file_object = client.files.retrieve(file_id)
        
        return {
            "id": file_object.id,
            "filename": file_object.filename,
            "bytes": file_object.bytes,
            "created_at": file_object.created_at,
            "purpose": file_object.purpose
        }
        
    except Exception as e:
        print(f"获取文件信息失败 {file_id}: {str(e)}")
        return None


def upload_multiple_files(api_key: str, file_paths: List[str], analysis_type: str) -> List[Dict[str, Any]]:
    """
    上传多个文件
    
    Args:
        api_key: 智谱API密钥
        file_paths: 文件路径列表
        analysis_type: 分析类型
        
    Returns:
        上传结果列表
    """
    upload_results = []
    
    for i, file_path in enumerate(file_paths):
        print(f"上传文件 {i+1}/{len(file_paths)}: {os.path.basename(file_path)}")
        
        file_id = upload_file(api_key, file_path)
        if file_id:
            upload_results.append({
                "file_path": file_path,
                "file_id": file_id,
                "status": "success"
            })
        else:
            upload_results.append({
                "file_path": file_path,
                "file_id": None,
                "status": "failed"
            })
    
    return upload_results


def create_multiple_batches(api_key: str, upload_results: List[Dict[str, Any]], analysis_type: str) -> List[Dict[str, Any]]:
    """
    为多个上传的文件创建批处理任务
    
    Args:
        api_key: 智谱API密钥
        upload_results: 上传结果列表
        analysis_type: 分析类型
        
    Returns:
        批处理任务结果列表
    """
    batch_results = []
    
    for i, upload_result in enumerate(upload_results):
        if upload_result["status"] != "success":
            continue
        
        file_path = upload_result["file_path"]
        file_id = upload_result["file_id"]
        
        # 为分片文件添加标识
        part_suffix = ""
        if "_part" in os.path.basename(file_path):
            part_suffix = f" (Part {i+1})"
        
        description = f"{analysis_type} analysis{part_suffix}"
        
        print(f"创建批处理任务 {i+1}/{len(upload_results)}: {description}")
        
        batch_id = create_batch(api_key, file_id, description)
        if batch_id:
            batch_results.append({
                "file_path": file_path,
                "file_id": file_id,
                "batch_id": batch_id,
                "description": description,
                "status": "success"
            })
        else:
            batch_results.append({
                "file_path": file_path,
                "file_id": file_id,
                "batch_id": None,
                "description": description,
                "status": "failed"
            })
    
    return batch_results


def wait_for_multiple_batches(api_key: str, batch_results: List[Dict[str, Any]], poll_interval: int = 60) -> Dict[str, Any]:
    """
    等待多个批处理任务完成
    
    Args:
        api_key: 智谱API密钥
        batch_results: 批处理任务结果列表
        poll_interval: 轮询间隔（秒）
        
    Returns:
        所有任务的最终状态
    """
    completed_batches = []
    failed_batches = []
    pending_batches = batch_results.copy()
    
    print(f"等待 {len(pending_batches)} 个批处理任务完成...")
    
    while pending_batches:
        print(f"\n检查任务状态... (剩余: {len(pending_batches)})")
        
        still_pending = []
        
        for batch_result in pending_batches:
            batch_id = batch_result["batch_id"]
            description = batch_result["description"]
            
            print(f"检查: {description}")
            
            status = get_batch_status(api_key, batch_id)
            if status:
                task_status = status.get("status")
                
                if task_status in ["completed", "failed", "cancelled", "expired"]:
                    if task_status == "completed":
                        completed_batches.append({
                            **batch_result,
                            "final_status": status
                        })
                        print(f"  ✓ 完成: {description}")
                    else:
                        failed_batches.append({
                            **batch_result,
                            "final_status": status
                        })
                        print(f"  ✗ 失败: {description} ({task_status})")
                else:
                    still_pending.append(batch_result)
                    print(f"  ⏳ 进行中: {description} ({task_status})")
            else:
                still_pending.append(batch_result)
                print(f"  ? 状态未知: {description}")
        
        pending_batches = still_pending
        
        if pending_batches:
            print(f"等待 {poll_interval} 秒后继续检查...")
            time.sleep(poll_interval)
    
    return {
        "completed": completed_batches,
        "failed": failed_batches,
        "total_completed": len(completed_batches),
        "total_failed": len(failed_batches)
    }


def download_multiple_results(api_key: str, completed_batches: List[Dict[str, Any]], output_dir: str) -> Dict[str, List[str]]:
    """
    下载多个批处理任务的结果文件
    
    Args:
        api_key: 智谱API密钥
        completed_batches: 已完成的批处理任务列表
        output_dir: 输出目录
        
    Returns:
        下载结果映射
    """
    downloaded_files = {}
    
    os.makedirs(output_dir, exist_ok=True)
    
    for batch_result in completed_batches:
        description = batch_result["description"]
        final_status = batch_result["final_status"]
        
        # 从描述中提取分析类型
        analysis_type = description.split(" analysis")[0]
        
        if analysis_type not in downloaded_files:
            downloaded_files[analysis_type] = []
        
        # 下载结果文件
        output_file_id = final_status.get("output_file_id")
        if output_file_id:
            # 为分片文件生成合适的文件名
            base_name = f"{analysis_type}_results"
            if "_part" in description:
                part_num = description.split("Part ")[1].split(")")[0]
                filename = f"{base_name}_part{part_num}.jsonl"
            else:
                filename = f"{base_name}.jsonl"
            
            output_path = os.path.join(output_dir, filename)
            
            print(f"下载结果: {description} -> {filename}")
            
            if download_file(api_key, output_file_id, output_path):
                downloaded_files[analysis_type].append(output_path)
        
        # 下载错误文件
        error_file_id = final_status.get("error_file_id")
        if error_file_id:
            base_name = f"{analysis_type}_errors"
            if "_part" in description:
                part_num = description.split("Part ")[1].split(")")[0]
                filename = f"{base_name}_part{part_num}.jsonl"
            else:
                filename = f"{base_name}.jsonl"
            
            error_path = os.path.join(output_dir, filename)
            
            print(f"下载错误文件: {description} -> {filename}")
            download_file(api_key, error_file_id, error_path)
    
    return downloaded_files


def upload_and_create_batches(api_key: str, file_paths: List[str], analysis_type: str) -> Dict[str, Any]:
    """
    上传文件并创建批处理任务（不等待完成）
    
    Args:
        api_key: 智谱API密钥
        file_paths: 文件路径列表
        analysis_type: 分析类型
        
    Returns:
        上传和创建任务的结果
    """
    print(f"\n开始处理 {analysis_type} 的 {len(file_paths)} 个文件")
    
    # 1. 上传文件
    upload_results = upload_multiple_files(api_key, file_paths, analysis_type)
    successful_uploads = [r for r in upload_results if r["status"] == "success"]
    
    if not successful_uploads:
        return {"status": "all_uploads_failed", "results": upload_results}
    
    print(f"成功上传 {len(successful_uploads)}/{len(file_paths)} 个文件")
    
    # 2. 创建批处理任务
    batch_results = create_multiple_batches(api_key, successful_uploads, analysis_type)
    successful_batches = [r for r in batch_results if r["status"] == "success"]
    
    if not successful_batches:
        return {"status": "all_batches_failed", "results": batch_results}
    
    print(f"成功创建 {len(successful_batches)}/{len(successful_uploads)} 个批处理任务")
    
    return {
        "status": "batches_created",
        "upload_results": upload_results,
        "batch_results": batch_results,
        "successful_batches": successful_batches
    }


def process_multiple_files_workflow(api_key: str, file_paths: List[str], analysis_type: str, output_dir: str = "batch/temp") -> Dict[str, Any]:
    """
    处理多个文件的完整工作流（包含等待和下载）
    
    Args:
        api_key: 智谱API密钥
        file_paths: 文件路径列表
        analysis_type: 分析类型
        output_dir: 输出目录
        
    Returns:
        处理结果
    """
    # 1. 上传文件并创建批处理任务
    upload_result = upload_and_create_batches(api_key, file_paths, analysis_type)
    
    if upload_result["status"] != "batches_created":
        return upload_result
    
    successful_batches = upload_result["successful_batches"]
    
    # 2. 等待任务完成
    batch_status = wait_for_multiple_batches(api_key, successful_batches)
    
    # 3. 下载结果
    if batch_status["total_completed"] > 0:
        downloaded_files = download_multiple_results(
            api_key, 
            batch_status["completed"], 
            output_dir
        )
    else:
        downloaded_files = {}
    
    return {
        "status": "completed",
        "upload_results": upload_result["upload_results"],
        "batch_results": upload_result["batch_results"],
        "batch_status": batch_status,
        "downloaded_files": downloaded_files
    }


def upload_and_create_all_batches(api_key: str, jsonl_files: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    上传所有分析类型的文件并创建批处理任务（不等待完成）
    
    Args:
        api_key: 智谱API密钥
        jsonl_files: 分析类型到文件路径列表的映射
        
    Returns:
        所有上传和创建任务的结果
    """
    all_results = {}
    
    for analysis_type, file_paths in jsonl_files.items():
        print(f"\n{'='*60}")
        print(f"开始处理分析类型: {analysis_type}")
        print(f"文件数量: {len(file_paths)}")
        print(f"{'='*60}")
        
        result = upload_and_create_batches(api_key, file_paths, analysis_type)
        all_results[analysis_type] = result
        
        if result["status"] == "batches_created":
            successful_batches = len(result["successful_batches"])
            total_batches = len(result["batch_results"])
            print(f"\n{analysis_type} 批处理任务创建完成: {successful_batches}/{total_batches} 成功")
        else:
            print(f"\n{analysis_type} 批处理任务创建失败: {result['status']}")
    
    return all_results


def process_all_analysis_types(api_key: str, jsonl_files: Dict[str, List[str]], output_dir: str = "batch/temp") -> Dict[str, Any]:
    """
    批量处理所有分析类型（支持多文件）
    
    Args:
        api_key: 智谱API密钥
        jsonl_files: 分析类型到文件路径列表的映射
        output_dir: 输出目录
        
    Returns:
        所有处理结果
    """
    all_results = {}
    
    for analysis_type, file_paths in jsonl_files.items():
        print(f"\n{'='*60}")
        print(f"开始处理分析类型: {analysis_type}")
        print(f"文件数量: {len(file_paths)}")
        print(f"{'='*60}")
        
        result = process_multiple_files_workflow(api_key, file_paths, analysis_type, output_dir)
        all_results[analysis_type] = result
        
        if result["status"] == "completed":
            completed = result["batch_status"]["total_completed"]
            failed = result["batch_status"]["total_failed"]
            print(f"\n{analysis_type} 处理完成: {completed} 成功, {failed} 失败")
        else:
            print(f"\n{analysis_type} 处理失败: {result['status']}")
    
    return all_results


# 保持向后兼容的旧函数
def complete_batch_workflow(api_key: str, jsonl_file_path: str, analysis_type: str) -> Optional[Dict[str, Any]]:
    """完整的批处理工作流（向后兼容）"""
    result = process_multiple_files_workflow(api_key, [jsonl_file_path], analysis_type)
    
    if result["status"] == "completed" and result["batch_status"]["total_completed"] > 0:
        completed = result["batch_status"]["completed"][0]
        return {
            "status": "success",
            "batch_id": completed["batch_id"],
            "result_file": result["downloaded_files"].get(analysis_type, [None])[0],
            "statistics": completed["final_status"].get("request_counts", {}),
            "metadata": completed["final_status"].get("metadata", {})
        }
    else:
        return None


if __name__ == "__main__":
    print("批处理客户端工具模块加载成功")
