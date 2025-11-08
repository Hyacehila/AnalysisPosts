"""
智谱Batch API客户端工具

提供简化的函数式API，用于文件上传、批处理任务创建、状态查询和结果下载
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
        
        print(f"文件上传成功: {file_object.filename} -> {file_object.id}")
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
        
        # 写入文件
        file_content.write_to_file(output_path)
        
        print(f"文件下载成功: {file_id} -> {output_path}")
        return True
        
    except Exception as e:
        print(f"下载文件失败 {file_id}: {str(e)}")
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


def complete_batch_workflow(api_key: str, jsonl_file_path: str, analysis_type: str) -> Optional[Dict[str, Any]]:
    """
    完整的批处理工作流：上传文件 -> 创建任务 -> 等待完成 -> 下载结果
    
    Args:
        api_key: 智谱API密钥
        jsonl_file_path: JSONL文件路径
        analysis_type: 分析类型
        
    Returns:
        处理结果统计，失败时返回None
    """
    try:
        # 1. 上传文件
        print(f"正在上传文件: {jsonl_file_path}")
        file_id = upload_file(api_key, jsonl_file_path)
        if not file_id:
            return None
        
        # 2. 创建批处理任务
        print("正在创建批处理任务...")
        batch_id = create_batch(api_key, file_id, f"{analysis_type} analysis")
        if not batch_id:
            return None
        
        # 3. 等待任务完成
        print("等待任务完成...")
        final_status = wait_for_completion(api_key, batch_id)
        if not final_status:
            return None
        
        if final_status['status'] == 'completed':
            # 4. 下载结果文件
            result_file_id = final_status.get('output_file_id')
            if result_file_id:
                output_path = f"results/{analysis_type}_results.jsonl"
                
                print(f"下载结果文件到: {output_path}")
                success = download_file(api_key, result_file_id, output_path)
                
                # 5. 如果有错误文件，也下载
                error_file_id = final_status.get('error_file_id')
                if error_file_id:
                    error_path = f"results/{analysis_type}_errors.jsonl"
                    download_file(api_key, error_file_id, error_path)
                
                if success:
                    return {
                        "status": "success",
                        "batch_id": batch_id,
                        "result_file": output_path,
                        "statistics": final_status.get('request_counts', {}),
                        "metadata": final_status.get('metadata', {})
                    }
                else:
                    return {"status": "download_failed", "batch_id": batch_id}
            else:
                return {"status": "no_result_file", "batch_id": batch_id}
        else:
            return {
                "status": "failed", 
                "batch_id": batch_id,
                "error": final_status.get('errors', 'Unknown error')
            }
            
    except Exception as e:
        print(f"批处理工作流失败: {str(e)}")
        return None


def process_all_analysis_types(api_key: str, jsonl_files: Dict[str, str]) -> Dict[str, Any]:
    """
    批量处理所有分析类型
    
    Args:
        api_key: 智谱API密钥
        jsonl_files: 分析类型到文件路径的映射
        
    Returns:
        所有处理结果
    """
    results = {}
    
    # 顺序处理（移除并发控制）
    for analysis_type, file_path in jsonl_files.items():
        print(f"\n开始处理 {analysis_type}...")
        
        result = complete_batch_workflow(api_key, file_path, analysis_type)
        results[analysis_type] = result
        
        if result and result.get("status") == "success":
            print(f"{analysis_type} 处理成功")
        else:
            print(f"{analysis_type} 处理失败: {result}")
    
    return results


if __name__ == "__main__":
    print("批处理客户端工具模块加载成功")
