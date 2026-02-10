# 智谱 Batch API 批处理子系统

> **文档状态**: 2026-02-10 创建  
> **关联源码**: `batch/` 目录  
> **上级文档**: [系统设计总览](design.md)

---

## 1. 概述

### 1.1 定位

`batch/` 子系统是 Stage 1（数据增强）的**替代实现**，直接利用智谱 AI 的 Batch API 进行大规模异步批处理。

### 1.2 适用场景

- 数据量巨大（>10,000 条）且对实时性要求不高
- 需要极致的成本控制（Batch API 通常比实时 API 更便宜且限流更宽松）
- 不需要 PocketFlow 运行时环境

### 1.3 与主流程对比

| 特性 | Batch 子系统 | 主流程 (PocketFlow) |
|:---|:---|:---|
| **实时性** | 低（异步排队） | 高（流式/并发响应） |
| **并发限制** | 极高（由厂商控制） | 需自行控制（Semaphore） |
| **可观测性** | 轮询查状态 | 实时日志 |
| **复杂度** | 多步脚本操作 | 一键运行 |
| **增强维度** | 六维一致 | 六维一致 |
| **成本** | 较低 | 较高 |

---

## 2. 文件结构

```
batch/
├── config.json                    # 核心配置文件
├── batch_run.py                   # 一键运行脚本（调度三步流程）
├── generate_jsonl.py              # Step 1：生成 JSONL 文件
├── upload_and_start.py            # Step 2：上传文件并创建 Batch 任务
├── download_results.py            # Step 3a：监控任务状态并下载结果
├── parse_and_integrate.py         # Step 3b：解析结果并整合原始数据
└── utils/                         # 工具函数库
    ├── jsonl_generator.py         # JSONL 文件构建逻辑（24KB）
    ├── batch_client.py            # Batch API 客户端（20KB）
    ├── result_parser.py           # 结果解析器（14KB）
    └── data_integration.py        # 数据整合器（18KB）
```

---

## 3. 工作流程

### 3.1 流程概览

```mermaid
flowchart LR
    A[准备数据] --> B["Step 1<br/>generate_jsonl.py"]
    B --> C["Step 2<br/>upload_and_start.py"]
    C --> D["Step 3a<br/>download_results.py"]
    D --> E["Step 3b<br/>parse_and_integrate.py"]
    E --> F[enhanced_blogs.json]
```

### 3.2 Step 1：生成 JSONL 文件 `generate_jsonl.py`

**数据加载**（`load_data_files()`）：
- `data/posts.json` — 原始博文数据
- `data/topics.json` — 主题层次结构
- `data/sentiment_attributes.json` — 情感属性列表
- `data/publisher_objects.json` — 发布者类型列表
- `data/believe_system_common.json` — 信念体系分类（可选）
- `data/publisher_decision.json` — 事件关联身份分类（可选）

**JSONL 生成**（委托 `utils/jsonl_generator.py`）：

通过 `create_all_jsonl_files()` 为每种分析维度生成独立的 JSONL 文件。

#### JSONL 行格式

```json
{
  "custom_id": "sentiment_polarity_0001",
  "method": "POST",
  "url": "/v4/chat/completions",
  "body": {
    "model": "glm-4.5-air",
    "messages": [
      {"role": "user", "content": "<prompt>"}
    ],
    "temperature": 0.7
  }
}
```

**`custom_id` 编码规则**：`{analysis_type}_{index:04d}`

| 分析类型 | `custom_id` 前缀 |
|:---|:---|
| 情感极性 | `sentiment_polarity_` |
| 情感属性 | `sentiment_attribute_` |
| 主题分析 | `topic_analysis_` |
| 发布者画像 | `publisher_analysis_` |
| 信念系统 | `belief_system_` |
| 关联身份 | `publisher_decision_` |

**自动拆分**：当文件超过 API 限制（1 万条 / 500MB）时自动拆分为多个文件。

**输出**：
- JSONL 文件保存到 `batch/temp/`
- 文件信息元数据保存为 `batch/temp/jsonl_files_info.json`

### 3.3 Step 2：上传并创建任务 `upload_and_start.py`

**流程**：
1. `get_api_key()` — 获取 API 密钥
2. `load_jsonl_files_info()` — 加载 Step 1 生成的文件信息
3. `check_jsonl_files()` — 验证所有 JSONL 文件存在
4. `upload_and_create_all_batches()` — 委托 `utils/batch_client.py`：
   - 上传 JSONL 文件到智谱服务器
   - 为每个文件创建 Batch 任务
5. `save_batch_results()` — 保存任务 ID 和状态到 `batch/temp/batch_results.json`

### 3.4 Step 3a：监控与下载 `download_results.py`

**流程**（462 行，是最复杂的脚本）：
1. `load_batch_info()` — 加载 Step 2 保存的任务信息
2. `check_all_batch_status()` — 检查所有任务当前状态
3. `wait_for_all_completion()` — 轮询等待所有任务完成
4. `download_result_files()` — 下载完成任务的结果文件
5. `save_download_info()` — 保存下载状态

**轮询策略**：
- 默认间隔：60 秒（`poll_interval`）
- 持续轮询直到所有任务完成或超时

### 3.5 Step 3b：解析与整合 `parse_and_integrate.py`

**流程**：
1. `load_reference_data()` — 加载原始博文作为基准
2. `check_result_files()` — 验证下载的结果文件
3. `load_and_integrate_all_results()` — 委托 `utils/data_integration.py`：
   - 解析结果 JSONL 中的 LLM 响应
   - 根据 `custom_id` 匹配回原始博文
   - 合并各维度分析结果到博文数据
4. `validate_enhanced_posts()` — 验证增强数据完整性
5. `save_enhanced_posts()` — 保存为 `enhanced_blogs.json`
6. `save_integration_report()` — 保存整合报告（成功/失败/覆盖率）

---

## 4. `batch/utils/` 工具模块

### 4.1 `jsonl_generator.py`（24KB）

核心函数 `create_all_jsonl_files()`：
- 为每种分析类型构建对应的 Prompt（复用主流程的 Prompt 逻辑）
- 生成符合智谱 Batch API 格式的 JSONL 行
- 支持自动拆分和文件大小检查

### 4.2 `batch_client.py`（20KB）

封装智谱 Batch API 的 HTTP 调用：
- `upload_and_create_all_batches()` — 批量上传并创建任务
- `get_batch_status()` — 查询任务状态
- `download_file()` — 下载结果文件
- `wait_for_completion()` — 等待单个任务完成

### 4.3 `result_parser.py`（14KB）

解析 Batch API 返回的结果 JSONL：
- 从每行 JSON 中提取 `custom_id` 和 LLM 响应
- 解析 LLM 文本输出中的 JSON 结构
- 处理解析失败的容错逻辑

### 4.4 `data_integration.py`（18KB）

数据整合核心逻辑：
- `load_and_integrate_all_results()` — 合并所有维度的分析结果
- `save_enhanced_posts()` — 原子写入增强数据
- `validate_enhanced_posts()` — 验证字段完整性和类型正确性
- `save_integration_report()` — 生成整合过程的统计报告

---

## 5. 配置文件 `config.json`

```json
{
  "batch": {
    "model": "glm-4-air",
    "multimodal_model": "glm-4v-plus",
    "max_concurrent_tasks": 3,
    "poll_interval": 60
  },
  "paths": {
    "data_dir": "../data",
    "enhanced_output": "./enhanced_blogs.json"
  }
}
```

| 配置项 | 说明 |
|:---|:---|
| `batch.model` | 文本模型名称 |
| `batch.multimodal_model` | 多模态模型名称（处理图片） |
| `batch.max_concurrent_tasks` | 最大并发 Batch 任务数 |
| `batch.poll_interval` | 状态轮询间隔（秒） |
| `paths.data_dir` | 数据文件目录（相对路径） |
| `paths.enhanced_output` | 增强数据输出路径 |

---

## 6. 故障恢复

### 6.1 状态持久化

每个步骤的状态均保存为 JSON 文件：

| 文件 | 步骤 | 内容 |
|:---|:---|:---|
| `batch/temp/jsonl_files_info.json` | Step 1 | 生成的 JSONL 文件路径和大小 |
| `batch/temp/batch_results.json` | Step 2 | Batch 任务 ID 和创建状态 |
| `batch/temp/download_info.json` | Step 3a | 下载文件路径和完成状态 |

### 6.2 断点续传

- 重新运行脚本会检测已有进度，跳过已完成的步骤
- Step 3a 支持单独重新下载失败的文件
- Step 3b 支持部分数据的增量整合

---

## 7. 使用指南

### 7.1 准备工作

```bash
pip install zai
```

确保 `data/` 目录包含：`posts.json`、`topics.json`、`sentiment_attributes.json`、`publisher_objects.json`

### 7.2 三步执行

```bash
cd batch
python generate_jsonl.py       # Step 1: 生成 JSONL
python upload_and_start.py     # Step 2: 上传并创建任务
python download_results.py     # Step 3a: 等待并下载
python parse_and_integrate.py  # Step 3b: 解析并整合
```

### 7.3 一键执行

```bash
python batch_run.py            # 自动依次执行全部步骤
```

### 7.4 与主流程集成

`BatchAPIEnhancementNode`（在 `nodes.py` 中）提供了 PocketFlow 集成方式：
- 通过子进程调用 batch 脚本
- 适用于 `config.analysis_mode = "batch"` 场景
