# 智谱Batch API批处理工作流

基于智谱Batch API的博文分析批处理系统，用于大规模博文数据的情感极性、情感属性、主题和发布者对象分析。

## 功能特点

- **无框架依赖**：使用多个Python脚本组成工作流，不依赖PocketFlow
- **完全一致性**：与现有nodes.py中的提示词和模型参数完全一致
- **模块化设计**：每个脚本负责特定功能，便于维护和测试
- **错误处理完善**：提供完整的错误处理和重试机制
- **性能优化**：支持大规模数据处理和并行执行
- **监控完善**：提供详细的进度监控和日志记录

## 文件结构

```
batch/
├── config.json                    # 配置文件
├── start_batch_workflow.py         # 启动批处理工作流脚本
├── check_and_download.py          # 检查批处理状态并下载结果脚本
├── integrate_and_save.py          # 整合结果并保存增强数据脚本
├── workflow_status.json           # 工作流状态文件（运行时生成）
├── downloaded_results.json        # 下载状态文件（运行时生成）
├── enhanced_blogs.json           # 增强后的博文数据（最终输出）
├── integration_report.txt         # 整合报告（运行时生成）
├── final_status.json            # 最终状态文件（运行时生成）
├── utils/                      # 工具函数脚本
│   ├── __init__.py
│   ├── batch_client.py          # 智谱Batch API客户端
│   ├── jsonl_generator.py       # JSONL文件生成工具
│   ├── result_parser.py         # 结果解析工具
│   └── data_integration.py      # 数据整合工具
├── temp/                       # 临时数据文件
│   ├── sentiment_polarity_batch.jsonl
│   ├── sentiment_attribute_batch.jsonl
│   ├── topic_analysis_batch.jsonl
│   └── publisher_analysis_batch.jsonl
└── results/                    # 批处理结果文件
    ├── sentiment_polarity_results.jsonl
    ├── sentiment_attribute_results.jsonl
    ├── topic_analysis_results.jsonl
    └── publisher_analysis_results.jsonl
```

## 使用前准备

### 1. 安装依赖

```bash
pip install zai
```

### 2. 设置环境变量

```bash
export ZHIPU_API_KEY="your_api_key_here"
```

### 3. 准备数据文件

确保以下数据文件存在于 `../data/` 目录中：
- `posts.json` - 博文数据
- `topics.json` - 主题层次结构
- `sentiment_attributes.json` - 情感属性列表
- `publisher_objects.json` - 发布者对象列表

## 使用步骤

### 步骤1：启动批处理工作流

```bash
cd batch
python start_batch_workflow.py
```

这个脚本会：
1. 加载配置和数据文件
2. 验证数据完整性
3. 生成四个JSONL文件
4. 上传文件到智谱服务器
5. 创建批处理任务
6. 保存工作流状态到 `workflow_status.json`

### 步骤2：监控任务状态并下载结果

```bash
python check_and_download.py
```

这个脚本会：
1. 读取工作流状态
2. 监控批处理任务状态
3. 等待所有任务完成
4. 下载结果文件到 `results/` 目录
5. 保存下载状态到 `downloaded_results.json`

### 步骤3：整合结果并保存增强数据

```bash
python integrate_and_save.py
```

这个脚本会：
1. 加载原始数据和下载的结果
2. 解析并整合所有分析结果
3. 生成增强后的博文数据
4. 验证数据完整性
5. 保存最终结果到 `enhanced_blogs.json`
6. 生成整合报告

## 配置说明

`config.json` 文件包含以下配置项：

```json
{
  "api": {
    "base_url": "https://open.bigmodel.cn/api/paas/v4/",
    "api_key_env": "ZHIPU_API_KEY",
    "timeout": 300
  },
  "batch": {
    "model": "glm-4-air",
    "multimodal_model": "glm-4v-plus",
    "max_file_size": 104857600,
    "max_requests_per_file": 10000,
    "poll_interval": 60,
    "task_timeout": 14400,
    "max_retries": 3,
    "retry_delay": 30,
    "max_concurrent_tasks": 3
  },
  "paths": {
    "data_dir": "../data",
    "batch_dir": ".",
    "temp_dir": "./temp",
    "results_dir": "./results",
    "utils_dir": "./utils"
  },
  "files": {
    "blog_data": "../data/posts.json",
    "topics": "../data/topics.json",
    "sentiment_attributes": "../data/sentiment_attributes.json",
    "publisher_objects": "../data/publisher_objects.json",
    "enhanced_output": "./enhanced_blogs.json",
    "workflow_status": "./workflow_status.json",
    "downloaded_results": "./downloaded_results.json"
  }
}
```

## 分析类型说明

系统支持四种分析类型：

### 1. 情感极性分析 (sentiment_polarity)
- **输出**：1-5的数字评分
- **模型**：glm-4v-plus（有图片）或 glm-4-air（无图片）
- **用途**：评估博文整体情感倾向

### 2. 情感属性分析 (sentiment_attribute)
- **输出**：情感属性JSON数组
- **模型**：glm-4-air
- **用途**：识别具体的情感状态

### 3. 主题分析 (topic_analysis)
- **输出**：主题JSON数组
- **模型**：glm-4v-plus（有图片）或 glm-4-air（无图片）
- **用途**：从预定义主题中选择相关主题

### 4. 发布者对象分析 (publisher_analysis)
- **输出**：发布者类型字符串
- **模型**：glm-4-air
- **用途**：识别博文发布者类型

## 输出文件说明

### 增强博文数据格式

每条增强博文包含原始字段和新增的分析字段：

```json
{
  "id": "original_id",
  "content": "博文内容",
  "image_urls": ["图片URL数组"],
  "sentiment_polarity": 3,
  "sentiment_attribute": ["中立", "客观"],
  "topics": [
    {"parent_topic": "政府工作", "sub_topic": "预警发布"}
  ],
  "publisher": "政府机构"
}
```

### 状态文件

- `workflow_status.json` - 记录批处理任务创建状态
- `downloaded_results.json` - 记录结果下载状态
- `final_status.json` - 记录最终整合状态

### 报告文件

- `integration_report.txt` - 数据整合详细报告

## 错误处理

### 常见错误及解决方案

1. **API密钥未设置**
   ```
   错误：环境变量 ZHIPU_API_KEY 未设置
   解决：export ZHIPU_API_KEY="your_api_key"
   ```

2. **数据文件不存在**
   ```
   错误：博文数据不存在
   解决：检查 ../data/ 目录下的文件
   ```

3. **批处理任务失败**
   ```
   错误：任务创建失败
   解决：检查网络连接和API配额
   ```

4. **结果下载失败**
   ```
   错误：下载文件失败
   解决：重新运行 check_and_download.py
   ```

## 性能优化建议

### 1. 大数据集处理
- 对于超过10,000条博文的数据，建议分批处理
- 每批处理8,000-10,000条，避免超出API限制

### 2. 并发控制
- 系统自动控制并发任务数（默认3个）
- 可通过配置文件调整 `max_concurrent_tasks`

### 3. 成本控制
- 监控API调用统计信息
- 设置合理的轮询间隔避免过度请求

## 监控和日志

### 实时监控
脚本运行时会显示实时进度：
- 任务创建状态
- 任务执行进度
- 结果下载状态
- 数据整合进度

### 日志记录
所有操作都会记录详细日志：
- 成功操作显示 ✅
- 失败操作显示 ❌
- 警告信息显示 ⚠️

## 故障恢复

### 断点续传
如果工作流中断，可以：
1. 重新运行对应步骤的脚本
2. 系统会自动读取之前的状态文件
3. 从中断点继续执行

### 部分失败处理
如果某些任务失败：
1. 检查失败原因
2. 修复问题后重新运行
3. 成功的任务不会重复执行

## 技术支持

如遇到问题，请检查：
1. 环境变量设置
2. 数据文件格式
3. 网络连接状态
4. API配额余额

更多技术细节请参考 `doc/batch_processing_design.md` 设计文档。
