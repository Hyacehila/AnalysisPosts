# 工具函数文档

> **文档状态**: 2026-02-10 创建  
> **关联源码**: `utils/call_llm.py`, `utils/data_loader.py`, `utils/console_safe.py`  
> **上级文档**: [系统设计总览](design.md)

---

## 1. LLM 调用层 `call_llm.py`

### 1.1 模型概览

系统使用智谱 AI（Zhipu AI）的 GLM 系列模型，通过 `zai-sdk` 进行调用。

| 调用函数 | 底层模型 | 类型 | 主要用途 | 默认温度 |
|:---|:---|:---|:---|:---|
| `call_glm_45_air` | `glm-4.5-air` | 纯文本 | 阶段 1 数据增强 | 0.7 |
| `call_glm4v_plus` | `glm-4.5-air`* | 纯文本 | 兼容旧代码（已废弃视觉功能） | 0.7 |
| `call_glm45v_thinking` | `glm-4.5v` | 多模态（文本+图像） | 图表视觉分析 | 0.7 |
| `call_glm46` | `glm-4.6` | 纯文本+推理 | Agent 决策、洞察生成、报告撰写 | 0.8 |

> \* `call_glm4v_plus` 原调用 `glm-4v-plus` 视觉模型，现已切换为 `glm-4.5-air`，保留函数签名（含 `image_paths`、`image_data` 参数）以兼容旧代码，但所有图像参数均被忽略。

### 1.2 客户端管理

**ThreadLocal 模式**：

```python
_thread_local = threading.local()

def get_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = ZaiClient(api_key=GLM_API_KEY)
    return _thread_local.client
```

- 每个线程独立持有一个 `ZaiClient` 实例
- 避免锁竞争，同时实现连接复用
- 适配阶段 1 的高并发异步批处理场景（60+ 并发信号量）

### 1.3 `call_glm_45_air` 详细说明

| 参数 | 类型 | 默认值 | 说明 |
|:---|:---|:---|:---|
| `prompt` | `str` | — | 输入提示词 |
| `temperature` | `float` | 0.7 | 控制随机性 |
| `max_tokens` | `int?` | `None` | 最大生成 token 数 |
| `enable_thinking` | `bool` | `False` | 思考模式（此模型不支持，参数保留） |
| `timeout` | `int` | 30 | 请求超时秒数 |

**错误分类处理**：
- 429 / rate limit → 抛出 "API速率限制" 异常
- timeout → 抛出 "请求超时" 异常
- 其他 → 抛出通用异常

### 1.4 `call_glm45v_thinking` 详细说明

| 参数 | 类型 | 默认值 | 说明 |
|:---|:---|:---|:---|
| `prompt` | `str` | — | 输入提示词 |
| `image_paths` | `List[str]?` | `None` | 图片文件路径列表 |
| `image_data` | `List[bytes]?` | `None` | 图片二进制数据（与 `image_paths` 二选一） |
| `temperature` | `float` | 0.7 | 控制随机性 |
| `max_tokens` | `int?` | `None` | 最大生成 token 数 |
| `enable_thinking` | `bool` | `True` | 开启思考模式 |

**图像编码流程**：
1. 读取图片文件 → `base64.b64encode`
2. 判断 MIME 类型：`.png` → `image/png`，其他 → `image/jpeg`
3. 构建 `data:URI` 格式：`data:{mime};base64,{data}`
4. 组装为 `{"type": "image_url", "image_url": {"url": "..."}}`

**思考模式参数**：
```python
if enable_thinking:
    params["thinking"] = {"enabled": True}
```

### 1.5 `call_glm46` 详细说明

| 参数 | 类型 | 默认值 | 说明 |
|:---|:---|:---|:---|
| `prompt` | `str` | — | 输入提示词 |
| `temperature` | `float` | 0.8 | 控制随机性 |
| `max_tokens` | `int?` | `None` | 最大生成 token 数 |
| `enable_reasoning` | `bool` | `True` | 开启推理模式 |
| `max_retries` | `int` | 3 | 429 错误最大重试次数 |
| `retry_delay` | `int` | 5 | 重试初始延迟秒数 |

**重试策略**：
- 仅对 429/并发限制/rate limit 错误进行重试
- **指数退避**：第 1 次等 5 秒，第 2 次等 10 秒，第 3 次等 15 秒
- 非并发类错误直接抛出
- 超过最大重试次数后抛出最终异常

**推理模式参数**：
```python
if enable_reasoning:
    params["thinking"] = {"enabled": True}
```

---

## 2. 数据加载层 `data_loader.py`

### 2.1 函数概览

| 函数 | 功能 | 输入 | 输出 |
|:---|:---|:---|:---|
| `load_blog_data` | 加载原始博文数据 | `data/posts.json` | `List[Dict]` |
| `load_topics` | 加载主题分类 | `data/topics.json` | `List[Dict]`（含 `parent_topic` + `sub_topics`） |
| `load_sentiment_attributes` | 加载情感属性列表 | `data/sentiment_attributes.json` | `List[str]` |
| `load_publisher_objects` | 加载发布者类型列表 | `data/publisher_objects.json` | `List[str]` |
| `load_belief_system` | 加载信念体系分类 | `data/believe_system_common.json` | `List[Dict]`（含 `category` + `subcategories`） |
| `load_publisher_decisions` | 加载事件关联身份分类 | `data/publisher_decision.json` | `List[Dict]` |
| `save_enhanced_blog_data` | 保存增强后数据 | 数据 + 路径 | `bool` |
| `load_enhanced_blog_data` | 加载增强后数据 | 路径 | `List[Dict]` |
| `save_analysis_results` | 保存分析结果 | 结果字典 + 路径 | `bool` |
| `load_analysis_results` | 加载分析结果 | 路径 | `Dict` |
| `check_stage_output_exists` | 检查阶段输出文件 | 阶段编号 | `Dict[str, bool]` |
| `get_sample_posts` | 博文抽样 | 数据 + 数量 + 策略 | `List[Dict]` |

### 2.2 `save_enhanced_blog_data` 原子写入

```python
# 先写入临时文件，再原子替换
temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
json.dump(data, temp_file, ensure_ascii=False, indent=2)
os.replace(temp_file.name, output_path)  # 原子替换
```

### 2.3 `check_stage_output_exists` 检查项

| 阶段 | 检查文件 |
|:---|:---|
| 1 | `data/enhanced_blogs.json` |
| 2 | `report/analysis_data.json`, `report/chart_analyses.json`, `report/insights.json`, `report/images/` |
| 3 | `report/report.md` |

### 2.4 `get_sample_posts` 三种抽样策略

| 策略 | 算法 | 适用场景 |
|:---|:---|:---|
| `random` | `random.sample()` | 通用抽样 |
| `influential` | 按 `repost_count + comment_count + like_count` 降序排列取 Top N | 高影响力案例 |
| `diverse` | 按情感极性分组 → 每组等比例抽取 → 随机填充不足部分 | 确保多样性覆盖 |

---

## 3. 辅助工具

### 3.1 `console_safe.py`

解决 Windows 控制台 Unicode 编码问题：

| 函数 | 说明 |
|:---|:---|
| `safe_print(text)` | 捕获 `UnicodeEncodeError`，用 ASCII 替换无法显示的字符 |
| `format_status_indicator(status)` | 返回 `[OK]` / `[X]`，避免 Emoji/特殊字符 |

### 3.2 `extract_test_posts.py`

从 `data/posts.json` 提取前 N 条数据，保存为 `data/test_posts.json`。

- 默认提取 100 条
- 打印统计信息：总条目数、有图片条目数、总图片数
- 显示前 3 条预览

### 3.3 `fix_posts_images.py`

修复博文数据中的图片数量（限制最多 3 张）：

- 遍历所有博文，截断 `image_urls` 至 ≤ 3 条
- 自动创建 `_backup.json` 备份文件
- 打印修复统计信息
