# 舆情分析智能体 — 系统设计总览

> **文档状态**: 2026-02-11 更新  
> **适用版本**: v0.1.0 → v1.0.0 (Phase 1–3)  
> **类型**: 核心设计文档（全景地图）

本文档是项目的**核心设计参考**，提供系统级的架构、数据流与配置概览。各子系统的**代码级实现细节**请参阅对应的分支文档（见 [§8 文档索引](#8-文档索引)）。

---

## 1. 项目概述

### 1.1 定位

本项目是一套**自动化舆情分析系统**（Intelligence Analysis Agent），面向社交媒体博文数据，实现从原始数据清洗到专业分析报告输出的全流程自动化。

### 1.2 技术选型

| 维度 | 选型 | 说明 |
|:---|:---|:---|
| 编排框架 | **PocketFlow** | 轻量级异步工作流引擎，`Node → Flow` 抽象 |
| LLM 能力 | **智谱 GLM 系列** | 4 个模型分别覆盖文本分析、视觉理解、深度推理 |
| 工具协议 | **MCP (Model Context Protocol)** | Agent 模式下的工具发现与调用 |
| 可视化 | **Matplotlib** | 生成 PNG 格式分析图表 |
| 数据处理 | **Pandas + NumPy** | DataFrame 统计、矩阵运算 |

### 1.3 设计理念

1. **中央调度 + 阶段解耦**：`DispatcherNode` 统一管理三阶段的执行顺序，各阶段封装为独立 Flow，互不依赖。
2. **阶段2固定 Agent + MCP**：Stage2 仅保留 Agent 模式，通过 MCP 进行工具发现与调用。
3. **单一数据源 (`shared`)**：所有节点通过共享字典 `shared` 通信，无隐式状态。
4. **容错与断点续传**：阶段 1 的长时间批处理支持 Checkpoint 机制，中断后可恢复。

---

## 2. 系统架构

### 2.1 核心架构图

```mermaid
graph TD
    User["用户入口<br>(main.py)"] -->|"初始化 shared"| Dispatcher["DispatcherNode<br>(中央调度)"]
    
    Dispatcher -->|"stage1_async"| S1A["AsyncEnhancementFlow<br>(异步并行增强)"]
    
    Dispatcher -->|"stage2_agent"| S2B["AgentAnalysisFlow<br>(自主探索分析)"]
    
    Dispatcher -->|"stage3_template"| S3A["TemplateReportFlow<br>(一次性生成)"]
    Dispatcher -->|"stage3_iterative"| S3B["IterativeReportFlow<br>(多轮迭代)"]
    
    S1A -->|"dispatch"| Dispatcher
    S2B -->|"dispatch"| Dispatcher
    S3A -->|"dispatch"| Dispatcher
    S3B -->|"dispatch"| Dispatcher
    
    Dispatcher -->|"done"| Terminal["TerminalNode<br>(结束)"]
```

### 2.2 数据流全景

```mermaid
flowchart LR
    subgraph 输入数据
        RAW["data/posts.json<br>(原始博文)"]
        REF["data/topics.json<br>data/sentiment_attributes.json<br>data/publisher_objects.json<br>data/believe_system_common.json<br>data/publisher_decision.json"]
    end

    subgraph "阶段1: 数据增强"
        S1["6维 LLM 分析 + 本地NLP<br>情感极性 · 情感属性<br>主题 · 发布者 · 信念 · 身份<br>关键词 · 实体 · 词典情感 · 相似度聚类"]
    end

    subgraph "阶段2: 深度分析"
        S2["统计分析 + 可视化<br>图表生成 · 图表读图 · 洞察"]
    end

    subgraph "阶段3: 报告生成"
        S3["Markdown 报告编排<br>图文整合 · 格式化"]
    end

    subgraph 输出
        ENH["data/enhanced_blogs.json"]
        CHARTS["report/images/*.png"]
    ANALYSIS["report/analysis_data.json<br>report/chart_analyses.json<br>report/insights.json"]
    REPORT["report/report.md"]
    end

    RAW --> S1
    REF --> S1
    S1 --> ENH
    ENH --> S2
    S2 --> CHARTS
    S2 --> ANALYSIS
    CHARTS --> S3
    ANALYSIS --> S3
    S3 --> REPORT
```

**输出路径说明**：
- 图表输出目录由 `PathManager` 统一管理（`report/images/`）
- 运行状态写入 `report/status.json` 供 Dashboard 监控

### 2.3 PocketFlow 节点类型

项目使用 PocketFlow 提供的 4 种基础节点类型：

| 类型 | 基类 | 用途 | 项目中的典型节点 |
|:---|:---|:---|:---|
| 同步节点 | `Node` | 简单的串行处理 | `DataLoadNode`、`SaveEnhancedDataNode` |
| 批处理节点 | `BatchNode` | 一次处理多条数据 | — |
| 异步节点 | `AsyncNode` | 异步 I/O（LLM 调用） | — |
| 异步批处理节点 | `AsyncNode + BatchNode` | 并发批量处理 | `AsyncParallelBatchNode`（自定义基类） |

所有节点遵循统一的 **`prep → exec → post`** 三阶段生命周期：
- **`prep(shared)`**：从 `shared` 读取本节点需要的数据
- **`exec(prep_res)`**：执行核心逻辑（纯函数，不操作 `shared`）
- **`post(shared, prep_res, exec_res)`**：将结果写回 `shared`，返回 Action 字符串决定下一跳

---

## 3. 中央调度器 — `DispatcherNode`

`DispatcherNode` 是整个系统的**唯一入口节点**和**路由中心**，实现了一个简单的状态机。

### 3.1 状态机逻辑

```mermaid
stateDiagram-v2
    [*] --> 首次进入: current_stage == 0
    首次进入 --> 执行阶段: 从 start_stage 开始
    执行阶段 --> 查找下一阶段: 阶段完成返回 dispatch
    查找下一阶段 --> 执行阶段: run_stages 中存在未完成阶段
    查找下一阶段 --> done: 所有阶段完成
    done --> [*]
```

### 3.2 Action 路由表

| Action 字符串 | 目标 Flow | 触发条件 |
|:---|:---|:---|
| `stage1_async` | `AsyncEnhancementFlow` | `next_stage == 1` 且 `enhancement_mode == "async"` |
| `stage2_agent` | `AgentAnalysisFlow` | `next_stage == 2` |
| `stage3_template` | `TemplateReportFlow` | `next_stage == 3` 且 `report_mode == "template"` |
| `stage3_iterative` | `IterativeReportFlow` | `next_stage == 3` 且 `report_mode == "iterative"` |
| `done` | `TerminalNode` | 所有计划阶段已完成 |

### 3.3 `prep → exec → post` 实现

- **`prep`**：读取 `shared["dispatcher"]` 和 `shared["config"]`，提取 `start_stage`、`run_stages`、`current_stage`、`completed_stages` 与阶段模式配置
- **`exec`**：计算下一个未完成阶段编号；Stage1/Stage3 使用 `stage{N}_{mode}`，Stage2 固定返回 `stage2_agent`
- **`post`**：更新 `shared["dispatcher"]["current_stage"]`，返回 Action

各阶段的 CompletionNode 在执行完毕后会将阶段编号追加到 `shared["dispatcher"]["completed_stages"]`，然后返回 `"dispatch"` 跳回 DispatcherNode。

---

## 4. Flow 编排 (`flow.py`)

`flow.py` 定义了 **4 个子 Flow** 和 **1 个主 Flow**。所有 Flow 由 `create_main_flow()` 统一注册到 DispatcherNode 的 Action 路由中。

### 4.1 Flow 清单

| 函数 | 类型 | 节点链路 |
|:---|:---|:---|
| `_create_async_enhancement_flow` | `AsyncFlow` | DataLoad → SentimentPolarity → SentimentAttribute → Topic → Publisher → Belief → PublisherDecision → Save → Validate → Complete |
| `_create_agent_analysis_flow` | `AsyncFlow` | LoadEnhanced → Summary → CollectTools → Decision ⇄ Execute ⇄ Process → ChartAnalysis → LLMInsight → SaveResults → Complete |
| `_create_template_report_flow` | `Flow` | LoadResults → GenerateFullReport → Format → Save → Complete |
| `_create_iterative_report_flow` | `AsyncFlow` | LoadResults → InitState → Generate ⇄ Review ⇄ ApplyFeedback → Format → Save → Complete |

> 注：`⇄` 表示存在循环路径

### 4.2 主 Flow `create_main_flow()`

```python
create_main_flow(
    concurrent_num: int = 60,   # 异步最大并发数
    max_retries: int = 3,       # 节点最大重试次数
    wait_time: int = 8          # 重试等待时间（秒）
) -> AsyncFlow
```

> `concurrent_num` 作为 Stage 1 异步增强的并发上限（传入所有 Stage1 的 AsyncParallelBatchNode）。

此函数创建 DispatcherNode + TerminalNode，实例化 4 个子 Flow，并通过 PocketFlow 的 `>>` 操作符连接 Action 路由：

```python
dispatcher - "stage1_async" >> async_enhancement_flow
async_enhancement_flow - "dispatch" >> dispatcher
# ... 其余 3 个子 Flow 同理
dispatcher - "done" >> terminal
```

### 4.3 独立运行入口

为方便调试，`flow.py` 还提供了两个独立运行函数：
- `create_stage2_only_flow()` — 跳过阶段 1，直接运行阶段 2
- `create_stage3_only_flow(report_mode)` — 跳过阶段 1 和 2，直接运行阶段 3

---

## 5. 核心数据结构 `shared`

`shared` 字典是所有节点间通信的**唯一数据总线**，由 `config.py` 的 `config_to_shared()` 初始化。

### 5.1 顶级结构

| 键 | 类型 | 说明 | 填充时机 |
|:---|:---|:---|:---|
| `data` | Dict | 博文数据 + 参考数据 + 数据路径 | `init_shared` 初始化 → `DataLoadNode` 填充数据 |
| `dispatcher` | Dict | 调度器状态控制 | `init_shared` 初始化 → `DispatcherNode` 读写 |
| `config` | Dict | 全局配置（模式、路径、阈值） | `init_shared` 初始化（只读） |
| `agent` | Dict | Stage 2 Agent 循环运行时状态 | `init_shared` 初始化 → Agent 节点读写 |
| `report` | Dict | Stage 3 报告迭代状态 | `init_shared` 初始化 → 报告节点读写 |
| `stage1_results` | Dict | 阶段 1 统计与保存状态 | 阶段 1 节点填充 |
| `stage2_results` | Dict | 阶段 2 图表、洞察产出 | 阶段 2 节点填充 |
| `stage3_results` | Dict | 阶段 3 报告状态与输出路径 | 阶段 3 节点填充 |
| `monitor` | Dict | 系统监控日志 | 各阶段节点追加 |
| `thinking` | Dict | LLM 思考过程记录 | Stage 2/3 LLM 节点填充 |

### 5.2 `config` 子结构详情

`config` 是运行时只读的配置字典，结构如下：

```
config
├── enhancement_mode         "async"
├── analysis_mode            "agent"
├── report_mode              "template" | "iterative"
├── tool_source              "mcp"
├── stage1_checkpoint
│   ├── enabled              bool (默认 True)
│   ├── save_every           int  (默认 100，每 N 条保存一次)
│   └── min_interval_seconds float (默认 20 秒)
├── stage1_nlp
│   ├── enabled              bool (默认 True)
│   ├── keyword_top_n         int  (默认 8)
│   ├── similarity_threshold  float (默认 0.85)
│   └── min_cluster_size      int  (默认 2)
├── agent_config
│   └── max_iterations       int  (默认 10)
├── iterative_report_config
│   ├── max_iterations       int  (默认 5)
│   ├── satisfaction_threshold int (默认 80)
│   ├── enable_review        bool
│   └── quality_check        bool
├── data_source
│   ├── type                 "original" | "enhanced"
│   ├── resume_if_exists     bool (默认 True)
│   └── enhanced_data_path   str
```

### 5.3 `data` 子结构详情

```
data
├── blog_data[]              博文数组（原始 → 增强后覆盖写回）
│   ├── content              str   博文正文
│   ├── publish_time         str   发布时间
│   ├── user_id              str   用户ID
│   ├── username             str   用户名
│   ├── repost_count         int   转发数
│   ├── comment_count        int   评论数
│   ├── like_count           int   点赞数
│   ├── images[]             list  图片URL列表
│   ├── ip_location          str   IP归属地
│   │   ── [增强字段] ──
│   ├── sentiment_polarity   int   情感极性 (1-5)
│   ├── sentiment_attribute  list  情感属性标签
│   ├── topics[]             list  [{parent, sub}] 二级主题
│   ├── publisher            str   发布者类型
│   ├── belief_signals       list  信念体系信号
│   └── publisher_decision   str   事件关联身份
├── topics_hierarchy[]       主题层次结构（参考数据）
├── sentiment_attributes[]   情感属性列表（参考数据）
├── publisher_objects[]      发布者类型列表（参考数据）
├── belief_system[]          信念体系分类（参考数据）
├── publisher_decisions[]    事件身份列表（参考数据）
└── data_paths{}             各文件路径映射
```

### 5.4 `stage2_results` 子结构详情

```
stage2_results
├── charts[]                 图表列表
│   ├── id                   str  图表标识符
│   ├── title                str  图表标题
│   ├── file_path            str  PNG 文件路径
│   ├── source_tool          str  生成该图表的工具名
│   └── analysis             str  GLM-4.5V 读图分析文本
├── tables[]                 数据表格列表
├── insights{}               LLM 生成的分析洞察
│   ├── sentiment_insight    str  情感趋势洞察
│   ├── topic_insight        str  主题演化洞察
│   ├── geographic_insight   str  地理分布洞察
│   ├── cross_dimension_insight str 多维交互洞察
│   └── summary_insight      str  综合洞察摘要
├── execution_log{}          执行记录
└── output_files{}           输出文件路径
```

### 5.5 `stage3_results` 子结构详情

```
stage3_results
├── generation_mode          "template" | "iterative"
├── current_draft            当前报告草稿（迭代/模板中间态）
├── final_report_text        最终 Markdown 文本
└── report_file              报告文件路径（report/report.md）
```

---

## 6. 入口配置（`config.yaml` + `config.py`）

### 6.1 `config.yaml` 结构

```yaml
  data:
    input_path: "data/posts_sample_30.json"
    output_path: "data/enhanced_posts_sample_30.json"
    resume_if_exists: true
    topics_path: "data/topics.json"
    sentiment_attributes_path: "data/sentiment_attributes.json"
    publisher_objects_path: "data/publisher_objects.json"
    belief_system_path: "data/believe_system_common.json"
    publisher_decision_path: "data/publisher_decision.json"

  llm:
    glm_api_key: ""

pipeline:
  start_stage: 1
  run_stages: [1, 2, 3]

stage1:
  mode: "async"
  checkpoint:
    enabled: true
    save_every: 100
    min_interval_seconds: 20
  nlp:
    enabled: true
    keyword_top_n: 8
    similarity_threshold: 0.85
    min_cluster_size: 2

stage2:
  mode: "agent"
  tool_source: "mcp"
  agent_max_iterations: 10

stage3:
  mode: "template"   # template | iterative
  max_iterations: 5
  min_score: 80

runtime:
  concurrent_num: 60
  max_retries: 3
  wait_time: 8
```

### 6.2 `config.py` 入口流程

`main.py` 中的启动流程如下：

1. `load_config("config.yaml")` 读取配置  
2. `validate_config(config)` 执行前置条件校验  
3. `config_to_shared(config)` 构建 `shared`  
4. `asyncio.run(run(shared, **runtime))` 启动主 Flow

### 6.3 典型运行示例

```yaml
# 仅运行阶段2 + 3（阶段1已完成）
pipeline:
  start_stage: 2
  run_stages: [2, 3]

stage2:
  mode: "agent"
  tool_source: "mcp"
```

```yaml
# Agent 模式分析 + MCP（可调迭代次数）
stage2:
  mode: "agent"
  tool_source: "mcp"
  agent_max_iterations: 40
```

### 6.4 前置条件检查

  `validate_config()` 自动检查：
  - 仅运行阶段2/3时，增强数据文件必须存在  
  - 仅运行阶段3时，`report/analysis_data.json` / `report/chart_analyses.json` / `report/insights.json` 必须存在  
  - Agent + MCP 模式下需要 `ENHANCED_DATA_PATH`（`main.py` 会自动设置）
  - 需要 GLM API Key（优先使用 `llm.glm_api_key`，否则读取环境变量 `GLM_API_KEY`）

---

## 7. 三阶段概览

> 以下为各阶段的概要说明。详细的节点实现、Prompt 构建、算法逻辑请参阅分支文档。

### 7.1 阶段 1：数据增强

将原始博文增强为六维结构化数据。

| 模式 | Flow | 特点 |
|:---|:---|:---|
| `async` | `AsyncEnhancementFlow` | 异步并行调用 LLM，支持并发控制与 Checkpoint |

**六个增强维度**：情感极性 · 情感属性 · 二级主题 · 发布者类型 · 信念体系 · 事件关联身份  
**新增 NLP 维度**：关键词 · 命名实体 · 词典情感 · 相似度聚类

→ 详见 [阶段 1：数据增强子系统](stage1_enhancement.md)

### 7.2 阶段 2：深度分析

对增强数据进行统计分析、可视化生成和 LLM 洞察。

| 模式 | Flow | 特点 |
|:---|:---|:---|
| `agent` | `AgentAnalysisFlow` | LLM 自主决策分析，探索性强 |

→ 详见 [阶段 2：深度分析子系统](stage2_analysis.md)

### 7.3 阶段 3：报告生成

将分析结果编排为 Markdown 格式的分析报告。

| 模式 | Flow | 特点 |
|:---|:---|:---|
| `template` | `TemplateReportFlow` | 一次性长文本生成（推荐） |
| `iterative` | `IterativeReportFlow` | 生成 → 评审 → 修改的多轮循环 |

**报告输出**：Markdown (`report/report.md`)

→ 详见 [阶段 3：报告生成子系统](stage3_report.md)

---

## 8. 文档索引

| 文档 | 说明 |
|:---|:---|
| **本文档 `design.md`** | 系统全景地图：架构、数据流、配置 |
| [阶段 1：数据增强子系统](stage1_enhancement.md) | 六维分析节点、Checkpoint、Prompt 策略 |
| [阶段 2：深度分析子系统](stage2_analysis.md) | Agent + MCP 模式、决策循环、图表分析 |
| [阶段 3：报告生成子系统](stage3_report.md) | Template/Iterative 模式、图片路径处理 |
| [分析工具库](analysis_tools.md) | 5 类工具模块、算法实现、注册表 |
| [工具函数文档](utils.md) | LLM 调用层、数据加载、路径处理 |
| [MCP 协议集成](mcp_integration.md) | MCP Server/Client、工具分发 |
| [测试工作流](testing_workflow.md) | 测试架构、Mock 策略、运行指南 |
| **新增 `docs/design.md`** | 面向 AI 的高层设计总览 |

---

## 9. 开发环境与依赖

本项目使用 `uv` 进行依赖管理。

### 环境设置

```bash
pip install uv    # 安装 uv
uv venv           # 创建虚拟环境
uv sync           # 同步依赖
```

### 核心依赖 (`pyproject.toml`)

| 包 | 版本要求 | 用途 |
|:---|:---|:---|
| `pocketflow` | ≥ 0.0.3 | 核心编排框架 |
| `zai-sdk` | ≥ 0.0.4.2 | 智谱 AI 模型调用（GLM 系列） |
| `matplotlib` | ≥ 3.10.7 | 数据可视化 |
| `pandas` | ≥ 2.2.0 | DataFrame 数据处理 |
| `numpy` | ≥ 2.3.5 | 数值计算 |
| `mcp[cli]` | ≥ 1.22.0 | MCP 协议核心 |
| `fastmcp` | ≥ 2.13.0 | MCP 服务端框架 |
| `pydantic` | ≥ 2.0.0 | 数据验证 |
| `requests` | ≥ 2.32.5 | HTTP 客户端 |
| `pyyaml` | ≥ 6.0.2 | YAML 配置解析 |
