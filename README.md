# Intelligence Analysis Agent (舆情分析智能体)

一个基于 LLM 和 PocketFlow 的自动化舆情分析系统，能够对大规模社交媒体数据进行深度挖掘，并生成专业的研究报告。

## 快速开始

### 环境依赖
本项目使用 `uv` 进行依赖管理。

```bash
# 安装依赖
uv sync

# 设置 API Key
$env:GLM_API_KEY="your_api_key"
```
也可在 `config.yaml` 中设置：
```yaml
llm:
  glm_api_key: "your_api_key"
```
Dashboard 里也支持配置（会写回 `config.yaml`）。当 YAML 中提供 key 时，会覆盖环境变量。

### 运行
```bash
# 修改 config.yaml 后运行
uv run analysis
# 或
uv run main.py
```

### Dashboard (Streamlit)
```bash
# 启动前端界面（推荐，确保使用 .venv 并设置 PYTHONPATH）
powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1

# 或临时方式
$env:PYTHONPATH = (Resolve-Path .)
.\.venv\Scripts\streamlit.exe run dashboard/app.py
```

### 样本数据运行（30 条）
默认 `config.yaml` 已指向：
- `data/posts_sample_30.json`
- `data/enhanced_posts_sample_30.json`

### 报告导出
Stage3 仅输出：
- `report/report.md`

### Troubleshooting
- **Stage2 工具发现失败 / tool_count=0 / no_candidate_tool**：
  - 先在项目根目录执行 `uv sync` 安装依赖（如 `matplotlib`, `fastmcp`, `mcp`）。
  - 运行时务必使用 `uv run analysis` 或 `uv run main.py`，避免使用系统 Python。
  - 若依旧失败，检查报错信息中的 “模块导入错误”，优先修复缺失依赖或导入异常。

## 文档索引

- **[文档总览](doc/README.md)** — `doc/` 目录导航与文档入口
- **[系统设计总览](doc/design.md)** — 核心文档：系统架构、`shared` 数据结构、中央调度器、Flow 编排、入口配置
- **[Agent 开发指南](AGENTS.md)** — Agentic Coding 规范及核心原则
- **子系统文档**:
    - [阶段 1：数据增强子系统](doc/stage1_enhancement.md) — 六维增强、异步并行、断点续传
    - [阶段 2：深度分析子系统](doc/stage2_analysis.md) — Agent + MCP 模式、分析工具调用、图表分析
    - [阶段 3：报告生成子系统](doc/stage3_report.md) — 模板/迭代模式、评审循环、图片路径处理
- **工具与基础设施**:
    - [分析工具库文档](doc/analysis_tools.md) — 37 个情感/主题/地理/交互/信念分析工具
    - [工具函数文档](doc/utils.md) — LLM 调用层、数据加载层、辅助脚本
    - [MCP 协议集成](doc/mcp_integration.md) — MCP 服务端/客户端、工具注册、通信流程
- **测试**:
    - [测试工作流指南](doc/testing_workflow.md) — 测试架构、源码↔测试映射、重构工作流

### 测试命令（TDD）
```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
uv run pytest tests/e2e -v
uv run pytest tests/ -v
```

> `tests/e2e/` 默认调用真实 API，并直接使用 `config.yaml` 的真实配置（含 `llm.glm_api_key`）。

## 主要特性

- **三阶段流水线**: 数据增强 -> 深度分析 -> 报告生成。
- **灵活调度**: 支持异步并发与 Agent 自主分析等多种模式。
- **高性能**: 基于 PocketFlow 框架，轻量且高效。
