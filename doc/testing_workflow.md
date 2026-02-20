# 测试工作流指南

> **Last Updated**: 2026-02-20  
> **关联源码**: `tests/` 目录  
> **上级文档**: [系统设计总览](design.md)

本文档定义当前项目的 TDD 测试分层、执行方式与重构规则。所有代码重构应遵循本指南与 `AGENTS.md`。

## 1. 测试架构（分层 + 按域分组）

```text
tests/
├── conftest.py
├── fixtures/
├── unit/
│   ├── core/       # config/dispatcher/path/state/基础工具
│   ├── stage1/     # Stage 1 节点与分析逻辑
│   ├── stage2/     # Stage 2 agent/tool 执行链路
│   ├── stage3/     # Stage 3 报告生成与清理逻辑
│   └── tools/      # 分析工具与工具注册/MCP 暴露一致性
├── integration/
│   ├── io/         # 数据文件与加载器契约测试
│   └── pipeline/   # 多节点串联与 shared 流转
└── e2e/
    └── cli/        # config.yaml 驱动的完整生命周期端到端（真实 API）
```

## 2. 执行命令

### 2.1 分层执行（推荐）

```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
uv run pytest tests/e2e -v
```

### 2.2 全量执行（任务完成前必须）

```bash
uv run pytest tests/ -v
```

若本地未配置 live API key，可先执行：

```bash
uv run pytest tests/ -v -m "not live_api"
```

## 3. E2E 与外部依赖策略

- `tests/e2e/` 默认执行，不使用 Mock。
- E2E 必须调用真实外部依赖（包括真实 GLM API）。
- E2E 配置来源为项目预留 `config.yaml`，仅在测试运行时覆盖 `runtime`、输出路径与 Stage2 循环上限。
- E2E 的 GLM API Key 优先使用 `config.yaml.llm.glm_api_key`，若为空则回退环境变量 `GLM_API_KEY`；两者都缺失时直接失败（`pytest.fail`）。
- Tavily live E2E 使用 `config.yaml.stage2.search_api_key`，若为空则回退环境变量 `TAVILY_API_KEY`。
- E2E 同时覆盖两条真实入口：Dashboard 后端 API 与 CLI 入口。
- Live API E2E 的默认平衡档循环限制（在 `tests/e2e/cli/_e2e_config_runtime.py` 注入）：
  - `stage2.agent_max_iterations=3`
  - `stage2.search_reflection_max_rounds=2`
  - `stage2.forum_max_rounds=3`
- E2E 验证至少覆盖：
  - 进程退出码
  - `stdout`/`stderr` 关键输出
  - 全链路产物文件（增强数据、Stage2 分析输出、最终报告）
- Windows 长跑场景建议附带 `--basetemp=.pytest_tmp_local/<case>`，避免临时目录锁冲突。

## 4. 迁移后的关键用例映射

| 关注模块 | 当前测试文件 |
|---|---|
| 配置加载/校验 + A3 搜索配置映射 | `tests/unit/core/test_config.py` |
| 仓库配置安全（禁止真实 key 入仓） | `tests/unit/core/test_no_real_secrets.py`, `tests/unit/core/test_e2e_config_runtime_keys.py` |
| A3 搜索封装（Tavily） | `tests/unit/core/test_web_search.py` |
| Dispatcher 调度与阶段切换 | `tests/unit/core/test_dispatcher.py` |
| Stage 1 节点/六维增强/NLP | `tests/unit/stage1/test_stage1_nodes.py`, `tests/unit/stage1/test_stage1_analysis.py`, `tests/unit/stage1/test_stage1_nlp_pipeline.py` |
| Stage 2 Agent 决策与执行 | `tests/unit/stage2/test_stage2_agent.py`, `tests/unit/stage2/test_stage2_execute_tools.py`, `tests/unit/stage2/test_stage2_ensure_charts.py`, `tests/unit/stage2/test_stage2_data_agent_trace.py` |
| Stage 2 QuerySearchFlow + SearchAgent + 并行桥接 | `tests/unit/stage2/test_stage2_search_flow.py`, `tests/unit/stage2/test_stage2_search_agent.py`, `tests/unit/stage2/test_stage2_parallel_flow.py` |
| Stage 2 Forum 动态循环（B5~B8） | `tests/unit/stage2/test_stage2_forum.py`, `tests/unit/stage2/test_stage2_supplement.py`, `tests/unit/stage2/test_stage2_visual.py`, `tests/unit/stage2/test_stage2_merge.py`, `tests/unit/stage2/test_stage2_chart_analysis_gapfill.py` |
| Stage 2 洞察语义兼容（analysis_content/analysis） | `tests/unit/stage2/test_stage2_insight.py` |
| Stage 3 统一报告链路（Outline/Chapters/Review/Trace/Methodology/Render） | `tests/unit/stage3/test_stage3_outline.py`, `tests/unit/stage3/test_stage3_chapters.py`, `tests/unit/stage3/test_stage3_review.py`, `tests/unit/stage3/test_stage3_trace_inject.py`, `tests/unit/stage3/test_stage3_methodology.py`, `tests/unit/stage3/test_stage3_render_html.py`, `tests/unit/stage3/test_stage3_report.py`, `tests/unit/stage3/test_report_cleanup.py`, `tests/unit/stage3/test_report_image_fallback.py` |
| 工具注册与 MCP 暴露一致性 | `tests/unit/tools/test_tool_registry.py`, `tests/unit/tools/test_mcp_tool_exposure.py` |
| I/O 契约与数据格式 | `tests/integration/io/test_data_loader_integration.py`, `tests/integration/io/test_data_format_contract.py`, `tests/integration/io/test_json_reference_contract.py` |
| 跨阶段串联集成 | `tests/integration/pipeline/test_flow_pipeline_integration.py`, `tests/integration/pipeline/test_stage2_forum_pipeline_integration.py`, `tests/integration/pipeline/test_stage3_unified_pipeline_integration.py` |
| 真实 API + 完整生命周期端到端 | `tests/e2e/cli/test_dashboard_pipeline_e2e.py`, `tests/e2e/cli/test_cli_pipeline_e2e.py` |
| Tavily 搜索真实 API E2E | `tests/e2e/cli/test_tavily_live_api_e2e.py` |
| E2E 运行时限额配置校验 | `tests/e2e/cli/test_e2e_runtime_profile.py` |
| Dashboard 配置读写/默认合并 | `dashboard/tests/test_pipeline_api.py` |

## 5. 重构中的测试流程（Red-Green-Refactor）

1. **先写测试/更新测试**：先定义期望行为。  
2. **验证 RED**：`uv run pytest <target>` 必须先失败。  
3. **实现最小改动**：仅补足使测试通过的代码。  
4. **验证 GREEN**：目标测试通过。  
5. **回归验证**：至少执行受影响层级；任务收尾执行 `uv run pytest tests/ -v`。  

## 6. 失败诊断原则

测试失败时按顺序判断：

1. 需求是否变化（原始需求/文档是否更新）。  
2. 测试断言是否表达了正确需求。  
3. 代码实现是否违背需求。  
4. 结论：修代码、修测试或两者都修，并记录原因。  

禁止“盲修”：

- 仅为过测删除断言/删除用例；
- 用 `assert True` 或 `pass` 替代真实验证；
- 在未定位根因时随意修改期望值。

## 7. 开发过程文件与 Git 忽略约定

为避免将开发过程临时文件误提交到仓库，`.gitignore` 需要至少覆盖以下内容：

- 测试缓存与临时目录：`.pytest_cache/`、`.pytest_tmp/`、`.pytest_tmp_local/`
- 开发过程记录：`/dev_progress.md`、`/development_plan.md`

如果文件在新增忽略规则前已经被 Git 跟踪，需要额外执行：

```bash
git rm --cached <file>
```
