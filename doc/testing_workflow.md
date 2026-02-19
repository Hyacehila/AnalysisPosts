# 测试工作流指南

> **Last Updated**: 2026-02-19  
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

## 3. E2E 与外部依赖策略

- `tests/e2e/` 默认执行，不使用 Mock。
- E2E 必须调用真实外部依赖（包括真实 GLM API）。
- E2E 配置来源为项目预留 `config.yaml`，仅在测试运行时覆盖 `runtime` 与输出路径。
- E2E 的 API Key 仅使用 `config.yaml.llm.glm_api_key`；缺失时直接失败（`pytest.fail`）。
- E2E 同时覆盖两条真实入口：Dashboard 后端 API 与 CLI 入口。
- E2E 验证至少覆盖：
  - 进程退出码
  - `stdout`/`stderr` 关键输出
  - 全链路产物文件（增强数据、Stage2 分析输出、最终报告）

## 4. 迁移后的关键用例映射

| 关注模块 | 当前测试文件 |
|---|---|
| Dispatcher 调度与阶段切换 | `tests/unit/core/test_dispatcher.py` |
| Stage 1 节点/六维增强/NLP | `tests/unit/stage1/test_stage1_nodes.py`, `tests/unit/stage1/test_stage1_analysis.py`, `tests/unit/stage1/test_stage1_nlp_pipeline.py` |
| Stage 2 Agent 决策与执行 | `tests/unit/stage2/test_stage2_agent.py`, `tests/unit/stage2/test_stage2_execute_tools.py`, `tests/unit/stage2/test_stage2_ensure_charts.py` |
| Stage 3 报告链路 | `tests/unit/stage3/test_stage3_report.py`, `tests/unit/stage3/test_report_cleanup.py`, `tests/unit/stage3/test_report_image_fallback.py` |
| 工具注册与 MCP 暴露一致性 | `tests/unit/tools/test_tool_registry.py`, `tests/unit/tools/test_mcp_tool_exposure.py` |
| I/O 契约与数据格式 | `tests/integration/io/test_data_loader_integration.py`, `tests/integration/io/test_data_format_contract.py`, `tests/integration/io/test_json_reference_contract.py` |
| 跨阶段串联集成 | `tests/integration/pipeline/test_flow_pipeline_integration.py` |
| 真实 API + 完整生命周期端到端 | `tests/e2e/cli/test_dashboard_pipeline_e2e.py`, `tests/e2e/cli/test_cli_pipeline_e2e.py` |

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
