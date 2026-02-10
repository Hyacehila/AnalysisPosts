# 测试工作流指南

> **文档状态**: 2026-02-10 更新  
> **关联源码**: `tests/` 目录  
> **上级文档**: [系统设计总览](design.md)

> 本文档面向参与本项目开发和重构的 Agent。测试套件是保障重构可行性的核心手段，请务必在每次代码修改后遵循本工作流。

## 1. 测试架构概览

```
tests/
├── conftest.py                    # 共享 Fixtures（数据 + Mock）
├── fixtures/                      # JSON 测试数据文件
│   ├── sample_posts.json          # 3 条原始博文
│   ├── sample_enhanced.json       # 3 条六维增强博文
│   ├── sample_topics.json         # 主题层次结构
│   ├── sample_sentiment_attrs.json
│   └── sample_publishers.json
├── unit/                          # 单元测试
│   ├── test_config.py             # P0 — YAML 配置加载/校验
│   ├── test_utils.py              # P0 — 纯工具函数
│   ├── test_dispatcher.py         # P0 — 调度器状态机
│   ├── test_stage1_nodes.py       # P1 — Stage 1 通用节点
│   ├── test_stage1_analysis.py    # P1 — Stage 1 六维分析节点
│   ├── test_stage2_workflow.py    # P2 — Stage 2 Workflow 路径
│   ├── test_stage2_agent.py       # P2 — Stage 2 Agent 路径
│   ├── test_stage3_report.py      # P3 — Stage 3 报告生成
│   └── test_flow_integration.py   # P4 — 端到端集成测试
├── test_data_format.py            # 数据格式验证（独立）
├── test_data_loader.py            # 数据加载器测试（独立）
├── test_json_files.py             # JSON 配置文件验证（独立）
└── test_llm_models.py             # LLM 在线测试（需 RUN_LLM_TESTS=1）
```

## 2. 运行测试

### 2.1 运行全部单元测试（推荐）

```bash
# 使用项目 venv
.venv/Scripts/python.exe -m pytest tests/unit/ -v --tb=short
```

当前基线: **153 passed, 0 failed** (< 1s)

### 2.2 运行特定模块

```bash
# 只跑工具函数测试
.venv/Scripts/python.exe -m pytest tests/unit/test_utils.py -v

# 只跑某个测试类
.venv/Scripts/python.exe -m pytest tests/unit/test_dispatcher.py::TestDispatcherExec -v

# 只跑某个测试方法
.venv/Scripts/python.exe -m pytest tests/unit/test_stage1_analysis.py::TestSentimentPolarityNode::test_exec_async_text_only -v
```

### 2.3 快速冒烟检查

```bash
# -x: 第一个失败即停止; -q: 简洁输出
.venv/Scripts/python.exe -m pytest tests/unit/ -x -q
```

### 2.4 根目录下的独立测试（可选）

这些测试依赖真实数据文件或 LLM API，不作为重构守护测试：

```bash
# 数据格式验证（需要 data/ 目录中的真实数据）
.venv/Scripts/python.exe -m pytest tests/test_data_format.py -v

# 数据加载器（需要真实 JSON 文件）
.venv/Scripts/python.exe -m pytest tests/test_data_loader.py -v

# LLM 在线测试（需要 RUN_LLM_TESTS=1，会产生 API 调用费用）
RUN_LLM_TESTS=1 .venv/Scripts/python.exe -m pytest tests/test_llm_models.py -v
```

## 3. 源码 ↔ 测试映射

修改源文件时，请参照此表确定需要检查的测试文件：

| 源文件 / 模块 | 测试文件 | 优先级 |
|---|---|---|
| `nodes/_utils.py` — `normalize_path`, `_strip_timestamp_suffix`, `_build_chart_path_index`, `_remap_report_images`, `ensure_dir_exists`, `get_project_relative_path` | `test_utils.py` | P0 |
| `nodes/dispatcher.py` — `DispatcherNode`, `TerminalNode`, `Stage{1,2,3}CompletionNode` | `test_dispatcher.py` | P0 |
| `nodes/stage1.py` — `DataLoadNode`, `SaveEnhancedDataNode`, `DataValidationAndOverviewNode` | `test_stage1_nodes.py` | P1 |
| `nodes/stage1.py` — `AsyncSentimentPolarityAnalysisBatchNode`, `AsyncSentimentAttributeAnalysisBatchNode`, `AsyncTwoLevelTopicAnalysisBatchNode`, `AsyncPublisherObjectAnalysisBatchNode`, `AsyncBeliefSystemAnalysisBatchNode`, `AsyncPublisherDecisionAnalysisBatchNode` | `test_stage1_analysis.py` | P1 |
| `nodes/stage2.py` — `LoadEnhancedDataNode`, `DataSummaryNode`, `ExecuteAnalysisScriptNode`, `ChartAnalysisNode`, `SaveAnalysisResultsNode`, `LLMInsightNode` | `test_stage2_workflow.py` | P2 |
| `nodes/stage2.py` — `CollectToolsNode`, `DecisionToolsNode` | `test_stage2_agent.py` | P2 |
| `nodes/stage3.py` — `LoadAnalysisResultsNode`, `FormatReportNode`, `SaveReportNode`, `GenerateFullReportNode`, `InitReportStateNode`, `ReviewReportNode` | `test_stage3_report.py` | P3 |
| 多节点串联行为、`shared` 字典流转、Dispatcher 多阶段调度 | `test_flow_integration.py` | P4 |
| `utils/data_loader.py` | `test_data_loader.py` | — |
| `utils/call_llm.py` | `test_llm_models.py` | — |

## 4. 在重构中使用测试

### 4.1 基本原则

```
修改代码 → 运行测试 → 分析失败 → 修复 or 更新测试 → 全部通过 → 提交
```

**关键规则：每次修改源代码后必须运行 `tests/unit/` 下的全部测试。** 不允许在测试未通过的状态下继续进行不相关的代码修改。

### 4.2 修改归类与测试策略

| 修改类型 | 操作 | 示例 |
|---|---|---|
| **Bug 修复** | 先写或更新测试覆盖 Bug 场景，然后修复源码，确认测试通过 | 修复 `_strip_timestamp_suffix` 的 regex 双转义 |
| **行为不变的重构** | 直接修改源码，运行测试，期望全部通过（0 个失败） | 提取函数、改变内部数据结构、优化性能 |
| **接口变更** | 修改源码后更新对应测试，遵循 §5 测试修改规范 | 修改 `DataLoadNode.prep` 的返回结构 |
| **新增功能** | 同步新增测试，确保覆盖 prep/exec/post | 新增分析节点 |
| **删除功能** | 删除对应测试，确保无悬挂引用 | 移除某个分析节点 |

### 4.3 重构工作流详细步骤

1. **开始前**：运行全部测试，确认基线全部通过
2. **修改代码**：进行一个原子性修改（尽量小粒度）
3. **运行测试**：`pytest tests/unit/ -v --tb=short`
4. **分析结果**：
   - ✅ 全部通过 → 继续下一个修改
   - ❌ 有失败 → 判断失败原因（见 §4.4）
5. **处理失败**：
   - 如果是源码 Bug → 修复源码
   - 如果是预期的接口变更 → 按 §5 更新测试
   - 如果是测试本身的问题 → 按 §5 谨慎修改
6. **确认全部通过后**再进行下一步修改

### 4.4 测试失败诊断

当测试失败时，按以下顺序排查：

```
1. 阅读错误信息 → 确定失败的测试方法和断言
2. 检查源码变更 → 是否改变了被测函数的签名、返回值或副作用
3. 区分原因：
   a. 源码 Bug  → 修复源码，测试不改
   b. 预期变更  → 按 §5 更新测试
   c. 测试数据过时 → 更新 fixtures/ 下的 JSON 文件
   d. Mock 不匹配 → 更新 Mock 配置（conftest.py 或测试内 @patch）
```

## 5. 测试修改规范

> ⚠️ **核心原则：修改测试必须保持谨慎。测试是项目正确性的守护者。**

### 5.1 允许的修改

| 场景 | 操作 | 要求 |
|---|---|---|
| 函数签名变更 | 更新测试的参数传递方式 | 保持测试的 **验证目的** 不变 |
| 返回值结构变更 | 更新断言中的字段名/路径 | 保持测试的 **验证逻辑** 不变 |
| 类/函数重命名 | 更新 import 和引用 | 保持测试的 **覆盖范围** 不变 |
| 新增字段 | 新增断言验证新字段 | 不删除现有断言 |
| Fixture 数据结构变更 | 更新 JSON fixture 文件 | 保持数据的 **代表性**（正常/边界/异常） |

### 5.2 禁止的修改

| 禁止操作 | 原因 |
|---|---|
| 删除测试用例以"修复"失败 | 掩盖了真实的 Bug 或回归 |
| 将断言改为 `assert True` 或 `pass` | 等同于删除测试 |
| 删除边界条件测试 | 边界条件往往是 Bug 高发区 |
| 不理解原因就修改断言值 | 可能引入错误的预期 |

### 5.3 修改测试的检查清单

在修改任何测试前，确认以下几点：

- [ ] 我理解这个测试验证的是什么行为
- [ ] 源码的变更是有意为之的（而非无意引入的 Bug）
- [ ] 修改后的测试仍然验证相同的行为语义
- [ ] 修改后的测试仍然覆盖正常路径 + 异常路径
- [ ] 如果删除了某个断言，有另一个测试覆盖该场景

## 6. 已知问题

### 6.1 `_strip_timestamp_suffix` regex Bug

**位置**: `nodes.py` 约第 93 行

**问题**: 正则表达式 `r"_\\d{8}_\\d{6}$"` 在 raw string 中使用了双转义 `\\d`，实际匹配的是字面的 `\d` 而非数字 `[0-9]`。该函数永远不会真正剥离时间戳后缀。

**测试中的处理**: `test_utils.py::TestStripTimestampSuffix` 中的测试标注了此行为（`test_with_digit_timestamp_not_stripped`），测试验证的是 **实际行为** 而非 **预期行为**。

**修复后**: 将 `r"_\\d{8}_\\d{6}$"` 改为 `r"_\d{8}_\d{6}$"`，然后更新测试断言以反映正确的剥离行为。

## 7. conftest.py 中的共享 Fixtures

| Fixture | 类型 | 说明 |
|---|---|---|
| `sample_blog_data` | 数据 | 3 条原始博文（无增强字段） |
| `sample_enhanced_data` | 数据 | 3 条增强博文（含六维标注） |
| `sample_topics` | 数据 | 主题层次结构 |
| `sample_sentiment_attrs` | 数据 | 情感属性列表 |
| `sample_publishers` | 数据 | 发布者类型列表 |
| `minimal_shared` | 结构 | 最小 `shared` 字典，驱动 Stage 1 节点 |
| `enhanced_shared` | 结构 | 含增强数据的 `shared` 字典，驱动 Stage 2/3 节点 |
| `mock_llm_calls` | Mock | 批量 Mock 全部 LLM 调用（air/4v/thinking/46） |

**修改注意**: 如果源码中 `shared` 字典的结构发生变更，请同步更新 `conftest.py` 中的 `minimal_shared` 和 `enhanced_shared` fixtures，以及 `tests/fixtures/` 下的 JSON 文件。

## 8. Mock 策略

本测试套件 Mock 了所有外部依赖，确保测试快速、确定性运行：

| Mock 目标 | 方式 | 说明 |
|---|---|---|
| LLM 调用 (`call_glm_45_air`, `call_glm4v_plus`, `call_glm45v_thinking`, `call_glm46`) | `@patch("nodes.stageX.xxx")` | 返回可控字符串/JSON |
| 文件 I/O (`load_blog_data`, `save_enhanced_blog_data` 等) | `@patch("nodes.stageX.xxx")` | 返回 fixture 数据 |
| MCP 工具 (`get_tools`, `set_mcp_mode`) | 通过 Mock `CollectToolsNode.exec` | 避免启动真实 MCP 服务 |
| 文件系统 (`os.path.exists`, `os.makedirs`) | `@patch` | 避免真实文件操作 |

**重构时**: `@patch("nodes.xxx")` 需指向具体模块，例如 `@patch("nodes.stage1.call_glm_45_air")`、`@patch("nodes.stage2.call_glm46")`。
