---
description: 运行测试并根据结果验证代码修改的正确性
---

# 测试工作流

> 完整文档见 `doc/testing_workflow.md`

## 前置条件

项目使用 `uv` 管理的虚拟环境 `.venv`。pytest 已安装在 venv 中。

## 步骤

### 1. 运行全部单元测试

// turbo
```bash
.venv\Scripts\python.exe -m pytest tests\unit\ -v --tb=short
```

基线: **150 passed, 0 failed** (< 1s)

### 2. 分析测试结果

- 如果全部通过 → 修改安全，可以继续
- 如果有失败 → 按以下优先级排查：
  1. **源码 Bug**：修复源码，不改测试
  2. **预期的接口变更**：更新测试以匹配新接口，但必须保持测试的验证目的不变
  3. **Fixture 数据过时**：更新 `tests/fixtures/` 下的 JSON 文件
  4. **Mock 路径变更**：更新 `@patch("nodes.xxx")` 中的路径

### 3. 源码 → 测试映射速查

| 修改了... | 需要检查... |
|---|---|
| 工具函数 (`normalize_path`, `_strip_timestamp_suffix` 等) | `test_utils.py` |
| `DispatcherNode` / `TerminalNode` / CompletionNode | `test_dispatcher.py` |
| `DataLoadNode` / `SaveEnhancedDataNode` / `DataValidationAndOverviewNode` | `test_stage1_nodes.py` |
| 六维异步分析节点 (`AsyncSentiment*`, `AsyncTopic*` 等) | `test_stage1_analysis.py` |
| `LoadEnhancedDataNode` / `DataSummaryNode` / `ChartAnalysisNode` / `LLMInsightNode` | `test_stage2_workflow.py` |
| `CollectToolsNode` / `DecisionToolsNode` | `test_stage2_agent.py` |
| Stage 3 报告节点 | `test_stage3_report.py` |
| `shared` 字典结构 / 多节点串联 | `test_flow_integration.py` + `conftest.py` |

### 4. 只跑特定模块（可选）

// turbo
```bash
.venv\Scripts\python.exe -m pytest tests\unit\test_utils.py -v --tb=short
```

### 5. 测试修改规范

修改测试时必须遵守以下规则（详见 `doc/testing_workflow.md` §5）：

- ✅ 允许：更新参数/断言以匹配新接口，但保持验证目的不变
- ✅ 允许：新增测试覆盖新功能
- ❌ 禁止：删除测试用例来"解决"失败
- ❌ 禁止：不理解原因就修改断言值
- ❌ 禁止：将断言改为 `assert True` 或 `pass`

### 6. 拆分 nodes.py 后的注意事项

如果将 `nodes.py` 拆分为多个模块，所有 `@patch("nodes.xxx")` 需要更新为新的模块路径。同时更新 `conftest.py` 和各测试文件开头的 `from nodes import ...` 语句。
