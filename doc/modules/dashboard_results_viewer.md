# Dashboard：Results Viewer 可追溯结果视图

> **文档状态**: 2026-02-21 更新  
> **关联源码**: `dashboard/pages/results_viewer.py`, `dashboard/logic/results_viewer_logic.py`  
> **上级文档**: [系统设计总览](../architecture.md), [测试工作流指南](../testing/testing_workflow.md)

---

## 1. 目标

Results Viewer 用于把一次运行的核心产物按来源完整展开，帮助人工核对“报告如何生成”：

- 图片结果（图表）
- Agent/MCP 工具输出的表格结果
- Forum 辩论轮次与决策过程
- 检索总结智能体输出
- 洞察到证据链的反向追溯
- 核心 JSON 原文展示与下载

---

## 2. 页面结构（单页多 Tab）

| Tab | 主要内容 | 数据来源 |
|:---|:---|:---|
| `Overview` | 核心计数、JSON 文件状态 | 聚合统计 + 核心 JSON 元信息 |
| `Image Results` | 图像预览、来源工具、图表解读 | `analysis_data.json.charts`, `chart_analyses.json`, `report/images/` 回退 |
| `Table Results` | 表格明细、执行日志 | `analysis_data.json.tables`, `analysis_data.json.execution_log` |
| `Forum Debate` | 每轮 decision/directive/gaps/conclusions | `trace.json.forum_rounds`, `trace.json.loop_status.forum` |
| `Search Summary` | 检索上下文、反思轮次、补检记录 | `analysis_data.json.search_context`, `trace.json.search_*` |
| `Evidence Chain` | insight 反查 supporting_evidence / executions / decisions | `insights.json`, `trace.json.insight_provenance`, `trace.json.executions`, `trace.json.decisions` |
| `JSON Files` | 核心文件原文预览 + 下载 | 5 个核心 JSON 文件 |

---

## 3. 核心 JSON 文件契约

Results Viewer 固定读取以下 5 个文件：

- `report/analysis_data.json`
- `report/chart_analyses.json`
- `report/insights.json`
- `report/trace.json`
- `report/status.json`

每个文件都输出统一元信息：

- `exists`
- `parse_ok`
- `size_bytes`
- `updated_at`
- `error`
- `text`（原文）
- `data`（解析后的 JSON）

---

## 4. 证据链视图（Insight -> Evidence -> Execution）

证据链以 `insights.json` 为入口；对每条洞察：

1. 查找 `trace.insight_provenance["insight_<key>"]`
2. 读取 `supporting_evidence`
3. 匹配 `trace.executions`（优先 `execution_id`，回退 `tool_name`）
4. 匹配 `trace.decisions`（优先 execution `decision_ref`，再看 `decision_id/tool_name`）

该视图用于快速验证：

- 洞察是否有直接证据
- 证据是否可回溯到执行记录
- 执行记录是否可回溯到决策步骤

---

## 5. 异常与回退策略

- 文件缺失：展示占位信息，不中断页面其他分区。
- 文件为空/非法 JSON：标记 `parse_ok=false` 并显示错误详情。
- 图表列表为空：回退读取 `report/images/*.png`。
- 非结构化表格数据：保留原始 JSON 展示，不强制 DataFrame 化。

---

## 6. 测试覆盖

新增测试文件：

- `dashboard/tests/test_results_viewer_logic.py`

覆盖场景：

- 来源分区聚合构建（图片/表格/Forum/检索/证据链）
- 核心 JSON 缺失与非法格式容错
- insight 证据链匹配执行与决策记录
- 图像目录回退逻辑
