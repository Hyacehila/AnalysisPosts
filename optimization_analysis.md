# 项目全面调优分析报告

## 一、核心问题诊断

通过检查 [report/report.md](file:///d:/PythonProject/AnalysisPosts/report/report.md)、[report/insights.json](file:///d:/PythonProject/AnalysisPosts/report/insights.json)、[report/chart_analyses.json](file:///d:/PythonProject/AnalysisPosts/report/chart_analyses.json) 以及各节点源码，发现本次执行结果存在**三个根本性问题**：

### 问题 1：最终报告充斥占位符，未使用真实数据

[report.md](file:///d:/PythonProject/AnalysisPosts/report/report.md) 中大量出现 `[议题A]`、`[议题B]`、`[争议点]`、`[媒体A]`、`[地区C]`、`[关键事件]` 等通用占位文字，报告**完全没有反映真实的舆情事件**——即"首都骑游文明公约"、张艺兴夜骑、共享单车夜骑管理等实际话题。

与此同时，[insights.json](file:///d:/PythonProject/AnalysisPosts/report/insights.json) 里已存有完整的真实数据摘要（张艺兴、单车、骑行、2024-08-16 至 2024-08-31），[chart_analyses.json](file:///d:/PythonProject/AnalysisPosts/report/chart_analyses.json) 里也有15张图表的详细分析文字。**真实数据存在，但没有被传入最终报告的撰写阶段。**

### 问题 2：证据追溯全部为空（0条）

报告中每个洞察的"证据"章节均显示"无可用证据"。这是 [insight.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py) 中 [_match_evidence](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#178-196) 方法的问题：该方法用工具名称的分词去匹配洞察文本，由于洞察文本是中文而工具名是英文下划线命名（如 `sentiment_analysis`），几乎永远匹配失败，最终回退到 [_fallback_evidence](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#170-177) 但 trace 里的 execution 记录若也为空则返回 `[]`。

### 问题 3：论坛主持人（Forum）引导方向错误

[forum.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/forum.py) 的 [ForumHostNode](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/forum.py#118-264) 只传入了通用的 `data_summary`，没有告诉 LLM"这次事件是什么"。Forum 主持人不知道核心事件是"北京夜骑共享单车"，因此无法有针对性地提出信息缺口（如"张艺兴活动的官方回应"、"8月21日骑行量峰值原因"），导致搜索方向缺乏焦点，补充查询质量低。

---

## 二、各节点详细问题与优化建议

### 2.1 [nodes/stage3/outline.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/outline.py) — 大纲规划节点

**现状问题：**

```python
prompt = (
    "你是舆情分析报告专家。请规划统一报告大纲，输出 JSON。\n"
    f"图表数量: {len(charts)}\n"
    f"洞察键: {list((insights or {}).keys())}\n"
    f"论坛轮次: {len(prep_res.get('forum_rounds', []))}\n"
    "仅输出 JSON 对象。"
)
```

只传入了图表数量和洞察字段名，**没有传入洞察内容本身**，LLM 规划大纲时对真实事件一无所知，产出的章节标题是通用的"执行摘要"、"趋势与结构分析"，无法指导后续章节写出事件专属内容。

**优化方案：**

```python
# 注入洞察摘要、核心关键词、时间范围
insight_preview = "\n".join(
    f"- {k}: {str(v)[:200]}" for k, v in (insights or {}).items()
)
keywords = stage3_data.get("top_keywords", [])  # 从 Stage2 结果中提取
event_period = f"{start_date} 至 {end_date}"

prompt = (
    f"你是舆情分析报告专家。请基于以下洞察数据规划专项报告大纲。\n"
    f"【核心事件关键词】: {keywords}\n"
    f"【舆情时间范围】: {event_period}\n"
    f"【洞察摘要】:\n{insight_preview}\n"
    f"图表数量: {len(charts)}\n"
    "要求：大纲章节标题应反映真实事件名称，不得使用[议题X]等占位符。\n"
    "仅输出 JSON 对象。"
)
```

---

### 2.2 [nodes/stage3/chapters.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py) — 章节生成节点

**现状问题（最关键）：**

```python
prompt = (
    f"请撰写舆情分析报告章节。\n"
    f"关键数据点: {key_data}\n"
    f"可用图表: {[c.get('id') for c in relevant_charts]}\n"  # 只传了ID！
    "要求：\n"
    "1. 内容结构完整，数据驱动。\n"
    ...
)
```

`relevant_charts` 传入的是图表对象，但 prompt 里只提取了 `c.get('id')`（图表ID），**没有传入图表的分析内容（`analysis_content`）**。LLM 写章节时既看不到图表分析文字，也看不到 [insights.json](file:///d:/PythonProject/AnalysisPosts/report/insights.json) 的具体内容，只能凭空编造，产生 `[议题A]` 这类占位符。

同时 [_insights](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#160-169) 字段虽然挂在 `prep_res` 上，但 [exec_async](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py#56-97) 中**完全没有使用 [_insights](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#160-169)**。

**优化方案：**

```python
# 1. 传入图表完整分析内容（取前800字符）
chart_content_str = "\n\n".join(
    f"【{c.get('title', c.get('id'))}】\n{c.get('analysis_content', '')[:800]}"
    for c in relevant_charts
)

# 2. 传入洞察全文
insights = prep_res.get("_insights", {})
insights_str = "\n".join(
    f"- {k}: {str(v)[:300]}" for k, v in (insights or {}).items()
)

# 3. 传入搜索背景（事件时间线、关键人物）
search_context = prep_res.get("_search_context", {})

prompt = (
    f"请基于以下真实数据撰写舆情分析报告章节。\n"
    f"【章节标题】: {title}\n"
    f"【目标字数】: {target_words}\n\n"
    f"【图表分析数据】:\n{chart_content_str}\n\n"
    f"【洞察摘要】:\n{insights_str}\n\n"
    f"【搜索背景】: {json.dumps(search_context, ensure_ascii=False)[:500]}\n\n"
    "严格要求：\n"
    "1. 只使用上方提供的数据，禁止出现[议题X][争议点]等任何占位符。\n"
    "2. 所有数字、比例必须来自图表分析数据。\n"
    "3. 事件名称必须与数据中的真实关键词一致。\n"
)
```

---

### 2.3 [nodes/stage3/review.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/review.py) — 章节评审节点

**现状问题：**

评审提示词只有三行：

```python
prompt = (
    "请评审以下报告章节，返回 JSON。\n"
    "字段: score(0-100), needs_revision(bool), feedback(str)。\n"
    f"章节标题: {chapter_title}\n"
    f"章节内容:\n{chapter_text[:2500]}\n"
    "仅输出 JSON。"
)
```

评审者不知道正确答案是什么，也没有明确的质量标准。LLM 评审时缺乏基准数据对照，无法判断章节内容是否和真实数据一致，因此无法有效识别"内容使用了占位符"这类严重问题。

**优化方案：**

```python
prompt = (
    f"请评审以下舆情报告章节，返回 JSON。\n"
    f"字段: score(0-100), needs_revision(bool), feedback(str)。\n"
    f"章节标题: {chapter_title}\n\n"
    f"【参考数据（章节内容必须与此一致）】:\n{reference_data_str}\n\n"
    f"章节内容:\n{chapter_text[:2500]}\n\n"
    "评审重点（必须明确反馈每项）：\n"
    "1. 是否存在[议题X][争议点]等占位符？存在则 score < 40，必须修订。\n"
    "2. 数字和比例是否与参考数据吻合？\n"
    "3. 事件/人物名称是否准确（如张艺兴、首都骑游文明公约）？\n"
    "4. 内容是否达到目标字数且逻辑清晰？\n"
    "仅输出 JSON。"
)
```

---

### 2.4 [nodes/stage2/forum.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/forum.py) — 论坛主持人节点

**现状问题：**

```python
prompt = f"""你是舆情分析论坛主持人（第{round_index}轮）。请对双信源结果做交叉评估并给出下一步动作。

数据摘要：
{prep_res.get("data_summary", "")}
...
```

`data_summary` 只是原始数据的通用摘要，LLM 主持人不知道当前是第几轮也没有**明确的事件焦点**，导致：
- 第1轮就直接判断 `sufficient` 但还没完成应有的信息搜集
- 即使决定 `supplement_search`，生成的查询词也缺乏针对性

**优化方案：**

在 prep 中提取核心事件关键词（从 Stage1 结果），在 Forum 的 prompt 中明确：

```python
prompt = f"""你是舆情分析论坛主持人。任务：对"【{event_topic}】"舆情事件进行深度分析。

【当前轮次】: 第{round_index}轮 / 共{max_rounds}轮
【事件核心关键词】: {core_keywords}
【舆情时间范围】: {event_period}

DataAgent数据分析结果（来自原始数据）:
{json.dumps(data_agent_results, ensure_ascii=False)[:2500]}

SearchAgent搜索结果（来自外部搜索）:
{json.dumps(search_agent_results, ensure_ascii=False)[:2500]}

【你的分析任务】:
1. 评估两个信源是否互相印证了核心事件的关键节点
2. 识别仍存在的盲区（如：{event_topic}的官方回应、传播峰值原因等）
3. 若盲区存在，生成针对"{event_topic}"的具体搜索词

输出严格JSON：
...
"""
```

---

### 2.5 [nodes/stage2/insight.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py) — 证据追溯逻辑修复

**现状问题（[_match_evidence](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#178-196) 方法）：**

```python
for token in tool_name.split("_"):  # 英文工具名分词
    if len(token) > 2 and token in text_lower:  # 中文洞察文本
        matches.append(execution)
```

中文洞察文本不会包含英文工具名分词（如 "sentiment"），导致匹配永远失败，最终"证据"为空。

**优化方案：**

改用关键词映射策略：

```python
# 定义工具名到中文关键词的映射
TOOL_KEYWORD_MAP = {
    "sentiment": ["情感", "极性", "正面", "负面", "中性"],
    "topic": ["主题", "话题", "议题", "热度"],
    "geographic": ["地区", "省", "北京", "地域", "分布"],
    "publisher": ["发布者", "用户", "媒体", "KOL"],
    "interaction": ["互动", "转发", "评论", "点赞"],
}

def _match_evidence(insight_text, trace_executions):
    matches = []
    for execution in trace_executions:
        tool_name = str(execution.get("tool_name", "")).lower()
        for key, keywords in TOOL_KEYWORD_MAP.items():
            if key in tool_name and any(kw in insight_text for kw in keywords):
                matches.append(execution)
                break
    return matches
```

---

### 2.6 [nodes/stage2/search.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/search.py) — 搜索查询生成

**现状问题：**

[ExtractQueriesNode](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/search.py#76-142) 用数据摘要的头24个字符作为搜索关键词，过于粗糙：

```python
base = str(prep_res.get("data_summary", "")).strip()[:24]
fallback = [f"{base} 官方回应", f"{base} 事件进展", ...]
```

[SearchReflectionNode](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/search.py#240-339) 的评估提示词也没有传入具体搜索内容片段，无法有效判断覆盖度。

**优化方案：**

从 Stage1 的分析结果中提取高频关键词（单车、夜骑、张艺兴等），以此驱动搜索。同时在 [SearchReflectionNode](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/search.py#240-339) 中传入文档前3条的 snippet 让 LLM 实质评估：

```python
prompt = f"""请评估搜索结果是否充分覆盖了"{event_topic}"事件。

- 核心关键词: {core_keywords}
- 查询词: {queries}
- 文档样本:
{json.dumps(docs[:3], ensure_ascii=False)}

判断缺口时考虑：官方回应、事件时间线、关键人物立场。
"""
```

---

## 三、数据流追踪：真实数据在哪里"丢失"

```mermaid
graph LR
    A[原始数据\n30篇博文\n关键词:单车/张艺兴] -->|Stage1分析| B[insights.json\n真实洞察已生成]
    B -->|Stage3 LoadResults| C[stage3_data.insights\n数据加载到内存]
    C -->|outline.py| D[大纲节点\n❌ 只用了insights的key名]
    D -->|chapters.py| E[章节生成\n❌ insights完全未使用\n❌ 图表只传ID不传内容]
    E -->|review.py| F[评审\n❌ 没有参考基准\n无法识别占位符问题]
    F --> G[report.md\n充满占位符]
```

**核心断裂发生在 [chapters.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py) 的 [exec_async](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py#56-97) 函数**：[_insights](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#160-169) 字段从 [prep_async](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py#28-55) 传入 `prep_res`，但在构建 prompt 时完全被忽略。这是导致报告质量失败最直接的单点错误。

---

## 四、优化优先级建议

| 优先级 | 文件 | 改动类型 | 预期效果 |
|--------|------|---------|---------|
| 🔴 P0 | [nodes/stage3/chapters.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/chapters.py) | 在 prompt 中注入 insights 全文 + 图表分析内容 | 消除占位符，报告包含真实数据 |
| 🔴 P0 | [nodes/stage3/outline.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/outline.py) | 在 prompt 中注入 insights 摘要和关键词 | 大纲章节标题反映真实事件 |
| 🟠 P1 | [nodes/stage3/review.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage3/review.py) | 增加参考数据和明确检查项（禁止占位符标准） | 评审能识别并驱动内容修订 |
| 🟠 P1 | [nodes/stage2/forum.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/forum.py) | 注入事件主题关键词，改进引导逻辑 | 论坛主持人能定向补充信息缺口 |
| 🟡 P2 | [nodes/stage2/insight.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py) | [_match_evidence](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/insight.py#178-196) 改为关键词映射匹配 | 证据追溯不再为空 |
| 🟡 P2 | [nodes/stage2/search.py](file:///d:/PythonProject/AnalysisPosts/nodes/stage2/search.py) | 搜索词基于真实关键词而非截断摘要 | 外部补充信息更精准 |

---

## 五、快速验证方法

完成 P0 修改后，评估报告质量可用以下简单标准：

1. **关键词命中**：[report.md](file:///d:/PythonProject/AnalysisPosts/report/report.md) 中是否出现 "单车"、"夜骑"、"骑行"、"张艺兴"、"2024-08" 等关键词
2. **无占位符**：在 [report.md](file:///d:/PythonProject/AnalysisPosts/report/report.md) 中搜索 `[议题`、`[争议`、`[媒体`，结果应为 0
3. **有证据链接**："证据追溯"章节格式应显示 ≥1 条证据记录
4. **数据一致性**：报告中情感比例（如"中性70%"）应与 [insights.json](file:///d:/PythonProject/AnalysisPosts/report/insights.json) 中的对应值吻合
