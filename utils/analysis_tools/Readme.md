## 工具函数集（D:\MULTI\TOOLS）

面向 `DataProcess/enhanced_posts.json` 的舆情分析工具，可直接运行，所有输出统一落在 `TOOLS/outputs/`。

### 0. 依赖与路径
- 数据：`DataProcess/enhanced_posts.json`
- Python 依赖：`pandas`（必需），`matplotlib`（图表），`networkx`（主题网络，可缺失）

### 1. 公共模块
- `base.py`：数据加载清洗、创建输出目录、主题展开、焦点窗口识别、统一 JSON 写入。

### 2. 情感分析 `sentiment_trend.py`
- 分布、时序、情感桶（含焦点窗口）、情感属性（含焦点窗口）、数值极性均值、发布者类型极性均值/中位数；生成折线/饼图。
- 输出：`outputs/sentiment_trend/` 下的 json/csv 及 png。

### 3. 主题分析 `topic_evolution.py`
- 主题频次、时序演化、共现网络、焦点窗口主题演化、焦点讨论词汇演化；生成趋势/排行/网络图。
- 输出：`outputs/topic_evolution/`。

### 4. 地理分析 `geographic_distribution.py`
- 地理分布、热点、地区情感差异；生成热度/柱状图。
- 输出：`outputs/geo_distribution/`。

### 5. 互动/发布者分析 `interaction.py`
- 发布者分布、情感桶、主题偏好、高互动账号、代表性帖子；生成对应图表。
- 输出：`outputs/interaction/`。

### 6. 运行方式 `main.py`
- 全量运行：`python -m TOOLS.main`
- 指定工具：`python -m TOOLS.main --tools sentiment_trend interaction`
- 运行后自动将所有 PNG 汇总到 `outputs/visualization/`

### 7. 维护提示
- 新增/调整输出文件名或图表时，请同步文档并确保能汇总至 `outputs/visualization/`。
- 未安装 matplotlib/networkx 时，数据仍输出，图表会跳过；安装后可重跑生成完整可视化。
