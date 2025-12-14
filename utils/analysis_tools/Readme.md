## 分析工具说明（项目内输出）

面向增强后的博文数据（例如 `data/enhanced_blogs.json` 或流程 stage1 产物），提供情感、主题、地理、交互四类分析工具。所有图表默认输出到项目内的 `report/images/`（可通过 `output_dir` 参数修改）

### 使用方式
- 直接按需导入函数：`from utils.analysis_tools import sentiment_trend_chart` 等。
- 或通过注册器调用：  
  ```python
  from utils.analysis_tools import execute_tool
  result = execute_tool("sentiment_trend_chart", blog_data, granularity="day")
  ```
- `blog_data` 需是包含增强字段的博文列表（如 sentiment_polarity、topics、publisher 等）。

### 工具清单

#### 情感趋势分析
- `sentiment_distribution_stats`（data）：情感极性分布统计。参数：`blog_data`。
- `sentiment_time_series`（data）：情感时间序列（hour/day）。参数：`blog_data`，`granularity`（default=hour）。
- `sentiment_anomaly_detection`（data）：情感异常点检测（标准差倍数）。参数：`blog_data`，`threshold`（default=2.0）。
- `sentiment_trend_chart`（chart）：情感趋势折线/面积图。参数：`blog_data`，`output_dir`（default=report/images），`granularity`（default=hour）。
- `sentiment_pie_chart`（chart）：情感分布饼图。参数：`blog_data`，`output_dir`。
- `sentiment_bucket_trend_chart`（chart）：正负中性情感桶堆叠趋势。参数：`blog_data`，`output_dir`，`granularity`（default=hour）。
- `sentiment_attribute_trend_chart`（chart）：Top 情感属性热度趋势。参数：`blog_data`，`output_dir`，`granularity`（default=day），`top_n`（default=6）。

#### 主题演化分析
- `topic_frequency_stats`（data）：父/子主题频次统计。参数：`blog_data`。
- `topic_time_evolution`（data）：主题时间演化（hour/day）。参数：`blog_data`，`granularity`（default=day），`top_n`（default=5）。
- `topic_cooccurrence_analysis`（data）：主题共现关系。参数：`blog_data`，`min_support`（default=2）。
- `topic_ranking_chart`（chart）：主题热度柱状图。参数：`blog_data`，`output_dir`，`top_n`（default=10）。
- `topic_evolution_chart`（chart）：主题演化时间图。参数：`blog_data`，`output_dir`，`granularity`（default=day），`top_n`（default=5）。
- `topic_focus_evolution_chart`（chart）：带焦点高亮的主题演化。参数：`blog_data`，`output_dir`，`granularity`（default=day），`top_n`（default=5）。
- `topic_keyword_trend_chart`（chart）：关键词热度趋势。参数：`blog_data`，`output_dir`，`granularity`（default=day），`top_n`（default=8）。
- `topic_network_chart`（chart）：主题共现网络图。参数：`blog_data`，`output_dir`，`min_support`（default=3）。

#### 地理分布分析
- `geographic_distribution_stats`（data）：地理分布统计。参数：`blog_data`。
- `geographic_hotspot_detection`（data）：区域热点识别。参数：`blog_data`，`threshold_percentile`（default=90）。
- `geographic_sentiment_analysis`（data）：地区情感差异。参数：`blog_data`，`min_posts`（default=5）。
- `geographic_heatmap`（chart）：地区舆情热力图。参数：`blog_data`，`output_dir`。
- `geographic_bar_chart`（chart）：地区分布柱状图。参数：`blog_data`，`output_dir`，`top_n`（default=15）。
- `geographic_sentiment_bar_chart`（chart）：地区正/负面占比对比。参数：`blog_data`，`output_dir`，`top_n`（default=12）。
- `geographic_topic_heatmap`（chart）：地区 × 主题热力。参数：`blog_data`，`output_dir`，`top_regions`（default=10），`top_topics`（default=8）。
- `geographic_temporal_heatmap`（chart）：地区 × 时间热力。参数：`blog_data`，`output_dir`，`granularity`（default=day），`top_regions`（default=8）。

#### 多维交互分析
- `publisher_distribution_stats`（data）：发布者类型分布。参数：`blog_data`。
- `cross_dimension_matrix`（data）：多维交叉矩阵（publisher/location/topic × sentiment 等）。参数：`blog_data`，`dim1`（default=publisher），`dim2`（default=sentiment_polarity）。
- `influence_analysis`（data）：互动/传播影响力 TopN。参数：`blog_data`，`top_n`（default=20）。
- `correlation_analysis`（data）：特征维度相关性。参数：`blog_data`。
- `interaction_heatmap`（chart）：维度交叉热力图。参数：`blog_data`，`output_dir`，`dim1`（default=publisher），`dim2`（default=sentiment_polarity）。
- `publisher_bar_chart`（chart）：发布者分布柱状图。参数：`blog_data`，`output_dir`。
- `publisher_sentiment_bucket_chart`（chart）：发布者 × 情感桶堆叠。参数：`blog_data`，`output_dir`，`top_n`（default=10）。
- `publisher_topic_distribution_chart`（chart）：发布者 × 主题堆叠。参数：`blog_data`，`output_dir`，`top_publishers`（default=8），`top_topics`（default=8）。
- `participant_trend_chart`（chart）：新增/累计参与用户趋势。参数：`blog_data`，`output_dir`，`granularity`（default=day）。

### 输出与目录
- data 类工具返回字典/列表，不写文件。
- chart 类工具会在 `output_dir` 下写入 PNG（文件名附时间戳），默认 `report/images/`。
- 需要聚合或自定义路径时，调用时传入 `output_dir="your/dir"`（相对项目根）。

