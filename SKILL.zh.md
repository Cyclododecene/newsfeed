---
name: newsfeed
description: 查询 GDELT 项目（全球事件、语言和语调数据库）的 API 和数据库，获取新闻文章、事件和实体数据。适用于处理全球新闻数据集、事件提取、时间线查询或访问 GDELT 数据库。
version: 0.1.4
---

# Newsfeed - GDELT 数据查询

## 概述

通过 CLI 工具和 Python API 查询和检索 **GDELT 项目**（全球事件、语言和语调数据库）的数据。

## 何时使用

当您需要执行以下操作时，可以使用此技能：

- 按日期范围查询全球新闻文章
- 访问 GDELT 事件、提及和全球知识图谱（GKG）数据库
- 对新闻事件进行时间线分析
- 从 URL 下载文章全文
- 分析全球事件、主题和关系

## 前置条件

要使用此技能，请使用以下命令设置环境：

1. 创建并激活新的或现有的 Python 环境（建议使用 Python 3.10 或更高版本）
2. 安装 `newsfeed` 包：
   ```bash
   pip install newsfeed==0.1.5
   ```

或从源代码下载：
   ```bash
    git clone https://github.com/Cyclododecene/newsfeed.git
    cd newsfeed
    git checkout dev
    pip install -e .
   ```

## 常用模式

1. **事件搜索**：根据日期范围和版本查询 GDELT 事件数据库
2. **提及搜索**：查询 GDELT 提及数据库，查找事件的媒体提及
3. **全球知识图谱搜索**：查询 GDELT GKG 获取主题、位置和关系
4. **全文下载**：从 URL 下载完整的文章内容
5. **导出结果**：将查询结果保存为 JSON 或 CSV 以供进一步分析

## CLI 使用（推荐）

与 GDELT 数据库交互的主要方式是通过 CLI 接口。

### 基本数据库查询

```bash
python -m newsfeed --db <数据库> --version <版本> --start <开始日期> --end <结束日期> [--format <格式>] [--output <文件名>]
```

#### 参数说明

| 参数 | 说明 | 必需 | 可选值 | 示例 |
|------|------|------|--------|------|
| `--db` | 数据库类型 | 是 | EVENT, GKG, MENTIONS | EVENT |
| `--version` | 数据库版本 | 是 | V1, V2 | V2 |
| `--start` | 开始日期 | 是 | V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS | 2021-01-01 或 2021-01-01-00-00-00 |
| `--end` | 结束日期 | 是 | V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS | 2021-01-02 或 2021-01-02-00-00-00 |
| `--format` | 输出格式 | 否 | csv, json (默认: csv) | json |
| `--output` | 输出文件名 | 否 | 任意文件名（未指定时自动生成） | results.csv |

### 数据库查询示例

1. **查询 Events V2 数据库**：
   ```bash
   python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00
   ```

2. **查询 GKG V1 数据库**：
   ```bash
   python -m newsfeed --db GKG --version V1 --start 2021-01-01 --end 2021-01-02
   ```

3. **查询 Mentions V2 并输出 JSON**：
   ```bash
   python -m newsfeed --db MENTIONS --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --format json
   ```

4. **指定输出文件名**：
   ```bash
   python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --output my_events.csv
   ```

### 全文下载

使用独立模式或查询模式从 URL 下载完整的文章文本。

#### 独立模式

1. **从单个 URL 下载**：
   ```bash
   python -m newsfeed --fulltext --url "https://example.com/article" --output article.json
   ```

2. **从 URL 列表文件下载**（每行一个 URL）：
   ```bash
   python -m newsfeed --fulltext --input urls.txt --output fulltexts.csv
   ```

3. **从 CSV 文件下载**：
   ```bash
   python -m newsfeed --fulltext --input results.csv --url-column SOURCEURL --output with_fulltext.csv
   ```

#### 查询模式 + 全文下载

查询数据库并自动下载全文：

```bash
python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --download-fulltext
```

这将会：
1. 查询 GDELT Events 数据库
2. 从 SOURCEURL 列提取唯一 URL
3. 下载每篇文章的全文
4. 将全文添加到 FULLTEXT 列
5. 导出包含全文的 CSV/JSON 文件

### 全文下载参数

| 参数 | 说明 | 模式 | 默认值 |
|------|------|------|----------|
| `--fulltext` | 启用全文下载模式 | 独立 | - |
| `--download-fulltext` | 查询后下载全文 | 查询 | False |
| `--url` | 单个 URL | 独立 | - |
| `--input` | 输入文件（txt 或 csv） | 独立 | - |
| `--url-column` | CSV 文件中的 URL 列名 | 两者 | SOURCEURL |
| `--fulltext-column` | 输出中的全文列名 | 查询 | FULLTEXT |
| `--format` | 输出格式（csv, json, txt） | 两者 | csv |

## Python API 使用

对于高级用例，您可以直接使用 Python API。

### 事件数据库

```python
from newsfeed.news.db.events import EventV1, EventV2
import pandas as pd

# Version 1（每日更新，日期格式：YYYY-MM-DD）
event_v1 = EventV1(start_date="2021-01-01", end_date="2021-01-02")
results_v1 = event_v1.query()

# Version 2（每15分钟更新，日期格式：YYYY-MM-DD-HH-MM-SS）
event_v2 = EventV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00", table="events")
results_v2 = event_v2.query()
```

### 提及数据库

```python
from newsfeed.news.db.events import EventV2

# 提及仅适用于 V2
mentions = EventV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00", table="mentions")
results = mentions.query()
```

### GKG 数据库

```python
from newsfeed.news.db.gkg import GKGV1, GKGV2

# Version 1
gkg_v1 = GKGV1(start_date="2021-01-01", end_date="2021-01-02")
results_v1 = gkg_v1.query()

# Version 2
gkg_v2 = GKGV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00")
results_v2 = gkg_v2.query()
```

### 全文下载

```python
from newsfeed.utils.fulltext import download

# 从 URL 下载全文
article = download("https://example.com/article")
if article:
    print(f"标题: {article.title}")
    print(f"正文: {article.text}")
    print(f"作者: {article.authors}")
    print(f"发布日期: {article.publish_date}")
```

## 数据库详情

### 事件数据库

包含全球事件数据，包括事件代码、参与者、地理位置和情感分析。

- **V1**: 日期格式 `YYYY-MM-DD`，每日更新
- **V2**: 日期格式 `YYYY-MM-DD-HH-MM-SS`，每15分钟更新

主要列：
- `GLOBALEVENTID`: 全球事件 ID
- `SQLDATE`: SQL 格式的日期
- `Actor1Code`, `Actor2Code`: 国家/组织代码
- `EventCode`: CAMEO 事件代码
- `GoldsteinScale`: 影响分数
- `AvgTone`: 情感分数
- `SOURCEURL`: 文章 URL

### GKG 数据库

包含全球知识图谱数据，包括主题、位置、人物、组织和情感。

- **V1**: 日期格式 `YYYY-MM-DD`，每日更新
- **V2**: 日期格式 `YYYY-MM-DD-HH-MM-SS`，每15分钟更新

主要列：
- `DATE`: 日期
- `V2SOURCECOMMONNAME`: 来源名称
- `V1THEMES`, `V2ENHANCEDTHEMES`: 主题
- `V1LOCATIONS`, `V2ENHANCEDLOCATIONS`: 位置
- `V1PERSONS`, `V2ENHANCEDPERSONS`: 人物
- `V1ORGANIZATIONS`, `V2ENHANCEDORGANIZATIONS`: 组织

### 提及数据库

包含事件的媒体提及，仅适用于 V2。

- **V2**: 日期格式 `YYYY-MM-DD-HH-MM-SS`，每15分钟更新

主要列：
- `GLOBALEVENTID`: 全球事件 ID
- `MentionTimeDate`: 事件被提及的时间
- `MentionSourceName`: 来源名称
- `MentionDocTone`: 提及的情感
- `Confidence`: 置信度分数

## 常见用例

### 1. 按国家分析事件

```python
import pandas as pd

# 查询数据
df = pd.read_csv('EVENT_V2_20210101000000_20210102000000.csv')

# 按国家筛选
china_events = df[df['Actor1CountryCode'] == 'CHN']
print(f"找到 {len(china_events)} 个中国事件")
```

### 2. 从 GKG 提取热门主题

```python
import pandas as pd
from collections import Counter

# 查询数据
df = pd.read_csv('GKG_V2_20210101000000_20210102000000.csv')

# 提取主题
all_themes = []
for themes in df['V2ENHANCEDTHEMES'].dropna():
    all_themes.extend(themes.split(';'))

# 统计主题
theme_counts = Counter(all_themes)
print("前 10 个主题：")
for theme, count in theme_counts.most_common(10):
    print(f"  {theme}: {count}")
```

### 3. 分析情感趋势

```python
import pandas as pd
import matplotlib.pyplot as plt

# 查询数据
df = pd.read_csv('EVENT_V2_20210101000000_20210102000000.csv')

# 转换日期
df['date'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')

# 按日期分组并计算平均情感
daily_tone = df.groupby('date')['AvgTone'].mean()

# 绘图
plt.figure(figsize=(12, 6))
daily_tone.plot()
plt.title('随时间变化的平均情感')
plt.xlabel('日期')
plt.ylabel('平均情感')
plt.show()
```

## 技巧和最佳实践

1. **日期格式**：始终使用与版本匹配的正确日期格式（V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS）
2. **查询范围**：保持合理的日期范围以避免长时间下载
3. **输出格式**：使用 JSON 进行程序化处理，使用 CSV 进行数据分析
4. **全文下载**：下载时间取决于 URL 数量和网络速度
5. **错误处理**：CLI 会在全文下载期间报告失败的 URL
6. **文件大小**：GDELT 数据库很大；请注意磁盘空间

## 故障排除

### 下载失败或耗时过长
- 检查网络连接
- 缩短日期范围
- 某些 URL 可能无法访问或有反爬措施

### 未找到结果
- 验证日期格式是否与版本匹配
- 检查该日期范围是否存在数据
- 尝试不同的日期范围

### 全文下载失败
- 某些网站阻止自动下载
- 稍后重试或使用 Internet Archive 备份（内置）
- 检查输出中的失败 URL 列表

## 其他资源

- **GitHub 仓库**: https://github.com/Cyclododecene/newsfeed
- **GDELT 项目**: https://www.gdeltproject.org/
- **CLI 文档**: 参见仓库中的 `CLI_USAGE.md`
- **API 文档**: 参见源代码中的文档字符串

## 帮助

获取 CLI 帮助：
```bash
newsfeed --help
python -m newsfeed --help
```