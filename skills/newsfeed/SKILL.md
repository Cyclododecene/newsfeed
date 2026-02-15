---
name: newsfeed
description: Comprehensive news aggregator that fetches, filter, and deeply analyze real-time/historical content from the GDELT Project (Global Database of Events, Language, and Tone) databases for news articles, events, and entity data. Use when user are working with global news, historical news report, global event extraction, or GDELT database access.
version: 0.1.7.2
---

# Newsfeed - GDELT Data Access

## Overview 

Fetch real-time/historical news from the **GDELT Project** (Global Database of Events, Language, and Tone) through CLI tools and Python APIs.

## When to Use

Use this skill when you need to:
- Analyse global news by keywords, themes, actors, or locations
- Query global news articles by date range
- Access GDELT Events, Mentions, and Global Knowledge Graph (GKG) databases
- Perform timeline analysis of news events
- Download full-text articles from URLs
- Analyze global events, themes, and relationships

## Prerequisites

To use this skill, set up your environment with the following commands:

1. Create and activate a new or existing Python environment (Python 3.10+ recommended)
2. Install `newsfeed` package:
   ```bash
   pip install newsfeed==0.1.7.2
   ```

Or just download from source code:
   ```bash
    git clone https://github.com/Cyclododecene/newsfeed.git
    cd newsfeed
    git checkout dev
    pip install -e .
   ```

## Common Patterns

1. Scenario 1: Search news by keywords/themes, e.g. `python script/script.py --scenario search-topics --days 1 --keywords "AI,GPT,LLM"` Query news about AI and LLM in the past 1 day and summarize the top themes, locations, and actors.
2. Scenario 2: View bialteral events between two countries, e.g. `python script/script.py --scenario bilateral-events --days 7 --country1 USA --country2 CHN` Query events between USA and China in the past 7 days and summarize the event types, sentiment, and main actors.
3. Scenario 3: View event media mentions and sentiment: e.g. `python skills/script.py --scenario event-mentions --event-id 12345` Query media mentions of event with ID 12345 and summarize the sentiment and source distribution.

## CLI Usage (Recommended)

The primary way to interact with GDELT databases is through the CLI interface.


### Basic Database Query

```bash
newsfeed --db <DATABASE> --version <VERSION> --start <START_DATE> --end <END_DATE> [--format <FORMAT>] [--output <FILENAME>]
```

#### Parameters

| Parameter | Description | Required | Options | Example |
|-----------|-------------|-----------|----------|----------|
| `--db` | Database type | Yes | EVENT, GKG, MENTIONS | EVENT |
| `--version` | Database version | Yes | V1, V2 | V2 |
| `--start` | Start date | Yes | V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS | 2021-01-01 or 2021-01-01-00-00-00 |
| `--end` | End date | Yes | V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS | 2021-01-02 or 2021-01-02-00-00-00 |
| `--format` | Output format | No | csv, json (default: csv) | json |
| `--output` | Output filename | No | Any filename (auto-generated if not specified) | results.csv |

### Database Query Examples

1. **Query Events V2 Database**:
   ```bash
   newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00
   ```

2. **Query GKG V1 Database**:
   ```bash
   newsfeed --db GKG --version V1 --start 2021-01-01 --end 2021-01-02
   ```

3. **Query Mentions V2 with JSON output**:
   ```bash
   newsfeed --db MENTIONS --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --format json
   ```

4. **Specify output filename**:
   ```bash
   newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --output my_events.csv
   ```

### Full-Text Download

Download complete article text from URLs using standalone mode or query mode.

#### Standalone Mode

1. **Download from single URL**:
   ```bash
   newsfeed --fulltext --url "https://example.com/article" --output article.json
   ```

2. **Download from URL list file** (one URL per line):
   ```bash
   newsfeed --fulltext --input urls.txt --output fulltexts.csv
   ```

3. **Download from CSV file**:
   ```bash
   newsfeed --fulltext --input results.csv --url-column SOURCEURL --output with_fulltext.csv
   ```

#### Query Mode with Full-Text Download

Query database and automatically download full text:

```bash
newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --download-fulltext
```

This will:
1. Query GDELT Events database
2. Extract unique URLs from SOURCEURL column
3. Download full text for each article
4. Add full text to FULLTEXT column
5. Export CSV/JSON with full text

### Full-Text Download Parameters

| Parameter | Description | Mode | Default |
|-----------|-------------|-------|----------|
| `--fulltext` | Enable full-text download mode | Standalone | - |
| `--download-fulltext` | Download full text after query | Query | False |
| `--url` | Single URL to download | Standalone | - |
| `--input` | Input file with URLs (txt or csv) | Standalone | - |
| `--url-column` | URL column name in CSV | Both | SOURCEURL |
| `--fulltext-column` | Column name for full text in output | Query | FULLTEXT |
| `--format` | Output format (csv, json, txt) | Both | csv |

## Python API Usage

For advanced use cases, you can use the Python API directly.

### Events Database

```python
from newsfeed.news.db.events import EventV1, EventV2
import pandas as pd

# Version 1 (Daily updates, date format: YYYY-MM-DD)
event_v1 = EventV1(start_date="2021-01-01", end_date="2021-01-02")
results_v1 = event_v1.query()

# Version 2 (15-minute updates, date format: YYYY-MM-DD-HH-MM-SS)
event_v2 = EventV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00", table="events")
results_v2 = event_v2.query()
```

### Mentions Database

```python
from newsfeed.news.db.events import EventV2

# Mentions only available in V2
mentions = EventV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00", table="mentions")
results = mentions.query()
```

### GKG Database

```python
from newsfeed.news.db.gkg import GKGV1, GKGV2

# Version 1
gkg_v1 = GKGV1(start_date="2021-01-01", end_date="2021-01-02")
results_v1 = gkg_v1.query()

# Version 2
gkg_v2 = GKGV2(start_date="2021-01-01-00-00-00", end_date="2021-01-02-00-00-00")
results_v2 = gkg_v2.query()
```

### Full-Text Download

```python
from newsfeed.utils.fulltext import download

# Download full text from URL
article = download("https://example.com/article")
if article:
    print(f"Title: {article.title}")
    print(f"Text: {article.text}")
    print(f"Authors: {article.authors}")
    print(f"Publish Date: {article.publish_date}")
```

## Database Details

### Events Database

Contains global event data including event codes, actors, geographic locations, and sentiment analysis.

- **V1**: Date format `YYYY-MM-DD`, daily updates
- **V2**: Date format `YYYY-MM-DD-HH-MM-SS`, updates every 15 minutes

Key columns:
- `GLOBALEVENTID`: Global event ID
- `SQLDATE`: Date in SQL format
- `Actor1Code`, `Actor2Code`: Country/organization codes
- `EventCode`: CAMEO event code
- `GoldsteinScale`: Impact score
- `AvgTone`: Sentiment score
- `SOURCEURL`: Article URL

### GKG Database

Contains global knowledge graph data including themes, locations, persons, organizations, and sentiment.

- **V1**: Date format `YYYY-MM-DD`, daily updates
- **V2**: Date format `YYYY-MM-DD-HH-MM-SS`, updates every 15 minutes

Key columns:
- `DATE`: Date
- `V2SOURCECOMMONNAME`: Source name
- `V1THEMES`, `V2ENHANCEDTHEMES`: Themes
- `V1LOCATIONS`, `V2ENHANCEDLOCATIONS`: Locations
- `V1PERSONS`, `V2ENHANCEDPERSONS`: Persons
- `V1ORGANIZATIONS`, `V2ENHANCEDORGANIZATIONS`: Organizations

### Mentions Database

Contains media mentions of events, only available in V2.

- **V2**: Date format `YYYY-MM-DD-HH-MM-SS`, updates every 15 minutes

Key columns:
- `GLOBALEVENTID`: Global event ID
- `MentionTimeDate`: When the event was mentioned
- `MentionSourceName`: Source name
- `MentionDocTone`: Sentiment of mention
- `Confidence`: Confidence score

## Common Use Cases

### 1. Analyze Events by Country

```python
import pandas as pd

# Query data
df = pd.read_csv('EVENT_V2_20210101000000_20210102000000.csv')

# Filter by country
china_events = df[df['Actor1CountryCode'] == 'CHN']
print(f"Found {len(china_events)} events in China")
```

### 2. Extract Top Themes from GKG

```python
import pandas as pd
from collections import Counter

# Query data
df = pd.read_csv('GKG_V2_20210101000000_20210102000000.csv')

# Extract themes
all_themes = []
for themes in df['V2ENHANCEDTHEMES'].dropna():
    all_themes.extend(themes.split(';'))

# Count themes
theme_counts = Counter(all_themes)
print("Top 10 themes:")
for theme, count in theme_counts.most_common(10):
    print(f"  {theme}: {count}")
```

### 3. Analyze Sentiment Trends

```python
import pandas as pd
import matplotlib.pyplot as plt

# Query data
df = pd.read_csv('EVENT_V2_20210101000000_20210102000000.csv')

# Convert date
df['date'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')

# Group by date and calculate average tone
daily_tone = df.groupby('date')['AvgTone'].mean()

# Plot
plt.figure(figsize=(12, 6))
daily_tone.plot()
plt.title('Average Sentiment Over Time')
plt.xlabel('Date')
plt.ylabel('Average Tone')
plt.show()
```

## Tips and Best Practices

1. **Date Formats**: Always use the correct date format for the version (V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS)
2. **Query Range**: Keep date ranges reasonable to avoid long download times
3. **Output Format**: Use JSON for programmatic processing, CSV for data analysis
4. **Full-Text Download**: Download times vary based on URL count and network speed
5. **Error Handling**: The CLI will report failed URLs during full-text download
6. **File Size**: GDELT databases are large; be mindful of disk space

## Troubleshooting

### Download fails or takes too long
- Check internet connection
- Reduce date range
- Some URLs may be inaccessible or have anti-scraping measures

### No results found
- Verify date format matches version
- Check if data exists for the date range
- Try a different date range

### Full-text download fails
- Some websites block automated downloads
- Try again later or use Internet Archive fallback (built-in)
- Check failed URL list in output

## Additional Resources

- **GitHub Repository**: https://github.com/Cyclododecene/newsfeed
- **GDELT Project**: https://www.gdeltproject.org/
- **CLI Documentation**: See `CLI_USAGE.md` in the repository
- **API Documentation**: See docstrings in source code

## Help

For CLI help:
```bash
newsfeed --help
python -m newsfeed --help
```
