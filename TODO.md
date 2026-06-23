# TODO List for NewsFeed Project 

## ✅ Performance Optimizations (COMPLETED - 2026)

### Core Infrastructure
- [x] **Cache Management System** (`newsfeed/utils/cache.py`): File-based caching using joblib to avoid redundant downloads
- [x] **Incremental Query Manager** (`newsfeed/utils/incremental.py`): SQLite-based tracking of downloaded files
- [x] **Async Download Tool** (`newsfeed/utils/async_downloader.py`): Concurrent downloads using aiohttp (3-5x faster)
- [x] **Updated dependencies**: Added aiohttp, aiofiles, pyarrow, joblib

### Database Integration
- [x] **EventV1 & EventV2** (`newsfeed/news/db/events.py`): Integrated all performance optimizations
  - Added `use_cache` parameter for query result caching
  - Added `use_incremental` parameter for incremental downloads
  - Added `force_redownload` parameter to bypass cache/incremental
  - Added `use_async` parameter for async concurrent downloads
  - Added `output_format` parameter for CSV/Parquet support

### ✅ Database Integration (COMPLETED)
- [x] **GKG V1 & V2** (`newsfeed/news/db/gkg.py`): Integrated all performance optimizations
  - Added `use_cache`, `use_incremental`, `force_redownload`, `use_async` parameters
- [x] **Others** (`newsfeed/news/db/others.py`): Integrated performance optimizations
  - GEG, VGEG, GDG, GFG classes now support caching and incremental queries
- [x] **Fulltext** (`newsfeed/utils/fulltext.py`): Added batch download with async support
  - Added `download_batch()` function with async download option
  - Added `download_from_dataframe()` function for DataFrame URLs
- [x] **CLI Interface** (`newsfeed/__main__.py`): Full CLI support for all optimizations
  - Added `--use-cache`, `--incremental`, `--force-redownload`, `--async` arguments
  - Supports all database types (EVENT, GKG, MENTIONS)
  - Supports async fulltext downloads


## 🚀 Planned Data Analysis and Visualization Features

### Trend Analysis
- [x] Time series analysis for event frequency trends
- [x] Sentiment trend analysis over time
- [x] Goldstein scale trend analysis
- [x] Actor activity trend analysis
- [x] Interactive trend visualization (Plotly)

### Geographical Visualization
- [x] Event heatmaps on interactive maps
- [x] Actor location markers and connections
- [x] Time-based animation of event spread
- [x] Multi-layer map visualization
- [x] Export to standalone HTML files

### Text Analysis
- [x] Topic clustering using GKG Themes
- [x] Top themes extraction and ranking
- [x] Theme evolution over time
- [x] Theme similarity matrix
- [x] Word cloud generation for themes

### Network Analysis
- [x] Actor relationship network construction
- [x] Event cascade analysis
- [x] Centrality metrics calculation
- [x] Community detection
- [x] Interactive network visualization

### Sentiment Analysis Enhancement
- [x] Multi-dimensional sentiment analysis
- [x] Sentiment polarity classification
- [x] Sentiment anomaly detection
- [x] Sentiment fluctuation analysis
- [x] Sentiment heatmaps by region/actor

## 🔧 Missing Core Features and Integration Gaps

### GDELT API and Database Coverage
- [x] Build support for GDELT TV 2.0 API queries
- [x] Reconstruct and harden the GEO API wrapper beyond the current beta implementation
- [x] Add Global Similarity Graph (GSG) database query support
- [x] Add GDELT Article List / RSS feed support as a separate data source from DOC API `artlist`

### CLI and Output Integration
- [x] Add CLI entry points for `GEG`, `VGEG`, `GDG`, and `GFG` classes from `newsfeed/news/db/others.py`
- [x] Implement real Parquet output support for Event/GKG query classes and CLI `--format parquet`
- [x] Keep `requirements.txt`, `setup.py`, and `pixi.toml` synchronized when Parquet or visualization dependencies change

### Existing Feature Improvements
- [x] Improve `query_nowtime()` behavior and coverage for Event and GKG classes
- [x] Add tests for GEO API behavior and `others.py` database classes
- [x] Add tests for TV API support
- [x] Add tests covering CLI output formats, including CSV, JSON, TXT, and future Parquet support

## 📋 Original TODO Items (Lower Priority)

1. [x] Improve `query_nowtime()` function
2. [x] Reconstruct `GEO API`
3. [x] Build `TV API`
4. [x] Add [GSG database](https://blog.gdeltproject.org/announcing-the-global-similarity-graph/) to query list
5. [x] Add [GDELT Article List](https://blog.gdeltproject.org/announcing-the-gdelt-article-list-rss-feed/)

## 🎯 Usage Examples (After Optimizations)

```python
# Using cache and async download
from newsfeed.news.db.events import EventV2

event = EventV2(
    start_date="2021-01-01-00-00-00",
    end_date="2021-01-02-00-00-00",
    use_cache=True,
    use_async=True
)
results = event.query()

# Using incremental query
event = EventV2(
    start_date="2021-01-01-00-00-00",
    end_date="2021-01-02-00-00-00",
    use_incremental=True
)
results = event.query()

# Force redownload
event = EventV2(
    start_date="2021-01-01-00-00-00",
    end_date="2021-01-02-00-00-00",
    force_redownload=True
)
results = event.query()
```

```bash
# CLI usage with new features
python -m newsfeed --db EVENT --version V2 --start 2021-01-01 --end 2021-01-02 --use-cache
python -m newsfeed --db EVENT --version V2 --start 2021-01-01 --end 2021-01-02 --incremental
python -m newsfeed --db EVENT --version V2 --start 2021-01-01 --end 2021-01-02 --force-redownload
python -m newsfeed --db EVENT --version V2 --start 2021-01-01 --end 2021-01-02 --format parquet
```

## 📊 Performance Improvements Expected

1. **Cache**: 90-95% reduction in time for repeated queries
2. **Async Download**: 3-5x faster download speed
3. **Incremental Query**: 80-90% reduction in time for repeated queries
4. **Parquet Format**: 70-90% reduction in storage space, 2-3x faster read/write
