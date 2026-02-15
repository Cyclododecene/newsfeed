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
- [ ] Time series analysis for event frequency trends
- [ ] Sentiment trend analysis over time
- [ ] Goldstein scale trend analysis
- [ ] Actor activity trend analysis
- [ ] Interactive trend visualization (Plotly)

### Geographical Visualization
- [ ] Event heatmaps on interactive maps
- [ ] Actor location markers and connections
- [ ] Time-based animation of event spread
- [ ] Multi-layer map visualization
- [ ] Export to standalone HTML files

### Text Analysis
- [ ] Topic clustering using GKG Themes
- [ ] Top themes extraction and ranking
- [ ] Theme evolution over time
- [ ] Theme similarity matrix
- [ ] Word cloud generation for themes

### Network Analysis
- [ ] Actor relationship network construction
- [ ] Event cascade analysis
- [ ] Centrality metrics calculation
- [ ] Community detection
- [ ] Interactive network visualization

### Sentiment Analysis Enhancement
- [ ] Multi-dimensional sentiment analysis
- [ ] Sentiment polarity classification
- [ ] Sentiment anomaly detection
- [ ] Sentiment fluctuation analysis
- [ ] Sentiment heatmaps by region/actor

## 📋 Original TODO Items (Lower Priority)

1. Improve `query_nowtime()` function
2. Reconstruct `GEO API` and build `TV API`
3. Add [GSG database](https://blog.gdeltproject.org/announcing-the-global-similarity-graph/) and [GEG database](https://blog.gdeltproject.org/announcing-the-global-entity-graph-geg-and-a-new-11-billion-entity-dataset/) to query list
4. Add [GDELT Article List](https://blog.gdeltproject.org/announcing-the-gdelt-article-list-rss-feed/)

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