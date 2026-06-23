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

## 🖥️ NewsFeed Terminal / TUI Roadmap

### v0.1 Local News Query TUI
- [x] Add Textual dependency and `scripts/newsfeed_terminal.sh` launcher
- [x] Create `newsfeed/tui/` package with `app.py`, `models.py`, `commands.py`, `services.py`, `widgets.py`, and `export.py`
- [x] Implement a command input bar with Bloomberg-like commands: `TOP`, `NEWS`, `READ`, `TL`, `FULLTEXT`, `EXPORT`, `HELP`, and `QUIT`
- [x] Implement `NEWS "<query>" SINCE:<window> COUNTRY:<codes>` using EventV2 and local filtering
- [x] Implement `TOP` as a default recent-news query with a configurable 6-hour window
- [x] Implement a dense news table showing headline, source, time, URL, country, and tone where available
- [x] Implement an article detail pane with title, source, URL, query context, and raw metadata
- [x] Implement `READ <row>` and keyboard selection to open article details
- [x] Implement non-blocking `FULLTEXT` download for the selected article using `newsfeed.utils.fulltext.download`
- [x] Implement `TL` timeline volume view using EventV2 local aggregation
- [x] Implement `EXPORT FORMAT:csv|json|parquet PATH:<path>` for current results
- [x] Add loading, empty, and error states so failed network calls do not crash the TUI
- [x] Add parser and service-layer tests that avoid real network calls

### v0.2 Watchlist Workspace
- [x] Add SQLite storage for workspaces, saved queries, articles, watchlists, and query history
- [x] Implement watchlist CRUD for keyword, company, country, theme, and source items
- [x] Cache downloaded full text as files under `~/.cache/newsfeed/fulltext/` and index only paths in SQLite
- [x] Add asynchronous enrichment status model with `none`, `pending`, `indexed`, and `failed`
- [x] Add bounded background full-text enrichment queue for `TOP`, `NEWS`, and `WATCH NEWS`
- [x] Implement `SEARCH "<query>"` over cached full-text files without SQLite FTS or body storage
- [x] Extend keyword watch matching to include cached full-text hits
- [x] Implement basic `ALERT ADD/LIST/DELETE/CHECK` against cached full-text articles
- [x] Highlight watchlist hits in the news table
- [x] Implement `WATCH NEWS` to query current watchlist items
- [x] Persist saved queries and workspace configuration across restarts
- [x] Add Top News ranking v1 using recency, watchlist matches, source diversity, relevance, tone extremity, and duplicate penalty

### v0.3 Alerts, Briefings, and Cache Views
- [x] Implement alert rule CRUD with query, country, source, tone threshold, and scan frequency fields
- [x] Run alert scans in background workers and persist hits in SQLite
- [x] Display unread alert count in the TUI status bar
- [x] Implement `BRIEF` Markdown export for current query, alert hits, and watchlist results
- [x] Add GEO view and timeline tone/source-country modes
- [x] Add `CACHE` view for cache size, file count, query history, and cleanup actions

### v1.0 Product Hardening
- [x] Support multiple workspaces and layout profiles
- [x] Add complete command help and keyboard shortcut reference
- [x] Add import/export for configuration, watchlists, saved queries, and alerts
- [x] Store large historical query results as Parquet
- [x] Keep the TUI data layer separate enough to support future Web/API/agent frontends

### Post-v1.0 Product Backlog from `news-tui-new.md`

#### P1 Product Gaps
- [x] Add `NEWS` `LANGUAGE:<code>` filtering
- [x] Add `NEWS` `SOURCE:<domain>` filtering
- [x] Add result sorting by tone, source count, and relevance
- [x] Add `SRC` command for source filtering and source statistics
- [ ] Let workspace profiles define default `TOP` query templates
- [ ] Support pinned breaking or high-alert news

#### Reader / Library
- [x] Save articles to a local library
- [x] Copy citation or export a Markdown snippet for the selected article
- [x] Expand reading state to saved/read/unread/notes workflows

#### Timeline / Geo
- [x] Add `TL MODE:language`
- [x] Drill down from a timeline bucket to matching news rows
- [x] Show GEO counts and percentages
- [x] Drill down from a GEO country/region row to matching news rows
- [ ] Generate GEO distribution from the active watchlist

#### Watchlist
- [x] Enable and disable individual watchlist items
- [ ] Add dedicated watchlist import/export commands
- [ ] Add watchlist refresh frequency settings

#### Alerts
- [x] Pause, resume, and mute alerts
- [x] Add alert states: active, paused, muted, failed, deleted
- [ ] Add desktop notification or webhook alert delivery
- [ ] Add advanced alert thresholds such as `min_new_articles` and `tone_below`

#### Briefing
- [x] Add fixed brief sections: Key Headlines, Watchlist Hits, Notable Trends, and Source Links
- [x] Clearly separate source facts from system inferences in briefs
- [ ] Add optional LLM briefing generation without making it the default path

#### Cache / Jobs
- [x] Clean expired cache entries instead of only deleting all results/fulltext files
- [x] Add cancellation for long-running queries
- [x] Add fulltext retry and partial text display for failed extraction

#### Hardening / Distribution
- [ ] Add `ALRT` as an alias for `ALERT`
- [x] Implement complete keyboard shortcuts: `/`, `Esc`, `j/k`, `h/l`, `r`, `s`, `a`, `w`, `b`, and `?`
