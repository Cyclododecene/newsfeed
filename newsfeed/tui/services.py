from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from math import log1p
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from newsfeed.news.db.events import EventV2
from newsfeed.tui.commands import ParsedCommand, parse_limit, parse_order, parse_since_window
from newsfeed.tui.models import Article, SourceStat, TimelinePoint, article_from_event_record, article_from_index_record
from newsfeed.tui.storage import TuiStorage
from newsfeed.utils.fulltext import download


DEFAULT_TOP_QUERY = ""
DEFAULT_ARTICLE_LIMIT = 50
DEFAULT_EVENT_LAG_MINUTES = 30
DEFAULT_EVENT_FALLBACK_LOOKBACK = 96
EVENT_FILTER_COLUMNS = [
    "Actor1Name",
    "Actor2Name",
    "Actor1CountryCode",
    "Actor2CountryCode",
    "ActionGeo_CountryCode",
    "ActionGeo_FullName",
    "Actor1Geo_FullName",
    "Actor2Geo_FullName",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "SOURCEURL",
]
WATCH_TYPES = {"keyword", "company", "country", "theme", "source", "event"}


@dataclass
class WatchItem:
    index: int
    kind: str
    value: str
    storage_id: int | None = None
    enabled: bool = True

    def label(self) -> str:
        state = "on" if self.enabled else "off"
        return f"{self.index}. [{state}] {self.kind}:{self.value}"


def query_event_v2_sequential(event_query: Any) -> pd.DataFrame | Exception:
    """Query EventV2 without multiprocessing so it is safe inside Textual workers."""
    if not hasattr(event_query, "_query_list") or not hasattr(event_query, "_download_file"):
        return event_query.query()

    if event_query.use_cache and not event_query.force_redownload:
        cached_data = event_query.cache_manager.get(
            db_type="EVENT",
            version="V2",
            start_date=event_query.start_date,
            end_date=event_query.end_date,
            table_type=event_query.table,
            translation=event_query.translation,
        )
        if cached_data is not None:
            return cached_data

    download_url_list = event_query._query_list()
    if isinstance(download_url_list, Exception):
        return download_url_list

    downloaded_dfs = []
    for url in download_url_list:
        result = event_query._download_file(url)
        if isinstance(result, pd.DataFrame):
            downloaded_dfs.append(result)

    columns = (
        event_query.columns_name_mentions
        if event_query.table == "mentions"
        else event_query.columns_name_events
    )
    if not downloaded_dfs:
        return pd.DataFrame(columns=columns)

    results = pd.concat(downloaded_dfs)
    results.reset_index(drop=True, inplace=True)
    results.columns = columns

    if event_query.use_cache:
        event_query.cache_manager.set(
            results,
            db_type="EVENT",
            version="V2",
            start_date=event_query.start_date,
            end_date=event_query.end_date,
            table_type=event_query.table,
            translation=event_query.translation,
        )

    return results


@dataclass
class NewsService:
    event_factory: Callable[..., Any] = EventV2
    event_query_func: Callable[[Any], Any] = query_event_v2_sequential
    fulltext_func: Callable[[str], Any] = download
    now_func: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    default_top_query: str = DEFAULT_TOP_QUERY
    default_article_limit: int = DEFAULT_ARTICLE_LIMIT
    event_lag_minutes: int = DEFAULT_EVENT_LAG_MINUTES
    event_fallback_lookback: int = DEFAULT_EVENT_FALLBACK_LOOKBACK
    fulltext_retry_limit: int = 2
    watchlist: list[WatchItem] = field(default_factory=list)
    storage: TuiStorage | None = None
    workspace_name: str = "default"

    def __post_init__(self) -> None:
        if self.storage is not None:
            self.workspace_name = str(self.storage.get_setting("active_workspace", self.workspace_name))
            self._reload_watchlist()

    def top(self, command: ParsedCommand) -> list[Article]:
        window = command.options.get("SINCE", "6h")
        countries = _split_codes(command.options.get("COUNTRY"))
        languages = _split_codes(command.options.get("LANGUAGE"))
        sources = _split_values(command.options.get("SOURCE"))
        limit = parse_limit(command.options.get("LIMIT"), default=self.default_article_limit)
        order = parse_order(command.options.get("ORDER"))
        return self.search_articles(
            self.default_top_query,
            window,
            countries,
            limit,
            order,
            ranked="ORDER" not in command.options or order == "relevance",
            languages=languages,
            sources=sources,
        )

    def news(self, command: ParsedCommand) -> list[Article]:
        query = command.query
        if not query:
            raise ValueError('NEWS requires a query, for example NEWS "oil prices" SINCE:6h.')
        countries = _split_codes(command.options.get("COUNTRY"))
        languages = _split_codes(command.options.get("LANGUAGE"))
        sources = _split_values(command.options.get("SOURCE"))
        limit = parse_limit(command.options.get("LIMIT"), default=self.default_article_limit)
        order = parse_order(command.options.get("ORDER"))
        return self.search_articles(
            query,
            command.options.get("SINCE", "6h"),
            countries,
            limit,
            order,
            ranked=order == "relevance",
            languages=languages,
            sources=sources,
        )

    def timeline(self, command: ParsedCommand) -> list[TimelinePoint]:
        window = command.options.get("SINCE", "24h")
        dataframe = self._query_events(window)
        dataframe = filter_events(
            dataframe,
            command.query or self.default_top_query,
            _split_codes(command.options.get("COUNTRY")),
            _split_codes(command.options.get("LANGUAGE")),
            _split_values(command.options.get("SOURCE")),
        )
        return dataframe_to_timeline(
            dataframe,
            mode=command.options.get("MODE", "count"),
            context=query_context(command, window),
        )

    def geo(self, command: ParsedCommand) -> list[TimelinePoint]:
        window = command.options.get("SINCE", "24h")
        dataframe = self._query_events(window)
        dataframe = filter_events(
            dataframe,
            command.query or self.default_top_query,
            _split_codes(command.options.get("COUNTRY")),
            _split_codes(command.options.get("LANGUAGE")),
            _split_values(command.options.get("SOURCE")),
        )
        return dataframe_to_geo(dataframe, context=query_context(command, window))

    def timeline_drilldown(self, point: TimelinePoint, limit: int = DEFAULT_ARTICLE_LIMIT) -> list[Article]:
        dataframe = self._query_events(str(point.raw.get("since", "24h")))
        dataframe = filter_events(
            dataframe,
            str(point.raw.get("query", "")),
            _split_codes(str(point.raw.get("country_filter", ""))),
            _split_codes(str(point.raw.get("language_filter", ""))),
            _split_values(str(point.raw.get("source_filter", ""))),
        )
        bucket = str(point.raw.get("bucket", point.timestamp))
        if bucket:
            dataframe = filter_timeline_bucket(dataframe, bucket)
        return self._articles_from_dataframe(dataframe, query=f"TL:{bucket}", limit=limit)

    def geo_drilldown(self, point: TimelinePoint, limit: int = DEFAULT_ARTICLE_LIMIT) -> list[Article]:
        dataframe = self._query_events(str(point.raw.get("since", "24h")))
        dataframe = filter_events(
            dataframe,
            str(point.raw.get("query", "")),
            _split_codes(str(point.raw.get("country_filter", ""))),
            _split_codes(str(point.raw.get("language_filter", ""))),
            _split_values(str(point.raw.get("source_filter", ""))),
        )
        country = str(point.raw.get("country", point.timestamp))
        dataframe = filter_geo_country(dataframe, country)
        return self._articles_from_dataframe(dataframe, query=f"GEO:{country}", limit=limit)

    def source_stats(self, command: ParsedCommand) -> list[SourceStat]:
        dataframe = self._query_events(command.options.get("SINCE", "24h"))
        dataframe = filter_events(
            dataframe,
            command.query,
            _split_codes(command.options.get("COUNTRY")),
            _split_codes(command.options.get("LANGUAGE")),
            _split_values(command.options.get("SOURCE")),
        )
        return source_stats_from_dataframe(dataframe)

    def source_stats_from_articles(self, articles: list[Article]) -> list[SourceStat]:
        return source_stats_from_articles(articles)

    def watch_news(self, command: ParsedCommand) -> list[Article]:
        if not self.active_watchlist:
            raise ValueError("Watchlist is empty. Use WATCH ADD keyword <value> or WATCH ADD country <code>.")
        window = command.options.get("SINCE", "6h")
        limit = parse_limit(command.options.get("LIMIT"), default=self.default_article_limit)
        order = parse_order(command.options.get("ORDER"))
        ranked = "ORDER" not in command.options
        dataframe = self._query_events(window)
        dataframe = filter_watchlist(
            dataframe,
            self.active_watchlist,
            fulltext_url_matches=self._watchlist_fulltext_urls(),
        )
        records = dataframe.fillna("").to_dict("records")
        articles = [article_from_event_record(i + 1, row, query="WATCH") for i, row in enumerate(records)]
        articles = self._prepare_article_results(articles, limit=limit, order=order, ranked=ranked)
        self._index_articles(articles)
        return articles

    def add_watch_item(self, kind: str, value: str) -> WatchItem:
        normalized_kind = kind.lower()
        if normalized_kind not in WATCH_TYPES:
            raise ValueError("WATCH kind must be keyword, company, country, theme, source, or event.")
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("WATCH ADD requires a value.")
        if self.storage is not None:
            stored = self.storage.add_watch_item(normalized_kind, normalized_value, watchlist=self.workspace_name)
            self._reload_watchlist()
            return next(item for item in self.watchlist if item.storage_id == stored.id)

        item = WatchItem(index=len(self.watchlist) + 1, kind=normalized_kind, value=normalized_value)
        self.watchlist.append(item)
        return item

    def delete_watch_item(self, index: int) -> WatchItem:
        if index < 1 or index > len(self.watchlist):
            raise ValueError(f"Watch item {index} is not available.")
        if self.storage is not None:
            item = self.watchlist[index - 1]
            if item.storage_id is not None:
                self.storage.delete_watch_item(item.storage_id)
            self._reload_watchlist()
            return item

        item = self.watchlist.pop(index - 1)
        for i, existing in enumerate(self.watchlist, start=1):
            existing.index = i
        return item

    def set_watch_item_enabled(self, index: int, enabled: bool) -> WatchItem:
        if index < 1 or index > len(self.watchlist):
            raise ValueError(f"Watch item {index} is not available.")
        item = self.watchlist[index - 1]
        if self.storage is not None:
            if item.storage_id is not None:
                self.storage.set_watch_item_enabled(item.storage_id, enabled)
            self._reload_watchlist()
            return self.watchlist[index - 1]
        item.enabled = enabled
        return item

    def watchlist_labels(self) -> list[str]:
        return [item.label() for item in self.watchlist]

    @property
    def active_watchlist(self) -> list[WatchItem]:
        return [item for item in self.watchlist if item.enabled]

    def _reload_watchlist(self) -> None:
        if self.storage is None:
            return
        self.watchlist = [
            WatchItem(index=i, kind=item.kind, value=item.value, storage_id=item.id, enabled=item.enabled)
            for i, item in enumerate(self.storage.list_watch_items(self.workspace_name), start=1)
        ]

    def set_workspace(self, name: str) -> None:
        if self.storage is None:
            raise ValueError("WORKSPACE requires TUI storage.")
        workspace = name.strip() or "default"
        self.storage.get_or_create_watchlist(workspace)
        self.storage.set_setting("active_workspace", workspace)
        self.workspace_name = workspace
        self._reload_watchlist()

    def list_workspaces(self) -> list[str]:
        if self.storage is None:
            return ["default"]
        names = self.storage.list_watchlists()
        return names or ["default"]

    def save_workspace_layout(self, name: str, layout: dict[str, Any]) -> int:
        if self.storage is None:
            raise ValueError("WORKSPACE requires TUI storage.")
        return self.storage.save_workspace_layout(name, layout)

    def load_workspace_layout(self, name: str) -> dict[str, Any]:
        if self.storage is None:
            raise ValueError("WORKSPACE requires TUI storage.")
        layout = self.storage.get_workspace_layout(name)
        if layout is None:
            raise ValueError(f"Workspace layout {name} not found.")
        return layout["layout"]

    def list_workspace_layouts(self) -> list[str]:
        if self.storage is None:
            return []
        return [
            f"{layout['name']}: {layout['updated_at']}"
            for layout in self.storage.list_workspace_layouts()
        ]

    def save_query(self, name: str, command: str) -> int:
        if self.storage is None:
            raise ValueError("SAVE requires TUI storage.")
        if not name or not command:
            raise ValueError("SAVE QUERY requires a name and command.")
        return self.storage.save_query(name, command)

    def list_saved_queries(self) -> list[str]:
        if self.storage is None:
            return []
        return [
            f"{query['id']}. {query['name']}: {query['command']}"
            for query in self.storage.list_saved_queries()
        ]

    def get_saved_query(self, name: str) -> str:
        if self.storage is None:
            raise ValueError("LOAD requires TUI storage.")
        row = self.storage.get_saved_query(name)
        if row is None:
            raise ValueError(f"Saved query {name} not found.")
        return row["command"]

    def cache_stats(self) -> dict[str, Any]:
        if self.storage is None:
            raise ValueError("CACHE requires TUI storage.")
        return self.storage.cache_stats()

    def cleanup_cache(self, target: str) -> dict[str, int]:
        if self.storage is None:
            raise ValueError("CACHE requires TUI storage.")
        parts = target.split(maxsplit=1)
        return self.storage.cleanup_cache(parts[0], parts[1] if len(parts) > 1 else "")

    def query_history_labels(self, limit: int = 20) -> list[str]:
        if self.storage is None:
            return []
        return [
            f"{row['id']}. {row['created_at']} {row['result_count']} rows {row['command']} -> {row['result_path']}"
            for row in self.storage.list_query_history(limit=limit)
        ]

    def export_config(self, path: str) -> str:
        if self.storage is None:
            raise ValueError("CONFIG requires TUI storage.")
        return str(self.storage.export_config(path))

    def import_config(self, path: str) -> dict[str, int]:
        if self.storage is None:
            raise ValueError("CONFIG requires TUI storage.")
        result = self.storage.import_config(path)
        self.workspace_name = str(self.storage.get_setting("active_workspace", self.workspace_name))
        self._reload_watchlist()
        return result

    def search_articles(
        self,
        query: str,
        since_window: str = "6h",
        countries: list[str] | None = None,
        max_records: int = DEFAULT_ARTICLE_LIMIT,
        order: str = "newest",
        ranked: bool = False,
        languages: list[str] | None = None,
        sources: list[str] | None = None,
    ) -> list[Article]:
        dataframe = self._query_events(since_window)
        dataframe = filter_events(dataframe, query, countries or [], languages or [], sources or [])
        records = dataframe.fillna("").to_dict("records")
        articles = [article_from_event_record(i + 1, row, query=query) for i, row in enumerate(records)]
        articles = self._prepare_article_results(articles, limit=max_records, order=order, ranked=ranked)
        self._index_articles(articles)
        return articles

    def _articles_from_dataframe(self, dataframe: pd.DataFrame, query: str, limit: int) -> list[Article]:
        records = dataframe.fillna("").to_dict("records")
        articles = [article_from_event_record(i + 1, row, query=query) for i, row in enumerate(records)]
        articles = self._prepare_article_results(articles, limit=limit, order="newest", ranked=False)
        self._index_articles(articles)
        return articles

    def search_fulltext(self, command: ParsedCommand) -> list[Article]:
        if self.storage is None:
            raise ValueError("SEARCH requires TUI storage.")
        query = command.query
        if not query:
            raise ValueError('SEARCH requires a query, for example SEARCH "oil price shock".')
        limit = parse_limit(command.options.get("LIMIT"), default=self.default_article_limit)
        rows = self.storage.search_fulltext(query, limit=limit)
        return [article_from_index_record(i + 1, row, query=query) for i, row in enumerate(rows)]

    def add_alert(
        self,
        name: str,
        query: str,
        *,
        country: str = "",
        source: str = "",
        tone_threshold: float | None = None,
        scan_frequency: int = 300,
    ) -> int:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        if not name or not query:
            raise ValueError("ALERT ADD requires a name and query.")
        return self.storage.add_alert(
            name,
            query,
            country=country,
            source=source,
            tone_threshold=tone_threshold,
            scan_frequency=scan_frequency,
        )

    def delete_alert(self, name: str) -> bool:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        return self.storage.delete_alert(name)

    def pause_alert(self, name: str) -> bool:
        return self.set_alert_state(name, "paused")

    def resume_alert(self, name: str) -> bool:
        return self.set_alert_state(name, "active")

    def mute_alert(self, name: str, window: str) -> bool:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        muted_until = (self.now_func() + parse_since_window(window)).replace(microsecond=0).isoformat()
        return self.storage.set_alert_state(name, "muted", muted_until=muted_until, last_error="")

    def set_alert_state(self, name: str, state: str) -> bool:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        return self.storage.set_alert_state(name, state, last_error="" if state == "active" else None)

    def list_alerts(self) -> list[str]:
        if self.storage is None:
            return []
        return [
            alert_label(alert)
            for alert in self.storage.list_alerts()
        ]

    def check_alerts(self) -> list[Article]:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        hits = self.storage.check_alerts()
        rows = []
        for hit in hits:
            row = self.storage.get_article_index(hit["article_id"])
            if row is not None:
                rows.append(row)
        return [article_from_index_record(i + 1, row, query="ALERT") for i, row in enumerate(rows)]

    def alert_hits(self, limit: int = 50, mark_read: bool = True) -> list[Article]:
        if self.storage is None:
            raise ValueError("ALERT requires TUI storage.")
        hits = self.storage.list_alert_hits(limit=limit)
        articles = []
        hit_ids = []
        for index, hit in enumerate(hits, start=1):
            row = self.storage.get_article_index(hit["article_id"])
            if row is None:
                continue
            article = article_from_index_record(index, row, query="ALERT")
            article.raw = {
                **article.raw,
                "alert_hit_id": hit["id"],
                "alert_name": hit["alert_name"],
                "alert_hit_at": hit["hit_at"],
                "alert_metadata_json": hit["metadata_json"],
                "alert_read_at": hit["read_at"],
            }
            try:
                metadata = json.loads(hit["metadata_json"])
            except json.JSONDecodeError:
                metadata = {}
            article.match_reason = f"alert:{hit['alert_name']} ({metadata.get('match_reason', 'rule')})"
            articles.append(article)
            hit_ids.append(int(hit["id"]))
        if mark_read and hit_ids:
            self.storage.mark_alert_hits_read(hit_ids)
        return articles

    def unread_alert_count(self) -> int:
        if self.storage is None:
            return 0
        return self.storage.unread_alert_count()

    def save_to_library(self, article: Article) -> str:
        if self.storage is None:
            raise ValueError("LIB requires TUI storage.")
        return self.storage.save_library_article(article, state="saved")

    def list_library(self, limit: int = 50) -> list[Article]:
        if self.storage is None:
            raise ValueError("LIB requires TUI storage.")
        rows = self.storage.list_library(limit=limit)
        return [article_from_index_record(i + 1, row, query="LIB") for i, row in enumerate(rows)]

    def delete_library_item(self, identifier: str) -> bool:
        if self.storage is None:
            raise ValueError("LIB requires TUI storage.")
        article_id = self._library_article_id(identifier)
        return self.storage.delete_library_item(article_id)

    def mark_library_article(self, article: Article, state: str) -> str:
        if self.storage is None:
            raise ValueError("LIB requires TUI storage.")
        article_id = self.storage.save_library_article(article, state=state)
        article.raw = {**article.raw, "reading_state": state}
        return article_id

    def note_library_article(self, article: Article, note: str) -> str:
        if self.storage is None:
            raise ValueError("LIB requires TUI storage.")
        article_id = self.storage.save_library_article(article, state=str(article.raw.get("reading_state") or "saved"))
        self.storage.set_article_note(article_id, note)
        article.raw = {**article.raw, "notes": note}
        return article_id

    def _library_article_id(self, identifier: str) -> str:
        if identifier.isdigit():
            rows = self.storage.list_library()
            index = int(identifier)
            if index < 1 or index > len(rows):
                raise ValueError(f"Library item {identifier} is not available.")
            return rows[index - 1]["id"]
        return identifier

    def fetch_fulltext(self, article: Article) -> Article:
        if not article.url:
            article.error = "Selected event has no SOURCEURL."
            article.enrichment_status = "failed"
            return article

        if self.storage is not None:
            cached = self.storage.load_cached_fulltext(article)
            if cached is not None:
                return article
            self.storage.set_enrichment_status(article, "pending")

        fulltext_article = None
        last_error = ""
        for attempt in range(max(self.fulltext_retry_limit, 1)):
            try:
                fulltext_article = self.fulltext_func(article.url)
                last_error = ""
                break
            except Exception as exc:
                last_error = str(exc)
                partial = partial_text_from(exc)
                if partial and self.storage is not None:
                    article.error = last_error
                    self.storage.save_partial_text(article, partial)
                if attempt + 1 >= max(self.fulltext_retry_limit, 1):
                    article.error = last_error
                    article.enrichment_status = "failed"
                    if self.storage is not None:
                        self.storage.set_enrichment_status(article, "failed", last_error)
                    return article

        if fulltext_article is None:
            article.error = "Full-text download returned no article."
            article.enrichment_status = "failed"
            if self.storage is not None:
                self.storage.set_enrichment_status(article, "failed", article.error)
            return article

        article.fulltext = getattr(fulltext_article, "text", "") or ""
        if not article.fulltext:
            partial = partial_text_from(fulltext_article)
            article.error = "Full-text download completed but text was empty."
            article.enrichment_status = "failed"
            if self.storage is not None:
                if partial:
                    self.storage.save_partial_text(article, partial)
                self.storage.set_enrichment_status(article, "failed", article.error)
        else:
            article.error = ""
            article.enrichment_status = "indexed"
            if self.storage is not None:
                content_hash, fulltext_path = self.storage.save_fulltext(article, article.fulltext)
                article.raw = {
                    **article.raw,
                    "content_hash": content_hash,
                    "fulltext_path": fulltext_path,
                }
        return article

    def _query_events(self, since_window: str) -> pd.DataFrame:
        start_date, end_date = event_since_to_range(
            since_window,
            default="6h",
            now=self.now_func(),
            lag_minutes=self.event_lag_minutes,
        )
        event_query = self.event_factory(
            start_date=start_date,
            end_date=end_date,
            table="events",
            use_cache=True,
        )
        result = self.event_query_func(event_query)
        if isinstance(result, Exception):
            raise RuntimeError(str(result)) from result
        if not isinstance(result, pd.DataFrame):
            raise RuntimeError(f"EVENT query returned {type(result).__name__}.")
        if result.empty and hasattr(event_query, "query_nowtime"):
            fallback = event_query.query_nowtime(max_lookback=self.event_fallback_lookback)
            if isinstance(fallback, Exception):
                raise RuntimeError(str(fallback)) from fallback
            if isinstance(fallback, pd.DataFrame):
                return fallback
        return result

    def _index_articles(self, articles: list[Article]) -> None:
        if self.storage is not None and articles:
            self.storage.index_articles(articles)
            self._hydrate_enrichment_status(articles)

    def _watchlist_fulltext_urls(self) -> dict[str, set[str]]:
        if self.storage is None:
            return {}
        return {
            item.value: self.storage.fulltext_match_urls(item.value)
            for item in self.active_watchlist
            if item.kind == "keyword"
        }

    def _prepare_article_results(
        self,
        articles: list[Article],
        *,
        limit: int,
        order: str,
        ranked: bool = False,
    ) -> list[Article]:
        self._hydrate_enrichment_status(articles)
        self._annotate_watch_hits(articles)
        if ranked:
            articles = rank_top_articles(articles)
        else:
            articles = sort_articles_by_published_at(articles, order)
        articles = articles[:limit]
        for index, article in enumerate(articles, start=1):
            article.index = index
        return articles

    def _hydrate_enrichment_status(self, articles: list[Article]) -> None:
        if self.storage is None:
            return
        for article in articles:
            row = self.storage.get_article_index_by_url(article.url)
            if row is None:
                continue
            article.enrichment_status = row.get("enrichment_status") or article.enrichment_status
            if row.get("fulltext_path"):
                article.raw = {
                    **article.raw,
                    "content_hash": row.get("content_hash", ""),
                    "fulltext_path": row.get("fulltext_path", ""),
                    "enriched_at": row.get("enriched_at", ""),
                    "enrichment_error": row.get("enrichment_error", ""),
                    "fulltext_retry_count": row.get("fulltext_retry_count", 0),
                    "partial_text_path": row.get("partial_text_path", ""),
                }
            elif row.get("partial_text_path") or row.get("fulltext_retry_count"):
                article.raw = {
                    **article.raw,
                    "enrichment_error": row.get("enrichment_error", ""),
                    "fulltext_retry_count": row.get("fulltext_retry_count", 0),
                    "partial_text_path": row.get("partial_text_path", ""),
                }

    def _annotate_watch_hits(self, articles: list[Article]) -> None:
        if not self.active_watchlist:
            return
        fulltext_url_matches = self._watchlist_fulltext_urls()
        for article in articles:
            hits = []
            for item in self.active_watchlist:
                reason = article_watch_match(article, item, fulltext_url_matches)
                if reason:
                    hits.append(f"{item.kind}:{item.value} ({reason})")
            article.watch_hits = hits
            article.match_reason = ", ".join(hits)


def event_since_to_range(
    window: str | None,
    default: str = "6h",
    now: datetime | None = None,
    lag_minutes: int = DEFAULT_EVENT_LAG_MINUTES,
) -> tuple[str, str]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    end = _floor_to_event_interval(current - timedelta(minutes=lag_minutes))
    start = end - parse_since_window(window, default=default)
    return _format_event_time(start), _format_event_time(end)


def _floor_to_event_interval(value: datetime) -> datetime:
    minute = 15 * (value.minute // 15)
    return value.replace(minute=minute, second=0, microsecond=0)


def _format_event_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d-%H-%M-%S")


def filter_events(
    dataframe: pd.DataFrame,
    query: str = "",
    countries: list[str] | None = None,
    languages: list[str] | None = None,
    sources: list[str] | None = None,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    filtered = dataframe
    country_codes = {country.upper() for country in countries or []}
    if country_codes:
        country_columns = [
            column
            for column in ["ActionGeo_CountryCode", "Actor1CountryCode", "Actor2CountryCode"]
            if column in filtered.columns
        ]
        if country_columns:
            country_mask = filtered[country_columns].fillna("").astype(str).apply(
                lambda row: any(value.upper() in country_codes for value in row),
                axis=1,
            )
            filtered = filtered[country_mask]

    language_codes = {language.lower() for language in languages or []}
    if language_codes:
        language_columns = [
            column
            for column in ["language", "lang", "Language", "SourceLanguage"]
            if column in filtered.columns
        ]
        if language_columns:
            language_mask = filtered[language_columns].fillna("").astype(str).apply(
                lambda row: any(value.lower() in language_codes for value in row),
                axis=1,
            )
            filtered = filtered[language_mask]

    source_domains = [source.lower() for source in sources or []]
    if source_domains and "SOURCEURL" in filtered.columns:
        source_mask = filtered["SOURCEURL"].fillna("").astype(str).apply(
            lambda url: any(source in article_domain(url) or source in url.lower() for source in source_domains)
        )
        filtered = filtered[source_mask]

    terms = [term for term in query.lower().split() if term]
    if terms:
        search_columns = [column for column in EVENT_FILTER_COLUMNS if column in filtered.columns]
        if search_columns:
            haystack = filtered[search_columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
            term_mask = pd.Series(True, index=filtered.index)
            for term in terms:
                term_mask &= haystack.str.contains(term, regex=False)
            filtered = filtered[term_mask]

    return filtered


def filter_watchlist(
    dataframe: pd.DataFrame,
    watchlist: list[WatchItem],
    fulltext_url_matches: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    if dataframe.empty or not watchlist:
        return dataframe

    mask = pd.Series(False, index=dataframe.index)
    for item in watchlist:
        mask |= watch_item_mask(dataframe, item, fulltext_url_matches=fulltext_url_matches)
    return dataframe[mask]


def watch_item_mask(
    dataframe: pd.DataFrame,
    item: WatchItem,
    fulltext_url_matches: dict[str, set[str]] | None = None,
) -> pd.Series:
    value = item.value.lower()
    if item.kind == "country":
        columns = [
            column
            for column in ["ActionGeo_CountryCode", "Actor1CountryCode", "Actor2CountryCode"]
            if column in dataframe.columns
        ]
        if not columns:
            return pd.Series(False, index=dataframe.index)
        codes = dataframe[columns].fillna("").astype(str).apply(
            lambda row: any(cell.lower() == value for cell in row),
            axis=1,
        )
        return codes

    if item.kind == "company":
        columns = [column for column in ["Actor1Name", "Actor2Name"] if column in dataframe.columns]
    elif item.kind == "source":
        columns = [column for column in ["SOURCEURL"] if column in dataframe.columns]
    elif item.kind == "event":
        columns = [column for column in ["EventCode", "EventBaseCode", "EventRootCode"] if column in dataframe.columns]
    elif item.kind in {"keyword", "theme"}:
        columns = [column for column in EVENT_FILTER_COLUMNS if column in dataframe.columns]
    else:
        columns = [column for column in EVENT_FILTER_COLUMNS if column in dataframe.columns]

    if not columns:
        metadata_mask = pd.Series(False, index=dataframe.index)
    else:
        haystack = dataframe[columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        metadata_mask = haystack.str.contains(value, regex=False)

    if item.kind != "keyword" or not fulltext_url_matches or "SOURCEURL" not in dataframe.columns:
        return metadata_mask

    urls = fulltext_url_matches.get(item.value, set())
    if not urls:
        return metadata_mask
    fulltext_mask = dataframe["SOURCEURL"].fillna("").astype(str).isin(urls)
    return metadata_mask | fulltext_mask


def rank_events(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    ranked = dataframe.copy()
    ranked["_rank_mentions"] = pd.to_numeric(ranked.get("NumMentions", 0), errors="coerce").fillna(0)
    ranked["_rank_articles"] = pd.to_numeric(ranked.get("NumArticles", 0), errors="coerce").fillna(0)
    ranked["_rank_tone"] = pd.to_numeric(ranked.get("AvgTone", 0), errors="coerce").fillna(0).abs()
    ranked["_rank_date"] = pd.to_numeric(ranked.get("DATEADDED", 0), errors="coerce").fillna(0)
    ranked = ranked.sort_values(
        by=["_rank_mentions", "_rank_articles", "_rank_tone", "_rank_date"],
        ascending=[False, False, False, False],
    )
    return ranked.drop(columns=["_rank_mentions", "_rank_articles", "_rank_tone", "_rank_date"])


def sort_events(dataframe: pd.DataFrame, order: str = "newest") -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    ascending = parse_order(order) == "oldest"
    sorted_frame = dataframe.copy()
    sorted_frame["_sort_date"] = pd.to_numeric(sorted_frame.get("DATEADDED", 0), errors="coerce").fillna(0)
    return sorted_frame.sort_values("_sort_date", ascending=ascending).drop(columns=["_sort_date"])


def dataframe_to_timeline(
    dataframe: pd.DataFrame,
    mode: str = "count",
    context: dict[str, str] | None = None,
) -> list[TimelinePoint]:
    if dataframe.empty:
        return []
    base_context = context or {}

    if "DATEADDED" in dataframe.columns:
        series = dataframe["DATEADDED"].fillna("").astype(str).str.slice(0, 12)
    elif "SQLDATE" in dataframe.columns:
        series = dataframe["SQLDATE"].fillna("").astype(str)
    else:
        series = pd.Series(["unknown"] * len(dataframe))

    normalized_mode = (mode or "count").lower()
    if normalized_mode == "tone":
        frame = dataframe.copy()
        frame["_bucket"] = series
        frame["_tone"] = pd.to_numeric(frame.get("AvgTone", 0), errors="coerce").fillna(0)
        values = frame.groupby("_bucket")["_tone"].mean().sort_index()
        return [
            TimelinePoint(
                timestamp=str(timestamp),
                value=round(float(value), 3),
                raw={**base_context, "mode": "tone", "bucket": str(timestamp)},
            )
            for timestamp, value in values.items()
        ]
    if normalized_mode in {"source_country", "country"}:
        frame = dataframe.copy()
        frame["_bucket"] = series
        country_series = frame.get("ActionGeo_CountryCode", pd.Series([""] * len(frame))).fillna("").astype(str)
        points = []
        for timestamp, group in frame.assign(_country=country_series).groupby("_bucket"):
            counts = group["_country"].replace("", "unknown").value_counts()
            top_country = counts.index[0] if not counts.empty else "unknown"
            points.append(
                TimelinePoint(
                    timestamp=str(timestamp),
                    value=str(top_country),
                    raw={
                        **base_context,
                        "mode": "source_country",
                        "bucket": str(timestamp),
                        "count": int(counts.iloc[0]) if not counts.empty else 0,
                    },
                )
            )
        return points
    if normalized_mode == "language":
        frame = dataframe.copy()
        frame["_bucket"] = series
        language_series = first_existing_series(frame, ["language", "lang", "Language", "SourceLanguage"])
        points = []
        for timestamp, group in frame.assign(_language=language_series).groupby("_bucket"):
            counts = group["_language"].replace("", "unknown").value_counts()
            top_language = counts.index[0] if not counts.empty else "unknown"
            points.append(
                TimelinePoint(
                    timestamp=str(timestamp),
                    value=str(top_language),
                    raw={
                        **base_context,
                        "mode": "language",
                        "bucket": str(timestamp),
                        "count": int(counts.iloc[0]) if not counts.empty else 0,
                    },
                )
            )
        return points
    if normalized_mode != "count":
        raise ValueError("TL MODE must be count, tone, source_country, or language.")

    counts = series.value_counts().sort_index()
    return [
        TimelinePoint(
            timestamp=str(timestamp),
            value=int(value),
            raw={**base_context, "mode": "count", "bucket": str(timestamp), "count": int(value)},
        )
        for timestamp, value in counts.items()
    ]


def dataframe_to_geo(dataframe: pd.DataFrame, context: dict[str, str] | None = None) -> list[TimelinePoint]:
    if dataframe.empty:
        return []
    country_columns = [
        column
        for column in ["ActionGeo_CountryCode", "Actor1CountryCode", "Actor2CountryCode"]
        if column in dataframe.columns
    ]
    if not country_columns:
        return []
    country_series = dataframe[country_columns].replace("", pd.NA).bfill(axis=1).iloc[:, 0].fillna("")
    counts = country_series.replace("", "unknown").value_counts()
    total = int(counts.sum()) or 1
    return [
        TimelinePoint(
            timestamp=str(country),
            value=f"{int(count)} ({(int(count) / total) * 100:.1f}%)",
            raw={
                **(context or {}),
                "mode": "geo",
                "country": str(country),
                "count": int(count),
                "percentage": round((int(count) / total) * 100, 3),
            },
        )
        for country, count in counts.items()
    ]


def query_context(command: ParsedCommand, since_window: str) -> dict[str, str]:
    return {
        "query": command.query,
        "since": since_window,
        "country_filter": command.options.get("COUNTRY", ""),
        "language_filter": command.options.get("LANGUAGE", ""),
        "source_filter": command.options.get("SOURCE", ""),
    }


def filter_timeline_bucket(dataframe: pd.DataFrame, bucket: str) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    if "DATEADDED" in dataframe.columns:
        series = dataframe["DATEADDED"].fillna("").astype(str).str.slice(0, len(bucket))
    elif "SQLDATE" in dataframe.columns:
        series = dataframe["SQLDATE"].fillna("").astype(str).str.slice(0, len(bucket))
    else:
        return dataframe
    return dataframe[series == bucket]


def filter_geo_country(dataframe: pd.DataFrame, country: str) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    country_columns = [
        column
        for column in ["ActionGeo_CountryCode", "Actor1CountryCode", "Actor2CountryCode"]
        if column in dataframe.columns
    ]
    if not country_columns:
        return dataframe.iloc[0:0]
    normalized = country.lower()
    values = dataframe[country_columns].fillna("").astype(str)
    if normalized == "unknown":
        mask = values.apply(lambda row: all(not cell for cell in row), axis=1)
    else:
        mask = values.apply(lambda row: any(cell.lower() == normalized for cell in row), axis=1)
    return dataframe[mask]


def first_existing_series(dataframe: pd.DataFrame, columns: list[str]) -> pd.Series:
    for column in columns:
        if column in dataframe.columns:
            return dataframe[column].fillna("").astype(str)
    return pd.Series([""] * len(dataframe), index=dataframe.index)


def source_stats_from_dataframe(dataframe: pd.DataFrame) -> list[SourceStat]:
    if dataframe.empty or "SOURCEURL" not in dataframe.columns:
        return []
    frame = dataframe.copy()
    frame["_source_domain"] = frame["SOURCEURL"].fillna("").astype(str).apply(article_domain).replace("", "unknown")
    frame["_tone"] = pd.to_numeric(frame.get("AvgTone", 0), errors="coerce").fillna(0)
    grouped = frame.groupby("_source_domain").agg(count=("_source_domain", "size"), avg_tone=("_tone", "mean"))
    grouped = grouped.sort_values(["count", "avg_tone"], ascending=[False, False])
    return [
        SourceStat(index=i, source=str(source), count=int(row["count"]), avg_tone=round(float(row["avg_tone"]), 3))
        for i, (source, row) in enumerate(grouped.iterrows(), start=1)
    ]


def source_stats_from_articles(articles: list[Article]) -> list[SourceStat]:
    buckets: dict[str, list[float]] = {}
    for article in articles:
        source = article_domain(article.url) or article.source or "unknown"
        buckets.setdefault(source, []).append(safe_float(article.tone))
    rows = sorted(
        buckets.items(),
        key=lambda item: (len(item[1]), sum(item[1]) / len(item[1]) if item[1] else 0),
        reverse=True,
    )
    return [
        SourceStat(index=i, source=source, count=len(tones), avg_tone=round(sum(tones) / len(tones), 3) if tones else 0.0)
        for i, (source, tones) in enumerate(rows, start=1)
    ]


def _split_codes(value: str | None) -> list[str]:
    if not value:
        return []
    return [code.strip().upper() for code in value.split(",") if code.strip()]


def _split_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def sort_articles_by_published_at(articles: list[Article], order: str = "newest") -> list[Article]:
    normalized = parse_order(order)
    if normalized == "tone":
        return sorted(articles, key=lambda article: abs(safe_float(article.tone)), reverse=True)
    if normalized == "sources":
        return sorted(
            articles,
            key=lambda article: safe_float(article.raw.get("NumSources") or article.raw.get("NumArticles") or 0),
            reverse=True,
        )
    if normalized == "relevance":
        return rank_top_articles(articles)
    reverse = normalized == "newest"
    return sorted(articles, key=lambda article: event_time_number(article.published_at), reverse=reverse)


def rank_top_articles(articles: list[Article]) -> list[Article]:
    if not articles:
        return []

    timestamps = [event_time_number(article.published_at) for article in articles]
    min_time = min(timestamps)
    max_time = max(timestamps)
    time_span = max(max_time - min_time, 1)
    url_counts: dict[str, int] = {}
    domain_counts: dict[str, int] = {}
    for article in articles:
        url_key = article.url.strip().lower()
        if url_key:
            url_counts[url_key] = url_counts.get(url_key, 0) + 1
        domain = article_domain(article.url)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

    def score(article: Article) -> tuple[float, int]:
        mentions = safe_float(article.mentions)
        num_articles = safe_float(article.raw.get("NumArticles", 0))
        tone = abs(safe_float(article.tone))
        recency = (event_time_number(article.published_at) - min_time) / time_span
        watch_boost = min(len(article.watch_hits), 3) * 4.0
        duplicate_penalty = max(url_counts.get(article.url.strip().lower(), 1) - 1, 0) * 3.0
        source_penalty = max(domain_counts.get(article_domain(article.url), 1) - 1, 0) * 0.35
        total = (
            recency * 2.0
            + log1p(max(mentions, 0)) * 1.2
            + log1p(max(num_articles, 0)) * 0.8
            + min(tone / 10.0, 1.0) * 1.5
            + watch_boost
            - duplicate_penalty
            - source_penalty
        )
        return total, event_time_number(article.published_at)

    return sorted(articles, key=score, reverse=True)


def article_watch_match(
    article: Article,
    item: WatchItem,
    fulltext_url_matches: dict[str, set[str]] | None = None,
) -> str:
    value = item.value.lower().strip()
    if not value:
        return ""

    if item.kind == "country":
        countries = [
            article.country,
            str(article.raw.get("ActionGeo_CountryCode", "")),
            str(article.raw.get("Actor1CountryCode", "")),
            str(article.raw.get("Actor2CountryCode", "")),
        ]
        return "metadata" if any(country.lower() == value for country in countries if country) else ""

    if item.kind == "company":
        haystack = " ".join(
            [
                article.actors,
                str(article.raw.get("Actor1Name", "")),
                str(article.raw.get("Actor2Name", "")),
            ]
        ).lower()
        return "metadata" if value in haystack else ""

    if item.kind == "source":
        return "metadata" if value in article.url.lower() else ""

    if item.kind == "event":
        codes = [
            article.event_code,
            str(article.raw.get("EventBaseCode", "")),
            str(article.raw.get("EventRootCode", "")),
        ]
        return "metadata" if any(code.lower() == value for code in codes if code) else ""

    metadata_match = value in article_metadata_haystack(article)
    if item.kind == "keyword" and fulltext_url_matches:
        urls = fulltext_url_matches.get(item.value, set())
        if article.url in urls:
            return "fulltext" if not metadata_match else "metadata+fulltext"
    return "metadata" if metadata_match else ""


def partial_text_from(value: Any) -> str:
    for name in ["partial_text", "text", "body"]:
        text = getattr(value, name, "")
        if text:
            return str(text)
    return ""


def article_metadata_haystack(article: Article) -> str:
    fields = [
        article.title,
        article.event_label,
        article.event_code,
        article.actors,
        article.country,
        article.tone,
        article.url,
    ]
    fields.extend(str(article.raw.get(column, "")) for column in EVENT_FILTER_COLUMNS)
    return " ".join(fields).lower()


def event_time_number(value: str) -> int:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return 0
    return int(digits[:14].ljust(14, "0"))


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def article_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""


def alert_label(alert: dict[str, Any]) -> str:
    extras = []
    extras.append(f"state:{alert.get('state', 'active')}")
    if alert.get("country"):
        extras.append(f"country:{alert['country']}")
    if alert.get("source"):
        extras.append(f"source:{alert['source']}")
    if alert.get("tone_threshold") not in {None, ""}:
        extras.append(f"tone:{alert['tone_threshold']}")
    extras.append(f"freq:{alert.get('scan_frequency', 300)}s")
    if alert.get("last_scanned_at"):
        extras.append(f"last:{alert['last_scanned_at']}")
    if alert.get("muted_until"):
        extras.append(f"muted_until:{alert['muted_until']}")
    if alert.get("last_error"):
        extras.append(f"error:{alert['last_error']}")
    suffix = f" [{' '.join(extras)}]" if extras else ""
    return f"{alert['id']}. {alert['name']}: {alert['query']}{suffix}"


def parse_scan_frequency(value: str | None, default: int = 300) -> int:
    if not value:
        return default
    text = value.strip().lower()
    if text.isdigit():
        return max(int(text), 1)
    if text[-1:] not in {"m", "h", "d"}:
        raise ValueError("FREQ must be seconds or a window like 5m, 1h, or 1d.")
    try:
        delta = parse_since_window(text)
    except ValueError as exc:
        raise ValueError("FREQ must be seconds or a window like 5m, 1h, or 1d.") from exc
    return max(int(delta.total_seconds()), 1)


def parse_tone_threshold(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError("TONE must be a number.") from exc
