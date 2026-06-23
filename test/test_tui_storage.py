import sqlite3

from newsfeed.tui.models import Article
from newsfeed.tui.storage import TuiStorage, default_db_candidates, stable_article_id


def test_storage_creates_workspace_tables(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")

    watchlist_id = storage.get_or_create_watchlist()

    assert watchlist_id == 1
    assert (tmp_path / "tui.db").exists()


def test_default_db_candidates_include_tmp_fallback(monkeypatch):
    monkeypatch.delenv("NEWSFEED_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)

    candidates = default_db_candidates()

    assert candidates[0].name == "tui.db"
    assert str(candidates[-1]).endswith("newsfeed/tui.db")


def test_storage_persists_watchlist_items(tmp_path):
    db_path = tmp_path / "tui.db"
    storage = TuiStorage(db_path)
    item = storage.add_watch_item("country", "US")
    storage.close()

    reopened = TuiStorage(db_path)
    items = reopened.list_watch_items()

    assert items[0].id == item.id
    assert items[0].kind == "country"
    assert items[0].value == "US"


def test_storage_deletes_watchlist_item(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    item = storage.add_watch_item("keyword", "oil")

    assert storage.delete_watch_item(item.id) is True

    assert storage.list_watch_items() == []


def test_saved_queries_and_settings_round_trip(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")

    storage.save_query("oil", 'NEWS "oil" SINCE:6h')
    storage.set_setting("theme", {"name": "dark"})

    assert storage.list_saved_queries()[0]["command"] == 'NEWS "oil" SINCE:6h'
    assert storage.get_saved_query("oil")["command"] == 'NEWS "oil" SINCE:6h'
    assert storage.get_setting("theme") == {"name": "dark"}


def test_query_history_stores_lightweight_result_metadata(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")

    history_id = storage.record_query_history(
        'TOP SINCE:6h LIMIT:25',
        result_count=25,
        result_path="~/.cache/newsfeed/results/top.parquet",
    )
    history = storage.list_query_history()

    assert history[0]["id"] == history_id
    assert history[0]["result_count"] == 25
    assert history[0]["result_path"].endswith("top.parquet")


def test_article_index_stores_only_lightweight_metadata_and_paths(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(
        index=1,
        title="Make a visit | ACTOR",
        url="https://example.com/a",
        published_at="20260623120000",
        country="US",
        tone="-1.2",
        event_code="042",
        event_label="Make a visit",
        actors="ACTOR",
        mentions="5",
        fulltext="this body must not be stored in sqlite",
    )

    article_id = storage.upsert_article_index(
        article,
        content_hash="abc123",
        fulltext_path="~/.cache/newsfeed/fulltext/abc123.txt",
        result_path="~/.cache/newsfeed/results/query.parquet",
    )
    row = storage.get_article_index(article_id)

    assert article_id == stable_article_id(article)
    assert row["source_url"] == "https://example.com/a"
    assert row["event_label"] == "Make a visit"
    assert row["fulltext_path"].endswith("abc123.txt")
    assert row["enrichment_status"] == "none"
    assert "this body must not be stored" not in str(row)


def test_fulltext_cache_writes_file_and_searches_without_sqlite_body(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")

    content_hash, fulltext_path = storage.save_fulltext(article, "oil price shock in article body")
    row = storage.get_article_index(stable_article_id(article))
    matches = storage.search_fulltext("price shock")

    assert fulltext_path.endswith(f"{content_hash}.txt")
    assert row["fulltext_path"] == fulltext_path
    assert row["content_hash"] == content_hash
    assert row["enrichment_status"] == "indexed"
    assert row["enriched_at"] != ""
    assert matches[0]["source_url"] == "https://example.com/a"
    assert "oil price shock in article body" not in str(row)


def test_fulltext_cache_loads_existing_file_by_url(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")
    _, fulltext_path = storage.save_fulltext(article, "cached article body")
    refreshed = Article(index=1, title="A refreshed", url="https://example.com/a")

    cached = storage.load_cached_fulltext(refreshed)

    assert cached == ("cached article body", fulltext_path)
    assert refreshed.fulltext == "cached article body"
    assert refreshed.enrichment_status == "indexed"
    assert refreshed.raw["fulltext_path"] == fulltext_path


def test_article_index_preserves_fulltext_path_on_later_metadata_upsert(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")
    _, fulltext_path = storage.save_fulltext(article, "cached body")

    article.title = "Updated metadata title"
    storage.upsert_article_index(article)
    row = storage.get_article_index(stable_article_id(article))

    assert row["title"] == "Updated metadata title"
    assert row["fulltext_path"] == fulltext_path
    assert row["enrichment_status"] == "indexed"


def test_get_article_index_by_url_returns_latest_metadata(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")
    storage.save_fulltext(article, "cached body")

    row = storage.get_article_index_by_url("https://example.com/a")

    assert row["source_url"] == "https://example.com/a"
    assert row["enrichment_status"] == "indexed"


def test_enrichment_status_pending_and_failed(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")

    storage.set_enrichment_status(article, "pending")
    storage.set_enrichment_status(article, "failed", "download failed")
    row = storage.get_article_index(stable_article_id(article))

    assert article.enrichment_status == "failed"
    assert row["enrichment_status"] == "failed"
    assert row["enrichment_error"] == "download failed"


def test_storage_migrates_existing_article_index_schema(tmp_path):
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE article_index (
            id TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            title TEXT NOT NULL,
            event_code TEXT NOT NULL,
            event_label TEXT NOT NULL,
            actors TEXT NOT NULL,
            country TEXT NOT NULL,
            published_at TEXT NOT NULL,
            tone TEXT NOT NULL,
            mentions TEXT NOT NULL,
            content_hash TEXT NOT NULL DEFAULT '',
            fulltext_path TEXT NOT NULL DEFAULT '',
            result_path TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()

    storage = TuiStorage(db_path)
    article = Article(index=1, title="A", url="https://example.com/a")
    article_id = storage.upsert_article_index(article)
    row = storage.get_article_index(article_id)

    assert "enrichment_status" in row
    assert row["enrichment_status"] == "none"


def test_alert_check_matches_cached_fulltext_and_deduplicates_hits(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")
    storage.save_fulltext(article, "oil price shock in article body")
    storage.add_alert("oil", "oil price shock")

    first = storage.check_alerts()
    second = storage.check_alerts()
    hits = storage.list_alert_hits()

    assert first[0]["source_url"] == "https://example.com/a"
    assert second[0]["source_url"] == "https://example.com/a"
    assert len(hits) == 1


def test_alert_rules_match_metadata_country_source_and_tone(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(
        index=1,
        title="Oil supply shock",
        url="https://example.com/oil",
        country="US",
        tone="-2.5",
        event_label="Make a visit",
        actors="OIL PRODUCER",
    )
    storage.upsert_article_index(article)
    storage.add_alert(
        "oil",
        "supply shock",
        country="US",
        source="example.com",
        tone_threshold=2.0,
        scan_frequency=60,
    )

    hits = storage.check_alerts()
    alert = storage.list_alerts()[0]

    assert hits[0]["source_url"] == "https://example.com/oil"
    assert hits[0]["match_reason"] == "metadata+country+source+tone"
    assert alert["country"] == "US"
    assert alert["source"] == "example.com"
    assert alert["tone_threshold"] == 2.0
    assert alert["scan_frequency"] == 60
    assert alert["last_scanned_at"] != ""


def test_alert_unread_count_and_mark_read(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = Article(index=1, title="A", url="https://example.com/a")
    storage.save_fulltext(article, "oil price shock")
    storage.add_alert("oil", "oil")

    storage.check_alerts()
    hits = storage.list_alert_hits()

    assert storage.unread_alert_count() == 1
    assert hits[0]["read_at"] == ""

    storage.mark_alert_hits_read([hits[0]["id"]])

    assert storage.unread_alert_count() == 0


def test_storage_migrates_existing_alert_schema(tmp_path):
    db_path = tmp_path / "old_alerts.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            query TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE alert_hits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL,
            article_id TEXT NOT NULL,
            hit_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    conn.commit()
    conn.close()

    storage = TuiStorage(db_path)
    storage.add_alert("oil", "oil")
    alert = storage.list_alerts()[0]

    assert "country" in alert
    assert "last_scanned_at" in alert
    assert alert["scan_frequency"] == 300


def test_workspace_layout_and_config_round_trip(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    storage.add_watch_item("country", "US", watchlist="macro")
    storage.save_query("oil", 'NEWS "oil"')
    storage.add_alert("oil", "oil")
    storage.set_setting("active_workspace", "macro")
    storage.save_workspace_layout("macro-layout", {"workspace": "macro", "page_size": 25})
    config_path = tmp_path / "config.json"

    storage.export_config(config_path)
    imported = TuiStorage(tmp_path / "imported.db")
    counts = imported.import_config(config_path)

    assert counts["watchlists"] == 1
    assert imported.list_watchlists() == ["macro"]
    assert imported.get_saved_query("oil")["command"] == 'NEWS "oil"'
    assert imported.list_alerts()[0]["name"] == "oil"
    assert imported.get_setting("active_workspace") == "macro"
    assert imported.get_workspace_layout("macro-layout")["layout"]["page_size"] == 25


def test_cache_stats_and_cleanup_results(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    result_path = storage.results_dir / "result.parquet"
    result_path.write_text("cached result", encoding="utf-8")

    stats = storage.cache_stats()
    cleaned = storage.cleanup_cache("results")

    assert stats["result_files"] == 1
    assert cleaned["removed_files"] == 1
    assert not result_path.exists()
