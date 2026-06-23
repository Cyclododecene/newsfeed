from datetime import datetime, timezone

import pandas as pd
import pytest

from newsfeed.tui.commands import parse_command
from newsfeed.tui.models import article_from_event_record, cameo_label, format_event_time
from newsfeed.tui.services import (
    NewsService,
    WatchItem,
    parse_scan_frequency,
    parse_tone_threshold,
    event_since_to_range,
    filter_events,
    filter_watchlist,
    query_event_v2_sequential,
    rank_events,
    rank_top_articles,
    sort_events,
)
from newsfeed.tui.storage import TuiStorage, stable_article_id


def fixed_now():
    return datetime(2026, 6, 23, 12, 0, 0, tzinfo=timezone.utc)


class FakeEventQuery:
    calls = []
    dataframe = pd.DataFrame()

    def __init__(self, **kwargs):
        self.calls.append(kwargs)

    def query(self):
        return self.dataframe


def event_dataframe():
    return pd.DataFrame(
        [
            {
                "GLOBALEVENTID": 1,
                "Actor1Name": "OIL PRODUCER",
                "Actor2Name": "GOVERNMENT",
                "Actor1CountryCode": "US",
                "Actor2CountryCode": "UK",
                "ActionGeo_CountryCode": "US",
                "ActionGeo_FullName": "Houston, Texas",
                "EventCode": "042",
                "NumMentions": 5,
                "NumArticles": 2,
                "AvgTone": "-1.2",
                "DATEADDED": "20260623114500",
                "SOURCEURL": "https://example.com/oil",
            },
            {
                "GLOBALEVENTID": 2,
                "Actor1Name": "CENTRAL BANK",
                "Actor2Name": "MARKET",
                "Actor1CountryCode": "FR",
                "Actor2CountryCode": "DE",
                "ActionGeo_CountryCode": "FR",
                "ActionGeo_FullName": "Paris, France",
                "EventCode": "010",
                "NumMentions": 50,
                "NumArticles": 9,
                "AvgTone": "0.5",
                "DATEADDED": "20260623113000",
                "SOURCEURL": "https://example.com/bank",
            },
        ]
    )


def test_news_query_uses_event_v2_without_doc_api():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    articles = service.news(parse_command('NEWS "oil producer" SINCE:2h COUNTRY:US LIMIT:25'))

    assert FakeEventQuery.calls[0]["start_date"] == "2026-06-23-09-30-00"
    assert FakeEventQuery.calls[0]["end_date"] == "2026-06-23-11-30-00"
    assert FakeEventQuery.calls[0]["table"] == "events"
    assert FakeEventQuery.calls[0]["use_cache"] is True
    assert articles[0].title == "Make a visit | OIL PRODUCER / GOVERNMENT | Houston, Texas"
    assert articles[0].event_label == "Make a visit"
    assert articles[0].actors == "OIL PRODUCER / GOVERNMENT"
    assert articles[0].mentions == "5"
    assert articles[0].display_time == "2026-06-23 11:45"
    assert articles[0].url == "https://example.com/oil"
    assert articles[0].country == "US"


def test_top_ranks_event_rows_by_mentions_articles_tone_and_date():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    articles = service.top(parse_command("TOP LIMIT:2 ORDER:oldest"))

    assert len(articles) == 2
    assert articles[0].actors == "CENTRAL BANK / MARKET"
    assert articles[1].actors == "OIL PRODUCER / GOVERNMENT"


def test_top_default_uses_top_news_ranking_v1():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    articles = service.top(parse_command("TOP LIMIT:2"))

    assert articles[0].actors == "CENTRAL BANK / MARKET"
    assert articles[1].actors == "OIL PRODUCER / GOVERNMENT"


def test_top_order_option_keeps_explicit_time_sort():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    articles = service.top(parse_command("TOP LIMIT:2 ORDER:newest"))

    assert articles[0].actors == "OIL PRODUCER / GOVERNMENT"
    assert articles[1].actors == "CENTRAL BANK / MARKET"


def test_timeline_aggregates_event_counts():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    points = service.timeline(parse_command('TL "oil" SINCE:24h'))

    assert points[0].timestamp == "202606231145"
    assert points[0].value == 1


def test_timeline_supports_tone_and_source_country_modes():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    tone_points = service.timeline(parse_command('TL "oil" SINCE:24h MODE:tone'))
    country_points = service.timeline(parse_command("TL SINCE:24h MODE:source_country"))

    assert tone_points[0].value == -1.2
    assert country_points[0].raw["mode"] == "source_country"
    assert country_points[0].value in {"US", "FR"}


def test_geo_aggregates_event_countries():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    points = service.geo(parse_command("GEO SINCE:24h"))

    assert {point.timestamp: point.value for point in points} == {"US": 1, "FR": 1}


def test_filter_events_matches_country_and_all_query_terms():
    dataframe = filter_events(event_dataframe(), query="oil houston", countries=["US"])

    assert dataframe["GLOBALEVENTID"].tolist() == [1]


def test_cameo_label_maps_event_code():
    assert cameo_label("042") == "Make a visit"
    assert cameo_label("42") == "Make a visit"


def test_format_event_time_makes_compact_timestamps_readable():
    assert format_event_time("20260623091500") == "2026-06-23 09:15"
    assert format_event_time("20260623") == "2026-06-23"


def test_article_from_event_record_uses_event_stream_fields():
    article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())

    assert article.event_code == "042"
    assert article.event_label == "Make a visit"
    assert article.actors == "OIL PRODUCER / GOVERNMENT"


def test_watchlist_filters_country_and_keyword_matches():
    watchlist = [
        WatchItem(index=1, kind="country", value="FR"),
        WatchItem(index=2, kind="keyword", value="oil"),
    ]

    dataframe = filter_watchlist(event_dataframe(), watchlist)

    assert dataframe["GLOBALEVENTID"].tolist() == [1, 2]


def test_watch_news_uses_saved_watch_items():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    service.add_watch_item("keyword", "oil")
    articles = service.watch_news(parse_command("WATCH NEWS SINCE:6h LIMIT:10"))

    assert len(articles) == 1
    assert articles[0].title.startswith("Make a visit")


def test_watch_news_keyword_uses_cached_fulltext_matches(tmp_path):
    dataframe = event_dataframe().copy()
    dataframe.loc[1, "Actor1Name"] = "CENTRAL BANK"
    dataframe.loc[1, "SOURCEURL"] = "https://example.com/bank"
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = dataframe
    storage = TuiStorage(tmp_path / "tui.db")
    indexed = article_from_event_record(1, dataframe.iloc[1].to_dict())
    storage.save_fulltext(indexed, "oil price shock appears only in full text")

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now, storage=storage)
    service.add_watch_item("keyword", "price shock")
    articles = service.watch_news(parse_command("WATCH NEWS SINCE:6h LIMIT:10"))

    assert len(articles) == 1
    assert articles[0].url == "https://example.com/bank"
    assert articles[0].watch_hits == ["keyword:price shock (fulltext)"]


def test_news_results_mark_watch_hits_from_event_metadata():
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now)
    service.add_watch_item("country", "US")
    service.add_watch_item("keyword", "oil")
    articles = service.news(parse_command('NEWS "oil" SINCE:2h LIMIT:5'))

    assert articles[0].watch_hits == ["country:US (metadata)", "keyword:oil (metadata)"]
    assert "country:US" in articles[0].match_reason


def test_rank_top_articles_boosts_watchlist_matches():
    low_mentions_watch = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    low_mentions_watch.watch_hits = ["keyword:oil (metadata)"]
    high_mentions = article_from_event_record(2, event_dataframe().iloc[1].to_dict())

    ranked = rank_top_articles([high_mentions, low_mentions_watch])

    assert ranked[0] is low_mentions_watch


def test_watchlist_uses_sqlite_storage_when_available(tmp_path):
    db_path = tmp_path / "tui.db"
    service = NewsService(storage=TuiStorage(db_path))
    service.add_watch_item("country", "US")

    restarted = NewsService(storage=TuiStorage(db_path))

    assert restarted.watchlist_labels() == ["1. country:US"]


def test_workspace_switch_isolates_watchlist_items(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    service = NewsService(storage=storage)

    service.add_watch_item("country", "US")
    service.set_workspace("macro")
    service.add_watch_item("keyword", "oil")
    service.set_workspace("default")

    assert service.watchlist_labels() == ["1. country:US"]
    service.set_workspace("macro")
    assert service.watchlist_labels() == ["1. keyword:oil"]


def test_service_fulltext_cache_and_search(tmp_path):
    class FakeFulltext:
        text = "oil price shock in body"

    storage = TuiStorage(tmp_path / "tui.db")
    service = NewsService(fulltext_func=lambda url: FakeFulltext(), storage=storage)
    article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())

    service.fetch_fulltext(article)
    matches = service.search_fulltext(parse_command('SEARCH "price shock" LIMIT:5'))
    row = storage.get_article_index(matches[0].raw["id"])

    assert article.enrichment_status == "indexed"
    assert article.raw["fulltext_path"].endswith(".txt")
    assert matches[0].url == article.url
    assert row["enrichment_status"] == "indexed"


def test_service_fetch_fulltext_reuses_cached_file_without_downloading(tmp_path):
    def fail_download(url):
        raise AssertionError("cached fulltext should not be downloaded again")

    storage = TuiStorage(tmp_path / "tui.db")
    cached_article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    storage.save_fulltext(cached_article, "cached fulltext body")
    refreshed_article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    service = NewsService(fulltext_func=fail_download, storage=storage)

    updated = service.fetch_fulltext(refreshed_article)

    assert updated.fulltext == "cached fulltext body"
    assert updated.enrichment_status == "indexed"
    assert updated.error == ""


def test_event_refresh_hydrates_existing_fulltext_status(tmp_path):
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()
    storage = TuiStorage(tmp_path / "tui.db")
    indexed = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    storage.save_fulltext(indexed, "cached fulltext")

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now, storage=storage)
    articles = service.top(parse_command("TOP LIMIT:2"))

    refreshed = next(article for article in articles if article.url == indexed.url)
    assert refreshed.enrichment_status == "indexed"
    assert refreshed.raw["fulltext_path"].endswith(".txt")


def test_service_fulltext_failure_sets_failed_status(tmp_path):
    def fail_download(url):
        raise RuntimeError("network failed")

    storage = TuiStorage(tmp_path / "tui.db")
    service = NewsService(fulltext_func=fail_download, storage=storage)
    article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())

    updated = service.fetch_fulltext(article)
    row = storage.get_article_index(stable_article_id(updated))

    assert updated.enrichment_status == "failed"
    assert row["enrichment_status"] == "failed"
    assert row["enrichment_error"] == "network failed"


def test_service_alert_add_and_check_cached_fulltext(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    storage.save_fulltext(article, "geopolitical oil shock")
    service = NewsService(storage=storage)

    alert_id = service.add_alert("oil", "oil shock")
    alerts = service.list_alerts()
    hits = service.check_alerts()

    assert alert_id == 1
    assert alerts == ["1. oil: oil shock [freq:300s]"]
    assert hits[0].url == article.url


def test_service_alert_extended_options_and_hits_mark_read(tmp_path):
    storage = TuiStorage(tmp_path / "tui.db")
    article = article_from_event_record(1, event_dataframe().iloc[0].to_dict())
    storage.save_fulltext(article, "geopolitical oil shock")
    service = NewsService(storage=storage)

    alert_id = service.add_alert(
        "oil",
        "oil shock",
        country="US",
        source="example.com",
        tone_threshold=1.0,
        scan_frequency=60,
    )
    checked = service.check_alerts()
    hits = service.alert_hits(mark_read=True)

    assert alert_id == 1
    assert checked[0].url == article.url
    assert hits[0].url == article.url
    assert hits[0].query == "ALERT"
    assert hits[0].match_reason.startswith("alert:oil")
    assert service.unread_alert_count() == 0


def test_parse_alert_rule_helpers():
    assert parse_scan_frequency("5m") == 300
    assert parse_scan_frequency("60") == 60
    assert parse_tone_threshold("1.5") == 1.5

    with pytest.raises(ValueError, match="FREQ"):
        parse_scan_frequency("bad")


def test_service_indexes_article_metadata_without_fulltext_body(tmp_path):
    FakeEventQuery.calls = []
    FakeEventQuery.dataframe = event_dataframe()
    storage = TuiStorage(tmp_path / "tui.db")

    service = NewsService(event_factory=FakeEventQuery, now_func=fixed_now, storage=storage)
    articles = service.top(parse_command("TOP LIMIT:1"))
    article_id = storage.index_articles(articles)[0]
    row = storage.get_article_index(article_id)

    assert row["source_url"] == articles[0].url
    assert row["event_label"] == articles[0].event_label
    assert "raw" not in row
    assert "fulltext" not in row


def test_rank_events_handles_empty_dataframe():
    dataframe = rank_events(pd.DataFrame())

    assert dataframe.empty


def test_sort_events_supports_newest_and_oldest():
    newest = sort_events(event_dataframe(), "newest")
    oldest = sort_events(event_dataframe(), "oldest")

    assert newest["GLOBALEVENTID"].tolist() == [1, 2]
    assert oldest["GLOBALEVENTID"].tolist() == [2, 1]


def test_event_since_to_range_applies_lag_and_15_minute_alignment():
    now = datetime(2026, 6, 23, 12, 7, 30, tzinfo=timezone.utc)

    start, end = event_since_to_range("1h", now=now)

    assert start == "2026-06-23-10-30-00"
    assert end == "2026-06-23-11-30-00"


def test_query_events_falls_back_to_nearest_available_file_when_window_is_empty():
    class EmptyThenNowEventQuery:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def query(self):
            return pd.DataFrame()

        def query_nowtime(self, max_lookback):
            assert max_lookback == 96
            return event_dataframe().head(1)

    service = NewsService(event_factory=EmptyThenNowEventQuery, now_func=fixed_now)
    articles = service.top(parse_command("TOP LIMIT:1"))

    assert len(articles) == 1
    assert articles[0].actors == "OIL PRODUCER / GOVERNMENT"


def test_service_converts_event_errors_to_runtime_errors():
    class FailingEventQuery:
        def __init__(self, **kwargs):
            pass

        def query(self):
            return ValueError("event download failed")

    service = NewsService(event_factory=FailingEventQuery, now_func=fixed_now)

    with pytest.raises(RuntimeError, match="event download failed"):
        service.news(parse_command('NEWS "markets"'))


def test_event_v2_sequential_helper_avoids_query_multiprocessing_branch():
    class FakeCache:
        def get(self, **kwargs):
            return None

        def set(self, *args, **kwargs):
            self.saved = True

    class SequentialEventQuery:
        use_cache = False
        force_redownload = False
        start_date = "20260623100000"
        end_date = "20260623120000"
        table = "events"
        translation = False
        cache_manager = FakeCache()
        columns_name_mentions = []
        columns_name_events = [
            "GLOBALEVENTID",
            "SQLDATE",
            "MonthYear",
            "Year",
            "FractionDate",
            "Actor1Code",
            "Actor1Name",
            "Actor1CountryCode",
            "Actor1KnownGroupCode",
            "Actor1EthnicCode",
            "Actor1Religion1Code",
            "Actor1Religion2Code",
            "Actor1Type1Code",
            "Actor1Type2Code",
            "Actor1Type3Code",
            "Actor2Code",
            "Actor2Name",
            "Actor2CountryCode",
            "Actor2KnownGroupCode",
            "Actor2EthnicCode",
            "Actor2Religion1Code",
            "Actor2Religion2Code",
            "Actor2Type1Code",
            "Actor2Type2Code",
            "Actor2Type3Code",
            "IsRootEvent",
            "EventCode",
            "EventBaseCode",
            "EventRootCode",
            "QuadClass",
            "GoldsteinScale",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "Actor1Geo_Type",
            "Actor1Geo_FullName",
            "Actor1Geo_CountryCode",
            "Actor1Geo_ADM1Code",
            "Actor1Geo_ADM2Code",
            "Actor1Geo_Lat",
            "Actor1Geo_Long",
            "Actor1Geo_FeatureID",
            "Actor2Geo_Type",
            "Actor2Geo_FullName",
            "Actor2Geo_CountryCode",
            "Actor2Geo_ADM1Code",
            "Actor2Geo_ADM2Code",
            "Actor2Geo_Lat",
            "Actor2Geo_Long",
            "Actor2Geo_FeatureID",
            "ActionGeo_Type",
            "ActionGeo_FullName",
            "ActionGeo_CountryCode",
            "ActionGeo_ADM1Code",
            "ActionGeo_ADM2Code",
            "ActionGeo_Lat",
            "ActionGeo_Long",
            "ActionGeo_FeatureID",
            "DATEADDED",
            "SOURCEURL",
        ]

        def query(self):
            raise AssertionError("multiprocessing query branch should not be called")

        def _query_list(self):
            return ["20260623100000.export.CSV.zip"]

        def _download_file(self, url):
            assert url == "20260623100000.export.CSV.zip"
            return pd.DataFrame([[1] + [""] * (len(self.columns_name_events) - 1)])

    dataframe = query_event_v2_sequential(SequentialEventQuery())

    assert dataframe.columns.tolist() == SequentialEventQuery.columns_name_events
    assert dataframe.iloc[0]["GLOBALEVENTID"] == 1


def test_fetch_fulltext_updates_article_without_network():
    from newsfeed.tui.models import Article

    class FakeFulltext:
        text = "Full article body"

    service = NewsService(fulltext_func=lambda url: FakeFulltext())
    article = Article(index=1, title="A", url="https://example.com/a")
    updated = service.fetch_fulltext(article)

    assert updated.fulltext == "Full article body"
    assert updated.error == ""
