from datetime import datetime, timezone

import pytest

from newsfeed.tui.commands import parse_command, parse_limit, parse_order, parse_row_number, since_to_range


def test_parse_news_command_with_query_and_options():
    command = parse_command('NEWS "oil prices" SINCE:12h COUNTRY:US,UK')

    assert command.name == "NEWS"
    assert command.query == "oil prices"
    assert command.options == {"SINCE": "12h", "COUNTRY": "US,UK"}


def test_parse_export_command():
    command = parse_command("EXPORT FORMAT:json PATH:/tmp/newsfeed.json")

    assert command.name == "EXPORT"
    assert command.options["FORMAT"] == "json"
    assert command.options["PATH"] == "/tmp/newsfeed.json"


def test_parse_brief_command():
    command = parse_command("BRIEF CURRENT PATH:/tmp/brief.md")

    assert command.name == "BRIEF"
    assert command.args == ["CURRENT"]
    assert command.options["PATH"] == "/tmp/brief.md"


def test_parse_watch_command():
    command = parse_command('WATCH ADD keyword "oil prices"')

    assert command.name == "WATCH"
    assert command.args == ["ADD", "keyword", "oil prices"]


def test_parse_search_and_alert_commands():
    search = parse_command('SEARCH "oil price shock" LIMIT:10')
    alert = parse_command('ALERT ADD oil "oil price shock" COUNTRY:US SOURCE:example.com TONE:1.5 FREQ:5m')
    hits = parse_command("ALERT HITS")

    assert search.name == "SEARCH"
    assert search.query == "oil price shock"
    assert search.options["LIMIT"] == "10"
    assert alert.name == "ALERT"
    assert alert.args == ["ADD", "oil", "oil price shock"]
    assert alert.options["COUNTRY"] == "US"
    assert alert.options["SOURCE"] == "example.com"
    assert alert.options["TONE"] == "1.5"
    assert alert.options["FREQ"] == "5m"
    assert hits.args == ["HITS"]


def test_parse_pagination_commands():
    assert parse_command("NEXT").name == "NEXT"
    assert parse_command("PREV").name == "PREV"
    page = parse_command("PAGE 2")

    assert page.name == "PAGE"
    assert page.args == ["2"]


def test_parse_workspace_cache_config_and_geo_commands():
    assert parse_command('GEO "oil" SINCE:6h').name == "GEO"
    assert parse_command('SAVE QUERY oil NEWS "oil" SINCE:6h').args == ["QUERY", "oil", "NEWS", "oil"]
    assert parse_command("LOAD QUERY oil").args == ["QUERY", "oil"]
    assert parse_command("WORKSPACE USE macro").args == ["USE", "macro"]
    assert parse_command("CACHE STATS").args == ["STATS"]
    assert parse_command("CONFIG EXPORT PATH:/tmp/config.json").options["PATH"] == "/tmp/config.json"


def test_parse_row_number_validation():
    assert parse_row_number(parse_command("READ 3")) == 3

    with pytest.raises(ValueError, match="READ row must be 1 or greater"):
        parse_row_number(parse_command("READ 0"))


def test_parse_limit_validation():
    assert parse_limit(None) == 50
    assert parse_limit("25") == 25

    with pytest.raises(ValueError, match="between 1 and 250"):
        parse_limit("500")


def test_parse_order_validation():
    assert parse_order(None) == "newest"
    assert parse_order("oldest") == "oldest"

    with pytest.raises(ValueError, match="ORDER must be newest or oldest"):
        parse_order("sideways")


def test_since_to_range_uses_utc_windows():
    now = datetime(2026, 6, 23, 12, 30, 0, tzinfo=timezone.utc)

    start, end = since_to_range("90m", now=now)

    assert start == "2026-06-23-11-00-00"
    assert end == "2026-06-23-12-30-00"


def test_unknown_command_is_rejected():
    with pytest.raises(ValueError, match="Unknown command"):
        parse_command("BAD")
