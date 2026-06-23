import json

import pandas as pd

from newsfeed.tui.export import articles_to_dataframe, cached_excerpt, export_articles, export_brief, export_timeline
from newsfeed.tui.models import Article, TimelinePoint


def test_articles_to_dataframe_keeps_dense_columns():
    article = Article(
        index=1,
        title="Headline",
        url="https://example.com",
        source="example.com",
        published_at="20260623120000",
        country="US",
        tone="0.1",
    )

    dataframe = articles_to_dataframe([article])

    assert list(dataframe.columns) == [
        "index",
        "title",
        "source",
        "published_at",
        "display_time",
        "url",
        "country",
        "language",
        "tone",
        "event_code",
        "event_label",
        "actors",
        "mentions",
        "enrichment_status",
        "query",
        "fulltext",
        "error",
    ]
    assert dataframe.iloc[0]["title"] == "Headline"


def test_export_articles_to_json(tmp_path):
    path = tmp_path / "articles.json"
    export_articles([Article(index=1, title="A", url="https://example.com")], "json", str(path))

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload[0]["title"] == "A"


def test_export_timeline_to_csv(tmp_path):
    path = tmp_path / "timeline.csv"
    export_timeline([TimelinePoint(timestamp="20260623120000", value=4)], "csv", str(path))

    dataframe = pd.read_csv(path)

    assert dataframe.to_dict("records") == [{"timestamp": 20260623120000, "value": 4}]


def test_export_brief_writes_markdown_with_cached_excerpt(tmp_path):
    fulltext_path = tmp_path / "fulltext.txt"
    fulltext_path.write_text("Oil price shock details " * 50, encoding="utf-8")
    path = tmp_path / "brief.md"
    article = Article(
        index=1,
        title="Oil",
        url="https://example.com/oil",
        display_time="2026-06-23 12:00",
        country="US",
        event_label="Make a visit",
        actors="OIL PRODUCER",
        tone="-1.2",
        match_reason="alert:oil (fulltext)",
        raw={"fulltext_path": str(fulltext_path)},
    )

    export_brief([article], str(path), title="Test Brief")
    content = path.read_text(encoding="utf-8")

    assert "# Test Brief" in content
    assert "- Time: 2026-06-23 12:00" in content
    assert "- SourceURL: https://example.com/oil" in content
    assert "- Match: alert:oil (fulltext)" in content
    assert str(fulltext_path) in content
    assert "Excerpt:" in content
    assert "Oil price shock details" in content


def test_cached_excerpt_truncates_text():
    article = Article(index=1, title="A", url="https://example.com", fulltext="x" * 1000)

    excerpt = cached_excerpt(article, max_chars=20)

    assert excerpt == ("x" * 20) + "..."
