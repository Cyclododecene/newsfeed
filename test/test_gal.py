import pandas as pd

import newsfeed.news.db.others as others
from newsfeed.news.db.others import GAL


def test_gal_query_reads_json_lines_and_deduplicates(monkeypatch):
    gal = GAL(start_date="2020-01-01-00-01-00", end_date="2020-01-01-00-02-00")
    calls = []

    def fake_download(url):
        calls.append(url)
        return pd.DataFrame([
            {"date": "20200101000100", "url": "https://example.com/a", "title": "A"},
            {"date": "20200101000100", "url": "https://example.com/a", "title": "A duplicate"},
        ])

    monkeypatch.setattr(gal, "_download_file", fake_download)

    result = gal.query()

    assert calls == ["20200101000100.gal.json.gz", "20200101000200.gal.json.gz"]
    assert list(result.columns) == gal.columns_name
    assert result["url"].tolist() == ["https://example.com/a"]


def test_gal_query_rss_feed_parses_items(monkeypatch):
    gal = GAL()
    rss = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Article A</title>
      <link>https://example.com/a</link>
      <description>Summary</description>
      <pubDate>Wed, 01 Jan 2020 00:01:00 GMT</pubDate>
      <guid>guid-a</guid>
    </item>
  </channel>
</rss>
"""

    class Response:
        ok = True
        content = rss
        status_code = 200

    monkeypatch.setattr(others.requests, "get", lambda *args, **kwargs: Response())

    result = gal.query_rss_feed()

    assert result.to_dict("records") == [{
        "title": "Article A",
        "url": "https://example.com/a",
        "description": "Summary",
        "pubDate": "Wed, 01 Jan 2020 00:01:00 GMT",
        "guid": "guid-a",
    }]


def test_gal_query_rss_feed_returns_value_error_for_http_error(monkeypatch):
    gal = GAL()

    class Response:
        ok = False
        content = b""
        status_code = 500

    monkeypatch.setattr(others.requests, "get", lambda *args, **kwargs: Response())

    result = gal.query_rss_feed()

    assert isinstance(result, ValueError)
    assert "status 500" in str(result)
