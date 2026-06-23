import pandas as pd

import newsfeed.news.apis.query as api_query
from newsfeed.news.apis.filters import Art_Filter


class FakeResponse:
    def __init__(self, text, ok=True, status_code=200, payload=None):
        self.text = text
        self.ok = ok
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("invalid json")
        return self._payload


def test_tv_query_search_builds_params_and_parses_json(monkeypatch):
    calls = []

    def fake_get(url, params=None, proxies=None, timeout=None):
        calls.append({
            "url": url,
            "params": params,
            "proxies": proxies,
            "timeout": timeout,
        })
        return FakeResponse(
            '{"timeline": []}',
            payload={"timeline": []},
        )

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.tv_query_search(
        query_string='trump station:CNN',
        mode="timelinevol",
        format="json",
        start_date="2017-08-29-12-00-00",
        end_date="2017-10-07-12-00-00",
        last24=False,
        timezoom=True,
        proxy={"https": "proxy"},
        timeout=5,
    )

    assert result == {"timeline": []}
    assert calls == [{
        "url": "https://api.gdeltproject.org/api/v2/tv/tv",
        "params": {
            "mode": "timelinevol",
            "format": "json",
            "datanorm": "perc",
            "timelinesmooth": 0,
            "datacomb": "sep",
            "query": "trump station:CNN",
            "STARTDATETIME": "20170829120000",
            "ENDDATETIME": "20171007120000",
            "last24": "no",
            "timezoom": "yes",
        },
        "proxies": {"https": "proxy"},
        "timeout": 5,
    }]


def test_tv_query_search_returns_dataframe_for_csv(monkeypatch):
    def fake_get(url, params=None, proxies=None, timeout=None):
        return FakeResponse("date,value\n20210101,2\n")

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.tv_query_search(
        query_string="trump",
        mode="timelinevol",
        format="csv",
        timespan="14days",
    )

    assert isinstance(result, pd.DataFrame)
    assert result.to_dict("records") == [{"date": 20210101, "value": 2}]


def test_tv_search_uses_art_filter_and_tv_operators(monkeypatch):
    captured = {}

    def fake_tv_query_search(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(api_query, "tv_query_search", fake_tv_query_search)
    query_filter = Art_Filter(
        keyword=["Puerto Rico", "hurricane"],
        start_date="20170829120000",
        end_date="20171007120000",
    )

    result = api_query.tv_search(
        query_filter=query_filter,
        station="CNN",
        market="National",
        context="relief",
        mode="clipgallery",
        format="json",
        maxrecords=25,
    )

    assert result == {"ok": True}
    assert "&startdatetime" not in captured["query_string"]
    assert '("Puerto Rico" OR hurricane)' in captured["query_string"]
    assert "station:CNN" in captured["query_string"]
    assert 'market:"National"' in captured["query_string"]
    assert 'context:"relief"' in captured["query_string"]
    assert captured["start_date"] == "20170829120000"
    assert captured["end_date"] == "20171007120000"
    assert captured["mode"] == "clipgallery"
    assert captured["maxrecords"] == 25


def test_tv_query_search_validates_inputs(monkeypatch):
    assert isinstance(api_query.tv_query_search(query_string=None), ValueError)
    assert isinstance(
        api_query.tv_query_search(query_string="test", timespan="1day", start_date="20210101000000"),
        ValueError,
    )
    assert isinstance(api_query.tv_query_search(query_string="test", maxrecords=0), ValueError)

    def fake_get(url, params=None, proxies=None, timeout=None):
        return FakeResponse("server error", ok=False, status_code=503)

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.tv_query_search(query_string="test")

    assert isinstance(result, ValueError)
    assert "status 503" in str(result)
