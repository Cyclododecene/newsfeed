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


def test_geo_query_search_uses_params_and_can_parse_json(monkeypatch):
    calls = []

    def fake_get(url, params=None, proxies=None, timeout=None):
        calls.append({
            "url": url,
            "params": params,
            "proxies": proxies,
            "timeout": timeout,
        })
        return FakeResponse(
            '{"type":"FeatureCollection","features":[]}',
            payload={"type": "FeatureCollection", "features": []},
        )

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.geo_query_search(
        query_string='theme:TAX_FNCACT "exchange rate"',
        timespan=7,
        proxy={"https": "proxy"},
        timeout=5,
        parse_json=True,
    )

    assert result == {"type": "FeatureCollection", "features": []}
    assert calls == [{
        "url": "https://api.gdeltproject.org/api/v2/geo/geo",
        "params": {
            "query": 'theme:TAX_FNCACT "exchange rate"',
            "format": "GeoJSON",
            "timespan": "7d",
        },
        "proxies": {"https": "proxy"},
        "timeout": 5,
    }]


def test_geo_query_search_returns_value_error_for_bad_inputs_and_http_errors(monkeypatch):
    assert isinstance(api_query.geo_query_search(query_string=None), ValueError)
    assert isinstance(api_query.geo_query_search(query_string="test", timespan=0), ValueError)

    def fake_get(url, params=None, proxies=None, timeout=None):
        return FakeResponse("server error", ok=False, status_code=500)

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.geo_query_search(query_string="test")

    assert isinstance(result, ValueError)
    assert "status 500" in str(result)


def test_geo_search_strips_doc_api_parameters_and_adds_source_language(monkeypatch):
    captured = {}

    def fake_geo_query_search(**kwargs):
        captured.update(kwargs)
        return "geo"

    monkeypatch.setattr(api_query, "geo_query_search", fake_geo_query_search)
    query_filter = Art_Filter(
        keyword=["Exchange Rate", "World"],
        start_date="20211231000000",
        end_date="20211231010000",
        country=["China", "US"],
    )

    result = api_query.geo_search(
        query_filter=query_filter,
        sourcelang="english",
        timespan=3,
        timeout=9,
        parse_json=True,
    )

    assert result == "geo"
    assert "&startdatetime" not in captured["query_string"]
    assert "&enddatetime" not in captured["query_string"]
    assert "&maxrecords" not in captured["query_string"]
    assert '("Exchange Rate" OR World)' in captured["query_string"]
    assert "sourcelang:english" in captured["query_string"]
    assert captured["timespan"] == 3
    assert captured["timeout"] == 9
    assert captured["parse_json"] is True


def test_doc_query_search_returns_value_error_for_malformed_timeline(monkeypatch):
    def fake_get(url, params=None, proxies=None, timeout=None):
        assert params["mode"] == "timelinevolraw"
        return FakeResponse('{"timeline": 0}')

    monkeypatch.setattr(api_query.requests, "get", fake_get)

    result = api_query.doc_query_search(query_string="test", mode="timelinevolraw")

    assert isinstance(result, ValueError)
    assert "timeline data" in str(result)


def test_timeline_search_returns_value_error_instead_of_raising(monkeypatch):
    query_filter = Art_Filter(
        keyword=["Exchange Rate", "World"],
        start_date="20211231000000",
        end_date="20211231010000",
    )

    monkeypatch.setattr(
        api_query,
        "doc_query_search",
        lambda **kwargs: ValueError("bad timeline"),
    )

    result = api_query.timeline_search(query_filter=query_filter, query_mode="timelinevolraw")

    assert isinstance(result, ValueError)
