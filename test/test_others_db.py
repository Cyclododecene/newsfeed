import pandas as pd

import newsfeed.news.db.others as others
from newsfeed.news.db.others import GEG, VGEG, GDG, GFG


def frame_for(columns):
    return pd.DataFrame([list(range(len(columns)))])


def test_geg_query_uses_async_downloader(monkeypatch):
    geg = GEG(start_date="2020-01-01", end_date="2020-01-02", use_async=True)
    monkeypatch.setattr(geg, "_query_list", lambda: ["https://example.com/geg.json.gz"])

    def fake_run_async_download(base_url, download_url_list, max_concurrent=10, proxy=None, is_full_url=False):
        assert base_url == ""
        assert download_url_list == ["https://example.com/geg.json.gz"]
        assert is_full_url is True
        return [frame_for(geg.columns_name)], []

    monkeypatch.setattr(others, "run_async_download", fake_run_async_download)

    result = geg.query()

    assert list(result.columns) == geg.columns_name


def test_vgeg_query_uses_async_downloader(monkeypatch):
    vgeg = VGEG(query_date="2020-01-01", domain="CNN", use_async=True)
    monkeypatch.setattr(vgeg, "_query_list", lambda: ["https://example.com/vgeg.json.gz"])

    def fake_run_async_download(base_url, download_url_list, max_concurrent=10, proxy=None, is_full_url=False):
        assert base_url == ""
        assert download_url_list == ["https://example.com/vgeg.json.gz"]
        assert is_full_url is True
        return [frame_for(vgeg.columns_name_vgeg)], []

    monkeypatch.setattr(others, "run_async_download", fake_run_async_download)

    result = vgeg.query()

    assert list(result.columns) == vgeg.columns_name_vgeg


def test_vgeg_query_list_allows_missing_domain(monkeypatch):
    vgeg = VGEG(query_date="2020-01-01", domain=None)

    def fake_read_csv(*args, **kwargs):
        return pd.DataFrame({"url": ["https://example.com/a.json.gz"]})

    monkeypatch.setattr(others.pd, "read_csv", fake_read_csv)

    assert vgeg._query_list() == ["https://example.com/a.json.gz"]


def test_gdg_query_reads_json_response(monkeypatch):
    gdg = GDG(query_date="2018-08-27-14-00-00")

    class Response:
        ok = True
        content = b"{}"

    monkeypatch.setattr(others.requests, "get", lambda *args, **kwargs: Response())
    monkeypatch.setattr(others.pd, "read_json", lambda *args, **kwargs: pd.DataFrame([{"score": 1}]))

    result = gdg.query()

    assert result.to_dict("records") == [{"score": 1}]


def test_gfg_query_reads_frontpage_links(monkeypatch):
    monkeypatch.setattr(GFG, "latest_date", lambda self: None)
    gfg = GFG(query_date="2018-03-02-02-00-00")

    class Response:
        ok = True

    monkeypatch.setattr(others.requests, "get", lambda *args, **kwargs: Response())
    monkeypatch.setattr(others.pd, "read_csv", lambda *args, **kwargs: frame_for(gfg.columns_name))

    result = gfg.query()

    assert list(result.columns) == gfg.columns_name
