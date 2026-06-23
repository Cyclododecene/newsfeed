import pytest
import pandas as pd

from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2


def frame_for(columns):
    return pd.DataFrame([list(range(len(columns)))])


def test_event_v1_query_nowtime_falls_back_to_previous_day(monkeypatch):
    event = EventV1()
    attempts = []

    def fake_download(url):
        attempts.append(url)
        if len(attempts) == 1:
            return "missing"
        return frame_for(event.columns_name)

    monkeypatch.setattr(event, "_download_file", fake_download)

    result = event.query_nowtime(date="2021-01-02", max_lookback=2)

    assert attempts == ["20210102.export.CSV.zip", "20210101.export.CSV.zip"]
    assert list(result.columns) == event.columns_name


def test_event_v2_query_nowtime_falls_back_by_15_minute_intervals(monkeypatch):
    event = EventV2(table="mentions")
    attempts = []

    def fake_download(url):
        attempts.append(url)
        if len(attempts) == 1:
            return "missing"
        return frame_for(event.columns_name_mentions)

    monkeypatch.setattr(event, "_download_file", fake_download)

    result = event.query_nowtime(date="2021-01-01-00-29-00", max_lookback=2)

    assert attempts == [
        "20210101001500.mentions.CSV.zip",
        "20210101000000.mentions.CSV.zip",
    ]
    assert list(result.columns) == event.columns_name_mentions


def test_gkg_v1_query_nowtime_falls_back_to_previous_day(monkeypatch):
    gkg = GKGV1()
    attempts = []

    def fake_download(url):
        attempts.append(url)
        if len(attempts) == 1:
            return RuntimeError("missing")
        return frame_for(gkg.columns_name)

    monkeypatch.setattr(gkg, "_download_file", fake_download)

    result = gkg.query_nowtime(date="2021-01-02", max_lookback=2)

    assert attempts == ["20210102.gkg.csv.zip", "20210101.gkg.csv.zip"]
    assert list(result.columns) == gkg.columns_name


def test_gkg_v2_query_nowtime_uses_translation_offset(monkeypatch):
    gkg = GKGV2(translation=True)
    attempts = []

    def fake_download(url):
        attempts.append(url)
        return frame_for(gkg.columns_name)

    monkeypatch.setattr(gkg, "_download_file", fake_download)

    result = gkg.query_nowtime(date="2021-01-01-00-29-00", max_lookback=0)

    assert attempts == ["20210101000000.translation.gkg.csv.zip"]
    assert list(result.columns) == gkg.columns_name


def test_query_nowtime_raises_after_bounded_lookback(monkeypatch):
    event = EventV1()
    attempts = []

    def fake_download(url):
        attempts.append(url)
        return "missing"

    monkeypatch.setattr(event, "_download_file", fake_download)

    with pytest.raises(ValueError, match="No Events V1 data found"):
        event.query_nowtime(date="2021-01-02", max_lookback=1)

    assert attempts == ["20210102.export.CSV.zip", "20210101.export.CSV.zip"]
