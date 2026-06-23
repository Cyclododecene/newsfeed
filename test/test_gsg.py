import pandas as pd

import newsfeed.news.db.others as others
from newsfeed.news.db.others import GSG


def test_gsg_docembed_query_list_uses_15_minute_files():
    gsg = GSG(
        start_date="2020-01-01-00-00-00",
        end_date="2020-01-01-00-30-00",
        dataset="docembed",
    )

    assert gsg._query_list() == [
        "20200101000000.gsg.docembed.json.gz",
        "20200101001500.gsg.docembed.json.gz",
        "20200101003000.gsg.docembed.json.gz",
    ]


def test_gsg_iatv_query_list_reads_daily_index_and_filters_station(monkeypatch):
    gsg = GSG(start_date="2009-07-02", dataset="iatvsentembed", station="CNN")

    def fake_read_csv(url, **kwargs):
        assert url == "http://data.gdeltproject.org/gdeltv3/gsg_iatvsentembed/20090702.txt"
        return pd.DataFrame({
            "url": [
                "http://data.gdeltproject.org/gdeltv3/gsg_iatvsentembed/CNN_20090702_190000_CNN_Newsroom.gsg.iatvsentembed.json.gz",
                "http://data.gdeltproject.org/gdeltv3/gsg_iatvsentembed/MSNBC_20090702_190000_Show.gsg.iatvsentembed.json.gz",
            ]
        })

    monkeypatch.setattr(others.pd, "read_csv", fake_read_csv)

    assert gsg._query_list() == [
        "http://data.gdeltproject.org/gdeltv3/gsg_iatvsentembed/CNN_20090702_190000_CNN_Newsroom.gsg.iatvsentembed.json.gz"
    ]


def test_gsg_query_downloads_and_concatenates_json_lines(monkeypatch):
    gsg = GSG(start_date="2020-01-01-00-00-00", dataset="docembed")
    monkeypatch.setattr(gsg, "_query_list", lambda: ["20200101000000.gsg.docembed.json.gz"])
    monkeypatch.setattr(gsg, "_download_file", lambda url: pd.DataFrame([{"url": "https://example.com", "embed": [0.1, 0.2]}]))

    class FakePool:
        def __init__(self, *args, **kwargs):
            pass

        def imap_unordered(self, func, iterable):
            return [func(item) for item in iterable]

        def close(self):
            pass

        def terminate(self):
            pass

        def join(self):
            pass

    monkeypatch.setattr(others.multiprocessing, "Pool", FakePool)

    result = gsg.query()

    assert result.to_dict("records") == [{"url": "https://example.com", "embed": [0.1, 0.2]}]


def test_gsg_rejects_unknown_dataset():
    try:
        GSG(dataset="unknown")
    except ValueError as exc:
        assert "dataset must" in str(exc)
    else:
        raise AssertionError("GSG accepted an unknown dataset")
