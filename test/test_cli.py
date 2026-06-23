import json
import sys

import pandas as pd

import newsfeed.__main__ as cli


def run_cli(monkeypatch, args):
    monkeypatch.setattr(sys, "argv", ["newsfeed"] + args)
    cli.main()


def run_database_cli(monkeypatch, output_path, output_format):
    class FakeEventV2:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def query(self):
            return pd.DataFrame([{"GLOBALEVENTID": 1, "SOURCEURL": "https://example.com"}])

    monkeypatch.setattr(cli, "EventV2", FakeEventV2)
    run_cli(
        monkeypatch,
        [
            "--db", "EVENT",
            "--version", "V2",
            "--start", "2021-01-01-00-00-00",
            "--end", "2021-01-01-00-15-00",
            "--format", output_format,
            "--output", str(output_path),
        ],
    )


def test_save_results_writes_csv_json_txt_and_parquet(tmp_path, monkeypatch):
    records = [{"url": "https://example.com/a", "text": "article body", "success": True}]
    df = pd.DataFrame(records)

    csv_path = tmp_path / "results.csv"
    assert cli.save_results(df, str(csv_path), "csv") == "csv"
    assert pd.read_csv(csv_path).to_dict("records") == records

    json_path = tmp_path / "results.json"
    assert cli.save_results(records, str(json_path), "json") == "json"
    assert json.loads(json_path.read_text(encoding="utf-8")) == records

    txt_path = tmp_path / "article.txt"
    assert cli.save_results(records, str(txt_path), "txt", allow_txt=True) == "txt"
    assert txt_path.read_text(encoding="utf-8") == "article body"

    parquet_path = tmp_path / "results.parquet"

    def fake_to_parquet(self, path, index=False):
        assert index is False
        self.to_csv(path, index=index)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    assert cli.save_results(df, str(parquet_path), "parquet") == "parquet"
    assert parquet_path.exists()


def test_save_results_txt_falls_back_to_csv_for_database_results(tmp_path):
    path = tmp_path / "results.txt"
    df = pd.DataFrame([{"GLOBALEVENTID": 1, "SOURCEURL": "https://example.com"}])

    actual_format = cli.save_results(df, str(path), "txt")

    assert actual_format == "csv"
    assert pd.read_csv(path).to_dict("records") == df.to_dict("records")


def test_database_cli_writes_csv_json_and_txt_fallback(tmp_path, monkeypatch):
    csv_path = tmp_path / "events.csv"
    run_database_cli(monkeypatch, csv_path, "csv")
    assert pd.read_csv(csv_path).to_dict("records")[0]["GLOBALEVENTID"] == 1

    json_path = tmp_path / "events.json"
    run_database_cli(monkeypatch, json_path, "json")
    assert json.loads(json_path.read_text(encoding="utf-8"))[0]["GLOBALEVENTID"] == 1

    txt_path = tmp_path / "events.txt"
    run_database_cli(monkeypatch, txt_path, "txt")
    assert pd.read_csv(txt_path).to_dict("records")[0]["GLOBALEVENTID"] == 1


def test_database_cli_accepts_parquet_and_passes_format(tmp_path, monkeypatch):
    output_path = tmp_path / "events.parquet"
    created = {}

    class FakeEventV2:
        def __init__(self, **kwargs):
            created.update(kwargs)

        def query(self):
            return pd.DataFrame([{"GLOBALEVENTID": 1, "SOURCEURL": "https://example.com"}])

    def fake_to_parquet(self, path, index=False):
        assert index is False
        self.to_csv(path, index=index)

    monkeypatch.setattr(cli, "EventV2", FakeEventV2)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    run_cli(
        monkeypatch,
        [
            "--db", "EVENT",
            "--version", "V2",
            "--start", "2021-01-01-00-00-00",
            "--end", "2021-01-01-00-15-00",
            "--format", "parquet",
            "--output", str(output_path),
        ],
    )

    assert created["output_format"] == "parquet"
    assert output_path.exists()


def test_v3_graph_cli_entry_points(tmp_path, monkeypatch):
    created = {}

    class FakeGEG:
        def __init__(self, **kwargs):
            created["GEG"] = kwargs

        def query(self):
            return pd.DataFrame([{"date": "20200101", "url": "https://example.com"}])

    class FakeVGEG:
        def __init__(self, **kwargs):
            created["VGEG"] = kwargs

        def query(self):
            return pd.DataFrame([{"date": "20200101", "station": "CNN"}])

    class FakeGDG:
        def __init__(self, **kwargs):
            created["GDG"] = kwargs

        def query(self):
            return pd.DataFrame([{"date": "20180827140000"}])

    class FakeGFG:
        def __init__(self, **kwargs):
            created["GFG"] = kwargs

        def query(self):
            return pd.DataFrame([{"DATE": "20180302020000"}])

    class FakeGAL:
        def __init__(self, **kwargs):
            created["GAL"] = kwargs

        def query(self):
            return pd.DataFrame([{"date": "20200101000100", "url": "https://example.com/a"}])

        def query_rss_feed(self):
            return pd.DataFrame([{"title": "Article A", "url": "https://example.com/a"}])

    class FakeGSG:
        def __init__(self, **kwargs):
            created["GSG"] = kwargs

        def query(self):
            return pd.DataFrame([{"url": "https://example.com/a", "embed": "[0.1, 0.2]"}])

    monkeypatch.setattr(cli, "GEG", FakeGEG)
    monkeypatch.setattr(cli, "VGEG", FakeVGEG)
    monkeypatch.setattr(cli, "GDG", FakeGDG)
    monkeypatch.setattr(cli, "GFG", FakeGFG)
    monkeypatch.setattr(cli, "GAL", FakeGAL)
    monkeypatch.setattr(cli, "GSG", FakeGSG)

    run_cli(
        monkeypatch,
        [
            "--db", "GEG",
            "--start", "2020-01-01",
            "--end", "2020-01-02",
            "--output", str(tmp_path / "geg.csv"),
        ],
    )
    assert created["GEG"]["start_date"] == "2020-01-01"
    assert created["GEG"]["end_date"] == "2020-01-02"

    run_cli(
        monkeypatch,
        [
            "--db", "VGEG",
            "--start", "2020-01-01",
            "--domain", "CNN",
            "--raw",
            "--output", str(tmp_path / "vgeg.csv"),
        ],
    )
    assert created["VGEG"]["query_date"] == "2020-01-01"
    assert created["VGEG"]["domain"] == "CNN"
    assert created["VGEG"]["raw"] is True

    run_cli(
        monkeypatch,
        [
            "--db", "GDG",
            "--start", "2018-08-27-14-00-00",
            "--output", str(tmp_path / "gdg.csv"),
        ],
    )
    assert created["GDG"]["query_date"] == "2018-08-27-14-00-00"

    run_cli(
        monkeypatch,
        [
            "--db", "GFG",
            "--start", "2018-03-02-02-00-00",
            "--output", str(tmp_path / "gfg.csv"),
        ],
    )
    assert created["GFG"]["query_date"] == "2018-03-02-02-00-00"

    run_cli(
        monkeypatch,
        [
            "--db", "GAL",
            "--start", "2020-01-01-00-01-00",
            "--output", str(tmp_path / "gal.csv"),
        ],
    )
    assert created["GAL"]["start_date"] == "2020-01-01-00-01-00"

    run_cli(
        monkeypatch,
        [
            "--db", "GAL",
            "--rss",
            "--format", "json",
            "--output", str(tmp_path / "gal_rss.json"),
        ],
    )
    assert (tmp_path / "gal_rss.json").exists()

    run_cli(
        monkeypatch,
        [
            "--db", "GSG",
            "--gsg-dataset", "iatvsentembed",
            "--start", "2009-07-02",
            "--domain", "CNN",
            "--output", str(tmp_path / "gsg.csv"),
        ],
    )
    assert created["GSG"]["dataset"] == "iatvsentembed"
    assert created["GSG"]["station"] == "CNN"
