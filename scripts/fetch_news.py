#!/usr/bin/env python
"""
README-style NewsFeed example script.

This script uses the package APIs directly:
- newsfeed.news.apis.filters.Art_Filter
- newsfeed.news.apis.query.article_search / timeline_search / geo_search
- newsfeed.utils.fulltext.download
"""
import argparse
import json
from pathlib import Path

import pandas as pd

from newsfeed.news.apis.filters import Art_Filter
from newsfeed.news.apis.query import article_search, doc_query_search, geo_search, timeline_search
from newsfeed.utils.fulltext import download


def parse_keywords(value):
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_optional_list(value):
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def build_filter(args):
    return Art_Filter(
        keyword=parse_keywords(args.keywords),
        start_date=args.start,
        end_date=args.end,
        country=parse_optional_list(args.countries),
        domain=parse_optional_list(args.domains),
        theme=parse_optional_list(args.themes),
        num_records=args.max_records,
    )


def add_fulltext(df, limit, url_column):
    if df.empty or url_column not in df.columns:
        return df

    df = df.copy()
    df["FULLTEXT_SUCCESS"] = False
    df["FULLTEXT_TITLE"] = None
    df["FULLTEXT"] = None

    urls = df[url_column].dropna().drop_duplicates().head(limit).tolist()
    for url in urls:
        try:
            article = download(url)
            if article is None:
                continue
            mask = df[url_column] == url
            df.loc[mask, "FULLTEXT_SUCCESS"] = bool(getattr(article, "text", None))
            df.loc[mask, "FULLTEXT_TITLE"] = getattr(article, "title", None)
            df.loc[mask, "FULLTEXT"] = getattr(article, "text", None)
        except Exception as exc:
            mask = df[url_column] == url
            df.loc[mask, "FULLTEXT_TITLE"] = f"ERROR: {exc}"

    return df


def save_result(result, output, output_format):
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(result, pd.DataFrame):
        if output_format == "json":
            result.to_json(output, orient="records", force_ascii=False, indent=2)
        elif output_format == "parquet":
            result.to_parquet(output, index=False)
        else:
            result.to_csv(output, index=False)
    elif isinstance(result, (dict, list)):
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        output.write_text(str(result), encoding="utf-8")

    return output


def main():
    parser = argparse.ArgumentParser(
        description="Fetch news using the NewsFeed package APIs, similar to the README examples."
    )
    parser.add_argument(
        "--mode",
        choices=["articles", "timeline", "geo"],
        default="articles",
        help="NewsFeed API example to run",
    )
    parser.add_argument(
        "--keywords",
        default="Exchange Rate,World",
        help="Comma-separated keywords, for example: 'Exchange Rate,World'",
    )
    parser.add_argument(
        "--start",
        default="20211231000000",
        help="Start time: YYYYMMDDHHMMSS or YYYY-MM-DD-HH-MM-SS",
    )
    parser.add_argument(
        "--end",
        default="20211231010000",
        help="End time: YYYYMMDDHHMMSS or YYYY-MM-DD-HH-MM-SS",
    )
    parser.add_argument(
        "--countries",
        default="China,US",
        help="Comma-separated source countries, or empty string for none",
    )
    parser.add_argument("--domains", default="", help="Comma-separated source domains")
    parser.add_argument("--themes", default="", help="Comma-separated GKG themes")
    parser.add_argument("--max-records", type=int, default=100, help="GDELT maxrecords, up to 250")
    parser.add_argument("--time-range", type=int, default=60, help="Article query split size in minutes")
    parser.add_argument("--parallel", action="store_true", help="Use README article_search(), which uses multiprocessing")
    parser.add_argument("--timeline-mode", default="timelinevolraw", help="GDELT timeline mode")
    parser.add_argument("--geo-timespan", type=int, default=7, help="GEO API timespan in days")
    parser.add_argument("--sourcelang", default="english", help="GEO source language filter")
    parser.add_argument("--download-fulltext", action="store_true", help="Download article body text for article results")
    parser.add_argument("--fulltext-limit", type=int, default=3, help="Maximum article URLs to parse")
    parser.add_argument("--format", choices=["csv", "json", "parquet", "txt"], default="csv")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    query_filter = build_filter(args)

    print("[+] NewsFeed filter:")
    print(f"    {query_filter.query_string}")

    if args.mode == "articles":
        if args.parallel:
            result = article_search(
                query_filter=query_filter,
                max_recursion_depth=100,
                time_range=args.time_range,
            )
        else:
            result = doc_query_search(
                query_string=query_filter.query_string,
                max_recursion_depth=100,
                mode="artlist",
            )
        if isinstance(result, Exception):
            raise result
        if args.download_fulltext:
            result = add_fulltext(result, args.fulltext_limit, "url")
        default_output = "data/newsfeed_articles.csv"

    elif args.mode == "timeline":
        result = timeline_search(
            query_filter=query_filter,
            max_recursion_depth=100,
            query_mode=args.timeline_mode,
        )
        default_output = "data/newsfeed_timeline.csv"

    else:
        result = geo_search(
            query_filter=query_filter,
            sourcelang=args.sourcelang,
            timespan=args.geo_timespan,
            parse_json=(args.format == "json"),
        )
        default_output = "data/newsfeed_geo.json" if args.format == "json" else "data/newsfeed_geo.txt"

    output = args.output or default_output
    saved = save_result(result, output, args.format)
    count = len(result) if isinstance(result, pd.DataFrame) else "text"
    print(f"[+] Result size: {count}")
    print(f"[+] Saved: {saved.resolve()}")


if __name__ == "__main__":
    main()
