from __future__ import annotations

import json
from typing import Any

from rich.text import Text
from textual.widgets import DataTable, Static

from newsfeed.tui.models import Article, TimelinePoint


class NewsTable(DataTable):
    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.add_event_columns()

    def load_articles(self, articles: list[Article]) -> None:
        self.clear()
        for row_number, article in enumerate(articles, start=1):
            article.index = row_number
            if not article.row_key:
                article.row_key = str(row_number)
            self.add_row(*article_cells(article), key=article.row_key)

    def update_article(self, article: Article) -> None:
        for column_key, value in zip(EVENT_COLUMN_KEYS, article_cells(article), strict=True):
            self.update_cell(article.row_key, column_key, value)

    def load_timeline(self, points: list[TimelinePoint]) -> None:
        self.clear(columns=True)
        self.add_columns("#", "Time", "Value")
        for i, point in enumerate(points, start=1):
            self.add_row(str(i), point.timestamp, str(point.value), key=str(i))

    def reset_article_columns(self) -> None:
        self.clear(columns=True)
        self.add_event_columns()

    def add_event_columns(self) -> None:
        for label, key in EVENT_COLUMNS:
            self.add_column(label, key=key)


class DetailPane(Static):
    def show_help(self) -> None:
        self.update(
            "Commands\n"
            "TOP SINCE:6h COUNTRY:US LIMIT:25 ORDER:newest\n"
            'NEWS "oil prices" SINCE:12h COUNTRY:US,UK LIMIT:50 ORDER:oldest\n'
            'TL "supply chain" SINCE:7d COUNTRY:US MODE:tone\n'
            'GEO "supply chain" SINCE:7d COUNTRY:US\n'
            "WATCH ADD country US\n"
            "WATCH ADD keyword oil\n"
            "WATCH LIST\n"
            "WATCH NEWS SINCE:6h LIMIT:25\n"
            'SEARCH "oil price shock" LIMIT:25\n'
            'ALERT ADD oil "oil price shock" COUNTRY:US SOURCE:example.com TONE:1.5 FREQ:5m\n'
            "ALERT LIST\n"
            "ALERT CHECK\n"
            "ALERT HITS\n"
            "READ 1\n"
            "FULLTEXT\n"
            "BRIEF CURRENT PATH:/tmp/brief.md\n"
            "BRIEF ALERTS PATH:/tmp/alerts.md\n"
            "BRIEF WATCH PATH:/tmp/watch.md\n"
            'SAVE QUERY oil NEWS "oil" SINCE:6h\n'
            "SAVE LIST\n"
            "LOAD QUERY oil\n"
            "WORKSPACE USE macro\n"
            "WORKSPACE LIST\n"
            "WORKSPACE SAVE macro-layout\n"
            "WORKSPACE LOAD macro-layout\n"
            "CACHE STATS\n"
            "CACHE HISTORY\n"
            "CACHE CLEAN RESULTS\n"
            "CONFIG EXPORT PATH:/tmp/newsfeed-config.json\n"
            "CONFIG IMPORT PATH:/tmp/newsfeed-config.json\n"
            "NEXT\n"
            "PREV\n"
            "PAGE 2\n"
            "EXPORT FORMAT:csv PATH:/tmp/newsfeed.csv\n"
            "QUIT"
        )

    def show_article(self, article: Article) -> None:
        metadata = json.dumps(article.raw, indent=2, ensure_ascii=False)[:4000]
        error = f"\n\nError\n{article.error}" if article.error else ""
        self.update(
            f"{article.event_label or article.title}\n"
            f"Code: {article.event_code}\n"
            f"Actors: {article.actors}\n"
            f"Mentions: {article.mentions}\n"
            f"Text: {article.enrichment_status}\n"
            f"Watch: {article.match_reason or '-'}\n"
            f"Time: {article.display_time or article.published_at}\n"
            f"Country: {article.country}\n"
            f"Tone: {article.tone}\n"
            f"URL: {article.url}\n"
            f"Query: {article.query}\n\n"
            f"Metadata\n{metadata}"
            f"{error}"
        )

    def show_fulltext(self, article: Article) -> None:
        path = article.raw.get("fulltext_path", "")
        header = (
            f"{article.event_label or article.title}\n"
            f"URL: {article.url}\n"
            f"Text: {article.enrichment_status}\n"
            f"Cached: {path}\n\n"
            "Full Text\n"
        )
        body = article.fulltext or article.error or "No full text available."
        self.update(header + body)

    def show_timeline(self, points: list[TimelinePoint]) -> None:
        lines = ["Timeline"]
        for point in points[:40]:
            lines.append(f"{point.timestamp}: {point.value}")
        self.update("\n".join(lines))

    def show_watchlist(self, labels: list[str]) -> None:
        if not labels:
            self.update("Watchlist is empty.")
            return
        self.update("Watchlist\n" + "\n".join(labels))

    def show_alerts(self, labels: list[str]) -> None:
        if not labels:
            self.update("Alerts are empty.")
            return
        self.update("Alerts\n" + "\n".join(labels))


EVENT_COLUMNS = [
    ("Time", "time"),
    ("Country", "country"),
    ("Event", "event"),
    ("Actors", "actors"),
    ("Mentions", "mentions"),
    ("Text", "text"),
    ("Tone", "tone"),
    ("SourceURL", "source_url"),
]
EVENT_COLUMN_KEYS = [key for _, key in EVENT_COLUMNS]


def article_cells(article: Article) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any]:
    values = [
        article.display_time or article.published_at,
        article.country,
        article.event_label or article.event_code or article.title,
        article.actors,
        article.mentions,
        article.enrichment_status,
        article.tone,
        article.url,
    ]
    if not article.watch_hits:
        return tuple(values)

    values[2] = f"WATCH {values[2]}"
    return tuple(Text(str(value), style="bold yellow" if index == 2 else "yellow") for index, value in enumerate(values))
