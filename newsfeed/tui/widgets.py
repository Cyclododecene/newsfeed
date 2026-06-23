from __future__ import annotations

import json
from typing import Any

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, ScrollableContainer
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static

from newsfeed.tui.models import Article, SourceStat, TimelinePoint


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

    def load_source_stats(self, stats: list[SourceStat]) -> None:
        self.clear(columns=True)
        self.add_columns("#", "Source", "Count", "AvgTone")
        for stat in stats:
            self.add_row(str(stat.index), stat.source, str(stat.count), f"{stat.avg_tone:.3f}", key=str(stat.index))

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
            'NEWS "oil prices" SINCE:12h COUNTRY:US,UK SOURCE:example.com LIMIT:50 ORDER:relevance\n'
            'SRC "oil prices" SINCE:12h\n'
            "SRC CURRENT\n"
            'TL "supply chain" SINCE:7d COUNTRY:US MODE:tone\n'
            'GEO "supply chain" SINCE:7d COUNTRY:US\n'
            "WATCH ADD country US\n"
            "WATCH ADD keyword oil\n"
            "WATCH LIST\n"
            "WATCH ENABLE 1\n"
            "WATCH DISABLE 1\n"
            "WATCH NEWS SINCE:6h LIMIT:25\n"
            'SEARCH "oil price shock" LIMIT:25\n'
            'ALERT ADD oil "oil price shock" COUNTRY:US SOURCE:example.com TONE:1.5 FREQ:5m\n'
            "ALERT LIST\n"
            "ALERT CHECK\n"
            "ALERT HITS\n"
            "ALERT PAUSE oil\n"
            "ALERT RESUME oil\n"
            "ALERT MUTE oil 1h\n"
            "ALERT STATE oil active\n"
            "READ 1\n"
            "FULLTEXT\n"
            "LIB SAVE\n"
            "LIB LIST\n"
            "LIB MARK read\n"
            'LIB NOTE "follow up"\n'
            "LIB DELETE 1\n"
            "CITE\n"
            "CITE PATH:/tmp/citation.md\n"
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
            "CACHE CLEAN EXPIRED 7d\n"
            "CONFIG EXPORT PATH:/tmp/newsfeed-config.json\n"
            "CONFIG IMPORT PATH:/tmp/newsfeed-config.json\n"
            "NEXT\n"
            "PREV\n"
            "PAGE 2\n"
            "EXPORT FORMAT:csv PATH:/tmp/newsfeed.csv\n"
            "QUIT\n\n"
            "Shortcuts\n"
            "/ focus command, Esc cancel/focus results, ? help\n"
            "j/k move rows, h/l jump through stream, r refresh\n"
            "Enter open reader, Tab switch reader sections, f download fulltext\n"
            "s save article, a alert hits, w watch news, b brief"
        )

    def show_article(self, article: Article) -> None:
        error = f"\n\nError\n{article.error}" if article.error else ""
        self.update(
            "Preview\n"
            f"Event: {article.event_label or article.event_code or article.title}\n"
            f"Time: {article.display_time or article.published_at}\n"
            f"Country: {article.country}\n"
            f"Actors: {article.actors}\n"
            f"Mentions: {article.mentions}\n"
            f"Tone: {article.tone}\n"
            f"SourceURL: {article.url}\n"
            f"Text: {article.enrichment_status}\n"
            f"Watch/Alert: {article.match_reason or article.raw.get('alert_name', '-')}\n"
            f"{error}"
        )

    def show_fulltext(self, article: Article) -> None:
        self.show_article(article)

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


class ArticleReaderScreen(ModalScreen[None]):
    CSS = """
    ArticleReaderScreen {
        align: center middle;
    }

    #reader-dialog {
        background: $surface;
        border: thick $accent;
        padding: 1;
    }

    #reader-dialog.reader-compact {
        width: 64%;
        height: 64%;
    }

    #reader-dialog.reader-wide {
        width: 84%;
        height: 84%;
    }

    #reader-dialog.reader-full {
        width: 96%;
        height: 96%;
    }

    #reader-title {
        height: 2;
        text-style: bold;
    }

    #reader-tabs {
        height: 1;
        color: $text-muted;
    }

    #reader-body {
        height: 1fr;
        overflow-y: auto;
    }

    #reader-content {
        width: 100%;
    }

    #reader-footer {
        height: 1;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close", priority=True),
        Binding("tab", "next_section", "Section", priority=True),
        Binding("f", "fetch_fulltext", "Fulltext", priority=True),
        Binding("plus", "increase_size", "Larger", priority=True),
        Binding("=", "increase_size", "Larger", priority=True),
        Binding("minus", "decrease_size", "Smaller", priority=True),
        Binding("m", "toggle_maximize", "Maximize", priority=True),
        Binding("j", "scroll_down", "Down", priority=True),
        Binding("down", "scroll_down", "Down", priority=True),
        Binding("k", "scroll_up", "Up", priority=True),
        Binding("up", "scroll_up", "Up", priority=True),
        Binding("pagedown", "page_down", "Page Down", priority=True),
        Binding("pageup", "page_up", "Page Up", priority=True),
    ]

    SECTIONS = ["Meta", "Fulltext", "Raw"]
    SIZES = ["reader-compact", "reader-wide", "reader-full"]

    def __init__(self, article: Article) -> None:
        super().__init__()
        self.article = article
        self.section = "Fulltext" if article.fulltext else "Meta"
        self.size_index = 1

    def compose(self) -> ComposeResult:
        with Container(id="reader-dialog", classes=self.SIZES[self.size_index]):
            yield Static(id="reader-title")
            yield Static(id="reader-tabs")
            with ScrollableContainer(id="reader-body"):
                yield Static(id="reader-content")
            yield Static(id="reader-footer")

    def on_mount(self) -> None:
        self.refresh_view()

    def action_close(self) -> None:
        close_reader = getattr(self.app, "close_reader", None)
        if callable(close_reader):
            close_reader(self)
        else:
            self.dismiss()

    def on_unmount(self) -> None:
        reader_closed = getattr(self.app, "reader_closed", None)
        if callable(reader_closed):
            reader_closed(self)

    def action_next_section(self) -> None:
        current_index = self.SECTIONS.index(self.section)
        self.section = self.SECTIONS[(current_index + 1) % len(self.SECTIONS)]
        self.refresh_view()

    def action_fetch_fulltext(self) -> None:
        request = getattr(self.app, "request_reader_fulltext", None)
        if callable(request):
            request(self.article.row_key)

    def action_increase_size(self) -> None:
        self.size_index = min(self.size_index + 1, len(self.SIZES) - 1)
        self.apply_size()

    def action_decrease_size(self) -> None:
        self.size_index = max(self.size_index - 1, 0)
        self.apply_size()

    def action_toggle_maximize(self) -> None:
        self.size_index = len(self.SIZES) - 1 if self.size_index < len(self.SIZES) - 1 else 1
        self.apply_size()

    def action_scroll_down(self) -> None:
        self.query_one("#reader-body", ScrollableContainer).scroll_relative(y=3, animate=False, force=True, immediate=True)

    def action_scroll_up(self) -> None:
        self.query_one("#reader-body", ScrollableContainer).scroll_relative(y=-3, animate=False, force=True, immediate=True)

    def action_page_down(self) -> None:
        self.query_one("#reader-body", ScrollableContainer).scroll_page_down(animate=False, force=True)

    def action_page_up(self) -> None:
        self.query_one("#reader-body", ScrollableContainer).scroll_page_up(animate=False, force=True)

    def update_article(self, article: Article) -> None:
        self.article = article
        if article.fulltext and self.section == "Meta":
            self.section = "Fulltext"
        self.refresh_view()

    def refresh_view(self) -> None:
        try:
            title = self.query_one("#reader-title", Static)
            tabs = self.query_one("#reader-tabs", Static)
            content = self.query_one("#reader-content", Static)
            footer = self.query_one("#reader-footer", Static)
        except Exception:
            return
        title.update(reader_title(self.article))
        tabs.update(reader_tabs(self.section))
        content.update(reader_content(self.article, self.section))
        footer.update(
            "Esc close | Tab section | f download/reload fulltext | +/- resize | m maximize | j/k scroll"
        )

    def apply_size(self) -> None:
        if not self.is_mounted:
            return
        dialog = self.query_one("#reader-dialog", Container)
        for size_class in self.SIZES:
            dialog.remove_class(size_class)
        dialog.add_class(self.SIZES[self.size_index])


def reader_title(article: Article) -> str:
    label = article.event_label or article.event_code or article.title or "Article"
    return f"{label} | {article.display_time or article.published_at} | {article.country}"


def reader_tabs(active_section: str) -> str:
    return " | ".join(f"[{section}]" if section == active_section else section for section in ArticleReaderScreen.SECTIONS)


def reader_content(article: Article, section: str) -> str:
    if section == "Fulltext":
        return reader_fulltext(article)
    if section == "Raw":
        return json.dumps(article.raw, indent=2, ensure_ascii=False)[:20000]
    return reader_meta(article)


def reader_meta(article: Article) -> str:
    lines = [
        "Meta",
        f"Event: {article.event_label or article.event_code or article.title}",
        f"Code: {article.event_code}",
        f"Time: {article.display_time or article.published_at}",
        f"Country: {article.country}",
        f"Actors: {article.actors}",
        f"Mentions: {article.mentions}",
        f"Tone: {article.tone}",
        f"SourceURL: {article.url}",
        f"Text: {article.enrichment_status}",
        f"Retries: {article.raw.get('fulltext_retry_count', 0)}",
        f"Cached: {article.raw.get('fulltext_path', '')}",
        f"Partial: {article.raw.get('partial_text_path', '')}",
        f"Watch/Alert: {article.match_reason or article.raw.get('alert_name', '-')}",
        f"Error: {article.error}" if article.error else "",
        "",
        "Press f to download or reload fulltext." if article.enrichment_status in {"none", "failed"} else "",
    ]
    return "\n".join(line for line in lines if line != "")


def reader_fulltext(article: Article) -> str:
    path = article.raw.get("fulltext_path", "")
    if article.fulltext:
        return f"Fulltext\nCached: {path}\n\n{article.fulltext}"
    if article.enrichment_status == "pending":
        return "Fulltext download is pending."
    if article.error:
        partial_path = article.raw.get("partial_text_path", "")
        partial = ""
        if partial_path:
            partial = f"\nPartial text: {partial_path}"
        return f"Fulltext unavailable.\nError: {article.error}{partial}\n\nPress f to retry."
    return "No cached fulltext is available.\n\nPress f to download fulltext."


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
