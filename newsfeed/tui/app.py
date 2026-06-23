from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from textual import on, work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, Input, Static

from newsfeed.tui.commands import ParsedCommand, parse_command, parse_row_number
from newsfeed.tui.export import citation_markdown, export_articles, export_brief, export_citation, export_timeline
from newsfeed.tui.models import Article, SourceStat, TimelinePoint
from newsfeed.tui.services import NewsService, parse_scan_frequency, parse_tone_threshold
from newsfeed.tui.storage import TuiStorage
from newsfeed.tui.widgets import ArticleReaderScreen, DetailPane, NewsTable


PAGE_SIZE = 25
ENRICHMENT_CONCURRENCY = 4


class NewsFeedTerminal(App):
    CSS = """
    Screen {
        layout: vertical;
    }

    #command {
        dock: top;
        height: 3;
    }

    #workspace {
        height: 1fr;
    }

    #left-pane {
        width: 65%;
    }

    #detail {
        width: 35%;
        border-left: solid $accent;
        padding: 1;
        overflow-y: auto;
    }

    #status {
        height: 1;
        color: $text-muted;
    }
    """

    BINDINGS = [
        ("ctrl+c", "quit", "Quit"),
        ("q", "quit", "Quit"),
        ("/", "focus_command", "Command"),
        ("escape", "focus_results", "Results"),
        ("question_mark", "show_help", "Help"),
        ("f5", "run_top", "Top"),
        ("r", "refresh_results", "Refresh"),
        ("enter", "open_reader", "Reader"),
        ("j", "select_next", "Down"),
        ("k", "select_previous", "Up"),
        ("h", "prev_page", "Prev"),
        ("l", "next_page", "Next"),
        ("s", "save_article", "Save"),
        ("a", "show_alert_hits", "Alerts"),
        ("w", "run_watchlist_news", "Watch"),
        ("b", "create_current_brief", "Brief"),
        ("pagedown", "next_page", "Next"),
        ("pageup", "prev_page", "Prev"),
    ]

    def __init__(self, service: NewsService | None = None) -> None:
        super().__init__()
        self.service = service or NewsService(storage=TuiStorage())
        self.all_articles: list[Article] = []
        self.articles: list[Article] = []
        self.timeline_points: list[TimelinePoint] = []
        self.current_view = "articles"
        self.selected_article: Article | None = None
        self.current_order = "newest"
        self.current_page = 0
        self.page_size = PAGE_SIZE
        self.auto_enrich_pages = False
        self.enrichment_limit = PAGE_SIZE
        self.enrichment_concurrency = ENRICHMENT_CONCURRENCY
        self.alert_scan_interval = 30
        self.alert_count = 0
        self.last_refresh_command: ParsedCommand | None = None
        self.reader_screen: ArticleReaderScreen | None = None
        self._alert_scan_active = False
        self._enrichment_queue: list[str] = []
        self._enrichment_inflight: set[str] = set()
        self._enrichment_active = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Input(placeholder='NEWS "oil prices" SINCE:6h COUNTRY:US LIMIT:50', id="command")
        with Horizontal(id="workspace"):
            with Vertical(id="left-pane"):
                yield NewsTable(id="news-table")
            yield DetailPane(id="detail")
        yield Static("Ready. Type HELP for commands.", id="status")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#command", Input).focus()
        self.query_one(DetailPane).show_help()
        self.refresh_alert_count()
        self.set_interval(self.alert_scan_interval, self.run_alert_scan)

    def action_run_top(self) -> None:
        self.dispatch_command(parse_command("TOP"))

    def action_focus_command(self) -> None:
        command_input = self.query_one("#command", Input)
        command_input.focus()
        command_input.cursor_position = len(command_input.value)
        self.set_status("Command focused.")

    def action_focus_results(self) -> None:
        if self.cancel_active_work():
            return
        self.query_one(NewsTable).focus()
        self.set_status("Results focused.")

    def action_show_help(self) -> None:
        self.query_one(DetailPane).show_help()
        self.set_status("Help ready.")

    def action_refresh_results(self) -> None:
        command = self.last_refresh_command or parse_command("TOP")
        self.dispatch_command(parse_command(command.raw))

    def action_select_next(self) -> None:
        self.move_result_cursor("down")

    def action_select_previous(self) -> None:
        self.move_result_cursor("up")

    def action_open_reader(self) -> None:
        if self.current_view in {"timeline", "geo"}:
            row_key = self.current_table_row_key()
            if row_key:
                self.open_timeline_point_key(row_key)
                return
        self.open_reader_for_selected()

    def action_save_article(self) -> None:
        self.dispatch_command(parse_command("LIB SAVE"))

    def action_show_alert_hits(self) -> None:
        self.dispatch_command(parse_command("ALERT HITS"))

    def action_run_watchlist_news(self) -> None:
        self.dispatch_command(parse_command("WATCH NEWS"))

    def action_create_current_brief(self) -> None:
        path = self.default_brief_path()
        try:
            output_path = self.create_brief(parse_command(f"BRIEF CURRENT PATH:{path}"))
        except Exception as exc:
            self.set_error(str(exc))
            return
        self.set_status(f"Brief exported {output_path}.")

    @on(Input.Submitted, "#command")
    def on_command_submitted(self, event: Input.Submitted) -> None:
        event.input.value = ""
        try:
            command = parse_command(event.value)
        except ValueError as exc:
            self.set_status(str(exc))
            return
        self.dispatch_command(command)

    @on(NewsTable.RowHighlighted)
    def on_row_highlighted(self, event: NewsTable.RowHighlighted) -> None:
        if self.current_view != "articles":
            return
        row_key = str(event.row_key.value)
        self.open_article_key(row_key)

    @on(NewsTable.RowSelected)
    def on_row_selected(self, event: NewsTable.RowSelected) -> None:
        row_key = str(event.row_key.value)
        if self.current_view in {"timeline", "geo"}:
            self.open_timeline_point_key(row_key)
            return
        if self.open_article_key(row_key):
            self.open_reader_for_selected()

    @on(NewsTable.HeaderSelected)
    def on_header_selected(self, event: NewsTable.HeaderSelected) -> None:
        if str(event.column_key.value) != "time" or not self.articles:
            return
        self.current_order = "oldest" if self.current_order == "newest" else "newest"
        self.sort_current_articles_by_time(self.current_order)
        self.set_status(f"Sorted Time {self.current_order}.")

    def dispatch_command(self, command: ParsedCommand) -> None:
        name = command.name
        if name == "HELP":
            self.query_one(DetailPane).show_help()
            self.set_status("Ready.")
        elif name == "QUIT":
            self.exit()
        elif name == "TOP":
            self.remember_refresh_command(command)
            self.run_top(command)
        elif name == "NEWS":
            self.remember_refresh_command(command)
            self.run_news(command)
        elif name == "SRC":
            self.dispatch_src(command)
        elif name == "GEO":
            self.remember_refresh_command(command)
            self.run_geo(command)
        elif name == "TL":
            self.remember_refresh_command(command)
            self.run_timeline(command)
        elif name == "READ":
            self.open_row(parse_row_number(command), open_reader=True)
        elif name == "FULLTEXT":
            self.open_reader_for_selected()
            self.request_reader_fulltext()
        elif name == "EXPORT":
            self.export_current(command)
        elif name == "BRIEF":
            self.run_brief(command)
        elif name == "CITE":
            self.dispatch_cite(command)
        elif name == "WATCH":
            self.dispatch_watch(command)
        elif name == "SEARCH":
            self.remember_refresh_command(command)
            self.run_search(command)
        elif name == "ALERT":
            self.dispatch_alert(command)
        elif name == "NEXT":
            self.next_page()
        elif name == "PREV":
            self.prev_page()
        elif name == "PAGE":
            self.open_page(command)
        elif name == "SAVE":
            self.dispatch_save(command)
        elif name == "LOAD":
            self.dispatch_load(command)
        elif name == "WORKSPACE":
            self.dispatch_workspace(command)
        elif name == "CACHE":
            self.dispatch_cache(command)
        elif name == "CONFIG":
            self.dispatch_config(command)
        elif name == "LIB":
            self.dispatch_library(command)
        elif name == "CANCEL":
            self.cancel_active_work()

    @work(thread=True)
    def run_top(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Loading TOP...")
        try:
            articles = self.service.top(command)
            self.archive_query_result(command, articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles, True)

    @work(thread=True)
    def run_news(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, f"Loading NEWS {command.query}...")
        try:
            articles = self.service.news(command)
            self.archive_query_result(command, articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles, True)

    @work(thread=True)
    def run_geo(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Loading GEO...")
        try:
            points = self.service.geo(command)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_timeline, points, "geo")

    @work(thread=True)
    def run_source_stats(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Loading source stats...")
        try:
            stats = self.service.source_stats(command)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_source_stats, stats)

    @work(thread=True)
    def run_timeline(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Loading timeline...")
        try:
            points = self.service.timeline(command)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_timeline, points)

    @work(thread=True)
    def run_fulltext(self) -> None:
        target_article = self.selected_article
        if target_article is None:
            self.call_from_thread(self.set_error, "Select an article first with READ <row>.")
            return
        if target_article.row_key in self._enrichment_inflight or target_article.row_key in self._enrichment_queue:
            self.call_from_thread(self.set_status, "Full text is already downloading for this row.")
            return
        self.call_from_thread(self.set_loading, "Loading full text...")
        target_article.enrichment_status = "pending"
        self.call_from_thread(self.refresh_article_table, target_article)
        self.call_from_thread(self.show_article_preview, target_article)
        self.call_from_thread(self.update_reader, target_article)
        article = self.service.fetch_fulltext(target_article)
        self.call_from_thread(self.apply_fulltext, article)

    @work(thread=True)
    def run_watch_news(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Loading WATCH NEWS...")
        try:
            articles = self.service.watch_news(command)
            self.archive_query_result(command, articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles, True)

    @work(thread=True)
    def run_search(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, f"Searching cached fulltext for {command.query}...")
        try:
            articles = self.service.search_fulltext(command)
            self.archive_query_result(command, articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles)

    @work(thread=True)
    def run_alert_check(self) -> None:
        self.call_from_thread(self.set_loading, "Checking alerts...")
        try:
            articles = self.service.check_alerts()
            self.archive_query_result(command=parse_command("ALERT CHECK"), articles=articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_alert_results, articles, "Alert check")

    @work(thread=True)
    def run_alert_hits(self) -> None:
        self.call_from_thread(self.set_loading, "Loading alert hits...")
        try:
            articles = self.service.alert_hits(mark_read=True)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_alert_results, articles, "Alert hits")

    @work(thread=True)
    def run_alert_scan(self) -> None:
        if self._alert_scan_active:
            return
        self._alert_scan_active = True
        try:
            before = self.service.unread_alert_count()
            self.service.check_alerts()
            after = self.service.unread_alert_count()
        except Exception as exc:
            self.call_from_thread(self.complete_alert_scan, 0, str(exc))
            return
        self.call_from_thread(self.complete_alert_scan, max(after - before, 0), "")

    @work(thread=True)
    def run_brief(self, command: ParsedCommand) -> None:
        self.call_from_thread(self.set_loading, "Generating brief...")
        try:
            output_path = self.create_brief(command)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.set_status, f"Brief exported {output_path}.")

    def dispatch_watch(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("WATCH requires ADD, LIST, DELETE, or NEWS.")
            return

        action = command.args[0].upper()
        try:
            if action == "ADD":
                if len(command.args) < 3:
                    raise ValueError("WATCH ADD requires a kind and value.")
                item = self.service.add_watch_item(command.args[1], " ".join(command.args[2:]))
                self.query_one(DetailPane).show_watchlist(self.service.watchlist_labels())
                self.set_status(f"Added watch item {item.kind}:{item.value}.")
            elif action == "LIST":
                self.query_one(DetailPane).show_watchlist(self.service.watchlist_labels())
                self.set_status(f"{len(self.service.watchlist)} watch items.")
            elif action == "DELETE":
                if len(command.args) < 2:
                    raise ValueError("WATCH DELETE requires an item number.")
                item = self.service.delete_watch_item(int(command.args[1]))
                self.query_one(DetailPane).show_watchlist(self.service.watchlist_labels())
                self.set_status(f"Deleted watch item {item.kind}:{item.value}.")
            elif action in {"ENABLE", "DISABLE"}:
                if len(command.args) < 2:
                    raise ValueError(f"WATCH {action} requires an item number.")
                item = self.service.set_watch_item_enabled(int(command.args[1]), action == "ENABLE")
                self.query_one(DetailPane).show_watchlist(self.service.watchlist_labels())
                self.set_status(f"Watch item {item.index} {'enabled' if item.enabled else 'disabled'}.")
            elif action == "NEWS":
                self.remember_refresh_command(command)
                self.run_watch_news(command)
            else:
                raise ValueError("WATCH requires ADD, LIST, DELETE, ENABLE, DISABLE, or NEWS.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_src(self, command: ParsedCommand) -> None:
        if command.args and command.args[0].upper() == "CURRENT":
            self.apply_source_stats(self.service.source_stats_from_articles(self.all_articles or self.articles))
            return
        self.remember_refresh_command(command)
        self.run_source_stats(command)

    def dispatch_save(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("SAVE requires QUERY or WORKSPACE.")
            return
        action = command.args[0].upper()
        try:
            if action == "QUERY":
                if len(command.args) < 3:
                    raise ValueError("SAVE QUERY requires a name and command.")
                query_id = self.service.save_query(command.args[1], " ".join(command.args[2:]))
                self.set_status(f"Saved query {query_id}.")
            elif action == "LIST":
                self.query_one(DetailPane).update("Saved Queries\n" + "\n".join(self.service.list_saved_queries()))
                self.set_status("Saved queries listed.")
            elif action == "WORKSPACE":
                name = command.args[1] if len(command.args) > 1 else self.service.workspace_name
                layout_id = self.service.save_workspace_layout(name, self.current_layout())
                self.set_status(f"Saved workspace layout {layout_id}.")
            else:
                raise ValueError("SAVE requires QUERY, LIST, or WORKSPACE.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_load(self, command: ParsedCommand) -> None:
        if len(command.args) < 2:
            self.set_error("LOAD requires QUERY <name> or WORKSPACE <name>.")
            return
        action = command.args[0].upper()
        try:
            if action == "QUERY":
                raw = self.service.get_saved_query(command.args[1])
                self.dispatch_command(parse_command(raw))
                self.set_status(f"Loaded query {command.args[1]}.")
            elif action == "WORKSPACE":
                self.apply_layout(self.service.load_workspace_layout(command.args[1]))
                self.set_status(f"Loaded workspace layout {command.args[1]}.")
            else:
                raise ValueError("LOAD requires QUERY or WORKSPACE.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_workspace(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("WORKSPACE requires USE, LIST, SAVE, LOAD, or LAYOUTS.")
            return
        action = command.args[0].upper()
        try:
            if action == "USE":
                if len(command.args) < 2:
                    raise ValueError("WORKSPACE USE requires a name.")
                self.service.set_workspace(command.args[1])
                self.set_status(f"Workspace {self.service.workspace_name} active.")
            elif action == "LIST":
                self.query_one(DetailPane).update("Workspaces\n" + "\n".join(self.service.list_workspaces()))
                self.set_status("Workspaces listed.")
            elif action == "SAVE":
                name = command.args[1] if len(command.args) > 1 else self.service.workspace_name
                self.service.save_workspace_layout(name, self.current_layout())
                self.set_status(f"Workspace layout {name} saved.")
            elif action == "LOAD":
                if len(command.args) < 2:
                    raise ValueError("WORKSPACE LOAD requires a name.")
                self.apply_layout(self.service.load_workspace_layout(command.args[1]))
                self.set_status(f"Workspace layout {command.args[1]} loaded.")
            elif action == "LAYOUTS":
                self.query_one(DetailPane).update("Layouts\n" + "\n".join(self.service.list_workspace_layouts()))
                self.set_status("Workspace layouts listed.")
            else:
                raise ValueError("WORKSPACE requires USE, LIST, SAVE, LOAD, or LAYOUTS.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_cache(self, command: ParsedCommand) -> None:
        action = command.args[0].upper() if command.args else "STATS"
        try:
            if action == "STATS":
                stats = self.service.cache_stats()
                self.query_one(DetailPane).update("Cache\n" + json.dumps(stats, indent=2))
                self.set_status("Cache stats loaded.")
            elif action == "HISTORY":
                labels = self.service.query_history_labels()
                self.query_one(DetailPane).update("Query History\n" + "\n".join(labels))
                self.set_status("Query history loaded.")
            elif action == "CLEAN":
                target = " ".join(command.args[1:]) if len(command.args) > 1 else "results"
                result = self.service.cleanup_cache(target)
                self.set_status(f"Cache cleaned {result['removed_files']} files.")
            else:
                raise ValueError("CACHE requires STATS, HISTORY, or CLEAN.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_config(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("CONFIG requires EXPORT or IMPORT.")
            return
        action = command.args[0].upper()
        path = command.options.get("PATH")
        if not path:
            self.set_error("CONFIG requires PATH:<path>.")
            return
        try:
            if action == "EXPORT":
                output = self.service.export_config(path)
                self.set_status(f"Config exported {output}.")
            elif action == "IMPORT":
                result = self.service.import_config(path)
                self.set_status(f"Config imported {result}.")
            else:
                raise ValueError("CONFIG requires EXPORT or IMPORT.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_library(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("LIB requires SAVE, LIST, DELETE, MARK, or NOTE.")
            return
        action = command.args[0].upper()
        try:
            if action == "SAVE":
                if self.selected_article is None:
                    raise ValueError("Select an article first.")
                article_id = self.service.save_to_library(self.selected_article)
                self.set_status(f"Saved article {article_id}.")
            elif action == "LIST":
                self.apply_articles(self.service.list_library())
                self.set_status("Library loaded.")
            elif action == "DELETE":
                if len(command.args) < 2:
                    raise ValueError("LIB DELETE requires an item number or article id.")
                deleted = self.service.delete_library_item(command.args[1])
                self.set_status("Library item deleted." if deleted else "Library item not found.")
            elif action == "MARK":
                if self.selected_article is None:
                    raise ValueError("Select an article first.")
                if len(command.args) < 2:
                    raise ValueError("LIB MARK requires read or unread.")
                state = command.args[1].lower()
                self.service.mark_library_article(self.selected_article, state)
                self.query_one(DetailPane).show_article(self.selected_article)
                self.set_status(f"Marked article {state}.")
            elif action == "NOTE":
                if self.selected_article is None:
                    raise ValueError("Select an article first.")
                note = " ".join(command.args[1:]).strip()
                if not note:
                    raise ValueError("LIB NOTE requires note text.")
                self.service.note_library_article(self.selected_article, note)
                self.query_one(DetailPane).show_article(self.selected_article)
                self.set_status("Article note saved.")
            else:
                raise ValueError("LIB requires SAVE, LIST, DELETE, MARK, or NOTE.")
        except ValueError as exc:
            self.set_error(str(exc))

    def dispatch_cite(self, command: ParsedCommand) -> None:
        if self.selected_article is None:
            self.set_error("Select an article first.")
            return
        path = command.options.get("PATH")
        try:
            if path:
                output = export_citation(self.selected_article, path)
                self.set_status(f"Citation exported {output}.")
            else:
                self.query_one(DetailPane).update(citation_markdown(self.selected_article))
                self.set_status("Citation ready.")
        except Exception as exc:
            self.set_error(str(exc))

    def dispatch_alert(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("ALERT requires ADD, LIST, DELETE, CHECK, HITS, PAUSE, RESUME, MUTE, or STATE.")
            return

        action = command.args[0].upper()
        try:
            if action == "ADD":
                if len(command.args) < 3:
                    raise ValueError("ALERT ADD requires a name and query.")
                alert_id = self.service.add_alert(
                    command.args[1],
                    " ".join(command.args[2:]),
                    country=command.options.get("COUNTRY", ""),
                    source=command.options.get("SOURCE", ""),
                    tone_threshold=parse_tone_threshold(
                        command.options.get("TONE") or command.options.get("TONE_THRESHOLD")
                    ),
                    scan_frequency=parse_scan_frequency(command.options.get("FREQ")),
                )
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status(f"Added alert {alert_id}.")
            elif action == "LIST":
                labels = self.service.list_alerts()
                self.query_one(DetailPane).show_alerts(labels)
                self.set_status(f"{len(labels)} alerts.")
            elif action == "DELETE":
                if len(command.args) < 2:
                    raise ValueError("ALERT DELETE requires an alert name.")
                deleted = self.service.delete_alert(command.args[1])
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status("Alert deleted." if deleted else "Alert not found.")
            elif action == "CHECK":
                self.remember_refresh_command(command)
                self.run_alert_check()
            elif action == "HITS":
                self.remember_refresh_command(command)
                self.run_alert_hits()
            elif action == "PAUSE":
                if len(command.args) < 2:
                    raise ValueError("ALERT PAUSE requires an alert name.")
                updated = self.service.pause_alert(command.args[1])
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status("Alert paused." if updated else "Alert not found.")
            elif action == "RESUME":
                if len(command.args) < 2:
                    raise ValueError("ALERT RESUME requires an alert name.")
                updated = self.service.resume_alert(command.args[1])
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status("Alert resumed." if updated else "Alert not found.")
            elif action == "MUTE":
                if len(command.args) < 3:
                    raise ValueError("ALERT MUTE requires an alert name and window.")
                updated = self.service.mute_alert(command.args[1], command.args[2])
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status("Alert muted." if updated else "Alert not found.")
            elif action == "STATE":
                if len(command.args) < 3:
                    raise ValueError("ALERT STATE requires an alert name and state.")
                updated = self.service.set_alert_state(command.args[1], command.args[2].lower())
                self.query_one(DetailPane).show_alerts(self.service.list_alerts())
                self.set_status("Alert state updated." if updated else "Alert not found.")
            else:
                raise ValueError("ALERT requires ADD, LIST, DELETE, CHECK, HITS, PAUSE, RESUME, MUTE, or STATE.")
        except ValueError as exc:
            self.set_error(str(exc))

    def apply_articles(self, articles: list[Article], auto_enrich: bool = False) -> None:
        self.timeline_points = []
        self.current_view = "articles"
        self.auto_enrich_pages = auto_enrich
        self.current_page = 0
        self.all_articles = list(articles)
        table = self.query_one(NewsTable)
        table.reset_article_columns()
        if not self.all_articles:
            self.articles = []
            self.selected_article = None
            table.load_articles([])
            self.query_one(DetailPane).update(
                "No rows found.\n"
                "For EVENT queries, try a wider SINCE window. For SEARCH/ALERT, run FULLTEXT on articles first."
            )
            self.set_status("No rows found.")
            return
        self.render_current_page(auto_enrich=auto_enrich)

    def apply_alert_results(self, articles: list[Article], label: str) -> None:
        self.apply_articles(articles)
        self.refresh_alert_count()
        if articles:
            self.set_status(f"{label}: {len(articles)} rows.")
        else:
            self.set_status(f"{label}: no hits.")

    def sort_current_articles_by_time(self, order: str) -> None:
        if self.all_articles:
            self.all_articles = sort_articles_by_time(self.all_articles, order)
        else:
            self.all_articles = sort_articles_by_time(self.articles, order)
        self.current_page = 0
        self.render_current_page(auto_enrich=self.auto_enrich_pages)

    def apply_timeline(self, points: list[TimelinePoint], view: str = "timeline") -> None:
        self.all_articles = []
        self.articles = []
        self.timeline_points = points
        self.current_view = view
        self.selected_article = None
        self.auto_enrich_pages = False
        table = self.query_one(NewsTable)
        table.load_timeline(points)
        self.query_one(DetailPane).show_timeline(points)
        self.set_status(f"{len(points)} timeline points loaded.")

    def apply_source_stats(self, stats: list[SourceStat]) -> None:
        self.all_articles = []
        self.articles = []
        self.timeline_points = []
        self.current_view = "source_stats"
        self.selected_article = None
        table = self.query_one(NewsTable)
        table.load_source_stats(stats)
        lines = ["Sources"]
        lines.extend(f"{stat.source}: {stat.count} avg tone {stat.avg_tone:.3f}" for stat in stats[:40])
        self.query_one(DetailPane).update("\n".join(lines) if stats else "No source stats.")
        self.set_status(f"{len(stats)} source rows loaded.")

    def apply_fulltext(self, article: Article) -> None:
        self.selected_article = article
        self.refresh_article_table(article)
        self.update_reader(article)
        self.query_one(DetailPane).show_article(article)
        if article.fulltext:
            path = article.raw.get("fulltext_path", "")
            self.set_status(f"Full text cached at {path}." if path else "Full text loaded.")
        else:
            self.set_status(article.error)

    def apply_background_fulltext(self, article: Article) -> None:
        selected_key = self.selected_article.row_key if self.selected_article else ""
        self.refresh_article_table(article)
        if selected_key and article.row_key == selected_key:
            self.query_one(DetailPane).show_article(article)
        self.update_reader(article)
        if article.enrichment_status == "failed":
            self.set_status(f"Background enrichment failed for row {article.index}: {article.error}")
        else:
            self.set_status(f"Background enrichment indexed row {article.index}.")

    def refresh_article_table(self, updated_article: Article) -> None:
        selected_key = self.selected_article.row_key if self.selected_article else ""
        merge_article_update(self.all_articles, updated_article)
        merge_article_update(self.articles, updated_article)
        table = self.query_one(NewsTable)
        if find_article_by_key(self.articles, updated_article.row_key) is not None:
            try:
                table.update_article(updated_article)
            except Exception:
                table.reset_article_columns()
                table.load_articles(self.articles)
        if selected_key:
            self.selected_article = find_article_by_key(self.all_articles, selected_key)
        else:
            self.selected_article = find_article_by_key(self.all_articles, updated_article.row_key) or updated_article

    def enqueue_background_enrichment(self, articles: list[Article]) -> int:
        queued = 0
        for article in enrichment_candidates(articles, self.enrichment_limit):
            if article.row_key in self._enrichment_inflight or article.row_key in self._enrichment_queue:
                continue
            article.enrichment_status = "pending"
            self.refresh_article_table(article)
            self._enrichment_queue.append(article.row_key)
            queued += 1
        self.start_next_enrichment()
        return queued

    def start_next_enrichment(self) -> None:
        while self._enrichment_active < self.enrichment_concurrency and self._enrichment_queue:
            row_key = self._enrichment_queue.pop(0)
            article = find_article_by_key(self.all_articles, row_key)
            if article is None or article.enrichment_status not in {"pending", "none", "failed"}:
                continue
            self._enrichment_active += 1
            self._enrichment_inflight.add(row_key)
            self.run_background_enrichment(article)

    @work(thread=True)
    def run_background_enrichment(self, article: Article) -> None:
        updated = self.service.fetch_fulltext(article)
        self.call_from_thread(self.complete_background_enrichment, updated)

    def complete_background_enrichment(self, article: Article) -> None:
        self._enrichment_active = max(self._enrichment_active - 1, 0)
        if article.row_key:
            self._enrichment_inflight.discard(article.row_key)
        self.apply_background_fulltext(article)
        self.start_next_enrichment()

    def render_current_page(self, auto_enrich: bool = False) -> None:
        self.current_page = 0
        self.articles = list(self.all_articles)
        for index, article in enumerate(self.articles, start=1):
            article.index = index
        table = self.query_one(NewsTable)
        table.reset_article_columns()
        table.load_articles(self.articles)
        self.selected_article = self.articles[0] if self.articles else None
        if self.selected_article is not None:
            self.query_one(DetailPane).show_article(self.selected_article)
        status = f"Rows 1-{len(self.articles)} of {len(self.all_articles)}. Continuous stream."
        if auto_enrich:
            queued = self.enqueue_background_enrichment(self.articles[: self.enrichment_limit])
            if queued:
                status += f" Enriching {queued} articles."
        self.set_status(status)

    def action_next_page(self) -> None:
        self.next_page()

    def action_prev_page(self) -> None:
        self.prev_page()

    def next_page(self) -> None:
        self.move_result_cursor("down", steps=self.page_size)

    def prev_page(self) -> None:
        self.move_result_cursor("up", steps=self.page_size)

    def open_page(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("PAGE requires a page number.")
            return
        try:
            page_number = int(command.args[0])
        except ValueError:
            self.set_error("PAGE number must be an integer.")
            return
        if page_number < 1:
            self.set_error("PAGE number must be 1 or greater.")
            return
        target_row = ((page_number - 1) * self.page_size) + 1
        if target_row > len(self.articles):
            self.set_error(f"Stream row {target_row} is not available.")
            return
        self.open_row(target_row)

    def move_result_cursor(self, direction: str, steps: int = 1) -> None:
        table = self.query_one(NewsTable)
        table.focus()
        if not self.articles:
            self.set_status("No rows to select.")
            return
        for _ in range(max(steps, 1)):
            if direction == "down":
                table.action_cursor_down()
            else:
                table.action_cursor_up()
        row_key = self.current_table_row_key()
        if row_key:
            self.open_article_key(row_key)

    def current_table_row_key(self) -> str:
        table = self.query_one(NewsTable)
        if table.row_count == 0:
            return ""
        try:
            cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        except Exception:
            return ""
        return str(cell_key.row_key.value)

    def open_row(self, row_number: int, open_reader: bool = False) -> None:
        if row_number < 1 or row_number > len(self.articles):
            self.set_error(f"Row {row_number} is not available.")
            return
        self.selected_article = self.articles[row_number - 1]
        self.show_selected_article()
        self.set_status(f"Opened row {row_number}.")
        if open_reader:
            self.open_reader_for_selected()

    def open_article_key(self, row_key: str) -> bool:
        if self.current_view in {"timeline", "geo"}:
            return self.open_timeline_point_key(row_key)
        article = find_article_by_key(self.articles, row_key)
        if article is None:
            self.set_error("Selected row is not available.")
            return False
        self.selected_article = article
        self.show_selected_article()
        self.set_status(f"Opened row {article.index}.")
        return True

    def open_timeline_point_key(self, row_key: str) -> bool:
        try:
            index = int(row_key)
        except ValueError:
            self.set_error("Selected aggregate row is not available.")
            return False
        if index < 1 or index > len(self.timeline_points):
            self.set_error(f"Aggregate row {index} is not available.")
            return False
        point = self.timeline_points[index - 1]
        if self.current_view == "geo":
            self.run_geo_drilldown(point)
        else:
            self.run_timeline_drilldown(point)
        return True

    @work(thread=True)
    def run_timeline_drilldown(self, point: TimelinePoint) -> None:
        self.call_from_thread(self.set_loading, f"Loading TL bucket {point.timestamp}...")
        try:
            articles = self.service.timeline_drilldown(point)
            self.archive_query_result(parse_command(f"TL {point.timestamp}"), articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles, True)

    @work(thread=True)
    def run_geo_drilldown(self, point: TimelinePoint) -> None:
        self.call_from_thread(self.set_loading, f"Loading GEO {point.timestamp}...")
        try:
            articles = self.service.geo_drilldown(point)
            self.archive_query_result(parse_command(f"GEO {point.timestamp}"), articles)
        except Exception as exc:
            self.call_from_thread(self.set_error, str(exc))
            return
        self.call_from_thread(self.apply_articles, articles, True)

    def cancel_active_work(self) -> bool:
        worker_count = len(getattr(self.workers, "_workers", []))
        if worker_count == 0 and not self._enrichment_queue and not self._enrichment_inflight and self._enrichment_active == 0:
            return False
        self._enrichment_queue.clear()
        self._enrichment_inflight.clear()
        self._enrichment_active = 0
        try:
            self.workers.cancel_all()
        except Exception:
            return False
        self.set_status("Cancelled active work.")
        return True

    def show_selected_article(self) -> None:
        if self.selected_article is None:
            return
        self.show_article_preview(self.selected_article)

    def show_article_preview(self, article: Article) -> None:
        self.query_one(DetailPane).show_article(article)

    def open_reader_for_selected(self) -> None:
        if self.selected_article is None:
            self.set_error("Select an article first.")
            return
        article = self.load_cached_fulltext(self.selected_article)
        if self.reader_screen is not None and self.reader_screen.is_mounted:
            self.reader_screen.update_article(article)
            self.set_status(f"Reader opened row {article.index}.")
            return
        screen = ArticleReaderScreen(article)
        self.reader_screen = screen
        self.push_screen(screen)
        self.set_status(f"Reader opened row {article.index}.")

    def close_reader(self, screen: ArticleReaderScreen) -> None:
        if self.reader_screen is screen:
            self.reader_screen = None
        screen.dismiss()
        self.query_one(NewsTable).focus()
        self.set_status("Reader closed.")

    def reader_closed(self, screen: ArticleReaderScreen) -> None:
        if self.reader_screen is screen:
            self.reader_screen = None
        try:
            self.query_one(NewsTable).focus()
        except Exception:
            pass

    def load_cached_fulltext(self, article: Article) -> Article:
        if not article.fulltext and self.service.storage is not None:
            self.service.storage.load_cached_fulltext(article)
            self.refresh_article_table(article)
        return article

    def request_reader_fulltext(self, row_key: str = "") -> None:
        if row_key:
            article = find_article_by_key(self.all_articles, row_key) or find_article_by_key(self.articles, row_key)
            if article is not None:
                self.selected_article = article
        self.run_fulltext()

    def update_reader(self, article: Article) -> None:
        screen = self.reader_screen
        if screen is None or not screen.is_mounted:
            return
        if screen.article.row_key and article.row_key and screen.article.row_key != article.row_key:
            return
        screen.update_article(article)

    def export_current(self, command: ParsedCommand) -> None:
        output_format = command.options.get("FORMAT", "csv")
        path = command.options.get("PATH")
        if not path:
            self.set_error("EXPORT requires PATH:<path>.")
            return
        try:
            if self.timeline_points:
                output_path = export_timeline(self.timeline_points, output_format, path)
            else:
                output_path = export_articles(self.articles, output_format, path)
        except Exception as exc:
            self.set_error(str(exc))
            return
        self.set_status(f"Exported {output_path}.")

    def create_brief(self, command: ParsedCommand):
        if not command.args:
            raise ValueError("BRIEF requires CURRENT, ALERTS, or WATCH.")
        path = command.options.get("PATH")
        if not path:
            raise ValueError("BRIEF requires PATH:<path>.")
        mode = command.args[0].upper()
        if mode == "CURRENT":
            articles = self.articles or self.all_articles
            title = "NewsFeed Current Brief"
        elif mode == "ALERTS":
            articles = self.service.alert_hits(mark_read=False)
            title = "NewsFeed Alert Brief"
        elif mode == "WATCH":
            articles = self.service.watch_news(command)
            title = "NewsFeed Watch Brief"
        else:
            raise ValueError("BRIEF requires CURRENT, ALERTS, or WATCH.")
        return export_brief(articles, path, title=title)

    def default_brief_path(self) -> Path:
        storage = self.service.storage
        base_path = storage.cache_dir if storage is not None else Path("/tmp") / "newsfeed"
        brief_dir = base_path / "briefs"
        brief_dir.mkdir(parents=True, exist_ok=True)
        filename = f"brief-{datetime_key().replace(':', '-')}.md"
        return brief_dir / filename

    def remember_refresh_command(self, command: ParsedCommand) -> None:
        if command.raw:
            self.last_refresh_command = command

    def current_layout(self) -> dict[str, object]:
        return {
            "workspace": self.service.workspace_name,
            "order": self.current_order,
            "page_size": self.page_size,
            "auto_enrich_pages": self.auto_enrich_pages,
            "enrichment_concurrency": self.enrichment_concurrency,
        }

    def apply_layout(self, layout: dict[str, object]) -> None:
        workspace = str(layout.get("workspace", self.service.workspace_name))
        self.service.set_workspace(workspace)
        self.current_order = str(layout.get("order", self.current_order))
        self.page_size = int(layout.get("page_size", self.page_size))
        self.auto_enrich_pages = bool(layout.get("auto_enrich_pages", self.auto_enrich_pages))
        self.enrichment_concurrency = int(layout.get("enrichment_concurrency", self.enrichment_concurrency))
        if self.all_articles:
            self.render_current_page(auto_enrich=False)

    def archive_query_result(self, command: ParsedCommand, articles: list[Article]) -> str:
        if self.service.storage is None:
            return ""
        basis = f"{command.raw}|{len(articles)}|{datetime_key()}"
        filename = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16] + ".parquet"
        path = self.service.storage.results_dir / filename
        try:
            export_articles(articles, "parquet", str(path))
            result_path = str(path)
            error = ""
        except Exception as exc:
            result_path = ""
            error = str(exc)
        self.service.storage.record_query_history(
            command.raw,
            result_count=len(articles),
            result_path=result_path,
            error=error,
        )
        return result_path

    def set_loading(self, message: str) -> None:
        self.set_status(message)

    def set_error(self, message: str) -> None:
        self.set_status(f"Error: {message}")

    def set_status(self, message: str) -> None:
        self.query_one("#status", Static).update(f"{message} | alerts: {self.alert_count}")

    def refresh_alert_count(self) -> int:
        self.alert_count = self.service.unread_alert_count()
        return self.alert_count

    def complete_alert_scan(self, new_hits: int, error: str = "") -> None:
        self._alert_scan_active = False
        self.refresh_alert_count()
        if error:
            self.set_status(f"Alert scan failed: {error}")
        elif new_hits:
            self.set_status(f"Alert scan found {new_hits} new hits.")


def sort_articles_by_time(articles: list[Article], order: str) -> list[Article]:
    reverse = order == "newest"
    return sorted(articles, key=lambda article: article.published_at, reverse=reverse)


def merge_article_update(articles: list[Article], updated_article: Article) -> bool:
    for position, article in enumerate(articles):
        if article.row_key and updated_article.row_key and article.row_key == updated_article.row_key:
            updated_article.index = article.index
            updated_article.row_key = article.row_key
            articles[position] = updated_article
            return True

    for position, article in enumerate(articles):
        same_url = bool(article.url and updated_article.url and article.url == updated_article.url)
        if same_url:
            updated_article.index = article.index
            updated_article.row_key = article.row_key
            articles[position] = updated_article
            return True
    return False


def find_article_by_key(articles: list[Article], row_key: str) -> Article | None:
    for article in articles:
        if article.row_key == row_key:
            return article
    return None


def enrichment_candidates(articles: list[Article], limit: int) -> list[Article]:
    candidates = [
        article
        for article in articles
        if article.url and article.enrichment_status in {"none", "failed"}
    ]
    return candidates[: max(limit, 0)]


def article_page(articles: list[Article], page: int, page_size: int = PAGE_SIZE) -> list[Article]:
    return list(articles)


def total_pages(articles: list[Article], page_size: int = PAGE_SIZE) -> int:
    return 1


def clamp_page(page: int, page_count: int) -> int:
    return 0


def page_bounds(total: int, page: int, page_size: int = PAGE_SIZE) -> tuple[int, int]:
    if total == 0:
        return 0, 0
    return 1, total


def datetime_key() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> None:
    NewsFeedTerminal().run()


if __name__ == "__main__":
    main()
