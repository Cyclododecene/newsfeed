from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from textual import on, work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, Input, Static

from newsfeed.tui.commands import ParsedCommand, parse_command, parse_row_number
from newsfeed.tui.export import export_articles, export_brief, export_timeline
from newsfeed.tui.models import Article, TimelinePoint
from newsfeed.tui.services import NewsService, parse_scan_frequency, parse_tone_threshold
from newsfeed.tui.storage import TuiStorage
from newsfeed.tui.widgets import DetailPane, NewsTable


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
        ("f5", "run_top", "Top"),
        ("pagedown", "next_page", "Next"),
        ("pageup", "prev_page", "Prev"),
    ]

    def __init__(self, service: NewsService | None = None) -> None:
        super().__init__()
        self.service = service or NewsService(storage=TuiStorage())
        self.all_articles: list[Article] = []
        self.articles: list[Article] = []
        self.timeline_points: list[TimelinePoint] = []
        self.selected_article: Article | None = None
        self.current_order = "newest"
        self.current_page = 0
        self.page_size = PAGE_SIZE
        self.auto_enrich_pages = False
        self.enrichment_limit = PAGE_SIZE
        self.enrichment_concurrency = ENRICHMENT_CONCURRENCY
        self.alert_scan_interval = 30
        self.alert_count = 0
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
        row_key = str(event.row_key.value)
        self.open_article_key(row_key)

    @on(NewsTable.RowSelected)
    def on_row_selected(self, event: NewsTable.RowSelected) -> None:
        row_key = str(event.row_key.value)
        if self.open_article_key(row_key):
            self.run_fulltext()

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
            self.run_top(command)
        elif name == "NEWS":
            self.run_news(command)
        elif name == "GEO":
            self.run_geo(command)
        elif name == "TL":
            self.run_timeline(command)
        elif name == "READ":
            self.open_row(parse_row_number(command))
        elif name == "FULLTEXT":
            self.run_fulltext()
        elif name == "EXPORT":
            self.export_current(command)
        elif name == "BRIEF":
            self.run_brief(command)
        elif name == "WATCH":
            self.dispatch_watch(command)
        elif name == "SEARCH":
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
        self.call_from_thread(self.apply_timeline, points)

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
            elif action == "NEWS":
                self.run_watch_news(command)
            else:
                raise ValueError("WATCH requires ADD, LIST, DELETE, or NEWS.")
        except ValueError as exc:
            self.set_error(str(exc))

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
                target = command.args[1] if len(command.args) > 1 else "results"
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

    def dispatch_alert(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("ALERT requires ADD, LIST, DELETE, CHECK, or HITS.")
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
                self.run_alert_check()
            elif action == "HITS":
                self.run_alert_hits()
            else:
                raise ValueError("ALERT requires ADD, LIST, DELETE, CHECK, or HITS.")
        except ValueError as exc:
            self.set_error(str(exc))

    def apply_articles(self, articles: list[Article], auto_enrich: bool = False) -> None:
        self.timeline_points = []
        self.auto_enrich_pages = auto_enrich
        self.current_page = 0
        self.all_articles = sort_articles_by_time(articles, self.current_order)
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

    def apply_timeline(self, points: list[TimelinePoint]) -> None:
        self.all_articles = []
        self.articles = []
        self.timeline_points = points
        self.selected_article = None
        self.auto_enrich_pages = False
        table = self.query_one(NewsTable)
        table.load_timeline(points)
        self.query_one(DetailPane).show_timeline(points)
        self.set_status(f"{len(points)} timeline points loaded.")

    def apply_fulltext(self, article: Article) -> None:
        self.selected_article = article
        self.refresh_article_table(article)
        if article.fulltext:
            self.query_one(DetailPane).show_fulltext(article)
            path = article.raw.get("fulltext_path", "")
            self.set_status(f"Full text cached at {path}." if path else "Full text loaded.")
        else:
            self.query_one(DetailPane).show_article(article)
            self.set_status(article.error)

    def apply_background_fulltext(self, article: Article) -> None:
        selected_key = self.selected_article.row_key if self.selected_article else ""
        self.refresh_article_table(article)
        if selected_key and article.row_key == selected_key:
            if article.fulltext:
                self.query_one(DetailPane).show_fulltext(article)
            else:
                self.query_one(DetailPane).show_article(article)
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
        page_count = total_pages(self.all_articles, self.page_size)
        self.current_page = clamp_page(self.current_page, page_count)
        self.articles = article_page(self.all_articles, self.current_page, self.page_size)
        for index, article in enumerate(self.articles, start=1):
            article.index = index
        table = self.query_one(NewsTable)
        table.reset_article_columns()
        table.load_articles(self.articles)
        self.selected_article = self.articles[0] if self.articles else None
        if self.selected_article is not None:
            self.query_one(DetailPane).show_article(self.selected_article)
        start, end = page_bounds(len(self.all_articles), self.current_page, self.page_size)
        status = f"Rows {start}-{end} of {len(self.all_articles)}. Page {self.current_page + 1}/{page_count}."
        if auto_enrich:
            queued = self.enqueue_background_enrichment(self.articles)
            if queued:
                status += f" Enriching {queued} articles."
        self.set_status(status)

    def action_next_page(self) -> None:
        self.next_page()

    def action_prev_page(self) -> None:
        self.prev_page()

    def next_page(self) -> None:
        if self.current_page + 1 >= total_pages(self.all_articles, self.page_size):
            self.set_status("Already on last page.")
            return
        self.current_page += 1
        self.render_current_page(auto_enrich=self.auto_enrich_pages)

    def prev_page(self) -> None:
        if self.current_page <= 0:
            self.set_status("Already on first page.")
            return
        self.current_page -= 1
        self.render_current_page(auto_enrich=self.auto_enrich_pages)

    def open_page(self, command: ParsedCommand) -> None:
        if not command.args:
            self.set_error("PAGE requires a page number.")
            return
        try:
            page_number = int(command.args[0])
        except ValueError:
            self.set_error("PAGE number must be an integer.")
            return
        page_count = total_pages(self.all_articles, self.page_size)
        if page_number < 1 or page_number > page_count:
            self.set_error(f"Page {page_number} is not available.")
            return
        self.current_page = page_number - 1
        self.render_current_page(auto_enrich=self.auto_enrich_pages)

    def open_row(self, row_number: int) -> None:
        if row_number < 1 or row_number > len(self.articles):
            self.set_error(f"Row {row_number} is not available.")
            return
        self.selected_article = self.articles[row_number - 1]
        self.show_selected_article()
        self.set_status(f"Opened row {row_number}.")

    def open_article_key(self, row_key: str) -> bool:
        article = find_article_by_key(self.articles, row_key)
        if article is None:
            self.set_error("Selected row is not available.")
            return False
        self.selected_article = article
        self.show_selected_article()
        self.set_status(f"Opened row {article.index}.")
        return True

    def show_selected_article(self) -> None:
        if self.selected_article is None:
            return
        article = self.selected_article
        if article.query == "ALERT" and article.raw.get("fulltext_path"):
            cached = self.service.fetch_fulltext(article)
            if cached.fulltext:
                self.query_one(DetailPane).show_fulltext(cached)
                return
        self.query_one(DetailPane).show_article(article)

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
    start = max(page, 0) * page_size
    return articles[start : start + page_size]


def total_pages(articles: list[Article], page_size: int = PAGE_SIZE) -> int:
    if not articles:
        return 1
    return ((len(articles) - 1) // page_size) + 1


def clamp_page(page: int, page_count: int) -> int:
    return min(max(page, 0), max(page_count - 1, 0))


def page_bounds(total: int, page: int, page_size: int = PAGE_SIZE) -> tuple[int, int]:
    if total == 0:
        return 0, 0
    start = (page * page_size) + 1
    end = min(start + page_size - 1, total)
    return start, end


def datetime_key() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> None:
    NewsFeedTerminal().run()


if __name__ == "__main__":
    main()
