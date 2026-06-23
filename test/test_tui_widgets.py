from newsfeed.tui.models import Article, SourceStat
import asyncio

from newsfeed.tui.widgets import ArticleReaderScreen, DetailPane, NewsTable, article_cells, reader_content


def test_sort_articles_by_time_toggles_newest_and_oldest():
    from newsfeed.tui.app import sort_articles_by_time

    older = Article(index=1, title="Old", url="a", published_at="20260623091500")
    newer = Article(index=2, title="New", url="b", published_at="20260623101500")

    assert [article.title for article in sort_articles_by_time([older, newer], "newest")] == ["New", "Old"]
    assert [article.title for article in sort_articles_by_time([older, newer], "oldest")] == ["Old", "New"]


def test_article_page_helpers_keep_continuous_stream():
    from newsfeed.tui.app import article_page, page_bounds, total_pages

    articles = [Article(index=i, title=f"A{i}", url=f"https://example.com/{i}") for i in range(30)]

    assert len(article_page(articles, 0)) == 30
    assert len(article_page(articles, 1)) == 30
    assert total_pages(articles) == 1
    assert page_bounds(len(articles), 1) == (1, 30)


def test_merge_article_update_replaces_current_row_for_table_refresh():
    from newsfeed.tui.app import merge_article_update

    original = Article(index=1, title="A", url="https://example.com/a", row_key="row-a", enrichment_status="none")
    updated = Article(index=99, title="A", url="https://example.com/a", row_key="row-a", enrichment_status="indexed")
    articles = [original]

    assert merge_article_update(articles, updated) is True
    assert articles[0].index == 1
    assert articles[0].enrichment_status == "indexed"


def test_merge_article_update_prefers_row_index_when_urls_are_duplicated():
    from newsfeed.tui.app import merge_article_update

    earlier = Article(index=12, title="Earlier", url="https://example.com/a", row_key="earlier", enrichment_status="none")
    selected = Article(index=13, title="Selected", url="https://example.com/a", row_key="selected", enrichment_status="none")
    updated = Article(index=13, title="Selected", url="https://example.com/a", row_key="selected", enrichment_status="indexed")
    articles = [earlier, selected]

    assert merge_article_update(articles, updated) is True
    assert articles[0].title == "Earlier"
    assert articles[0].enrichment_status == "none"
    assert articles[1].title == "Selected"
    assert articles[1].enrichment_status == "indexed"


def test_detail_pane_show_fulltext_keeps_preview_only():
    pane = DetailPane()
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        event_label="Make a visit",
        enrichment_status="indexed",
        fulltext="Full article body",
        raw={"fulltext_path": "/tmp/newsfeed/fulltext/hash.txt"},
    )

    pane.show_fulltext(article)

    rendered = str(pane.content)
    assert "Preview" in rendered
    assert "Text: indexed" in rendered
    assert "Full article body" not in rendered


def test_detail_pane_show_article_includes_watch_match_reason():
    pane = DetailPane()
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        match_reason="keyword:oil (metadata)",
    )

    pane.show_article(article)

    assert "Watch/Alert: keyword:oil (metadata)" in str(pane.content)


def test_detail_pane_show_article_includes_library_state_and_notes():
    pane = DetailPane()
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        raw={"reading_state": "read", "notes": "important"},
    )

    pane.show_article(article)

    rendered = str(pane.content)
    assert "Preview" in rendered
    assert "SourceURL: https://example.com/a" in rendered
    assert "Metadata" not in rendered


def test_article_reader_sections_render_meta_fulltext_and_raw():
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        event_label="Make a visit",
        enrichment_status="indexed",
        fulltext="Full article body",
        raw={"fulltext_path": "/tmp/newsfeed/fulltext/hash.txt", "raw_key": "raw value"},
    )

    assert "Meta" in reader_content(article, "Meta")
    assert "Full article body" in reader_content(article, "Fulltext")
    assert "raw value" in reader_content(article, "Raw")


def test_article_reader_without_fulltext_prompts_download():
    article = Article(index=1, title="Article", url="https://example.com/a", enrichment_status="none")

    assert "Press f to download fulltext" in reader_content(article, "Fulltext")


def test_detail_pane_help_lists_keyboard_shortcuts():
    pane = DetailPane()

    pane.show_help()

    rendered = str(pane.content)
    assert "Shortcuts" in rendered
    assert "j/k move rows" in rendered
    assert "s save article" in rendered


def test_article_cells_marks_watch_hits_with_styled_event_cell():
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        event_label="Make a visit",
        watch_hits=["keyword:oil (metadata)"],
    )

    cells = article_cells(article)

    assert cells[2].plain == "WATCH Make a visit"
    assert "yellow" in cells[2].style


def test_news_table_load_articles_uses_unique_display_row_keys():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            table = app.query_one(NewsTable)
            articles = [
                Article(index=13, title="A", url="https://example.com/a", row_key="a"),
                Article(index=13, title="B", url="https://example.com/a", row_key="b"),
            ]
            table.load_articles(articles)
            assert articles[0].index == 1
            assert articles[1].index == 2

    asyncio.run(run_test())


def test_news_table_load_source_stats():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            table = app.query_one(NewsTable)
            table.load_source_stats([SourceStat(index=1, source="example.com", count=2, avg_tone=-1.25)])

            assert table.row_count == 1

    asyncio.run(run_test())


def test_keyboard_actions_focus_move_and_save(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        brief_path = tmp_path / "shortcut-brief.md"
        app.default_brief_path = lambda: brief_path

        async with app.run_test() as pilot:
            app.apply_articles(
                [
                    Article(index=1, title="First", url="https://example.com/first", row_key="first"),
                    Article(index=2, title="Second", url="https://example.com/second", row_key="second"),
                ]
            )

            app.action_focus_results()
            await pilot.pause()
            assert app.query_one(NewsTable).has_focus

            app.action_focus_command()
            await pilot.pause()
            assert app.query_one("#command").has_focus

            app.action_select_next()
            assert app.selected_article is not None
            assert app.selected_article.row_key == "second"

            app.action_select_previous()
            assert app.selected_article is not None
            assert app.selected_article.row_key == "first"

            app.action_save_article()
            assert storage.list_library()[0]["source_url"] == "https://example.com/first"

            app.action_create_current_brief()
            assert "# NewsFeed Current Brief" in brief_path.read_text(encoding="utf-8")

    asyncio.run(run_test())


def test_command_input_enter_still_submits_command():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test() as pilot:
            command_input = app.query_one("#command")
            command_input.value = "HELP"
            command_input.focus()

            await pilot.press("enter")
            await pilot.pause()

            assert "Commands" in str(app.query_one(DetailPane).content)
            assert app.reader_screen is None

    asyncio.run(run_test())


def test_default_brief_path_and_current_brief_export(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        async with app.run_test():
            app.apply_articles([Article(index=1, title="Brief", url="https://example.com/brief")])
            default_path = app.default_brief_path()
            output = app.create_brief(parse_command(f"BRIEF CURRENT PATH:{default_path}"))

            assert output == default_path
            assert default_path.parent.name == "briefs"
            assert "# NewsFeed Current Brief" in default_path.read_text(encoding="utf-8")

    asyncio.run(run_test())


def test_open_reader_loads_cached_fulltext_in_modal(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        article = Article(index=1, title="Cached", url="https://example.com/cached", row_key="cached")
        storage.save_fulltext(article, "cached article body")
        app = NewsFeedTerminal(service=NewsService(storage=storage))

        async with app.run_test() as pilot:
            app.apply_articles([article])
            app.open_reader_for_selected()
            await pilot.pause()

            assert app.reader_screen is not None
            assert app.reader_screen.section == "Fulltext"
            assert "cached article body" in str(app.reader_screen.query_one("#reader-content").content)

    asyncio.run(run_test())


def test_reader_escape_allows_opening_next_article(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        first = Article(index=1, title="First", url="https://example.com/first", row_key="first")
        second = Article(index=2, title="Second", url="https://example.com/second", row_key="second")
        storage.save_fulltext(first, "first body")
        storage.save_fulltext(second, "second body")
        app = NewsFeedTerminal(service=NewsService(storage=storage))

        async with app.run_test() as pilot:
            app.apply_articles([first, second])
            app.query_one(NewsTable).focus()
            await pilot.press("enter")
            await pilot.pause()

            assert app.reader_screen is not None
            assert app.reader_screen.article.row_key == "first"

            await pilot.press("escape")
            await pilot.pause()
            assert app.reader_screen is None
            assert app.query_one(NewsTable).has_focus

            await pilot.press("j")
            await pilot.press("enter")
            await pilot.pause()

            assert app.reader_screen is not None
            assert app.reader_screen.article.row_key == "second"
            assert "second body" in str(app.reader_screen.query_one("#reader-content").content)

    asyncio.run(run_test())


def test_reader_updates_when_fulltext_finishes():
    article = Article(index=1, title="Article", url="https://example.com/a", row_key="a")
    screen = ArticleReaderScreen(article)
    updated = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        row_key="a",
        enrichment_status="indexed",
        fulltext="updated body",
    )

    async def run_test():
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            def compose(self) -> ComposeResult:
                yield Static("host")

        app = TestApp()
        async with app.run_test() as pilot:
            app.push_screen(screen)
            await pilot.pause()
            screen.update_article(updated)

            assert screen.section == "Fulltext"
            assert "updated body" in str(screen.query_one("#reader-content").content)

    asyncio.run(run_test())


def test_reader_size_actions_update_dialog_class():
    article = Article(index=1, title="Article", url="https://example.com/a")
    screen = ArticleReaderScreen(article)

    async def run_test():
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            def compose(self) -> ComposeResult:
                yield Static("host")

        app = TestApp()
        async with app.run_test() as pilot:
            app.push_screen(screen)
            await pilot.pause()
            dialog = screen.query_one("#reader-dialog")

            screen.action_increase_size()
            assert "reader-full" in dialog.classes

            screen.action_decrease_size()
            assert "reader-wide" in dialog.classes

            screen.action_decrease_size()
            assert "reader-compact" in dialog.classes

            screen.action_toggle_maximize()
            assert "reader-full" in dialog.classes

    asyncio.run(run_test())


def test_reader_fulltext_scrolls_with_j_k_and_page_keys():
    long_text = "\n".join(f"line {index}" for index in range(200))
    article = Article(
        index=1,
        title="Long",
        url="https://example.com/long",
        enrichment_status="indexed",
        fulltext=long_text,
    )
    screen = ArticleReaderScreen(article)

    async def run_test():
        from textual.app import App, ComposeResult
        from textual.widgets import Static

        class TestApp(App):
            def compose(self) -> ComposeResult:
                yield Static("host")

        app = TestApp()
        async with app.run_test(size=(80, 24)) as pilot:
            app.push_screen(screen)
            await pilot.pause()
            body = screen.query_one("#reader-body")

            assert body.max_scroll_y > 0
            assert body.scroll_y == 0

            await pilot.press("j")
            await pilot.pause()
            assert body.scroll_y > 0

            after_j = body.scroll_y
            await pilot.press("k")
            await pilot.pause()
            assert body.scroll_y < after_j

            await pilot.press("pagedown")
            await pilot.pause()
            assert body.scroll_y > 0

    asyncio.run(run_test())


def test_refresh_article_table_preserves_selected_non_first_row():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            first = Article(index=1, title="First", url="https://example.com/a", row_key="first")
            second = Article(index=2, title="Second", url="https://example.com/a", row_key="second")
            app.apply_articles([first, second])
            assert app.open_article_key("second") is True

            updated = Article(
                index=2,
                title="Second",
                url="https://example.com/a",
                row_key="second",
                enrichment_status="indexed",
            )
            app.refresh_article_table(updated)

            assert app.selected_article is not None
            assert app.selected_article.row_key == "second"
            assert app.articles[0].enrichment_status == "none"
            assert app.articles[1].enrichment_status == "indexed"

    asyncio.run(run_test())


def test_refresh_article_table_does_not_steal_selection_for_background_update():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            first = Article(index=1, title="First", url="https://example.com/a", row_key="first")
            second = Article(index=2, title="Second", url="https://example.com/b", row_key="second")
            app.apply_articles([first, second])
            assert app.open_article_key("second") is True

            updated = Article(
                index=1,
                title="First",
                url="https://example.com/a",
                row_key="first",
                enrichment_status="indexed",
            )
            app.refresh_article_table(updated)

            assert app.selected_article is not None
            assert app.selected_article.row_key == "second"
            assert app.articles[0].enrichment_status == "indexed"

    asyncio.run(run_test())


def test_enrichment_candidates_limits_none_and_failed_articles():
    from newsfeed.tui.app import enrichment_candidates

    articles = [
        Article(index=1, title="A", url="https://example.com/a", enrichment_status="indexed"),
        Article(index=2, title="B", url="https://example.com/b", enrichment_status="none"),
        Article(index=3, title="C", url="", enrichment_status="none"),
        Article(index=4, title="D", url="https://example.com/d", enrichment_status="failed"),
    ]

    candidates = enrichment_candidates(articles, limit=1)

    assert [article.title for article in candidates] == ["B"]


def test_apply_articles_streams_all_rows_and_enriches_first_batch_only():
    from newsfeed.tui.app import NewsFeedTerminal

    async def run_test():
        app = NewsFeedTerminal()
        app.enrichment_concurrency = 0
        async with app.run_test():
            articles = [
                Article(
                    index=i,
                    title=f"Article {i}",
                    url=f"https://example.com/{i}",
                    row_key=f"row-{i}",
                    published_at="20260623120000",
                )
                for i in range(30)
            ]

            app.apply_articles(articles, auto_enrich=True)

            assert len(app.all_articles) == 30
            assert len(app.articles) == 30
            assert len(app._enrichment_queue) == 25
            assert app.articles[0].title == "Article 0"
            assert app.articles[-1].title == "Article 29"

            app.next_page()

            assert app.current_page == 0
            assert len(app.articles) == 30
            assert len(app._enrichment_queue) == 25
            assert app.selected_article is not None
            assert app.selected_article.title in {"Article 24", "Article 25"}

    asyncio.run(run_test())


def test_src_current_uses_current_articles():
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            app.apply_articles(
                [
                    Article(index=1, title="A", url="https://example.com/a", tone="-1"),
                    Article(index=2, title="B", url="https://example.com/b", tone="3"),
                ]
            )
            app.dispatch_src(parse_command("SRC CURRENT"))

            assert "example.com: 2" in str(app.query_one(DetailPane).content)

    asyncio.run(run_test())


def test_alert_hit_read_opens_cached_fulltext(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        source = Article(index=1, title="Oil", url="https://example.com/oil")
        storage.save_fulltext(source, "cached alert fulltext")
        storage.add_alert("oil", "oil")
        storage.check_alerts()
        service = NewsService(storage=storage)
        app = NewsFeedTerminal(service=service)

        async with app.run_test() as pilot:
            articles = service.alert_hits(mark_read=False)
            app.apply_articles(articles)
            app.open_row(1, open_reader=True)
            await pilot.pause()

            assert "Preview" in str(app.query_one(DetailPane).content)
            assert "cached alert fulltext" not in str(app.query_one(DetailPane).content)
            assert app.reader_screen is not None
            assert "cached alert fulltext" in str(app.reader_screen.query_one("#reader-content").content)

    asyncio.run(run_test())


def test_create_brief_current_exports_current_page(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command

    async def run_test():
        app = NewsFeedTerminal()
        async with app.run_test():
            path = tmp_path / "brief.md"
            app.apply_articles([Article(index=1, title="Current", url="https://example.com/current")])

            output = app.create_brief(parse_command(f"BRIEF CURRENT PATH:{path}"))

            assert output == path
            content = path.read_text(encoding="utf-8")
            assert "# NewsFeed Current Brief" in content
            assert "https://example.com/current" in content

    asyncio.run(run_test())


def test_create_brief_alerts_uses_alert_hits(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        article = Article(index=1, title="Oil", url="https://example.com/oil")
        storage.save_fulltext(article, "oil shock body")
        storage.add_alert("oil", "oil")
        storage.check_alerts()
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        path = tmp_path / "alerts.md"

        async with app.run_test():
            output = app.create_brief(parse_command(f"BRIEF ALERTS PATH:{path}"))

            assert output == path
            content = path.read_text(encoding="utf-8")
            assert "# NewsFeed Alert Brief" in content
            assert "https://example.com/oil" in content
            assert "oil shock body" in content

    asyncio.run(run_test())


def test_library_commands_save_list_mark_note_and_cite(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        citation_path = tmp_path / "citation.md"

        async with app.run_test():
            article = Article(index=1, title="Library", url="https://example.com/library")
            app.apply_articles([article])
            app.dispatch_library(parse_command("LIB SAVE"))
            app.dispatch_library(parse_command("LIB MARK read"))
            app.dispatch_library(parse_command('LIB NOTE "important"'))
            app.dispatch_library(parse_command("LIB LIST"))

            assert app.articles[0].raw["reading_state"] == "read"
            assert app.articles[0].raw["notes"] == "important"

            app.dispatch_cite(parse_command(f"CITE PATH:{citation_path}"))
            assert "https://example.com/library" in citation_path.read_text(encoding="utf-8")

            app.dispatch_library(parse_command("LIB DELETE 1"))
            assert storage.list_library() == []

    asyncio.run(run_test())


def test_archive_query_result_writes_parquet_and_history(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.commands import parse_command
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        async with app.run_test():
            path = app.archive_query_result(
                parse_command('NEWS "oil"'),
                [Article(index=1, title="Oil", url="https://example.com/oil")],
            )

            history = storage.list_query_history()
            assert path.endswith(".parquet")
            assert history[0]["result_count"] == 1
            assert history[0]["result_path"] == path

    asyncio.run(run_test())


def test_workspace_layout_round_trip_in_app(tmp_path):
    from newsfeed.tui.app import NewsFeedTerminal
    from newsfeed.tui.services import NewsService
    from newsfeed.tui.storage import TuiStorage

    async def run_test():
        storage = TuiStorage(tmp_path / "tui.db")
        app = NewsFeedTerminal(service=NewsService(storage=storage))
        async with app.run_test():
            app.page_size = 10
            app.service.set_workspace("macro")
            layout_id = app.service.save_workspace_layout("macro-layout", app.current_layout())
            app.page_size = 25
            app.service.set_workspace("default")
            app.apply_layout(app.service.load_workspace_layout("macro-layout"))

            assert layout_id == 1
            assert app.page_size == 10
            assert app.service.workspace_name == "macro"

    asyncio.run(run_test())


def test_find_article_by_key_returns_selected_article_not_first_row():
    from newsfeed.tui.app import find_article_by_key

    first = Article(index=1, title="First", url="https://example.com/a", row_key="first")
    second = Article(index=2, title="Second", url="https://example.com/a", row_key="second")

    assert find_article_by_key([first, second], "second") is second
