from newsfeed.tui.models import Article
import asyncio

from newsfeed.tui.widgets import DetailPane, NewsTable, article_cells


def test_sort_articles_by_time_toggles_newest_and_oldest():
    from newsfeed.tui.app import sort_articles_by_time

    older = Article(index=1, title="Old", url="a", published_at="20260623091500")
    newer = Article(index=2, title="New", url="b", published_at="20260623101500")

    assert [article.title for article in sort_articles_by_time([older, newer], "newest")] == ["New", "Old"]
    assert [article.title for article in sort_articles_by_time([older, newer], "oldest")] == ["Old", "New"]


def test_article_page_helpers_limit_rows_to_25():
    from newsfeed.tui.app import article_page, page_bounds, total_pages

    articles = [Article(index=i, title=f"A{i}", url=f"https://example.com/{i}") for i in range(30)]

    assert len(article_page(articles, 0)) == 25
    assert len(article_page(articles, 1)) == 5
    assert total_pages(articles) == 2
    assert page_bounds(len(articles), 1) == (26, 30)


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


def test_detail_pane_show_fulltext_opens_reader_view():
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
    assert "Full Text" in rendered
    assert "Full article body" in rendered
    assert "Text: indexed" in rendered
    assert "/tmp/newsfeed/fulltext/hash.txt" in rendered


def test_detail_pane_show_article_includes_watch_match_reason():
    pane = DetailPane()
    article = Article(
        index=1,
        title="Article",
        url="https://example.com/a",
        match_reason="keyword:oil (metadata)",
    )

    pane.show_article(article)

    assert "Watch: keyword:oil (metadata)" in str(pane.content)


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


def test_apply_articles_paginates_and_enriches_current_page_only():
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
            assert len(app.articles) == 25
            assert len(app._enrichment_queue) == 25
            assert app.articles[0].title == "Article 0"
            assert app.articles[-1].title == "Article 24"

            app.next_page()

            assert app.current_page == 1
            assert len(app.articles) == 5
            assert len(app._enrichment_queue) == 30
            assert app.articles[0].title == "Article 25"

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

        async with app.run_test():
            articles = service.alert_hits(mark_read=False)
            app.apply_articles(articles)
            app.open_row(1)

            assert "Full Text" in str(app.query_one(DetailPane).content)
            assert "cached alert fulltext" in str(app.query_one(DetailPane).content)

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
