from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from newsfeed.tui.models import Article, TimelinePoint


def articles_to_dataframe(articles: list[Article]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "index": article.index,
                "title": article.title,
                "source": article.source,
                "published_at": article.published_at,
                "display_time": article.display_time,
                "url": article.url,
                "country": article.country,
                "language": article.language,
                "tone": article.tone,
                "event_code": article.event_code,
                "event_label": article.event_label,
                "actors": article.actors,
                "mentions": article.mentions,
                "enrichment_status": article.enrichment_status,
                "query": article.query,
                "fulltext": article.fulltext,
                "error": article.error,
            }
            for article in articles
        ]
    )


def timeline_to_dataframe(points: list[TimelinePoint]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"timestamp": point.timestamp, "value": point.value, **point.raw} for point in points]
    )


def export_records(records: pd.DataFrame, output_format: str, path: str) -> Path:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = output_format.lower()

    if fmt == "csv":
        records.to_csv(output_path, index=False)
    elif fmt == "json":
        output_path.write_text(
            json.dumps(records.to_dict("records"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    elif fmt == "parquet":
        records.to_parquet(output_path, index=False)
    else:
        raise ValueError("EXPORT FORMAT must be csv, json, or parquet.")

    return output_path


def export_articles(articles: list[Article], output_format: str, path: str) -> Path:
    return export_records(articles_to_dataframe(articles), output_format, path)


def export_timeline(points: list[TimelinePoint], output_format: str, path: str) -> Path:
    return export_records(timeline_to_dataframe(points), output_format, path)


def export_brief(articles: list[Article], path: str, *, title: str = "NewsFeed Brief") -> Path:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    lines = [
        f"# {title}",
        "",
        f"Generated: {generated_at}",
        f"Items: {len(articles)}",
        "",
        "## Key Headlines",
        "",
    ]
    if not articles:
        lines.extend(["No articles.", ""])
    else:
        for article in articles[:10]:
            event = article.event_label or article.event_code or article.title or "Untitled"
            lines.append(f"- {article.display_time or article.published_at} | {article.country or '-'} | {event}")
        lines.append("")

    lines.extend(["## Watchlist Hits", ""])
    watch_hits = [article for article in articles if article.match_reason or article.watch_hits]
    if watch_hits:
        for article in watch_hits[:20]:
            event = article.event_label or article.event_code or article.title or "Untitled"
            lines.append(f"- {event}: {article.match_reason or ', '.join(article.watch_hits)}")
    else:
        lines.append("No watchlist or alert matches.")
    lines.append("")

    lines.extend(["## Notable Trends", "", "System Inferences:"])
    if articles:
        country_counts = top_counts(article.country or "unknown" for article in articles)
        tone_values = [safe_float(article.tone) for article in articles if article.tone not in {"", None}]
        lines.append(f"- Most frequent countries: {', '.join(country_counts) if country_counts else 'none'}")
        if tone_values:
            avg_tone = sum(tone_values) / len(tone_values)
            lines.append(f"- Average tone across listed articles: {avg_tone:.3f}")
        else:
            lines.append("- Average tone unavailable.")
    else:
        lines.append("- No trend inference because there are no articles.")
    lines.append("")

    lines.extend(["## Source Links", "", "Source Facts:"])
    if articles:
        for article in articles:
            event = article.event_label or article.event_code or article.title or "Untitled"
            lines.append(f"- {event}: {article.url or '-'}")
    else:
        lines.append("- No source links.")
    lines.append("")

    lines.extend(["## Article Details", "", "Source Facts:"])
    for article in articles:
        lines.extend(article_brief_lines(article))
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def citation_markdown(article: Article, excerpt_chars: int = 500) -> str:
    event = article.event_label or article.event_code or article.title or "Untitled"
    path = str(article.raw.get("fulltext_path", ""))
    lines = [
        f"## {event}",
        "",
        f"- Time: {article.display_time or article.published_at}",
        f"- SourceURL: {article.url}",
        f"- Fulltext: {path or '-'}",
    ]
    excerpt = cached_excerpt(article, excerpt_chars)
    if excerpt:
        lines.extend(["", "Excerpt:", "", excerpt])
    lines.append("")
    return "\n".join(lines)


def export_citation(article: Article, path: str) -> Path:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(citation_markdown(article), encoding="utf-8")
    return output_path


def article_brief_lines(article: Article, excerpt_chars: int = 800) -> list[str]:
    event = article.event_label or article.event_code or article.title or "Untitled"
    path = str(article.raw.get("fulltext_path", ""))
    lines = [
        f"## {article.index}. {event}",
        "",
        f"- Time: {article.display_time or article.published_at}",
        f"- Country: {article.country}",
        f"- Event: {event}",
        f"- Actors: {article.actors}",
        f"- Tone: {article.tone}",
        f"- SourceURL: {article.url}",
        f"- Match: {article.match_reason or '-'}",
        f"- Fulltext: {path or '-'}",
    ]
    excerpt = cached_excerpt(article, excerpt_chars)
    if excerpt:
        lines.extend(["", "Excerpt:", "", excerpt])
    lines.append("")
    return lines


def cached_excerpt(article: Article, max_chars: int = 800) -> str:
    text = article.fulltext or ""
    if not text:
        path = str(article.raw.get("fulltext_path", ""))
        if path:
            try:
                text = Path(path).expanduser().read_text(encoding="utf-8", errors="ignore")
            except OSError:
                text = ""
    text = normalize_excerpt(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def normalize_excerpt(text: str) -> str:
    return " ".join(text.split())


def top_counts(values) -> list[str]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    rows = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [f"{value} ({count})" for value, count in rows[:5]]


def safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
