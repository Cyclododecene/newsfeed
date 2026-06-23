from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
import hashlib
from typing import Any

from newsfeed.utils.CAMEO import eventcode


@dataclass
class Article:
    index: int
    title: str
    url: str
    row_key: str = ""
    source: str = ""
    published_at: str = ""
    display_time: str = ""
    country: str = ""
    language: str = ""
    tone: str = ""
    event_code: str = ""
    event_label: str = ""
    actors: str = ""
    mentions: str = ""
    enrichment_status: str = "none"
    watch_hits: list[str] = field(default_factory=list)
    match_reason: str = ""
    query: str = ""
    fulltext: str = ""
    error: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class TimelinePoint:
    timestamp: str
    value: float | int | str
    raw: dict[str, Any] = field(default_factory=dict)


def _value(row: dict[str, Any], *names: str) -> str:
    for name in names:
        if name in row and row[name] is not None:
            value = row[name]
            if value != "":
                return str(value)
    return ""


@lru_cache(maxsize=1)
def cameo_labels() -> dict[str, str]:
    return dict(zip(eventcode["Code"].astype(str), eventcode["EventDescription"].astype(str)))


def cameo_label(code: str) -> str:
    if not code:
        return ""
    labels = cameo_labels()
    normalized = str(code).strip()
    for candidate in [normalized, normalized.zfill(2), normalized.zfill(3), normalized.zfill(4)]:
        if candidate in labels:
            return labels[candidate]
    return ""


def article_from_record(index: int, row: dict[str, Any], query: str = "") -> Article:
    article = Article(
        index=index,
        title=_value(row, "title", "Title", "headline", "Headline"),
        url=_value(row, "url", "URL", "SOURCEURL", "sourceurl"),
        source=_value(row, "domain", "source", "sourceCommonName", "source_name"),
        published_at=_value(row, "seendate", "timeadded", "date", "published_at"),
        display_time=format_event_time(_value(row, "seendate", "timeadded", "date", "published_at")),
        country=_value(row, "sourcecountry", "country", "location"),
        language=_value(row, "language", "lang"),
        tone=_value(row, "tone", "avgTone", "avgtone"),
        query=query,
        raw=row,
    )
    article.row_key = stable_row_key(article)
    return article


def article_from_event_record(index: int, row: dict[str, Any], query: str = "") -> Article:
    actor1 = _value(row, "Actor1Name")
    actor2 = _value(row, "Actor2Name")
    event_code = _value(row, "EventCode")
    event_label = cameo_label(event_code)
    location = _value(row, "ActionGeo_FullName", "Actor1Geo_FullName", "Actor2Geo_FullName")
    actors = " / ".join([part for part in [actor1, actor2] if part])
    title_parts = [part for part in [event_label or event_code, actors, location] if part]
    title = " | ".join(title_parts) or f"EVENT {_value(row, 'GLOBALEVENTID')}"

    article = Article(
        index=index,
        title=title,
        url=_value(row, "SOURCEURL"),
        source=_value(row, "SOURCEURL"),
        published_at=_value(row, "DATEADDED", "SQLDATE"),
        display_time=format_event_time(_value(row, "DATEADDED", "SQLDATE")),
        country=_value(
            row,
            "ActionGeo_CountryCode",
            "Actor1CountryCode",
            "Actor2CountryCode",
        ),
        tone=_value(row, "AvgTone"),
        event_code=event_code,
        event_label=event_label,
        actors=actors,
        mentions=_value(row, "NumMentions"),
        enrichment_status=_value(row, "enrichment_status") or "none",
        query=query,
        raw=row,
    )
    article.row_key = stable_row_key(article)
    return article


def article_from_index_record(index: int, row: dict[str, Any], query: str = "") -> Article:
    article = Article(
        index=index,
        title=_value(row, "title", "event_label"),
        url=_value(row, "source_url"),
        source=_value(row, "source_url"),
        published_at=_value(row, "published_at"),
        display_time=format_event_time(_value(row, "published_at")),
        country=_value(row, "country"),
        tone=_value(row, "tone"),
        event_code=_value(row, "event_code"),
        event_label=_value(row, "event_label"),
        actors=_value(row, "actors"),
        mentions=_value(row, "mentions"),
        enrichment_status=_value(row, "enrichment_status") or "none",
        query=query,
        raw=row,
    )
    article.row_key = _value(row, "id") or stable_row_key(article)
    return article


def stable_row_key(article: Article) -> str:
    raw_id = _value(article.raw, "GLOBALEVENTID", "id")
    basis = raw_id or "|".join(
        [
            article.url,
            article.published_at,
            article.event_code,
            article.actors,
            str(article.index),
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def format_event_time(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    for fmt, length in [
        ("%Y%m%d%H%M%S", 14),
        ("%Y%m%d%H%M", 12),
        ("%Y%m%d", 8),
    ]:
        if len(digits) >= length:
            try:
                dt = datetime.strptime(digits[:length], fmt)
            except ValueError:
                continue
            if length == 8:
                return dt.strftime("%Y-%m-%d")
            return dt.strftime("%Y-%m-%d %H:%M")
    return text
