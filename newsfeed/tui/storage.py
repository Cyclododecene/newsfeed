from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import gettempdir
from typing import Any

from newsfeed.tui.models import Article


@dataclass
class StoredWatchItem:
    id: int
    kind: str
    value: str


class TuiStorage:
    """SQLite workspace storage for lightweight TUI metadata only."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        candidates = [Path(db_path).expanduser()] if db_path is not None else default_db_candidates()
        last_error = None
        for candidate in candidates:
            try:
                candidate.parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(candidate, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                self._conn = conn
                self._lock = threading.Lock()
                self.db_path = candidate
                self.init_schema()
                self.assert_writable()
                break
            except (OSError, sqlite3.Error) as exc:
                last_error = exc
                try:
                    self._conn.close()
                except Exception:
                    pass
        else:
            raise sqlite3.OperationalError(f"unable to open TUI storage database: {last_error}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            ensure_columns(
                self._conn,
                "article_index",
                {
                    "enrichment_status": "TEXT NOT NULL DEFAULT 'none'",
                    "enriched_at": "TEXT NOT NULL DEFAULT ''",
                    "enrichment_error": "TEXT NOT NULL DEFAULT ''",
                },
            )
            ensure_columns(
                self._conn,
                "alerts",
                {
                    "country": "TEXT NOT NULL DEFAULT ''",
                    "source": "TEXT NOT NULL DEFAULT ''",
                    "tone_threshold": "REAL",
                    "scan_frequency": "INTEGER NOT NULL DEFAULT 300",
                    "last_scanned_at": "TEXT NOT NULL DEFAULT ''",
                },
            )
            ensure_columns(
                self._conn,
                "alert_hits",
                {
                    "read_at": "TEXT NOT NULL DEFAULT ''",
                },
            )
            self._conn.commit()

    def assert_writable(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS __newsfeed_write_probe(id INTEGER PRIMARY KEY);
                INSERT INTO __newsfeed_write_probe DEFAULT VALUES;
                DROP TABLE __newsfeed_write_probe;
                """
            )
            self._conn.commit()

    @property
    def cache_dir(self) -> Path:
        return self.db_path.parent

    @property
    def fulltext_dir(self) -> Path:
        path = self.cache_dir / "fulltext"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_or_create_watchlist(self, name: str = "default") -> int:
        now = utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO watchlists(name, created_at, updated_at)
                VALUES (?, ?, ?)
                """,
                (name, now, now),
            )
            row = self._conn.execute(
                "SELECT id FROM watchlists WHERE name = ?",
                (name,),
            ).fetchone()
            self._conn.commit()
            return int(row["id"])

    def add_watch_item(self, kind: str, value: str, watchlist: str = "default") -> StoredWatchItem:
        watchlist_id = self.get_or_create_watchlist(watchlist)
        normalized_kind = kind.lower()
        normalized_value = value.strip()
        now = utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO watchlist_items(watchlist_id, kind, value, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (watchlist_id, normalized_kind, normalized_value, now),
            )
            row = self._conn.execute(
                """
                SELECT id, kind, value
                FROM watchlist_items
                WHERE watchlist_id = ? AND kind = ? AND value = ?
                """,
                (watchlist_id, normalized_kind, normalized_value),
            ).fetchone()
            self._conn.commit()
            return StoredWatchItem(id=int(row["id"]), kind=row["kind"], value=row["value"])

    def list_watch_items(self, watchlist: str = "default") -> list[StoredWatchItem]:
        watchlist_id = self.get_or_create_watchlist(watchlist)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, kind, value
                FROM watchlist_items
                WHERE watchlist_id = ?
                ORDER BY id
                """,
                (watchlist_id,),
            ).fetchall()
        return [StoredWatchItem(id=int(row["id"]), kind=row["kind"], value=row["value"]) for row in rows]

    def list_watchlists(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT name FROM watchlists ORDER BY name").fetchall()
        return [row["name"] for row in rows]

    def delete_watch_item(self, item_id: int) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM watchlist_items WHERE id = ?", (item_id,))
            self._conn.commit()
            return cursor.rowcount > 0

    def save_query(self, name: str, command: str) -> int:
        now = utc_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO saved_queries(name, command, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET command = excluded.command, updated_at = excluded.updated_at
                """,
                (name, command, now, now),
            )
            row = self._conn.execute("SELECT id FROM saved_queries WHERE name = ?", (name,)).fetchone()
            self._conn.commit()
            return int(row["id"] if row else cursor.lastrowid)

    def list_saved_queries(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, name, command, created_at, updated_at FROM saved_queries ORDER BY name"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_saved_query(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, name, command, created_at, updated_at FROM saved_queries WHERE name = ?",
                (name,),
            ).fetchone()
        return dict(row) if row else None

    def record_query_history(
        self,
        command: str,
        *,
        result_count: int = 0,
        result_path: str = "",
        error: str = "",
    ) -> int:
        now = utc_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO query_history(command, result_count, result_path, error, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (command, result_count, result_path, error, now),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def list_query_history(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, command, result_count, result_path, error, created_at
                FROM query_history
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_setting(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        now = utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO settings(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
                """,
                (key, encoded, now),
            )
            self._conn.commit()

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT value_json FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return json.loads(row["value_json"])

    def list_settings(self) -> dict[str, Any]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value_json FROM settings ORDER BY key").fetchall()
        return {row["key"]: json.loads(row["value_json"]) for row in rows}

    def save_workspace_layout(self, name: str, layout: dict[str, Any]) -> int:
        now = utc_now()
        encoded = json.dumps(layout, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO workspace_layouts(name, layout_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    layout_json = excluded.layout_json,
                    updated_at = excluded.updated_at
                """,
                (name, encoded, now),
            )
            row = self._conn.execute("SELECT id FROM workspace_layouts WHERE name = ?", (name,)).fetchone()
            self._conn.commit()
            return int(row["id"])

    def get_workspace_layout(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT name, layout_json, updated_at FROM workspace_layouts WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            return None
        return {"name": row["name"], "layout": json.loads(row["layout_json"]), "updated_at": row["updated_at"]}

    def list_workspace_layouts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name, layout_json, updated_at FROM workspace_layouts ORDER BY name"
            ).fetchall()
        return [
            {"name": row["name"], "layout": json.loads(row["layout_json"]), "updated_at": row["updated_at"]}
            for row in rows
        ]

    def upsert_article_index(
        self,
        article: Article,
        *,
        content_hash: str = "",
        fulltext_path: str = "",
        result_path: str = "",
        enrichment_status: str = "",
        enrichment_error: str = "",
    ) -> str:
        article_id = stable_article_id(article)
        now = utc_now()
        status = enrichment_status or article.enrichment_status or "none"
        enriched_at = now if status == "indexed" else ""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO article_index(
                    id, source_url, title, event_code, event_label, actors, country,
                    published_at, tone, mentions, content_hash, fulltext_path,
                    result_path, enrichment_status, enriched_at, enrichment_error,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    source_url = excluded.source_url,
                    title = excluded.title,
                    event_code = excluded.event_code,
                    event_label = excluded.event_label,
                    actors = excluded.actors,
                    country = excluded.country,
                    published_at = excluded.published_at,
                    tone = excluded.tone,
                    mentions = excluded.mentions,
                    content_hash = COALESCE(NULLIF(excluded.content_hash, ''), article_index.content_hash),
                    fulltext_path = COALESCE(NULLIF(excluded.fulltext_path, ''), article_index.fulltext_path),
                    result_path = COALESCE(NULLIF(excluded.result_path, ''), article_index.result_path),
                    enrichment_status = CASE
                        WHEN excluded.enrichment_status != 'none' THEN excluded.enrichment_status
                        ELSE article_index.enrichment_status
                    END,
                    enriched_at = COALESCE(NULLIF(excluded.enriched_at, ''), article_index.enriched_at),
                    enrichment_error = CASE
                        WHEN excluded.enrichment_error != '' THEN excluded.enrichment_error
                        WHEN excluded.enrichment_status = 'indexed' THEN ''
                        ELSE article_index.enrichment_error
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    article_id,
                    article.url,
                    article.title,
                    article.event_code,
                    article.event_label,
                    article.actors,
                    article.country,
                    article.published_at,
                    article.tone,
                    article.mentions,
                    content_hash,
                    fulltext_path,
                    result_path,
                    status,
                    enriched_at,
                    enrichment_error,
                    now,
                    now,
                ),
            )
            self._conn.commit()
        return article_id

    def index_articles(self, articles: list[Article], *, result_path: str = "") -> list[str]:
        return [self.upsert_article_index(article, result_path=result_path) for article in articles]

    def get_article_index(self, article_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM article_index WHERE id = ?", (article_id,)).fetchone()
        return dict(row) if row else None

    def get_article_index_by_url(self, source_url: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT *
                FROM article_index
                WHERE source_url = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (source_url,),
            ).fetchone()
        return dict(row) if row else None

    def set_enrichment_status(self, article: Article, status: str, error: str = "") -> str:
        if status not in {"none", "pending", "indexed", "failed"}:
            raise ValueError("Invalid enrichment status")
        article_id = self.upsert_article_index(
            article,
            enrichment_status=status,
            enrichment_error=error,
        )
        article.enrichment_status = status
        if error:
            article.error = error
        return article_id

    def save_fulltext(self, article: Article, text: str) -> tuple[str, str]:
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        fulltext_path = self.fulltext_dir / f"{content_hash}.txt"
        fulltext_path.write_text(text, encoding="utf-8")
        self.upsert_article_index(
            article,
            content_hash=content_hash,
            fulltext_path=str(fulltext_path),
            enrichment_status="indexed",
        )
        article.enrichment_status = "indexed"
        return content_hash, str(fulltext_path)

    def load_cached_fulltext(self, article: Article) -> tuple[str, str] | None:
        row = self.get_article_index_by_url(article.url) if article.url else None
        fulltext_path = ""
        content_hash = ""
        if row is not None:
            fulltext_path = row.get("fulltext_path", "")
            content_hash = row.get("content_hash", "")
        if not fulltext_path:
            fulltext_path = str(article.raw.get("fulltext_path", ""))
            content_hash = str(article.raw.get("content_hash", ""))
        if not fulltext_path:
            return None

        path = Path(fulltext_path).expanduser()
        if not path.exists() or not path.is_file():
            return None
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return None
        if not text:
            return None

        article.fulltext = text
        article.enrichment_status = "indexed"
        article.error = ""
        article.raw = {
            **article.raw,
            "content_hash": content_hash or path.stem,
            "fulltext_path": str(path),
        }
        if row is not None:
            article.raw = {
                **article.raw,
                "enriched_at": row.get("enriched_at", ""),
                "enrichment_error": row.get("enrichment_error", ""),
            }
        return text, str(path)

    def search_fulltext(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        terms = normalized_terms(query)
        if not terms:
            return []
        rows = self._article_rows_with_fulltext()
        matches: list[dict[str, Any]] = []
        for row in rows:
            path = Path(row["fulltext_path"]).expanduser()
            if not path.exists():
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore").lower()
            except OSError:
                continue
            if all(term in text for term in terms):
                matches.append(dict(row))
                if len(matches) >= limit:
                    break
        return matches

    def fulltext_match_urls(self, query: str) -> set[str]:
        return {row["source_url"] for row in self.search_fulltext(query, limit=10000)}

    def add_alert(
        self,
        name: str,
        query: str,
        *,
        country: str = "",
        source: str = "",
        tone_threshold: float | None = None,
        scan_frequency: int = 300,
        enabled: bool = True,
    ) -> int:
        now = utc_now()
        frequency = max(int(scan_frequency), 1)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO alerts(
                    name, query, country, source, tone_threshold, scan_frequency,
                    enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    query = excluded.query,
                    country = excluded.country,
                    source = excluded.source,
                    tone_threshold = excluded.tone_threshold,
                    scan_frequency = excluded.scan_frequency,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    name,
                    query,
                    country.upper().strip(),
                    source.strip(),
                    tone_threshold,
                    frequency,
                    int(enabled),
                    now,
                    now,
                ),
            )
            row = self._conn.execute("SELECT id FROM alerts WHERE name = ?", (name,)).fetchone()
            self._conn.commit()
            return int(row["id"])

    def list_alerts(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        where = "WHERE enabled = 1" if enabled_only else ""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, name, query, country, source, tone_threshold, scan_frequency,
                       last_scanned_at, enabled, created_at, updated_at
                FROM alerts
                """
                f"{where} ORDER BY name"
            ).fetchall()
        return [dict(row) for row in rows]

    def delete_alert(self, name: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM alerts WHERE name = ?", (name,))
            self._conn.commit()
            return cursor.rowcount > 0

    def check_alerts(self) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        for alert in self.list_alerts(enabled_only=True):
            for article in self.match_alert_articles(alert):
                hit = {
                    "alert_id": alert["id"],
                    "alert_name": alert["name"],
                    "query": alert["query"],
                    "country": alert.get("country", ""),
                    "source": alert.get("source", ""),
                    "tone_threshold": alert.get("tone_threshold"),
                    "article_id": article["id"],
                    "title": article["title"],
                    "source_url": article["source_url"],
                    "match_reason": article.get("_alert_match_reason", ""),
                }
                self.record_alert_hit(alert["id"], article["id"], hit)
                hits.append(hit)
            self.mark_alert_scanned(alert["id"])
        return hits

    def match_alert_articles(self, alert: dict[str, Any]) -> list[dict[str, Any]]:
        query_terms = normalized_terms(alert.get("query", ""))
        country = str(alert.get("country") or "").lower()
        source = str(alert.get("source") or "").lower()
        tone_threshold = alert.get("tone_threshold")
        rows = self.list_article_index(limit=10000)
        matches = []
        for row in rows:
            if country and str(row.get("country", "")).lower() != country:
                continue
            if source and source not in str(row.get("source_url", "")).lower():
                continue
            if tone_threshold is not None and tone_threshold != "":
                try:
                    if abs(float(row.get("tone") or 0)) < float(tone_threshold):
                        continue
                except (TypeError, ValueError):
                    continue

            metadata_match = terms_match(article_index_metadata(row), query_terms)
            fulltext_match = self.article_fulltext_matches(row, query_terms)
            if query_terms and not (metadata_match or fulltext_match):
                continue

            match = dict(row)
            reason = []
            if metadata_match:
                reason.append("metadata")
            if fulltext_match:
                reason.append("fulltext")
            if country:
                reason.append("country")
            if source:
                reason.append("source")
            if tone_threshold not in {None, ""}:
                reason.append("tone")
            match["_alert_match_reason"] = "+".join(reason) or "rule"
            matches.append(match)
        return matches

    def list_article_index(self, limit: int = 10000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM article_index
                ORDER BY published_at DESC, updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def article_fulltext_matches(self, row: dict[str, Any], terms: list[str]) -> bool:
        if not terms:
            return False
        fulltext_path = row.get("fulltext_path", "")
        if not fulltext_path:
            return False
        path = Path(fulltext_path).expanduser()
        if not path.exists():
            return False
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except OSError:
            return False
        return terms_match(text, terms)

    def mark_alert_scanned(self, alert_id: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE alerts
                SET last_scanned_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), utc_now(), alert_id),
            )
            self._conn.commit()

    def record_alert_hit(self, alert_id: int, article_id: str, metadata: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO alert_hits(alert_id, article_id, hit_at, metadata_json, read_at)
                VALUES (?, ?, ?, ?, '')
                """,
                (alert_id, article_id, utc_now(), json.dumps(metadata, ensure_ascii=False)),
            )
            self._conn.commit()

    def list_alert_hits(self, limit: int = 50, unread_only: bool = False) -> list[dict[str, Any]]:
        where = "WHERE alert_hits.read_at = ''" if unread_only else ""
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT alert_hits.id, alerts.name AS alert_name, alert_hits.article_id,
                       alert_hits.hit_at, alert_hits.metadata_json, alert_hits.read_at
                FROM alert_hits
                JOIN alerts ON alerts.id = alert_hits.alert_id
                {where}
                ORDER BY alert_hits.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def unread_alert_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS count FROM alert_hits WHERE read_at = ''"
            ).fetchone()
        return int(row["count"])

    def mark_alert_hits_read(self, hit_ids: list[int] | None = None) -> None:
        now = utc_now()
        with self._lock:
            if hit_ids:
                self._conn.executemany(
                    "UPDATE alert_hits SET read_at = ? WHERE id = ?",
                    [(now, hit_id) for hit_id in hit_ids],
                )
            else:
                self._conn.execute("UPDATE alert_hits SET read_at = ? WHERE read_at = ''", (now,))
            self._conn.commit()

    @property
    def results_dir(self) -> Path:
        path = self.cache_dir / "results"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def cache_stats(self) -> dict[str, Any]:
        with self._lock:
            article_count = int(self._conn.execute("SELECT COUNT(*) AS c FROM article_index").fetchone()["c"])
            alert_count = int(self._conn.execute("SELECT COUNT(*) AS c FROM alerts").fetchone()["c"])
            history_count = int(self._conn.execute("SELECT COUNT(*) AS c FROM query_history").fetchone()["c"])
        fulltext_files, fulltext_bytes = directory_stats(self.fulltext_dir)
        result_files, result_bytes = directory_stats(self.results_dir)
        db_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        return {
            "cache_dir": str(self.cache_dir),
            "db_path": str(self.db_path),
            "db_bytes": db_bytes,
            "fulltext_files": fulltext_files,
            "fulltext_bytes": fulltext_bytes,
            "result_files": result_files,
            "result_bytes": result_bytes,
            "article_count": article_count,
            "alert_count": alert_count,
            "history_count": history_count,
        }

    def cleanup_cache(self, target: str = "results") -> dict[str, int]:
        normalized = target.lower()
        if normalized == "results":
            directory = self.results_dir
        elif normalized == "fulltext":
            directory = self.fulltext_dir
        else:
            raise ValueError("CACHE CLEAN target must be RESULTS or FULLTEXT.")
        removed_files = 0
        removed_bytes = 0
        for path in directory.glob("*"):
            if not path.is_file():
                continue
            size = path.stat().st_size
            path.unlink()
            removed_files += 1
            removed_bytes += size
        return {"removed_files": removed_files, "removed_bytes": removed_bytes}

    def export_config(self, path: str | Path) -> Path:
        output_path = Path(path).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "watchlists": self.export_watchlists(),
            "saved_queries": self.list_saved_queries(),
            "alerts": self.list_alerts(),
            "settings": self.list_settings(),
            "workspace_layouts": self.list_workspace_layouts(),
        }
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return output_path

    def import_config(self, path: str | Path) -> dict[str, int]:
        payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
        counts = {"watchlists": 0, "saved_queries": 0, "alerts": 0, "settings": 0, "workspace_layouts": 0}
        for watchlist in payload.get("watchlists", []):
            name = watchlist.get("name", "default")
            for item in watchlist.get("items", []):
                self.add_watch_item(item["kind"], item["value"], watchlist=name)
                counts["watchlists"] += 1
        for query in payload.get("saved_queries", []):
            self.save_query(query["name"], query["command"])
            counts["saved_queries"] += 1
        for alert in payload.get("alerts", []):
            self.add_alert(
                alert["name"],
                alert["query"],
                country=alert.get("country", ""),
                source=alert.get("source", ""),
                tone_threshold=alert.get("tone_threshold"),
                scan_frequency=int(alert.get("scan_frequency") or 300),
                enabled=bool(alert.get("enabled", 1)),
            )
            counts["alerts"] += 1
        for key, value in payload.get("settings", {}).items():
            self.set_setting(key, value)
            counts["settings"] += 1
        for layout in payload.get("workspace_layouts", []):
            self.save_workspace_layout(layout["name"], layout.get("layout", {}))
            counts["workspace_layouts"] += 1
        return counts

    def export_watchlists(self) -> list[dict[str, Any]]:
        return [
            {
                "name": name,
                "items": [
                    {"kind": item.kind, "value": item.value}
                    for item in self.list_watch_items(name)
                ],
            }
            for name in self.list_watchlists()
        ]

    def _article_rows_with_fulltext(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT *
                FROM article_index
                WHERE fulltext_path != ''
                ORDER BY published_at DESC, updated_at DESC
                """
            ).fetchall()


def default_db_path() -> Path:
    return default_db_candidates()[0]


def default_db_candidates() -> list[Path]:
    if os.environ.get("NEWSFEED_CACHE_DIR"):
        primary = Path(os.environ["NEWSFEED_CACHE_DIR"]).expanduser() / "tui.db"
    elif os.environ.get("XDG_CACHE_HOME"):
        primary = Path(os.environ["XDG_CACHE_HOME"]).expanduser() / "newsfeed" / "tui.db"
    else:
        primary = Path.home() / ".cache" / "newsfeed" / "tui.db"
    fallback = Path(gettempdir()) / "newsfeed" / "tui.db"
    return [primary, fallback]


def stable_article_id(article: Article) -> str:
    basis = article.url or f"{article.published_at}|{article.event_code}|{article.actors}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def normalized_terms(query: str) -> list[str]:
    return [term for term in query.lower().split() if term]


def terms_match(text: str, terms: list[str]) -> bool:
    if not terms:
        return True
    haystack = text.lower()
    return all(term in haystack for term in terms)


def article_index_metadata(row: dict[str, Any]) -> str:
    fields = [
        row.get("title", ""),
        row.get("event_code", ""),
        row.get("event_label", ""),
        row.get("actors", ""),
        row.get("country", ""),
        row.get("source_url", ""),
    ]
    return " ".join(str(field) for field in fields).lower()


def directory_stats(directory: Path) -> tuple[int, int]:
    files = 0
    total_bytes = 0
    if not directory.exists():
        return 0, 0
    for path in directory.glob("*"):
        if path.is_file():
            files += 1
            total_bytes += path.stat().st_size
    return files, total_bytes


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS watchlists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(watchlist_id, kind, value)
);

CREATE TABLE IF NOT EXISTS saved_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS query_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    result_path TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    query TEXT NOT NULL,
    country TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    tone_threshold REAL,
    scan_frequency INTEGER NOT NULL DEFAULT 300,
    last_scanned_at TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id INTEGER NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    article_id TEXT NOT NULL,
    hit_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    read_at TEXT NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_hits_unique
ON alert_hits(alert_id, article_id);

CREATE TABLE IF NOT EXISTS article_index (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    title TEXT NOT NULL,
    event_code TEXT NOT NULL,
    event_label TEXT NOT NULL,
    actors TEXT NOT NULL,
    country TEXT NOT NULL,
    published_at TEXT NOT NULL,
    tone TEXT NOT NULL,
    mentions TEXT NOT NULL,
    content_hash TEXT NOT NULL DEFAULT '',
    fulltext_path TEXT NOT NULL DEFAULT '',
    result_path TEXT NOT NULL DEFAULT '',
    enrichment_status TEXT NOT NULL DEFAULT 'none',
    enriched_at TEXT NOT NULL DEFAULT '',
    enrichment_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reading_state (
    article_id TEXT PRIMARY KEY REFERENCES article_index(id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    last_read_at TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS workspace_layouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    layout_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""
