from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


COMMANDS = {
    "TOP",
    "NEWS",
    "SRC",
    "GEO",
    "READ",
    "TL",
    "FULLTEXT",
    "EXPORT",
    "BRIEF",
    "CITE",
    "HELP",
    "QUIT",
    "WATCH",
    "SEARCH",
    "ALERT",
    "NEXT",
    "PREV",
    "PAGE",
    "SAVE",
    "LOAD",
    "WORKSPACE",
    "CACHE",
    "CONFIG",
    "LIB",
    "CANCEL",
}


@dataclass
class ParsedCommand:
    name: str
    args: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)
    raw: str = ""

    @property
    def query(self) -> str:
        return " ".join(self.args).strip()


def parse_command(text: str) -> ParsedCommand:
    raw = text.strip()
    if not raw:
        raise ValueError("Enter a command. Try HELP.")

    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid command syntax: {exc}") from exc

    if not tokens:
        raise ValueError("Enter a command. Try HELP.")

    name = tokens[0].upper()
    if name not in COMMANDS:
        raise ValueError(f"Unknown command: {tokens[0]}")

    args: list[str] = []
    options: dict[str, str] = {}
    for token in tokens[1:]:
        if ":" in token:
            key, value = token.split(":", 1)
            if key and re.fullmatch(r"[A-Za-z_]+", key):
                options[key.upper()] = value
                continue
        args.append(token)

    return ParsedCommand(name=name, args=args, options=options, raw=raw)


def parse_row_number(command: ParsedCommand) -> int:
    if not command.args:
        raise ValueError("READ requires a row number.")
    try:
        row_number = int(command.args[0])
    except ValueError as exc:
        raise ValueError("READ row must be an integer.") from exc
    if row_number < 1:
        raise ValueError("READ row must be 1 or greater.")
    return row_number


def parse_limit(value: str | None, default: int = 50) -> int:
    if value is None or value == "":
        return default
    try:
        limit = int(value)
    except ValueError as exc:
        raise ValueError("LIMIT must be an integer.") from exc
    if limit < 1 or limit > 250:
        raise ValueError("LIMIT must be between 1 and 250.")
    return limit


def parse_order(value: str | None, default: str = "newest") -> str:
    order = (value or default).strip().lower()
    if order not in {"newest", "oldest", "tone", "sources", "relevance"}:
        raise ValueError("ORDER must be newest, oldest, tone, sources, or relevance.")
    return order


def parse_since_window(window: str | None, default: str = "6h") -> timedelta:
    value = (window or default).strip().lower()
    match = re.fullmatch(r"(\d+)(m|h|d)", value)
    if not match:
        raise ValueError("SINCE must use a window like 30m, 6h, or 7d.")

    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        raise ValueError("SINCE must be positive.")
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


def since_to_range(
    window: str | None,
    default: str = "6h",
    now: datetime | None = None,
) -> tuple[str, str]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    start = current - parse_since_window(window, default=default)
    return _format_gdelt(start), _format_gdelt(current)


def _format_gdelt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d-%H-%M-%S")
