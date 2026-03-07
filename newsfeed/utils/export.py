"""
Export utility for GDELT query results.
author: Terence Junjie LIU
date: 2026
"""
import io
import os
import logging
from pathlib import Path
from typing import Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = ("csv", "json", "parquet", "jsonl")


def export_results(
    df: pd.DataFrame,
    output_format: str,
    filepath: Optional[Union[str, Path]] = None,
) -> None:
    """
    Export a DataFrame to the specified format.

    Args:
        df:            DataFrame to export.
        output_format: One of ``"csv"``, ``"json"``, ``"jsonl"``,
                       ``"parquet"``.
        filepath:      Destination file path.  If *None*, a filename is
                       auto-generated in the current working directory.

    Raises:
        ValueError: If ``output_format`` is not supported.

    Example::

        from newsfeed.utils.export import export_results
        export_results(df, "parquet", "events_2021.parquet")
    """
    fmt = output_format.lower().strip()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. Choose from: {SUPPORTED_FORMATS}"
        )

    if filepath is None:
        filepath = f"newsfeed_output.{fmt}"
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(filepath, index=False, encoding="utf-8")

    elif fmt == "json":
        df.to_json(filepath, orient="records", force_ascii=False, indent=2)

    elif fmt == "jsonl":
        df.to_json(filepath, orient="records", lines=True, force_ascii=False)

    elif fmt == "parquet":
        df.to_parquet(filepath, index=False, engine="pyarrow")

    logger.info(f"Exported {len(df)} rows to '{filepath}' [{fmt}]")
    print(f"[+] Exported {len(df)} rows → {filepath} [{fmt}]")


def df_to_bytes(df: pd.DataFrame, output_format: str) -> bytes:
    """
    Serialise a DataFrame to raw bytes in the specified format (without
    touching the file system).  Useful for streaming responses.

    Args:
        df:            DataFrame to serialise.
        output_format: One of ``"csv"``, ``"json"``, ``"jsonl"``,
                       ``"parquet"``.

    Returns:
        Serialised bytes.
    """
    fmt = output_format.lower().strip()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. Choose from: {SUPPORTED_FORMATS}"
        )

    buf = io.BytesIO()
    if fmt == "csv":
        df.to_csv(buf, index=False, encoding="utf-8")
    elif fmt == "json":
        df.to_json(buf, orient="records", force_ascii=False, indent=2)
    elif fmt == "jsonl":
        df.to_json(buf, orient="records", lines=True, force_ascii=False)
    elif fmt == "parquet":
        df.to_parquet(buf, index=False, engine="pyarrow")

    return buf.getvalue()
