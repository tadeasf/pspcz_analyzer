"""Parse UNL (pipe-delimited) files into Polars DataFrames."""

from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from pspcz_analyzer.config import UNL_ENCODING, UNL_SEPARATOR


def parse_unl(
    file_path: Path,
    columns: list[str],
    dtypes: dict[str, Any] | None = None,
    *,
    disable_quoting: bool = False,
) -> pl.DataFrame:
    """Parse a single UNL file into a Polars DataFrame.

    UNL files are pipe-delimited, Windows-1250 encoded, with no header row
    and a trailing pipe on each line (producing an extra empty column).

    Set disable_quoting=True for files that contain unescaped double-quote
    characters in data fields (e.g. bod_schuze.unl).
    """
    raw_bytes = file_path.read_bytes()
    if not raw_bytes.strip():
        logger.info("Skipping empty file {}", file_path.name)
        return pl.DataFrame({c: pl.Series([], dtype=pl.Utf8) for c in columns})

    text = raw_bytes.decode(UNL_ENCODING)
    utf8_bytes = text.encode("utf-8")

    # UNL has trailing pipe -> extra column
    all_columns = columns + ["_trailing"]

    csv_kwargs: dict = dict(
        separator=UNL_SEPARATOR,
        has_header=False,
        new_columns=all_columns,
        infer_schema_length=0,
        truncate_ragged_lines=True,
        encoding="utf8",
    )
    if disable_quoting:
        csv_kwargs["quote_char"] = None

    df = pl.read_csv(utf8_bytes, **csv_kwargs)

    df = df.drop("_trailing")

    # Cast typed columns
    if dtypes:
        cast_exprs = []
        for col_name, dtype in dtypes.items():
            if col_name in df.columns:
                cast_exprs.append(
                    pl.col(col_name).str.strip_chars().cast(dtype, strict=False)
                )
        if cast_exprs:
            df = df.with_columns(cast_exprs)

    logger.info("Parsed {}: {} rows x {} cols", file_path.name, df.height, df.width)
    return df


def parse_unl_multi(
    directory: Path,
    glob_pattern: str,
    columns: list[str],
    dtypes: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Parse multiple UNL files matching a glob pattern and concatenate them."""
    files = sorted(directory.rglob(glob_pattern))
    if not files:
        msg = f"No files matching {glob_pattern} in {directory}"
        raise FileNotFoundError(msg)

    dfs = [parse_unl(f, columns, dtypes) for f in files]
    dfs = [df for df in dfs if df.height > 0]
    if not dfs:
        return pl.DataFrame({c: pl.Series([], dtype=pl.Utf8) for c in columns})
    result = pl.concat(dfs)
    logger.info(
        "Parsed {} files ({}): {} total rows",
        len(files),
        glob_pattern,
        result.height,
    )
    return result
