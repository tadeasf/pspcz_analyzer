"""Atomic file I/O helpers shared across the data pipelines.

The frontend and backend processes share one cache directory (a Docker bind
mount in production) and the frontend's file watcher reloads pipeline outputs
whenever their mtime changes. Plain writes leave a window where the file is
truncated or half-written, so a concurrent reader can crash on a torn parquet
or a crash mid-write can corrupt the file permanently. These helpers always
write to a temp file in the destination's directory and then move it into
place with ``os.replace`` (atomic within one filesystem).
"""

import json
import os
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import polars as pl

_CHUNK_SIZE = 65536


def _atomic_write(path: Path, writer: Callable[[Path], None]) -> None:
    """Write ``path`` atomically via a same-directory temp file + ``os.replace``.

    The temp file must live in the destination's directory: ``os.replace`` is
    atomic only within a single filesystem and fails with EXDEV across devices
    (e.g. /tmp vs. a Docker bind mount).

    Args:
        path: Destination file path.
        writer: Callback that writes the payload to the temp path it receives.
            If it raises, the temp file is removed and ``path`` is left
            untouched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        # mkstemp creates files with mode 0600; restore normal permissions so
        # the cache stays world-readable (other container users read it too).
        os.chmod(tmp, 0o644)
        writer(tmp)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _write_parquet(df: pl.DataFrame, tmp: Path) -> None:
    """Write a DataFrame to a temp path (writer callback for ``_atomic_write``)."""
    df.write_parquet(tmp)


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet without exposing a torn file to readers.

    Args:
        df: DataFrame to persist.
        path: Destination parquet path.
    """
    _atomic_write(path, lambda tmp: _write_parquet(df, tmp))


def _write_json(data: Any, tmp: Path) -> None:
    """Serialize JSON to a temp path (writer callback for ``_atomic_write``)."""
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_json_atomic(data: Any, path: Path) -> None:
    """Write a JSON-serializable object atomically (indented, trailing newline).

    Args:
        data: JSON-serializable payload.
        path: Destination JSON path.
    """
    _atomic_write(path, lambda tmp: _write_json(data, tmp))


def _stream_chunks(response: httpx.Response, tmp: Path, chunk_size: int) -> None:
    """Copy a streamed response body to a file in chunks."""
    with open(tmp, "wb") as f:
        for chunk in response.iter_bytes(chunk_size=chunk_size):
            f.write(chunk)


def stream_to_atomic(response: httpx.Response, dest: Path, chunk_size: int = _CHUNK_SIZE) -> None:
    """Stream an open httpx response body to ``dest`` atomically.

    A failure mid-stream (connection reset, timeout) removes the temp file and
    leaves ``dest`` absent rather than truncated, so the next run downloads
    cleanly instead of tripping over a partial file.

    Args:
        response: Response from ``client.stream(...)``, used inside its context
            manager.
        dest: Destination file path.
        chunk_size: Read chunk size in bytes.
    """
    _atomic_write(dest, lambda tmp: _stream_chunks(response, tmp, chunk_size))


def assert_zip_intact(path: Path) -> None:
    """Verify that a ZIP archive is complete and all members pass CRC checks.

    Args:
        path: Path to the ZIP file.

    Raises:
        zipfile.BadZipFile: If the archive structure is corrupt or a member
            fails its CRC check.
    """
    with zipfile.ZipFile(path) as zf:
        bad_member = zf.testzip()
    if bad_member is not None:
        raise zipfile.BadZipFile(f"Corrupt member in {path.name}: {bad_member}")
