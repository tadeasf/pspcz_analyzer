"""Tests for atomic file I/O helpers."""

import json
import zipfile
from unittest.mock import MagicMock

import httpx
import polars as pl
import pytest

from pspcz_analyzer.fileio import (
    assert_zip_intact,
    stream_to_atomic,
    write_json_atomic,
    write_parquet_atomic,
)


class TestWriteParquetAtomic:
    def test_round_trip(self, tmp_path):
        """Atomic parquet write should be readable and identical."""
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        dest = tmp_path / "out.parquet"
        write_parquet_atomic(df, dest)
        assert pl.read_parquet(dest).equals(df)

    def test_replaces_existing(self, tmp_path):
        """A second write should fully replace the previous file."""
        dest = tmp_path / "out.parquet"
        write_parquet_atomic(pl.DataFrame({"v": [1]}), dest)
        write_parquet_atomic(pl.DataFrame({"v": [2]}), dest)
        assert pl.read_parquet(dest)["v"].to_list() == [2]

    def test_creates_parent_dirs(self, tmp_path):
        """Missing parent directories should be created."""
        dest = tmp_path / "nested" / "deeper" / "out.parquet"
        write_parquet_atomic(pl.DataFrame({"v": [1]}), dest)
        assert dest.exists()


class TestWriteJsonAtomic:
    def test_format(self, tmp_path):
        """Output should be indented, unescaped UTF-8, with trailing newline."""
        dest = tmp_path / "out.json"
        write_json_atomic({"město": "Praha"}, dest)
        text = dest.read_text("utf-8")
        assert text.endswith("\n")
        assert "město" in text  # not \\u-escaped
        assert json.loads(text) == {"město": "Praha"}

    def test_failure_leaves_original_and_no_temp(self, tmp_path):
        """A failed write must not touch the destination or leak temp files."""
        dest = tmp_path / "out.json"
        dest.write_text("original", encoding="utf-8")

        with pytest.raises(TypeError):
            write_json_atomic({"bad": object()}, dest)

        assert dest.read_text() == "original"
        assert list(tmp_path.glob(".out.json.*.tmp")) == []


class TestStreamToAtomic:
    def test_streams_chunks(self, tmp_path):
        """Response chunks should be concatenated into the destination."""
        response = MagicMock(spec=httpx.Response)
        response.iter_bytes.return_value = [b"abc", b"def"]
        dest = tmp_path / "file.bin"

        stream_to_atomic(response, dest)

        assert dest.read_bytes() == b"abcdef"

    def test_mid_stream_failure_leaves_no_partial(self, tmp_path):
        """A failure mid-download must leave the destination absent."""

        def _interrupted_stream(**_kwargs):
            yield b"partial"
            raise httpx.ReadError("connection reset")

        response = MagicMock(spec=httpx.Response)
        response.iter_bytes.side_effect = _interrupted_stream
        dest = tmp_path / "file.bin"

        with pytest.raises(httpx.ReadError):
            stream_to_atomic(response, dest)

        assert not dest.exists()
        assert list(tmp_path.glob(".file.bin.*.tmp")) == []


class TestAssertZipIntact:
    def test_accepts_valid_archive(self, tmp_path):
        """A complete archive should pass verification."""
        zpath = tmp_path / "ok.zip"
        with zipfile.ZipFile(zpath, "w") as zf:
            zf.writestr("inner.txt", "hello" * 1000)
        assert_zip_intact(zpath)

    def test_rejects_truncated_archive(self, tmp_path):
        """A truncated download should be rejected."""
        zpath = tmp_path / "trunc.zip"
        with zipfile.ZipFile(zpath, "w") as zf:
            zf.writestr("inner.txt", "hello" * 1000)
        data = zpath.read_bytes()
        zpath.write_bytes(data[: len(data) // 2])

        with pytest.raises(zipfile.BadZipFile):
            assert_zip_intact(zpath)

    def test_names_crc_corrupt_member(self, tmp_path):
        """A member with a flipped byte should be reported by name."""
        zpath = tmp_path / "crc.zip"
        with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("good.txt", "ok")
            zf.writestr("bad.txt", "X" * 64)

        raw = bytearray(zpath.read_bytes())
        raw[raw.index(b"X" * 64)] = ord("Y")
        zpath.write_bytes(bytes(raw))

        with pytest.raises(zipfile.BadZipFile, match="bad.txt"):
            assert_zip_intact(zpath)
