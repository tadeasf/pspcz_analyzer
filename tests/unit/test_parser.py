"""Tests for UNL file parser."""

import polars as pl

from pspcz_analyzer.config import UNL_ENCODING
from pspcz_analyzer.data.parser import parse_unl


def _write_unl(tmp_path, filename, lines):
    """Write a UNL file with pipe-delimited lines (Windows-1250 encoded)."""
    content = "\n".join(lines)
    (tmp_path / filename).write_bytes(content.encode(UNL_ENCODING))
    return tmp_path / filename


class TestParseUnl:
    def test_basic_parsing(self, tmp_path):
        """Parse a simple 3-column UNL file."""
        path = _write_unl(
            tmp_path,
            "test.unl",
            [
                "1|Jan|Novák|",
                "2|Petr|Svoboda|",
            ],
        )
        df = parse_unl(path, ["id", "jmeno", "prijmeni"])
        assert df.height == 2
        assert df.width == 3
        assert df["id"].to_list() == ["1", "2"]

    def test_trailing_pipe_handled(self, tmp_path):
        """Trailing pipe should not produce an extra column."""
        path = _write_unl(tmp_path, "test.unl", ["1|hello|world|"])
        df = parse_unl(path, ["a", "b", "c"])
        assert "_trailing" not in df.columns
        assert df.width == 3

    def test_windows_1250_decoding(self, tmp_path):
        """Czech characters should be decoded correctly from Windows-1250."""
        path = _write_unl(tmp_path, "test.unl", ["1|Dvořák|Černý|"])
        df = parse_unl(path, ["id", "first", "last"])
        assert df["first"].to_list() == ["Dvořák"]
        assert df["last"].to_list() == ["Černý"]

    def test_czech_diacritics_preserved(self, tmp_path):
        """Extended Czech chars: ř, ž, ů, ě, š, č, ý, á, í."""
        path = _write_unl(tmp_path, "test.unl", ["řžůěšč|ýáí|ňťď|"])
        df = parse_unl(path, ["a", "b", "c"])
        assert df["a"].to_list() == ["řžůěšč"]
        assert df["b"].to_list() == ["ýáí"]
        assert df["c"].to_list() == ["ňťď"]

    def test_dtype_casting(self, tmp_path):
        """Integer dtype casting should work on valid numeric strings."""
        path = _write_unl(
            tmp_path,
            "test.unl",
            [
                "1|100|text|",
                "2|200|more|",
            ],
        )
        df = parse_unl(
            path,
            ["id", "num", "name"],
            dtypes={"id": pl.Int64, "num": pl.Int32},
        )
        assert df["id"].dtype == pl.Int64
        assert df["num"].dtype == pl.Int32
        assert df["name"].dtype == pl.Utf8

    def test_empty_file(self, tmp_path):
        """Empty file should return empty DataFrame with correct columns."""
        path = tmp_path / "empty.unl"
        path.write_bytes(b"")
        df = parse_unl(path, ["a", "b", "c"])
        assert df.height == 0
        assert df.columns == ["a", "b", "c"]

    def test_quote_char_none(self, tmp_path):
        """Literal double quotes in data should not be treated as CSV quoting."""
        path = _write_unl(
            tmp_path,
            "test.unl",
            [
                '1|He said "hello"|done|',
            ],
        )
        df = parse_unl(path, ["id", "text", "status"])
        assert '"hello"' in df["text"].to_list()[0]

    def test_whitespace_stripped_in_casts(self, tmp_path):
        """Whitespace around numeric values should be stripped before casting."""
        path = _write_unl(tmp_path, "test.unl", [" 42 |  100  |text|"])
        df = parse_unl(
            path,
            ["a", "b", "c"],
            dtypes={"a": pl.Int64, "b": pl.Int32},
        )
        assert df["a"].to_list() == [42]
        assert df["b"].to_list() == [100]
