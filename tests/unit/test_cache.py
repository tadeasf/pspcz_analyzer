"""Tests for parquet caching layer."""

import time

import polars as pl

from pspcz_analyzer.data.cache import get_or_parse


class TestGetOrParse:
    def test_round_trip(self, test_cache_dir, tmp_path):
        """Data should be cached as parquet and loaded back identically."""
        source = tmp_path / "source.unl"
        source.write_text("dummy")
        # Ensure source mtime is settled before creating the cache
        time.sleep(0.05)

        df_orig = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        result = get_or_parse(
            "test_table",
            source,
            lambda: df_orig,
            cache_dir=test_cache_dir,
        )
        assert result.equals(df_orig)

        # Second call should load from cache (parse_fn not called)
        call_count = 0

        def _counter():
            nonlocal call_count
            call_count += 1
            return df_orig

        result2 = get_or_parse(
            "test_table",
            source,
            _counter,
            cache_dir=test_cache_dir,
        )
        assert result2.equals(df_orig)
        assert call_count == 0

    def test_staleness_detection(self, test_cache_dir, tmp_path):
        """Cache should be invalidated when source file is newer."""
        source = tmp_path / "source.unl"
        source.write_text("v1")

        df_v1 = pl.DataFrame({"val": [1]})
        get_or_parse("stale_test", source, lambda: df_v1, cache_dir=test_cache_dir)

        # Make source newer than cache
        time.sleep(0.1)
        source.write_text("v2")

        df_v2 = pl.DataFrame({"val": [2]})
        result = get_or_parse("stale_test", source, lambda: df_v2, cache_dir=test_cache_dir)
        assert result["val"].to_list() == [2]

    def test_missing_source_triggers_parse(self, test_cache_dir, tmp_path):
        """If source doesn't exist, parse_fn should still be called."""
        nonexistent = tmp_path / "missing.unl"
        df = pl.DataFrame({"x": [42]})
        result = get_or_parse("missing_test", nonexistent, lambda: df, cache_dir=test_cache_dir)
        assert result["x"].to_list() == [42]
