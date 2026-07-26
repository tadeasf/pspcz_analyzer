"""Tests for the tisk topic classifier save batching."""

from pathlib import Path

from pspcz_analyzer.services.tisk import classifier
from pspcz_analyzer.services.tisk.classifier import classify_and_save


class _FakeLLM:
    """Minimal LLM client stand-in: always available, never called."""

    model = "fake-model"

    def is_available(self) -> bool:
        return True


def _patch_classifier(monkeypatch) -> list[int]:
    """Fake the LLM client, per-tisk classification, and parquet writes.

    Returns:
        List that records the DataFrame height for every atomic write.
    """
    writes: list[int] = []
    monkeypatch.setattr(classifier, "create_llm_client", lambda: _FakeLLM())
    monkeypatch.setattr(
        classifier, "write_parquet_atomic", lambda df, path: writes.append(df.height)
    )
    monkeypatch.setattr(
        classifier,
        "_classify_single_tisk",
        lambda ct, text_path, llm, use_ai, i, total, existing_record=None, cancel_check=None: {
            "ct": ct,
            "topic": '["téma"]',
            "topic_en": '["topic"]',
            "summary": "souhrn",
            "summary_en": "summary",
            "source": "llm:fake-model",
        },
    )
    return writes


class TestClassifyAndSaveBatching:
    """classify_and_save must flush parquet periodically, not per tisk."""

    def test_saves_every_25_plus_final(self, tmp_path: Path, monkeypatch) -> None:
        writes = _patch_classifier(monkeypatch)
        text_paths = {ct: tmp_path / f"{ct}.txt" for ct in range(1, 31)}

        topic_map, summary_map, summary_en_map = classify_and_save(9, text_paths, tmp_path)

        # 30 tisks: one write at 25, one final flush at 30 — not 30 writes
        assert writes == [25, 30]
        assert len(topic_map) == 30
        assert topic_map[1] == ["téma"]
        assert len(summary_map) == 30
        assert len(summary_en_map) == 30

    def test_single_final_flush_under_threshold(self, tmp_path: Path, monkeypatch) -> None:
        writes = _patch_classifier(monkeypatch)
        text_paths = {ct: tmp_path / f"{ct}.txt" for ct in range(1, 8)}

        classify_and_save(9, text_paths, tmp_path)

        assert writes == [7]

    def test_no_writes_when_nothing_to_process(self, tmp_path: Path, monkeypatch) -> None:
        writes = _patch_classifier(monkeypatch)

        classify_and_save(9, {}, tmp_path)

        assert writes == []

    def test_progress_callback_fires_per_item(self, tmp_path: Path, monkeypatch) -> None:
        _patch_classifier(monkeypatch)
        text_paths = {ct: tmp_path / f"{ct}.txt" for ct in range(1, 4)}
        callbacks: list[tuple[int, int]] = []

        classify_and_save(
            9, text_paths, tmp_path, progress_callback=lambda i, t: callbacks.append((i, t))
        )

        assert callbacks == [(1, 3), (2, 3), (3, 3)]
