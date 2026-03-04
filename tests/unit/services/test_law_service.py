"""Tests for the law listing and detail service."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.law_service import (
    get_all_status_labels,
    law_detail,
    list_laws,
)
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory
from tests.fixtures.sample_data import (
    make_mp_info,
    make_mp_votes,
    make_void_votes,
    make_votes,
)


def _make_tisk(
    ct: int,
    nazev: str,
    status: str = "projednáváno",
    topics: list[str] | None = None,
    topics_en: list[str] | None = None,
) -> TiskInfo:
    """Create a test TiskInfo with optional history."""
    history = TiskHistory(ct=ct, period=10, current_status=status, submitter="Vláda")
    return TiskInfo(
        id_tisk=ct * 10,
        ct=ct,
        nazev=nazev,
        period=10,
        topics=topics or ["Ekonomika", "Finance"],
        topics_en=topics_en or ["Economy", "Finance"],
        summary="Shrnutí zákona.",
        summary_en="Bill summary.",
        history=history,
    )


def _make_bill(schuze: int, bod: int, ct: int) -> BillAmendmentData:
    """Create a test BillAmendmentData."""
    amendments = [
        AmendmentVote(letter="A", vote_number=100, id_hlasovani=1000, result="accepted"),
        AmendmentVote(letter="B", vote_number=101, id_hlasovani=1001, result="rejected"),
    ]
    final = AmendmentVote(
        letter="", vote_number=102, id_hlasovani=1002, result="accepted", is_final_vote=True
    )
    return BillAmendmentData(
        period=10,
        schuze=schuze,
        bod=bod,
        ct=ct,
        tisk_nazev=f"Tisk {ct}",
        amendments=amendments,
        final_vote=final,
    )


def _make_data_with_laws() -> PeriodData:
    """Create PeriodData with tisk_lookup and amendment_data."""
    tisk_200 = _make_tisk(
        200,
        "Zákon o daních",
        "vyhlášeno",
        topics=["Ekonomika", "Finance"],
        topics_en=["Economy", "Finance"],
    )
    tisk_300 = _make_tisk(
        300, "Zákon o vzdělávání", "zamítnuto", topics=["Školství"], topics_en=["Education"]
    )
    tisk_400 = _make_tisk(
        400,
        "Zákon o zdravotnictví",
        "projednáváno",
        topics=["Zdravotnictví", "Finance"],
        topics_en=["Healthcare", "Finance"],
    )

    return PeriodData(
        period=10,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        tisk_lookup={
            (78, 1): tisk_200,
            (78, 2): tisk_300,
            (79, 1): tisk_400,
        },
        amendment_data={
            (78, 1): _make_bill(78, 1, 200),
        },
    )


class TestListLaws:
    def test_returns_pagination_dict(self):
        data = _make_data_with_laws()
        result = list_laws(data)
        assert isinstance(result, dict)
        assert "rows" in result
        assert "total" in result
        assert "page" in result
        assert "per_page" in result
        assert "total_pages" in result

    def test_total_matches_unique_tisky(self):
        data = _make_data_with_laws()
        result = list_laws(data)
        assert result["total"] == 3

    def test_search_filters_by_name(self):
        data = _make_data_with_laws()
        result = list_laws(data, search="daních")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 200

    def test_search_case_insensitive(self):
        data = _make_data_with_laws()
        result = list_laws(data, search="ZÁKON")
        assert result["total"] == 3

    def test_status_filter_exact_match(self):
        data = _make_data_with_laws()
        result = list_laws(data, status_filter="vyhlášeno")
        assert result["total"] == 1
        assert result["rows"][0]["status"] == "vyhlášeno"

    def test_status_filter_zamítnuto(self):
        data = _make_data_with_laws()
        result = list_laws(data, status_filter="zamítnuto")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 300

    def test_status_filter_projednáváno(self):
        data = _make_data_with_laws()
        result = list_laws(data, status_filter="projednáváno")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 400

    def test_status_filter_empty_returns_all(self):
        data = _make_data_with_laws()
        result = list_laws(data, status_filter="")
        assert result["total"] == 3

    def test_topic_filter_cs(self):
        data = _make_data_with_laws()
        result = list_laws(data, topic_filter="Školství")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 300

    def test_topic_filter_en(self):
        data = _make_data_with_laws()
        result = list_laws(data, topic_filter="Healthcare", lang="en")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 400

    def test_topic_filter_shared_topic(self):
        data = _make_data_with_laws()
        result = list_laws(data, topic_filter="Finance")
        assert result["total"] == 2

    def test_topic_filter_empty_returns_all(self):
        data = _make_data_with_laws()
        result = list_laws(data, topic_filter="")
        assert result["total"] == 3

    def test_combined_filters(self):
        data = _make_data_with_laws()
        result = list_laws(data, topic_filter="Finance", status_filter="vyhlášeno")
        assert result["total"] == 1
        assert result["rows"][0]["ct"] == 200

    def test_pagination(self):
        data = _make_data_with_laws()
        result = list_laws(data, per_page=2, page=1)
        assert len(result["rows"]) == 2
        assert result["total_pages"] == 2

    def test_row_has_expected_fields(self):
        data = _make_data_with_laws()
        result = list_laws(data)
        row = result["rows"][0]
        assert "ct" in row
        assert "nazev" in row
        assert "topics" in row
        assert "submitter" in row
        assert "status" in row
        assert "has_amendments" in row

    def test_amendment_info_present(self):
        data = _make_data_with_laws()
        result = list_laws(data)
        # ct=200 has amendments
        ct200_row = next(r for r in result["rows"] if r["ct"] == 200)
        assert ct200_row["has_amendments"] is True
        assert ct200_row["amendment_count"] == 2

    def test_lang_en_uses_english_topics(self):
        data = _make_data_with_laws()
        result = list_laws(data, lang="en")
        row = result["rows"][0]
        assert (
            "Economy" in row["topics"]
            or "Finance" in row["topics"]
            or "Education" in row["topics"]
            or "Healthcare" in row["topics"]
        )

    def test_empty_tisk_lookup(self):
        data = PeriodData(
            period=10,
            votes=make_votes(),
            mp_votes=make_mp_votes(),
            void_votes=make_void_votes(),
            mp_info=make_mp_info(),
        )
        result = list_laws(data)
        assert result["total"] == 0
        assert result["rows"] == []

    def test_deduplicates_by_ct(self):
        """Multiple (schuze, bod) for same ct should yield one row."""
        tisk = _make_tisk(500, "Zákon duplicitní")
        data = PeriodData(
            period=10,
            votes=make_votes(),
            mp_votes=make_mp_votes(),
            void_votes=make_void_votes(),
            mp_info=make_mp_info(),
            tisk_lookup={
                (78, 1): tisk,
                (78, 2): tisk,
                (79, 1): tisk,
            },
        )
        result = list_laws(data)
        assert result["total"] == 1


class TestGetAllStatusLabels:
    def test_returns_sorted_unique(self):
        data = _make_data_with_laws()
        labels = get_all_status_labels(data)
        assert isinstance(labels, list)
        assert labels == sorted(labels)
        assert len(labels) == len(set(labels))

    def test_contains_expected_statuses(self):
        data = _make_data_with_laws()
        labels = get_all_status_labels(data)
        assert "vyhlášeno" in labels
        assert "zamítnuto" in labels
        assert "projednáváno" in labels

    def test_empty_tisk_lookup(self):
        data = PeriodData(
            period=10,
            votes=make_votes(),
            mp_votes=make_mp_votes(),
            void_votes=make_void_votes(),
            mp_info=make_mp_info(),
        )
        labels = get_all_status_labels(data)
        assert labels == []


class TestLawDetail:
    def test_returns_dict_for_existing_ct(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200)
        assert isinstance(result, dict)
        assert result["ct"] == 200

    def test_not_found_returns_none(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=999)
        assert result is None

    def test_contains_bill_info(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200)
        assert result is not None
        assert result["nazev"] == "Zákon o daních"
        assert result["status"] == "vyhlášeno"
        assert result["submitter"] == "Vláda"

    def test_contains_summary(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200)
        assert result is not None
        assert result["summary"] == "Shrnutí zákona."

    def test_english_summary(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200, lang="en")
        assert result is not None
        assert result["summary"] == "Bill summary."

    def test_contains_amendment_entries(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200)
        assert result is not None
        assert result["has_amendments"] is True
        assert len(result["amendment_entries"]) == 1
        assert result["amendment_entries"][0]["schuze"] == 78

    def test_no_amendments_for_bill(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=300)
        assert result is not None
        assert result["has_amendments"] is False
        assert len(result["amendment_entries"]) == 0

    def test_contains_topics(self):
        data = _make_data_with_laws()
        result = law_detail(data, ct=200)
        assert result is not None
        assert "Ekonomika" in result["topics"]
