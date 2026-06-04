"""Tests for government vs opposition classification."""

from pspcz_analyzer.services.affiliation import (
    GOVERNMENT,
    MIXED,
    OPPOSITION,
    UNKNOWN,
    amendment_driver,
    classify_party,
)

# Period-10-style coalition for tests
COALITION = frozenset({"ANO", "SPD", "MS"})


class TestClassifyParty:
    def test_government_party(self):
        assert classify_party("ANO", COALITION) == GOVERNMENT
        assert classify_party("SPD", COALITION) == GOVERNMENT
        assert classify_party("MS", COALITION) == GOVERNMENT

    def test_opposition_party(self):
        assert classify_party("ODS", COALITION) == OPPOSITION
        assert classify_party("Piráti", COALITION) == OPPOSITION
        assert classify_party("STAN", COALITION) == OPPOSITION

    def test_independent_is_unknown(self):
        assert classify_party("Nezařazení", COALITION) == UNKNOWN
        assert classify_party("Nezařaz", COALITION) == UNKNOWN

    def test_empty_party_is_unknown(self):
        assert classify_party("", COALITION) == UNKNOWN

    def test_empty_coalition_is_unknown(self):
        # Period not detected → coalition empty → everything unknown
        assert classify_party("ANO", frozenset()) == UNKNOWN
        assert classify_party("ODS", frozenset()) == UNKNOWN


class TestAmendmentDriver:
    def test_all_government(self):
        assert amendment_driver(["ANO", "SPD"], COALITION) == GOVERNMENT

    def test_all_opposition(self):
        assert amendment_driver(["ODS", "Piráti"], COALITION) == OPPOSITION

    def test_mixed(self):
        assert amendment_driver(["ANO", "ODS"], COALITION) == MIXED

    def test_unknown_parties_ignored(self):
        assert amendment_driver(["ANO", "Nezařazení"], COALITION) == GOVERNMENT

    def test_no_classifiable_parties(self):
        assert amendment_driver(["Nezařazení", ""], COALITION) == UNKNOWN

    def test_empty_list(self):
        assert amendment_driver([], COALITION) == UNKNOWN

    def test_empty_coalition(self):
        assert amendment_driver(["ANO", "ODS"], frozenset()) == UNKNOWN
