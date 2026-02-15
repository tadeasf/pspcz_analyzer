"""Integration tests: parse real UNL files, verify schema compatibility."""

import pytest

from pspcz_analyzer.data.parser import parse_unl
from pspcz_analyzer.models.schemas import (
    HL_HLASOVANI_COLUMNS,
    HL_HLASOVANI_DTYPES,
    HL_POSLANEC_COLUMNS,
    HL_POSLANEC_DTYPES,
    ORGANY_COLUMNS,
    ORGANY_DTYPES,
    OSOBY_COLUMNS,
    OSOBY_DTYPES,
    POSLANEC_COLUMNS,
    POSLANEC_DTYPES,
    ZARAZENI_COLUMNS,
    ZARAZENI_DTYPES,
)

pytestmark = pytest.mark.integration


class TestVotingParsing:
    def test_hl_hlasovani_schema(self, voting_dir_period1):
        """hl1993s.unl should parse with our HL_HLASOVANI schema."""
        f = next(voting_dir_period1.rglob("hl1993s.unl"))
        df = parse_unl(f, HL_HLASOVANI_COLUMNS, HL_HLASOVANI_DTYPES)
        assert df.height > 0
        assert set(HL_HLASOVANI_COLUMNS).issubset(set(df.columns))

    def test_hl_hlasovani_has_valid_ids(self, voting_dir_period1):
        """Parsed voting data should have non-null id_hlasovani."""
        f = next(voting_dir_period1.rglob("hl1993s.unl"))
        df = parse_unl(f, HL_HLASOVANI_COLUMNS, HL_HLASOVANI_DTYPES)
        null_count = df["id_hlasovani"].null_count()
        assert null_count == 0

    def test_hl_poslanec_schema(self, voting_dir_period1):
        """MP vote files should parse with our HL_POSLANEC schema."""
        f = next(voting_dir_period1.rglob("hl1993h*.unl"))
        df = parse_unl(f, HL_POSLANEC_COLUMNS, HL_POSLANEC_DTYPES)
        assert df.height > 0
        assert "id_poslanec" in df.columns
        assert "id_hlasovani" in df.columns
        assert "vysledek" in df.columns

    def test_reasonable_row_count(self, voting_dir_period1):
        """Period 1 should have a reasonable number of votes."""
        f = next(voting_dir_period1.rglob("hl1993s.unl"))
        df = parse_unl(f, HL_HLASOVANI_COLUMNS, HL_HLASOVANI_DTYPES)
        # Period 1 (1993-1996) should have hundreds to thousands of votes
        assert df.height > 100


class TestPoslanciParsing:
    def test_osoby_schema(self, poslanci_dir):
        """osoby.unl should parse with our OSOBY schema."""
        f = next(poslanci_dir.rglob("osoby.unl"))
        df = parse_unl(f, OSOBY_COLUMNS, OSOBY_DTYPES)
        assert df.height > 0
        assert df["id_osoba"].null_count() == 0

    def test_poslanec_schema(self, poslanci_dir):
        """poslanec.unl should parse with our POSLANEC schema."""
        f = next(poslanci_dir.rglob("poslanec.unl"))
        df = parse_unl(f, POSLANEC_COLUMNS, POSLANEC_DTYPES)
        assert df.height > 0
        assert df["id_poslanec"].null_count() == 0

    def test_organy_schema(self, poslanci_dir):
        """organy.unl should parse with our ORGANY schema."""
        f = next(poslanci_dir.rglob("organy.unl"))
        df = parse_unl(f, ORGANY_COLUMNS, ORGANY_DTYPES)
        assert df.height > 0

    def test_zarazeni_schema(self, poslanci_dir):
        """zarazeni.unl should parse with our ZARAZENI schema."""
        f = next(poslanci_dir.rglob("zarazeni.unl"))
        df = parse_unl(f, ZARAZENI_COLUMNS, ZARAZENI_DTYPES)
        assert df.height > 0

    def test_no_all_null_columns(self, poslanci_dir):
        """Schema drift canary: parsed osoby shouldn't have all-null typed columns."""
        f = next(poslanci_dir.rglob("osoby.unl"))
        df = parse_unl(f, OSOBY_COLUMNS, OSOBY_DTYPES)
        for col in OSOBY_DTYPES:
            assert df[col].null_count() < df.height, f"Column {col} is entirely null"
