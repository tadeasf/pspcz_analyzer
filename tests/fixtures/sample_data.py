"""Factory functions for creating mock Polars DataFrames for testing."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData


def make_votes(n: int = 5) -> pl.DataFrame:
    """Create a votes DataFrame with n votes matching HL_HLASOVANI schema."""
    return pl.DataFrame(
        {
            "id_hlasovani": list(range(1, n + 1)),
            "id_organ": [165] * n,
            "schuze": [1] * n,
            "cislo": list(range(1, n + 1)),
            "bod": list(range(1, n + 1)),
            "datum": ["2024-01-01"] * n,
            "cas": ["10:00:00"] * n,
            "pro": [100] * n,
            "proti": [50] * n,
            "zdrzel": [10] * n,
            "nehlasoval": [20] * n,
            "prihlaseno": [180] * n,
            "kvorum": [90] * n,
            "druh_hlasovani": ["N"] * n,
            "vysledek": ["A"] * n,
            "nazev_dlouhy": [f"Test vote {i}" for i in range(1, n + 1)],
            "nazev_kratky": [f"TV{i}" for i in range(1, n + 1)],
        },
        schema={
            "id_hlasovani": pl.Int64,
            "id_organ": pl.Int32,
            "schuze": pl.Int32,
            "cislo": pl.Int32,
            "bod": pl.Int32,
            "datum": pl.Utf8,
            "cas": pl.Utf8,
            "pro": pl.Int32,
            "proti": pl.Int32,
            "zdrzel": pl.Int32,
            "nehlasoval": pl.Int32,
            "prihlaseno": pl.Int32,
            "kvorum": pl.Int32,
            "druh_hlasovani": pl.Utf8,
            "vysledek": pl.Utf8,
            "nazev_dlouhy": pl.Utf8,
            "nazev_kratky": pl.Utf8,
        },
    )


def make_mp_votes() -> pl.DataFrame:
    """Create MP votes for 3 MPs across 5 votes.

    MP 1 (ANO): loyal — always votes YES
    MP 2 (ANO): loyal — always votes YES
    MP 3 (ODS): rebel — votes NO on votes 1-3 (against ODS majority of YES),
                 but also has YES on votes 4-5

    MPs 4 and 6 (ODS): always YES — establish ODS majority as YES (2 YES > 1 NO)
    """
    records = []
    # MP 1 (ANO) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 1, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 2 (ANO) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 2, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 3 (ODS) - rebels on votes 1-3 (NO), loyal on 4-5 (YES)
    for vid in range(1, 4):
        records.append({"id_poslanec": 3, "id_hlasovani": vid, "vysledek": VoteResult.NO})
    for vid in range(4, 6):
        records.append({"id_poslanec": 3, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 4 (ODS) - all YES (establishes ODS majority direction with MP 6)
    for vid in range(1, 6):
        records.append({"id_poslanec": 4, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 6 (ODS) - all YES (2nd ODS member voting YES to ensure majority)
    for vid in range(1, 6):
        records.append({"id_poslanec": 6, "id_hlasovani": vid, "vysledek": VoteResult.YES})

    # Add attendance-testing records: MP 5 with varied results
    records.append({"id_poslanec": 5, "id_hlasovani": 1, "vysledek": VoteResult.YES})
    records.append({"id_poslanec": 5, "id_hlasovani": 2, "vysledek": VoteResult.ABSENT})
    records.append({"id_poslanec": 5, "id_hlasovani": 3, "vysledek": VoteResult.EXCUSED})
    records.append({"id_poslanec": 5, "id_hlasovani": 4, "vysledek": VoteResult.DID_NOT_VOTE})
    records.append({"id_poslanec": 5, "id_hlasovani": 5, "vysledek": VoteResult.ABSTAINED})

    return pl.DataFrame(
        records,
        schema={
            "id_poslanec": pl.Int64,
            "id_hlasovani": pl.Int64,
            "vysledek": pl.Utf8,
        },
    )


def make_mp_info() -> pl.DataFrame:
    """Create MP info for 6 MPs: 2 ANO, 3 ODS, 1 STAN."""
    return pl.DataFrame(
        {
            "id_poslanec": [1, 2, 3, 4, 5, 6],
            "id_osoba": [101, 102, 103, 104, 105, 106],
            "jmeno": ["Jan", "Petr", "Karel", "Ondřej", "Marie", "Tomáš"],
            "prijmeni": ["Novák", "Svoboda", "Dvořák", "Černý", "Nová", "Bílý"],
            "party": ["ANO", "ANO", "ODS", "ODS", "STAN", "ODS"],
        },
        schema={
            "id_poslanec": pl.Int64,
            "id_osoba": pl.Int64,
            "jmeno": pl.Utf8,
            "prijmeni": pl.Utf8,
            "party": pl.Utf8,
        },
    )


def make_void_votes() -> pl.DataFrame:
    """Create an empty void votes DataFrame."""
    return pl.DataFrame(
        {"id_hlasovani": pl.Series([], dtype=pl.Int64)},
    )


def make_period_data(period: int = 1) -> PeriodData:
    """Create a complete PeriodData fixture for testing."""
    return PeriodData(
        period=period,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        tisk_lookup={},
    )
