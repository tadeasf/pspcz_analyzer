"""UNL table column definitions and Polars dtype schemas.

Column names match psp.cz documentation exactly (Czech names) for traceability.
UNL files have no headers — these lists define the column order.
"""

from typing import Any

import polars as pl

# Polars dtype classes (e.g. pl.Int64, pl.Int32) are type *classes*, not instances.
# Pylance expects dict[str, DataType] (instances) but we pass classes which Polars
# accepts at runtime. Using dict[str, Any] avoids false positives from Pylance.
PolarsSchemaDict = dict[str, Any]

# ---- Voting summary (hlXXXXs.unl) ----

HL_HLASOVANI_COLUMNS: list[str] = [
    "id_hlasovani",
    "id_organ",
    "schuze",
    "cislo",
    "bod",
    "datum",
    "cas",
    "pro",
    "proti",
    "zdrzel",
    "nehlasoval",
    "prihlaseno",
    "kvorum",
    "druh_hlasovani",
    "vysledek",
    "nazev_dlouhy",
    "nazev_kratky",
]

HL_HLASOVANI_DTYPES: PolarsSchemaDict = {
    "id_hlasovani": pl.Int64,
    "id_organ": pl.Int32,
    "schuze": pl.Int32,
    "cislo": pl.Int32,
    "bod": pl.Int32,
    "pro": pl.Int32,
    "proti": pl.Int32,
    "zdrzel": pl.Int32,
    "nehlasoval": pl.Int32,
    "prihlaseno": pl.Int32,
    "kvorum": pl.Int32,
}

# ---- Individual MP votes (hlXXXXhN.unl) ----

HL_POSLANEC_COLUMNS: list[str] = [
    "id_poslanec",
    "id_hlasovani",
    "vysledek",
]

HL_POSLANEC_DTYPES: PolarsSchemaDict = {
    "id_poslanec": pl.Int64,
    "id_hlasovani": pl.Int64,
}

# ---- Persons (osoby.unl from poslanci.zip) ----

OSOBY_COLUMNS: list[str] = [
    "id_osoba",
    "pred",
    "prijmeni",
    "jmeno",
    "za",
    "narozeni",
    "pohlavi",
    "zmena",
    "umrti",
]

OSOBY_DTYPES: PolarsSchemaDict = {
    "id_osoba": pl.Int64,
}

# ---- MPs (poslanec.unl from poslanci.zip) ----

POSLANEC_COLUMNS: list[str] = [
    "id_poslanec",
    "id_osoba",
    "id_kraj",
    "id_kandidatka",
    "id_obdobi",
    "web",
    "ulice",
    "obec",
    "psc",
    "email",
    "telefon",
    "fax",
    "psp_telefon",
    "foto",
    "facebook",
]

POSLANEC_DTYPES: PolarsSchemaDict = {
    "id_poslanec": pl.Int64,
    "id_osoba": pl.Int64,
    "id_kraj": pl.Int32,
    "id_kandidatka": pl.Int32,
    "id_obdobi": pl.Int32,
}

# ---- Organs / organizations (organy.unl from poslanci.zip) ----

ORGANY_COLUMNS: list[str] = [
    "id_organ",
    "organ_id_organ",
    "id_typ_organu",
    "zkratka",
    "nazev_organu_cz",
    "nazev_organu_en",
    "od_organ",
    "do_organ",
    "priorita",
    "cl_organ_base",
]

ORGANY_DTYPES: PolarsSchemaDict = {
    "id_organ": pl.Int32,
    "organ_id_organ": pl.Int32,
    "id_typ_organu": pl.Int32,
}

# ---- Memberships (zarazeni.unl from poslanci.zip) ----

ZARAZENI_COLUMNS: list[str] = [
    "id_osoba",
    "id_of",
    "cl_funkce",
    "od_o",
    "do_o",
    "od_f",
    "do_f",
]

ZARAZENI_DTYPES: PolarsSchemaDict = {
    "id_osoba": pl.Int64,
    "id_of": pl.Int32,
}

# ---- Void votes (zmatecne.unl from hl-XXXXps.zip) ----

ZMATECNE_COLUMNS: list[str] = [
    "id_hlasovani",
]

ZMATECNE_DTYPES: PolarsSchemaDict = {
    "id_hlasovani": pl.Int64,
}

# ---- Sessions (schuze.unl from schuze.zip) ----

SCHUZE_COLUMNS: list[str] = [
    "id_schuze",
    "id_org",
    "schuze",
    "od_schuze",
    "do_schuze",
    "aktualizace",
    "pozvanka",
]

SCHUZE_DTYPES: PolarsSchemaDict = {
    "id_schuze": pl.Int64,
    "id_org": pl.Int32,
    "schuze": pl.Int32,
}

# ---- Agenda items (bod_schuze.unl from schuze.zip) ----

BOD_SCHUZE_COLUMNS: list[str] = [
    "id_bod",
    "id_schuze",
    "id_tisk",
    "id_typ",
    "bod",
    "uplny_naz",
    "uplny_kon",
    "poznamka",
    "id_bod_stav",
    "pozvanka",
    "rj",
    "pozn2",
    "druh_bodu",
    "id_sd",
    "zkratka",
]

BOD_SCHUZE_DTYPES: PolarsSchemaDict = {
    "id_bod": pl.Int64,
    "id_schuze": pl.Int64,
    "id_tisk": pl.Int64,
    "id_typ": pl.Int32,
    "bod": pl.Int32,
    "id_bod_stav": pl.Int32,
    "id_sd": pl.Int64,
}

# ---- Parliamentary prints (tisky.unl from tisky.zip) ----

TISKY_COLUMNS: list[str] = [
    "id_tisk",
    "id_druh",
    "id_typ_zakon",
    "ct",
    "id_navrh",
    "id_predkladatel",
    "id_org",
    "id_obdobi",
    "id_navrhovatele",
    "predkladatel",
    "nazev_tisku",
    "datum_doruceni",
    "datum_rozeslani",
    "cas_doruceni",
    "lhuta",
    "id_stav",
    "nazev_cast",
    "popis",
    "dalsi_cast",
    "id_navrh_typ",
    "id_session",
    "id_poznamka",
    "url_tisk",
    "id_eklep",
]

TISKY_DTYPES: PolarsSchemaDict = {
    "id_tisk": pl.Int64,
    "id_druh": pl.Int32,
    "id_typ_zakon": pl.Int32,
    "ct": pl.Int32,
    "id_navrh": pl.Int32,
    "id_predkladatel": pl.Int32,
    "id_org": pl.Int32,
    "id_obdobi": pl.Int32,
    "id_navrhovatele": pl.Int64,
    "id_stav": pl.Int32,
}
