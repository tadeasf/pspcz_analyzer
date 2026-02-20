"""Topic classification for parliamentary prints (tisky) using keyword matching."""

from pspcz_analyzer.utils.text import normalize_czech

# Taxonomy: id -> (label_cs, label_en, [normalized keywords])
# Keywords are already normalized (no diacritics, lowercase)
TOPIC_TAXONOMY: dict[str, tuple[str, str, list[str]]] = {
    "finance": (
        "Finance a rozpočet",
        "Finance & Budget",
        [
            "rozpocet",
            "dan",
            "dane",
            "danovy",
            "financni",
            "statni rozpocet",
            "dph",
            "schodek",
            "deficit",
            "ucetnictvi",
            "bankovni",
            "banka",
            "poplatek",
            "uver",
            "dluhopis",
            "cel",
            "celni",
            "penzijni",
        ],
    ),
    "healthcare": (
        "Zdravotnictví",
        "Healthcare",
        [
            "zdravotni",
            "nemocnice",
            "lekar",
            "pacient",
            "pojisten",
            "zdravotnictvi",
            "lecba",
            "lecivo",
            "lek",
            "farmaceut",
            "epidemi",
            "pandemi",
            "ockovani",
            "hygiena",
            "zdravi",
        ],
    ),
    "education": (
        "Školství a vzdělávání",
        "Education",
        [
            "skolstvi",
            "skola",
            "vzdelavani",
            "ucitel",
            "student",
            "vysoka skola",
            "univerzit",
            "maturity",
            "ucebni",
            "stipend",
            "vyzkum",
            "veda",
            "akadem",
        ],
    ),
    "defense": (
        "Obrana a bezpečnost",
        "Defense & Security",
        [
            "obrana",
            "armada",
            "vojensk",
            "nato",
            "bezpecnost",
            "policie",
            "hasic",
            "krizov",
            "terorism",
            "zpravodajsk",
            "zbran",
            "vojak",
            "brannost",
        ],
    ),
    "justice": (
        "Spravedlnost a právo",
        "Justice & Law",
        [
            "soud",
            "soudni",
            "trestni",
            "zakon",
            "pravni",
            "ustavni",
            "advokatn",
            "exekuc",
            "insolvenc",
            "notarsk",
            "vezenstv",
            "kriminal",
            "pravo",
            "spravni rad",
        ],
    ),
    "environment": (
        "Životní prostředí",
        "Environment",
        [
            "zivotni prostredi",
            "ekolog",
            "emis",
            "klima",
            "odpad",
            "voda",
            "ovzdusi",
            "priroda",
            "ochrana prirody",
            "les",
            "narodni park",
            "krajin",
            "rekulti",
            "sucho",
            "povoden",
        ],
    ),
    "transport": (
        "Doprava",
        "Transport",
        [
            "doprav",
            "silnic",
            "dalnic",
            "zeleznic",
            "leteck",
            "autobus",
            "mhd",
            "ridic",
            "vozidl",
            "silnice",
            "most",
            "tunel",
            "infrastruktur",
        ],
    ),
    "social": (
        "Sociální politika",
        "Social Policy",
        [
            "socialn",
            "duchod",
            "duchodov",
            "invalidn",
            "sirotc",
            "davk",
            "hmotna nouze",
            "chudoba",
            "rodina",
            "dite",
            "detsk",
            "matersk",
            "rodicovsk",
            "opatrovnictv",
        ],
    ),
    "labor": (
        "Práce a zaměstnanost",
        "Labor & Employment",
        [
            "zamestnan",
            "prace",
            "pracovni",
            "mzda",
            "plat",
            "odbor",
            "nezamestnanost",
            "bezpecnost prace",
            "urad prace",
            "podnikani",
            "zivnostensk",
        ],
    ),
    "eu": (
        "Evropská unie",
        "European Union",
        [
            "evropsk",
            "eu",
            "unie",
            "smernice",
            "narizeni eu",
            "schengen",
            "eurozony",
            "fondy eu",
            "predsednictv",
        ],
    ),
    "foreign": (
        "Zahraniční politika",
        "Foreign Policy",
        [
            "zahranicn",
            "mezinarodn",
            "smlouva",
            "diplomat",
            "ambasad",
            "migrac",
            "azyl",
            "uprchl",
            "viza",
            "konzularn",
        ],
    ),
    "housing": (
        "Bydlení a stavebnictví",
        "Housing & Construction",
        [
            "bydlen",
            "staveb",
            "stavba",
            "nemovitost",
            "byt",
            "najem",
            "hypote",
            "katastr",
            "uzemni plan",
            "stavebn",
            "bytov",
        ],
    ),
    "agriculture": (
        "Zemědělství",
        "Agriculture",
        [
            "zemedelstv",
            "zemedelec",
            "farmaf",
            "potravin",
            "veterinar",
            "rostlin",
            "dotace",
            "hospodarstv",
            "rybarstvi",
            "zvire",
            "chov",
        ],
    ),
    "digital": (
        "Digitalizace a IT",
        "Digital & IT",
        [
            "digit",
            "elektronick",
            "kybernetick",
            "internet",
            "informacn",
            "datov",
            "egovernment",
            "egov",
            "telekomunikac",
            "sit",
        ],
    ),
    "constitutional": (
        "Ústavní a procesní",
        "Constitutional & Procedural",
        [
            "ustav",
            "ustavni",
            "referendum",
            "voleb",
            "volebni",
            "mandatov",
            "imunit",
            "jednaci rad",
            "poslanec",
            "senat",
            "prezident",
        ],
    ),
    "procedural": (
        "Procedurální",
        "Procedural",
        [
            "proceduraln",
            "jednaci",
            "hlasovani o",
            "schvaleni programu",
            "preruseni",
            "zahajeni schuze",
            "ukonceni",
            "bod poradu",
        ],
    ),
}


def classify_tisk(text: str, title: str) -> list[tuple[str, int]]:
    """Classify a tisk by matching normalized text against topic keywords.

    Returns list of (topic_id, match_count) sorted by match count descending.
    """
    normalized = normalize_czech(f"{title} {text}")
    results: list[tuple[str, int]] = []

    for topic_id, (_label_cs, _label_en, keywords) in TOPIC_TAXONOMY.items():
        count = sum(1 for kw in keywords if kw in normalized)
        if count > 0:
            results.append((topic_id, count))

    results.sort(key=lambda x: x[1], reverse=True)
    return results


def classify_tisk_primary_label(text: str, title: str) -> tuple[str, str] | None:
    """Return (label_cs, label_en) of the best-matching topic, or None."""
    results = classify_tisk(text, title)
    if not results:
        return None
    topic_id = results[0][0]
    label_cs, label_en, _ = TOPIC_TAXONOMY[topic_id]
    return label_cs, label_en
