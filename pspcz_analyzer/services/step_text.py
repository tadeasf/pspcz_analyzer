"""Legislative steps as sentences with a verb, and phase labels (#106, #108, #110, #111).

"30. 10. · Zákon dostal k podpisu prezident. Rozhodnout musí do 14. 11." instead of the
raw history outcome ("doručeno prezidentovi"). Sentences are keyed by the history stage
type and outcome (services/tisk/io/history_stages); the noun follows the document type
(návrh, smlouva, zpráva, interpelace) and a bill becomes "zákon" once the Chamber passed it.
"""

import re
from datetime import date, timedelta

from pspcz_analyzer.models.tisk_models import TiskInfo

PRESIDENT_DAYS = 15  # Ústava čl. 50: the President decides within 15 days of delivery

_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")

# document type (law_filters.TABS) -> (cs accusative, cs nominative, en)
_NOUNS: dict[str, tuple[str, str, str]] = {
    "navrhy": ("návrh", "Návrh", "bill"),
    "smlouvy": ("smlouvu", "Smlouva", "treaty"),
    "zpravy": ("zprávu", "Zpráva", "report"),
    "interpelace": ("interpelaci", "Interpelace", "interpellation"),
}
# stages after which a passed bill is called "zákon"
_LAW_STAGES = {"senat_postoupeno", "senat_organizacni", "senat_vybor", "senat", "prezident"}
_LAW_STAGES |= {"sbirka", "vyhlaseni"}
# wordings written for bills only
_BILL_ONLY = {"1_cteni", "2_cteni", "3_cteni", "garant", "prezident", "sbirka", "vyhlaseni"}

# (stage type, outcome) -> (cs, en); {a} = accusative noun, {N} = nominative, {e} = English
_SENTENCES: dict[tuple[str, str], tuple[str, str]] = {
    ("ps", ""): ("Poslanci dostali {a}.", "The {e} was sent to MPs."),
    ("ps", "schválen"): ("Sněmovna {a} schválila.", "The Chamber passed the {e}."),
    ("ps", "nepřijat"): ("Sněmovna {a} nepřijala.", "The Chamber did not pass the {e}."),
    ("ps", "setrvala na původním návrhu"): (
        "Sněmovna přehlasovala Senát a trvala na svém znění.",
        "The Chamber overrode the Senate and kept its version.",
    ),
    ("vlada", ""): ("Vláda k návrhu vydala stanovisko.", "The government gave its opinion."),
    ("vlada", "vzat zpět"): ("Navrhovatel {a} vzal zpět.", "The sponsor withdrew the {e}."),
    ("organizacni", ""): (
        "Organizační výbor určil zpravodaje a výbory.",
        "The Organising Committee named the rapporteur and committees.",
    ),
    ("1_cteni", ""): ("Proběhlo 1. čtení.", "First reading took place."),
    ("1_cteni", "přikázán výborům"): (
        "1. čtení: návrh poslán do výborů. Poslanci posuzují základní smysl návrhu.",
        "First reading: sent to committees. MPs judge the basic idea of the bill.",
    ),
    ("1_cteni", "schválen"): (
        "Sněmovna {a} schválila už v 1. čtení.",
        "The Chamber passed the {e} in first reading.",
    ),
    ("1_cteni", "vzat zpět"): ("Navrhovatel {a} vzal zpět.", "The sponsor withdrew the {e}."),
    ("1_cteni", "zamítnut"): ("Sněmovna {a} v 1. čtení zamítla.", "The Chamber rejected the {e}."),
    ("vybor", ""): ("Výbor {a} projednal.", "A committee discussed the {e}."),
    ("vybor", "doporučuje schválit"): (
        "Výbor doporučil {a} schválit.",
        "A committee recommended passing the {e}.",
    ),
    ("vybor", "neprojednal"): ("Výbor {a} neprojednal.", "A committee did not discuss the {e}."),
    ("vybor", "přerušeno"): ("Výbor projednávání přerušil.", "A committee adjourned the debate."),
    ("2_cteni", ""): (
        "2. čtení: podrobná debata, poslanci mohli podat pozměňovací návrhy.",
        "Second reading: detailed debate; MPs could file amendments.",
    ),
    ("2_cteni", "schválen"): ("Sněmovna {a} schválila.", "The Chamber passed the {e}."),
    ("garant", ""): (
        "Garanční výbor vydal stanovisko k pozměňovacím návrhům.",
        "The lead committee gave its opinion on the amendments.",
    ),
    ("garant", "neprojednal"): (
        "Garanční výbor {a} neprojednal.",
        "The lead committee did not discuss the {e}.",
    ),
    ("3_cteni", ""): ("Proběhlo 3. čtení.", "Third reading took place."),
    ("3_cteni", "schválen"): (
        "3. čtení: Sněmovna zákon schválila.",
        "Third reading: the Chamber passed the bill.",
    ),
    ("3_cteni", "zamítnut"): (
        "Sněmovna zákon ve 3. čtení neschválila.",
        "The Chamber did not pass the bill in third reading.",
    ),
    ("senat_postoupeno", ""): (
        "Sněmovna poslala {a} Senátu.",
        "The Chamber sent it to the Senate.",
    ),
    ("senat_postoupeno", "postoupeno Senátu"): (
        "Sněmovna poslala {a} Senátu.",
        "The Chamber sent it to the Senate.",
    ),
    ("senat_postoupeno", "předloženo Senátu"): (
        "Senát dostal {a} k projednání.",
        "The Senate received the {e}.",
    ),
    ("senat_organizacni", ""): (
        "Senát určil výbor, který {a} projedná.",
        "The Senate named the committee to discuss it.",
    ),
    ("senat_vybor", ""): ("Výbor Senátu {a} projednal.", "A Senate committee discussed it."),
    ("senat_vybor", "neprojednal"): (
        "Výbor Senátu {a} neprojednal.",
        "A Senate committee did not discuss it.",
    ),
    ("senat_vybor", "přerušeno"): (
        "Výbor Senátu projednávání přerušil.",
        "A Senate committee adjourned the debate.",
    ),
    ("senat", ""): ("Senát {a} projednal.", "The Senate discussed it."),
    ("senat", "schválil"): ("Senát {a} schválil.", "The Senate passed it."),
    ("senat", "vrátil"): (
        "Senát zákon vrátil Sněmovně s pozměňovacími návrhy.",
        "The Senate returned it to the Chamber with amendments.",
    ),
    ("senat", "zamítl"): (
        "Senát zákon zamítl, rozhodne znovu Sněmovna.",
        "The Senate rejected it; the Chamber votes again.",
    ),
    ("senat", "neprojednal"): (
        "Senát o zákonu nerozhodl. Když se Senát do 30 dnů nevyjádří, platí, že zákon přijal.",
        "The Senate did not decide. If it does not act within 30 days, the law counts as passed.",
    ),
    ("prezident", "doručeno prezidentovi"): (
        "Zákon dostal k podpisu prezident.",
        "The bill went to the President for signature.",
    ),
    ("prezident", "podepsal"): ("Prezident zákon podepsal.", "The President signed it."),
    ("prezident", "vrátil"): (
        "Prezident zákon vrátil Sněmovně.",
        "The President returned it to the Chamber.",
    ),
    ("sbirka", "odesláno k publikaci"): (
        "Zákon byl odeslán k vyhlášení ve Sbírce zákonů.",
        "The law was sent for publication in the Collection of Laws.",
    ),
    ("vyhlaseni", "vyhlášen"): (
        "Zákon vyšel ve Sbírce zákonů.",
        "The law was published in the Collection of Laws.",
    ),
}
# Who submitted a bill (law_filters.submitter_kind) -> "… předložil návrh."
_SUBMITTED: dict[str, tuple[str, str]] = {
    "vlada": ("Vláda předložila návrh.", "The government submitted the bill."),
    "poslanci": ("Poslanci předložili návrh.", "MPs submitted the bill."),
    "senat": ("Senát předložil návrh.", "The Senate submitted the bill."),
    "kraje": ("Kraj předložil návrh.", "A region submitted the bill."),
}
_SUBMITTED_RE = re.compile(r"rozeslán\w* (?:všem )?poslancům", re.I)

# Non-bill steps recognised from the stage text
_DETAIL_SENTENCES: tuple[tuple[re.Pattern[str], tuple[str, str]], ...] = (
    (
        re.compile(r"Senát dává souhlas s ratifikac", re.I),
        ("Senát dal souhlas s ratifikací.", "The Senate consented to ratification."),
    ),
    (
        re.compile(r"souhlas s ratifikac", re.I),
        ("Sněmovna vyslovila souhlas s ratifikací.", "The Chamber consented to ratification."),
    ),
    (
        re.compile(r"doručen prezidentovi", re.I),
        ("Smlouvu dostal k ratifikaci prezident.", "The treaty went to the President to ratify."),
    ),
    (
        re.compile(r"vzala? na vědomí", re.I),
        ("Sněmovna vzala {a} na vědomí.", "The Chamber noted the {e}."),
    ),
    (
        re.compile(r"Interpelace projednána.*Bez usnesení", re.I | re.S),
        ("Sněmovna interpelaci projednala bez usnesení.", "Debated without a resolution."),
    ),
    (
        re.compile(r"Interpelace projednána", re.I),
        ("Sněmovna interpelaci projednala.", "The Chamber debated the interpellation."),
    ),
)

# phase label keys ("phl.*" in translations), see phase_key
_STATUS_PHASES: dict[str, str] = {
    "vyhlášeno": "phl.valid",
    "vzato zpět": "phl.withdrawn",
    "zamítnuto": "phl.failed",
    "nepřijato": "phl.failed",
    "odesláno k vyhlášení": "phl.publishing",
    "podepsáno": "phl.publishing",
    "u prezidenta": "phl.president",
    "schváleno Senátem": "phl.president",
    "schváleno sněmovnou": "phl.senate",
    "v Senátu": "phl.senate",
    "vráceno Senátem": "phl.chamber_again",
    "zamítnuto Senátem": "phl.chamber_again",
    "vráceno prezidentem": "phl.chamber_again",
}
_CHAMBER_PHASES: dict[str, str] = {
    "ps": "phl.before_1st",
    "vlada": "phl.before_1st",
    "organizacni": "phl.before_1st",
    "1_cteni": "phl.committees",
    "vybor": "phl.committees",
    "2_cteni": "phl.before_3rd",
    "garant": "phl.before_3rd",
    "3_cteni": "phl.before_3rd",
}


def _date(value: str | None) -> date | None:
    m = _DATE_RE.search(value or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _nouns(tab: str) -> tuple[str, str, str]:
    return _NOUNS.get(tab, _NOUNS["zpravy"])


def step_sentence(
    stage_type: str,
    outcome: str,
    details: str,
    tab: str,
    lang: str = "cs",
    submitter: str | None = None,
    before_chamber: bool = False,
) -> str:
    """One history step as a sentence with a verb.

    Args:
        stage_type: History stage type ("1_cteni", "prezident", …).
        outcome: Stage outcome ("" when none).
        details: The stage's full text (used for non-bill documents).
        tab: Document type (law_filters.TABS).
        lang: "cs" or "en".
        submitter: Submitter kind of a bill (vlada/poslanci/senat/kraje), names the
            first step ("Vláda předložila návrh.").
        before_chamber: The step is a Senate bill's own Senate proceeding, before the
            Chamber got it, so it is still a "návrh", not a "zákon".

    Returns:
        The sentence; "" when the step has no known wording.
    """
    acc, nom, en = _nouns(tab)
    if tab == "navrhy" and stage_type == "ps" and submitter in _SUBMITTED:
        if _SUBMITTED_RE.search(details):
            return _SUBMITTED[submitter][lang == "en"]
    if tab != "navrhy":
        for pattern, pair in _DETAIL_SENTENCES:
            if pattern.search(details):
                return (pair[1] if lang == "en" else pair[0]).format(a=acc, N=nom, e=en)
    if tab != "navrhy" and stage_type in _BILL_ONLY:
        return ""  # readings of treaties and reports: psp.cz's own sentence is clearer
    if stage_type == "ps" and not outcome and not _SUBMITTED_RE.search(details):
        return ""  # other Chamber notes ("zařazen na pořad") have no fixed wording
    pair = _SENTENCES.get((stage_type, outcome or "")) or _SENTENCES.get((stage_type, ""))
    if not pair:
        return ""
    if tab == "navrhy" and stage_type in _LAW_STAGES and not before_chamber:
        acc, nom = "zákon", "Zákon"
    return (pair[1] if lang == "en" else pair[0]).format(a=acc, N=nom, e=en)


def before_chamber(tisk: TiskInfo, stage: object) -> bool:
    """Whether a Senate step comes before the Chamber got the bill (a Senate bill)."""
    stages = tisk.history.stages if tisk.history else []
    if not getattr(stage, "stage_type", "").startswith("senat"):
        return False
    for s in stages:
        if s is stage:
            return True
        if not s.stage_type.startswith("senat"):
            return False
    return False


def president_deadline(tisk: TiskInfo) -> date | None:
    """Last day the President can decide, when the law waits for him; else None."""
    stages = tisk.history.stages if tisk.history else []
    last = stages[-1] if stages else None
    if not last or last.stage_type != "prezident" or last.outcome != "doručeno prezidentovi":
        return None
    delivered = _date(last.details) or _date(last.date)
    return delivered + timedelta(days=PRESIDENT_DAYS) if delivered else None


def phase_key(tisk: TiskInfo) -> str:
    """Translation key of the phase label ("ve výborech", "platný zákon", …) of a bill."""
    history = tisk.history
    if history is None:
        return "phl.before_1st"
    if key := _STATUS_PHASES.get(history.current_status):
        return key
    chamber = [s for s in history.stages if s.stage_type in _CHAMBER_PHASES]
    return _CHAMBER_PHASES[chamber[-1].stage_type] if chamber else "phl.before_1st"
