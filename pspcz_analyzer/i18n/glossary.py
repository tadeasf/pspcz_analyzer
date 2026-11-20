"""Glossary of the guide "Jak se v tom vyznat" (#118), the one source of term explanations.

The guide renders these texts and every other page shows them as explanations of terms
(#106) through the ``term`` macro, so a text is kept in one place only. Texts are taken
verbatim from ``jak-se-v-tom-vyznat.md`` (docs/ux-rework).
"""

import re

_TAG_RE = re.compile(r"<[^>]+>")

# slug -> lang -> (term, explanation HTML); GLOSSARY_ORDER is the order on the guide page
GLOSSARY: dict[str, dict[str, tuple[str, str]]] = {
    "snemovni-tisk": {
        "cs": (
            "Sněmovní tisk",
            "Číslovaný písemný podklad pro jednání Sněmovny v daném volebním období. Návrhem zákona je jen část tisků, další jsou například usnesení výborů, zprávy, mezinárodní smlouvy nebo písemné interpelace, které Sněmovna projednává.",
        ),
        "en": (
            "Chamber print (sněmovní tisk)",
            "A numbered written document for the Chamber's proceedings in a given electoral period. Only some prints are bills; others are, for example, committee resolutions, reports, international treaties or written interpellations the Chamber discusses.",
        ),
    },
    "navrhovatel": {
        "cs": (
            "Navrhovatel",
            "Autor návrhu. U vládního návrhu ho ve Sněmovně hájí pověřený člen vlády.",
        ),
        "en": (
            "Sponsor",
            "The author of the bill. A government bill is defended in the Chamber by an appointed member of the government.",
        ),
    },
    "zpravodaj": {
        "cs": (
            "Zpravodaj",
            "Poslanec, který Sněmovnu provází projednáváním návrhu, shrnuje práci výboru a ve 3. čtení oznamuje pořadí hlasování.",
        ),
        "en": (
            "Rapporteur",
            "The MP who guides the Chamber through the bill, summarises the committee's work and announces the voting order in the 3rd reading.",
        ),
    },
    "proceduralni-hlasovani": {
        "cs": (
            "Procedurální hlasování",
            "Hlasování o průběhu jednání, ne o obsahu zákona. Například o pořadu schůze, délce projevů, přestávce nebo o námitce.",
        ),
        "en": (
            "Procedural vote",
            "A vote on how the sitting proceeds, not on the content of the law. For example on the agenda, the length of speeches, a break or an objection.",
        ),
    },
    "omluven-neprihlasen": {
        "cs": (
            "Omluven, nepřihlášen, zdržel se",
            "Omluvený poslanec nahlásil nepřítomnost; omluvy psp.cz někdy doplňuje se zpožděním. Nepřihlášený nebyl při hlasování přihlášen kartou. Kdo se zdržel, byl přihlášen, ale nehlasoval pro ani proti.",
        ),
        "en": (
            "Excused, not logged in, abstained",
            "An excused MP reported their absence; psp.cz sometimes adds excuses with a delay. An MP not logged in was not logged in with their card during the vote. An MP who abstained was logged in but voted neither for nor against.",
        ),
    },
    "zpochybneni": {
        "cs": (
            "Zpochybnění hlasování",
            "Námitka proti průběhu nebo výsledku hlasování, například když zařízení poslanci zaznamenalo jiný hlas. Sněmovna o ní hlasuje bez debaty. Když jí vyhoví, hlasuje se znovu a platí nový výsledek.",
        ),
        "en": (
            "Challenged vote",
            "An objection to the course or result of a vote, for example when the device recorded a different vote for an MP. The Chamber votes on it without debate. If it upholds it, the vote is repeated and the new result applies.",
        ),
    },
    "zmatecne": {
        "cs": (
            "Zmatečné hlasování",
            "Hlasování, které bylo prohlášeno za zmatečné, obvykle kvůli technické chybě. K jeho výsledku se nepřihlíží a zpravidla se opakuje.",
        ),
        "en": (
            "Void vote",
            "A vote declared void, usually because of a technical error. Its result is disregarded and it is usually repeated.",
        ),
    },
    "namitka": {
        "cs": (
            "Námitka proti postupu předsedajícího",
            "Poslanec nesouhlasí s tím, jak předsedající řídí jednání. Rozhodne o ní Sněmovna hlasováním.",
        ),
        "en": (
            "Objection to the chair's conduct",
            "An MP disagrees with how the chair is running the sitting. The Chamber decides on it by a vote.",
        ),
    },
    "revokace": {
        "cs": (
            "Revokace",
            "Zrušení dříve přijatého rozhodnutí. U pozměňovacích a některých dalších návrhů ji smí navrhnout jen poslanec, který pro ně hlasoval. Zamítnutí návrhu revokovat nelze.",
        ),
        "en": (
            "Revocation",
            "Cancelling an earlier decision. For amendments and some other motions only an MP who voted for them may propose it. A rejection of a bill cannot be revoked.",
        ),
    },
    "pevny-cas": {
        "cs": (
            "Pevný čas hlasování",
            "Sněmovna určí, kdy se o návrhu bude hlasovat. V tu chvíli debata skončí, i když jsou přihlášení další řečníci. Jen ve zcela výjimečných případech, a když poslanci měli dost času se s věcí seznámit a vyjádřit se.",
        ),
        "en": (
            "Fixed voting time",
            "The Chamber sets when a bill will be voted on. At that moment the debate ends even if more speakers are signed up. Only in entirely exceptional cases, and when MPs have had enough time to study and comment on the matter.",
        ),
    },
    "recnicka-doba": {
        "cs": (
            "Omezení řečnické doby",
            "Sněmovna může zkrátit délku projevů, nejméně na 10 minut, a omezit počet vystoupení na dvě. Týká se i zpravodaje, řečníka za klub a řečníků s přednostním právem; jejich první projev v rozpravě může trvat nejvýš 30 minut.",
        ),
        "en": (
            "Limit on speaking time",
            "The Chamber can shorten speeches, to no less than 10 minutes, and limit the number of contributions to two. This also applies to the rapporteur, a speaker for a club and speakers with priority; their first speech in the debate may last at most 30 minutes.",
        ),
    },
    "prednostni-pravo": {
        "cs": (
            "Přednostní právo",
            "Právo dostat slovo kdykoli. Má ho prezident, členové vlády, vedení Sněmovny, předsedové klubů a předsedové stran, které mají ve Sněmovně klub ustavený na začátku volebního období.",
        ),
        "en": (
            "Priority right to speak",
            "The right to take the floor at any time. It belongs to the President, members of the government, the Chamber's leadership, club chairs and chairs of parties that have a club formed at the start of the electoral period.",
        ),
    },
    "fakticka-poznamka": {
        "cs": (
            "Faktická poznámka",
            "Krátká reakce, nejvýš 2 minuty, na vystoupení jiného řečníka k projednávané věci.",
        ),
        "en": (
            "Factual remark",
            "A short reaction, at most 2 minutes, to another speaker's contribution on the matter under discussion.",
        ),
    },
    "poslanecky-klub": {
        "cs": (
            "Poslanecký klub",
            "Poslanci zvolení za stejnou stranu, kteří ve Sněmovně vystupují společně. Na začátku volebního období potřebuje klub aspoň 3 členy, nový klub během období aspoň 10.",
        ),
        "en": (
            "Parliamentary club",
            "MPs elected for the same party who act together in the Chamber. At the start of the electoral period a club needs at least 3 members, a new club formed during the period at least 10.",
        ),
    },
    "cteni1": {
        "cs": (
            "1. čtení",
            "Debata o smyslu návrhu. Sněmovna ho zamítne, vrátí k dopracování, nebo pošle do výborů. Výjimečně ho může rovnou schválit.",
        ),
        "en": (
            "1st reading",
            "A debate on the purpose of the bill. The Chamber rejects it, returns it for redrafting, or sends it to committees. In exceptional cases it can pass it straight away.",
        ),
    },
    "cteni2": {
        "cs": (
            "2. čtení",
            "Obecná a podrobná debata. V podrobné části poslanci podávají pozměňovací návrhy.",
        ),
        "en": (
            "2nd reading",
            "General and detailed debate. In the detailed part MPs table amendments.",
        ),
    },
    "cteni3": {
        "cs": (
            "3. čtení",
            "Hlasuje se o případném zamítnutí, o pozměňovacích návrzích a nakonec o zákonu jako celku.",
        ),
        "en": (
            "3rd reading",
            "Votes on a possible rejection, on the amendments and finally on the bill as a whole.",
        ),
    },
    "garancni-vybor": {
        "cs": (
            "Garanční výbor",
            "Garanční výbor určuje Sněmovna na konci 1. čtení a má návrh na starosti až do 3. čtení. Doporučí Sněmovně, jestli návrh schválit, a může navrhnout změny. Po 2. čtení se zpravidla vyjádří ke všem pozměňovacím návrhům a navrhne, v jakém pořadí se o nich bude hlasovat. K návrhu se mohou vyjádřit i další výbory.",
        ),
        "en": (
            "Lead committee",
            "The <b>lead committee</b> is chosen by the Chamber at the end of the 1st reading and is in charge of the bill until the 3rd reading. It recommends whether to pass the bill and can propose changes. After the 2nd reading it usually gives its view on all amendments and proposes the order in which they will be voted on. Other committees may also comment on the bill.",
        ),
    },
}
GLOSSARY_ORDER: tuple[str, ...] = (
    "snemovni-tisk",
    "navrhovatel",
    "zpravodaj",
    "proceduralni-hlasovani",
    "omluven-neprihlasen",
    "zpochybneni",
    "zmatecne",
    "namitka",
    "revokace",
    "pevny-cas",
    "recnicka-doba",
    "prednostni-pravo",
    "fakticka-poznamka",
    "poslanecky-klub",
)


def term_text(slug: str, lang: str) -> tuple[str, str]:
    """``(term, plain-text explanation)`` of a glossary entry in a language ("" if unknown)."""
    entry = GLOSSARY.get(slug, {})
    term, text = entry.get(lang) or entry.get("cs") or ("", "")
    return term, _TAG_RE.sub("", text)
