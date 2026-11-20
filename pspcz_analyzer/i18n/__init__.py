"""Internationalization support for Czech/English UI localization."""

import contextvars
from datetime import date, datetime

from jinja2 import Environment

from pspcz_analyzer.i18n.glossary import GLOSSARY, GLOSSARY_ORDER, term_text
from pspcz_analyzer.i18n.translations import TRANSLATIONS

SUPPORTED_LANGUAGES: tuple[str, ...] = ("cs", "en")
DEFAULT_LANGUAGE: str = "cs"

_locale_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "locale", default=DEFAULT_LANGUAGE
)


def get_locale() -> str:
    """Return the current request's locale from the ContextVar."""
    return _locale_var.get()


def set_locale(lang: str) -> None:
    """Set the current request's locale in the ContextVar."""
    _locale_var.set(lang)


def gettext(key: str) -> str:
    """Look up a translated string for the current locale.

    Falls back to the key itself if no translation is found.
    """
    locale = get_locale()
    return TRANSLATIONS.get(locale, {}).get(key, key)


def ngettext(singular: str, plural: str, n: int) -> str:
    """Simple plural-aware translation lookup."""
    key = singular if n == 1 else plural
    return gettext(key)


def format_int(value: int) -> str:
    """Format an integer with locale-appropriate thousands grouping.

    Czech uses non-breaking spaces (1 234 567), English uses commas (1,234,567).
    """
    grouped = f"{int(value):,}"
    if get_locale() == "cs":
        return grouped.replace(",", " ")
    return grouped


def format_number(value: float | int | None, decimals: int = 0) -> str:
    """Format a number the site's way: "1 377" / "43,5" (cs) or "1,377" / "43.5" (en) (#106).

    Args:
        value: Number to format (None renders as an empty string).
        decimals: Digits after the decimal point.

    Returns:
        Locale-formatted number.
    """
    if value is None:
        return ""
    text = f"{value:,.{decimals}f}"
    if get_locale() == "cs":
        return text.replace(",", "\u00a0").replace(".", ",")
    return text


def format_percent(value: float | None, decimals: int = 1) -> str:
    """Format a percentage value (0-100): "43,0 %" (cs) or "43.0%" (en).

    Args:
        value: Percentage (not a fraction); None renders as an empty string.
        decimals: Digits after the decimal point.

    Returns:
        Locale-formatted percentage.
    """
    if value is None:
        return ""
    sep = "\u00a0" if get_locale() == "cs" else ""
    return f"{format_number(value, decimals)}{sep}%"


def _to_date(value: object) -> date | None:
    """Parse a date from a date/datetime, "dd.mm.yyyy" or ISO "yyyy-mm-dd" string."""
    if isinstance(value, date):
        return value
    text = "".join(str(value or "").split())[:10]  # "18. 11. 2026" -> "18.11.2026"
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def format_date_cz(value: object, short: bool = False) -> str:
    """Format a date the site's way: "23. 9. 2026" or "23. 9." (cs), "Sep 23, 2026" (en).

    Args:
        value: Date, datetime or date string; unparseable input is returned as is.
        short: Omit the year (within the current year).

    Returns:
        Locale-formatted date.
    """
    d = _to_date(value)
    if d is None:
        return str(value or "")
    if get_locale() == "cs":
        return f"{d.day}. {d.month}." if short else f"{d.day}. {d.month}. {d.year}"
    return d.strftime("%b %-d") if short else d.strftime("%b %-d, %Y")


def format_count(n: int, base: str) -> str:
    """Number with the noun in the right plural form, e.g. "3 pozměňovací návrhy".

    Args:
        n: The count.
        base: Translation key prefix; keys base_one, base_few and base_many
            hold "{n} ..." templates (Czech 1 / 2-4 / other).

    Returns:
        The formatted phrase.
    """
    match n:
        case 1:
            form = "_one"
        case 2 | 3 | 4:
            form = "_few"
        case _:
            form = "_many"
    return gettext(base + form).format(n=format_number(n))


def gloss(slug: str) -> tuple[str, str]:
    """``(term, plain explanation)`` of a glossary entry in the current language (#106, #118)."""
    return term_text(slug, get_locale())


def gloss_entry(slug: str) -> tuple[str, str]:
    """``(term, explanation HTML)`` of a glossary entry for the guide page (#118)."""
    entry = GLOSSARY.get(slug, {})
    return entry.get(get_locale()) or entry.get("cs") or ("", "")


def setup_jinja2_i18n(env: Environment) -> None:
    """Install gettext callables and locale-aware filters on a Jinja2 environment."""
    env.add_extension("jinja2.ext.i18n")
    env.install_gettext_callables(gettext, ngettext, newstyle=False)  # type: ignore[attr-defined]
    env.filters["thousands"] = format_int  # type: ignore[attr-defined]
    env.filters["num"] = format_number  # type: ignore[attr-defined]
    env.filters["pct"] = format_percent  # type: ignore[attr-defined]
    env.filters["cz_date"] = format_date_cz  # type: ignore[attr-defined]
    env.filters["count"] = format_count  # type: ignore[attr-defined]
    env.globals["gloss"] = gloss  # type: ignore[attr-defined]
    env.globals["gloss_entry"] = gloss_entry  # type: ignore[attr-defined]
    env.globals["gloss_order"] = GLOSSARY_ORDER  # type: ignore[attr-defined]
