"""Internationalization support for Czech/English UI localization."""

import contextvars

from jinja2 import Environment

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


def setup_jinja2_i18n(env: Environment) -> None:
    """Install gettext callables on a Jinja2 environment."""
    env.add_extension("jinja2.ext.i18n")
    env.install_gettext_callables(gettext, ngettext, newstyle=False)  # type: ignore[attr-defined]
