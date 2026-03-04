"""Runtime configuration: mutable settings persisted to JSON."""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR

_RUNTIME_CONFIG_FILENAME = "runtime_config.json"


@dataclass
class RuntimeConfig:
    """Mutable runtime settings that can be changed without restarting."""

    # LLM provider
    llm_provider: str = "ollama"
    ollama_base_url: str = "http://localhost:11434"
    ollama_api_key: str = ""
    ollama_model: str = "qwen3:8b"
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    llm_structured_output: bool = True
    llm_empty_retries: int = 2

    # Tisk processing
    tisk_shortener: bool = False
    ai_periods_limit: int = 3

    # Daily refresh
    daily_refresh_enabled: bool = True
    daily_refresh_hour: int = 3

    # Amendment analysis
    amendments_enabled: bool = True

    # Dev skip flags
    dev_skip_classify_and_summarize: bool = False
    dev_skip_version_diffs: bool = False
    dev_skip_amendments: bool = False

    # Fields that should be masked in API responses
    _secret_fields: list[str] = field(
        default_factory=lambda: ["ollama_api_key", "openai_api_key"],
        repr=False,
    )

    def to_dict(self, mask_secrets: bool = False) -> dict:
        """Serialize to dict, optionally masking secret fields."""
        d = asdict(self)
        d.pop("_secret_fields", None)
        if mask_secrets:
            for key in self._secret_fields:
                if d.get(key):
                    d[key] = "***"
        return d


def _defaults_from_env() -> dict:
    """Read current config.py constants (loaded from .env) as RuntimeConfig defaults.

    When no runtime_config.json exists, this ensures the admin dashboard
    shows the actual .env values instead of hardcoded dataclass defaults.
    """
    import pspcz_analyzer.config as cfg

    return {
        "llm_provider": cfg.LLM_PROVIDER,
        "ollama_base_url": cfg.OLLAMA_BASE_URL,
        "ollama_api_key": cfg.OLLAMA_API_KEY,
        "ollama_model": cfg.OLLAMA_MODEL,
        "openai_base_url": cfg.OPENAI_BASE_URL,
        "openai_api_key": cfg.OPENAI_API_KEY,
        "openai_model": cfg.OPENAI_MODEL,
        "llm_structured_output": cfg.LLM_STRUCTURED_OUTPUT,
        "llm_empty_retries": cfg.LLM_EMPTY_RETRIES,
        "tisk_shortener": cfg.TISK_SHORTENER,
        "ai_periods_limit": cfg.AI_PERIODS_LIMIT,
        "daily_refresh_enabled": cfg.DAILY_REFRESH_ENABLED,
        "daily_refresh_hour": cfg.DAILY_REFRESH_HOUR,
        "amendments_enabled": cfg.AMENDMENTS_ENABLED,
        "dev_skip_classify_and_summarize": cfg.DEV_SKIP_CLASSIFY_AND_SUMMARIZE,
        "dev_skip_version_diffs": cfg.DEV_SKIP_VERSION_DIFFS,
        "dev_skip_amendments": cfg.DEV_SKIP_AMENDMENTS,
    }


def _config_path(cache_dir: Path) -> Path:
    """Path to the runtime config JSON file."""
    return cache_dir / _RUNTIME_CONFIG_FILENAME


def load_runtime_config(cache_dir: Path = DEFAULT_CACHE_DIR) -> RuntimeConfig:
    """Load runtime config from JSON file, or return defaults from .env."""
    path = _config_path(cache_dir)
    if not path.exists():
        logger.debug("[runtime-config] No config file at {}, using .env defaults", path)
        return RuntimeConfig(**_defaults_from_env())

    try:
        data = json.loads(path.read_text("utf-8"))
        # Filter out unknown keys to handle schema changes gracefully
        known_fields = {
            f.name
            for f in RuntimeConfig.__dataclass_fields__.values()
            if not f.name.startswith("_")
        }
        filtered = {k: v for k, v in data.items() if k in known_fields}
        config = RuntimeConfig(**filtered)
        logger.info("[runtime-config] Loaded from {}", path)
        return config
    except Exception:
        logger.opt(exception=True).warning(
            "[runtime-config] Failed to load {}, using .env defaults", path
        )
        return RuntimeConfig(**_defaults_from_env())


def save_runtime_config(config: RuntimeConfig, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
    """Persist runtime config to JSON file."""
    path = _config_path(cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.to_dict(), indent=2) + "\n", encoding="utf-8")
    logger.info("[runtime-config] Saved to {}", path)


def apply_runtime_config(config: RuntimeConfig) -> None:
    """Hot-patch config.py module-level constants from runtime config.

    Works because LLM clients read config at call time, not import time.
    """
    import pspcz_analyzer.config as cfg

    cfg.LLM_PROVIDER = config.llm_provider
    cfg.OLLAMA_BASE_URL = config.ollama_base_url
    cfg.OLLAMA_API_KEY = config.ollama_api_key
    cfg.OLLAMA_MODEL = config.ollama_model
    cfg.OPENAI_BASE_URL = config.openai_base_url
    cfg.OPENAI_API_KEY = config.openai_api_key
    cfg.OPENAI_MODEL = config.openai_model
    cfg.LLM_STRUCTURED_OUTPUT = config.llm_structured_output
    cfg.LLM_EMPTY_RETRIES = config.llm_empty_retries
    cfg.TISK_SHORTENER = config.tisk_shortener
    cfg.AI_PERIODS_LIMIT = config.ai_periods_limit
    cfg.DAILY_REFRESH_ENABLED = config.daily_refresh_enabled
    cfg.DAILY_REFRESH_HOUR = config.daily_refresh_hour
    cfg.AMENDMENTS_ENABLED = config.amendments_enabled
    cfg.DEV_SKIP_CLASSIFY_AND_SUMMARIZE = config.dev_skip_classify_and_summarize
    cfg.DEV_SKIP_VERSION_DIFFS = config.dev_skip_version_diffs
    cfg.DEV_SKIP_AMENDMENTS = config.dev_skip_amendments

    logger.info(
        "[runtime-config] Applied: provider={}, model={}/{}",
        config.llm_provider,
        config.ollama_model,
        config.openai_model,
    )
