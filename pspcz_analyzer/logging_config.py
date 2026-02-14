"""Application-wide logging configuration using loguru.

Intercepts all standard-library logging (from uvicorn, httpx, etc.)
and routes it through loguru for consistent colorful output.
"""

import inspect
import logging
import sys

from loguru import logger


class _InterceptHandler(logging.Handler):
    """Redirect standard logging messages to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = inspect.currentframe(), 0
        while frame:
            filename = frame.f_code.co_filename
            is_logging = filename == logging.__file__
            is_frozen = "importlib" in filename and "_bootstrap" in filename
            if depth > 0 and not (is_logging or is_frozen):
                break
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging() -> None:
    """Configure loguru as the sole logging handler for the entire application."""
    logger.remove()
    logger.add(
        sys.stderr,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
        level="INFO",
    )

    # Intercept all standard logging (uvicorn, httpx, etc.)
    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
