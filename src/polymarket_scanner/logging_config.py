"""Structured logging setup."""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

import yaml

from polymarket_scanner.config import CONFIG_DIR, ROOT_DIR, get_config


def setup_logging(level: str | None = None) -> None:
    cfg = get_config()
    log_dir = ROOT_DIR / cfg.logging.dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logging_yaml = CONFIG_DIR / "logging.yaml"
    if logging_yaml.exists():
        with logging_yaml.open("r", encoding="utf-8") as fh:
            content = yaml.safe_load(fh) or {}
        file_handler = content.get("handlers", {}).get("file", {})
        if "filename" in file_handler:
            file_handler["filename"] = str(log_dir / Path(file_handler["filename"]).name)
        if level:
            content.setdefault("root", {})["level"] = level.upper()
            for handler in content.get("handlers", {}).values():
                if isinstance(handler, dict) and "level" in handler:
                    handler["level"] = level.upper()
        logging.config.dictConfig(content)
    else:
        logging.basicConfig(
            level=(level or cfg.logging.level).upper(),
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
