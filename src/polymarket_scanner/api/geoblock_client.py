"""Geoblock status client — display only, no bypass."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from polymarket_scanner.api.http_base import ReadOnlyHttpClient
from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import GeoblockStatus

logger = get_logger(__name__)


class GeoblockClient:
    """Fetches geographic restriction status for UI display only."""

    def __init__(self) -> None:
        self.cfg = get_config()

    async def check(self) -> GeoblockStatus:
        url = self.cfg.api.geoblock_url
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path or "/api/geoblock"
        try:
            async with ReadOnlyHttpClient(base) as client:
                raw: Any = await client.get_json(path)
            if not isinstance(raw, dict):
                return GeoblockStatus(error="unexpected geoblock payload")
            return GeoblockStatus(
                blocked=raw.get("blocked"),
                ip=raw.get("ip"),
                country=raw.get("country"),
                region=raw.get("region"),
            )
        except Exception as exc:
            logger.warning("Geoblock check failed: %s", exc)
            return GeoblockStatus(error=str(exc))
