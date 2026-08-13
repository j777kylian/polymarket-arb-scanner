"""Shared async HTTP client helpers (read-only)."""

from __future__ import annotations

import os
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.safety import assert_trading_disabled, guard_write_endpoint

logger = get_logger(__name__)


class RateLimitError(Exception):
    def __init__(self, message: str, status_code: int = 429) -> None:
        super().__init__(message)
        self.status_code = status_code


class ReadOnlyHttpClient:
    """HTTP client that only allows GET (and HEAD) for public endpoints."""

    def __init__(self, base_url: str, timeout: float | None = None) -> None:
        assert_trading_disabled()
        cfg = get_config()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout if timeout is not None else cfg.api.http_timeout_seconds
        self._client: httpx.AsyncClient | None = None
        proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("ALL_PROXY")
        kwargs: dict[str, Any] = {
            "base_url": self.base_url,
            "timeout": self.timeout,
            "headers": {"Accept": "application/json", "User-Agent": "polymarket-arb-scanner/0.1"},
        }
        if proxy:
            kwargs["proxy"] = proxy
        self._client_kwargs = kwargs

    async def __aenter__(self) -> ReadOnlyHttpClient:
        self._client = httpx.AsyncClient(**self._client_kwargs)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("HTTP client not started; use async with")
        return self._client

    async def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        guard_write_endpoint("GET", path)
        cfg = get_config()

        @retry(
            retry=retry_if_exception_type((RateLimitError, httpx.TransportError)),
            stop=stop_after_attempt(cfg.api.retry_max_attempts),
            wait=wait_exponential_jitter(
                initial=cfg.api.retry_min_wait_seconds,
                max=cfg.api.retry_max_wait_seconds,
            ),
            reraise=True,
        )
        async def _do() -> Any:
            resp = await self.client.get(path, params=params)
            if resp.status_code == 429:
                logger.warning("HTTP 429 on %s%s — backing off", self.base_url, path)
                raise RateLimitError(f"429 for {path}")
            if resp.status_code >= 500:
                logger.warning("HTTP %s on %s%s", resp.status_code, self.base_url, path)
                raise httpx.TransportError(f"server error {resp.status_code}")
            resp.raise_for_status()
            return resp.json()

        return await _do()
