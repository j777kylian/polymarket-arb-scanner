"""Gamma API client — market discovery (keyset pagination)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, AsyncIterator

from polymarket_scanner.api.http_base import ReadOnlyHttpClient
from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import FeeSchedule, MarketInfo

logger = get_logger(__name__)


def _parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            logger.warning("Failed to parse JSON list: %s", value[:80])
            return []
    return []


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        logger.warning("Unparseable datetime: %s", value)
        return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def parse_gamma_market(raw: dict[str, Any]) -> MarketInfo | None:
    """Parse a Gamma market payload into MarketInfo. Missing fields become None."""
    market_id = str(raw.get("id") or "")
    condition_id = raw.get("conditionId") or raw.get("condition_id")
    if not market_id or not condition_id:
        logger.warning("Skipping market with missing id/conditionId: %s", raw.get("id"))
        return None

    token_ids = _parse_json_list(raw.get("clobTokenIds"))
    outcomes = [str(x).upper() for x in _parse_json_list(raw.get("outcomes"))]
    yes_token: str | None = None
    no_token: str | None = None
    if len(token_ids) >= 2:
        # Convention: [Yes, No] when outcomes present; else assume first=Yes
        if outcomes and "YES" in outcomes and "NO" in outcomes:
            yes_idx = outcomes.index("YES")
            no_idx = outcomes.index("NO")
            yes_token = str(token_ids[yes_idx]) if yes_idx < len(token_ids) else None
            no_token = str(token_ids[no_idx]) if no_idx < len(token_ids) else None
        else:
            yes_token = str(token_ids[0])
            no_token = str(token_ids[1])

    events = raw.get("events") or []
    event_id = None
    event_slug = None
    category = raw.get("category")
    tags: list[str] = []
    if isinstance(events, list) and events:
        ev0 = events[0] if isinstance(events[0], dict) else {}
        event_id = str(ev0.get("id")) if ev0.get("id") is not None else None
        event_slug = ev0.get("slug")
        if not category:
            category = ev0.get("category")
        # tags may be nested
        for tag in ev0.get("tags") or []:
            if isinstance(tag, dict) and tag.get("label"):
                tags.append(str(tag["label"]))
            elif isinstance(tag, str):
                tags.append(tag)
    for tag in raw.get("tags") or []:
        if isinstance(tag, dict) and tag.get("label"):
            tags.append(str(tag["label"]))
        elif isinstance(tag, str):
            tags.append(tag)
    # dedupe
    tags = list(dict.fromkeys(tags))

    fees_enabled = raw.get("feesEnabled")
    if fees_enabled is None:
        fees_enabled = raw.get("fees_enabled")
    fee_schedule = FeeSchedule.from_api(raw.get("feeSchedule") or raw.get("fee_schedule"))

    return MarketInfo(
        event_id=event_id,
        market_id=market_id,
        condition_id=str(condition_id),
        question=raw.get("question"),
        slug=raw.get("slug"),
        event_slug=event_slug,
        category=category,
        tags=tags,
        yes_token_id=yes_token,
        no_token_id=no_token,
        active=bool(raw.get("active", True)),
        closed=bool(raw.get("closed", False)),
        accepting_orders=bool(raw.get("acceptingOrders", raw.get("accepting_orders", True))),
        enable_order_book=bool(raw.get("enableOrderBook", raw.get("enable_order_book", True))),
        neg_risk=bool(raw.get("negRisk", raw.get("neg_risk", False))),
        minimum_tick_size=_parse_decimal(
            raw.get("orderPriceMinTickSize") or raw.get("minimum_tick_size")
        ),
        minimum_order_size=_parse_decimal(raw.get("orderMinSize") or raw.get("minimum_order_size")),
        fees_enabled=fees_enabled if fees_enabled is None else bool(fees_enabled),
        fee_schedule=fee_schedule,
        start_date=_parse_dt(raw.get("startDate") or raw.get("start_date")),
        end_date=_parse_dt(raw.get("endDate") or raw.get("end_date")),
        resolution_source=raw.get("resolutionSource") or raw.get("resolution_source"),
        description=raw.get("description"),
        volume=_parse_decimal(raw.get("volumeNum") or raw.get("volume")),
        liquidity=_parse_decimal(raw.get("liquidityNum") or raw.get("liquidity")),
        last_updated=_parse_dt(raw.get("updatedAt") or raw.get("updated_at")),
        raw=raw,
    )


class GammaClient:
    def __init__(self, client: ReadOnlyHttpClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client
        self.cfg = get_config()

    async def __aenter__(self) -> GammaClient:
        if self._client is None:
            self._client = ReadOnlyHttpClient(self.cfg.api.gamma_url)
            await self._client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._owns_client and self._client is not None:
            await self._client.__aexit__(*args)

    @property
    def client(self) -> ReadOnlyHttpClient:
        if self._client is None:
            raise RuntimeError("GammaClient not started")
        return self._client

    async def iter_markets_keyset(
        self,
        *,
        closed: bool = False,
        active: bool | None = True,
        limit: int | None = None,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield raw market dicts via /markets/keyset."""
        page_limit = limit or self.cfg.api.market_page_limit
        after_cursor: str | None = None
        pages = 0
        while True:
            params: dict[str, Any] = {"closed": str(closed).lower(), "limit": page_limit}
            if active is not None:
                params["active"] = str(active).lower()
            if after_cursor:
                params["after_cursor"] = after_cursor
            try:
                payload = await self.client.get_json("/markets/keyset", params=params)
            except Exception as exc:
                logger.error("Gamma keyset fetch failed: %s", exc)
                raise
            markets = payload.get("markets") if isinstance(payload, dict) else None
            if not isinstance(markets, list):
                logger.warning("Unexpected keyset payload type: %s", type(payload))
                break
            for m in markets:
                if isinstance(m, dict):
                    yield m
            pages += 1
            next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
            if not next_cursor:
                break
            if max_pages is not None and pages >= max_pages:
                break
            after_cursor = next_cursor

    async def fetch_tradable_markets(
        self,
        *,
        max_pages: int | None = None,
    ) -> list[MarketInfo]:
        results: list[MarketInfo] = []
        async for raw in self.iter_markets_keyset(closed=False, active=True, max_pages=max_pages):
            info = parse_gamma_market(raw)
            if info is None:
                continue
            if not (
                info.active
                and not info.closed
                and info.accepting_orders
                and info.enable_order_book
            ):
                continue
            results.append(info)
        logger.info("Discovered %s tradable markets from Gamma", len(results))
        return results
