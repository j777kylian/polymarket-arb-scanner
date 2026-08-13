"""CLOB API client — order books and market fee details (GET only)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from polymarket_scanner.api.http_base import ReadOnlyHttpClient
from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import FeeSchedule, OrderBookLevel, OrderBookSnapshot, OutcomeSide

logger = get_logger(__name__)


def _dec(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _parse_book_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        # CLOB often returns epoch ms as string
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            ms = int(value)
            return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        logger.warning("Unparseable orderbook timestamp: %s", value)
        return None


def normalize_orderbook_levels(
    bids_raw: list[dict[str, Any]],
    asks_raw: list[dict[str, Any]],
) -> tuple[list[OrderBookLevel], list[OrderBookLevel]]:
    """
    Normalize API levels to:
      bids: high -> low
      asks: low -> high
    CLOB docs: bids ascending, asks descending (best is last).
    """
    bids: list[OrderBookLevel] = []
    asks: list[OrderBookLevel] = []
    for row in bids_raw or []:
        try:
            price = _dec(row.get("price"))
            size = _dec(row.get("size"))
            if not (Decimal("0") < price <= Decimal("1")):
                continue
            if size <= 0:
                continue
            bids.append(OrderBookLevel(price=price, size=size))
        except Exception:
            continue
    for row in asks_raw or []:
        try:
            price = _dec(row.get("price"))
            size = _dec(row.get("size"))
            if not (Decimal("0") < price <= Decimal("1")):
                continue
            if size <= 0:
                continue
            asks.append(OrderBookLevel(price=price, size=size))
        except Exception:
            continue
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)
    return bids, asks


def parse_orderbook(
    raw: dict[str, Any],
    *,
    outcome: OutcomeSide,
    expected_token_id: str | None = None,
    fetched_at: datetime | None = None,
) -> OrderBookSnapshot:
    token_id = str(raw.get("asset_id") or raw.get("token_id") or raw.get("tokenId") or "")
    if expected_token_id and token_id and token_id != expected_token_id:
        logger.warning(
            "Orderbook token mismatch: expected %s got %s", expected_token_id, token_id
        )
    condition_id = str(
        raw.get("market") or raw.get("condition_id") or raw.get("conditionId") or ""
    )
    bids, asks = normalize_orderbook_levels(raw.get("bids") or [], raw.get("asks") or [])
    return OrderBookSnapshot(
        condition_id=condition_id,
        token_id=token_id or (expected_token_id or ""),
        outcome=outcome,
        timestamp=_parse_book_timestamp(raw.get("timestamp")),
        hash=raw.get("hash"),
        bids=bids,
        asks=asks,
        tick_size=_dec(raw.get("tick_size") or raw.get("tickSize"), "0.01"),
        min_order_size=_dec(raw.get("min_order_size") or raw.get("minOrderSize"), "5"),
        neg_risk=bool(raw.get("neg_risk") if "neg_risk" in raw else raw.get("negRisk", False)),
        fetched_at=fetched_at or datetime.now(timezone.utc),
        raw=raw,
    )


class ClobClient:
    def __init__(self, client: ReadOnlyHttpClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client
        self.cfg = get_config()

    async def __aenter__(self) -> ClobClient:
        if self._client is None:
            self._client = ReadOnlyHttpClient(self.cfg.api.clob_url)
            await self._client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._owns_client and self._client is not None:
            await self._client.__aexit__(*args)

    @property
    def client(self) -> ReadOnlyHttpClient:
        if self._client is None:
            raise RuntimeError("ClobClient not started")
        return self._client

    async def get_order_book(
        self,
        token_id: str,
        *,
        outcome: OutcomeSide,
    ) -> OrderBookSnapshot:
        raw = await self.client.get_json("/book", params={"token_id": token_id})
        if not isinstance(raw, dict):
            raise ValueError(f"Unexpected orderbook response for {token_id}")
        return parse_orderbook(raw, outcome=outcome, expected_token_id=token_id)

    async def get_market(self, condition_id: str) -> dict[str, Any]:
        """Official CLOB market details: GET /clob-markets/{condition_id}."""
        raw = await self.client.get_json(f"/clob-markets/{condition_id}")
        if not isinstance(raw, dict):
            raise ValueError(f"Unexpected clob-markets response for {condition_id}")
        return raw

    async def get_fee_schedule_for_condition(
        self, condition_id: str
    ) -> tuple[bool | None, FeeSchedule | None]:
        """Fee enrichment from GET /clob-markets/{condition_id}.

        Compact payload uses ``fd`` = {{r, e, to}} for the fee curve.
        Do not use legacy GET /markets/{{id}} here — it lacks fee curve details.
        """
        try:
            raw = await self.get_market(condition_id)
        except Exception as exc:
            logger.warning("CLOB market fetch failed for %s: %s", condition_id, exc)
            return None, None

        schedule = FeeSchedule.from_api(
            raw.get("fd") or raw.get("fee_schedule") or raw.get("feeSchedule")
        )

        fees_enabled = raw.get("fees_enabled")
        if fees_enabled is None:
            fees_enabled = raw.get("feesEnabled")
        if fees_enabled is None and schedule is not None:
            fees_enabled = schedule.rate > 0
        if fees_enabled is None:
            tbf = raw.get("tbf", raw.get("taker_base_fee", raw.get("takerBaseFee")))
            if tbf is not None:
                try:
                    fees_enabled = int(tbf) > 0
                except (TypeError, ValueError):
                    fees_enabled = None

        if schedule is None:
            logger.warning(
                "CLOB /clob-markets/%s missing fee details (fd); fees_enabled=%s tbf=%s",
                condition_id,
                fees_enabled,
                raw.get("tbf", raw.get("taker_base_fee")),
            )
        return (bool(fees_enabled) if fees_enabled is not None else None), schedule
