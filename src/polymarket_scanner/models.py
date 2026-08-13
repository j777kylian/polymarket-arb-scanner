"""Pydantic domain models — Decimal for money/prices."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class OutcomeSide(str, Enum):
    YES = "YES"
    NO = "NO"


class ArbDirection(str, Enum):
    FORWARD = "forward"  # buy YES+NO asks when sum < 1
    REVERSE = "reverse"  # sell YES+NO bids when sum > 1 (requires split)


class SimulationQuality(str, Enum):
    OBSERVED_SNAPSHOT = "observed_snapshot"
    ESTIMATED = "estimated"
    UNAVAILABLE = "unavailable"
    STALE = "stale"


class SimulationProfileName(str, Enum):
    OPTIMISTIC = "optimistic"
    BASE = "base"
    PESSIMISTIC = "pessimistic"
    CUSTOM = "custom"


class FeeSchedule(BaseModel):
    rate: Decimal = Decimal("0")
    exponent: Decimal = Decimal("1")
    taker_only: bool = True
    rebate_rate: Decimal = Decimal("0")

    @field_validator("rate", "exponent", "rebate_rate", mode="before")
    @classmethod
    def _to_decimal(cls, v: Any) -> Decimal:
        if v is None:
            return Decimal("0")
        return Decimal(str(v))

    @classmethod
    def from_api(cls, payload: dict[str, Any] | None) -> FeeSchedule | None:
        """Parse Gamma ``feeSchedule`` or CLOB ``fd`` ({r, e, to})."""
        if not payload or not isinstance(payload, dict):
            return None
        # Unwrap {"fd": {...}} wrappers; never treat CLOB root ``r`` (rewards dict) as rate
        if "rate" not in payload and isinstance(payload.get("fd"), dict):
            payload = payload["fd"]

        if "rate" in payload or "rebateRate" in payload or "takerOnly" in payload:
            rate = payload.get("rate")
            if rate is None:
                return None
            return cls(
                rate=rate,
                exponent=payload.get("exponent", 1),
                taker_only=bool(payload.get("takerOnly", payload.get("taker_only", True))),
                rebate_rate=payload.get("rebateRate", payload.get("rebate_rate", 0)),
            )

        # Compact CLOB fee details
        rate = payload.get("r")
        if rate is None or isinstance(rate, dict):
            return None
        return cls(
            rate=rate,
            exponent=payload.get("e", 1),
            taker_only=bool(payload.get("to", True)),
            rebate_rate=payload.get("rebateRate", payload.get("rebate_rate", 0)),
        )


class OrderBookLevel(BaseModel):
    price: Decimal
    size: Decimal

    @field_validator("price", "size", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return Decimal(str(v))


class OrderBookSnapshot(BaseModel):
    condition_id: str
    token_id: str
    outcome: OutcomeSide
    timestamp: datetime | None = None
    hash: str | None = None
    bids: list[OrderBookLevel] = Field(default_factory=list)  # high -> low
    asks: list[OrderBookLevel] = Field(default_factory=list)  # low -> high
    tick_size: Decimal = Decimal("0.01")
    min_order_size: Decimal = Decimal("5")
    neg_risk: bool = False
    fetched_at: datetime
    raw: dict[str, Any] | None = None
    connection_generation: int | None = None

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None

    def ask_depth_usd(self, max_levels: int | None = None) -> Decimal:
        levels = self.asks if max_levels is None else self.asks[:max_levels]
        return sum((lvl.price * lvl.size for lvl in levels), Decimal("0"))


class MarketInfo(BaseModel):
    event_id: str | None = None
    market_id: str
    condition_id: str
    question: str | None = None
    slug: str | None = None
    event_slug: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    yes_token_id: str | None = None
    no_token_id: str | None = None
    active: bool = True
    closed: bool = False
    accepting_orders: bool = True
    enable_order_book: bool = True
    neg_risk: bool = False
    minimum_tick_size: Decimal | None = None
    minimum_order_size: Decimal | None = None
    fees_enabled: bool | None = None
    fee_schedule: FeeSchedule | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    resolution_source: str | None = None
    description: str | None = None
    volume: Decimal | None = None
    liquidity: Decimal | None = None
    last_updated: datetime | None = None
    raw: dict[str, Any] | None = None
    parse_reasons: list[str] = Field(default_factory=list)

    @property
    def is_binary_tradable(self) -> bool:
        return bool(
            self.active
            and not self.closed
            and self.accepting_orders
            and self.enable_order_book
            and self.yes_token_id
            and self.no_token_id
        )


class FillSlice(BaseModel):
    price: Decimal
    size: Decimal
    notional: Decimal
    fee: Decimal = Decimal("0")


class WalkResult(BaseModel):
    quantity: Decimal
    yes_cost: Decimal
    no_cost: Decimal
    yes_vwap: Decimal
    no_vwap: Decimal
    total_cost: Decimal
    gross_profit: Decimal
    fee_yes: Decimal
    fee_no: Decimal
    net_profit: Decimal
    net_profit_per_share: Decimal
    net_profit_rate: Decimal
    levels_used_yes: int
    levels_used_no: int
    marginal_net_profit: Decimal
    yes_fills: list[FillSlice] = Field(default_factory=list)
    no_fills: list[FillSlice] = Field(default_factory=list)
    profitable: bool = False


class OpportunitySignal(BaseModel):
    market_id: str
    condition_id: str
    question: str | None = None
    direction: ArbDirection
    discovered_at: datetime
    data_age_seconds: float
    stale: bool = False
    quantity: Decimal
    yes_vwap: Decimal
    no_vwap: Decimal
    gross_profit: Decimal
    fee_total: Decimal
    net_profit: Decimal
    net_profit_per_share: Decimal
    net_profit_rate: Decimal
    levels_used_yes: int
    levels_used_no: int
    fees_enabled: bool | None = None
    neg_risk: bool = False
    risk_tags: list[str] = Field(default_factory=list)
    walk: WalkResult | None = None
    requires_split_inventory: bool = False
    books_ready: bool = True
    book_skew_ms: float | None = None
    books_skewed: bool = False
    passes_rule_set: bool | None = None


class SimulationLegResult(BaseModel):
    side: OutcomeSide
    role: str  # first|second|close
    fills: list[FillSlice] = Field(default_factory=list)
    quantity: Decimal = Decimal("0")
    vwap: Decimal | None = None
    fee: Decimal = Decimal("0")
    notional: Decimal = Decimal("0")


class SimulationResult(BaseModel):
    profile: str
    quality: SimulationQuality
    quantity: Decimal
    gross_profit: Decimal
    fees: Decimal
    operational_cost: Decimal
    safety_buffer: Decimal
    net_profit: Decimal
    worst_loss: Decimal = Decimal("0")
    unhedged_quantity: Decimal = Decimal("0")
    still_arbitrage: bool = False
    one_leg_risk: bool = False
    legs: list[SimulationLegResult] = Field(default_factory=list)
    details: str = ""
    risk_tags: list[str] = Field(default_factory=list)
    realized_pnl: Decimal = Decimal("0")
    unrealized_inventory_cost: Decimal = Decimal("0")
    remaining_inventory: Decimal = Decimal("0")


class RuleCondition(BaseModel):
    field: str
    operator: str
    value: Any
    enabled: bool = True


class RuleGroup(BaseModel):
    """Reserved for OR groups; v1 evaluates AND across conditions."""

    logic: str = "AND"  # AND | OR
    conditions: list[RuleCondition] = Field(default_factory=list)
    groups: list[RuleGroup] = Field(default_factory=list)


class RuleSetModel(BaseModel):
    name: str
    enabled: bool = True
    description: str | None = None
    logic: str = "AND"
    conditions: list[RuleCondition] = Field(default_factory=list)
    groups: list[RuleGroup] = Field(default_factory=list)


class GeoblockStatus(BaseModel):
    blocked: bool | None = None
    ip: str | None = None
    country: str | None = None
    region: str | None = None
    error: str | None = None
