"""SQLAlchemy 2.x models and database helpers."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    select,
    text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

from polymarket_scanner.config import ROOT_DIR, get_config
from polymarket_scanner.logging_config import get_logger

logger = get_logger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize datetimes for SQLite (often returns naive UTC)."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def str_to_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(value)


class Base(DeclarativeBase):
    pass


class MarketRow(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)
    question: Mapped[str | None] = mapped_column(Text, nullable=True)
    slug: Mapped[str | None] = mapped_column(String(512), nullable=True)
    event_slug: Mapped[str | None] = mapped_column(String(512), nullable=True)
    category: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tags_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    yes_token_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    no_token_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    accepting_orders: Mapped[bool] = mapped_column(Boolean, default=True)
    enable_order_book: Mapped[bool] = mapped_column(Boolean, default=True)
    neg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    minimum_tick_size: Mapped[str | None] = mapped_column(String(32), nullable=True)
    minimum_order_size: Mapped[str | None] = mapped_column(String(32), nullable=True)
    fees_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    volume: Mapped[str | None] = mapped_column(String(64), nullable=True)
    liquidity: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    tokens: Mapped[list[TokenRow]] = relationship(back_populates="market")
    fee_schedule: Mapped[FeeScheduleRow | None] = relationship(
        back_populates="market", uselist=False
    )


class TokenRow(Base):
    __tablename__ = "tokens"
    __table_args__ = (UniqueConstraint("token_id", name="uq_token_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), index=True)
    token_id: Mapped[str] = mapped_column(String(128), index=True)
    outcome: Mapped[str] = mapped_column(String(16))
    market: Mapped[MarketRow] = relationship(back_populates="tokens")


class FeeScheduleRow(Base):
    __tablename__ = "fee_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(
        ForeignKey("markets.market_id"), unique=True, index=True
    )
    rate: Mapped[str] = mapped_column(String(32), default="0")
    exponent: Mapped[str] = mapped_column(String(32), default="1")
    taker_only: Mapped[bool] = mapped_column(Boolean, default=True)
    rebate_rate: Mapped[str] = mapped_column(String(32), default="0")
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    market: Mapped[MarketRow] = relationship(back_populates="fee_schedule")


class OrderBookSnapshotRow(Base):
    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_ob_condition_token_fetched", "condition_id", "token_id", "fetched_at"),
        Index("ix_ob_hash", "hash"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)
    token_id: Mapped[str] = mapped_column(String(128), index=True)
    outcome: Mapped[str] = mapped_column(String(16))
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tick_size: Mapped[str | None] = mapped_column(String(32), nullable=True)
    min_order_size: Mapped[str | None] = mapped_column(String(32), nullable=True)
    neg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    levels: Mapped[list[OrderBookLevelRow]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan"
    )


class OrderBookLevelRow(Base):
    __tablename__ = "orderbook_levels"
    __table_args__ = (Index("ix_obl_snapshot_side", "snapshot_id", "side"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("orderbook_snapshots.id"), index=True)
    side: Mapped[str] = mapped_column(String(8))  # bid|ask
    level_index: Mapped[int] = mapped_column(Integer)
    price: Mapped[str] = mapped_column(String(32))
    size: Mapped[str] = mapped_column(String(32))

    snapshot: Mapped[OrderBookSnapshotRow] = relationship(back_populates="levels")


class OpportunityRow(Base):
    __tablename__ = "opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)
    question: Mapped[str | None] = mapped_column(Text, nullable=True)
    direction: Mapped[str] = mapped_column(String(16))
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    data_age_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    stale: Mapped[bool] = mapped_column(Boolean, default=False)
    quantity: Mapped[str] = mapped_column(String(32))
    yes_vwap: Mapped[str] = mapped_column(String(32))
    no_vwap: Mapped[str] = mapped_column(String(32))
    gross_profit: Mapped[str] = mapped_column(String(32))
    fee_total: Mapped[str] = mapped_column(String(32))
    net_profit: Mapped[str] = mapped_column(String(32))
    net_profit_per_share: Mapped[str] = mapped_column(String(32))
    net_profit_rate: Mapped[str] = mapped_column(String(32))
    levels_used_yes: Mapped[int] = mapped_column(Integer, default=0)
    levels_used_no: Mapped[int] = mapped_column(Integer, default=0)
    fees_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    neg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    risk_tags_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    requires_split_inventory: Mapped[bool] = mapped_column(Boolean, default=False)
    net_profitable: Mapped[bool] = mapped_column(Boolean, default=False)
    optimistic_net: Mapped[str | None] = mapped_column(String(32), nullable=True)
    base_net: Mapped[str | None] = mapped_column(String(32), nullable=True)
    pessimistic_net: Mapped[str | None] = mapped_column(String(32), nullable=True)
    simulation_quality: Mapped[str | None] = mapped_column(String(32), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class SimulationRunRow(Base):
    __tablename__ = "simulation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    profile: Mapped[str] = mapped_column(String(32))
    quality: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    quantity: Mapped[str] = mapped_column(String(32))
    gross_profit: Mapped[str] = mapped_column(String(32))
    fees: Mapped[str] = mapped_column(String(32))
    operational_cost: Mapped[str] = mapped_column(String(32))
    safety_buffer: Mapped[str] = mapped_column(String(32))
    net_profit: Mapped[str] = mapped_column(String(32))
    worst_loss: Mapped[str] = mapped_column(String(32), default="0")
    unhedged_quantity: Mapped[str] = mapped_column(String(32), default="0")
    still_arbitrage: Mapped[bool] = mapped_column(Boolean, default=False)
    one_leg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    params_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    legs: Mapped[list[SimulationLegRow]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class SimulationLegRow(Base):
    __tablename__ = "simulation_legs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("simulation_runs.id"), index=True)
    side: Mapped[str] = mapped_column(String(8))
    role: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[str] = mapped_column(String(32))
    vwap: Mapped[str | None] = mapped_column(String(32), nullable=True)
    fee: Mapped[str] = mapped_column(String(32), default="0")
    notional: Mapped[str] = mapped_column(String(32), default="0")
    fills_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped[SimulationRunRow] = relationship(back_populates="legs")


class RuleSetRow(Base):
    __tablename__ = "rule_sets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    logic: Mapped[str] = mapped_column(String(8), default="AND")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    rules: Mapped[list[RuleRow]] = relationship(
        back_populates="rule_set", cascade="all, delete-orphan"
    )


class RuleRow(Base):
    __tablename__ = "rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_set_id: Mapped[int] = mapped_column(ForeignKey("rule_sets.id"), index=True)
    field: Mapped[str] = mapped_column(String(128))
    operator: Mapped[str] = mapped_column(String(32))
    value_json: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    rule_set: Mapped[RuleSetRow] = relationship(back_populates="rules")


class ScannerRunRow(Base):
    __tablename__ = "scanner_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="running")
    markets_synced: Mapped[int] = mapped_column(Integer, default=0)
    books_fetched: Mapped[int] = mapped_column(Integer, default=0)
    signals_found: Mapped[int] = mapped_column(Integer, default=0)
    api_errors: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class ApiErrorRow(Base):
    __tablename__ = "api_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    source: Mapped[str] = mapped_column(String(64))
    endpoint: Mapped[str | None] = mapped_column(String(512), nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    message: Mapped[str] = mapped_column(Text)
    context_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class DailyReportRow(Base):
    __tablename__ = "daily_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_date: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    markets_scanned: Mapped[int] = mapped_column(Integer, default=0)
    raw_signals: Mapped[int] = mapped_column(Integer, default=0)
    net_signals: Mapped[int] = mapped_column(Integer, default=0)
    base_profitable: Mapped[int] = mapped_column(Integer, default=0)
    pessimistic_profitable: Mapped[int] = mapped_column(Integer, default=0)
    total_sim_profit: Mapped[str] = mapped_column(String(32), default="0")
    max_single_profit: Mapped[str] = mapped_column(String(32), default="0")
    max_one_leg_loss: Mapped[str] = mapped_column(String(32), default="0")
    avg_duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    html_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    csv_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    summary_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class AppSettingRow(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(128), unique=True)
    value_json: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class OpportunityEpisodeRow(Base):
    __tablename__ = "opportunity_episodes"
    __table_args__ = (
        Index("ix_episode_market_dir_open", "market_id", "direction", "is_open"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)
    question: Mapped[str | None] = mapped_column(Text, nullable=True)
    direction: Mapped[str] = mapped_column(String(16))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    disappeared_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_open: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    peak_net_profit: Mapped[str] = mapped_column(String(32), default="0")
    last_net_profit: Mapped[str] = mapped_column(String(32), default="0")
    last_quantity: Mapped[str] = mapped_column(String(32), default="0")
    last_opportunity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class LatencySampleRow(Base):
    __tablename__ = "latency_samples"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    event_type: Mapped[str] = mapped_column(String(32))
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    latency_ms: Mapped[float] = mapped_column(Float)


class PaperAccountRow(Base):
    __tablename__ = "paper_account"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cash: Mapped[str] = mapped_column(String(32), default="1000")
    occupied: Mapped[str] = mapped_column(String(32), default="0")
    realized_pnl: Mapped[str] = mapped_column(String(32), default="0")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PaperTradeRow(Base):
    __tablename__ = "paper_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    episode_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tif: Mapped[str] = mapped_column(String(8))
    delay_ms: Mapped[int] = mapped_column(Integer, default=500)
    status: Mapped[str] = mapped_column(String(32), index=True)
    yes_qty: Mapped[str] = mapped_column(String(32), default="0")
    no_qty: Mapped[str] = mapped_column(String(32), default="0")
    matched_qty: Mapped[str] = mapped_column(String(32), default="0")
    unhedged_qty: Mapped[str] = mapped_column(String(32), default="0")
    cash_used: Mapped[str] = mapped_column(String(32), default="0")
    merge_proceeds: Mapped[str] = mapped_column(String(32), default="0")
    pnl: Mapped[str] = mapped_column(String(32), default="0")
    cash_after: Mapped[str] = mapped_column(String(32), default="0")
    details: Mapped[str | None] = mapped_column(Text, nullable=True)


_engine = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine(url: str | None = None):
    global _engine, _SessionLocal
    cfg = get_config()
    db_url = url or cfg.database.url
    if db_url.startswith("sqlite:///./"):
        rel = db_url.replace("sqlite:///./", "", 1)
        abs_path = ROOT_DIR / rel
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        db_url = f"sqlite:///{abs_path}"
    elif db_url.startswith("sqlite:///") and not db_url.startswith("sqlite:////"):
        # relative path without ./
        path_part = db_url.replace("sqlite:///", "", 1)
        if not path_part.startswith("/"):
            abs_path = ROOT_DIR / path_part
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{abs_path}"

    if _engine is None or url is not None:
        connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
        _engine = create_engine(db_url, future=True, connect_args=connect_args)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    return _engine


def get_session_factory(url: str | None = None) -> sessionmaker[Session]:
    get_engine(url)
    assert _SessionLocal is not None
    return _SessionLocal


@contextmanager
def session_scope(url: str | None = None) -> Iterator[Session]:
    factory = get_session_factory(url)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


DEFAULT_RULE_SETS: list[dict[str, Any]] = [
    {
        "name": "Conservative",
        "description": "Higher profit thresholds, fresh data, fee-aware",
        "conditions": [
            {"field": "net_profit", "operator": ">=", "value": 1.0},
            {"field": "net_profit_per_share", "operator": ">=", "value": 0.005},
            {"field": "data_age_seconds", "operator": "<=", "value": 15},
            {"field": "stale", "operator": "==", "value": False},
            {"field": "base_net_profit", "operator": ">", "value": 0},
        ],
    },
    {
        "name": "Balanced",
        "description": "Default balanced filters",
        "conditions": [
            {"field": "net_profit", "operator": ">=", "value": 0.5},
            {"field": "net_profit_per_share", "operator": ">=", "value": 0.002},
            {"field": "quantity", "operator": ">=", "value": 10},
            {"field": "data_age_seconds", "operator": "<=", "value": 60},
            {"field": "stale", "operator": "==", "value": False},
        ],
    },
    {
        "name": "Exploratory",
        "description": "Loose filters for research",
        "conditions": [
            {"field": "gross_profit", "operator": ">", "value": 0},
            {"field": "data_age_seconds", "operator": "<=", "value": 120},
        ],
    },
    {
        "name": "Fee-free only",
        "description": "Only markets with fees disabled",
        "conditions": [
            {"field": "fees_enabled", "operator": "==", "value": False},
            {"field": "net_profit", "operator": ">", "value": 0},
            {"field": "stale", "operator": "==", "value": False},
        ],
    },
]


def init_db(url: str | None = None) -> None:
    engine = get_engine(url)
    Base.metadata.create_all(engine)
    with session_scope(url) as session:
        existing = {r.name for r in session.scalars(select(RuleSetRow)).all()}
        for rs in DEFAULT_RULE_SETS:
            if rs["name"] in existing:
                continue
            row = RuleSetRow(
                name=rs["name"],
                enabled=rs["name"] == "Balanced",
                description=rs.get("description"),
                logic="AND",
            )
            session.add(row)
            session.flush()
            for i, cond in enumerate(rs["conditions"]):
                session.add(
                    RuleRow(
                        rule_set_id=row.id,
                        field=cond["field"],
                        operator=cond["operator"],
                        value_json=json.dumps(cond["value"]),
                        enabled=True,
                        sort_order=i,
                    )
                )
        # Seed settings
        defaults = {
            "max_data_age_seconds": get_config().scanner.max_data_age_seconds,
            "default_rule_set": get_config().scanner.default_rule_set,
            "timezone": get_config().reporting.timezone,
            "read_only_banner": "READ-ONLY MODE — REAL TRADING DISABLED",
        }
        for key, value in defaults.items():
            found = session.scalar(select(AppSettingRow).where(AppSettingRow.key == key))
            if not found:
                session.add(AppSettingRow(key=key, value_json=json.dumps(value)))
        if session.scalar(select(PaperAccountRow).limit(1)) is None:
            session.add(
                PaperAccountRow(
                    cash=str(get_config().paper.starting_capital),
                    occupied="0",
                    realized_pnl="0",
                )
            )
    logger.info("Database initialized")


def record_api_error(
    session: Session,
    source: str,
    message: str,
    endpoint: str | None = None,
    status_code: int | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    session.add(
        ApiErrorRow(
            source=source,
            endpoint=endpoint,
            status_code=status_code,
            message=message,
            context_json=json.dumps(context) if context else None,
        )
    )


def get_setting(session: Session, key: str, default: Any = None) -> Any:
    row = session.scalar(select(AppSettingRow).where(AppSettingRow.key == key))
    if not row:
        return default
    try:
        return json.loads(row.value_json)
    except json.JSONDecodeError:
        return row.value_json


def set_setting(session: Session, key: str, value: Any) -> None:
    row = session.scalar(select(AppSettingRow).where(AppSettingRow.key == key))
    payload = json.dumps(value)
    if row:
        row.value_json = payload
        row.updated_at = utcnow()
    else:
        session.add(AppSettingRow(key=key, value_json=payload))


def dumps_decimal_dict(data: dict[str, Any]) -> str:
    def _default(obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return format(obj, "f")
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(type(obj))

    return json.dumps(data, default=_default)
