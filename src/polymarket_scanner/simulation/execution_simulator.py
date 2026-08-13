"""Execution simulator with delay / slippage / one-leg risk."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from polymarket_scanner.models import (
    FeeSchedule,
    FillSlice,
    MarketInfo,
    OrderBookSnapshot,
    OutcomeSide,
    SimulationLegResult,
    SimulationQuality,
    SimulationResult,
    WalkResult,
)
from polymarket_scanner.simulation.orderbook_walker import (
    _apply_ask_slippage,
    _apply_bid_slippage,
    _apply_depth_factor,
    find_optimal_forward_arb,
    walk_buy_asks,
    walk_sell_bids,
)
from polymarket_scanner.simulation.scenario_profiles import ScenarioProfile, get_builtin_profiles

ZERO = Decimal("0")
ONE = Decimal("1")


def _leg_from_fills(
    side: OutcomeSide, role: str, fills: list[FillSlice]
) -> SimulationLegResult:
    qty = sum((f.size for f in fills), ZERO)
    notional = sum((f.notional for f in fills), ZERO)
    fee = sum((f.fee for f in fills), ZERO)
    vwap = (notional / qty) if qty else None
    return SimulationLegResult(
        side=side, role=role, fills=fills, quantity=qty, vwap=vwap, fee=fee, notional=notional
    )


def select_delayed_books(
    yes_t0: OrderBookSnapshot,
    no_t0: OrderBookSnapshot,
    *,
    delay_ms: int,
    yes_later: OrderBookSnapshot | None = None,
    no_later: OrderBookSnapshot | None = None,
    tolerance_ms: float | None = None,
    max_skew_ms: float | None = None,
) -> tuple[OrderBookSnapshot, OrderBookSnapshot, SimulationQuality]:
    from polymarket_scanner.config import get_config

    cfg = get_config().scanner
    tolerance = float(tolerance_ms if tolerance_ms is not None else cfg.observed_delay_tolerance_ms)
    max_skew = float(max_skew_ms if max_skew_ms is not None else cfg.max_book_skew_ms)

    if delay_ms <= 0:
        return yes_t0, no_t0, SimulationQuality.OBSERVED_SNAPSHOT
    if yes_later is None or no_later is None:
        return yes_t0, no_t0, SimulationQuality.ESTIMATED

    target_y = yes_t0.fetched_at + timedelta(milliseconds=delay_ms)
    target_n = no_t0.fetched_at + timedelta(milliseconds=delay_ms)

    def in_window(book: OrderBookSnapshot, target) -> bool:
        end = target + timedelta(milliseconds=tolerance)
        return target <= book.fetched_at <= end

    if not in_window(yes_later, target_y) or not in_window(no_later, target_n):
        too_old = yes_later.fetched_at < target_y or no_later.fetched_at < target_n
        return yes_t0, no_t0, SimulationQuality.STALE if too_old else SimulationQuality.ESTIMATED

    skew_ms = abs((yes_later.fetched_at - no_later.fetched_at).total_seconds() * 1000.0)
    if skew_ms > max_skew:
        return yes_t0, no_t0, SimulationQuality.UNAVAILABLE
    return yes_later, no_later, SimulationQuality.OBSERVED_SNAPSHOT


def _prepare_asks(book: OrderBookSnapshot, profile: ScenarioProfile) -> list:
    return _apply_ask_slippage(
        _apply_depth_factor(book.asks, profile.depth_factor),
        profile.slippage_ticks,
        book.tick_size,
    )


def _prepare_bids(book: OrderBookSnapshot, profile: ScenarioProfile) -> list:
    return _apply_bid_slippage(
        _apply_depth_factor(book.bids, profile.depth_factor),
        profile.slippage_ticks,
        book.tick_size,
    )


def _truncate_pair_fills(
    walk: WalkResult, qty: Decimal
) -> tuple[list[FillSlice], list[FillSlice]]:
    yes_fills: list[FillSlice] = []
    no_fills: list[FillSlice] = []
    remaining = qty
    for yf, nf in zip(walk.yes_fills, walk.no_fills):
        if remaining <= ZERO:
            break
        take = min(remaining, yf.size, nf.size)
        y_fee = (yf.fee * take / yf.size) if yf.size else ZERO
        n_fee = (nf.fee * take / nf.size) if nf.size else ZERO
        yes_fills.append(
            FillSlice(price=yf.price, size=take, notional=take * yf.price, fee=y_fee)
        )
        no_fills.append(
            FillSlice(price=nf.price, size=take, notional=take * nf.price, fee=n_fee)
        )
        remaining -= take
    return yes_fills, no_fills


def _cost_for_qty(
    fills: list[FillSlice], qty: Decimal, *, from_end: bool = False
) -> tuple[Decimal, Decimal]:
    remaining = qty
    cost = ZERO
    fee = ZERO
    seq = list(reversed(fills)) if from_end else fills
    for f in seq:
        if remaining <= ZERO:
            break
        take = min(remaining, f.size)
        cost += take * f.price
        fee += (f.fee * take / f.size) if f.size else ZERO
        remaining -= take
    return cost, fee


def simulate_forward(
    market: MarketInfo,
    yes_t0: OrderBookSnapshot,
    no_t0: OrderBookSnapshot,
    profile: ScenarioProfile,
    *,
    target_quantity: Decimal | None = None,
    yes_delayed: OrderBookSnapshot | None = None,
    no_delayed: OrderBookSnapshot | None = None,
    second_leg_fill_ratio: Decimal | None = None,
) -> SimulationResult:
    """Simulate buying YES+NO under a scenario profile."""
    schedule: FeeSchedule | None = market.fee_schedule
    fees_enabled = market.fees_enabled
    if (
        profile.delay_ms > 0
        and (yes_delayed is None or no_delayed is None)
        and yes_t0.token_id
        and no_t0.token_id
    ):
        try:
            from polymarket_scanner.config import get_config
            from polymarket_scanner.discovery.orderbook_collector import snapshot_in_window

            tol = get_config().scanner.observed_delay_tolerance_ms
            target_y = yes_t0.fetched_at + timedelta(milliseconds=profile.delay_ms)
            target_n = no_t0.fetched_at + timedelta(milliseconds=profile.delay_ms)
            if yes_delayed is None:
                yes_delayed = snapshot_in_window(
                    yes_t0.token_id, target=target_y, tolerance_ms=tol
                )
            if no_delayed is None:
                no_delayed = snapshot_in_window(
                    no_t0.token_id, target=target_n, tolerance_ms=tol
                )
        except Exception:
            pass

    yes_exec, no_exec, quality = select_delayed_books(
        yes_t0,
        no_t0,
        delay_ms=profile.delay_ms,
        yes_later=yes_delayed,
        no_later=no_delayed,
    )

    # Discover size on t0
    walk_t0 = find_optimal_forward_arb(
        yes_t0,
        no_t0,
        schedule,
        fees_enabled=fees_enabled,
        operational_cost=ZERO,
        safety_buffer=ZERO,
        depth_factor=profile.depth_factor,
        slippage_ticks=profile.slippage_ticks,
        is_taker=profile.is_taker,
    )
    if walk_t0 is None or walk_t0.quantity <= ZERO:
        return SimulationResult(
            profile=profile.name,
            quality=quality,
            quantity=ZERO,
            gross_profit=ZERO,
            fees=ZERO,
            operational_cost=profile.operational_cost,
            safety_buffer=profile.safety_buffer,
            net_profit=ZERO,
            details="No forward arb depth at t0",
            risk_tags=["no opportunity"],
        )

    qty = target_quantity if target_quantity is not None else walk_t0.quantity
    qty = min(qty, walk_t0.quantity)
    ratio = second_leg_fill_ratio if second_leg_fill_ratio is not None else profile.partial_second_leg_ratio
    ratio = max(ZERO, min(ONE, ratio))

    yes_asks = _prepare_asks(yes_exec, profile)
    no_asks = _prepare_asks(no_exec, profile)

    first_side = OutcomeSide.YES if profile.first_leg.upper() == "YES" else OutcomeSide.NO
    if first_side == OutcomeSide.YES:
        first_asks, second_asks = yes_asks, no_asks
        first_label = OutcomeSide.YES
    else:
        first_asks, second_asks = no_asks, yes_asks
        first_label = OutcomeSide.NO

    # First leg
    f_filled, _, _, f_fills, _ = walk_buy_asks(
        first_asks, qty, schedule, fees_enabled=fees_enabled, is_taker=profile.is_taker
    )
    # Second leg (possibly partial)
    second_target = f_filled if not profile.sequential_legs else (f_filled * ratio)
    if not profile.sequential_legs:
        second_target = f_filled
    s_filled, _, _, s_fills, _ = walk_buy_asks(
        second_asks,
        second_target,
        schedule,
        fees_enabled=fees_enabled,
        is_taker=profile.is_taker,
    )

    if first_label == OutcomeSide.YES:
        yes_fills, no_fills = f_fills, s_fills
    else:
        yes_fills, no_fills = s_fills, f_fills

    # If simultaneous and no sequential partial, prefer paired walk on exec books
    if not profile.sequential_legs or ratio >= ONE:
        walk_exec = find_optimal_forward_arb(
            yes_exec,
            no_exec,
            schedule,
            fees_enabled=fees_enabled,
            operational_cost=ZERO,
            safety_buffer=ZERO,
            depth_factor=profile.depth_factor,
            slippage_ticks=profile.slippage_ticks,
            is_taker=profile.is_taker,
        )
        if walk_exec and walk_exec.quantity > ZERO:
            use_qty = min(qty, walk_exec.quantity)
            yes_fills, no_fills = _truncate_pair_fills(walk_exec, use_qty)

    yes_qty = sum((f.size for f in yes_fills), ZERO)
    no_qty = sum((f.size for f in no_fills), ZERO)
    matched = min(yes_qty, no_qty)
    unhedged = abs(yes_qty - no_qty)
    one_leg = unhedged > ZERO

    yes_matched_cost, yes_matched_fee = _cost_for_qty(yes_fills, matched)
    no_matched_cost, no_matched_fee = _cost_for_qty(no_fills, matched)
    gross = matched - yes_matched_cost - no_matched_cost
    fees = yes_matched_fee + no_matched_fee

    close_leg: SimulationLegResult | None = None
    worst_loss = ZERO
    remaining_inventory = ZERO
    unrealized_inventory_cost = ZERO
    close_realized = ZERO
    if one_leg and profile.force_close_unhedged and unhedged > ZERO:
        if yes_qty > no_qty:
            close_book, close_side, long_fills = yes_exec, OutcomeSide.YES, yes_fills
        else:
            close_book, close_side, long_fills = no_exec, OutcomeSide.NO, no_fills
        extra_cost, extra_buy_fee = _cost_for_qty(long_fills, unhedged, from_end=True)
        bids = _prepare_bids(close_book, profile)
        c_filled, proceeds, c_fees, c_fills, _ = walk_sell_bids(
            bids, unhedged, schedule, fees_enabled=fees_enabled, is_taker=True
        )
        if unhedged > ZERO and c_filled > ZERO:
            frac = c_filled / unhedged
        else:
            frac = ZERO
        closed_cost = extra_cost * frac
        closed_buy_fee = extra_buy_fee * frac
        close_realized = proceeds - closed_cost - closed_buy_fee - c_fees
        worst_loss = min(ZERO, close_realized)
        fees += c_fees + closed_buy_fee
        gross += proceeds - closed_cost
        close_leg = _leg_from_fills(close_side, "close", c_fills)
        remaining_inventory = unhedged - c_filled
        leftover_frac = (unhedged - c_filled) / unhedged if unhedged else ZERO
        unrealized_inventory_cost = (extra_cost + extra_buy_fee) * leftover_frac
        unhedged = remaining_inventory
    elif one_leg:
        # Mark potential loss if forced to exit at opposite best bid without executing
        if yes_qty > no_qty and yes_exec.bids:
            mark = yes_exec.bids[0].price
            extra_cost, extra_buy_fee = _cost_for_qty(yes_fills, unhedged, from_end=True)
            worst_loss = min(ZERO, mark * unhedged - extra_cost - extra_buy_fee)
            remaining_inventory = unhedged
            unrealized_inventory_cost = extra_cost + extra_buy_fee
        elif no_qty > yes_qty and no_exec.bids:
            mark = no_exec.bids[0].price
            extra_cost, extra_buy_fee = _cost_for_qty(no_fills, unhedged, from_end=True)
            worst_loss = min(ZERO, mark * unhedged - extra_cost - extra_buy_fee)
            remaining_inventory = unhedged
            unrealized_inventory_cost = extra_cost + extra_buy_fee

    net = gross - fees - profile.operational_cost - profile.safety_buffer
    # Remaining inventory cost is not realized P&L; net already excludes leftover acquisition.
    realized_pnl = net
    still = matched > ZERO and (yes_matched_cost + no_matched_cost) < matched

    tags: list[str] = []
    if quality == SimulationQuality.ESTIMATED:
        tags.append("estimated simulation")
    if quality == SimulationQuality.STALE:
        tags.append("stale delayed snapshot")
    if quality == SimulationQuality.UNAVAILABLE:
        tags.append("observed delay unavailable")
    if one_leg:
        tags.append("one-leg risk")
    if profile.delay_ms > 0:
        tags.append(f"delay {profile.delay_ms}ms")

    legs = [
        _leg_from_fills(OutcomeSide.YES, "yes", yes_fills),
        _leg_from_fills(OutcomeSide.NO, "no", no_fills),
    ]
    if close_leg:
        legs.append(close_leg)

    return SimulationResult(
        profile=profile.name,
        quality=quality,
        quantity=matched,
        gross_profit=gross,
        fees=fees,
        operational_cost=profile.operational_cost,
        safety_buffer=profile.safety_buffer,
        net_profit=net,
        worst_loss=worst_loss,
        unhedged_quantity=unhedged,
        still_arbitrage=still and net > ZERO,
        one_leg_risk=one_leg,
        legs=legs,
        details=(
            f"profile={profile.name} quality={quality.value} matched={matched} "
            f"unhedged={unhedged} delay_ms={profile.delay_ms} realized={realized_pnl} "
            f"remaining={remaining_inventory} t0_yes_hash={yes_t0.hash} "
            f"t0_no_hash={no_t0.hash} t0_yes_at={yes_t0.fetched_at.isoformat()} "
            f"t0_no_at={no_t0.fetched_at.isoformat()} "
            f"t0_gen={yes_t0.connection_generation}/{no_t0.connection_generation}"
        ),
        risk_tags=tags,
        realized_pnl=realized_pnl,
        unrealized_inventory_cost=unrealized_inventory_cost,
        remaining_inventory=remaining_inventory,
    )


def simulate_all_profiles(
    market: MarketInfo,
    yes_t0: OrderBookSnapshot,
    no_t0: OrderBookSnapshot,
    *,
    yes_delayed: OrderBookSnapshot | None = None,
    no_delayed: OrderBookSnapshot | None = None,
) -> dict[str, SimulationResult]:
    profiles = get_builtin_profiles()
    return {
        name: simulate_forward(
            market,
            yes_t0,
            no_t0,
            profile,
            yes_delayed=yes_delayed,
            no_delayed=no_delayed,
        )
        for name, profile in profiles.items()
    }
