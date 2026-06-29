"""Risk controls — pure functions over ``(signal, state, limits)``.

Two kinds live here:

- **Sizing** (:func:`size_position`): fixed-fractional ``qty = risk$ / |entry −
  stop|``, floored to whole shares (bracket orders cannot be fractional — Alpaca
  reserves fractional/notional for TIF-day simple orders), scaled by the
  drawdown-ladder halve rung.
- **Gates** (``check_*``): each returns a :class:`ControlCheck` verdict. The
  engine runs *all* of them (no short-circuit) so every order's audit trail
  records the whole picture, then approves iff every gate passed.

The volatility halt is wired as an always-pass hook: its real trigger (realized
vol / VIX) needs the Phase 4 regime feed, and faking it now would be a lie in
the risk layer. Keeping the control visible means turning it on later is one
line, not a new seam.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING

from app.engine.risk.approval import ControlCheck
from app.models.enums import OrderType

if TYPE_CHECKING:
    from app.core.config import Settings
    from app.engine.events import SignalEvent
    from app.engine.risk.state import RiskState

_HUNDRED = Decimal("100")

# Entry order types the engine can build today. Stop / stop-limit entries need a
# trigger price the SignalEvent does not carry (its stop_price is the protective
# bracket leg, not an entry trigger), so they are rejected explicitly here rather
# than silently failing OrderRequest validation downstream.
_SUPPORTED_ENTRY_TYPES = (OrderType.market, OrderType.limit)


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Resolved risk knobs for one evaluation (env-tunable via Settings)."""

    default_risk_pct: Decimal
    unproven_risk_pct: Decimal
    max_risk_pct: Decimal
    daily_loss_limit_pct: Decimal
    daily_loss_risk_multiple: Decimal
    max_consecutive_losses: int
    drawdown_halve_pct: Decimal
    drawdown_halt_pct: Decimal
    margin_headroom_factor: Decimal
    flatten_buffer_minutes: int
    symbol_cooldown_seconds: int

    @classmethod
    def from_settings(cls, settings: Settings) -> RiskLimits:
        return cls(
            default_risk_pct=settings.risk_per_trade_pct,
            unproven_risk_pct=settings.unproven_risk_per_trade_pct,
            max_risk_pct=settings.max_risk_per_trade_pct,
            daily_loss_limit_pct=settings.daily_loss_limit_pct,
            daily_loss_risk_multiple=settings.daily_loss_risk_multiple,
            max_consecutive_losses=settings.max_consecutive_losses,
            drawdown_halve_pct=settings.drawdown_halve_pct,
            drawdown_halt_pct=settings.drawdown_halt_pct,
            margin_headroom_factor=settings.margin_headroom_factor,
            flatten_buffer_minutes=settings.flatten_buffer_minutes,
            symbol_cooldown_seconds=settings.symbol_cooldown_seconds,
        )


@dataclass(frozen=True, slots=True)
class Sizing:
    """Result of :func:`size_position`: the share qty plus the math behind it.

    ``check.passed`` is False when the position cannot be sized (entry==stop, or
    the sized qty rounds below one whole share) — in which case ``qty`` is zero
    and the engine rejects without building an order.
    """

    qty: Decimal
    risk_pct: Decimal
    risk_amount: Decimal
    drawdown_factor: Decimal
    check: ControlCheck


# ── Sizing ───────────────────────────────────────────────────────────


def effective_risk_pct(
    limits: RiskLimits, *, strategy_risk_pct: Decimal | None, strategy_proven: bool
) -> Decimal:
    """Resolve risk-per-trade %: strategy override or default, clamped.

    Unproven strategies (anything not yet ``live``) are clamped to the unproven
    ceiling (0.25%); everything is clamped to the 2% hard ceiling regardless of
    what the strategy row asks for.
    """
    base = strategy_risk_pct if strategy_risk_pct is not None else limits.default_risk_pct
    if not strategy_proven:
        base = min(base, limits.unproven_risk_pct)
    return min(base, limits.max_risk_pct)


def drawdown_size_factor(limits: RiskLimits, state: RiskState) -> Decimal:
    """Drawdown-ladder size multiplier. Halve rung here; the halt rung is a gate."""
    if state.drawdown_pct >= limits.drawdown_halve_pct:
        return Decimal("0.5")
    return Decimal("1")


def size_position(signal: SignalEvent, state: RiskState, limits: RiskLimits) -> Sizing:
    """Fixed-fractional sizing, floored to whole shares, drawdown-scaled."""
    risk_pct = effective_risk_pct(
        limits, strategy_risk_pct=state.strategy_risk_pct, strategy_proven=state.strategy_proven
    )
    risk_amount = state.equity * risk_pct / _HUNDRED
    dd_factor = drawdown_size_factor(limits, state)
    per_share_risk = abs(signal.entry_price - signal.stop_price)

    if per_share_risk <= 0:
        return Sizing(
            Decimal("0"),
            risk_pct,
            risk_amount,
            dd_factor,
            ControlCheck(
                "sizing",
                passed=False,
                detail="entry equals stop — no risk distance to size against",
                observed=f"entry={signal.entry_price} stop={signal.stop_price}",
            ),
        )

    qty = (risk_amount * dd_factor / per_share_risk).to_integral_value(rounding=ROUND_DOWN)
    if qty < 1:
        return Sizing(
            Decimal("0"),
            risk_pct,
            risk_amount,
            dd_factor,
            ControlCheck(
                "sizing",
                passed=False,
                detail="sized quantity rounds below one whole share",
                limit="qty>=1",
                observed=str(qty),
            ),
        )

    dd_note = f" × dd {dd_factor}" if dd_factor != 1 else ""
    return Sizing(
        qty,
        risk_pct,
        risk_amount,
        dd_factor,
        ControlCheck(
            "sizing",
            passed=True,
            detail=f"risk {risk_pct}% of {state.equity} ÷ {per_share_risk}/sh{dd_note}",
            observed=str(qty),
        ),
    )


# ── Gates ────────────────────────────────────────────────────────────


def check_account_tradeable(state: RiskState) -> ControlCheck:
    reasons = []
    if state.trading_blocked:
        reasons.append("trading_blocked")
    if state.account_blocked:
        reasons.append("account_blocked")
    if state.trading_halted:
        reasons.append("kill_switch")
    ok = not reasons
    return ControlCheck(
        "account_tradeable",
        passed=ok,
        detail="account and operator blocks clear" if ok else "blocked: " + ", ".join(reasons),
    )


def check_entry_order_type(signal: SignalEvent) -> ControlCheck:
    ok = signal.order_type in _SUPPORTED_ENTRY_TYPES
    return ControlCheck(
        "entry_order_type",
        passed=ok,
        detail="market/limit entry"
        if ok
        else f"{signal.order_type.value} entries are not supported "
        "(only market/limit; stop-triggered entries need a trigger price)",
        observed=signal.order_type.value,
    )


def check_extended_hours(signal: SignalEvent) -> ControlCheck:
    if signal.extended_hours:
        return ControlCheck(
            "extended_hours",
            passed=False,
            detail="extended-hours entries need self-managed exits (Phase 2c) — blocked",
            observed="extended_hours=True",
        )
    return ControlCheck("extended_hours", passed=True, detail="RTH entry")


def check_session_open(state: RiskState, limits: RiskLimits) -> ControlCheck:
    if not state.market_open:
        return ControlCheck(
            "session_open", passed=False, detail="market is closed", observed="closed"
        )
    cutoff = state.next_close - timedelta(minutes=limits.flatten_buffer_minutes)
    if state.now >= cutoff:
        return ControlCheck(
            "session_open",
            passed=False,
            detail=f"within {limits.flatten_buffer_minutes}m of close — no new entries",
            limit=cutoff.isoformat(),
            observed=state.now.isoformat(),
        )
    return ControlCheck("session_open", passed=True, detail="RTH, outside the flatten buffer")


def check_daily_loss(state: RiskState, limits: RiskLimits, risk_amount: Decimal) -> ControlCheck:
    strat_limit = limits.daily_loss_risk_multiple * risk_amount
    if strat_limit > 0 and state.day_realized_pnl_strategy <= -strat_limit:
        return ControlCheck(
            "daily_loss",
            passed=False,
            detail="strategy daily realized-loss limit hit — halt new entries this session",
            limit=f"-{strat_limit}",
            observed=str(state.day_realized_pnl_strategy),
        )
    port_limit = limits.daily_loss_limit_pct / _HUNDRED * state.equity
    if port_limit > 0 and state.day_pnl_portfolio <= -port_limit:
        return ControlCheck(
            "daily_loss",
            passed=False,
            detail="portfolio daily-loss limit hit — halt new entries this session",
            limit=f"-{port_limit}",
            observed=str(state.day_pnl_portfolio),
        )
    return ControlCheck(
        "daily_loss", passed=True, detail="within strategy and portfolio loss limits"
    )


def check_consecutive_losses(state: RiskState, limits: RiskLimits) -> ControlCheck:
    hit = state.consecutive_losses >= limits.max_consecutive_losses
    return ControlCheck(
        "consecutive_losses",
        passed=not hit,
        detail="consecutive-loss limit hit — strategy paused pending review"
        if hit
        else "loss streak within limit",
        limit=str(limits.max_consecutive_losses),
        observed=str(state.consecutive_losses),
    )


def check_drawdown_halt(state: RiskState, limits: RiskLimits) -> ControlCheck:
    hit = state.drawdown_pct >= limits.drawdown_halt_pct
    return ControlCheck(
        "drawdown_halt",
        passed=not hit,
        detail="max-drawdown halt — manual re-arm required"
        if hit
        else f"drawdown {state.drawdown_pct:.2f}% under halt threshold",
        limit=f"{limits.drawdown_halt_pct}%",
        observed=f"{state.drawdown_pct:.2f}%",
    )


def check_margin_headroom(
    state: RiskState, limits: RiskLimits, *, qty: Decimal, entry: Decimal
) -> ControlCheck:
    new_notional = qty * entry
    projected = abs(state.position_market_value) + new_notional
    cap = state.buying_power * limits.margin_headroom_factor
    ok = projected <= cap
    return ControlCheck(
        "margin_headroom",
        passed=ok,
        detail="projected exposure within buying-power headroom"
        if ok
        else "projected exposure exceeds buying-power headroom (iron law #3)",
        limit=str(cap),
        observed=str(projected),
    )


def check_max_positions(state: RiskState) -> ControlCheck:
    if state.strategy_max_positions is None:
        return ControlCheck("max_positions", passed=True, detail="no max-positions cap configured")
    ok = state.open_positions_count < state.strategy_max_positions
    return ControlCheck(
        "max_positions",
        passed=ok,
        detail="below max concurrent positions" if ok else "max concurrent positions reached",
        limit=str(state.strategy_max_positions),
        observed=str(state.open_positions_count),
    )


def check_no_pyramiding(state: RiskState) -> ControlCheck:
    flat = state.strategy_open_qty == 0
    return ControlCheck(
        "no_pyramiding",
        passed=flat,
        detail="strategy is flat in symbol — entry allowed"
        if flat
        else "strategy already holds symbol — no add until flat",
        observed=str(state.strategy_open_qty),
    )


def check_cooldown(state: RiskState, limits: RiskLimits) -> ControlCheck:
    if state.last_entry_at is None:
        return ControlCheck("cooldown", passed=True, detail="no prior entry for symbol")
    elapsed = (state.now - state.last_entry_at).total_seconds()
    ok = elapsed >= limits.symbol_cooldown_seconds
    return ControlCheck(
        "cooldown",
        passed=ok,
        detail="symbol cooldown elapsed" if ok else "symbol cooldown active",
        limit=f"{limits.symbol_cooldown_seconds}s",
        observed=f"{elapsed:.0f}s",
    )


def check_volatility_halt() -> ControlCheck:
    return ControlCheck(
        "volatility_halt",
        passed=True,
        detail="disabled until Phase 4 regime/vol feed (hook in place)",
    )
