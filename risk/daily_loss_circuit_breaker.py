"""Daily-loss circuit breaker in USDT, used by BINBOT2.

Same behaviour as `DrawdownCircuitBreaker` (same session handling, daily
auto-reset at UTC midnight, snapshot/restore, force_halt) but the limit is
a fixed USDT amount instead of a % of equity:

    halt when  max(start_equity - equity, peak_equity - equity) >= limit

i.e. the day's loss from the starting balance OR the give-back from the
day's high, whichever is bigger — the same two measures the % breaker
uses. Equity is Binance's total margin balance, so open positions'
unrealized P&L counts too.

When tripped: new entries stop and working orders are cancelled; open
positions are still managed by the position monitor (stops keep working).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from config.logging_config import get_logger
from risk.circuit_breaker import DrawdownCircuitBreaker

logger = get_logger(__name__)


@dataclass
class DailyLossCircuitBreaker(DrawdownCircuitBreaker):
    max_daily_loss_usdt: float = field(default=100.0, kw_only=True)
    # True (BINBOT2 default): loss = max(drop from day start, give-back from
    # the day's high). False (BINBOT1): only the drop from the day's
    # starting balance counts, so giving back open profit on a good day
    # can't trip the halt.
    include_peak_giveback: bool = field(default=True, kw_only=True)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.max_daily_loss_usdt <= 0:
            raise ValueError("max_daily_loss_usdt must be positive")

    def update(self, current_equity: float, *, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        if now.date() != self._session_date:
            logger.warning(
                "circuit_breaker.auto_session_reset",
                old_session_date=str(self._session_date),
                new_session_date=str(now.date()),
                starting_equity=current_equity,
                msg="update() detected a new calendar day; auto-resetting session",
            )
            self.reset_session(current_equity, session_date=now.date())

        self._peak_equity = max(self._peak_equity, current_equity)
        if self._halted:
            return False

        loss_from_start = self.starting_equity - current_equity
        loss_from_peak = self._peak_equity - current_equity
        loss = max(loss_from_start, loss_from_peak) if self.include_peak_giveback else loss_from_start
        if loss >= self.max_daily_loss_usdt:
            self._halted = True
            self._halt_reason = (
                f"daily loss {loss:.2f} USDT >= limit {self.max_daily_loss_usdt:.2f} USDT "
                f"(equity={current_equity:.2f}, start={self.starting_equity:.2f}, "
                f"peak={self._peak_equity:.2f})"
            )
            logger.error("circuit_breaker.tripped", reason=self._halt_reason)
            return True
        return False
