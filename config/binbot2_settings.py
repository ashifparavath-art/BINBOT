"""Settings for BINBOT2 — the ATR volatility-breakout strategy.

Kept in its own file (rather than added to config/settings.py) so binbot2
can be dropped onto any copy of the repo without touching shared code.

Signal parameters, trade size and the fixed stop-loss / take-profit live
here. Leverage, margin type and max open positions are still read from
the existing SCANNER_* settings.

Override any value in .env with an ATR_ prefix, e.g. ATR_ATR_MULTIPLIER=2.5
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ATRBreakoutSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ATR_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Candles ---
    candle_interval: str = "5m"  # Binance kline interval the strategy runs on
    # How many closed candles to keep per symbol (must cover the lookbacks below).
    history_candles: int = 60
    # Seconds to wait after a candle closes before fetching it, so Binance
    # has definitely finalised it.
    poll_delay_s: float = 3.0
    # Parallel REST requests while polling ~780 symbols. 780 calls per 5 min
    # at weight 1 each is far inside Binance's 2400/min limit.
    poll_concurrency: int = 10
    # Hard cap on request rate, so a full cycle (~780 symbols) is spread
    # over ~20s instead of bursting. Keeps well clear of Binance's limits
    # (the server has been IP-banned for bursting before).
    max_requests_per_s: float = 40.0

    # --- Signal: ATR breakout ---
    atr_period: int = 14
    mean_period: int = 20
    # Trigger when the candle closes more than this many ATRs away from the
    # rolling mean of the previous `mean_period` closes.
    atr_multiplier: float = 2.0

    # --- Filters (the VICUSDT lesson: never trust a move on a thin pair) ---
    # Absolute floor: the trigger candle itself must trade at least this much
    # USDT. Stops near-zero-volume symbols from qualifying.
    min_candle_quote_volume_usdt: float = 100_000.0
    # Relative: trigger candle volume vs. average of the previous
    # `mean_period` candles.
    min_volume_multiple: float = 2.0
    # Wick filter: for a LONG the close must sit in the top part of the
    # candle's range (0.7 = top 30%); mirrored for a SHORT. Rejects
    # spike-and-reject candles.
    min_close_position: float = 0.7

    # --- Sizing & exits (binbot2's own; leverage / margin type / max open
    # positions still come from SCANNER_*) ---
    order_notional_usdt: float = 250.0  # position size per trade, in USDT notional
    # Stop-loss as a % of the position's notional (5% of 100 USDT = 5 USDT).
    # Set to 0 to use the fixed stop_loss_usdt below instead.
    stop_loss_pct: float = 2.0
    stop_loss_usdt: float = 5.0  # fixed fallback, only used when stop_loss_pct is 0
    take_profit_usdt: float = 10.0  # close at this unrealized profit (fixed, no trailing)
    # Take-profit as a % of notional (4% of 250 = 10 USDT). 0 = use take_profit_usdt.
    # Both take-profit settings are IGNORED while the profit ladder is on.
    take_profit_pct: float = 4.0

    # --- Profit ladder (replaces the fixed take-profit), % of notional ---
    # peak >= 4%  -> stop to 0% (breakeven)
    # peak >= 8%  -> stop to +4%
    # every further +2% of peak -> stop +1%
    ladder_enabled: bool = False
    breakeven_trigger_pct: float = 4.0
    breakeven_lock_pct: float = 0.0
    trail_start_pct: float = 8.0
    trail_start_lock_pct: float = 4.0
    trail_step_pct: float = 2.0
    trail_step_lock_pct: float = 1.0

    # --- Daily loss limit (USDT) ---
    # Halt new entries once today's loss (from the day's starting balance or
    # from the day's high, whichever is bigger) reaches this. 0 = use the
    # old % limit (RISK_MAX_DAILY_DRAWDOWN_PCT) instead.
    max_daily_loss_usdt: float = 100.0

    # Don't re-enter the same symbol for this long after a signal.
    symbol_cooldown_min: float = 60.0

    # Which directions to trade.
    allow_long: bool = True
    allow_short: bool = True

    # Dashboard websocket port. 8766 is the port the old scanner used, so
    # the existing dashboard tab connects to binbot2 with no changes.
    dashboard_port: int = 8766

    @field_validator("atr_period", "mean_period")
    @classmethod
    def _validate_periods(cls, v: int) -> int:
        if v < 2:
            raise ValueError("periods must be at least 2")
        return v

    @field_validator("stop_loss_pct", "take_profit_pct")
    @classmethod
    def _validate_pct(cls, v: float) -> float:
        if not (0.0 <= v < 100.0):
            raise ValueError("percentage must be in [0, 100)")
        return v

    @field_validator("trail_step_pct", "trail_start_pct", "breakeven_trigger_pct")
    @classmethod
    def _validate_ladder_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("ladder trigger/step values must be positive")
        return v

    @field_validator("order_notional_usdt", "stop_loss_usdt", "take_profit_usdt")
    @classmethod
    def _validate_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("min_close_position")
    @classmethod
    def _validate_close_position(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("min_close_position must be in [0, 1]")
        return v

    @property
    def interval_seconds(self) -> int:
        unit = self.candle_interval[-1]
        n = int(self.candle_interval[:-1])
        return n * {"m": 60, "h": 3600, "d": 86400}[unit]


@lru_cache(maxsize=1)
def get_binbot2_settings() -> ATRBreakoutSettings:
    return ATRBreakoutSettings()
