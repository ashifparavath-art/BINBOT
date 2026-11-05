"""Set the Phase 1 Option C (final) SCANNER_* values in .env, in one command.

    .venv/bin/python3 scripts/apply_phase1_option_c_env.py            # show what would change
    .venv/bin/python3 scripts/apply_phase1_option_c_env.py --apply    # back up .env, then write

- Only the SCANNER_* keys listed below are touched; every other line
  (API keys, BINANCE_TESTNET, ATR_*, TELEGRAM_*, ...) is left exactly as is.
- Existing keys are replaced in place; missing ones are appended at the end.
- Before writing, the current file is copied to .env.bak-phase1-<timestamp>.
- Never prints values of keys outside the list, so no secrets reach the screen.
"""
from __future__ import annotations

import shutil
import sys
import time
from pathlib import Path

VALUES = {
    "SCANNER_TRADE_DIRECTION": "follow",
    "SCANNER_PRICE_JUMP_PCT": "10",
    "SCANNER_PRICE_JUMP_WINDOW_MIN": "20",
    "SCANNER_FUNDING_TREND_WINDOW_MIN": "20",
    "SCANNER_LEVERAGE": "3",
    "SCANNER_NOTIONAL_USDT": "100",
    "SCANNER_LADDER_ENABLED": "true",
    "SCANNER_STOP_LOSS_PCT": "10",
    "SCANNER_LADDER_TRAIL_START_PCT": "15",
    "SCANNER_LADDER_TRAIL_START_LOCK_PCT": "-5",
    "SCANNER_LADDER_TRAIL_STEP_PCT": "1",
    "SCANNER_LADDER_TRAIL_STEP_LOCK_PCT": "1",
    "SCANNER_MAX_HOLD_HOURS": "72",
    "SCANNER_MAX_OPEN_POSITIONS": "6",
    "SCANNER_MAX_YOUNG_POSITIONS": "4",
    "SCANNER_YOUNG_AGE_HOURS": "24",
    "SCANNER_REENTRY_PAUSE_S": "120",
    "SCANNER_MAX_DAILY_LOSS_USDT": "200",
}


def main() -> int:
    apply = "--apply" in sys.argv
    path = Path(sys.argv[sys.argv.index("--env") + 1]) if "--env" in sys.argv else Path(".env")
    if not path.exists():
        print(f"{path} not found — run this from the BINBOT folder")
        return 1
    lines = path.read_text(encoding="utf-8").splitlines()
    seen: set[str] = set()
    out: list[str] = []
    changes: list[str] = []
    for line in lines:
        key = line.split("=", 1)[0].strip() if "=" in line and not line.lstrip().startswith("#") else None
        if key in VALUES:
            if key in seen:  # drop duplicate definitions so only one value remains
                changes.append(f"  removed duplicate {key}")
                continue
            seen.add(key)
            old = line.split("=", 1)[1].strip()
            new = VALUES[key]
            if old != new:
                changes.append(f"  {key}: {old} -> {new}")
            out.append(f"{key}={new}")
        else:
            out.append(line)
    missing = [k for k in VALUES if k not in seen]
    if missing:
        out.append("")
        out.append("# --- Phase 1 Option C (final), added by scripts/apply_phase1_option_c_env.py ---")
        for k in missing:
            out.append(f"{k}={VALUES[k]}")
            changes.append(f"  {k}: (not set) -> {VALUES[k]}")

    if not changes:
        print("Nothing to change — .env already has the Phase 1 Option C values.")
        return 0
    print("Changes:" if apply else "Would change (run again with --apply to write):")
    print("\n".join(changes))
    if apply:
        backup = path.with_name(f"{path.name}.bak-phase1-{time.strftime('%Y%m%d-%H%M%S')}")
        shutil.copy2(path, backup)
        path.write_text("\n".join(out) + "\n", encoding="utf-8")
        print(f"Written. Backup of the old file: {backup}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
