"""Append-only JSONL persistence for dashboard history (closed trades,
audit/risk events) so a browser tab reconnecting — or the bot process
itself restarting — can be caught back up on what already happened,
instead of only ever seeing events from the moment it (re)connects.

One file per UTC calendar day, purely for simple rotation (so no single
file grows forever across a long-running deployment) — this is NOT the
same as the "today" a dashboard viewer sees, which is computed client-side
in their own local timezone from whatever `read_recent()` returns. See
server/dashboard_bridge.py for how the two are kept separate.

Same lightweight, dependency-free JSON-on-disk spirit as
utils/state_recovery.py rather than introducing a database for what is,
at this scale, a small rolling window of records.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.logging_config import get_logger

logger = get_logger(__name__)


class DailyJsonlLog:
    def __init__(self, path_prefix: str) -> None:
        self._path_prefix = Path(path_prefix)

    def _path_for(self, day: "datetime") -> Path:
        return self._path_prefix.parent / f"{self._path_prefix.name}_{day.date().isoformat()}.jsonl"

    def append(self, record: dict) -> None:
        """Best-effort: a write failure is logged, never raised — a
        history log must never be able to break the live event it's
        recording."""
        try:
            path = self._path_for(datetime.now(timezone.utc))
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str))
                f.write("\n")
        except OSError as exc:
            logger.error("daily_log.append_failed", path=str(self._path_prefix), error=str(exc))

    def read_recent(self, hours: int = 48) -> list[dict]:
        """Reads every day-file touched by the last `hours` hours (today's
        plus enough prior days to cover the window), oldest first. Wide
        enough to safely cover "today" in any viewer timezone without the
        backend needing to know what that timezone is."""
        now = datetime.now(timezone.utc)
        days_back = max(1, (hours // 24) + 2)
        records: list[dict] = []
        for i in range(days_back, -1, -1):
            day = now - timedelta(days=i)
            path = self._path_for(day)
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except OSError as exc:
                logger.error("daily_log.read_failed", path=str(path), error=str(exc))
        return records
