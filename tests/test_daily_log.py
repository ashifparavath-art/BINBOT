"""Tests for the append-only JSONL log backing dashboard history backfill.

The whole point of this module is surviving a process restart, so the
tests focus on exactly that: does a fresh read after a restart (a new
DailyJsonlLog instance, same path) see everything a prior instance wrote,
including across a simulated day boundary, and does it degrade gracefully
(never raise) on a corrupted line or an unwritable path.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from utils.daily_log import DailyJsonlLog


class TestAppendAndReadRecent:
    def test_round_trips_in_order(self, tmp_path):
        log = DailyJsonlLog(str(tmp_path / "events"))
        log.append({"a": 1})
        log.append({"a": 2})

        assert log.read_recent() == [{"a": 1}, {"a": 2}]

    def test_fresh_log_with_no_prior_writes_returns_empty(self, tmp_path):
        log = DailyJsonlLog(str(tmp_path / "events"))
        assert log.read_recent() == []

    def test_a_new_instance_at_the_same_path_sees_prior_writes(self, tmp_path):
        # Simulates a process restart: a brand new DailyJsonlLog object,
        # same path_prefix, must recover everything the old one wrote.
        path_prefix = str(tmp_path / "events")
        DailyJsonlLog(path_prefix).append({"a": "before restart"})

        recovered = DailyJsonlLog(path_prefix).read_recent()

        assert recovered == [{"a": "before restart"}]

    def test_read_recent_includes_yesterdays_file(self, tmp_path):
        log = DailyJsonlLog(str(tmp_path / "events"))
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        path = log._path_for(yesterday)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"a": "yesterday"}) + "\n", encoding="utf-8")
        log.append({"a": "today"})

        result = log.read_recent(hours=48)

        assert {"a": "yesterday"} in result
        assert {"a": "today"} in result

    def test_malformed_line_is_skipped_not_raised(self, tmp_path):
        log = DailyJsonlLog(str(tmp_path / "events"))
        path = log._path_for(datetime.now(timezone.utc))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"a": 1}\nnot valid json\n{"a": 2}\n', encoding="utf-8")

        assert log.read_recent() == [{"a": 1}, {"a": 2}]

    def test_append_failure_is_swallowed_not_raised(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("occupied by a file, not a directory", encoding="utf-8")
        log = DailyJsonlLog(str(blocker / "events"))

        log.append({"a": 1})  # must not raise despite an unwritable path
