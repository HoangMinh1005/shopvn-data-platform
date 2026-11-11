from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from shopvn.orchestration.deadline import deadline_missed, runtime_deadline_missed

VN = ZoneInfo("Asia/Ho_Chi_Minh")
ENABLED = date(2026, 11, 12)


def stamp(value):
    return datetime.fromisoformat(value)


@pytest.mark.parametrize(
    "now,finished,expected",
    [
        ("2026-11-11T09:00:00+07:00", None, False),
        ("2026-11-12T07:59:59+07:00", None, False),
        ("2026-11-12T08:00:00+07:00", None, True),
        ("2026-11-12T01:00:00+00:00", None, True),
        ("2026-11-12T09:00:00+07:00", "2026-11-12T07:59:59+07:00", False),
        ("2026-11-12T09:00:00+07:00", "2026-11-12T08:00:00+07:00", True),
        ("2026-11-12T09:00:00+07:00", "2026-11-12T08:30:00+07:00", True),
        ("2026-11-13T09:00:00+07:00", "2026-11-12T07:00:00+07:00", True),
        ("2026-11-12T09:00:00+07:00", "2026-11-12T00:59:59+00:00", False),
    ],
)
def test_daily_wall_clock_deadline(now, finished, expected):
    assert deadline_missed(stamp(now), ENABLED, stamp(finished) if finished else None) is expected


def test_deadline_resets_each_vietnamese_day():
    assert not deadline_missed(datetime(2026, 11, 13, 0, 1, tzinfo=VN), ENABLED, None)


@pytest.mark.parametrize(
    "finished,expected",
    [
        (None, True),
        ("2026-11-12T03:05:00+07:00", False),
        ("2026-11-12T03:30:00+07:00", False),
        ("2026-11-12T03:30:01+07:00", True),
    ],
)
def test_runtime_callback_ignores_runs_terminated_before_deadline(finished, expected):
    assert (
        runtime_deadline_missed(stamp(finished) if finished else None, "2026-11-12T03:30:00+07:00")
        is expected
    )
