"""Approved INV-CUTOFF-v1: observed near midnight, never reconstructed historic stock."""

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from shopvn.common.time import utc_instant

VN = ZoneInfo("Asia/Ho_Chi_Minh")
POLICY_VERSION = "INV-CUTOFF-v1"


@dataclass(frozen=True)
class InventoryPolicy:
    go_live_date: date
    window_seconds: int = 60

    @classmethod
    def from_env(cls):
        raw = os.getenv("INVENTORY_GO_LIVE_DATE", "")
        if not raw or raw.startswith("replace-"):
            raise ValueError("Configure INVENTORY_GO_LIVE_DATE before inventory go-live")
        return cls(date.fromisoformat(raw))

    def classify(self, observed_at, *, mode="current", business_date=None):
        instant = utc_instant(observed_at)
        local = instant.astimezone(VN)
        if local.date() < self.go_live_date:
            raise ValueError("Inventory observation is before configured go-live")
        if mode not in {"current", "eod"}:
            raise ValueError("Unknown inventory capture mode")
        day = date.fromisoformat(business_date) if isinstance(business_date, str) else business_date
        day = day or (local.date() - timedelta(days=1) if mode == "eod" else local.date())
        if day < self.go_live_date:
            raise ValueError("Inventory history before go-live is blocked (BR32)")
        if mode == "current" and day != local.date():
            raise ValueError("Current stock cannot be relabelled as a different business day")
        cutoff = datetime.combine(day + timedelta(days=1), time.min, VN)
        lag = (instant - cutoff).total_seconds()
        eligible = mode == "eod" and 0 <= lag < self.window_seconds
        return {
            "policy_version": POLICY_VERSION,
            "go_live_date": self.go_live_date.isoformat(),
            "mode": mode,
            "business_date": day.isoformat(),
            "observed_at_utc": instant.isoformat(),
            "cutoff_at": cutoff.isoformat(),
            "capture_lag_seconds": lag if mode == "eod" else None,
            "window_seconds": self.window_seconds,
            "eod_eligible": eligible,
            "snapshot_method": "observed_eod_cutoff_window" if eligible else "current_observed",
        }


def eod_publication_missing(now, go_live_date, last_eod_day):
    local = utc_instant(now).astimezone(VN)
    expected = local.date() - timedelta(days=1)
    if expected < go_live_date or local.time() < time(0, 10):
        return False
    return last_eod_day != expected
