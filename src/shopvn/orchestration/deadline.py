"""Timezone-aware wall-clock SLA independent of scheduler liveness."""

from datetime import datetime, time
from zoneinfo import ZoneInfo

VN = ZoneInfo("Asia/Ho_Chi_Minh")


def deadline_missed(
    now: datetime, enabled_from, successful_scheduled_finish: datetime | None
) -> bool:
    local = now.astimezone(VN)
    if local.date() < enabled_from or local.timetz().replace(tzinfo=None) < time(8):
        return False
    if successful_scheduled_finish is None:
        return True
    completed = successful_scheduled_finish.astimezone(VN)
    return completed.date() != local.date() or completed.time() >= time(8)
