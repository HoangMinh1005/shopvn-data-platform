"""Aware clocks for observations; never infer a timezone for source TIMESTAMP_NTZ."""

from datetime import datetime, timezone


def utc_instant(value):
    value = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Observation timestamp must include a timezone")
    # Airflow supplies Pendulum DateTime, whose arithmetic with stdlib ZoneInfo
    # can differ from datetime. Normalize to a native datetime before arithmetic.
    return datetime.fromtimestamp(value.timestamp(), timezone.utc)
