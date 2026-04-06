"""Canonical Airflow batch date helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ModuleNotFoundError:  # pragma: no cover - Python 3.8 Airflow fallback
    ZoneInfo = None  # type: ignore[assignment]
    import pendulum

ET_TIMEZONE = "America/New_York"


def et_timezone():
    """Return the ET timezone object for the current runtime."""
    if ZoneInfo is not None:
        return ZoneInfo(ET_TIMEZONE)
    return pendulum.timezone(ET_TIMEZONE)


def et_datetime(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """Return an ET-aware datetime across Python 3.8+ runtimes."""
    if ZoneInfo is not None:
        return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(ET_TIMEZONE))
    return pendulum.datetime(year, month, day, hour, minute, tz=ET_TIMEZONE)


def canonical_data_date(logical_date: datetime) -> date:
    """Return the ET business date minus one day for an Airflow logical date."""
    if logical_date.tzinfo is None:
        raise ValueError("logical_date must be timezone-aware")
    et_logical_date = logical_date.astimezone(et_timezone())
    return et_logical_date.date() - timedelta(days=1)


def canonical_data_date_iso(logical_date: datetime) -> str:
    """Return the canonical batch data_date as YYYY-MM-DD."""
    return canonical_data_date(logical_date).isoformat()


def canonical_data_date_from_iso_logical_date(logical_date: str) -> str:
    """Parse an ISO logical date string and return the canonical data_date."""
    normalized_logical_date = logical_date.replace("Z", "+00:00")
    return canonical_data_date_iso(datetime.fromisoformat(normalized_logical_date))


def date_range(start_date: str, end_date: str) -> list[str]:
    """Return an ordered list of YYYY-MM-DD strings from start_date to end_date inclusive.

    Raises ValueError if:
    - start_date > end_date
    - end_date >= today (future dates not allowed)
    - the range spans more than 90 dates
    """
    today = date.today()
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(
            f"start_date {start_date} must be <= end_date {end_date}"
        )
    if end >= today:
        raise ValueError(
            f"end_date {end_date} must be before today {today.isoformat()}"
        )
    if (end - start).days + 1 > 90:
        raise ValueError(
            f"Date range exceeds 90 days: {(end - start).days + 1} dates requested"
        )

    result: list[str] = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result
