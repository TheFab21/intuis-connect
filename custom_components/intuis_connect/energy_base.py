"""Absolute level the hourly series is anchored on.

Adapted from TheFab21's Node-RED flow (TheFab21/Time-Planning-intuis).

The hourly series on its own is relative: ``_replace_windows`` continues sums
from the row preceding the written period, so a missed or revised hour enters
the cumulative sum and stays there, and an overlapping re-import shifts
everything after it.

The level fixes that. For the last local day the cloud has had time to
consolidate, it is the series' own sum at that day's start plus the cloud's
``1day`` measure for the day itself. Hours after that day are then written
against it, so they recompute identically on a re-import and a gap in the
hours of that day is absorbed by its daily total.

Nothing is persisted: the anchor is read back from the recorder each cycle,
which is also where the hourly progress itself lives.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math
from typing import Mapping
from zoneinfo import ZoneInfo

# The cloud gets a full day to finish revising a day before it is taken as
# final, so the level describes the day before yesterday.
SETTLE_DAYS = 2


def local_midnight(day: date, zone: ZoneInfo) -> datetime:
    """Start of a local day, as an aware datetime."""
    return datetime(day.year, day.month, day.day, tzinfo=zone)


def settled_day(now: datetime, zone: ZoneInfo, settle_days: int = SETTLE_DAYS) -> date:
    """Last local day the cloud is assumed to have consolidated."""
    if now.tzinfo is None:
        raise ValueError("Settled day requires an aware clock")
    return now.astimezone(zone).date() - timedelta(days=settle_days)


@dataclass(frozen=True)
class SettledPeriod:
    """The consolidated day, and the UTC bounds that frame it.

    ``end`` is both the exclusive end of the day and the first second the
    hourly pass covers, so a level and the window it anchors always meet.
    """

    day: date
    begin: int
    end: int


def settled_period(now: datetime, zone: ZoneInfo) -> SettledPeriod:
    """Bounds of the consolidated day, in local time so a DST day fits."""
    day = settled_day(now, zone)
    return SettledPeriod(
        day=day,
        begin=int(local_midnight(day, zone).timestamp()),
        end=int(local_midnight(day + timedelta(days=1), zone).timestamp()),
    )


def daily_total(
    samples: list[tuple[int, float]], zone: ZoneInfo, day: date
) -> float | None:
    """Measured kWh of one local day, or None when the cloud measured none.

    The cloud stamps a day inside it, at local noon, so a sample belongs to
    the local day containing its timestamp. None is a day without measurement,
    never a fabricated zero: the caller leaves the level alone rather than
    anchoring the series on a total that does not exist.
    """
    total: float | None = None
    for timestamp, wh in samples:
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(timestamp)
            or timestamp != int(timestamp)
            or isinstance(wh, bool)
            or not isinstance(wh, (int, float))
            or not math.isfinite(wh)
            or wh < 0
        ):
            raise ValueError("Invalid daily timestamp or energy value")
        if datetime.fromtimestamp(int(timestamp), timezone.utc).astimezone(zone).date() != day:
            continue
        if total is not None:
            raise ValueError(f"Duplicate daily measurement for {day.isoformat()}")
        total = wh / 1000.0
    return total


def level_from(anchor: float, measured: float | None) -> float | None:
    """The day's closing level: its opening sum plus what the cloud measured."""
    if measured is None:
        return None
    if not math.isfinite(anchor) or anchor < 0:
        raise ValueError("Anchor must be a finite, nonnegative sum")
    return round(anchor + measured, 3)


def levels_for(
    anchors: Mapping[str, float], measured: Mapping[str, float | None]
) -> dict[str, float]:
    """Levels of the rooms the cloud measured; the others keep none.

    All or nothing is the caller's choice, not this one: a room without a
    level simply is not anchored.
    """
    levels: dict[str, float] = {}
    for room_id, anchor in anchors.items():
        level = level_from(anchor, measured.get(room_id))
        if level is not None:
            levels[room_id] = level
    return levels
