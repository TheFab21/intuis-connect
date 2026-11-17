"""Absolute level the hourly series is anchored on.

The hourly series on its own is relative, so these contracts are what stop a
missed or revised hour from staying in the cumulative sum for good.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from custom_components.intuis_connect.energy_base import (
    daily_total,
    level_from,
    levels_for,
    local_midnight,
    settled_day,
    settled_period,
)

ZONE = ZoneInfo("Europe/Zurich")
NOW = datetime(2026, 11, 17, 9, 30, tzinfo=timezone.utc)


def noon(day: date) -> int:
    """The cloud stamps a day at its local noon."""
    return int(datetime(day.year, day.month, day.day, 12, tzinfo=ZONE).timestamp())


def test_settled_day_leaves_the_cloud_a_full_day() -> None:
    assert settled_day(NOW, ZONE) == date(2026, 11, 15)


def test_settled_day_refuses_a_naive_clock() -> None:
    with pytest.raises(ValueError):
        settled_day(datetime(2026, 11, 17, 9, 30), ZONE)


def test_the_period_ends_where_the_hourly_window_starts() -> None:
    """A level and the window it anchors must meet exactly."""
    period = settled_period(NOW, ZONE)
    assert period.day == date(2026, 11, 15)
    assert period.begin == int(local_midnight(date(2026, 11, 15), ZONE).timestamp())
    assert period.end == int(local_midnight(date(2026, 11, 16), ZONE).timestamp())


def test_a_day_that_gains_an_hour_is_bounded_in_local_time() -> None:
    """Europe/Zurich leaves summer time on 25 October 2026: a 25-hour day."""
    period = settled_period(datetime(2026, 10, 27, 9, tzinfo=timezone.utc), ZONE)
    assert period.day == date(2026, 10, 25)
    assert period.end - period.begin == 90000


def test_daily_total_maps_local_noon_to_its_day_in_kwh() -> None:
    day = date(2026, 11, 15)
    assert daily_total([(noon(day), 4200.0)], ZONE, day) == 4.2


def test_daily_total_ignores_another_day() -> None:
    assert daily_total([(noon(date(2026, 11, 14)), 4200.0)], ZONE, date(2026, 11, 15)) is None


def test_a_day_the_cloud_never_measured_has_no_total() -> None:
    """None, never a fabricated zero: the caller leaves the level alone."""
    assert daily_total([], ZONE, date(2026, 11, 15)) is None


def test_daily_total_refuses_two_measurements_for_one_day() -> None:
    day = date(2026, 11, 15)
    with pytest.raises(ValueError):
        daily_total([(noon(day), 1.0), (noon(day) + 60, 2.0)], ZONE, day)


@pytest.mark.parametrize("value", [-1.0, float("nan"), True])
def test_daily_total_refuses_an_invalid_measurement(value: object) -> None:
    day = date(2026, 11, 15)
    with pytest.raises(ValueError):
        daily_total([(noon(day), value)], ZONE, day)


def test_the_level_closes_the_day_on_its_opening_sum() -> None:
    assert level_from(100.0, 7.3) == 107.3


def test_without_a_measurement_there_is_no_level() -> None:
    assert level_from(100.0, None) is None


@pytest.mark.parametrize("anchor", [-1.0, float("nan")])
def test_an_unusable_anchor_is_refused(anchor: float) -> None:
    with pytest.raises(ValueError):
        level_from(anchor, 1.0)


def test_only_the_rooms_the_cloud_measured_get_a_level() -> None:
    levels = levels_for({"a": 10.0, "b": 20.0}, {"a": 1.5, "b": None})
    assert levels == {"a": 11.5}
