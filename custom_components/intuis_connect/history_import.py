"""Historical energy data import for Intuis Connect.

This module provides functionality to import historical energy data from the
Intuis cloud API into Home Assistant's statistics database. The imported data
appears on the existing sensor entities, not as separate external statistics.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import (
    Statistics,
    StatisticsShortTerm,
    StatisticsMeta,
)
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    statistics_during_period,
)
from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.recorder import session_scope
from homeassistant.helpers.storage import Store
from sqlalchemy import delete, select, and_

from .utils.const import (
    DOMAIN,
    API_DATA_DELAY_HOURS,
    MAX_DAYS_PER_HOURLY_REQUEST,
    HISTORY_IMPORT_IN_PROGRESS,
)
from .intuis_api.api import RateLimitError, APIError, CannotConnect

if TYPE_CHECKING:
    from .intuis_api.api import IntuisAPI
    from .entity.intuis_home import IntuisHome

_LOGGER = logging.getLogger(__name__)

# Storage version for import state
STORAGE_VERSION = 1
STORAGE_KEY = f"{DOMAIN}.history_import"

# Import configuration
DEFAULT_HISTORY_DAYS = 365
MAX_HISTORY_DAYS = 730
API_DELAY_SECONDS = 2.0  # Delay between API calls to avoid rate limiting

# Minimum discontinuity threshold to trigger adjustment (in kWh)
DISCONTINUITY_THRESHOLD = 1.0


async def _get_existing_statistics(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime,
    end_time: datetime | None = None,
) -> list[dict]:
    """Get existing statistics for an entity in a time range.

    Args:
        hass: Home Assistant instance.
        entity_id: The entity ID (statistic_id) to query.
        start_time: Only return statistics after this time.
        end_time: Only return statistics before this time (None = now).

    Returns:
        List of statistic entries with 'start', 'state', 'sum' keys.
    """
    try:
        instance = get_instance(hass)
        result = await instance.async_add_executor_job(
            statistics_during_period,
            hass,
            start_time,
            end_time,
            {entity_id},
            "hour",
            None,  # units (use native)
            {"sum", "state"},
        )
        return result.get(entity_id, [])
    except Exception as err:
        _LOGGER.warning(
            "Failed to query existing statistics for %s: %s",
            entity_id,
            err,
        )
        return []


async def _get_baseline_sum(
    hass: HomeAssistant,
    entity_id: str,
    room_name: str,
    import_start_time: datetime,
) -> float:
    """Get the cumulative sum baseline from existing statistics before import.

    This queries existing statistics that were recorded BEFORE the import period
    and returns the last known sum value. The import should continue from this
    baseline to avoid discontinuity.

    Args:
        hass: Home Assistant instance.
        entity_id: The entity ID (statistic_id) to query.
        room_name: Room name for logging.
        import_start_time: The start time of the import period.

    Returns:
        The last known sum value before import, or 0.0 if no prior data exists.
    """
    # Query statistics from a reasonable time before import (up to 2 years)
    query_start = import_start_time - timedelta(days=730)

    existing_stats = await _get_existing_statistics(
        hass, entity_id, query_start, import_start_time
    )

    if not existing_stats:
        _LOGGER.debug(
            "No existing statistics found before import period for %s, starting at 0",
            entity_id,
        )
        return 0.0

    # Get the last entry's sum (statistics are ordered by time)
    last_stat = existing_stats[-1]
    baseline_sum = last_stat.get("sum", 0) or 0

    _LOGGER.info(
        "Found existing statistics for %s before import: baseline sum = %.3f kWh",
        room_name,
        baseline_sum,
    )

    return baseline_sum


async def _clear_statistics_in_range(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime,
    end_time: datetime,
) -> int:
    """Clear ALL statistics for an entity within a time range.

    This deletes statistics entries at ANY hour within the range, which is
    necessary because:
    - Historical import creates entries at 00:00 UTC
    - Live sensor creates entries at various hours
    - Both coexist if timestamps don't match exactly

    By clearing before import, we ensure no conflicting entries remain.

    Args:
        hass: Home Assistant instance.
        entity_id: The statistic_id (entity_id) to clear.
        start_time: Start of the time range to clear (inclusive).
        end_time: End of the time range to clear (exclusive).

    Returns:
        Number of statistics entries deleted.
    """
    instance = get_instance(hass)

    def _do_clear() -> int:
        with session_scope(session=instance.get_session()) as session:
            # Get metadata_id for this entity
            result = session.execute(
                select(StatisticsMeta.id).where(
                    StatisticsMeta.statistic_id == entity_id
                )
            ).scalar()

            if not result:
                _LOGGER.debug(
                    "No statistics metadata found for %s, nothing to clear",
                    entity_id,
                )
                return 0

            metadata_id = result
            start_ts = start_time.timestamp()
            end_ts = end_time.timestamp()

            # Delete from Statistics (long-term) table
            deleted = session.execute(
                delete(Statistics).where(
                    and_(
                        Statistics.metadata_id == metadata_id,
                        Statistics.start_ts >= start_ts,
                        Statistics.start_ts < end_ts,
                    )
                )
            ).rowcount

            # Also clear from StatisticsShortTerm table
            session.execute(
                delete(StatisticsShortTerm).where(
                    and_(
                        StatisticsShortTerm.metadata_id == metadata_id,
                        StatisticsShortTerm.start_ts >= start_ts,
                        StatisticsShortTerm.start_ts < end_ts,
                    )
                )
            )

            return deleted

    # Retry with backoff in case of database lock
    max_retries = 3
    for attempt in range(max_retries):
        try:
            deleted = await instance.async_add_executor_job(_do_clear)
            if deleted > 0:
                _LOGGER.debug(
                    "Cleared %d statistics entries for %s in range",
                    deleted,
                    entity_id,
                )
            return deleted
        except Exception as err:
            if "database is locked" in str(err) and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2s, 4s
                _LOGGER.debug(
                    "Database locked, retrying in %ds (attempt %d/%d)",
                    wait_time, attempt + 1, max_retries
                )
                await asyncio.sleep(wait_time)
            else:
                _LOGGER.warning(
                    "Failed to clear statistics for %s: %s",
                    entity_id,
                    err,
                )
                return 0
    return 0


async def _clear_all_statistics(
    hass: HomeAssistant,
    entity_id: str,
) -> int:
    """Clear ALL statistics for an entity (all time).

    Use this when changing granularity (daily -> hourly) to avoid
    sum discontinuity issues.

    Args:
        hass: Home Assistant instance.
        entity_id: The statistic_id (entity_id) to clear.

    Returns:
        Number of statistics entries deleted.
    """
    instance = get_instance(hass)

    def _do_clear() -> int:
        with session_scope(session=instance.get_session()) as session:
            # Get metadata_id for this entity
            result = session.execute(
                select(StatisticsMeta.id).where(
                    StatisticsMeta.statistic_id == entity_id
                )
            ).scalar()

            if not result:
                _LOGGER.debug(
                    "No statistics metadata found for %s, nothing to clear",
                    entity_id,
                )
                return 0

            metadata_id = result

            # Delete ALL entries from Statistics (long-term) table
            deleted = session.execute(
                delete(Statistics).where(
                    Statistics.metadata_id == metadata_id,
                )
            ).rowcount

            # Also clear from StatisticsShortTerm table
            session.execute(
                delete(StatisticsShortTerm).where(
                    StatisticsShortTerm.metadata_id == metadata_id,
                )
            )

            _LOGGER.info(
                "Cleared all %d statistics entries for %s",
                deleted,
                entity_id,
            )

            return deleted

    # Retry with backoff in case of database lock
    max_retries = 3
    for attempt in range(max_retries):
        try:
            deleted = await instance.async_add_executor_job(_do_clear)
            return deleted
        except Exception as err:
            if "database is locked" in str(err) and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2s, 4s
                _LOGGER.debug(
                    "Database locked, retrying in %ds (attempt %d/%d)",
                    wait_time, attempt + 1, max_retries
                )
                await asyncio.sleep(wait_time)
            else:
                _LOGGER.error("Failed to clear statistics for %s: %s", entity_id, err)
                return 0
    return 0


async def _fix_post_import_discontinuity(
    hass: HomeAssistant,
    entity_id: str,
    room_name: str,
    import_end_time: datetime,
    import_end_sum: float,
    metadata: StatisticMetaData,
) -> int:
    """Fix statistics discontinuity for data recorded AFTER the import period.

    If the live sensor recorded statistics after the import period ends, those
    statistics have their own sum baseline. This function detects that discontinuity
    and adjusts post-import statistics to continue from the import's final sum.

    Args:
        hass: Home Assistant instance.
        entity_id: The entity ID to fix.
        room_name: Room name for logging.
        import_end_time: The timestamp of the last imported statistic.
        import_end_sum: The cumulative sum at the end of the import.
        metadata: StatisticMetaData for re-importing adjusted statistics.

    Returns:
        Number of statistics entries adjusted.
    """
    # Query existing statistics AFTER the import period
    query_start = import_end_time + timedelta(hours=1)
    existing_stats = await _get_existing_statistics(hass, entity_id, query_start)

    if not existing_stats:
        _LOGGER.debug(
            "No existing statistics found after import period for %s",
            entity_id,
        )
        return 0

    # Check for discontinuity: first post-import sum should continue from import
    first_existing = existing_stats[0]
    first_existing_sum = first_existing.get("sum", 0) or 0
    first_existing_state = first_existing.get("state", 0) or 0

    # Expected: first_existing_sum ≈ import_end_sum + first_existing_state
    # If first_existing_sum << import_end_sum, there's a discontinuity
    expected_first_sum = import_end_sum + first_existing_state
    discontinuity = expected_first_sum - first_existing_sum

    if abs(discontinuity) <= DISCONTINUITY_THRESHOLD:
        _LOGGER.debug(
            "No significant post-import discontinuity for %s (delta: %.3f kWh)",
            entity_id,
            discontinuity,
        )
        return 0

    _LOGGER.info(
        "Detected post-import discontinuity for %s: "
        "import ends at %.3f kWh, first post-import sum is %.3f kWh (state: %.3f), "
        "adjusting %d entries by %.3f kWh",
        room_name,
        import_end_sum,
        first_existing_sum,
        first_existing_state,
        len(existing_stats),
        discontinuity,
    )

    # Create adjusted statistics to overwrite the existing ones
    adjusted_statistics = []
    for stat in existing_stats:
        stat_start = stat.get("start")
        if isinstance(stat_start, (int, float)):
            stat_start = datetime.fromtimestamp(stat_start, tz=timezone.utc)

        adjusted_statistics.append(
            StatisticData(
                start=stat_start,
                state=stat.get("state", 0) or 0,
                sum=(stat.get("sum", 0) or 0) + discontinuity,
            )
        )

    # Import the adjusted statistics (overwrites existing)
    async_import_statistics(hass, metadata, adjusted_statistics)

    _LOGGER.info(
        "Adjusted %d post-import statistics entries for %s",
        len(adjusted_statistics),
        room_name,
    )

    return len(adjusted_statistics)


class HistoryImportManager:
    """Manages persistent state for energy history import."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        """Initialize the import manager."""
        self.hass = hass
        self.entry_id = entry_id
        self.store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY}_{entry_id}")
        self._state: dict = {}
        self._running = False
        self._cancelled = False

        # Progress tracking
        self.status = "idle"
        self.current_room: str | None = None
        self.rooms_completed = 0
        self.total_rooms = 0
        self.days_imported = 0
        self.total_days = 0
        self.last_error: str | None = None

    async def async_load(self) -> None:
        """Load import state from storage."""
        data = await self.store.async_load()
        self._state = data or {
            "rooms": {},  # room_id -> {"last_imported_day": int, "cumulative_sum": float}
        }

    async def async_save(self) -> None:
        """Save import state to storage."""
        await self.store.async_save(self._state)

    def get_room_progress(self, room_id: str) -> dict:
        """Get progress for a specific room."""
        return self._state.get("rooms", {}).get(room_id, {
            "last_imported_day": 0,
            "cumulative_sum": 0.0,
        })

    def update_room_progress(
        self, room_id: str, last_day: int, cumulative_sum: float
    ) -> None:
        """Update progress for a specific room."""
        if "rooms" not in self._state:
            self._state["rooms"] = {}
        self._state["rooms"][room_id] = {
            "last_imported_day": last_day,
            "cumulative_sum": cumulative_sum,
        }

    def clear_room_progress(self, room_id: str) -> None:
        """Clear progress for a specific room to allow re-import."""
        if "rooms" in self._state and room_id in self._state["rooms"]:
            del self._state["rooms"][room_id]

    def cancel(self) -> None:
        """Request cancellation of the running import."""
        self._cancelled = True

    @property
    def is_running(self) -> bool:
        """Check if import is currently running."""
        return self._running


async def async_import_energy_history(
    hass: HomeAssistant,
    api: "IntuisAPI",
    intuis_home: "IntuisHome",
    manager: HistoryImportManager,
    days: int = DEFAULT_HISTORY_DAYS,
    room_filter: str | None = None,
    home_id: str | None = None,
    granularity: str = "hourly",
    clear_existing: bool = False,
) -> dict:
    """Import historical energy data into Home Assistant statistics.

    This function fetches energy consumption for the configured number
    of days and imports it as statistics for existing sensor entities.

    Args:
        hass: Home Assistant instance.
        api: Intuis API client.
        intuis_home: Intuis home data.
        manager: Import manager for state persistence.
        days: Number of days of history to import (1-730).
        room_filter: Optional room name to import only that room.
        home_id: Home ID for building entity unique_ids.
        granularity: "hourly" for hourly stats (recommended) or "daily" for daily stats.
        clear_existing: If True, delete existing statistics before importing.

    Returns:
        Dict with import results: rooms_imported, total_energy, errors.
    """
    if manager.is_running:
        _LOGGER.warning("Import already running, ignoring request")
        return {"error": "Import already running"}

    manager._running = True
    manager._cancelled = False
    manager.status = "importing"
    manager.last_error = None
    
    # Set global flag to pause HourlyStatsUpdater during import
    HISTORY_IMPORT_IN_PROGRESS[home_id or intuis_home.id] = True
    _LOGGER.info("History import started - HourlyStatsUpdater paused for home %s", home_id or intuis_home.id)
    
    # Validate granularity
    if granularity not in ("hourly", "daily"):
        granularity = "hourly"

    # Clamp days to valid range and ensure integer
    days = int(max(1, min(days, MAX_HISTORY_DAYS)))

    # Get the home_id from intuis_home if not provided
    if home_id is None:
        home_id = intuis_home.id if hasattr(intuis_home, 'id') else ""

    # Get entity registry to find actual entity IDs
    ent_reg = er.async_get(hass)

    # Build list of rooms to import
    rooms_data = intuis_home.rooms
    rooms_to_import: list[dict] = []

    for room_id, room_def in rooms_data.items():
        room_name = room_def.name if hasattr(room_def, 'name') else str(room_id)

        # Apply room filter if specified
        if room_filter and room_name.lower() != room_filter.lower():
            continue

        # Build the unique_id for the energy sensor: intuis_{home_id}_{room_id}_energy
        unique_id = f"intuis_{home_id}_{room_id}_energy"

        # Look up entity in registry
        entity_entry = ent_reg.async_get_entity_id("sensor", DOMAIN, unique_id)
        if not entity_entry:
            _LOGGER.warning(
                "Energy sensor with unique_id %s not found in registry, skipping room %s",
                unique_id,
                room_name,
            )
            continue

        rooms_to_import.append({
            "id": room_id,
            "name": room_name,
            "entity_id": entity_entry,
        })

    if not rooms_to_import:
        manager._running = False
        manager.status = "idle"
        if room_filter:
            return {"error": f"Room '{room_filter}' not found"}
        return {"error": "No rooms found"}

    manager.total_rooms = len(rooms_to_import)
    manager.rooms_completed = 0
    manager.total_days = days

    _LOGGER.info(
        "Starting energy history import for %d rooms, %d days",
        len(rooms_to_import),
        days,
    )

    now = datetime.now(timezone.utc)
    results = {
        "rooms_imported": 0,
        "total_energy_kwh": 0.0,
        "errors": [],
    }

    try:
        for room_info in rooms_to_import:
            if manager._cancelled:
                _LOGGER.info("Import cancelled by user")
                manager.status = "cancelled"
                break

            room_id = room_info["id"]
            room_name = room_info["name"]
            entity_id = room_info["entity_id"]
            manager.current_room = room_name

            _LOGGER.info(
                "Importing energy history for room: %s (entity: %s)",
                room_name,
                entity_id,
            )

            # Calculate import start time (first day we'll import)
            import_start_date = now - timedelta(days=days)
            import_start_time = datetime.combine(
                import_start_date.date(),
                datetime.min.time(),
                tzinfo=timezone.utc
            )

            if clear_existing:
                # Clear ALL statistics for this entity (changing granularity scenario)
                # This ensures no daily/hourly conflicts remain
                cleared_count = await _clear_all_statistics(hass, entity_id)
                if cleared_count > 0:
                    results["statistics_cleared"] = results.get(
                        "statistics_cleared", 0
                    ) + cleared_count
                    _LOGGER.info(
                        "Cleared all %d statistics for %s before import",
                        cleared_count,
                        room_name,
                    )
                # Start from zero since we cleared everything
                baseline_sum = 0.0
            else:
                # Get baseline sum from existing statistics BEFORE import period
                # This ensures imported data continues from existing baseline
                baseline_sum = await _get_baseline_sum(
                    hass, entity_id, room_name, import_start_time
                )

                # Clear existing statistics within the import range only
                # This removes both import entries (at 00:00 UTC) and live sensor
                # entries (at various hours) to prevent coexisting conflicting data
                cleared_count = await _clear_statistics_in_range(
                    hass, entity_id, import_start_time, now
                )
                if cleared_count > 0:
                    results["statistics_cleared"] = results.get(
                        "statistics_cleared", 0
                    ) + cleared_count

            cumulative_sum = baseline_sum

            # Collect hourly statistics using BULK retrieval (one API call per room)
            statistics: list[StatisticData] = []
            manager.days_imported = 0

            # Check circuit breaker before the bulk request
            if hasattr(api, 'circuit_breaker'):
                wait_time = api.circuit_breaker.check()
                if wait_time > 0:
                    _LOGGER.warning(
                        "Circuit breaker open, pausing import for %.0f seconds",
                        wait_time,
                    )
                    manager.status = "rate_limited"
                    manager.last_error = f"Circuit breaker open, retry in {int(wait_time)}s"
                    manager.rooms_completed += 1
                    await manager.async_save()
                    break

            try:
                # Calculate date range for bulk fetch
                # Skip the current hour which may still be accumulating data
                data_cutoff = now - timedelta(hours=API_DATA_DELAY_HOURS)
                range_start = now - timedelta(days=days)
                
                date_begin_dt = datetime.combine(
                    range_start.date(),
                    datetime.min.time(),
                    tzinfo=timezone.utc
                )
                date_end_dt = data_cutoff

                if granularity == "hourly":
                    _LOGGER.info(
                        "Fetching %d days of HOURLY energy data for %s (in chunks of %d days)",
                        days,
                        room_name,
                        MAX_DAYS_PER_HOURLY_REQUEST,
                    )

                    # Split into chunks to avoid API 500 errors
                    energy_values: list[tuple[int, float]] = []
                    chunk_start = date_begin_dt
                    chunk_num = 0
                    
                    while chunk_start < date_end_dt:
                        if manager._cancelled:
                            break
                            
                        chunk_end = min(
                            chunk_start + timedelta(days=MAX_DAYS_PER_HOURLY_REQUEST),
                            date_end_dt
                        )
                        
                        chunk_begin_ts = int(chunk_start.timestamp())
                        chunk_end_ts = int(chunk_end.timestamp())
                        
                        try:
                            chunk_values = await api.async_get_room_energy_hourly(
                                room_id, chunk_begin_ts, chunk_end_ts
                            )
                            if chunk_values:
                                energy_values.extend(chunk_values)
                                chunk_num += 1
                                _LOGGER.debug(
                                    "Chunk %d for %s: %d hourly values (%s to %s)",
                                    chunk_num,
                                    room_name,
                                    len(chunk_values),
                                    chunk_start.date(),
                                    chunk_end.date(),
                                )
                        except Exception as chunk_err:
                            _LOGGER.warning(
                                "Chunk fetch failed for %s (%s to %s): %s",
                                room_name,
                                chunk_start.date(),
                                chunk_end.date(),
                                chunk_err,
                            )
                        
                        # Move to next chunk
                        chunk_start = chunk_end
                        
                        # Small delay between chunks
                        if chunk_start < date_end_dt:
                            await asyncio.sleep(API_DELAY_SECONDS)
                    
                    step_name = "hour"
                    _LOGGER.info(
                        "Fetched %d hourly values for %s in %d chunks",
                        len(energy_values),
                        room_name,
                        chunk_num,
                    )
                else:
                    _LOGGER.info(
                        "Fetching %d days of DAILY energy data for %s (data cutoff: %s)",
                        days,
                        room_name,
                        data_cutoff.isoformat(),
                    )

                    date_begin = int(date_begin_dt.timestamp())
                    date_end = int(date_end_dt.timestamp())
                    
                    # BULK fetch: one API call returns all daily values
                    energy_values = await api.async_get_room_energy_daily(
                        room_id, date_begin, date_end
                    )
                    step_name = "day"

                if not energy_values:
                    _LOGGER.warning(
                        "No %s energy data returned for %s, skipping",
                        step_name,
                        room_name,
                    )
                    _LOGGER.warning(
                        "No %s energy data returned for %s, skipping",
                        step_name,
                        room_name,
                    )
                else:
                    # Sort by timestamp to ensure chronological order
                    energy_values.sort(key=lambda x: x[0])

                    # Build statistics from values
                    entries_with_data = 0
                    for ts, energy_wh in energy_values:
                        if manager._cancelled:
                            break

                        energy_kwh = energy_wh / 1000.0

                        if energy_kwh > 0:
                            cumulative_sum += energy_kwh
                            entries_with_data += 1
                            
                        # Create a statistic entry
                        # Normalize timestamp to the top of the hour (required by HA)
                        entry_start = datetime.fromtimestamp(ts, tz=timezone.utc)
                        entry_start = entry_start.replace(minute=0, second=0, microsecond=0)
                        
                        statistics.append(
                            StatisticData(
                                start=entry_start,
                                state=energy_kwh,
                                sum=cumulative_sum,
                            )
                        )
                        _LOGGER.debug(
                            "%s %s %s: %.3f kWh (total: %.3f kWh)",
                            room_name,
                            step_name,
                            entry_start.isoformat(),
                            energy_kwh,
                            cumulative_sum,
                        )

                    # Calculate progress
                    if granularity == "hourly":
                        manager.days_imported = len(energy_values) // 24
                    else:
                        manager.days_imported = len(energy_values)

                    _LOGGER.info(
                        "Bulk fetch completed for %s: %d %s entries, %d with data (%.3f kWh total)",
                        room_name,
                        len(statistics),
                        step_name,
                        entries_with_data,
                        cumulative_sum - baseline_sum,
                    )

                # Small delay between rooms to be polite to the API
                await asyncio.sleep(API_DELAY_SECONDS)

            except RateLimitError as err:
                _LOGGER.warning(
                    "Rate limited while importing %s. Saving progress. Retry after: %s",
                    room_name,
                    getattr(err, 'retry_after', 'unknown'),
                )
                manager.status = "rate_limited"
                manager.last_error = f"Rate limited by API: {err}"

            except (APIError, CannotConnect, asyncio.TimeoutError) as err:
                _LOGGER.warning(
                    "Failed to fetch energy for %s: %s",
                    room_name,
                    err,
                )
                results["errors"].append(f"Failed to fetch {room_name}: {err}")

            # Import collected statistics for this room
            if statistics:
                try:
                    # Use StatisticMeanType if available (HA 2025.11+)
                    try:
                        from homeassistant.components.recorder.models import StatisticMeanType
                        metadata = StatisticMetaData(
                            source="recorder",
                            statistic_id=entity_id,
                            name=f"{room_name} Energy",
                            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                            has_sum=True,
                            mean_type=StatisticMeanType.NONE,
                            unit_class=SensorDeviceClass.ENERGY,
                        )
                    except ImportError:
                        # Fallback for older HA versions
                        metadata = StatisticMetaData(
                            source="recorder",
                            statistic_id=entity_id,
                            name=f"{room_name} Energy",
                            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                            has_mean=False,
                            has_sum=True,
                            unit_class=SensorDeviceClass.ENERGY,
                        )

                    async_import_statistics(hass, metadata, statistics)

                    results["rooms_imported"] += 1
                    # Track imported energy (excluding baseline)
                    imported_energy = cumulative_sum - baseline_sum
                    results["total_energy_kwh"] += imported_energy

                    _LOGGER.info(
                        "Imported %d %s statistics for %s: "
                        "%.3f kWh imported (baseline: %.3f, final sum: %.3f)",
                        len(statistics),
                        "hourly" if granularity == "hourly" else "daily",
                        room_name,
                        imported_energy,
                        baseline_sum,
                        cumulative_sum,
                    )

                    # Fix any discontinuity with live sensor data recorded AFTER import
                    import_end_time = statistics[-1]["start"]
                    if isinstance(import_end_time, (int, float)):
                        import_end_time = datetime.fromtimestamp(
                            import_end_time, tz=timezone.utc
                        )
                    adjusted_count = await _fix_post_import_discontinuity(
                        hass=hass,
                        entity_id=entity_id,
                        room_name=room_name,
                        import_end_time=import_end_time,
                        import_end_sum=cumulative_sum,
                        metadata=metadata,
                    )
                    if adjusted_count > 0:
                        results["statistics_adjusted"] = results.get(
                            "statistics_adjusted", 0
                        ) + adjusted_count

                except (ValueError, TypeError) as err:
                    _LOGGER.error(
                        "Failed to import statistics for %s: %s",
                        room_name,
                        err,
                    )
                    results["errors"].append(
                        f"Failed to import {room_name}: {err}"
                    )

            manager.rooms_completed += 1
            await manager.async_save()

            if manager.status == "rate_limited":
                break

    finally:
        manager._running = False
        if manager.status not in ("cancelled", "rate_limited"):
            manager.status = "completed"
        manager.current_room = None
        
        # Clear global flag to resume HourlyStatsUpdater
        effective_home_id = home_id or intuis_home.id
        HISTORY_IMPORT_IN_PROGRESS[effective_home_id] = False
        _LOGGER.info("History import finished - HourlyStatsUpdater resumed for home %s", effective_home_id)

    _LOGGER.info(
        "Energy history import finished: %d rooms, %.3f kWh total",
        results["rooms_imported"],
        results["total_energy_kwh"],
    )

    return results
