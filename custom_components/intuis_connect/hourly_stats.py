"""Hourly statistics updater for Intuis Connect.

This module replicates the Node-RED logic for importing hourly energy data
into Home Assistant statistics. It fetches data from J-1 00:00 local time
to now, calculates cumulative sums with proper continuity handling, and
imports them via recorder.async_import_statistics.

Key features:
- Exact replication of Node-RED "Prépare les imports (cumul kWh + dédup)" logic
- Persistent storage for anchors (prev_sum, prev_start) and base_kwh
- Priority to hour-to-hour continuity, fallback to day base + cumulative
- Daily rebase at 00:02 local time (adds J-2 daily consumption to base_kwh)
- Always sends all stats (idempotent - same data = same result)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
)
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.db_schema import (
    Statistics,
    StatisticsShortTerm,
    StatisticsMeta,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.storage import Store
from homeassistant.helpers.recorder import session_scope
from sqlalchemy import delete, select, and_

from .utils.const import DOMAIN, HISTORY_IMPORT_IN_PROGRESS

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from .intuis_api.api import IntuisAPI
    from .entity.intuis_home import IntuisHome

_LOGGER = logging.getLogger(__name__)

# Storage configuration
STORAGE_VERSION = 1
STORAGE_KEY_PREFIX = "intuis_connect.hourly_stats_v2_"

# Constants matching Node-RED
HOUR_MS = 3600 * 1000  # 1 hour in milliseconds
HOUR_S = 3600  # 1 hour in seconds
MAX_HOURS_PER_REQUEST = 48
CHUNK_DELAY_SECONDS = 2.0


class HourlyStatsUpdater:
    """Updates hourly energy statistics from Intuis API to Home Assistant.
    
    This class replicates the Node-RED flow logic exactly:
    1. Fetch hourly data from J-1 00:00 local → now
    2. For each room, calculate cumulative sum with continuity priority
    3. Import all stats (always, even if unchanged - idempotent)
    4. Persist anchors for next run
    """

    def __init__(
        self,
        hass: HomeAssistant,
        api: IntuisAPI,
        intuis_home: IntuisHome,
        entry_id: str,
        home_id: str | None = None,
        update_interval_minutes: int = 60,
        timezone_str: str = "Europe/Paris",
    ) -> None:
        """Initialize the hourly stats updater."""
        self._hass = hass
        self._api = api
        self._intuis_home = intuis_home
        self._entry_id = entry_id
        self._home_id = home_id or intuis_home.id
        self._update_interval = update_interval_minutes
        self._timezone = ZoneInfo(timezone_str)
        self._timezone_str = timezone_str
        
        # Persistent storage
        self._store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY_PREFIX}{entry_id}")
        self._data: dict[str, Any] = {}
        
        # Loaded flag
        self._loaded = False
        
        # Update task
        self._update_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def _load_storage(self) -> None:
        """Load persistent data from storage."""
        if self._loaded:
            return
            
        stored = await self._store.async_load()
        if stored:
            self._data = stored
        else:
            self._data = {
                "base_kwh": {},      # statistic_id -> base kWh (EOD J-2)
                "anchors": {},       # statistic_id -> {"prev_sum": float, "prev_start": int (ms)}
            }
        self._loaded = True
        _LOGGER.debug("Loaded hourly stats storage: %s", self._data)

    async def _save_storage(self) -> None:
        """Save persistent data to storage."""
        await self._store.async_save(self._data)

    def _get_statistic_id(self, room_id: str) -> str | None:
        """Get the statistic ID (entity_id) for a room from the entity registry.
        
        This must match exactly how history_import.py finds entity_ids,
        otherwise we'll have mismatched statistic_ids.
        """
        from homeassistant.helpers import entity_registry as er
        
        # Build unique_id the same way as sensor.py and history_import.py
        unique_id = f"intuis_{self._intuis_home.id}_{room_id}_energy"
        
        # Look up entity in registry
        ent_reg = er.async_get(self._hass)
        entity_id = ent_reg.async_get_entity_id("sensor", DOMAIN, unique_id)
        
        if entity_id:
            _LOGGER.debug("Found entity_id %s for room %s (unique_id=%s)", entity_id, room_id, unique_id)
            return entity_id
        
        # Log what we tried
        room = self._intuis_home.rooms.get(room_id)
        room_name = room.name if room else room_id
        _LOGGER.warning(
            "Entity not found in registry: unique_id=%s, room=%s, home_id=%s",
            unique_id, room_name, self._intuis_home.id
        )
        return None

    def _get_local_datetime(self, utc_dt: datetime) -> datetime:
        """Convert UTC datetime to local timezone."""
        return utc_dt.astimezone(self._timezone)

    def _get_day_key(self, timestamp_ms: int) -> str:
        """Get YYYY-MM-DD day key in local timezone from milliseconds."""
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=self._timezone)
        return dt.strftime("%Y-%m-%d")

    def _get_hour(self, timestamp_ms: int) -> int:
        """Get hour (0-23) in local timezone from milliseconds."""
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=self._timezone)
        return dt.hour

    def _snap_to_hour(self, timestamp_sec: float) -> int:
        """Snap timestamp to top of hour, return milliseconds."""
        # Round down to nearest hour
        timestamp_ms = int(timestamp_sec * 1000)
        snapped_ms = (timestamp_ms // HOUR_MS) * HOUR_MS
        return snapped_ms

    async def _clear_statistics_in_range(
        self,
        statistic_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Clear statistics for an entity within a time range.
        
        This is necessary because old corrupted stats from previous versions
        need to be removed before importing new correct stats.
        """
        instance = get_instance(self._hass)

        def _do_clear() -> int:
            with session_scope(session=instance.get_session()) as session:
                # Get metadata_id for this entity
                result = session.execute(
                    select(StatisticsMeta.id).where(
                        StatisticsMeta.statistic_id == statistic_id
                    )
                ).scalar()

                if not result:
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

        try:
            deleted = await instance.async_add_executor_job(_do_clear)
            if deleted > 0:
                _LOGGER.debug(
                    "%s: Cleared %d old statistics from %s to %s",
                    statistic_id, deleted, start_time.isoformat(), end_time.isoformat()
                )
            return deleted
        except Exception as err:
            _LOGGER.warning("Failed to clear old statistics for %s: %s", statistic_id, err)
            return 0

    async def _get_last_existing_stat(self, statistic_id: str) -> dict | None:
        """Get the maximum sum from existing statistics for a sensor.
        
        This is used to initialize base_kwh when no storage exists.
        Returns dict with 'sum' and 'start' keys, or None if not found.
        """
        from homeassistant.components.recorder.statistics import (
            statistics_during_period,
        )
        
        now = datetime.now(self._timezone)
        start_time = now - timedelta(days=200)  # Look back 200 days to cover imported history
        
        try:
            stats = await get_instance(self._hass).async_add_executor_job(
                statistics_during_period,
                self._hass,
                start_time,
                None,
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )
            
            if statistic_id in stats and stats[statistic_id]:
                # Find the stat with the MAXIMUM sum
                all_stats = stats[statistic_id]
                max_sum_stat = max(all_stats, key=lambda s: s.get("sum", 0) or 0)
                
                max_sum = max_sum_stat.get("sum", 0) or 0
                
                _LOGGER.debug(
                    "%s: Found %d existing stats, max sum=%.3f",
                    statistic_id, len(all_stats), max_sum
                )
                
                return {
                    "sum": max_sum,
                    "start": max_sum_stat.get("start"),
                }
            else:
                _LOGGER.debug("%s: No existing stats found", statistic_id)
        except Exception as err:
            _LOGGER.debug("Could not get existing stats for %s: %s", statistic_id, err)
        
        return None

    async def _get_sum_at_j1_midnight(self, statistic_id: str) -> float:
        """Get the sum at J-1 midnight (00:00 local time yesterday).
        
        This is the anchor point for hourly updates - we need the cumulative
        sum at the start of yesterday to calculate today's sums.
        
        Returns the sum value, or 0 if not found.
        """
        from homeassistant.components.recorder.statistics import (
            statistics_during_period,
        )
        
        # Calculate J-1 midnight in local time
        now_local = datetime.now(self._timezone)
        today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        j1_midnight = today_midnight - timedelta(days=1)
        
        # We need to find the stat at J-1 00:00
        # Search from a bit before to a bit after
        search_start = j1_midnight - timedelta(hours=2)
        search_end = j1_midnight + timedelta(hours=2)
        
        try:
            stats = await get_instance(self._hass).async_add_executor_job(
                statistics_during_period,
                self._hass,
                search_start,
                search_end,
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )
            
            if statistic_id in stats and stats[statistic_id]:
                all_stats = stats[statistic_id]
                
                # Find the stat closest to J-1 midnight
                j1_midnight_ts = j1_midnight.timestamp()
                best_stat = None
                best_diff = float('inf')
                
                for stat in all_stats:
                    stat_start = stat.get("start")
                    if stat_start is None:
                        continue
                    
                    if hasattr(stat_start, 'timestamp'):
                        stat_ts = stat_start.timestamp()
                    else:
                        stat_ts = float(stat_start)
                    
                    diff = abs(stat_ts - j1_midnight_ts)
                    if diff < best_diff:
                        best_diff = diff
                        best_stat = stat
                
                if best_stat and best_diff < 3600:  # Within 1 hour
                    sum_val = best_stat.get("sum", 0) or 0
                    _LOGGER.debug(
                        "%s: Found sum at J-1 midnight: %.3f kWh",
                        statistic_id, sum_val
                    )
                    return sum_val
            
            _LOGGER.debug("%s: No stat found at J-1 midnight", statistic_id)
        except Exception as err:
            _LOGGER.debug("Could not get J-1 midnight stat for %s: %s", statistic_id, err)
        
        return 0.0

    async def async_update(self) -> int:
        """Fetch and import hourly statistics.
        
        Returns the number of statistics imported.
        """
        # Check if history import is in progress - skip update to avoid conflicts
        if HISTORY_IMPORT_IN_PROGRESS.get(self._home_id, False):
            _LOGGER.info(
                "History import in progress for home %s - skipping hourly stats update",
                self._home_id
            )
            return 0
        
        await self._load_storage()
        
        # Calculate time window: J-1 00:00 local → now
        now_local = datetime.now(self._timezone)
        today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_midnight = today_midnight - timedelta(days=1)
        
        date_begin = int(yesterday_midnight.timestamp())
        date_end = int(now_local.timestamp())
        
        _LOGGER.info(
            "Fetching hourly data from %s to %s",
            yesterday_midnight.isoformat(),
            now_local.isoformat()
        )
        
        total_imported = 0
        
        # Fetch for all rooms - one by one using existing API
        try:
            for room_id, room in self._intuis_home.rooms.items():
                try:
                    # Get hourly data for this room
                    hourly_data = await self._api.async_get_room_energy_hourly(
                        room_id=room_id,
                        date_begin=date_begin,
                        date_end=date_end,
                    )
                    
                    if not hourly_data:
                        _LOGGER.debug("No hourly data for room %s", room_id)
                        continue
                    
                    count = await self._process_room(room_id, hourly_data)
                    total_imported += count
                    
                except Exception as room_err:
                    _LOGGER.warning("Error fetching hourly data for room %s: %s", room_id, room_err)
                    continue
            
            # Save updated anchors
            await self._save_storage()
            
            _LOGGER.info("Imported %d hourly statistics", total_imported)
            return total_imported
            
        except Exception as err:
            _LOGGER.error("Error updating hourly stats: %s", err, exc_info=True)
            return 0

    async def _process_room(self, room_id: str, hourly_data: list[tuple[int, float]]) -> int:
        """Process hourly data for a single room.
        
        This replicates the Node-RED logic EXACTLY:
        1. Get the sum at J-1 midnight from HA stats (this is our base)
        2. For each hour from J-1 00:00:
           - Calculate cumulative sum from midnight
           - sum = base_at_j1_midnight + cumul_since_j1_midnight
        3. ALWAYS rewrite ALL hours (idempotent - same data = same result)
        4. Update anchors for next run
        
        Args:
            room_id: The room ID
            hourly_data: List of (timestamp_seconds, energy_wh) tuples from API
        
        Returns number of stats imported.
        """
        if not hourly_data:
            return 0
        
        statistic_id = self._get_statistic_id(room_id)
        if not statistic_id:
            room = self._intuis_home.rooms.get(room_id)
            room_name = room.name if room else room_id
            _LOGGER.warning(
                "Energy sensor not found in registry for room %s (room_id=%s). Skipping.",
                room_name, room_id
            )
            return 0
        
        # **KEY: Get the sum at J-1 midnight from existing HA stats**
        # This is where the historical import stopped, so this is our anchor
        base_at_j1_midnight = await self._get_sum_at_j1_midnight(statistic_id)
        
        # Also try persistent storage as fallback
        base_kwh_all = self._data.get("base_kwh", {})
        stored_base = float(base_kwh_all.get(statistic_id, 0))
        
        # Use whichever is higher (to avoid losing data)
        if base_at_j1_midnight == 0 and stored_base > 0:
            base_at_j1_midnight = stored_base
            _LOGGER.info(
                "%s: Using stored base_kwh=%.3f (no stat found at J-1 midnight)",
                statistic_id, stored_base
            )
        elif base_at_j1_midnight > 0:
            _LOGGER.info(
                "%s: Using sum at J-1 midnight=%.3f from HA stats",
                statistic_id, base_at_j1_midnight
            )
        
        if base_at_j1_midnight == 0:
            _LOGGER.warning(
                "%s: No base found at J-1 midnight. Run historical import first. Skipping.",
                statistic_id
            )
            return 0
        
        _LOGGER.info(
            "%s: Processing %d data points with base=%.3f kWh",
            statistic_id, len(hourly_data), base_at_j1_midnight
        )
        
        # Track cumulative consumption from J-1 midnight
        cumul_from_j1_midnight = 0.0
        stats: list[StatisticData] = []
        
        for timestamp_sec, wh in hourly_data:
            # Validate
            if wh is None or not (isinstance(wh, (int, float)) and wh >= 0 and wh == wh):
                continue
            
            # Snap to top of hour (HA requires mm:ss = 00:00)
            start_ms = self._snap_to_hour(timestamp_sec)
            start_dt = datetime.fromtimestamp(start_ms / 1000, tz=self._timezone)
            
            # Add hourly consumption (Wh -> kWh)
            energy_kwh = wh / 1000.0
            cumul_from_j1_midnight += energy_kwh
            
            # Calculate sum: base + cumul since J-1 midnight
            sum_kwh = round(base_at_j1_midnight + cumul_from_j1_midnight, 3)
            
            # Build stat entry - ALWAYS include (idempotent)
            stats.append(StatisticData(
                start=start_dt,
                sum=sum_kwh,
                state=sum_kwh,
            ))
        
        if not stats:
            _LOGGER.debug("%s: No valid data points to import", statistic_id)
            return 0
        
        # Update persistent base_kwh (for next run / daily rebase)
        if "base_kwh" not in self._data:
            self._data["base_kwh"] = {}
        # Store the last sum as the new base for tomorrow
        self._data["base_kwh"][statistic_id] = stats[-1]["sum"]
        
        # Update persistent anchors
        last_stat = stats[-1]
        last_ms = int(last_stat["start"].timestamp() * 1000)
        
        if "anchors" not in self._data:
            self._data["anchors"] = {}
        self._data["anchors"][statistic_id] = {
            "prev_sum": last_stat["sum"],
            "prev_start": last_ms,
        }
        
        # Get room name for metadata
        room = self._intuis_home.rooms.get(room_id)
        room_name = room.name if room else f"Room {room_id}"
        
        # **CRITICAL: Clear old statistics before importing new ones**
        # This removes any corrupted stats from previous versions
        first_stat_time = stats[0]["start"]
        # Clear from first stat time to now + 1 hour (to cover current hour)
        clear_end = datetime.now(self._timezone) + timedelta(hours=1)
        await self._clear_statistics_in_range(statistic_id, first_stat_time, clear_end)
        
        # Import statistics
        metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{room_name} Energy",
            source="recorder",
            statistic_id=statistic_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )
        
        try:
            async_import_statistics(self._hass, metadata, stats)
            _LOGGER.info(
                "%s: Imported %d hours, first: %s (sum=%.3f), last: %s (sum=%.3f)",
                statistic_id, len(stats),
                stats[0]["start"].isoformat(), stats[0]["sum"],
                stats[-1]["start"].isoformat(), stats[-1]["sum"]
            )
            return len(stats)
        except Exception as err:
            _LOGGER.error("Failed to import stats for %s: %s", statistic_id, err)
            return 0

    async def async_daily_rebase(self) -> None:
        """Perform daily rebase: add J-2 consumption to base_kwh.
        
        This is called at 00:02 local time and adds the total daily
        consumption of J-2 to each sensor's base_kwh value.
        
        This ensures base_kwh always represents the cumulative total
        up to end of J-2, allowing proper calculation even after
        gaps in data collection.
        """
        await self._load_storage()
        
        # Calculate J-2 date range
        now_local = datetime.now(self._timezone)
        today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        j2_midnight = today_midnight - timedelta(days=2)
        j1_midnight = today_midnight - timedelta(days=1)
        
        date_begin = int(j2_midnight.timestamp())
        date_end = int(j1_midnight.timestamp()) - 1  # End of J-2
        
        _LOGGER.info(
            "Daily rebase: fetching J-2 consumption (%s)",
            j2_midnight.strftime("%Y-%m-%d")
        )
        
        try:
            if "base_kwh" not in self._data:
                self._data["base_kwh"] = {}
            
            for room_id, room in self._intuis_home.rooms.items():
                try:
                    # Get daily data for this room
                    daily_data = await self._api.async_get_room_energy_daily(
                        room_id=room_id,
                        date_begin=date_begin,
                        date_end=date_end,
                    )
                    
                    if not daily_data:
                        _LOGGER.debug("No daily data for room %s", room_id)
                        continue
                    
                    statistic_id = self._get_statistic_id(room_id)
                    if not statistic_id:
                        _LOGGER.debug("No entity_id found for room %s, skipping rebase", room_id)
                        continue
                    
                    # Sum daily consumption (Wh)
                    # daily_data is list of (timestamp, energy_wh) tuples
                    total_wh = sum(wh for ts, wh in daily_data)
                    
                    # Add to base_kwh
                    add_kwh = total_wh / 1000.0
                    current_base = self._data["base_kwh"].get(statistic_id, 0.0)
                    new_base = round(current_base + add_kwh, 3)
                    self._data["base_kwh"][statistic_id] = new_base
                    
                    _LOGGER.debug(
                        "Rebase %s: %.3f + %.3f = %.3f kWh",
                        statistic_id, current_base, add_kwh, new_base
                    )
                    
                except Exception as room_err:
                    _LOGGER.warning("Error fetching daily data for room %s: %s", room_id, room_err)
            
            await self._save_storage()
            _LOGGER.info("Daily rebase completed")
            
        except Exception as err:
            _LOGGER.error("Error during daily rebase: %s", err, exc_info=True)

    async def async_initialize_base_from_statistics(self) -> None:
        """Initialize base_kwh from existing HA statistics.
        
        This is useful when starting fresh or recovering from data loss.
        It queries the last known statistics for each sensor and uses
        them to initialize the base_kwh values.
        """
        await self._load_storage()
        
        from homeassistant.components.recorder.statistics import (
            statistics_during_period,
        )
        
        now = datetime.now(self._timezone)
        start_time = now - timedelta(days=30)
        
        _LOGGER.info("Initializing base_kwh from existing statistics")
        
        for room_id, room in self._intuis_home.rooms.items():
            statistic_id = self._get_statistic_id(room_id)
            if not statistic_id:
                _LOGGER.debug("No entity_id found for room %s, skipping init", room_id)
                continue
            
            try:
                stats = await get_instance(self._hass).async_add_executor_job(
                    statistics_during_period,
                    self._hass,
                    start_time,
                    None,
                    {statistic_id},
                    "hour",
                    None,
                    {"sum"},
                )
                
                if statistic_id in stats and stats[statistic_id]:
                    last_stat = stats[statistic_id][-1]
                    last_sum = last_stat.get("sum", 0)
                    last_start = last_stat.get("start")
                    
                    if last_sum is not None:
                        # Initialize base_kwh
                        if "base_kwh" not in self._data:
                            self._data["base_kwh"] = {}
                        self._data["base_kwh"][statistic_id] = round(float(last_sum), 3)
                        
                        # Initialize anchors
                        if "anchors" not in self._data:
                            self._data["anchors"] = {}
                        
                        if last_start:
                            last_ms = int(last_start.timestamp() * 1000)
                            self._data["anchors"][statistic_id] = {
                                "prev_sum": float(last_sum),
                                "prev_start": last_ms,
                            }
                        
                        _LOGGER.info(
                            "Initialized %s: base=%.3f kWh from %s",
                            statistic_id, last_sum, last_start
                        )
                        
            except Exception as err:
                _LOGGER.warning(
                    "Could not initialize %s from statistics: %s",
                    statistic_id, err
                )
        
        await self._save_storage()
        _LOGGER.info("Base initialization completed")

    async def async_start(self) -> None:
        """Start the periodic update task."""
        if self._update_task is not None:
            _LOGGER.warning("Hourly stats updater already running")
            return
        
        self._stop_event.clear()
        self._update_task = asyncio.create_task(self._update_loop())
        _LOGGER.info("Hourly stats updater started (interval: %d min)", self._update_interval)

    async def async_stop(self) -> None:
        """Stop the periodic update task."""
        if self._update_task is None:
            return
        
        self._stop_event.set()
        self._update_task.cancel()
        try:
            await self._update_task
        except asyncio.CancelledError:
            pass
        self._update_task = None
        _LOGGER.info("Hourly stats updater stopped")

    async def _update_loop(self) -> None:
        """Periodic update loop."""
        # Initial delay to let HA stabilize
        await asyncio.sleep(60)
        
        while not self._stop_event.is_set():
            try:
                await self.async_update()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                _LOGGER.error("Error in hourly stats update loop: %s", err, exc_info=True)
            
            # Wait for next update
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._update_interval * 60
                )
                # If we get here, stop was requested
                break
            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                pass

    async def async_rebase_daily(self) -> None:
        """Alias for async_daily_rebase (for compatibility)."""
        await self.async_daily_rebase()
