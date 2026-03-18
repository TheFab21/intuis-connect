# Intuis Connect – Home Assistant Integration

> **Full control of your Muller / Campa / Intuis electric radiators** with Netatmo "Intuitiv" modules – climate control, energy tracking, schedule management, and more.

[![HACS Badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![GitHub Release](https://img.shields.io/github/v/release/antoine-pyre/homeassistant-intuis-connect)](https://github.com/antoine-pyre/homeassistant-intuis-connect/releases)

---

## Features at a Glance

| Feature | Description |
| --- | --- |
| **Climate Control** | Full thermostat control per room with Auto/Heat/Off modes |
| **Presets** | Schedule, Away, and Boost modes with configurable durations |
| **Energy Monitoring** | Daily kWh consumption per room with historical import |
| **Energy Cost Tracking** | Daily cost per room injected into HA Long-Term Statistics |
| **Energy Dashboard** | Full integration with HA's native Energy Dashboard |
| **Schedule Management** | View, switch, and edit heating schedules |
| **Visual Planning UI** | Standalone web page for visual weekly schedule editing |
| **Calendar Integration** | Visualize weekly schedules as calendar events |
| **Smart Detection** | Presence detection, open window detection, heating anticipation |
| **Indefinite Override** | Keep manual temperatures active indefinitely |

---

## Installation

> **Requires Home Assistant 2024.6+ and HACS 1.33+**

### One-Click Install (Recommended)

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=antoine-pyre&repository=homeassistant-intuis-connect&category=integration)

### Manual HACS Install

1. Open **HACS → Integrations**
2. Click **⋮** → **Custom repositories**
3. Add:
   * URL: `https://github.com/antoine-pyre/intuis-connect`
   * Category: **Integration**
4. Search for "Intuis Connect" → **Download** → **Restart** Home Assistant

### Configuration

1. Go to **Settings → Devices & Services → + Add Integration**
2. Search for **Intuis Connect**
3. Enter your Intuis/Muller app credentials (email & password)
4. Configure options (durations, temperatures, energy settings)

---

## Entities Created

### Per Room

| Entity Type | Name | Description |
| --- | --- | --- |
| **Climate** | `climate.<room>` | Full thermostat control |
| **Sensor** | `sensor.<room>_temperature` | Current room temperature (°C) |
| **Sensor** | `sensor.<room>_target_temperature` | Current setpoint (°C) |
| **Sensor** | `sensor.<room>_scheduled_temperature` | Temperature according to schedule |
| **Sensor** | `sensor.<room>_energy` | Daily energy consumption (kWh) |
| **Sensor** | `sensor.<room>_heating_minutes` | Heating time today |
| **Sensor** | `sensor.<room>_override_expires` | When manual override ends |
| **Binary Sensor** | `binary_sensor.<room>_presence` | Motion/presence detected |
| **Binary Sensor** | `binary_sensor.<room>_open_window` | Window open detected |
| **Binary Sensor** | `binary_sensor.<room>_anticipation` | Pre-heating active |
| **Binary Sensor** | `binary_sensor.<room>_boost` | Boost mode active |

### Per Schedule

| Entity Type | Name | Description |
| --- | --- | --- |
| **Sensor** | `sensor.<schedule>_current_zone` | Currently active zone (Comfort, Night, Eco...) |
| **Sensor** | `sensor.<schedule>_next_zone_change` | When the next zone starts |
| **Sensor** | `sensor.<schedule>_summary` | Full schedule data (for cards and planning UI) |
| **Calendar** | `calendar.<schedule>` | Weekly schedule visualization |

### Home Level

| Entity Type | Name | Description |
| --- | --- | --- |
| **Select** | `select.intuis_active_schedule` | Switch between schedules |
| **Sensor** | Various config sensors | Home settings and configuration |

---

## Climate Modes & Presets

### HVAC Modes

| Mode | Description |
| --- | --- |
| **Auto** | Follow the active schedule |
| **Heat** | Manual temperature control |
| **Off** | Frost protection only |

### Presets

| Preset | Description |
| --- | --- |
| **Schedule** | Return to automatic schedule |
| **Away** | Reduced temperature for extended absence |
| **Boost** | Maximum heating for quick warmup |

All preset durations and temperatures are configurable in the integration options.

---

## Services

### `intuis_connect.switch_schedule`

Switch to a different heating schedule.

```yaml
service: intuis_connect.switch_schedule
data:
  schedule_name: "Comfort"
```

### `intuis_connect.set_schedule_slot`

Edit a time slot in the active schedule. Supports multi-day spans.

```yaml
service: intuis_connect.set_schedule_slot
data:
  start_day: "4"        # Friday (0=Monday, 6=Sunday)
  start_time: "22:00"
  end_day: "5"          # Saturday
  end_time: "08:00"
  zone_name: "Night"
```

### `intuis_connect.sync_schedule`

Push a full weekly timetable to the API (used by the Planning UI). The timetable is provided as a JSON array of `{zone_id, m_offset}` entries where `m_offset` is the number of minutes from Monday 00:00.

```yaml
service: intuis_connect.sync_schedule
data:
  schedule_id: "abc123"
  timetable: '[{"zone_id":0,"m_offset":0},{"zone_id":1,"m_offset":1320}]'
```

### `intuis_connect.set_zone_temperature`

Set the temperature for a specific zone and room within a schedule.

```yaml
service: intuis_connect.set_zone_temperature
data:
  schedule_id: "abc123"
  zone_name: "Comfort"
  room_name: "Living Room"
  temperature: 20.5
```

### `intuis_connect.refresh_schedules`

Force refresh all schedule data from the Intuis API.

```yaml
service: intuis_connect.refresh_schedules
```

---

## Configuration Options

Accessible via **Settings → Devices & Services → Intuis Connect → Configure**

### Override Durations

| Option | Default | Range | Description |
| --- | --- | --- | --- |
| Manual Duration | 5 min | 1-720 min | How long manual temperature lasts |
| Away Duration | 24 hours | 1-10080 min | How long away mode lasts |
| Boost Duration | 30 min | 1-720 min | How long boost mode lasts |

### Override Temperatures

| Option | Default | Range | Description |
| --- | --- | --- | --- |
| Away Temperature | 16°C | 5-30°C | Temperature during away mode |
| Boost Temperature | 30°C | 5-30°C | Temperature during boost mode |

### Energy Settings

| Option | Default | Description |
| --- | --- | --- |
| Energy Scale | 1 day | Granularity: 5min, 30min, 1hour, or 1day |
| Import History | Off | Import historical energy data on setup |
| History Days | 30 | Days of history to import (7, 30, 90, 365) |

### Special Modes

| Option | Default | Description |
| --- | --- | --- |
| Indefinite Mode | Off | Auto-reapply overrides before they expire |

---

## Energy Monitoring

The integration tracks energy consumption per room:

* **Daily consumption** in kWh
* **Heating minutes** per day
* **Historical import** available (up to 365 days)
* **Multiple tariff support** (aggregates all EJP tariffs)

Energy data is fetched based on your configured scale:

* `1day`: Cached daily totals (fetched after 2 AM)
* `5min/30min/1hour`: Real-time tracking

---

## Energy Cost Tracking *(new)*

The integration can calculate and track the daily heating cost per room and inject it into Home Assistant's Long-Term Statistics (LTS), making it visible in the **Energy Dashboard**.

### How it works

- A dedicated `sensor.<room>_cost` sensor is created for each room, reporting the day's estimated heating cost.
- Cost statistics are injected via `async_import_statistics` with `source="recorder"`, which performs UPSERTs — re-importing history is safe and will never create duplicate entries.
- The integration uses a persistent `midnight_bases` structure to correctly anchor cumulative values at day boundaries, ensuring clean daily totals without dips or spikes at midnight.

### Setup

1. Create an `input_number` entity to store your electricity price (€/kWh):

```yaml
# configuration.yaml
input_number:
  electricity_price:
    name: Prix électricité
    min: 0
    max: 1
    step: 0.001
    unit_of_measurement: "€/kWh"
    icon: mdi:currency-eur
```

2. Wrap it with a template sensor so that Home Assistant generates Long-Term Statistics for it (required for the Energy Dashboard to retroactively calculate costs):

```yaml
template:
  - sensor:
      - name: "Prix électricité LTS"
        unit_of_measurement: "€/kWh"
        state_class: measurement
        state: "{{ states('input_number.electricity_price') | float(0) }}"
```

3. In the Intuis Connect options, point the integration to your price entity.

4. Add `sensor.<room>_cost` sensors to the **Energy Dashboard** (Settings → Energy → Individual devices).

> **Note on entity naming:** The HA Energy Dashboard automatically creates virtual `sensor.X_energy_cost` shadow entities for any energy sensor added to its configuration. The integration's own cost sensors use the `_cost` suffix (not `_energy_cost`) to avoid this naming collision. A startup migration function automatically renames any previously created entities if you are upgrading from an earlier version.

---

## Schedule Management

### Viewing Schedules

* **Calendar entities** show weekly schedules as events
* **Schedule summary sensor** contains full timetable data
* **Current zone sensor** shows what zone is active now
* **Next zone change sensor** shows upcoming transitions

### Editing Schedules

Use the `set_schedule_slot` service, the `sync_schedule` service, or the **Intuis Planning web interface** (see below) for visual editing.

### Schedule Structure

Schedules contain **zones** (Comfort, Night, Eco, etc.) with:

* Temperature settings per room
* Weekly timetable (minute-precision, 0–10080 minutes from Monday 00:00)

---

## Intuis Planning — Visual Web Interface *(new)*

`intuis_planning.html` is a standalone single-file web application for visually editing your Intuis heating schedules. It can be used in two ways:

**Embedded in Home Assistant** — place the file in your `/config/www/` folder and add it as a panel via a Webpage card or a custom panel. When loaded inside a same-origin HA iframe, the page accesses the `hass` object directly (no token needed).

**Standalone browser access** — open the file directly in any browser. A configuration dialog will prompt for:
- Your Home Assistant URL (e.g. `http://homeassistant.local:8123`)
- A long-lived access token (created in HA under Profile → Long-Lived Access Tokens)
- An optional "Remember token" checkbox (stores the token in `localStorage`; leave unchecked on shared computers)

In standalone mode, all HA communication goes through the REST API.

### Features

- **Full week grid** — each day is displayed as a horizontal bar with colored segments per zone. Hour markers appear every 2 hours for readability.
- **Click to edit** — clicking any segment opens a dialog to change its start time, end time (with optional next-day selection), and zone.
- **Add slots** — a `+` button at the end of each day row adds a new time slot.
- **Zone temperature editing** — zone cards below the grid allow per-room temperature adjustment with `+` / `−` buttons.
- **Away & frost-guard temperatures** — editable directly from the top controls.
- **Multi-schedule support** — a dropdown lists all available schedules; the currently active one is marked with ✓.
- **Change detection** — the Save button is only enabled when changes have been made. Switching schedule without saving prompts a confirmation.
- **Save** — timetable changes are pushed via `intuis_connect.sync_schedule`; temperature changes are pushed via `intuis_connect.set_zone_temperature`, one call per changed room. A `refresh_schedules` is triggered automatically after saving.
- **Refresh** — the 🔄 button triggers `refresh_schedules` and reloads state from HA.
- **Light / Dark mode** — follows the OS preference via `prefers-color-scheme`.
- **Firefox / ES5 compatible** — no optional chaining, nullish coalescing, or template literals; works in the HA frontend iframe on Firefox.
- **Post-restart resilience** — when embedded in HA after a restart, the page waits up to 45 seconds for the WebSocket to reconnect, displaying a live countdown. If still unavailable, it falls back to REST mode using any previously stored credentials.

### Installation

```
/config/www/intuis_planning.html
```

Then add a panel in your dashboard:

```yaml
type: iframe
url: /local/intuis_planning.html
aspect_ratio: 100%
```

---

## Dashboard Examples

### Thermostat Card

```yaml
type: thermostat
entity: climate.living_room
```

### Boost Button

```yaml
type: button
icon: mdi:fire
name: Boost 30 min
tap_action:
  action: call-service
  service: climate.set_preset_mode
  target:
    entity_id: climate.living_room
  data:
    preset_mode: boost
```

### Away Button

```yaml
type: button
icon: mdi:home-export-outline
name: Away Mode
tap_action:
  action: call-service
  service: climate.set_preset_mode
  target:
    entity_id: climate.living_room
  data:
    preset_mode: away
```

### Schedule Selector

```yaml
type: entities
entities:
  - entity: select.intuis_active_schedule
```

### Energy History Graph

```yaml
type: history-graph
entities:
  - entity: sensor.living_room_energy
hours_to_show: 168
```

### Planning Interface (iframe panel)

```yaml
type: iframe
url: /local/intuis_planning.html
aspect_ratio: 100%
```

### Visual Schedule Editor (companion card)

Install the companion [Intuis Schedule Card](https://github.com/antoine-pyre/intuis-schedule-card):

```yaml
type: custom:intuis-schedule-card
entity: sensor.intuis_home_schedule_summary
```

---

## Smart Features

### Presence Detection

Detects motion in rooms (sensor updates every 2 minutes).

### Open Window Detection

Automatically detects open windows to pause heating.

### Heating Anticipation

Pre-heats rooms before scheduled zone changes.

### Indefinite Override Mode

When enabled, the integration automatically re-applies manual overrides before they expire, effectively making them permanent until you switch back to schedule mode.

### `hvac_action` Reporting

The `climate` entity reports the current `hvac_action` (`heating` or `idle`) based on the `radiator_state` field returned by the Netatmo API. This field is updated by the API within approximately one minute of an actual state change, so no inference or temperature-delta fallback is needed.

---

## Troubleshooting

### Climate entity not responding

* Check Home Assistant logs for API errors
* Verify your Intuis credentials are still valid
* Try the `refresh_schedules` service

### `hvac_action` stuck on `idle`

* Enable `DEBUG` logging for the integration and check the logs after the radiator has been heating for at least 2–3 minutes
* Verify that the `radiator_state` attribute is updating on the `climate` entity
* The Netatmo API has ~1 minute latency on `radiator_state`; brief heating pulses may not be captured

### Energy data missing

* Energy is only fetched after 2 AM for daily scale
* Check that rooms have associated bridge IDs
* Try switching to a different energy scale

### Energy graph shows a V-shaped dip at midnight

* This was a known bug in earlier versions caused by stale baseline values at day boundaries
* Update to the latest release; the midnight boundary is now captured incrementally during the last polling cycle of each day

### Energy cost sensors missing or duplicated

* The HA Energy Dashboard auto-generates virtual `sensor.X_energy_cost` entities for any sensor added to its configuration — these are not created by the integration
* The integration's own cost sensors use the `_cost` suffix to avoid collision
* If upgrading from an older version, a startup migration function will automatically rename any conflicting entities and their statistics metadata

### Planning UI not loading

* Ensure the file is placed in `/config/www/` and HA has been restarted
* If accessing standalone (outside HA), verify the URL and token are correct
* After a HA restart, the page waits up to 45 seconds for reconnection — do not refresh during this countdown
* Check the browser console (F12) for errors

### Schedule changes not appearing

* Click the 🔄 refresh button or call `refresh_schedules`
* Changes made in the Intuis app may take 2 minutes to sync

### Authentication errors

* Re-authenticate by removing and re-adding the integration
* Check if your password changed in the Intuis app

---

## Technical Details

* **Update interval**: 2 minutes (configurable)
* **API**: Uses Netatmo cloud endpoints
* **Multi-cluster**: Automatic failover between API endpoints
* **Token refresh**: Automatic with 60-second buffer
* **Statistics backend**: `async_import_statistics` with `source="recorder"` — UPSERTs on `(metadata_id, start_ts)`, safe for repeated imports
* **Storage**: Persistent hourly stats stored at `/config/.storage/intuis_connect.hourly_stats_v3_<entry_id>`

---

## Related Projects

* **[Intuis Schedule Card](https://github.com/antoine-pyre/intuis-schedule-card)** — Visual schedule editor card for Lovelace

---

## License

MIT License — see [LICENSE](LICENSE) for details.

## Credits

Created by [antoine-pyre](https://github.com/antoine-pyre)

Based on reverse-engineering of the Intuis/Netatmo API.

Community improvements by [Fabrice](https://github.com/Fabrice) — energy cost tracking, Long-Term Statistics integration, midnight boundary fix, and the Intuis Planning web interface.
