# OTReader — LLRP Race Timing Middleware

OTReader sits between an LLRP RFID reader (e.g. Impinj Speedway / R700 family) and race
timing software. It connects to the reader, configures it for **start-line** or
**finish-line** timing, turns raw tag reads into de-duplicated **timing events** with
estimated crossing times, stores them durably, and delivers them to the timing software via
**HTTP callbacks** and a **REST API**.

It is built for long unattended runs on a Linux server or Raspberry Pi over an unreliable race
LAN. It reconnects to the reader automatically, persists its state to SQLite, and never
drops an accepted event because a callback failed.

> **Hardware status.** Every feature has been tested against the bundled LLRP reader
> simulator, which speaks real LLRP bytes encoded and decoded by pyllrp. **Nothing has been
> checked against a physical reader yet.** Before race use, run the hardware tests
> ([§12](#12-automated-tests)) on your reader and read [§2](#2-supported-reader-capabilities-and-known-limitations).

---

## Contents

1. [Overview and architecture](#1-overview-and-architecture)
2. [Supported reader capabilities and known limitations](#2-supported-reader-capabilities-and-known-limitations)
3. [Installation](#3-installation)
4. [Configuration reference](#4-configuration-reference)
5. [Starting the application](#5-starting-the-application)
6. [Connecting to an LLRP reader](#6-connecting-to-an-llrp-reader)
7. [Operating modes](#7-operating-modes)
8. [Finish-line estimation algorithm](#8-finish-line-estimation-algorithm)
9. [API reference](#9-api-reference)
10. [Persistence and disaster recovery](#10-persistence-and-disaster-recovery)
11. [Callback delivery and retries](#11-callback-delivery-and-retries)
12. [Automated tests](#12-automated-tests)
13. [Troubleshooting reader connectivity](#13-troubleshooting-reader-connectivity)
14. [Antenna test mode](#14-antenna-test-mode)
15. [Example integration with race timing software](#15-example-integration-with-race-timing-software)
16. [Timekeeping](#16-timekeeping)
17. [Security and operations](#17-security-and-operations)

---

## 1. Overview and architecture

```
               LLRP/TCP 5084                                   HTTP POST (callbacks)
 ┌──────────┐  ◄──────────►  ┌───────────────────────────────┐  ───────────────────►  ┌──────────────┐
 │  Reader  │                │ OTReader                      │                        │ Race timing  │
 └──────────┘                │                               │  ◄───────────────────  │   software   │
                             │  llrp/transport  (asyncio +   │   REST /api/v1         └──────────────┘
                             │     pyllrp codec)             │
                             │  llrp/client  ReaderManager   │── routes reports by ROSpecID ──┐
                             │  llrp/capabilities, config    │                                │
                             │                               │         timing ◄───────────────┤
                             │  timing/processor  TimingEngine│        test   ◄── test buffer ─┘
                             │  timing/finish_estimator      │
                             │  timing/inventory + cooldown  │
                             │                               │
                             │  persistence/repository (SQLite WAL, single ordered writer)
                             │  persistence/recovery         │
                             │  callbacks/dispatcher         │
                             │  api/routes (FastAPI)         │
                             └───────────────────────────────┘
```

| Component | Module | Responsibility |
|---|---|---|
| LLRP transport | `app/llrp/transport.py` | Asyncio TCP framing, MessageID request/response correlation, keepalive ACKs; pyllrp does encoding/decoding on a dedicated parser thread |
| Reader manager | `app/llrp/client.py` | State machine, serialised operations, discovery, ROSpec negotiation, reconnect supervisor, keepalive watchdog, report routing |
| Capability manager | `app/llrp/capabilities.py` | Parses capabilities/config into a model with per-field availability |
| Reader configuration | `app/llrp/configuration.py` | RF mode selection from the advertised mode table, per-mode ROSpec builders, feature fallback levels |
| Tag processor | `app/llrp/events.py` | `RO_ACCESS_REPORT` → normalised `Observation`s (EPC, antenna, RSSI, timestamps, phase, Doppler) |
| Timing engine | `app/timing/processor.py` | Start-line immediate events; finish-line pass tracking and finalisation |
| Finish estimator | `app/timing/finish_estimator.py` | Pure function: observations → crossing-time estimate, method and confidence |
| Inventory / cooldown | `app/timing/inventory.py`, `cooldown.py` | O(1) EPC lookup, cooldown on a monotonic clock, snapshot/restore |
| Persistence | `app/persistence/` | Durable event log, callback queue, snapshots, integrity check, salvage, recovery |
| Callbacks | `app/callbacks/` | Bounded in-memory queue backed by SQLite, retries/backoff, URL validation, optional HMAC |
| API | `app/api/`, `app/main.py` | REST endpoints, auth, CORS, body limits, health/readiness, OpenAPI |
| Service container | `app/services.py` | Wires components together; dependency-injection point for tests |

Concurrency model: there is one asyncio event loop. LLRP parsing runs on one worker thread
(which keeps reports in order), SQLite writes run on one writer thread (in submission order),
and SQLite reads use two reader threads. API handlers never block the RFID processing path.

---

## 2. Supported reader capabilities and known limitations

### How pyllrp is used

[pyllrp](https://pypi.org/project/pyllrp/) 3.1.1 generates Python classes for every LLRP 1.0.1
message and parameter, and for the **Impinj extension set, version 1.18**. It builds, validates,
packs and unpacks them. OTReader uses it for **all** LLRP encoding, decoding and validation.

These findings shaped the design. Each one is handled inside the LLRP adapter (`app/llrp/`):

| pyllrp behaviour | Impact | Handling |
|---|---|---|
| `LLRPConnector.transact()` asserts that its listener thread is **not** running | You can't send DISABLE_ROSPEC etc. while receiving reports without first stopping the listener, which blocks for up to the socket timeout | OTReader keeps pyllrp as the codec and implements the transport on asyncio (`transport.py`) |
| `LLRPConnector` uses the deprecated `datetime.utcfromtimestamp` and computes a naive time correction | Timestamp handling can't be controlled | OTReader has its own `ReaderClockMapper` (§16) |
| Unpacked `bits:N` fields (e.g. `C1G2SingulationControl.Session`) are `bitstring` objects, not `int` | Unpacked messages fail re-validation and JSON serialisation | Normalised to `int` in `unpack_message()` (test: `test_unpack_normalises_pyllrp_bits_fields`) |
| `RO_ACCESS_REPORT.getTagData()` strips leading zeros from EPCs and ignores Impinj parameters | Corrupted EPCs; RSSI detail lost | OTReader walks the parameters itself (`events.py`) |
| The Impinj 1.18 definitions include only these search modes: `Reader_Selected`, `Single_Target`, `Dual_Target`, `Single_Target_With_Suppression` | Newer firmware modes (e.g. B→A select variants) **can't be configured**, because pyllrp has no parameter definition for them | Mode selection is isolated in `configuration.py`. Adding a newer mode means adding a pyllrp parameter definition |
| Unknown message types raise during unpack; unknown vendor parameters decode as a generic `Custom` | — | The transport logs and skips malformed or unknown messages without losing framing |

### What is discovered and reported

`GET /api/v1/reader/capabilities` reports:

* manufacturer (IANA PEN; "Impinj" when PEN 25882), model code, firmware version
* maximum antennas, per-antenna connection state and gain (from `AntennaProperties`)
* transmit power table (dBm), receive sensitivity table, GPIO counts
* regulatory data: country code, communications standard, hop tables
* the full C1G2 RF mode table: BDR, Miller value, spectral mask and Tari for each mode
* LLRP capabilities: maximum ROSpecs, state-aware singulation, report holding, and more
* a summary of the reader's current configuration

With Impinj extensions it also reports model name, **serial number**, software, FPGA and PCBA
versions, and reader temperature.

Every item has a `field_status` entry:

| Status | Meaning |
|---|---|
| `available` | The reader reported it |
| `unsupported` | Neither standard LLRP nor this reader provides it. For example, the serial number without Impinj extensions |
| `unavailable` | The query should have worked but failed |
| `not_queried` | The reader has never been connected |

`applied_configuration` shows exactly what the reader accepted for each mode, including any
options that were negotiated away and the reader's error for each.

### Impinj extension negotiation

With `impinj_extensions: auto` (the default), OTReader sends `IMPINJ_ENABLE_EXTENSIONS` only
when the reader's PEN is Impinj's. It treats the extensions as available only if the reader
answers with success. Being an Impinj reader is never taken as proof of support.

Impinj readers don't advertise which search modes they support. OTReader therefore *tries*
the configured options and falls back step by step if `ADD_ROSPEC` is rejected (see
`FEATURE_LEVELS` in `configuration.py`):

1. Full configuration
2. Without the Impinj search mode
3. Also without the Impinj report selector
4. Also without an RF mode (reader default)
5. Also without power settings
6. Also without the optional report fields

### Behaviour that must be confirmed on hardware

The implementation follows the LLRP 1.0.1 standard and the Impinj 1.18 definitions shipped
with pyllrp. Before race use, confirm the following on your reader with
`pytest -m hardware -s tests/hardware`:

* Which Impinj search modes your model and firmware accept (check `negotiation_failures`).
* The contents of the RF mode table, and whether vendor "AutoSet" modes (ModeIdentifier ≥
  1000) should be set explicitly with `rf_mode: <id>`.
* That `GET_ROSPECS` reports `Active`/`Disabled`. OTReader uses it to verify start and stop,
  and falls back to trusting the response when the reader doesn't support it
  (`inventory_state_verified: null`).
* Keepalive delivery, and `AntennaEvent` notifications when antennas are unplugged.
* Report latency with `ImpinjReportBufferConfiguration: Low_Latency`.
* The Doppler scaling (OTReader assumes raw/16 = Hz, the Impinj Octane SDK convention). Only
  the sign change is used, so the scale doesn't affect timing.
* The reader's clock synchronisation (§16).

---

## 3. Installation

Requires Python ≥ 3.12 on Linux (x86-64 or Raspberry Pi OS 64-bit).

```bash
git clone <this repo> /opt/otreader && cd /opt/otreader
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt      # or: .venv/bin/pip install -e .[dev]
cp config.yaml config.local.yaml               # edit reader.host etc.
.venv/bin/python -m app.main --config config.local.yaml --check-config
```

Runtime dependencies: `pyllrp`, `fastapi`, `uvicorn`, `pydantic`, `PyYAML`, `httpx`, `asyncssh`.
SQLite comes with Python.

---

## 4. Configuration reference

The configuration is a YAML file, validated with Pydantic at startup. Unknown keys are
rejected. Invalid values produce a readable list of errors, for example:

```
ERROR: Invalid configuration in config.yaml:
  - reader.port: Input should be less than or equal to 65535
```

`config.yaml` is annotated and shows every option with its default. The key settings:

| Key | Default | Description |
|---|---|---|
| `reader.host_file` | /var/run/reader | File holding the reader's IP address (§6). Re-read every `host_file_poll_seconds` (5 s) until it holds a valid address |
| `reader.host` | null | Reader IP or hostname, used only when `host_file` is `null` (development, simulator) |
| `reader.port` | 5084 | LLRP port |
| `reader.reconnect_interval` / `reconnect_max_interval` | 5 / 60 s | Exponential backoff start and ceiling (±10 % jitter) |
| `reader.connection_timeout` | 10 s | TCP connect and per-command response timeout |
| `reader.keepalive_interval_ms` / `keepalive_missed_limit` | 5000 / 3 | LLRP keepalive period; the link is declared dead after N missed keepalives. 0 disables |
| `reader.antennas` | null (all) | e.g. `[1, 2]` |
| `reader.impinj_extensions` | auto | `auto`, `enabled` or `disabled` |
| `reader.ssh.username` / `password` / `port` / `timeout_seconds` / `legacy_host_keys` | root / impinj / 22 / 10 / true | SSH (RShell) access used to set the reader clock (§16) |
| `reader.time_sync.enabled` / `tolerance_ms` / `max_attempts` / `processing_estimate_ms` / `require_host_clock_synchronized` | true / 20 / 3 / 60 / true | Set the reader clock at start-up (§16) |
| `reader.start_line`, `finish_line`, `test` | see §7 | Per-mode inventory profile: `session`, `tag_population`, `impinj_search_mode`, `rf_mode` (`auto`, `reader_default` or an integer), `rf_mode_strategy`, `transmit_power_dbm`, `receive_sensitivity_db`, Impinj report fields |
| `application.mode` | finish_line | Initial mode. A mode changed through the API is persisted and wins after a restart |
| `application.data_directory` | ./data | SQLite database and logs |
| `application.persistence_interval` | 5 s | Inventory snapshot interval |
| `application.log_level`, `log_file`, `log_rotation` | INFO, otreader.log, internal | `internal` rotates by size; `external` is logrotate-friendly |
| `application.auto_start_inventory` | false | Start inventory on first boot |
| `application.resume_inventory_on_restart` | true | Restore the "inventory running" state after a restart or crash |
| `timing.cooldown_seconds` | 300 | Per-tag cooldown after an accepted event |
| `timing.finish_estimation_window_seconds` | 2.0 | How long a tag must be silent before its finish pass is finalised |
| `timing.finish_max_pass_seconds` | 30 | A pass longer than this is finalised anyway |
| `timing.finish_top_window_db` / `finish_min_fit_points` / `finish_cluster_gap_seconds` / `finish_peak_tie_db` | 10 / 5 / 1.0 / 3.0 | Estimator parameters (§8) |
| `timing.finish_time_offset_ms` | 0 | Calibration offset (§8) |
| `timing.clock_mode` / `max_reader_clock_offset_seconds` | auto / 2.0 | §16 |
| `callbacks.url` | null | Initial callback URL. A URL set through the API is persisted and wins |
| `callbacks.timeout_seconds` / `max_retries` / `retry_delay_seconds` / `retry_max_delay_seconds` | 3 / 3 / 1 / 60 | §11 |
| `callbacks.queue_max_size` / `concurrency` | 1000 / 1 | In-memory bound (the overflow waits on disk); 1 keeps delivery in order |
| `callbacks.secret` | null | HMAC-SHA256 signing |
| `callbacks.allow_loopback` / `allow_private_networks` / `allow_public_networks` / `allowed_networks` | true / true / true / null | Callback target restrictions (§17) |
| `api.host` / `api.port` | 0.0.0.0 / 8000 | Listen address |
| `api.api_key` | null | Optional auth |
| `api.cors_origins` | [] | Allowed CORS origins |
| `api.max_request_bytes` | 65536 | Request body limit |
| `test_mode.buffer_size` | 5000 | Test-report ring buffer |

---

## 5. Starting the application

```bash
.venv/bin/python -m app.main --config config.yaml
# API docs: http://<host>:8000/docs   (OpenAPI JSON: /openapi.json)
```

On startup OTReader:

1. Validates the configuration.
2. Sets up logging.
3. Checks the database's integrity, and quarantines and salvages it if it's damaged.
4. Restores the inventory and cooldowns.
5. Starts callbacks and the timing engine.
6. Starts the reader supervisor.

If the reader is unreachable, the application still starts, reports `reconnect_wait`, and keeps
retrying with backoff. Once connected, OTReader resumes inventory automatically if it was
running before the restart.

**Web page.** Open `http://<host>:<api.port>/` in a browser for the operator page:

* **Reader:** manufacturer, model, serial number, firmware, Impinj extensions, reader clock offset,
  temperature and the last error, plus antennas with connection status and gain. The full
  capability details are in a collapsible section.
* **Control:** connect, reconnect and disconnect; choose the **Start line** or **Finish line**
  profile, with the settings the reader actually accepted; **Start reading** and
  **Stop reading**. Actions that interrupt timing ask for confirmation.
* **Antenna test:** the same test as the console client. It shows a peak meter for each antenna
  (dark green track = connected, bright green bar = current read held for 1 s, bright green mark
  = session peak), whether each antenna has seen a tag, read and tag counts, and recent reads.
  Timing controls are locked while the test runs. Click the chevron next to the panel title to
  collapse the panel; the choice is remembered, and the running/inactive status stays visible.
* **Recent timing events.** **Clear** empties this list in your browser only. The event log on the
  server is never deleted from the page, so export, rewind/replay and callbacks are unaffected.
  New events appear as they arrive, and **Show all** brings the earlier ones back.

The page is a single self-contained HTML file with no external resources, so it works on a race
LAN without internet access. The page itself is served without authentication (it contains no
data). If `api.api_key` is set, the page asks for the key once and stores it in the browser; the
**API key** button changes it.

**Running as a service.** `deploy/otreader.service` runs OTReader under systemd as an unprivileged
`otreader` user, from `/opt/otreader`, with its data in `/opt/otreader/data`:

```bash
# 1. User, code, virtual environment
sudo useradd --system --home-dir /opt/otreader --shell /usr/sbin/nologin otreader
sudo git clone <this repo> /opt/otreader          # or copy the checkout there
sudo python3 -m venv /opt/otreader/.venv
sudo /opt/otreader/.venv/bin/pip install -r /opt/otreader/requirements.txt

# 2. Configuration (keep application.data_directory: ./data) and data directory
sudo cp /path/to/your/config.yaml /opt/otreader/config.yaml
sudo chown root:otreader /opt/otreader/config.yaml && sudo chmod 640 /opt/otreader/config.yaml
sudo install -d -o otreader -g otreader -m 750 /opt/otreader/data
sudo -u otreader /opt/otreader/.venv/bin/python -m app.main --config /opt/otreader/config.yaml --check-config

# 3. Install and start
sudo cp /opt/otreader/deploy/otreader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now otreader
journalctl -u otreader -f
```

The config file holds the reader SSH password and any API key, so it is readable only by root and
the `otreader` group. `/var/run/reader` only has to be readable by the `otreader` user; the
default 0644 is fine.

What the unit does:

* **Starts at boot without waiting for the network.** It is ordered after `network.target`, not
  `network-online.target`, which can hold boot for minutes on a Pi whose only cable goes to a
  reader that isn't up yet. OTReader waits for `/var/run/reader` and retries the reader
  connection itself.
* **Waits up to 60 s for the host clock to synchronise** (`ExecStartPre` runs
  `python -m app.clockwait`). The reader clock is set from the host clock once at start-up, and
  only if the host clock is synchronised. On a Pi that boots at the venue, NTP usually syncs a few
  seconds after boot. Without a time source, OTReader starts anyway after 60 s and logs that the
  reader clock was not set.
* **Restarts after any failure** (5 s delay) and never gives up (`StartLimitIntervalSec=0`).
  After a crash or reboot, OTReader recovers its state and resumes inventory if it was running.
* **Stops gracefully.** `systemctl stop` sends SIGINT. OTReader then stops the reader inventory,
  finalises open finish-line passes, commits pending events, writes a final snapshot and closes
  the database, within the 45 s stop timeout.
* **Is sandboxed.** The filesystem is read-only except `/opt/otreader/data`, with no access to
  home directories, no capabilities and only IP/Unix sockets. Data files are created
  owner/group-only (`UMask=0027`). `ProtectClock=` is deliberately not used: its system-call
  filter blocks the read-only `adjtimex()` call OTReader uses to check that the host clock is
  synchronised.
* **Ports.** The API port must be above 1024 unless you add `CAP_NET_BIND_SERVICE` (see the
  comment in the unit).

Day-to-day commands:

```bash
sudo systemctl status otreader        # running? last log lines
journalctl -u otreader --since today  # full log (also in /opt/otreader/data/otreader.log)
sudo systemctl restart otreader       # e.g. after editing config.yaml
```

**Without hardware**, use the simulator:

```bash
.venv/bin/python -m app.llrp.simulator --port 5084 --tags 20 --pass-interval 2
# config: reader.host: 127.0.0.1
```

---

## 6. Connecting to an LLRP reader

1. Tell OTReader the reader's address. By default it is read from **`/var/run/reader`**, a file
   containing just the reader's IP address (IPv4 or IPv6; surrounding whitespace and a trailing
   newline are fine). This suits a host that learns the address at run time, for example a
   Raspberry Pi cabled to the reader with a DHCP-lease hook that writes the file.
   * **Until the file exists and holds a valid IP,** OTReader keeps running, shows
     `waiting_for_address` (with the reason) in `/reader/status`, `/ready` and the web page, and
     re-reads the file every 5 s. Each new reason is logged once. Waiting does not count as a
     failed connection attempt, so it adds no backoff.
   * **The file is re-read on every connection attempt,** so a reader that reboots onto a new
     address is picked up after the connection drops.
   * **If the file changes to a different valid address while connected,** OTReader stops
     inventory on the old reader, connects to the new one, resumes inventory if it was running,
     and sets the new reader's clock.
   * **A file that is briefly missing or empty never drops a working connection.**
   * `/var/run` is cleared at boot, so whatever writes the file must recreate it after each boot,
     and the OTReader service user must be able to read it.

   To use a fixed address instead, set `reader.host_file: null` and `reader.host: <ip>`.
2. Make sure nothing else holds the LLRP connection. Readers accept only one client, so close
   ItemTest, MultiReader, other middleware, and the reader's web UI inventory. If a second
   client is connected, the reader refuses the connection with
   `Failed_A_Client_Initiated_Connection_Already_Exists`, and OTReader reports it and retries.
3. Enable NTP on the reader and the host (§16).
4. Start OTReader and check `GET /api/v1/reader/status` and `GET /api/v1/reader/capabilities`.
5. Verify the antennas with test mode (§14), then `POST /api/v1/reader/start`.

The connection sequence is: TCP connect → wait for the `ConnectionAttemptEvent` (success) →
`GET_READER_CAPABILITIES` → optional factory reset → `IMPINJ_ENABLE_EXTENSIONS` (negotiated) →
`GET_READER_CONFIG` → `SET_READER_CONFIG` (event notifications, keepalive, low-latency reports,
each dropped if rejected) → delete stale ROSpecs/AccessSpecs → add the ROSpec for the current
mode (negotiated) → enable it if inventory should be running.

Reader control endpoints are serialised by one lock, so concurrent requests queue instead of
interleaving. An endpoint reports success only after the reader has confirmed the change
(`ENABLE`/`DISABLE` responses, then verified with `GET_ROSPECS` where supported).

---

## 7. Operating modes

Both modes report every read immediately (`ROReportSpec N=1`) with the ROSpecID, antenna,
PeakRSSI, first/last-seen timestamps, seen count and channel. With Impinj extensions they also
report high-resolution RSSI (0.01 dB). Finish-line and test modes add RF phase and Doppler.

### Start line (`start_line`, ROSpecID 1101)

This mode is for dense populations: hundreds of tags in the field at the gun.

* **Session 2 + Impinj `Single_Target`** (the defaults). Each tag is inventoried once, then
  stays silent: session-2 flags persist while the tag is energised. The reader's time goes to
  tags it hasn't read yet. This is the standard strategy for dense-population inventory.
  `Single_Target_With_Suppression` (TagFocus) can be configured, but it only helps with tags
  that support it.
* **RF mode `robust`**. From the reader's *advertised* mode table, OTReader picks a
  Dense-Interrogator spectral-mask mode with Miller-4 encoding. That trades raw speed for noise
  tolerance and sensitivity among bodies and other readers. It never chooses by mode name.
* `tag_population: 256` gives the reader's Q algorithm a starting hint.
* **Event:** emitted on the first read of a tag not in cooldown. The timestamp is the reader's
  `FirstSeenTimestamp` for that read, with `estimation_method: first_observation`. Later reads
  are counted but suppressed until the cooldown expires.

### Finish line (`finish_line`, ROSpecID 1102)

Tags pass quickly and the population is sparse.

* **Session 0 + Impinj `Dual_Target`** (the defaults). Tags are re-inventoried continuously
  (A→B→A…), which gives the dense RSSI-versus-time profile the estimator needs.
* **RF mode `throughput`**. The advertised mode with the highest effective link rate
  (BDR ÷ Miller factor) maximises reads per pass.
* `tag_population: 32`.
* **Event:** see §8.

Without Impinj extensions, the same session and population go through standard
`C1G2SingulationControl` and there is no search-mode control. Pin RF modes with
`rf_mode: <ModeIdentifier>` or `reader_default`. The per-mode `transmit_power_dbm` is matched
to the largest table entry that doesn't exceed it.

### Changing mode at runtime

`PUT /api/v1/reader/mode {"mode": "start_line"}` does the following:

1. Stops inventory.
2. Deletes the old ROSpec. Many readers, Impinj included, allow only one ROSpec.
3. Installs the new one.
4. Restarts inventory if it was running.

If any step fails, OTReader reinstalls the previous configuration, returns **502** with
`"still in finish_line"`, and the status keeps showing the old mode. When the reader is
disconnected, the mode is stored and applied on the next connect (`applied_to_reader: false`).

Every report is routed by the **ROSpecID it carries**, not by the current mode. Reads buffered
under the old ROSpec are still processed as the old mode, and test reads can never become
timing reads.

---

## 8. Finish-line estimation algorithm

**Physical model.** As a tag passes an antenna, RSSI rises, peaks near the point of closest
approach, and falls again. Near the peak, RSSI in dB against time is roughly a downward
parabola. The estimator finds the time of that maximum.

**Pass tracking.** Reads of a tag that is not in cooldown build up a *pass*. The pass is
finalised when the tag has been silent for `finish_estimation_window_seconds` (default 2 s), or
when it exceeds `finish_max_pass_seconds`. So an event is emitted about 2 s after the tag
leaves the field.

**Estimate (per pass):**

1. **Clusters.** The reads are split wherever a gap exceeds `finish_cluster_gap_seconds`, for
   example a tag that left the field and came back. The cluster with the strongest peak wins.
   If other clusters are within `finish_peak_tie_db` of it, the earliest wins (the first genuine
   crossing) and confidence drops one level.
2. **Antennas.** Each antenna is estimated separately, because antennas have different gain and
   geometry. The antenna with the strongest peak is primary, and the others are reported under
   `details.per_antenna`. If two antennas disagree by more than 250 ms, confidence drops.
3. **Methods, in order of preference:**

| `estimation_method` | When | Notes |
|---|---|---|
| `rssi_quadratic_fit` | ≥ `finish_min_fit_points` reads within `finish_top_window_db` of the peak level | Least-squares parabola, vertex = estimate. The window is measured from a smoothed peak level, because the raw maximum is biased upwards by noise. If the fit fails, it is retried with a window twice as wide. Uncertainty comes from the delta-method standard error |
| `rssi_plateau_center` | Several reads tie at the maximum (common with 1 dB LLRP RSSI) | Centre of the tied reads |
| `rssi_peak_at_edge` | The maximum is the first or last read | The true peak may lie outside the observed reads. Confidence `low` |
| `rssi_peak_interpolation` | Otherwise, with ≥ 3 reads | 3-point parabola through the raw reads around the smoothed maximum (non-uniform spacing), clamped to the neighbouring reads |
| `strongest_of_two` / `two_point_midpoint` | 2 reads | The stronger read if they differ by ≥ 1 dB, otherwise the midpoint |
| `single_observation` | 1 read | The raw timestamp. `estimated: false`, confidence `low` |
| `timestamp_median` | No RSSI reported | Median read time |

4. **Doppler.** With Impinj Doppler enabled, the radial-velocity zero crossing nearest the
   estimate is reported (`details.doppler_zero_crossing_offset_ms`, `doppler_agrees`). It's
   diagnostic only and never moves the estimate.
5. `finish_time_offset_ms` is added to the result (calibration).

**Confidence.** `high` requires a fit on ≥ 7 points, residual ≤ 3 dB RMS, ≥ 2 points on each
side of the peak, and uncertainty ≤ 100 ms. Confidence is lowered for multiple candidate peaks,
disagreeing antennas, or host-clock timestamps. `details.confidence_reasons` explains each
downgrade.

**Accuracy on synthetic data** (`tests/test_finish_estimator.py`): a parabolic profile sampled
every 25 ms, peak at a random sub-sample instant, ±5 ms sampling jitter.

| RSSI noise | Median error | p95 error |
|---|---|---|
| 2 dB Gaussian | ≈ 9 ms | ≈ 28 ms |
| 1 dB quantised + 0.5 dB | ≈ 2 ms | ≈ 6 ms |

These figures show the algorithm is internally correct. **They are not field accuracy.**

**Limitations — read before relying on the timestamps:**

* The event timestamp is an **estimate of when the tag was closest to the antenna**, not a
  measured line crossing. The event carries its method, confidence and `uncertainty_ms`
  (a heuristic).
* A tag on the far side of the body, multipath, reflections from barriers, frequency hopping
  (RSSI varies per channel), and dense bunches (fewer reads per tag) all distort the RSSI
  profile.
* A mat antenna and a side-mounted panel have different geometry. Where the RSSI peak falls
  relative to the timing line depends on how the antenna is mounted, which is a constant bias.
  Remove it by calibration.
* 1 dB standard LLRP RSSI is coarse. Impinj 0.01 dB PeakRSSI gives a much better fit.

**Calibration.** Every finish event stores its raw reads (`GET /api/v1/events/{id}/observations`).

1. Record 30 or more passes that also have an independent reference time, such as photo-finish
   or video on a synchronised clock.
2. Run:

   ```bash
   python -m app.timing.calibration --db data/otreader.db --truth truth.csv
   # truth.csv:  event_id,truth   (or epc,truth), truth = ISO 8601 with timezone
   ```

The tool re-runs the estimator over a grid of settings. It reports bias and the median, p95 and
maximum error for each, and recommends `finish_top_window_db`, `finish_min_fit_points`,
`finish_cluster_gap_seconds` and `finish_time_offset_ms`.

---

## 9. API reference

Base path: `/api/v1`. Interactive docs are at `/docs`. All timestamps are ISO 8601 UTC, for
example `2026-09-30T13:30:15.123456Z`. Query timestamps must include a timezone.

Errors always use this shape:

```json
{"error": {"code": "reader_unavailable", "message": "reader is not connected (state: reconnect_wait)", "details": {...}}}
```

| Status | Code | Meaning |
|---|---|---|
| 400 | `bad_request` | e.g. the confirm interlock |
| 401 | `unauthorized` | Missing or invalid API key |
| 404 | `not_found` | |
| 409 | `invalid_reader_state` or `reader_busy` | e.g. starting inventory during test mode |
| 413 | `request_too_large` | |
| 422 | `validation_error` | |
| 502 | `reader_configuration_failed` | The reader rejected a command |
| 503 | `reader_unavailable` | Reader not connected |

### Health

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness. Always 200 while serving; `status: ok` or `degraded` |
| GET | `/ready` | 200 when persistence is healthy **and** the reader is connected; otherwise 503 with `reasons` |
| GET | `/api/v1/system/status` | Reader, timing, persistence (including `last_successful_write` and `last_inventory_snapshot`), callbacks, test mode |
| GET | `/api/v1/system/config` | Effective configuration, secrets masked |

### Reader

| Method | Path | Description |
|---|---|---|
| GET | `/reader/status` | `connection_state`, which is one of `disconnected`, `connecting`, `configuring`, `idle`, `starting`, `inventorying`, `stopping`, `disconnecting`, `reconnect_wait`. Also host and port, manufacturer, model, serial, firmware, `impinj_extensions`, `mode`, `inventorying`, `test_mode_active`, `last_connected_at`, `last_error`, reconnect progress, clock offset and counters |
| GET | `/reader/capabilities` | See §2, plus `applied_configuration` |
| POST | `/reader/connect` | Connect and initialise. Returns once done; 503/502 on failure, with background retry continuing |
| POST | `/reader/reconnect` | Stop, disconnect, reconnect, re-apply the configuration, and resume the previous activity |
| POST | `/reader/disconnect` | Stop inventory and disconnect; automatic reconnection stays off until the next connect |
| POST | `/reader/start` | Start inventory in the current mode (verified on the reader) |
| POST | `/reader/sync-time` | Set the reader clock from the host now (§16). 409 while reading or testing |
| POST | `/reader/stop` | Stop inventory; the inventory and history are kept |
| PUT | `/reader/mode` | `{"mode": "start_line" \| "finish_line"}`; see §7 |

```bash
curl -X PUT localhost:8000/api/v1/reader/mode -H 'content-type: application/json' -d '{"mode":"start_line"}'
# {"mode":"start_line","changed":true,"applied_to_reader":true,"inventorying":true}
```

### Tags (active inventory)

| Method | Path | Description |
|---|---|---|
| GET | `/tags?epc=&antenna=&seen_after=&seen_before=&status=&offset=&limit=` | Paginated. `status` is one of `tracking`, `cooldown`, `eligible`, `eligible_after_cooldown` |
| GET | `/tags/{epc}` | One record: first/last seen, estimated crossing, peak/last RSSI, antenna counts, observations, suppressed reads, event ID, cooldown remaining |
| DELETE | `/tags/{epc}` | Remove the tag and clear its cooldown and any open pass. **It is eligible again immediately.** Events are kept |
| DELETE | `/tags?confirm=true` | Clear the inventory and all cooldowns (requires `confirm=true`). Events are kept |

### Events

| Method | Path | Description |
|---|---|---|
| GET | `/events?epc=&antenna=&mode=&start_time=&end_time=&after_sequence=&order_by=timestamp\|sequence&order=asc\|desc&offset=&limit=` | `start_time` is inclusive, `end_time` exclusive. `antenna` matches any antenna that saw the tag |
| GET | `/events/{event_id}` | One event |
| GET | `/events/{event_id}/observations` | Raw reads behind the event |
| GET | `/events/export?format=json\|csv&...filters` | Streamed export in sequence order |
| POST | `/events/rewind` | Create a replay cursor (below) |
| GET | `/events/replay/{cursor_id}?limit=&position=` | Next page; advances the cursor |
| DELETE | `/events/replay/{cursor_id}` | Delete a cursor |

A timing event, which is also the callback body:

```json
{
  "event_id": "4f1c2b7e9a0d4e61b2c3d4e5f6a7b8c9",
  "sequence": 42,
  "epc": "300833B2DDD9014000000001",
  "timestamp": "2026-09-30T13:30:15.123456Z",
  "mode": "finish_line",
  "antenna": 1,
  "antennas": [1, 2],
  "peak_rssi": -42.5,
  "first_seen": "2026-09-30T13:30:14.900112Z",
  "last_seen": "2026-09-30T13:30:15.200871Z",
  "observation_count": 8,
  "estimation_method": "rssi_quadratic_fit",
  "confidence": "medium",
  "estimated": true,
  "timestamp_source": "reader_utc",
  "uncertainty_ms": 18.5,
  "created_at": "2026-09-30T13:30:17.310200Z",
  "details": {"fit_points": 6, "fit_rms_db": 0.8, "finalized_by": "quiet_window"},
  "replayed": false
}
```

The stable contract is every field except `details`, which is diagnostic. `replayed` appears
only in API responses. `sequence` is the position in the append-only log, in creation order.
Finish-line events are created about 2 s after the crossing, so sequence order and timestamp
order can differ slightly.

**Rewind and replay.** `POST /events/rewind` takes exactly one of these parameters:

* `{"event_id": "..."}`: replay **starts with that event (inclusive)** and continues with every
  later event, in original sequence order.
* `{"timestamp": "2026-09-30T13:30:00Z"}`: replay every event whose `timestamp` is **≥** that
  instant (inclusive), in original sequence order.

```json
201 {"cursor_id": "8eb8…", "start_sequence": 2, "end_sequence": 11, "position": 1, "total_events": 10,
     "next_url": "/api/v1/events/replay/8eb8…",
     "live_continuation": "/api/v1/events?order_by=sequence&after_sequence=11", ...}
```

The cursor covers events up to `end_sequence`, the head of the log at rewind time. Page through
it with `GET next_url?limit=100` until `has_more` is false. Replayed events carry
`"replayed": true`. Then switch to live polling with `live_continuation`. Cursors are
per-client: they never change history or other clients' positions, and they expire after
`ttl_seconds` (default 24 h). If a response is lost, re-read with `?position=<previous position>`.

### Callbacks

| Method | Path | Description |
|---|---|---|
| GET | `/callback` | URL (credentials redacted), enabled, pending/failed/delivered counts, in-memory queue, last success, last failure |
| PUT | `/callback` | `{"url": "http://192.168.8.20:8080/rfid"}`; validated (§17) and persisted |
| DELETE | `/callback` | Disable callbacks. RFID processing and the event log continue |
| GET | `/callback/queue?status=undelivered\|pending\|failed\|delivered\|all` | Attempts, next attempt, last error, HTTP status |
| POST | `/callback/retry` | `{"event_ids": [...]?, "include_pending": false}`; resets failed (and optionally pending) callbacks for an immediate retry |

### Test mode

| Method | Path | Description |
|---|---|---|
| POST | `/test/start` | See §14 |
| GET | `/test/reports?after_sequence=&limit=` | Raw reads and a per-antenna summary |
| POST | `/test/stop` | Restore timing |

---

## 10. Persistence and disaster recovery

Everything is stored in SQLite (`data/otreader.db`), in WAL mode with `synchronous=FULL`:

| Table | Contents |
|---|---|
| `events` | Append-only event log; unique `event_id` and unique `(epc, mode, timestamp)` |
| `event_observations` | Raw reads for each event |
| `callbacks` | The durable callback queue |
| `inventory` | Snapshot of the tag inventory and cooldowns |
| `tag_resets` / `meta` | Operator reset markers, runtime state, snapshot time |
| `replay_sessions` | Replay cursors |

* **Events are committed as soon as they are accepted.** Each commit is a single transaction
  that writes the event row, its raw reads, its pending callback and the tag's cooldown row.
  After a crash there is never an event without its cooldown or callback, and never a cooldown
  without its event. Commit failures (for example, a full disk) are retried with backoff; the
  event stays queued in memory and nothing is dropped.
* **Inventory snapshots** are written every `persistence_interval` seconds (default 5 s) when
  the inventory has changed, and at least every 60 s regardless. The table is replaced inside
  one transaction, so a reader sees either the old or the new snapshot, never a mix. A snapshot
  never persists the cooldown of an event that hasn't been committed yet.
* **Removing or clearing tags** never deletes events. It writes reset markers, so recovery
  doesn't bring back cooldowns the operator cleared.
* **Recovery on startup:**
  1. `PRAGMA quick_check`.
  2. If the database is corrupt or isn't SQLite, it is moved to
     `otreader.db.corrupt-<timestamp>`, a fresh database is created, and every readable row is
     salvaged into it (`startup_database_check` in `/system/status`).
  3. The inventory and cooldowns are restored from the snapshot. Cooldowns use wall-clock
     expiry, re-anchored to the monotonic clock, and are capped at the configured duration.
  4. The event log is cross-checked: a tag with an event inside the cooldown window gets its
     cooldown back unless the operator reset it later.
  5. Pending callbacks resume.
  6. The reader state (mode, inventory running) is restored.
* Interrupted writes roll back automatically (SQLite WAL). Previously committed events are
  unaffected.
* **Clean shutdown** finalises open passes, drains event commits, writes a final snapshot and
  checkpoints the WAL.
* **Remaining window:** reads that are still in an *open* finish-line pass, or an event
  accepted within the last few milliseconds before a power loss, are lost with process memory.
  The event log has everything committed before that.

`GET /api/v1/system/status` → `persistence` shows `last_successful_write`,
`last_inventory_snapshot`, errors and the startup recovery report.

---

## 11. Callback delivery and retries

* After an event is committed, it is POSTed as JSON (schema in §9) to the callback URL with
  these headers:
  * `Content-Type: application/json`
  * `X-OTReader-Event-Id`
  * `X-OTReader-Signature: sha256=<hex HMAC of the body>`, when `callbacks.secret` is set
* A 2xx response means `delivered`. Any other status, a connection error or a timeout
  (`timeout_seconds`) is a failed attempt. It is rescheduled with exponential backoff
  (`retry_delay_seconds × 2^(attempt-1)`, capped at `retry_max_delay_seconds`). After
  `1 + max_retries` attempts the callback is marked `failed` until someone calls
  `POST /callback/retry`.
* **Delivery is at least once.** A crash between the receiver's 2xx and the status being
  recorded causes one re-delivery. **Receivers must de-duplicate on `event_id`.**
* **Backpressure.** The durable SQLite table is the real queue. The in-memory queue holds at
  most `queue_max_size` IDs; anything beyond that waits on disk and is loaded as space frees
  up. A slow or dead endpoint never blocks tag processing and never grows memory without
  limit, and no event is discarded.
* **Order.** With `concurrency: 1`, events are sent in sequence order, except that retries are
  rescheduled. Use `sequence` if ordering matters to you.
* **Disabling callbacks** (`DELETE /callback`): events accepted while callbacks are off are
  still logged, but not queued for delivery. Use rewind or `GET /events` to catch up. Callbacks
  that were already pending stay pending and are sent after you re-enable.
* Redirects are not followed, and at most `max_response_bytes` of a response body is read.

---

## 12. Automated tests

```bash
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest            # 181 tests, ~35 s, no hardware needed
```

| Test file | Coverage |
|---|---|
| `test_config.py` | Validation and error messages |
| `test_capabilities.py` | Capability parsing, availability states |
| `test_llrp_normalization.py` | Report normalisation, timestamps and clock mapping, pyllrp quirks |
| `test_configuration_selection.py` | RF mode, power and ROSpec construction per mode (validated by pyllrp) |
| `test_finish_estimator.py` | Synthetic ideal, noisy, quantised, single, double, plateau, edge, multi-antenna and reappearing profiles; calibration |
| `test_cooldown_inventory.py`, `test_processor.py` | Cooldown, removal, recovery, pass finalisation, test isolation, commit retry |
| `test_repository.py` | Ordering, filters, rewind semantics, export, snapshots, callback queue, corruption salvage, recovery |
| `test_callbacks.py` | Retries, failure, manual retry, bounded queue spill, timeouts, signing, URL validation |
| `test_integration.py` | Full stack + REST API against the LLRP simulator over TCP: connection, discovery, configuration, start/stop, mode change and its failure, Impinj negotiation, generic reader, reconnection after a network drop, keepalive watchdog, reader unavailable at startup, report processing, callbacks, test-mode isolation, restart recovery, concurrent API requests, rewind/replay/export, uptime and offset clocks, auth and limits |

**Hardware tests** are marked `hardware` and excluded by default:

```bash
OTREADER_HARDWARE_HOST=192.168.8.100 .venv/bin/python -m pytest -m hardware -s tests/hardware
```

They connect, print the discovered capabilities and the configuration the reader accepted for
each mode, inventory for a few seconds per mode, exercise test mode, and check the reader clock
against the host.

---

## 13. Troubleshooting reader connectivity

| Symptom (`/reader/status`) | Likely cause and fix |
|---|---|
| `waiting_for_address` | `/var/run/reader` is missing, empty or not an IP address. `address_problem` says which. Check the process that writes it, and file permissions |
| `reconnect_wait`, "timed out connecting" | Wrong IP, reader not booted, wrong subnet. Check `ping <reader>` and `nc -vz <reader> 5084` |
| "reader refused connection: Failed_A_Client_Initiated_Connection_Already_Exists" | Another LLRP client is connected. Close it, or reboot the reader |
| "connection refused" | LLRP is disabled on the reader, or the port is wrong |
| Connects, then `reader_configuration_failed` | See `last_error` and `/reader/capabilities` → `applied_configuration.*.negotiation_failures`. Pin `rf_mode`, or remove `transmit_power_dbm` |
| Frequent "keepalive timeout" | Lossy link or Wi-Fi bridge. Raise `keepalive_missed_limit`, or use wired Ethernet |
| Inventorying but no events | Use test mode (§14). Check `counters.observations`, antenna connections, and whether tags are in cooldown (`/tags?status=cooldown`) |
| `observations_unknown_rospec` counter rising | Another application added ROSpecs. Keep `delete_existing_rospecs: true` |
| `timestamp_source: reader_utc_corrected` | The reader clock is more than 2 s off the host clock. Fix NTP on the reader (§16) |
| Report buffer warnings or overflow in the log | The network can't keep up with the reader. Check cabling and reduce `tag_population` |

Set `application.log_level: DEBUG` for per-message logging. The log file is in the data
directory.

---

## 14. Antenna test mode

Use test mode to check antenna placement and cabling before the race without creating timing
data:

```bash
curl -X POST localhost:8000/api/v1/test/start
curl 'localhost:8000/api/v1/test/reports?limit=20'        # poll with after_sequence=<next_after_sequence>
curl -X POST localhost:8000/api/v1/test/stop
```

* Test mode stops timing inventory (and remembers whether it was running), deletes the timing
  ROSpec, then installs and starts a separate **test ROSpec (ID 1199)**.
* Each report shows the EPC, antenna, RSSI and RSSI source, reader timestamp and its source,
  first/last seen, seen count, channel, phase, Doppler and host receive time. The summary gives
  reads, unique tags and RSSI minimum/maximum/average for each antenna. The buffer is bounded
  by `test_mode.buffer_size`.
* Reports are routed by the ROSpecID *in each report*. Test reads never enter the inventory,
  never create events, never apply cooldown and never trigger callbacks, even if they arrive
  after test mode ends.
* While test mode is active, `/reader/start`, `/reader/stop` and `/reader/mode` return 409.
* `POST /test/stop` removes the test ROSpec, reinstalls the timing ROSpec, and resumes timing
  inventory if it was running before.

### Console client

`clients/antenna_test.py` is a terminal client for this workflow. It talks only to the REST API,
so it can run on any machine that can reach the middleware:

```bash
python -m clients.antenna_test --url http://192.168.8.10:8000        # or: otreader-antenna-test
```

It prints the reader's identity and antennas, then asks before starting test mode. It warns you
if timing inventory is running, because test mode pauses timing. It then refreshes a table every
second:

```
  Antenna  Connection      Tag seen     Reads  Tags  Peak RSSI  Last read (UTC)
  1        connected       YES            294     5  -37.0 dBm  15:34:59.026
  2        connected       YES            147     3  -37.7 dBm  15:35:00.473
  3        DISCONNECTED    no               0     0          -  -
```

The table lists every antenna the reader supports, with its connection state (updated live from
reader antenna events) and whether a tag has been seen on it since the test started. Recent reads
are shown below the table. Ctrl+C stops test mode and restores timing.

Above the table is a **peak meter** for each antenna, on a dBm scale:

* **Dark green track:** the antenna is connected. The track is grey when the antenna is
  disconnected or its state is unknown.
* **Bright green bar:** RSSI of the most recent read on that antenna. The bar holds for `--hold`
  seconds (default 1 s) after the last read and then clears, so a tag that has left the field
  doesn't look like a live signal.
* **Bright green mark `┃`:** the highest RSSI seen on that antenna this test session.

```
  Ant 1   [####################-------------------|----------]  now  -61.0  peak  -38.1
  Ant 3   [..................................................]  now      -  peak      -  DISCONNECTED
               -80     -70     -60      -50     -40     -30  dBm
```

In a colour terminal the meter is drawn with real colours; with `--no-color`, or when output is
redirected, it falls back to the ASCII form shown above. The screen redraws in place (default
every 0.25 s), so it doesn't flicker.

If another client already started test mode, this client monitors that session and leaves it
running on exit.

Options:

| Option | Description |
|---|---|
| `--api-key` (or `OTREADER_API_KEY`) | API key, if the middleware requires one |
| `--interval` | Refresh interval in seconds (default 0.25) |
| `--hold` | How long the current-read bar is held after the last read (default 1 s) |
| `--rssi-min` / `--rssi-max` | Meter scale in dBm (default -85 to -25) |
| `--meter-width` | Meter width in characters (default 50) |
| `--duration N` | Stop automatically after N seconds |
| `--yes` | Answer yes to all prompts (non-interactive use) |
| `--no-color` | Disable colour output |

---

## 15. Example integration with race timing software

A receiver that trusts callbacks but can recover anything it missed:

```python
# receiver.py  —  pip install fastapi uvicorn httpx
import hashlib, hmac, httpx
from fastapi import FastAPI, Request, HTTPException

OTREADER = "http://192.168.8.10:8000/api/v1"
SECRET = b"shared-secret"            # = callbacks.secret
seen: set[str] = set()               # persist this in your own database
last_sequence = 0

app = FastAPI()

def record(ev: dict) -> None:
    global last_sequence
    if ev["event_id"] in seen:       # at-least-once delivery -> de-duplicate
        return
    seen.add(ev["event_id"])
    last_sequence = max(last_sequence, ev["sequence"] or 0)
    print(ev["epc"], ev["mode"], ev["timestamp"], ev["confidence"], ev["estimation_method"])

@app.post("/rfid")
async def rfid(request: Request):
    body = await request.body()
    sig = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, request.headers.get("X-OTReader-Signature", "")):
        raise HTTPException(401)
    record(await request.json())
    return {"ok": True}

async def catch_up() -> None:
    """After our own downtime: replay anything we missed, then continue live."""
    async with httpx.AsyncClient() as c:
        page = (await c.get(f"{OTREADER}/events", params={
            "order_by": "sequence", "after_sequence": last_sequence, "limit": 1000})).json()
        for ev in page["events"]:
            record(ev)
```

Register the receiver:

```bash
curl -X PUT localhost:8000/api/v1/callback -H 'content-type: application/json' \
     -d '{"url":"http://192.168.8.20:9000/rfid"}'
```

To replay a whole race segment, for example after re-importing results:

```bash
curl -X POST localhost:8000/api/v1/events/rewind -H 'content-type: application/json' \
     -d '{"timestamp":"2026-09-30T13:00:00Z"}'
curl 'localhost:8000/api/v1/events/replay/<cursor_id>?limit=500'   # repeat until has_more=false
```

Recommended race-day sequence:

1. Start OTReader and confirm `/ready` returns 200.
2. Check antennas with test mode.
3. `PUT /reader/mode` to `start_line` and `POST /reader/start`.
4. After the start, switch mode to `finish_line`, or use a second reader and OTReader instance
   for the finish.
5. At the end, export with `GET /events/export?format=csv`.

---

## 16. Timekeeping

* Internally, timestamps are integer microseconds since the epoch, UTC. Externally they are ISO
  8601 with a `Z` suffix and microsecond precision. The *reader's* real resolution depends on
  its firmware; LLRP carries microseconds.
* Each event's `timestamp_source` says where its time came from:

| `timestamp_source` | Meaning | Uncertainty |
|---|---|---|
| `reader_utc` | The reader's `FirstSeenTimestampUTC`, used as-is. Preferred when the reader is NTP-synchronised | Depends on the reader's NTP |
| `reader_utc_corrected` | Reader UTC plus the measured reader-to-host offset. Used when the reader clock is off by more than `max_reader_clock_offset_seconds` (`clock_mode: auto`), or always with `reader_corrected` | About the minimum network latency; tagged 10 ms |
| `reader_uptime_mapped` | The reader reported only uptime timestamps; mapped to host UTC through the offset between uptime and host clock | Same |
| `host_receive` | No reader timestamp was available, or `clock_mode: host`. Includes network and buffering latency | Events are downgraded one confidence level |

* **Offset measurement.** For every report, `host_receive − reader_timestamp = offset + latency`.
  The running minimum is the best estimate of the offset. It is shown as
  `clock.reader_minus_host_ms` in `/reader/status`. A few milliseconds is normal; seconds means
  one of the clocks isn't synchronised.
* When the reader aggregates several reads into one report (`TagSeenCount > 1`), the reported
  peak RSSI's instant is unknown within [first, last seen]. OTReader uses the midpoint and
  records half the span as the uncertainty.
* Cooldowns, pass windows and backoff use the monotonic clock, so NTP adjustments don't affect
  them. Only wall-clock values are persisted.
* The callback delivery time is never used as an observation time. `created_at` is when
  OTReader accepted the event; `timestamp` is the estimated crossing.

**Recommendation:** run NTP (chrony) on the host, point the reader's NTP at the host or at the
same server, and check `reader_minus_host_ms` before the start.

---

### Setting the reader clock at start-up

Readers without NTP drift. On the test R420, the clock was 350 ms slow and wandered by tens of
milliseconds a minute. So when OTReader starts, it sets the reader's clock from the host's clock
**before the first LLRP connection**, using the Impinj RShell command
`config system time YYYY.MM.DD-HH:mm:ss` over SSH (`reader.ssh` credentials, default
`root` / `impinj`).

* **Whole seconds only.** RShell accepts only whole seconds, so the command is timed so the
  reader sets its clock as close as possible to the start of the named second. OTReader measures
  the SSH round trip (RTT) and sends at `second − RTT/2 − expected processing time`, spinning for
  the final milliseconds.
* **Processing time varies.** RShell spends 50–100 ms on an R420 before it sets the clock, and it
  sets the clock just before replying. OTReader therefore estimates the achieved error from the
  reply timing (`reader − host ≈ second − (reply − RTT/2)`). If the error is outside
  `time_sync.tolerance_ms` (20 ms), it retries using the processing time it just observed, up to
  `max_attempts`.
* **Independent check.** The LLRP connection timestamp then measures the achieved offset
  independently (`time_sync.measured_offset_ms` in `/reader/status`).
* **Measured on a Speedway R420:** −108 → +10 ms, −78 → +12 ms, and +30 → +15 ms (one retry).
  Estimates agreed with the LLRP measurement within about 4 ms.
* **Legacy SSH.** Older Impinj firmware (OpenSSH 4.4) offers only `ssh-rsa`/`ssh-dss` host keys.
  OTReader tries modern algorithms first, then falls back (`reader.ssh.legacy_host_keys`, the
  equivalent of `HostKeyAlgorithms=+ssh-rsa`). It uses asyncssh, because paramiko ≥ 5 can no
  longer negotiate `ssh-rsa`. The reader's host key is pinned on first use in
  `data/reader_host_keys.json`. If the reader is replaced, delete its entry there.
* **Host clock safeguard.** The reader can only be as right as the host. With
  `time_sync.require_host_clock_synchronized: true` (the default), OTReader won't set the reader
  unless the kernel reports this host's clock as NTP- or GPS-synchronised (the same flag
  `timedatectl` shows). A **Raspberry Pi has no battery-backed clock**; cabled only to the reader,
  it boots with a stale time. Give it a time source (NTP over Wi-Fi or cellular, a GPS receiver,
  or an RTC board), or the sync is skipped and reported.
* **Failure handling.** A failed sync never blocks timing. If SSH is not reachable yet, the sync
  is retried before each LLRP connection attempt until LLRP connects. Other failures (wrong
  password, rejected command, host key mismatch) are reported in `/reader/status` → `time_sync`
  and in the log.
* **Setting it again later.** Use **Set reader clock** on the web page or
  `POST /api/v1/reader/sync-time`. This is refused while reading or during an antenna test,
  because stepping the reader clock mid-race would distort timestamps.
* **Accuracy.** Expect ±10–20 ms plus the host clock's own error. Between syncs, the offset
  measured continuously over LLRP (`clock.reader_minus_host_ms`) shows how far the reader has
  drifted.
* **Other readers.** Readers without RShell (non-Impinj) simply report the sync as failed. Set
  `reader.time_sync.enabled: false` for them, and when using the simulator.

## 17. Security and operations

* **Authentication** is optional. Set `api.api_key` and send `X-API-Key: <key>` or
  `Authorization: Bearer <key>`. `/health`, `/ready`, `/docs` and `/openapi.json` stay open
  for monitoring.
* **Secrets** (API key, callback secret, reader SSH password, credentials inside the callback
  URL) are masked in all API responses. The reader host and port are shown, because they are needed for diagnostics.
* **Callback URL validation:** only `http` and `https` are allowed, and every resolved address
  is checked. Link-local addresses (including 169.254.169.254 cloud metadata), multicast,
  unspecified and reserved addresses are always rejected. Loopback, private and public targets
  can each be switched off, and `allowed_networks` restricts targets to a CIDR allow-list.
  Recommended setting if the API is reachable beyond the race LAN:
  `allowed_networks: ["<timing LAN>/24"]`. DNS names are checked when set, not on every
  request.
* Request bodies are limited (`api.max_request_bytes` → 413), and so are callback response
  reads. CORS is off unless `api.cors_origins` is set.
* **Logs** go through a background queue so file I/O never blocks the event loop. Rotation is
  either internal (size-based) or external (logrotate with `WatchedFileHandler`, see
  `deploy/logrotate.conf`); neither interrupts operation.
* `DELETE /tags` requires `?confirm=true` to prevent accidental clears on race day.

## Project layout

```
app/
  main.py, services.py, config.py, clock.py, models.py, logging_setup.py
  api/        routes.py, schemas.py
  llrp/       transport.py, client.py, capabilities.py, configuration.py, events.py, simulator.py
  timing/     processor.py, finish_estimator.py, cooldown.py, inventory.py, test_mode.py, calibration.py
  persistence/ repository.py, recovery.py
  callbacks/  dispatcher.py, url_validation.py
tests/        unit + integration (simulator), hardware/ (opt-in)
deploy/       otreader.service, logrotate.conf
config.yaml, requirements.txt, pyproject.toml
```

`llrp/transport.py` was added to the suggested structure to keep asyncio framing separate from
the reader state machine in `client.py`. `services.py` is the dependency-injection container.
