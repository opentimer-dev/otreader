"""Reader connection manager.

Responsibilities:

* Owns the single :class:`LLRPConnection` to the reader.
* Serialises every state-changing reader operation behind one asyncio lock so
  concurrent API requests can never interleave conflicting commands.
* Discovers capabilities, negotiates Impinj extensions, installs the ROSpec for
  the current operating mode (with progressive feature fallback) and verifies
  start/stop via GET_ROSPECS where the reader supports it.
* A supervisor task reconnects with exponential backoff after an unexpected
  disconnect, watches LLRP keepalives, and restores the desired state
  (mode, inventory running, test mode) after reconnecting.
* Routes each tag report by the ROSpecID embedded in the report, so reports
  from the antenna-test ROSpec can never enter the timing pipeline even if they
  arrive after a mode switch.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from pyllrp import pyllrp as P

from app.clock import Clock, ReaderClockMapper, SystemClock, host_clock_status, iso_us
from app.config import AppConfig, InventoryProfileConfig, OperatingMode
from app.llrp.capabilities import (
    IMPINJ_PEN,
    Availability,
    ReaderCapabilities,
    apply_capabilities_response,
    apply_config_response,
    apply_impinj_capabilities,
    empty_capabilities,
    finalize_capabilities,
)
from app.llrp.configuration import (
    FEATURE_LEVELS,
    ROSPEC_ID_FINISH_LINE,
    ROSPEC_ID_START_LINE,
    ROSPEC_ID_TEST,
    build_add_rospec,
    build_reader_config,
    make_plan,
)
from app.llrp.rshell import HostKeyStore, TimeSyncResult, sync_reader_time
from app.llrp.events import llrp_status, normalize_report, parse_reader_event, parse_timestamp_param
from app.llrp.transport import LLRPConnection, LLRPError, LLRPStatusError
from app.models import Observation, ReaderActivity, ReaderState, ReportPurpose

log = logging.getLogger(__name__)

ObservationSink = Callable[[list[Observation], ReportPurpose, OperatingMode | None], None]
StatePersister = Callable[[dict[str, Any]], Awaitable[None]]


class ReaderOperationError(Exception):
    http_status = 500
    code = "reader_error"

    def __init__(self, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class ReaderBusyError(ReaderOperationError):
    http_status = 409
    code = "reader_busy"


class ReaderStateError(ReaderOperationError):
    http_status = 409
    code = "invalid_reader_state"


class ReaderUnavailableError(ReaderOperationError):
    http_status = 503
    code = "reader_unavailable"


class ReaderConfigurationError(ReaderOperationError):
    http_status = 502
    code = "reader_configuration_failed"


def read_reader_address(path: str) -> tuple[str | None, str | None]:
    """Read the reader IP address from *path*.

    Returns (address, None) when the file holds exactly one IPv4/IPv6 address
    (surrounding whitespace allowed), otherwise (None, reason).
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(1024)
    except FileNotFoundError:
        return None, f"{path} does not exist yet"
    except OSError as exc:
        return None, f"cannot read {path}: {exc.strerror or exc}"
    value = text.strip()
    if not value:
        return None, f"{path} is empty"
    try:
        return str(ipaddress.ip_address(value)), None
    except ValueError:
        shown = value if len(value) <= 60 else value[:57] + "..."
        return None, f"{path} does not contain an IP address (found {shown!r})"


def rospec_id_for_mode(mode: OperatingMode) -> int:
    return ROSPEC_ID_START_LINE if mode == OperatingMode.START_LINE else ROSPEC_ID_FINISH_LINE


class ReaderManager:
    def __init__(
        self,
        config: AppConfig,
        sink: ObservationSink,
        *,
        clock: Clock | None = None,
        persist_state: StatePersister | None = None,
        connection_factory: Callable[..., LLRPConnection] = LLRPConnection,
        time_syncer: Callable[[], Awaitable[TimeSyncResult]] | None = None,
    ) -> None:
        self.config = config
        self.rcfg = config.reader
        self._sink = sink
        self._clock = clock or SystemClock()
        self._persist = persist_state
        self._connection_factory = connection_factory
        self._time_syncer = time_syncer or self._default_time_sync
        # Reader clock is set once per application start, before the first LLRP connection.
        self._time_sync_pending = config.reader.time_sync.enabled
        self.time_sync: dict[str, Any] | None = None
        # Reader address: from reader.host_file (re-read until valid) or reader.host.
        self._host: str | None = None if config.reader.host_file else config.reader.host
        self.address_problem: str | None = None
        self._last_address_check = 0.0
        self._parse_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="llrp-parse")
        self.clock_mapper = ReaderClockMapper(config.timing.clock_mode, config.timing.max_reader_clock_offset_seconds)

        # Desired state (what the operator asked for; restored after reconnects).
        self.desired_connected = config.reader.auto_connect
        self.desired_inventory = False
        self.mode: OperatingMode = config.application.mode

        # Actual state.
        self.state = ReaderState.DISCONNECTED
        self.activity = ReaderActivity.NONE
        self.caps: ReaderCapabilities = empty_capabilities()
        self.applied_configuration: dict[str, Any] = {}
        self.inventory_verified: bool | None = None
        self.last_connected_at_us: int | None = None
        self.last_disconnected_at_us: int | None = None
        self.last_error: dict[str, Any] | None = None
        self.reconnect_attempts = 0
        self.next_reconnect_at_us: int | None = None
        self.keepalive_active = False
        self._test_resume_timing = False

        self._conn: LLRPConnection | None = None
        self._intentional_close = False
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._shutdown = False
        self._supervisor: asyncio.Task[None] | None = None
        self.counters: dict[str, int] = defaultdict(int)
        self.reader_events: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ lifecycle

    async def start(self, restored: dict[str, Any] | None = None) -> None:
        """Start the supervisor.  Never blocks on the reader being reachable."""
        if restored:
            if restored.get("mode") in (m.value for m in OperatingMode):
                self.mode = OperatingMode(restored["mode"])
            if "desired_connected" in restored:
                self.desired_connected = bool(restored["desired_connected"])
            if self.config.application.resume_inventory_on_restart and restored.get("desired_inventory"):
                self.desired_inventory = True
        elif self.config.application.auto_start_inventory:
            self.desired_inventory = True
        self._supervisor = asyncio.create_task(self._supervise(), name="reader-supervisor")

    async def shutdown(self) -> None:
        self._shutdown = True
        self._wake.set()
        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except (asyncio.CancelledError, Exception):
                pass
        try:
            async with asyncio.timeout(15):
                async with self._lock:
                    await self._teardown(graceful=True, stop_inventory=True)
        except Exception as exc:
            log.warning("Error during reader shutdown: %s", exc)
            if self._conn is not None:
                await self._conn.close(graceful=False)
        self._parse_executor.shutdown(wait=False)

    @property
    def connected(self) -> bool:
        return self._conn is not None and not self._conn.closed.is_set() and self.state in (
            ReaderState.IDLE,
            ReaderState.INVENTORYING,
            ReaderState.STARTING,
            ReaderState.STOPPING,
            ReaderState.CONFIGURING,
        )

    @property
    def inventorying(self) -> bool:
        return self.state == ReaderState.INVENTORYING

    # ------------------------------------------------------------------ supervisor

    async def _supervise(self) -> None:
        while not self._shutdown:
            try:
                if self.desired_connected and not self.connected:
                    await self._attempt_connect_from_supervisor()
                elif self.connected:
                    await self._monitor_connection()
                else:
                    self._wake.clear()
                    await self._wake.wait()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - supervisor must never die
                log.exception("Reader supervisor error")
                await asyncio.sleep(1.0)

    @property
    def host(self) -> str | None:
        return self._host

    def _refresh_address(self) -> str | None:
        """Re-read reader.host_file.  Returns the address to connect to, or None if not available.

        Logs only when the address or the reason it is unavailable changes.  A missing or invalid
        file never clears an address already in use (the caller decides what a change means).
        """
        path = self.rcfg.host_file
        if not path:
            return self._host
        self._last_address_check = time.monotonic()
        address, problem = read_reader_address(path)
        if address is None:
            if problem != self.address_problem:
                log.warning("Reader address not available: %s (checking every %gs)", problem,
                            self.rcfg.host_file_poll_seconds)
            self.address_problem = problem
            return None
        if self.address_problem is not None or address != self._host:
            log.info("Reader address %s read from %s", address, path)
        self.address_problem = None
        return address

    def _adopt_address(self, address: str) -> None:
        if self._host is not None and address != self._host:
            log.warning("Reader address changed %s -> %s", self._host, address)
            # A different reader (or the same one renumbered): set its clock again.
            self._time_sync_pending = self.rcfg.time_sync.enabled
        self._host = address

    async def _wait_for_address(self) -> bool:
        """Supervisor: block (re-reading every poll interval) until an address is available."""
        address = self._refresh_address()
        if address is not None:
            self._adopt_address(address)
            return True
        self.state = ReaderState.WAITING_FOR_ADDRESS
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=self.rcfg.host_file_poll_seconds)
        except TimeoutError:
            pass
        return False

    async def _attempt_connect_from_supervisor(self) -> None:
        if self.rcfg.host_file and not await self._wait_for_address():
            return  # not a failed connection attempt: no backoff while waiting for the file
        try:
            async with self._lock:
                if self.connected or not self.desired_connected or self._shutdown:
                    return
                await self._establish()
            self.reconnect_attempts = 0
            self.next_reconnect_at_us = None
        except Exception as exc:
            self.reconnect_attempts += 1
            delay = min(
                self.rcfg.reconnect_interval * (2 ** max(0, self.reconnect_attempts - 1)),
                self.rcfg.reconnect_max_interval,
            )
            delay *= random.uniform(0.9, 1.1)
            self.next_reconnect_at_us = self._clock.now_us() + int(delay * 1_000_000)
            if self.state != ReaderState.DISCONNECTED or self.desired_connected:
                self.state = ReaderState.RECONNECT_WAIT
            log.warning(
                "Reader connection attempt %d to %s:%d failed (%s); retrying in %.1fs",
                self.reconnect_attempts, self._host, self.rcfg.port, exc, delay,
            )
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def _monitor_connection(self) -> None:
        conn = self._conn
        assert conn is not None
        tick = 1.0
        closed_wait = asyncio.ensure_future(conn.closed.wait())
        wake_wait = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({closed_wait, wake_wait}, timeout=tick, return_when=asyncio.FIRST_COMPLETED)
        finally:
            closed_wait.cancel()
            wake_wait.cancel()
        self._wake.clear()
        if conn.closed.is_set():
            if not self._intentional_close and self._conn is conn:
                async with self._lock:
                    if self._conn is conn:
                        self._on_unexpected_disconnect(conn.close_reason or "connection lost")
            return
        if self.rcfg.host_file and time.monotonic() - self._last_address_check >= self.rcfg.host_file_poll_seconds:
            address = self._refresh_address()
            # Only a *different valid* address moves us; a briefly missing/empty file never drops
            # a working connection.
            if address is not None and address != self._host:
                async with self._lock:
                    if self._conn is conn:
                        log.warning("Reader address in %s changed to %s; reconnecting", self.rcfg.host_file, address)
                        await self._teardown(graceful=True, stop_inventory=True)
                        self._adopt_address(address)
                self._wake.set()
                return
        if self.keepalive_active and self.rcfg.keepalive_interval_ms > 0:
            limit = self.rcfg.keepalive_interval_ms / 1000.0 * self.rcfg.keepalive_missed_limit + 1.0
            silent = conn.seconds_since_last_message()
            if silent > limit:
                log.error("No LLRP traffic (keepalives) for %.1fs; treating connection as dead", silent)
                async with self._lock:
                    if self._conn is conn:
                        await conn.close(graceful=False)
                        self._on_unexpected_disconnect(f"keepalive timeout ({silent:.1f}s without traffic)")

    def _on_unexpected_disconnect(self, reason: str) -> None:
        self.counters["unexpected_disconnects"] += 1
        self._record_error("connection", f"reader connection lost: {reason}")
        log.error("Reader connection lost: %s (will reconnect)", reason)
        self._conn = None
        self.last_disconnected_at_us = self._clock.now_us()
        self.state = ReaderState.RECONNECT_WAIT if self.desired_connected else ReaderState.DISCONNECTED
        # Activity is restored from desired state on reconnect; test mode is resumed too.
        self._wake.set()

    # ------------------------------------------------------------------ helpers

    @asynccontextmanager
    async def _operation(self):
        timeout = self.config.api.reader_operation_timeout_seconds
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
        except TimeoutError:
            raise ReaderBusyError(f"another reader operation is in progress (waited {timeout:.0f}s)") from None
        try:
            yield
        finally:
            self._lock.release()

    def _record_error(self, operation: str, message: str) -> None:
        self.last_error = {"at": iso_us(self._clock.now_us()), "operation": operation, "message": message}

    async def _save_state(self) -> None:
        if self._persist is None:
            return
        try:
            await self._persist(
                {
                    "mode": self.mode.value,
                    "desired_inventory": self.desired_inventory,
                    "desired_connected": self.desired_connected,
                }
            )
        except Exception:
            log.exception("Failed to persist reader runtime state")

    async def _transact_ok(self, msg: Any, what: str | None = None, timeout: float | None = None) -> Any:
        assert self._conn is not None
        name = what or type(msg).__name__.replace("_Message", "")
        resp = await self._conn.transact(msg, timeout=timeout)
        ok, detail = llrp_status(resp)
        if not ok:
            raise LLRPStatusError(name, detail)
        return resp

    async def _transact_quiet(self, msg: Any) -> tuple[bool, str]:
        assert self._conn is not None
        try:
            resp = await self._conn.transact(msg)
        except LLRPError as exc:
            return False, str(exc)
        return llrp_status(resp)

    # ------------------------------------------------------------------ establish

    # ------------------------------------------------------------------ reader clock (SSH)

    async def _default_time_sync(self) -> TimeSyncResult:
        ssh, ts = self.rcfg.ssh, self.rcfg.time_sync
        return await sync_reader_time(
            self._host or "", port=ssh.port, username=ssh.username, password=ssh.password,
            timeout=ssh.timeout_seconds, legacy_fallback=ssh.legacy_host_keys,
            host_keys=HostKeyStore(Path(self.config.application.data_directory) / "reader_host_keys.json"),
            tolerance_ms=ts.tolerance_ms, max_attempts=ts.max_attempts,
            processing_estimate_ms=ts.processing_estimate_ms,
            host_clock_check=host_clock_status,
            require_host_clock_synchronized=ts.require_host_clock_synchronized,
        )

    async def _run_time_sync(self) -> TimeSyncResult:
        budget = self.rcfg.ssh.timeout_seconds * 2 + self.rcfg.time_sync.max_attempts * 3 + 5
        try:
            result = await asyncio.wait_for(self._time_syncer(), timeout=budget)
        except Exception as exc:  # the syncer handles its own errors; this is a last resort
            result = TimeSyncResult(success=False, host=self._host or "", started_at=iso_us(self._clock.now_us()) or "",
                                    error_kind="timeout" if isinstance(exc, TimeoutError) else "protocol",
                                    error=f"reader time sync failed: {exc!r}")
        self.time_sync = result.to_dict()
        return result

    async def _maybe_sync_time_before_connect(self) -> None:
        """Set the reader clock before the first LLRP connection after start-up.  Caller holds the lock."""
        if not self._time_sync_pending:
            return
        self.state = ReaderState.SYNCING_TIME
        result = await self._run_time_sync()
        # Retry on later connection attempts only if the reader was simply not reachable yet.
        if result.success or result.error_kind not in ("unreachable", "timeout"):
            self._time_sync_pending = False

    async def sync_time(self) -> dict[str, Any]:
        """Operator-requested reader clock set (refused while reading: it would shift timestamps mid-race)."""
        async with self._operation():
            if self.activity == ReaderActivity.TEST or self.state == ReaderState.INVENTORYING:
                raise ReaderStateError("stop reading (and any antenna test) before setting the reader clock; "
                                       "stepping the clock while reading would distort timestamps")
            result = await self._run_time_sync()
            if result.success:
                self._time_sync_pending = False
                self.clock_mapper.reset()  # re-learn the reader/host offset from new reports
            else:
                raise ReaderOperationError(f"reader time sync failed: {result.error}", self.time_sync)
        return self.time_sync or {}

    async def _establish(self) -> None:
        """Connect, discover and configure.  Caller holds the lock."""
        if self.rcfg.host_file:
            address = self._refresh_address()
            if address is not None:
                self._adopt_address(address)
            elif self._host is None:
                self.state = ReaderState.WAITING_FOR_ADDRESS
                raise ReaderUnavailableError(f"reader address not available yet: {self.address_problem}")
        await self._maybe_sync_time_before_connect()
        self.state = ReaderState.CONNECTING
        self._intentional_close = False
        conn = self._connection_factory(
            self._host,
            self.rcfg.port,
            self.rcfg.connection_timeout,
            self._on_message,
            self._parse_executor,
        )
        try:
            notification, received_us = await conn.open()
        except Exception as exc:
            self.state = ReaderState.DISCONNECTED
            self._record_error("connect", str(exc))
            raise ReaderUnavailableError(str(exc)) from None
        self._conn = conn
        kind, ts = parse_timestamp_param(notification)
        self.clock_mapper.on_connect(kind, ts, received_us)
        try:
            self.state = ReaderState.CONFIGURING
            await self._discover()
            await self._configure_reader()
            await self._restore_activity()
        except Exception as exc:
            msg = str(exc) if isinstance(exc, (LLRPError, ReaderOperationError)) else repr(exc)
            self._record_error("configure", msg)
            log.error("Reader initialisation failed: %s", msg)
            self._intentional_close = True
            await conn.close(graceful=True)
            self._conn = None
            self.state = ReaderState.DISCONNECTED
            if isinstance(exc, ReaderOperationError):
                raise
            raise ReaderConfigurationError(f"reader initialisation failed: {msg}") from None
        self.last_connected_at_us = self._clock.now_us()
        # LLRP is reachable, so SSH not answering is not a start-up race: stop retrying the sync.
        self._time_sync_pending = False
        if self.time_sync and self.time_sync.get("success") and self.clock_mapper.connect_offset_us is not None:
            # Independent check of the achieved offset from the LLRP connection timestamp
            # (includes the one-way network latency, typically 1-5 ms).
            self.time_sync["measured_offset_ms"] = round(-self.clock_mapper.connect_offset_us / 1000, 1)
            self.time_sync["measured_at"] = iso_us(self._clock.now_us())
        log.info(
            "Reader ready: %s %s (fw %s, serial %s) at %s:%d, mode=%s, state=%s",
            self.caps.manufacturer, self.caps.model_name, self.caps.firmware_version,
            self.caps.serial_number, self._host, self.rcfg.port, self.mode.value, self.state.value,
        )

    async def _discover(self) -> None:
        caps = empty_capabilities()
        resp = await self._transact_ok(
            P.GET_READER_CAPABILITIES_Message(RequestedData=P.GetReaderCapabilitiesRequestedData.All),
            "GET_READER_CAPABILITIES",
        )
        apply_capabilities_response(caps, resp)

        if self.rcfg.reset_to_factory_default:
            await self._transact_ok(P.SET_READER_CONFIG_Message(ResetToFactoryDefault=True), "factory reset")

        attempt_impinj = self.rcfg.impinj_extensions == "enabled" or (
            self.rcfg.impinj_extensions == "auto" and caps.manufacturer_pen == IMPINJ_PEN
        )
        if attempt_impinj:
            ok, detail = await self._transact_quiet(P.IMPINJ_ENABLE_EXTENSIONS_Message())
            caps.impinj.extensions_enabled = ok
            if ok:
                log.info("Impinj LLRP extensions enabled")
                try:
                    resp = await self._transact_ok(
                        P.GET_READER_CAPABILITIES_Message(
                            RequestedData=P.GetReaderCapabilitiesRequestedData.All,
                            Parameters=[
                                P.ImpinjRequestedData_Parameter(
                                    RequestedData=P.ImpinjRequestedDataType.All_Capabilities
                                )
                            ],
                        ),
                        "GET_READER_CAPABILITIES (Impinj)",
                    )
                    apply_impinj_capabilities(caps, resp)
                except LLRPError as exc:
                    caps.errors.append(f"Impinj capability query failed: {exc}")
                    caps.field_status["impinj_detailed_version"] = Availability.UNAVAILABLE
            else:
                caps.impinj.extensions_error = detail
                log.info("Impinj extensions not available: %s", detail)
        elif self.rcfg.impinj_extensions == "auto":
            caps.impinj.extensions_error = "not attempted: reader manufacturer is not Impinj"
        else:
            caps.impinj.extensions_error = "disabled by configuration"

        cfg_params = []
        if caps.impinj_extensions:
            cfg_params = [P.ImpinjRequestedData_Parameter(RequestedData=P.ImpinjRequestedDataType.All_Configuration)]
        try:
            resp = await self._transact_ok(
                P.GET_READER_CONFIG_Message(
                    AntennaID=0, RequestedData=P.GetReaderConfigRequestedData.All, Parameters=cfg_params
                ),
                "GET_READER_CONFIG",
            )
            apply_config_response(caps, resp)
        except LLRPError as exc:
            if cfg_params:
                try:
                    resp = await self._transact_ok(
                        P.GET_READER_CONFIG_Message(AntennaID=0, RequestedData=P.GetReaderConfigRequestedData.All),
                        "GET_READER_CONFIG",
                    )
                    apply_config_response(caps, resp)
                except LLRPError as exc2:
                    caps.errors.append(f"GET_READER_CONFIG failed: {exc2}")
                    caps.field_status["current_configuration"] = Availability.UNAVAILABLE
                    caps.field_status["connected_antennas"] = Availability.UNAVAILABLE
            else:
                caps.errors.append(f"GET_READER_CONFIG failed: {exc}")
                caps.field_status["current_configuration"] = Availability.UNAVAILABLE
                caps.field_status["connected_antennas"] = Availability.UNAVAILABLE
        finalize_capabilities(caps)
        caps.discovered_at = iso_us(self._clock.now_us())
        self.caps = caps

    async def _configure_reader(self) -> None:
        impinj = self.caps.impinj_extensions
        attempts = [(self.rcfg.keepalive_interval_ms, impinj)]
        if impinj:
            attempts.append((self.rcfg.keepalive_interval_ms, False))
        if self.rcfg.keepalive_interval_ms:
            attempts.append((0, False))
        last = ""
        for keepalive_ms, low_latency in attempts:
            ok, detail = await self._transact_quiet(build_reader_config(keepalive_ms, impinj, low_latency))
            if ok:
                self.keepalive_active = keepalive_ms > 0
                self.applied_configuration["reader_config"] = {
                    "keepalive_ms": keepalive_ms,
                    "impinj_low_latency_reports": low_latency and impinj,
                    "events": ["ROSpec", "ReportBufferFillWarning", "ReaderException", "Antenna"],
                }
                break
            last = detail
            log.warning("SET_READER_CONFIG rejected (%s); retrying with fewer options", detail)
        else:
            raise ReaderConfigurationError(f"reader rejected SET_READER_CONFIG: {last}")

        if self.rcfg.delete_existing_rospecs:
            await self._transact_quiet(P.DISABLE_ROSPEC_Message(ROSpecID=0))
            await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=0))
            await self._transact_quiet(P.DELETE_ACCESSSPEC_Message(AccessSpecID=0))

    async def _restore_activity(self) -> None:
        """After (re)connecting: install the correct ROSpec and resume desired activity."""
        if self.activity == ReaderActivity.TEST:
            await self._install(ROSPEC_ID_TEST, "test", self.rcfg.test)
            await self._enable(ROSPEC_ID_TEST)
            self.state = ReaderState.INVENTORYING
            return
        rid = rospec_id_for_mode(self.mode)
        await self._install(rid, self.mode.value, self._profile(self.mode))
        self.state = ReaderState.IDLE
        self.activity = ReaderActivity.NONE
        if self.desired_inventory:
            self.state = ReaderState.STARTING
            await self._enable(rid)
            self.state = ReaderState.INVENTORYING
            self.activity = ReaderActivity.TIMING
            log.info("Inventory resumed in %s mode", self.mode.value)

    def _profile(self, mode: OperatingMode) -> InventoryProfileConfig:
        return self.rcfg.start_line if mode == OperatingMode.START_LINE else self.rcfg.finish_line

    # ------------------------------------------------------------------ ROSpec ops

    async def _install(self, rospec_id: int, purpose: str, profile: InventoryProfileConfig) -> dict[str, Any]:
        plan = make_plan(rospec_id, purpose, profile, self.caps, self.rcfg.antennas)
        failures: list[dict[str, str]] = []
        tried: set[tuple] = set()
        for features in FEATURE_LEVELS:
            effective = (
                features.impinj_search_mode and plan.impinj,
                features.impinj_report_selector and plan.impinj,
                features.rf_control and plan.rf_mode.mode_id is not None,
                features.power_settings and (plan.tx_power_index is not None or plan.rx_sensitivity_index is not None),
                features.extended_report_fields,
            )
            if effective in tried:
                continue
            tried.add(effective)
            try:
                msg = build_add_rospec(plan, features)
            except AssertionError as exc:  # pyllrp field validation
                failures.append({"features": str(features.describe()), "error": f"invalid parameters: {exc}"})
                continue
            await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=rospec_id))
            ok, detail = await self._transact_quiet(msg)
            if ok:
                applied = plan.describe(features)
                applied["negotiation_failures"] = failures
                self.applied_configuration[purpose] = applied
                if failures:
                    log.warning("ROSpec %s installed after %d fallback(s): %s", purpose, len(failures), failures)
                else:
                    log.info("ROSpec %s installed: %s", purpose, applied)
                return applied
            failures.append({"features": str(features.describe()), "error": detail})
            if self._conn is None or self._conn.closed.is_set():
                break
        raise ReaderConfigurationError(f"reader rejected every {purpose} ROSpec configuration", failures)

    async def _rospec_state(self, rospec_id: int) -> str | None:
        """Return 'Disabled'/'Inactive'/'Active', or None if not verifiable."""
        try:
            resp = await self._transact_ok(P.GET_ROSPECS_Message(), "GET_ROSPECS")
        except LLRPError:
            return None
        for spec in resp.getAllParametersByClass(P.ROSpec_Parameter):
            if spec.ROSpecID == rospec_id:
                return P.ROSpecState.getName(spec.CurrentState)
        return "Missing"

    async def _enable(self, rospec_id: int) -> None:
        await self._transact_ok(P.ENABLE_ROSPEC_Message(ROSpecID=rospec_id), "ENABLE_ROSPEC")
        state = await self._rospec_state(rospec_id)
        if state is None:
            self.inventory_verified = None
            log.warning("Could not verify ROSpec %d state (GET_ROSPECS unsupported); trusting ENABLE response", rospec_id)
        elif state == "Active":
            self.inventory_verified = True
        else:
            self.inventory_verified = False
            raise ReaderConfigurationError(f"reader accepted ENABLE_ROSPEC but reports ROSpec state {state}")

    async def _disable(self, rospec_id: int) -> None:
        await self._transact_ok(P.DISABLE_ROSPEC_Message(ROSpecID=rospec_id), "DISABLE_ROSPEC")
        state = await self._rospec_state(rospec_id)
        if state not in (None, "Disabled", "Missing"):
            raise ReaderConfigurationError(f"reader accepted DISABLE_ROSPEC but reports ROSpec state {state}")

    async def _teardown(self, graceful: bool, stop_inventory: bool) -> None:
        conn = self._conn
        if conn is None:
            self.state = ReaderState.DISCONNECTED
            return
        self._intentional_close = True
        self.state = ReaderState.DISCONNECTING
        if graceful and stop_inventory and not conn.closed.is_set():
            await self._transact_quiet(P.DISABLE_ROSPEC_Message(ROSpecID=0))
            await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=0))
        await conn.close(graceful=graceful)
        self._conn = None
        self.last_disconnected_at_us = self._clock.now_us()
        self.state = ReaderState.DISCONNECTED
        if self.activity == ReaderActivity.TIMING:
            self.activity = ReaderActivity.NONE

    def _require_connected(self) -> None:
        if not self.connected:
            raise ReaderUnavailableError(
                f"reader is not connected (state: {self.state.value})",
                {"last_error": self.last_error},
            )

    # ------------------------------------------------------------------ public API

    async def connect(self) -> dict[str, Any]:
        self.desired_connected = True
        await self._save_state()
        async with self._operation():
            if self.connected:
                return self.status()
            try:
                await self._establish()
                self.reconnect_attempts = 0
            except ReaderOperationError:
                self._wake.set()  # supervisor keeps retrying in the background
                raise
        self._wake.set()
        return self.status()

    async def disconnect(self) -> dict[str, Any]:
        self.desired_connected = False
        await self._save_state()
        self._wake.set()
        async with self._operation():
            await self._teardown(graceful=True, stop_inventory=True)
            if self.activity == ReaderActivity.TEST:
                self.activity = ReaderActivity.NONE
        self.next_reconnect_at_us = None
        return self.status()

    async def reconnect(self) -> dict[str, Any]:
        self.desired_connected = True
        await self._save_state()
        async with self._operation():
            test_active = self.activity == ReaderActivity.TEST
            await self._teardown(graceful=True, stop_inventory=True)
            if test_active:
                self.activity = ReaderActivity.TEST  # resume test mode after reconnect
            try:
                await self._establish()
                self.reconnect_attempts = 0
            except ReaderOperationError:
                self._wake.set()
                raise
        self._wake.set()
        return self.status()

    async def start_inventory(self) -> dict[str, Any]:
        async with self._operation():
            if self.activity == ReaderActivity.TEST:
                raise ReaderStateError("antenna test mode is active; stop it first (POST /api/v1/test/stop)")
            self._require_connected()
            if self.state == ReaderState.INVENTORYING:
                return self.status()
            rid = rospec_id_for_mode(self.mode)
            self.state = ReaderState.STARTING
            try:
                await self._enable(rid)
            except (LLRPError, ReaderOperationError) as exc:
                self.state = ReaderState.IDLE if self.connected or self._conn else ReaderState.DISCONNECTED
                self._record_error("start", str(exc))
                if isinstance(exc, ReaderOperationError):
                    raise
                raise ReaderConfigurationError(f"failed to start inventory: {exc}") from None
            self.state = ReaderState.INVENTORYING
            self.activity = ReaderActivity.TIMING
            self.desired_inventory = True
        await self._save_state()
        log.info("Inventory started (%s)", self.mode.value)
        return self.status()

    async def stop_inventory(self) -> dict[str, Any]:
        async with self._operation():
            if self.activity == ReaderActivity.TEST:
                raise ReaderStateError("antenna test mode is active; use POST /api/v1/test/stop")
            self.desired_inventory = False
            if self.connected and self.state == ReaderState.INVENTORYING:
                self.state = ReaderState.STOPPING
                try:
                    await self._disable(rospec_id_for_mode(self.mode))
                except (LLRPError, ReaderOperationError) as exc:
                    self.state = ReaderState.INVENTORYING
                    self.desired_inventory = True
                    self._record_error("stop", str(exc))
                    raise ReaderConfigurationError(f"failed to stop inventory: {exc}") from None
                self.state = ReaderState.IDLE
            self.activity = ReaderActivity.NONE
        await self._save_state()
        log.info("Inventory stopped")
        return self.status()

    async def set_mode(self, mode: OperatingMode) -> dict[str, Any]:
        async with self._operation():
            if self.activity == ReaderActivity.TEST:
                raise ReaderStateError("cannot change mode while antenna test mode is active")
            if mode == self.mode:
                return {"mode": mode.value, "changed": False, "applied_to_reader": self.connected}
            old = self.mode
            if not self.connected:
                self.mode = mode
                await self._save_state()
                return {
                    "mode": mode.value,
                    "changed": True,
                    "applied_to_reader": False,
                    "note": "reader not connected; mode will be applied when the connection is established",
                }
            was_running = self.state == ReaderState.INVENTORYING
            old_id, new_id = rospec_id_for_mode(old), rospec_id_for_mode(mode)
            try:
                if was_running:
                    self.state = ReaderState.STOPPING
                    await self._disable(old_id)
                self.state = ReaderState.CONFIGURING
                # Many readers (e.g. Impinj) support only one ROSpec: remove the old first.
                await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=old_id))
                await self._install(new_id, mode.value, self._profile(mode))
                if was_running:
                    self.state = ReaderState.STARTING
                    await self._enable(new_id)
            except (LLRPError, ReaderOperationError) as exc:
                self._record_error("set_mode", f"failed to switch to {mode.value}: {exc}")
                log.error("Mode change to %s failed: %s; restoring %s", mode.value, exc, old.value)
                await self._restore_after_failed_mode_change(old, new_id, was_running)
                if isinstance(exc, ReaderOperationError):
                    exc.message = f"mode change to {mode.value} failed; still in {old.value}: {exc.message}"
                    raise
                raise ReaderConfigurationError(
                    f"mode change to {mode.value} failed; still in {old.value}: {exc}"
                ) from None
            self.mode = mode
            self.state = ReaderState.INVENTORYING if was_running else ReaderState.IDLE
            self.activity = ReaderActivity.TIMING if was_running else ReaderActivity.NONE
        await self._save_state()
        log.info("Operating mode changed %s -> %s", old.value, mode.value)
        return {"mode": mode.value, "changed": True, "applied_to_reader": True, "inventorying": was_running}

    async def _restore_after_failed_mode_change(self, old: OperatingMode, new_id: int, was_running: bool) -> None:
        if self._conn is None or self._conn.closed.is_set():
            return
        await self._transact_quiet(P.DISABLE_ROSPEC_Message(ROSpecID=new_id))
        await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=new_id))
        try:
            await self._install(rospec_id_for_mode(old), old.value, self._profile(old))
            if was_running:
                await self._enable(rospec_id_for_mode(old))
                self.state = ReaderState.INVENTORYING
            else:
                self.state = ReaderState.IDLE
        except Exception as exc:
            self.state = ReaderState.IDLE
            self.activity = ReaderActivity.NONE
            self._record_error("set_mode", f"failed to restore {old.value} configuration: {exc}")
            log.error("Could not restore previous mode configuration: %s", exc)

    async def start_test(self) -> dict[str, Any]:
        async with self._operation():
            if self.activity == ReaderActivity.TEST:
                return self.status()
            self._require_connected()
            was_running = self.state == ReaderState.INVENTORYING
            timing_id = rospec_id_for_mode(self.mode)
            try:
                if was_running:
                    self.state = ReaderState.STOPPING
                    await self._disable(timing_id)
                self.state = ReaderState.CONFIGURING
                await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=timing_id))
                await self._install(ROSPEC_ID_TEST, "test", self.rcfg.test)
                self.state = ReaderState.STARTING
                await self._enable(ROSPEC_ID_TEST)
            except (LLRPError, ReaderOperationError) as exc:
                self._record_error("test_start", str(exc))
                await self._transact_quiet(P.DISABLE_ROSPEC_Message(ROSpecID=ROSPEC_ID_TEST))
                await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=ROSPEC_ID_TEST))
                try:
                    await self._install(timing_id, self.mode.value, self._profile(self.mode))
                    if was_running:
                        await self._enable(timing_id)
                except Exception as exc2:
                    log.error("Failed to restore timing ROSpec after test-mode failure: %s", exc2)
                    was_running = False
                self.state = ReaderState.INVENTORYING if was_running else ReaderState.IDLE
                raise ReaderConfigurationError(f"failed to start antenna test mode: {exc}") from None
            self._test_resume_timing = was_running
            self.activity = ReaderActivity.TEST
            self.state = ReaderState.INVENTORYING
        log.info("Antenna test mode started (timing inventory was %s)", "running" if was_running else "stopped")
        return self.status()

    async def stop_test(self) -> dict[str, Any]:
        async with self._operation():
            if self.activity != ReaderActivity.TEST:
                raise ReaderStateError("antenna test mode is not active")
            resume = self._test_resume_timing or self.desired_inventory
            if self.connected:
                self.state = ReaderState.STOPPING
                ok, detail = await self._transact_quiet(P.DISABLE_ROSPEC_Message(ROSpecID=ROSPEC_ID_TEST))
                if not ok:
                    log.warning("DISABLE test ROSpec: %s", detail)
                await self._transact_quiet(P.DELETE_ROSPEC_Message(ROSpecID=ROSPEC_ID_TEST))
                self.activity = ReaderActivity.NONE
                timing_id = rospec_id_for_mode(self.mode)
                try:
                    self.state = ReaderState.CONFIGURING
                    await self._install(timing_id, self.mode.value, self._profile(self.mode))
                    self.state = ReaderState.IDLE
                    if resume:
                        self.state = ReaderState.STARTING
                        await self._enable(timing_id)
                        self.state = ReaderState.INVENTORYING
                        self.activity = ReaderActivity.TIMING
                except (LLRPError, ReaderOperationError) as exc:
                    self.state = ReaderState.IDLE
                    self._record_error("test_stop", str(exc))
                    raise ReaderConfigurationError(
                        f"test mode stopped but restoring {self.mode.value} failed: {exc}"
                    ) from None
            else:
                self.activity = ReaderActivity.NONE
        log.info("Antenna test mode stopped; timing inventory %s", "resumed" if resume else "idle")
        return self.status()

    # ------------------------------------------------------------------ messages

    def _route(self, rospec_id: int | None) -> tuple[ReportPurpose, OperatingMode | None] | None:
        if rospec_id == ROSPEC_ID_TEST:
            return ReportPurpose.TEST, None
        if rospec_id == ROSPEC_ID_START_LINE:
            return ReportPurpose.TIMING, OperatingMode.START_LINE
        if rospec_id == ROSPEC_ID_FINISH_LINE:
            return ReportPurpose.TIMING, OperatingMode.FINISH_LINE
        if rospec_id is None:
            if self.activity == ReaderActivity.TEST:
                return ReportPurpose.TEST, None
            return ReportPurpose.TIMING, self.mode
        return None

    def _on_message(self, msg: Any, received_us: int, received_mono: float) -> None:
        if isinstance(msg, P.RO_ACCESS_REPORT_Message):
            self.counters["reports"] += 1
            observations, skipped = normalize_report(msg, self.clock_mapper, received_us, received_mono)
            self.counters["observations"] += len(observations)
            self.counters["tag_entries_skipped"] += skipped
            groups: dict[int | None, list[Observation]] = defaultdict(list)
            for o in observations:
                groups[o.rospec_id].append(o)
            for rid, obs in groups.items():
                route = self._route(rid)
                if route is None:
                    self.counters["observations_unknown_rospec"] += len(obs)
                    continue
                try:
                    self._sink(obs, route[0], route[1])
                except Exception:
                    log.exception("Observation sink failed")
        elif isinstance(msg, P.READER_EVENT_NOTIFICATION_Message):
            for ev in parse_reader_event(msg):
                self._handle_reader_event(ev)
        elif isinstance(msg, P.KEEPALIVE_Message):
            self.counters["keepalives"] += 1
        else:
            self.counters["other_messages"] += 1
            log.debug("Unhandled reader message %s", type(msg).__name__)

    def _handle_reader_event(self, ev: dict[str, Any]) -> None:
        ev = {"at": iso_us(self._clock.now_us()), **ev}
        self.reader_events.append(ev)
        del self.reader_events[:-100]
        t = ev["type"]
        if t == "antenna":
            aid, connected = ev["antenna"], ev["connected"]
            log.warning("Antenna %d %s", aid, "connected" if connected else "DISCONNECTED")
            found = False
            for a in self.caps.antennas:
                if a.antenna_id == aid:
                    a.connected = connected
                    found = True
            if not found:
                from app.llrp.capabilities import AntennaInfo

                self.caps.antennas.append(AntennaInfo(antenna_id=aid, connected=connected))
            self.caps.connected_antennas = sorted(a.antenna_id for a in self.caps.antennas if a.connected)
        elif t == "reader_exception":
            self._record_error("reader", f"reader exception: {ev.get('message')}")
            log.error("Reader exception event: %s", ev.get("message"))
        elif t == "report_buffer_warning":
            log.warning("Reader report buffer %s%% full", ev.get("percent_full"))
        elif t == "report_buffer_overflow":
            self._record_error("reader", "reader report buffer overflow: tag reports may have been lost")
            log.error("Reader report buffer OVERFLOW - reports may have been lost")
        elif t == "connection_close":
            log.warning("Reader announced it is closing the connection")
        else:
            log.debug("Reader event: %s", ev)

    # ------------------------------------------------------------------ status

    def status(self) -> dict[str, Any]:
        caps = self.caps
        conn = self._conn
        return {
            "connection_state": self.state.value,
            "connected": self.connected,
            "host": self._host,
            "address_source": f"file:{self.rcfg.host_file}" if self.rcfg.host_file else "config",
            "address_problem": self.address_problem,
            "port": self.rcfg.port,
            "manufacturer": caps.manufacturer,
            "model": caps.model_name,
            "serial_number": caps.serial_number,
            "firmware_version": caps.firmware_version,
            "impinj_extensions": caps.impinj.extensions_enabled,
            "mode": self.mode.value,
            "activity": self.activity.value,
            "inventorying": self.state == ReaderState.INVENTORYING and self.activity == ReaderActivity.TIMING,
            "test_mode_active": self.activity == ReaderActivity.TEST,
            "inventory_state_verified": self.inventory_verified,
            "desired": {"connected": self.desired_connected, "inventorying": self.desired_inventory},
            "last_connected_at": iso_us(self.last_connected_at_us),
            "last_disconnected_at": iso_us(self.last_disconnected_at_us),
            "last_error": self.last_error,
            "reconnect_attempts": self.reconnect_attempts,
            "next_reconnect_at": iso_us(self.next_reconnect_at_us),
            "keepalive_active": self.keepalive_active,
            "seconds_since_last_message": round(conn.seconds_since_last_message(), 3) if conn else None,
            "clock": self.clock_mapper.status(),
            "time_sync": self.time_sync,
            "counters": dict(self.counters)
            | ({"malformed_messages": conn.malformed_messages} if conn else {}),
        }
