"""Application service container.

Builds and wires every component; tests inject a manual clock, a connection
factory or an HTTP client factory here.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from app.callbacks.dispatcher import CallbackDispatcher
from app.callbacks.url_validation import CallbackURLError, redact_url
from app.clock import Clock, SystemClock, iso_us
from app.config import AppConfig
from app.llrp.client import ReaderManager
from app.llrp.transport import LLRPConnection
from app.models import TimingEvent
from app.persistence.recovery import prepare_database, recover_state
from app.persistence.repository import Repository
from app.timing.cooldown import CooldownTracker
from app.timing.inventory import Inventory
from app.timing.processor import TimingEngine
from app.timing.test_mode import TestReportBuffer

log = logging.getLogger(__name__)

DB_FILENAME = "otreader.db"
RUNTIME_KEY = "runtime_state"
SNAPSHOT_MAX_AGE_S = 60.0  # write an unchanged snapshot at least this often


class Services:
    def __init__(
        self,
        config: AppConfig,
        *,
        clock: Clock | None = None,
        connection_factory: Any = LLRPConnection,
        http_client_factory: Any = None,
    ) -> None:
        self.config = config
        self.clock = clock or SystemClock()
        self.data_dir = Path(config.application.data_directory)
        self.db_path = self.data_dir / DB_FILENAME
        self.repo = Repository(self.db_path, self.clock.now_us)
        self.cooldown = CooldownTracker(config.timing.cooldown_seconds, self.clock)
        self.inventory = Inventory(self.cooldown, self.clock)
        self.test_buffer = TestReportBuffer(config.test_mode.buffer_size)
        self.dispatcher = CallbackDispatcher(config.callbacks, self.repo, self.clock, http_client_factory)
        self.engine = TimingEngine(
            config.timing, self.inventory, self.clock, self._commit, self._on_committed, self.test_buffer
        )
        self.reader = ReaderManager(
            config, self.engine.handle, clock=self.clock, persist_state=self._persist_reader_state,
            connection_factory=connection_factory,
        )
        self.runtime: dict[str, Any] = {}
        self.database_report: dict[str, Any] = {}
        self.recovery_report: dict[str, Any] = {}
        self.started_at_us: int | None = None
        self.started = False
        self._snapshot_task: asyncio.Task[None] | None = None
        self._last_snapshot_version = -1
        self._last_snapshot_mono = 0.0
        self.snapshot_error: str | None = None

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.database_report = await asyncio.to_thread(prepare_database, self.db_path)
        await self.repo.open()
        self.recovery_report = await recover_state(
            self.repo, self.inventory, self.config.timing.cooldown_seconds, self.clock.now_us()
        )
        self.runtime = await self.repo.get_meta(RUNTIME_KEY, {}) or {}

        url = self.runtime["callback_url"] if "callback_url" in self.runtime else self.config.callbacks.url
        await self.dispatcher.start(None)
        if url:
            try:
                await self.dispatcher.set_url(url)
            except CallbackURLError as exc:
                log.error("Configured callback URL %s rejected: %s; callbacks disabled", redact_url(url), exc)

        await self.engine.start()
        self._last_snapshot_version = self.inventory.version
        self._last_snapshot_mono = self.clock.monotonic()
        self._snapshot_task = asyncio.create_task(self._snapshot_loop(), name="inventory-snapshot")
        await self.reader.start(restored=self.runtime.get("reader"))
        self.started_at_us = self.clock.now_us()
        self.started = True
        log.info("OTReader started (data directory %s)", self.data_dir.resolve())

    async def stop(self) -> None:
        log.info("Shutting down...")
        await self.reader.shutdown()  # stop tag ingestion first
        await self.engine.stop()  # finalise open passes, drain commits
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self.snapshot(force=True)
        except Exception as exc:
            log.error("Final inventory snapshot failed: %s", exc)
        await self.dispatcher.stop()
        await self.repo.close()
        self.started = False
        log.info("Shutdown complete")

    # ---------------------------------------------------------------- wiring

    async def _commit(self, event: TimingEvent, observations: list[dict[str, Any]], row: dict[str, Any] | None) -> int | None:
        return await self.repo.insert_event(event, observations, row, create_callback=self.dispatcher.enabled)

    def _on_committed(self, event: TimingEvent) -> None:
        self.dispatcher.notify(event.event_id)

    async def _persist_runtime(self) -> None:
        await self.repo.set_meta(RUNTIME_KEY, self.runtime)

    async def _persist_reader_state(self, state: dict[str, Any]) -> None:
        self.runtime["reader"] = state
        await self._persist_runtime()

    # ---------------------------------------------------------------- snapshots

    async def snapshot(self, force: bool = False) -> bool:
        version = self.inventory.version
        age = self.clock.monotonic() - self._last_snapshot_mono
        if not force and version == self._last_snapshot_version and age < SNAPSHOT_MAX_AGE_S:
            return False
        rows = self.inventory.snapshot(self.engine.uncommitted_epcs)  # serialised on the loop, written in order
        await self.repo.save_inventory_snapshot(rows)
        self._last_snapshot_version = version
        self._last_snapshot_mono = self.clock.monotonic()
        return True

    async def _snapshot_loop(self) -> None:
        interval = self.config.application.persistence_interval
        while True:
            await asyncio.sleep(interval)
            try:
                await self.snapshot()
                self.snapshot_error = None
            except Exception as exc:
                self.snapshot_error = f"{type(exc).__name__}: {exc}"
                log.error("Inventory snapshot failed: %s", exc)

    # ---------------------------------------------------------------- operations

    async def set_callback_url(self, url: str) -> str:
        validated = await self.dispatcher.set_url(url)
        self.runtime["callback_url"] = validated
        await self._persist_runtime()
        return validated

    async def disable_callbacks(self) -> None:
        self.dispatcher.disable()
        self.runtime["callback_url"] = None
        await self._persist_runtime()

    async def remove_tag(self, epc: str) -> bool:
        existed = self.engine.remove_tag(epc)
        await self.repo.remove_inventory_tag(epc)
        return existed

    async def clear_inventory(self) -> int:
        n = self.engine.clear()
        await self.repo.clear_inventory()
        return n

    # ---------------------------------------------------------------- health

    def persistence_status(self) -> dict[str, Any]:
        h = self.repo.health()
        return {
            "healthy": h["healthy"] and self.snapshot_error is None and self.engine.last_commit_error is None,
            "database": h["path"],
            "size_bytes": h["size_bytes"],
            "last_successful_write": iso_us(h["last_write_us"]),
            "last_inventory_snapshot": iso_us(h["last_snapshot_us"]),
            "snapshot_interval_seconds": self.config.application.persistence_interval,
            "last_error": h["last_error"] or self.snapshot_error or self.engine.last_commit_error,
            "write_failures": h["write_failures"],
            "uncommitted_events": self.engine.stats()["uncommitted_events"],
            "startup_database_check": self.database_report,
            "startup_recovery": self.recovery_report,
        }

    def readiness(self) -> tuple[bool, list[str]]:
        reasons = []
        if not self.started:
            reasons.append("application is starting")
        if not self.persistence_status()["healthy"]:
            reasons.append("persistence is failing")
        if not self.reader.connected:
            reasons.append(f"reader not connected (state: {self.reader.state.value})")
        return not reasons, reasons
