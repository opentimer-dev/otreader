"""Asynchronous callback dispatcher.

Delivery model (at-least-once):

* When an event is committed, its callback row (status ``pending``) is written
  in the same SQLite transaction.  The durable table *is* the queue; nothing is
  lost if the process stops or the endpoint is down.
* An in-memory ``asyncio.Queue`` bounded by ``queue_max_size`` holds event IDs
  ready to send.  New events are offered with ``put_nowait``; if the queue is
  full they simply stay ``pending`` in SQLite and a refill loop loads them later.
  Tag ingestion therefore never blocks and memory stays bounded however long the
  endpoint is unavailable (backpressure = spill to disk).
* Workers POST the event JSON.  2xx = ``delivered``.  Otherwise the attempt count
  increases and the row is rescheduled with exponential backoff
  (``retry_delay_seconds * 2^(attempt-1)``, capped at ``retry_max_delay_seconds``).
  After ``1 + max_retries`` attempts the row becomes ``failed`` and waits for an
  operator retry (``POST /api/v1/callback/retry``).
* Receivers must de-duplicate on ``event_id`` (a crash between a successful POST
  and recording it causes one re-delivery).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import Callable
from typing import Any

import httpx

from app.callbacks.url_validation import redact_url, validate_callback_url
from app.clock import Clock, iso_us
from app.config import CallbackConfig
from app.persistence.repository import NotFoundError, Repository

log = logging.getLogger(__name__)

USER_AGENT = "OTReader/1.0"


class CallbackDispatcher:
    def __init__(
        self,
        config: CallbackConfig,
        repo: Repository,
        clock: Clock,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        self.cfg = config
        self._repo = repo
        self._clock = clock
        self._client_factory = client_factory or (
            lambda: httpx.AsyncClient(timeout=config.timeout_seconds, follow_redirects=False,
                                      headers={"User-Agent": USER_AGENT})
        )
        self._client: httpx.AsyncClient | None = None
        self.url: str | None = None
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=config.queue_max_size)
        self._queued: set[str] = set()  # in queue or being delivered
        self._refill = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self.last_success_us: int | None = None
        self.last_failure: dict[str, Any] | None = None
        self.delivered = 0
        self.failed_attempts = 0
        self.overflowed = 0

    @property
    def enabled(self) -> bool:
        return self.url is not None

    async def start(self, url: str | None) -> None:
        self.url = url
        self._client = self._client_factory()
        self._tasks = [asyncio.create_task(self._refill_loop(), name="callback-refill")]
        self._tasks += [
            asyncio.create_task(self._worker(i), name=f"callback-worker-{i}") for i in range(self.cfg.concurrency)
        ]
        self._refill.set()

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def set_url(self, url: str) -> str:
        validated = await validate_callback_url(url, self.cfg)
        self.url = validated
        self._refill.set()
        log.info("Callback URL set to %s", redact_url(validated))
        return validated

    def disable(self) -> None:
        self.url = None
        log.info("Callbacks disabled")

    def notify(self, event_id: str) -> None:
        """Offer a freshly committed event; never blocks."""
        if not self.enabled:
            return
        if event_id in self._queued:
            return
        try:
            self._queue.put_nowait(event_id)
            self._queued.add(event_id)
        except asyncio.QueueFull:
            self.overflowed += 1  # stays pending in SQLite; refill loop picks it up

    def kick(self) -> None:
        self._refill.set()

    async def _refill_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._refill.wait(), timeout=1.0)
            except TimeoutError:
                pass
            self._refill.clear()
            if not self.enabled:
                continue
            space = self._queue.maxsize - self._queue.qsize()
            if space <= 0:
                continue
            try:
                ids = await self._repo.due_callbacks(self._clock.now_us(), space, set(self._queued))
            except Exception as exc:
                log.warning("Callback refill failed: %s", exc)
                continue
            for eid in ids:
                if eid in self._queued:
                    continue
                try:
                    self._queue.put_nowait(eid)
                    self._queued.add(eid)
                except asyncio.QueueFull:
                    break

    async def _worker(self, n: int) -> None:
        while True:
            eid = await self._queue.get()
            try:
                if not self.enabled:
                    continue  # remains pending; resent after re-enable
                await self._deliver(eid)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Callback worker error for %s", eid)
            finally:
                self._queued.discard(eid)
                self._queue.task_done()
                if self._queue.empty():
                    self._refill.set()  # pull spilled backlog immediately

    def _signature(self, body: bytes) -> str | None:
        if not self.cfg.secret:
            return None
        return "sha256=" + hmac.new(self.cfg.secret.encode(), body, hashlib.sha256).hexdigest()

    async def _deliver(self, event_id: str) -> None:
        try:
            payload, state = await self._repo.callback_payload(event_id)
        except NotFoundError:
            return
        if state["status"] != "pending":
            return
        url = self.url
        assert url is not None and self._client is not None
        payload["details"] = payload.get("details") or {}
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json", "X-OTReader-Event-Id": event_id}
        sig = self._signature(body)
        if sig:
            headers["X-OTReader-Signature"] = sig
        attempts = state["attempts"] + 1
        now = self._clock.now_us()
        error: str | None = None
        status_code: int | None = None
        try:
            async with self._client.stream("POST", url, content=body, headers=headers,
                                           timeout=self.cfg.timeout_seconds) as resp:
                status_code = resp.status_code
                snippet = b""
                async for chunk in resp.aiter_bytes():
                    snippet += chunk
                    if len(snippet) >= self.cfg.max_response_bytes:
                        break  # never read unbounded responses
                if not 200 <= resp.status_code < 300:
                    error = f"HTTP {resp.status_code}: {snippet[:200].decode(errors='replace')}"
        except httpx.TimeoutException:
            error = f"timeout after {self.cfg.timeout_seconds}s"
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"

        if error is None:
            await self._repo.update_callback(
                event_id, status="delivered", attempts=attempts, last_attempt_us=now, delivered_us=now,
                last_error=None, last_status_code=status_code,
            )
            self.delivered += 1
            self.last_success_us = now
            return

        self.failed_attempts += 1
        self.last_failure = {"at": iso_us(now), "event_id": event_id, "error": error}
        if attempts > self.cfg.max_retries:
            await self._repo.update_callback(
                event_id, status="failed", attempts=attempts, last_attempt_us=now, last_error=error,
                last_status_code=status_code,
            )
            log.error("Callback for event %s failed permanently after %d attempts: %s", event_id, attempts, error)
        else:
            delay = min(self.cfg.retry_delay_seconds * (2 ** (attempts - 1)), self.cfg.retry_max_delay_seconds)
            await self._repo.update_callback(
                event_id, status="pending", attempts=attempts, last_attempt_us=now,
                next_attempt_us=now + int(delay * 1e6), last_error=error, last_status_code=status_code,
            )
            log.warning("Callback for event %s failed (attempt %d): %s; retry in %.1fs", event_id, attempts, error, delay)

    async def retry(self, event_ids: list[str] | None, include_pending: bool) -> int:
        n = await self._repo.retry_callbacks(event_ids, include_pending, self._clock.now_us())
        self._refill.set()
        return n

    async def status(self) -> dict[str, Any]:
        counts = await self._repo.callback_counts()
        return {
            "enabled": self.enabled,
            "url": redact_url(self.url) if self.url else None,
            "pending": counts.get("pending", 0),
            "failed": counts.get("failed", 0),
            "delivered": counts.get("delivered", 0),
            "in_memory_queue": self._queue.qsize(),
            "in_memory_queue_capacity": self._queue.maxsize,
            "spilled_to_disk": self.overflowed,
            "last_success_at": iso_us(self.last_success_us),
            "last_failure": self.last_failure,
            "settings": {
                "timeout_seconds": self.cfg.timeout_seconds,
                "max_retries": self.cfg.max_retries,
                "retry_delay_seconds": self.cfg.retry_delay_seconds,
                "retry_max_delay_seconds": self.cfg.retry_max_delay_seconds,
                "signed": bool(self.cfg.secret),
            },
        }
