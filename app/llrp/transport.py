"""Asynchronous LLRP connection using pyllrp as the message codec.

Why not ``pyllrp.LLRPConnector``?  It is thread based and its ``transact()``
asserts that the listener thread is *not* running, so commands such as
DISABLE_ROSPEC cannot be issued while tag reports are being received without
first stopping the listener (which blocks for up to the socket timeout).  It also
uses the deprecated ``datetime.utcfromtimestamp``.  We therefore keep pyllrp for
what it does well - building, validating, packing and unpacking every LLRP and
Impinj message - and implement framing and request/response correlation on
asyncio streams:

* LLRP framing: 10-byte header (ver/type 16 bits, length 32, message id 32).
* Responses are matched by MessageID *and* by being a ``*_RESPONSE`` or
  ``ERROR_MESSAGE``, so reader-initiated messages (reports, events, keepalives)
  with coincidental IDs are never mistaken for responses.
* KEEPALIVE messages are acknowledged automatically.
* Parsing runs on a dedicated single worker thread (preserving order) so a burst
  of reports cannot stall the event loop that serves the API.
* Malformed messages are logged and skipped; framing stays intact because the
  length is taken from the header.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import bitstring
from pyllrp import pyllrp as P

log = logging.getLogger(__name__)

HEADER = struct.Struct(">HII")
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


class LLRPError(Exception):
    """Base class for LLRP communication errors."""


class LLRPConnectionError(LLRPError):
    pass


class LLRPTimeoutError(LLRPError):
    pass


class LLRPStatusError(LLRPError):
    """The reader answered with a non-success LLRPStatus or ERROR_MESSAGE."""

    def __init__(self, request: str, detail: str) -> None:
        super().__init__(f"{request} rejected by reader: {detail}")
        self.request = request
        self.detail = detail


MessageHandler = Callable[[Any, int, float], None]  # (message, host_received_us, host_received_mono)


def pack_message(message: Any) -> bytes:
    return message.pack(bitstring.BitStream()).tobytes()


def _normalize_bits_fields(obj: Any) -> None:
    """pyllrp limitation: unpacked ``bits:N`` fields (e.g. C1G2SingulationControl.Session)
    are left as ``bitstring`` objects instead of ints, which breaks re-validation and
    JSON serialisation.  Convert them to ints in place."""
    for f in getattr(obj, "FieldDefs", ()):
        if f.TypeCode.startswith("bits"):
            v = getattr(obj, f.Name, 0)
            if not isinstance(v, int):
                try:
                    setattr(obj, f.Name, v.uint)
                except Exception:
                    setattr(obj, f.Name, 0)
    for p in getattr(obj, "Parameters", ()):
        _normalize_bits_fields(p)


def unpack_message(data: bytes) -> Any:
    msg = P.UnpackMessage(bitstring.ConstBitStream(bytes=data))
    if not isinstance(msg, P.RO_ACCESS_REPORT_Message):  # hot path: tag reports carry no bits fields we use
        _normalize_bits_fields(msg)
    return msg


def _is_response(msg: Any) -> bool:
    name = type(msg).__name__
    return name.endswith("_RESPONSE_Message") or name == "ERROR_MESSAGE_Message"


class LLRPConnection:
    def __init__(
        self,
        host: str,
        port: int,
        timeout: float,
        on_message: MessageHandler,
        parse_executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._on_message = on_message
        self._own_executor = parse_executor is None
        self._executor = parse_executor or ThreadPoolExecutor(max_workers=1, thread_name_prefix="llrp-parse")
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._read_task: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self.closed = asyncio.Event()
        self.close_reason: str | None = None
        self._closing = False
        self.last_message_mono: float = time.monotonic()
        self.malformed_messages = 0
        self.messages_received = 0

    async def open(self) -> tuple[Any, int]:
        """Connect and wait for the reader's connection READER_EVENT_NOTIFICATION.

        Returns (notification message, host receive time in µs).
        """
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout
            )
        except TimeoutError:
            raise LLRPConnectionError(f"timed out connecting to {self.host}:{self.port}") from None
        except OSError as exc:
            raise LLRPConnectionError(f"cannot connect to {self.host}:{self.port}: {exc}") from None
        _configure_socket(self._writer)
        try:
            data = await asyncio.wait_for(self._read_frame(), timeout=self.timeout)
            received_us = time.time_ns() // 1000
            msg = await asyncio.get_running_loop().run_in_executor(self._executor, unpack_message, data)
        except TimeoutError:
            await self._abort("no connection notification from reader")
            raise LLRPConnectionError("reader did not send a connection notification") from None
        except Exception as exc:
            await self._abort(f"bad connection notification: {exc}")
            raise LLRPConnectionError(f"invalid connection notification from reader: {exc}") from None
        if not isinstance(msg, P.READER_EVENT_NOTIFICATION_Message):
            await self._abort("unexpected first message")
            raise LLRPConnectionError(f"expected READER_EVENT_NOTIFICATION, got {type(msg).__name__}")
        event = msg.getFirstParameterByClass(P.ConnectionAttemptEvent_Parameter)
        if event is not None and event.Status != P.ConnectionAttemptStatusType.Success:
            reason = P.ConnectionAttemptStatusType.getName(event.Status)
            await self._abort(reason)
            raise LLRPConnectionError(f"reader refused connection: {reason}")
        self.last_message_mono = time.monotonic()
        self._read_task = asyncio.create_task(self._read_loop(), name="llrp-read")
        return msg, received_us

    async def _read_frame(self) -> bytes:
        assert self._reader is not None
        header = await self._reader.readexactly(HEADER.size)
        _, length, _ = HEADER.unpack(header)
        if length < HEADER.size or length > MAX_MESSAGE_BYTES:
            raise LLRPConnectionError(f"invalid LLRP message length {length}")
        body = await self._reader.readexactly(length - HEADER.size)
        return header + body

    async def _read_loop(self) -> None:
        loop = asyncio.get_running_loop()
        reason = "connection closed by reader"
        try:
            while True:
                data = await self._read_frame()
                received_us = time.time_ns() // 1000
                received_mono = time.monotonic()
                self.last_message_mono = received_mono
                self.messages_received += 1
                type_code = HEADER.unpack_from(data)[0] & 0x3FF
                message_id = HEADER.unpack_from(data)[2]
                try:
                    msg = await loop.run_in_executor(self._executor, unpack_message, data)
                except Exception as exc:
                    self.malformed_messages += 1
                    log.warning(
                        "Skipping malformed/unsupported LLRP message type=%d id=%d len=%d: %r",
                        type_code, message_id, len(data), exc,
                    )
                    fut = self._pending.get(message_id)
                    if fut is not None and not fut.done() and type_code not in (61, 62, 63):
                        fut.set_exception(LLRPError(f"unparseable response (type {type_code}): {exc}"))
                    continue
                self._dispatch(msg, received_us, received_mono)
        except asyncio.IncompleteReadError:
            reason = "connection closed by reader"
        except asyncio.CancelledError:
            reason = "connection closed locally"
            raise
        except LLRPConnectionError as exc:
            reason = str(exc)
        except OSError as exc:
            reason = f"socket error: {exc}"
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("Unexpected error in LLRP read loop")
            reason = f"unexpected error: {exc!r}"
        finally:
            self._mark_closed(reason if not self._closing else "connection closed locally")

    def _dispatch(self, msg: Any, received_us: int, received_mono: float) -> None:
        if _is_response(msg):
            fut = self._pending.get(msg.MessageID)
            if fut is not None and not fut.done():
                fut.set_result(msg)
                return
            log.debug("Unsolicited response %s id=%s", type(msg).__name__, msg.MessageID)
            return
        if isinstance(msg, P.KEEPALIVE_Message):
            asyncio.create_task(self._send_quiet(P.KEEPALIVE_ACK_Message(MessageID=msg.MessageID)))
        try:
            self._on_message(msg, received_us, received_mono)
        except Exception:
            log.exception("Error in LLRP message handler for %s", type(msg).__name__)

    async def _send_quiet(self, msg: Any) -> None:
        try:
            await self.send(msg)
        except Exception as exc:
            log.debug("Failed to send %s: %s", type(msg).__name__, exc)

    async def send(self, msg: Any) -> None:
        if self._writer is None or self.closed.is_set():
            raise LLRPConnectionError("not connected")
        data = pack_message(msg)
        async with self._send_lock:
            self._writer.write(data)
            await self._writer.drain()

    async def transact(self, msg: Any, timeout: float | None = None) -> Any:
        """Send *msg* and wait for its response.  Raises on timeout/disconnect.

        The caller inspects LLRPStatus (see :func:`app.llrp.events.llrp_status`).
        ERROR_MESSAGE responses are returned too, for the caller to interpret.
        """
        if self.closed.is_set():
            raise LLRPConnectionError(self.close_reason or "not connected")
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        mid = msg.MessageID
        self._pending[mid] = fut
        closed_wait = asyncio.ensure_future(self.closed.wait())
        try:
            await self.send(msg)
            done, _ = await asyncio.wait(
                {fut, closed_wait}, timeout=timeout or self.timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if fut in done:
                return fut.result()
            if closed_wait in done:
                raise LLRPConnectionError(f"connection lost during {type(msg).__name__}: {self.close_reason}")
            raise LLRPTimeoutError(f"no response to {type(msg).__name__} within {timeout or self.timeout}s")
        finally:
            closed_wait.cancel()
            self._pending.pop(mid, None)
            if not fut.done():
                fut.cancel()

    def _mark_closed(self, reason: str) -> None:
        if self.closed.is_set():
            return
        self.close_reason = reason
        self.closed.set()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(LLRPConnectionError(reason))
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        if self._own_executor:
            self._executor.shutdown(wait=False)

    async def _abort(self, reason: str) -> None:
        self._closing = True
        self._mark_closed(reason)
        await self._wait_writer_closed()

    async def close(self, graceful: bool = True) -> None:
        """Close the connection (sends CLOSE_CONNECTION first when graceful)."""
        if self.closed.is_set():
            await self._wait_writer_closed()
            return
        self._closing = True
        if graceful:
            try:
                await self.transact(P.CLOSE_CONNECTION_Message(), timeout=min(self.timeout, 3.0))
            except Exception as exc:
                log.debug("CLOSE_CONNECTION failed: %s", exc)
        if self._read_task is not None:
            self._read_task.cancel()
            try:
                await self._read_task
            except (asyncio.CancelledError, Exception):
                pass
        self._mark_closed("connection closed locally")
        await self._wait_writer_closed()

    async def _wait_writer_closed(self) -> None:
        if self._writer is not None:
            try:
                await asyncio.wait_for(self._writer.wait_closed(), timeout=2.0)
            except Exception:
                pass

    def seconds_since_last_message(self) -> float:
        return time.monotonic() - self.last_message_mono


def _configure_socket(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Linux-specific TCP keepalive tuning; detects dead peers when LLRP keepalives are unavailable.
        for opt, val in (("TCP_KEEPIDLE", 10), ("TCP_KEEPINTVL", 5), ("TCP_KEEPCNT", 3)):
            if hasattr(socket, opt):
                sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt), val)
    except OSError:
        pass
