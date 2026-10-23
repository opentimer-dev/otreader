"""REST endpoints (mounted under /api/v1, health endpoints at the root)."""

from __future__ import annotations

import hmac
import uuid
from pathlib import Path
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from app.callbacks.url_validation import CallbackURLError
from app.clock import datetime_to_us, iso_us
from app.config import OperatingMode
from app.llrp.events import normalize_epc
from app.persistence.repository import NotFoundError
from app.services import Services

from . import schemas as S

ERR = {
    404: {"model": S.ErrorResponse},
    409: {"model": S.ErrorResponse},
    422: {"model": S.ErrorResponse},
    502: {"model": S.ErrorResponse},
    503: {"model": S.ErrorResponse},
}


def get_services(request: Request) -> Services:
    return request.app.state.services


Svc = Annotated[Services, Depends(get_services)]


def require_api_key(request: Request) -> None:
    key = request.app.state.services.config.api.api_key
    if not key:
        return
    supplied = request.headers.get("x-api-key")
    auth = request.headers.get("authorization", "")
    if not supplied and auth.lower().startswith("bearer "):
        supplied = auth[7:].strip()
    if not supplied or not hmac.compare_digest(supplied.encode(), key.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid API key",
                            headers={"WWW-Authenticate": "Bearer"})


def _epc(epc: str) -> str:
    try:
        return normalize_epc(epc)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


def _ts(dt: datetime | None, name: str) -> int | None:
    if dt is None:
        return None
    try:
        return datetime_to_us(dt)
    except ValueError:
        raise HTTPException(422, f"{name} must include a timezone") from None


health_router = APIRouter(tags=["health"])
web_router = APIRouter(include_in_schema=False)

_INDEX_HTML = (Path(__file__).resolve().parent.parent / "web" / "index.html").read_text(encoding="utf-8")


@web_router.get("/", response_class=HTMLResponse)
async def web_ui() -> HTMLResponse:
    """Operator web page.  Served without authentication (it contains no data); the page
    itself calls the API and prompts for the API key when one is configured."""
    return HTMLResponse(_INDEX_HTML, headers={"Cache-Control": "no-cache"})
router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


# ======================================================================= health


@health_router.get("/health", summary="Liveness check")
async def health(svc: Svc) -> dict[str, Any]:
    """Always 200 while the process is serving requests; reports component status."""
    p = svc.persistence_status()
    return {
        "status": "ok" if p["healthy"] else "degraded",
        "started_at": iso_us(svc.started_at_us),
        "reader": {"state": svc.reader.state.value, "connected": svc.reader.connected},
        "persistence": {"healthy": p["healthy"], "last_successful_write": p["last_successful_write"],
                        "last_error": p["last_error"]},
    }


@health_router.get("/ready", summary="Readiness check", responses={503: {"description": "Not ready"}})
async def ready(svc: Svc) -> JSONResponse:
    """200 when persistence is healthy and the reader is connected; otherwise 503 with reasons."""
    ok, reasons = svc.readiness()
    return JSONResponse({"ready": ok, "reasons": reasons}, status_code=200 if ok else 503)


@router.get("/system/status", tags=["health"], summary="Detailed operational status")
async def system_status(svc: Svc) -> dict[str, Any]:
    return {
        "reader": svc.reader.status(),
        "timing": svc.engine.stats() | {"inventory": svc.inventory.stats()},
        "persistence": svc.persistence_status(),
        "callbacks": await svc.dispatcher.status(),
        "test_mode": {"active": svc.reader.status()["test_mode_active"],
                      "buffered_reports": svc.test_buffer.last_sequence},
    }


@router.get("/system/config", tags=["health"], summary="Effective configuration (secrets masked)")
async def system_config(svc: Svc) -> dict[str, Any]:
    return svc.config.sanitized()


# ======================================================================= reader


@router.get("/reader/status", tags=["reader"], response_model=S.ReaderStatusOut, summary="Reader connection status")
async def reader_status(svc: Svc) -> dict[str, Any]:
    """Connection state (including transitional states), reader identity, mode, inventory state,
    last connection time, last error, reconnect progress, clock offset and counters."""
    return svc.reader.status()


@router.get("/reader/capabilities", tags=["reader"], summary="Discovered reader capabilities")
async def reader_capabilities(svc: Svc) -> dict[str, Any]:
    """Antenna count and connection state, transmit power table, receive sensitivities, regulatory
    information, RF mode table, LLRP capabilities and Impinj extension details.  ``field_status``
    distinguishes *available*, *unsupported*, *unavailable* (query failed) and *not_queried*."""
    caps = svc.reader.caps.model_dump(mode="json")
    caps["applied_configuration"] = svc.reader.applied_configuration
    return caps


@router.post("/reader/connect", tags=["reader"], response_model=S.ReaderStatusOut, responses=ERR,
             summary="Connect to the reader")
async def reader_connect(svc: Svc) -> dict[str, Any]:
    """Connects, discovers capabilities and installs the current mode's configuration.  Returns
    once initialisation has completed.  On failure (503/502) the application keeps retrying in the
    background with exponential backoff."""
    return await svc.reader.connect()


@router.post("/reader/reconnect", tags=["reader"], response_model=S.ReaderStatusOut, responses=ERR,
             summary="Disconnect and reconnect")
async def reader_reconnect(svc: Svc) -> dict[str, Any]:
    """Stops inventory, disconnects, reconnects and re-applies the configuration.  Inventory is
    resumed if it was running."""
    return await svc.reader.reconnect()


@router.post("/reader/disconnect", tags=["reader"], response_model=S.ReaderStatusOut, responses=ERR,
             summary="Stop inventory and disconnect")
async def reader_disconnect(svc: Svc) -> dict[str, Any]:
    """Disables automatic reconnection until the next connect request."""
    return await svc.reader.disconnect()


@router.post("/reader/start", tags=["reader"], response_model=S.ReaderStatusOut, responses=ERR,
             summary="Start inventory")
async def reader_start(svc: Svc) -> dict[str, Any]:
    """Enables the ROSpec for the current mode and verifies (GET_ROSPECS) that it is active."""
    return await svc.reader.start_inventory()


@router.post("/reader/stop", tags=["reader"], response_model=S.ReaderStatusOut, responses=ERR,
             summary="Stop inventory")
async def reader_stop(svc: Svc) -> dict[str, Any]:
    """Stops inventory.  The tag inventory and event history are kept."""
    return await svc.reader.stop_inventory()


@router.post("/reader/sync-time", tags=["reader"], responses=ERR, summary="Set the reader clock now")
async def reader_sync_time(svc: Svc) -> dict[str, Any]:
    """Sets the reader's UTC clock from this host's clock over SSH (Impinj RShell
    ``config system time``), timed to the second boundary.  This also happens automatically when
    the application starts.  Refused (409) while reading or during an antenna test, because
    stepping the reader clock mid-race would distort timestamps.  Returns the attempts and the
    estimated remaining offset (reader − host, ms)."""
    return await svc.reader.sync_time()


@router.put("/reader/mode", tags=["reader"], responses=ERR, summary="Change operating mode")
async def reader_mode(body: S.ModeRequest, svc: Svc) -> dict[str, Any]:
    """Stops inventory, installs the new mode's reader configuration and resumes inventory if it
    was running.  On failure the previous mode is restored and 502 is returned; the response never
    reports the new mode as active unless the reader accepted it.  If the reader is not connected
    the mode is stored and applied on connection (``applied_to_reader: false``)."""
    return await svc.reader.set_mode(body.mode)


# ======================================================================= tags


@router.get("/tags", tags=["tags"], response_model=S.TagPage, summary="Current tag inventory")
async def list_tags(
    svc: Svc,
    epc: Annotated[str | None, Query(description="Case-insensitive substring match on EPC")] = None,
    antenna: int | None = None,
    seen_after: Annotated[datetime | None, Query(description="Last seen at or after (ISO 8601 with timezone)")] = None,
    seen_before: Annotated[datetime | None, Query(description="First seen at or before")] = None,
    status_filter: Annotated[
        Literal["tracking", "cooldown", "eligible", "eligible_after_cooldown"] | None, Query(alias="status")
    ] = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    total, tags = svc.inventory.query(
        epc=epc, antenna=antenna, seen_after_us=_ts(seen_after, "seen_after"),
        seen_before_us=_ts(seen_before, "seen_before"), status=status_filter, offset=offset, limit=limit,
    )
    return {"total": total, "offset": offset, "limit": limit, "tags": tags}


@router.get("/tags/{epc}", tags=["tags"], response_model=S.TagOut, responses=ERR, summary="One tag record")
async def get_tag(epc: str, svc: Svc) -> dict[str, Any]:
    rec = svc.inventory.get(_epc(epc))
    if rec is None:
        raise NotFoundError(f"tag {epc} not in inventory")
    return svc.inventory.view(rec)


@router.delete("/tags/{epc}", tags=["tags"], response_model=S.OperationResult, responses=ERR,
               summary="Remove a tag (makes it eligible immediately)")
async def delete_tag(epc: str, svc: Svc) -> dict[str, Any]:
    """Removes the tag from the active inventory, clears its cooldown and any open finish-line pass.
    The next observation can create a new event.  Historical events are retained."""
    e = _epc(epc)
    existed = await svc.remove_tag(e)
    if not existed:
        raise NotFoundError(f"tag {e} not in inventory")
    return {"ok": True, "message": f"tag {e} removed; eligible for detection", "details": {"epc": e}}


@router.delete("/tags", tags=["tags"], response_model=S.OperationResult, responses=ERR,
               summary="Clear the inventory and all cooldowns")
async def clear_tags(
    svc: Svc, confirm: Annotated[bool, Query(description="Must be true (safety interlock)")] = False
) -> dict[str, Any]:
    """Administrative: clears the active inventory and cooldown state.  Historical events are kept.
    Requires ``?confirm=true``."""
    if not confirm:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "add ?confirm=true to clear the entire inventory")
    n = await svc.clear_inventory()
    return {"ok": True, "message": f"cleared {n} tag(s) and all cooldowns", "details": {"cleared": n}}


# ======================================================================= events


def _event_filters(
    epc: str | None, antenna: int | None, mode: OperatingMode | None, start_time: datetime | None,
    end_time: datetime | None,
) -> dict[str, Any]:
    return {
        "epc": _epc(epc) if epc else None,
        "antenna": antenna,
        "mode": mode.value if mode else None,
        "start_us": _ts(start_time, "start_time"),
        "end_us": _ts(end_time, "end_time"),
    }


@router.get("/events", tags=["events"], response_model=S.EventPage, summary="Processed timing events")
async def list_events(
    svc: Svc,
    epc: str | None = None,
    antenna: int | None = None,
    mode: OperatingMode | None = None,
    start_time: Annotated[datetime | None, Query(description="timestamp >= start_time (inclusive)")] = None,
    end_time: Annotated[datetime | None, Query(description="timestamp < end_time (exclusive)")] = None,
    after_sequence: Annotated[int | None, Query(ge=0, description="Only events with sequence > this (live polling)")] = None,
    order_by: Literal["timestamp", "sequence"] = "timestamp",
    order: Literal["asc", "desc"] = "asc",
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    """Filter by EPC, antenna, mode and time range; order by crossing timestamp or by sequence.
    For live consumption poll with ``order_by=sequence&after_sequence=<last seen>``."""
    f = _event_filters(epc, antenna, mode, start_time, end_time)
    total, events = await svc.repo.query_events(
        **f, after_seq=after_sequence, order_by=order_by, descending=order == "desc", limit=limit, offset=offset
    )
    return {
        "total": total, "offset": offset, "limit": limit, "events": events,
        "last_sequence": max((e["sequence"] for e in events), default=None),
    }


@router.get("/events/export", tags=["events"], summary="Export event history (JSON or CSV)",
            response_class=StreamingResponse)
async def export_events(
    svc: Svc,
    format: Literal["json", "csv"] = "json",
    epc: str | None = None,
    antenna: int | None = None,
    mode: OperatingMode | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> StreamingResponse:
    """Streams all matching events in sequence order."""
    f = _event_filters(epc, antenna, mode, start_time, end_time)
    media = "application/json" if format == "json" else "text/csv"
    name = f"otreader-events.{format}"
    return StreamingResponse(
        svc.repo.iter_export(format, **f), media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


@router.post("/events/rewind", tags=["events"], response_model=S.ReplaySession, status_code=201, responses=ERR,
             summary="Create a replay cursor")
async def rewind(body: S.RewindRequest, svc: Svc, request: Request) -> dict[str, Any]:
    """Creates an independent replay cursor; the event history and other clients are unaffected.

    * ``event_id``: replay starts **with** that event (inclusive) and continues with every later event
      in original (sequence) order.
    * ``timestamp``: replays every event whose ``timestamp`` is **>=** the given instant (inclusive),
      in original (sequence) order.

    The replay covers events up to ``end_sequence`` (the log head at rewind time).  Afterwards
    continue with live polling: ``GET /api/v1/events?order_by=sequence&after_sequence=<end_sequence>``.
    Replayed events carry ``"replayed": true``."""
    cursor = uuid.uuid4().hex
    try:
        s = await svc.repo.create_replay(
            cursor, body.event_id, _ts(body.timestamp, "timestamp"), body.ttl_seconds
        )
    except NotFoundError:
        raise NotFoundError(f"event {body.event_id} not found") from None
    return _session_out(s, request)


def _session_out(s: dict[str, Any], request: Request) -> dict[str, Any]:
    return {
        "cursor_id": s["cursor_id"],
        "start_sequence": s["start_sequence"],
        "end_sequence": s["end_sequence"],
        "position": s["position"],
        "total_events": s["total_events"],
        "from_event_id": s["from_event_id"],
        "from_timestamp": iso_us(s["from_timestamp_us"]),
        "expires_at": iso_us(s["expires_us"]),
        "semantics": (
            "event_id: replay starts with that event (inclusive); timestamp: events with timestamp >= value "
            "(inclusive). Events are returned in original sequence order up to end_sequence."
        ),
        "next_url": f"/api/v1/events/replay/{s['cursor_id']}",
        "live_continuation": f"/api/v1/events?order_by=sequence&after_sequence={s['end_sequence']}",
    }


@router.get("/events/replay/{cursor_id}", tags=["events"], response_model=S.ReplayPage, responses=ERR,
            summary="Fetch the next page of a replay")
async def replay_page(
    cursor_id: str,
    svc: Svc,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    position: Annotated[int | None, Query(ge=0, description="Re-read from this sequence position (exclusive)")] = None,
) -> dict[str, Any]:
    """Returns the next page and advances the cursor.  Pass ``position`` (a previous page's
    ``position``) to re-read after a lost response."""
    try:
        p = await svc.repo.replay_page(cursor_id, limit, position)
    except NotFoundError:
        raise NotFoundError(f"replay cursor {cursor_id} not found or expired") from None
    for e in p["events"]:
        e["replayed"] = True
    return {
        "cursor_id": cursor_id, "events": p["events"], "position": p["position"], "has_more": p["has_more"],
        "end_sequence": p["end_sequence"], "total_events": p["total_events"],
        "live_continuation": f"/api/v1/events?order_by=sequence&after_sequence={p['end_sequence']}",
    }


@router.delete("/events/replay/{cursor_id}", tags=["events"], response_model=S.OperationResult, responses=ERR,
               summary="Delete a replay cursor")
async def delete_replay(cursor_id: str, svc: Svc) -> dict[str, Any]:
    if not await svc.repo.delete_replay(cursor_id):
        raise NotFoundError(f"replay cursor {cursor_id} not found")
    return {"ok": True, "message": "replay cursor deleted"}


@router.get("/events/{event_id}", tags=["events"], response_model=S.EventOut, responses=ERR,
            summary="One event")
async def get_event(event_id: str, svc: Svc) -> dict[str, Any]:
    try:
        return await svc.repo.get_event(event_id)
    except NotFoundError:
        raise NotFoundError(f"event {event_id} not found") from None


@router.get("/events/{event_id}/observations", tags=["events"], responses=ERR,
            summary="Raw observations behind an event (diagnostics / calibration)")
async def get_event_observations(event_id: str, svc: Svc) -> dict[str, Any]:
    """Compact keys: t=timestamp µs UTC, a=antenna, r=RSSI dBm, rs=RSSI source, n=seen count,
    ph=phase degrees, d=Doppler Hz, ch=channel index, src=timestamp source, fs/ls=first/last seen µs,
    hr=host receive µs."""
    try:
        obs = await svc.repo.get_event_observations(event_id)
    except NotFoundError:
        raise NotFoundError(f"event {event_id} not found") from None
    return {"event_id": event_id, "observations": obs}


# ======================================================================= callbacks


@router.get("/callback", tags=["callbacks"], summary="Callback configuration and delivery status")
async def get_callback(svc: Svc) -> dict[str, Any]:
    return await svc.dispatcher.status()


@router.put("/callback", tags=["callbacks"], responses=ERR, summary="Set the callback URL")
async def put_callback(body: S.CallbackURLRequest, svc: Svc) -> dict[str, Any]:
    """Validates and stores the URL (persisted across restarts).  Every subsequently accepted event is
    POSTed to it.  Pending callbacks are delivered to the new URL."""
    try:
        await svc.set_callback_url(body.url)
    except CallbackURLError as exc:
        raise HTTPException(422, f"invalid callback URL: {exc}") from None
    return await svc.dispatcher.status()


@router.delete("/callback", tags=["callbacks"], summary="Disable callbacks")
async def delete_callback(svc: Svc) -> dict[str, Any]:
    """Disables delivery without affecting RFID processing.  Events accepted while disabled are
    still recorded in the event log (retrieve them via /events or rewind) but are not queued for
    callback delivery.  Already-pending callbacks remain pending until callbacks are re-enabled."""
    await svc.disable_callbacks()
    return await svc.dispatcher.status()


@router.get("/callback/queue", tags=["callbacks"], response_model=S.CallbackQueuePage,
            summary="Pending / failed callbacks")
async def callback_queue(
    svc: Svc,
    status_filter: Annotated[Literal["pending", "failed", "delivered", "undelivered", "all"], Query(alias="status")] = "undelivered",
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    statuses = {
        "undelivered": ["pending", "failed"], "all": ["pending", "failed", "delivered"],
    }.get(status_filter, [status_filter])
    total, rows = await svc.repo.callback_queue(statuses, limit, offset)
    items = [
        {
            "event_id": r["event_id"], "sequence": r["seq"], "epc": r["epc"], "status": r["status"],
            "attempts": r["attempts"],
            "next_attempt_at": iso_us(r["next_attempt_us"]) if r["status"] == "pending" else None,
            "last_attempt_at": iso_us(r["last_attempt_us"]), "delivered_at": iso_us(r["delivered_us"]),
            "last_error": r["last_error"], "last_status_code": r["last_status_code"],
        }
        for r in rows
    ]
    return {"total": total, "offset": offset, "limit": limit, "items": items}


@router.post("/callback/retry", tags=["callbacks"], response_model=S.OperationResult, summary="Retry callbacks")
async def callback_retry(svc: Svc, body: S.CallbackRetryRequest | None = None) -> dict[str, Any]:
    """Resets failed callbacks (and optionally pending ones) to be retried immediately, optionally
    limited to specific event IDs."""
    body = body or S.CallbackRetryRequest()
    n = await svc.dispatcher.retry(body.event_ids, body.include_pending)
    msg = f"{n} callback(s) scheduled for immediate retry"
    if not svc.dispatcher.enabled:
        msg += " (callbacks are currently disabled; they will be sent when re-enabled)"
    return {"ok": True, "message": msg, "details": {"scheduled": n}}


# ======================================================================= test mode


@router.post("/test/start", tags=["test mode"], responses=ERR, summary="Start antenna test mode")
async def test_start(svc: Svc) -> dict[str, Any]:
    """Stops timing inventory (remembering whether it was running), installs a separate test ROSpec
    and starts it.  Test reads are routed by ROSpecID to a bounded buffer only: they never enter the
    inventory, never create events, never apply cooldown and never trigger callbacks."""
    if not svc.reader.status()["test_mode_active"]:
        svc.test_buffer.reset(svc.clock.now_us())
    status_ = await svc.reader.start_test()
    return {"test_mode_active": True, "reader": status_}


@router.get("/test/reports", tags=["test mode"], summary="Recent raw test reports")
async def test_reports(
    svc: Svc,
    after_sequence: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 100,
) -> dict[str, Any]:
    """Raw observations (EPC, antenna, RSSI, reader timestamp, phase, Doppler, channel...) plus a
    per-antenna summary.  Poll with ``after_sequence=<next_after_sequence>``."""
    return {"test_mode_active": svc.reader.status()["test_mode_active"],
            **svc.test_buffer.list(after_sequence, limit)}


@router.post("/test/stop", tags=["test mode"], responses=ERR, summary="Stop antenna test mode")
async def test_stop(svc: Svc) -> dict[str, Any]:
    """Removes the test ROSpec, re-installs the timing configuration for the selected mode and resumes
    timing inventory if it was running before the test."""
    status_ = await svc.reader.stop_test()
    return {"test_mode_active": False, "reader": status_}
