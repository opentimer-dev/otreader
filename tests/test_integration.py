"""Integration tests: full service stack + REST API against the simulated LLRP reader.

The reader simulator speaks real LLRP over TCP (encoded/decoded by pyllrp), so these
tests exercise the production transport, discovery, configuration negotiation,
report routing, timing, persistence and callbacks end to end.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
from contextlib import asynccontextmanager

import httpx
import pytest
from pyllrp import pyllrp as P

from app.llrp.configuration import ROSPEC_ID_FINISH_LINE, ROSPEC_ID_START_LINE, ROSPEC_ID_TEST
from app.llrp.simulator import SimulatedReader, SimulatorOptions
from app.main import create_app
from app.services import Services
from conftest import make_config, wait_for

EPC = "300833B2DDD9014000000001"


class Hook:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.bodies: list[dict] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        if self.fail:
            return httpx.Response(500)
        self.bodies.append(json.loads(request.content))
        return httpx.Response(200)


@asynccontextmanager
async def running(tmp_path, port, hook: Hook | None = None, **sections):
    config = make_config(tmp_path, port=port, **sections)
    hook = hook or Hook()
    svc = Services(config, http_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(hook)))
    await svc.start()
    app = create_app(config, svc)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    try:
        yield svc, client, hook
    finally:
        await client.aclose()
        await svc.stop()


async def connected(svc):
    await wait_for(lambda: svc.reader.connected and svc.reader.state.value in ("idle", "inventorying"), timeout=10)


def reader_now(sim):
    return sim.reader_now_us()


async def inject_pass(sim, epc=EPC, antenna=1, offset_us=0):
    peak = reader_now(sim) - 600_000 + offset_us
    reads = sim.make_pass(epc, peak, antenna=antenna)
    assert await sim.inject_reads(reads) == len(reads)
    return peak


async def events(client, **params):
    r = await client.get("/api/v1/events", params=params)
    assert r.status_code == 200, r.text
    return r.json()["events"]


# --------------------------------------------------------------------------- tests


async def test_connect_discovery_and_status(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        st = (await client.get("/api/v1/reader/status")).json()
        assert st["connected"] and st["connection_state"] == "idle"
        assert st["manufacturer"] == "Impinj" and st["serial_number"] == "SIM-0001"
        assert st["model"] == "Speedway R420 (simulated)" and st["firmware_version"] == "7.5.0.240"
        assert st["impinj_extensions"] is True and st["mode"] == "finish_line" and st["inventorying"] is False
        assert st["host"] == "127.0.0.1" and st["last_connected_at"].endswith("Z")
        caps = (await client.get("/api/v1/reader/capabilities")).json()
        assert caps["max_antennas"] == 4 and caps["connected_antennas"] == [1, 2]
        assert len(caps["transmit_power_levels"]) == 91 and caps["rf_modes"]
        assert caps["field_status"]["serial_number"] == "available"
        fin = caps["applied_configuration"]["finish_line"]
        assert fin["impinj_search_mode"] == "Dual_Target" and fin["session"] == 0 and fin["rf_mode_id"] == 0
        assert "IMPINJ_ENABLE_EXTENSIONS" in sim.received
        assert (await client.get("/ready")).status_code == 200


async def test_start_stop_verified_on_reader(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        r = await client.post("/api/v1/reader/start")
        assert r.status_code == 200 and r.json()["inventorying"] is True
        assert sim.rospec_state[ROSPEC_ID_FINISH_LINE] == P.ROSpecState.Active
        assert r.json()["inventory_state_verified"] is True
        r = await client.post("/api/v1/reader/stop")
        assert r.status_code == 200 and r.json()["inventorying"] is False
        assert sim.rospec_state[ROSPEC_ID_FINISH_LINE] == P.ROSpecState.Disabled


async def test_finish_line_end_to_end_with_callback_cooldown_and_removal(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, hook):
        await connected(svc)
        r = await client.put("/api/v1/callback", json={"url": "http://127.0.0.1:8123/hook"})
        assert r.status_code == 200 and r.json()["enabled"]
        await client.post("/api/v1/reader/start")
        peak = await inject_pass(sim)
        await wait_for(lambda: len(hook.bodies) == 1, timeout=5)
        body = hook.bodies[0]
        assert body["epc"] == EPC and body["mode"] == "finish_line"
        assert body["estimation_method"] == "rssi_quadratic_fit" and body["timestamp_source"] == "reader_utc"
        ev = (await client.get(f"/api/v1/events/{body['event_id']}")).json()
        assert ev["event_id"] == body["event_id"] and ev["replayed"] is False
        from app.clock import datetime_to_us
        from datetime import datetime

        t = datetime_to_us(datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00")))
        assert abs(t - peak) < 5_000  # noiseless pass: within 5 ms of the true peak
        obs = (await client.get(f"/api/v1/events/{body['event_id']}/observations")).json()["observations"]
        assert len(obs) == ev["observation_count"] > 10
        tag = (await client.get(f"/api/v1/tags/{EPC}")).json()
        assert tag["status"] == "cooldown" and tag["event_id"] == body["event_id"]

        # Tag lingers at the finish: suppressed by cooldown.
        await inject_pass(sim)
        await asyncio.sleep(0.8)
        assert len(await events(client)) == 1
        # Operator removes the tag: immediately eligible again.
        r = await client.delete(f"/api/v1/tags/{EPC.lower()}")
        assert r.status_code == 200
        assert (await client.get(f"/api/v1/tags/{EPC}")).status_code == 404
        await inject_pass(sim)
        await wait_for(lambda: len(hook.bodies) == 2, timeout=5)
        assert len(await events(client)) == 2
        q = (await client.get("/api/v1/callback/queue", params={"status": "delivered"})).json()
        assert q["total"] == 2


async def test_start_line_mode_via_api(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        await client.post("/api/v1/reader/start")
        r = await client.put("/api/v1/reader/mode", json={"mode": "start_line"})
        assert r.status_code == 200 and r.json() == {"mode": "start_line", "changed": True, "applied_to_reader": True,
                                                     "inventorying": True}
        assert sim.rospec_state == {ROSPEC_ID_START_LINE: P.ROSpecState.Active}
        spec = sim.rospecs[ROSPEC_ID_START_LINE]
        assert spec.getFirstParameterByClass(P.C1G2SingulationControl_Parameter).Session == 2
        assert spec.getFirstParameterByClass(P.ImpinjInventorySearchMode_Parameter).InventorySearchMode == 1
        # Dense start: many tags at once, one event each, reader timestamps preserved.
        now = reader_now(sim)
        from app.llrp.simulator import SimTagRead

        reads = [SimTagRead(epc=f"3008{i:020X}", antenna=1 + i % 2, rssi=-55, timestamp_us=now + i) for i in range(200)]
        reads += reads[:50]  # repeated reads of the same tags
        await sim.inject_reads(reads, per_report=10)
        await wait_for(lambda: svc.repo.event_count(), timeout=5)

        async def all_in():
            return await svc.repo.event_count() == 200

        await wait_for(all_in, timeout=10)
        evs = await events(client, limit=1, order_by="sequence")
        assert evs[0]["estimation_method"] == "first_observation" and evs[0]["mode"] == "start_line"
        assert svc.engine.counters["suppressed_cooldown"] == 50


async def test_mode_change_failure_keeps_previous_mode(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        await client.post("/api/v1/reader/start")
        sim.opts.reject_rospec_ids = {ROSPEC_ID_START_LINE}
        r = await client.put("/api/v1/reader/mode", json={"mode": "start_line"})
        assert r.status_code == 502
        assert r.json()["error"]["code"] == "reader_configuration_failed"
        assert "still in finish_line" in r.json()["error"]["message"]
        st = (await client.get("/api/v1/reader/status")).json()
        assert st["mode"] == "finish_line" and st["inventorying"] is True
        assert sim.rospec_state == {ROSPEC_ID_FINISH_LINE: P.ROSpecState.Active}


async def test_impinj_option_negotiation_fallback(tmp_path):
    sim = SimulatedReader(options=SimulatorOptions(reject_impinj_search_mode=True))
    await sim.start()
    try:
        async with running(tmp_path, sim.port) as (svc, client, _):
            await connected(svc)
            fin = (await client.get("/api/v1/reader/capabilities")).json()["applied_configuration"]["finish_line"]
            assert fin["impinj_search_mode"] is None
            assert fin["impinj_report_fields"] is not None  # other Impinj options retained
            assert "ImpinjInventorySearchMode" in fin["negotiation_failures"][0]["error"]
    finally:
        await sim.stop()


async def test_generic_reader_without_impinj_extensions(tmp_path, generic_sim):
    async with running(tmp_path, generic_sim.port) as (svc, client, _):
        await connected(svc)
        st = (await client.get("/api/v1/reader/status")).json()
        assert st["impinj_extensions"] is None and st["serial_number"] is None
        caps = (await client.get("/api/v1/reader/capabilities")).json()
        assert caps["impinj"]["extensions_error"].startswith("not attempted")
        assert "IMPINJ_ENABLE_EXTENSIONS" not in generic_sim.received
        assert not any(generic_sim._has_impinj(s) for s in generic_sim.rospecs.values())
        await client.post("/api/v1/reader/start")
        await inject_pass(generic_sim)
        await wait_for(svc.repo.event_count, timeout=5)
        ev = (await events(client))[0]
        assert ev["details"]["rssi_source"] == "llrp_peak_rssi"


async def test_forced_impinj_on_generic_reader_is_negotiated_away(tmp_path, generic_sim):
    async with running(tmp_path, generic_sim.port, reader={"impinj_extensions": "enabled"}) as (svc, client, _):
        await connected(svc)
        caps = (await client.get("/api/v1/reader/capabilities")).json()
        assert caps["impinj"]["extensions_enabled"] is False
        assert "UnsupportedMessage" in caps["impinj"]["extensions_error"]


async def test_reconnect_after_network_interruption(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        await client.post("/api/v1/reader/start")
        await inject_pass(sim, epc="3008000000000000000000AA")
        await wait_for(svc.repo.event_count, timeout=5)
        await sim.drop_connection()
        await wait_for(lambda: svc.reader.counters["unexpected_disconnects"] == 1, timeout=5)
        await wait_for(lambda: svc.reader.inventorying and sim.connections == 2, timeout=10)
        assert sim.rospec_state[ROSPEC_ID_FINISH_LINE] == P.ROSpecState.Active
        await inject_pass(sim, epc="3008000000000000000000BB")

        async def two():
            return await svc.repo.event_count() == 2

        await wait_for(two, timeout=5)  # history kept across reconnection
        st = (await client.get("/api/v1/reader/status")).json()
        assert "connection lost" in st["last_error"]["message"]


async def test_keepalive_watchdog_detects_silent_reader(tmp_path, sim):
    async with running(tmp_path, sim.port, reader={"keepalive_interval_ms": 150, "keepalive_missed_limit": 2}) as (
        svc, client, _
    ):
        await connected(svc)
        assert svc.reader.keepalive_active
        await asyncio.sleep(0.5)
        sim.silent = True  # reader stops sending anything (hung network path)
        await wait_for(lambda: svc.reader.counters["unexpected_disconnects"] >= 1, timeout=10)
        await wait_for(lambda: svc.reader.connected and sim.connections >= 2, timeout=10)


async def test_reader_unavailable_at_startup_then_appears(tmp_path):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    async with running(tmp_path, port) as (svc, client, _):
        await wait_for(lambda: svc.reader.reconnect_attempts >= 1, timeout=5)
        st = (await client.get("/api/v1/reader/status")).json()
        assert st["connected"] is False and st["connection_state"] in ("reconnect_wait", "disconnected", "connecting")
        r = await client.get("/ready")
        assert r.status_code == 503 and "reader not connected" in r.json()["reasons"][0]
        assert (await client.get("/health")).status_code == 200
        r = await client.post("/api/v1/reader/start")
        assert r.status_code == 503 and r.json()["error"]["code"] == "reader_unavailable"
        sim = SimulatedReader(port=port)
        await sim.start()
        try:
            await connected(svc)
            assert (await client.get("/ready")).status_code == 200
        finally:
            await sim.stop()


async def test_antenna_test_mode_isolated_from_timing(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, hook):
        await connected(svc)
        await client.put("/api/v1/callback", json={"url": "http://127.0.0.1:8123/hook"})
        await client.post("/api/v1/reader/start")
        r = await client.post("/api/v1/test/start")
        assert r.status_code == 200 and r.json()["test_mode_active"] is True
        assert sim.rospec_state == {ROSPEC_ID_TEST: P.ROSpecState.Active}
        assert (await client.put("/api/v1/reader/mode", json={"mode": "start_line"})).status_code == 409
        assert (await client.post("/api/v1/reader/start")).status_code == 409
        await inject_pass(sim, antenna=1)
        await inject_pass(sim, epc="3008000000000000000000CC", antenna=2)
        await asyncio.sleep(0.8)
        rep = (await client.get("/api/v1/test/reports", params={"limit": 5})).json()
        assert rep["test_mode_active"] and len(rep["reports"]) == 5
        first = rep["reports"][0]
        assert {"epc", "antenna", "rssi", "timestamp", "phase_angle_deg", "doppler_hz", "sequence"} <= set(first)
        assert [a["antenna"] for a in rep["summary"]["per_antenna"]] == [1, 2]
        page2 = (await client.get("/api/v1/test/reports", params={"after_sequence": rep["next_after_sequence"]})).json()
        assert page2["reports"][0]["sequence"] == rep["next_after_sequence"] + 1
        # No contamination of timing state.
        assert await svc.repo.event_count() == 0 and len(svc.inventory) == 0 and hook.bodies == []

        r = await client.post("/api/v1/test/stop")
        assert r.status_code == 200 and r.json()["reader"]["inventorying"] is True
        assert sim.rospec_state == {ROSPEC_ID_FINISH_LINE: P.ROSpecState.Active}
        assert await svc.repo.event_count() == 0
        await inject_pass(sim)
        await wait_for(lambda: len(hook.bodies) == 1, timeout=5)
        assert (await client.post("/api/v1/test/stop")).status_code == 409


async def test_restart_recovery(tmp_path, sim):
    failing = Hook(fail=True)
    cb = {"url": "http://127.0.0.1:8123/hook", "max_retries": 100, "retry_delay_seconds": 0.05}
    async with running(tmp_path, sim.port, hook=failing, callbacks=cb) as (svc, client, _):
        await connected(svc)
        await client.post("/api/v1/reader/start")
        await inject_pass(sim)
        await wait_for(svc.repo.event_count, timeout=5)
        await asyncio.sleep(0.3)
        cbs = (await client.get("/api/v1/callback")).json()
        assert cbs["pending"] == 1 and cbs["delivered"] == 0
        event_id = (await events(client))[0]["event_id"]
    assert sim.active_rospec() is None  # clean shutdown stopped the reader

    ok = Hook()
    async with running(tmp_path, sim.port, hook=ok, callbacks=cb) as (svc, client, _):
        # Inventory state, cooldown and history recovered; inventory resumed automatically.
        assert svc.recovery_report["inventory_records"] == 1
        assert (await client.get(f"/api/v1/tags/{EPC}")).json()["status"] == "cooldown"
        await wait_for(lambda: svc.reader.inventorying, timeout=10)
        # Pending callback from before the restart is delivered now.
        await wait_for(lambda: len(ok.bodies) == 1, timeout=5)
        assert ok.bodies[0]["event_id"] == event_id
        # The same tag is still in cooldown: no duplicate event after restart.
        await inject_pass(sim)
        await asyncio.sleep(0.8)
        assert len(await events(client)) == 1
        p = (await client.get("/api/v1/system/status")).json()["persistence"]
        assert p["healthy"] and p["last_successful_write"].endswith("Z")


async def test_concurrent_api_requests_are_serialised(tmp_path, sim):
    async with running(tmp_path, sim.port) as (svc, client, _):
        await connected(svc)
        reqs = []
        for i in range(6):
            reqs += [client.post("/api/v1/reader/start"), client.post("/api/v1/reader/stop"),
                     client.put("/api/v1/reader/mode", json={"mode": ["start_line", "finish_line"][i % 2]})]
        results = await asyncio.gather(*reqs)
        assert all(r.status_code == 200 for r in results), [r.text for r in results if r.status_code != 200]
        st = (await client.get("/api/v1/reader/status")).json()
        expected_id = ROSPEC_ID_START_LINE if st["mode"] == "start_line" else ROSPEC_ID_FINISH_LINE
        assert list(sim.rospecs) == [expected_id]
        want = P.ROSpecState.Active if st["inventorying"] else P.ROSpecState.Disabled
        assert sim.rospec_state[expected_id] == want


async def test_rewind_replay_and_export_api(tmp_path, sim):
    async with running(tmp_path, sim.port, application={"mode": "start_line"}) as (svc, client, _):
        await connected(svc)
        await client.post("/api/v1/reader/start")
        for i in range(5):
            await sim.inject_read(f"3008{i:020X}", rssi=-50)
        await wait_for(lambda: svc.engine.stats()["counters"].get("events_start_line") == 5)

        async def five():
            return await svc.repo.event_count() == 5

        await wait_for(five)
        evs = await events(client, order_by="sequence")
        r = await client.post("/api/v1/events/rewind", json={"event_id": evs[2]["event_id"]})
        assert r.status_code == 201
        cur = r.json()
        assert cur["start_sequence"] == 3 and cur["total_events"] == 3
        p1 = (await client.get(cur["next_url"], params={"limit": 2})).json()
        assert [e["sequence"] for e in p1["events"]] == [3, 4] and all(e["replayed"] for e in p1["events"])
        p2 = (await client.get(cur["next_url"])).json()
        assert [e["sequence"] for e in p2["events"]] == [5] and not p2["has_more"]
        r = await client.post("/api/v1/events/rewind", json={"timestamp": evs[3]["timestamp"]})
        assert r.json()["total_events"] == 2
        # Live polling is unaffected by replay cursors.
        live = await events(client, order_by="sequence", after_sequence=3)
        assert [e["sequence"] for e in live] == [4, 5] and not any(e["replayed"] for e in live)
        assert (await client.post("/api/v1/events/rewind", json={})).status_code == 422
        both = {"event_id": evs[0]["event_id"], "timestamp": evs[0]["timestamp"]}
        assert (await client.post("/api/v1/events/rewind", json=both)).status_code == 422
        assert (await client.post("/api/v1/events/rewind", json={"timestamp": "2026-01-01T00:00:00"})).status_code == 422
        nf = await client.post("/api/v1/events/rewind", json={"event_id": "missing"})
        assert nf.status_code == 404 and nf.json()["error"]["code"] == "not_found"
        csv_text = (await client.get("/api/v1/events/export", params={"format": "csv"})).text
        assert len(csv_text.strip().splitlines()) == 6
        js = (await client.get("/api/v1/events/export")).json()
        assert len(js) == 5


async def test_uptime_timestamps_and_unsynchronised_reader_clock(tmp_path):
    for opts, source in (
        (SimulatorOptions(uptime_timestamps=True), "reader_uptime_mapped"),
        (SimulatorOptions(clock_offset_us=-3_600_000_000), "reader_utc_corrected"),
    ):
        sim = SimulatedReader(options=opts)
        await sim.start()
        try:
            async with running(tmp_path / source, sim.port, application={"mode": "start_line"}) as (svc, client, _):
                await connected(svc)
                await client.post("/api/v1/reader/start")
                await sim.inject_read(EPC)
                await wait_for(svc.repo.event_count, timeout=5)
                ev = (await events(client))[0]
                assert ev["timestamp_source"] == source
                from datetime import datetime

                t = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00")).timestamp()
                assert abs(t - time.time()) < 5  # mapped onto host UTC
        finally:
            await sim.stop()


async def test_api_security_and_validation(tmp_path, sim):
    async with running(tmp_path, sim.port, api={"api_key": "k3y", "max_request_bytes": 2048}) as (svc, client, _):
        assert (await client.get("/health")).status_code == 200
        r = await client.get("/api/v1/reader/status")
        assert r.status_code == 401 and r.json()["error"]["code"] == "unauthorized"
        assert (await client.get("/api/v1/reader/status", headers={"X-API-Key": "k3y"})).status_code == 200
        assert (await client.get("/api/v1/reader/status", headers={"Authorization": "Bearer k3y"})).status_code == 200
        h = {"X-API-Key": "k3y"}
        big = await client.put("/api/v1/callback", headers=h, content=b"x" * 5000)
        assert big.status_code == 413
        assert (await client.delete("/api/v1/tags", headers=h)).status_code == 400
        assert (await client.delete("/api/v1/tags", headers=h, params={"confirm": "true"})).status_code == 200
        assert (await client.get("/api/v1/tags/not-hex!", headers=h)).status_code == 422
        bad = await client.put("/api/v1/callback", headers=h, json={"url": "http://169.254.169.254/"})
        assert bad.status_code == 422
        cfg = (await client.get("/api/v1/system/config", headers=h)).json()
        assert cfg["api"]["api_key"] == "***"
        spec = (await client.get("/openapi.json")).json()
        for path in ("/api/v1/reader/status", "/api/v1/events/rewind", "/api/v1/test/start", "/api/v1/callback/retry"):
            assert path in spec["paths"]


@pytest.mark.parametrize("mode", ["start_line", "finish_line"])
async def test_mode_set_while_disconnected_applies_on_connect(tmp_path, sim, mode):
    async with running(tmp_path, sim.port, reader={"auto_connect": False}) as (svc, client, _):
        other = "start_line" if mode == "finish_line" else "finish_line"
        r = await client.put("/api/v1/reader/mode", json={"mode": other})
        assert r.json()["applied_to_reader"] is False
        r = await client.post("/api/v1/reader/connect")
        assert r.status_code == 200 and r.json()["mode"] == other
        expected = ROSPEC_ID_START_LINE if other == "start_line" else ROSPEC_ID_FINISH_LINE
        assert list(sim.rospecs) == [expected]
        r = await client.post("/api/v1/reader/disconnect")
        assert r.json()["connected"] is False and not sim.client_connected


async def test_web_ui_served_at_root_without_auth(tmp_path, sim):
    async with running(tmp_path, sim.port, api={"api_key": "k3y"}) as (svc, client, _):
        r = await client.get("/")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
        page = r.text
        for element_id in ("reader-info", "btn-mode-start", "btn-mode-finish", "btn-start", "btn-stop",
                           "btn-test-start", "btn-test-stop", "meters", "antenna-rows"):
            assert f'id="{element_id}"' in page
        assert "https://" not in page  # self-contained: works on a race LAN without internet
        assert (await client.get("/api/v1/reader/status")).status_code == 401  # API still protected
        assert "/" not in (await client.get("/openapi.json")).json()["paths"]
