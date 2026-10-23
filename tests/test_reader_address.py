"""Reader address read from reader.host_file (default /var/run/reader)."""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from app.llrp.client import ReaderManager, read_reader_address
from app.llrp.configuration import ROSPEC_ID_FINISH_LINE
from app.llrp.rshell import TimeSyncResult
from app.llrp.simulator import SimulatedReader
from app.main import create_app
from app.services import Services
from conftest import make_config, wait_for
from pyllrp import pyllrp as P


@pytest.mark.parametrize(
    "content, expected, problem",
    [
        (None, None, "does not exist yet"),
        ("", None, "is empty"),
        ("   \n", None, "is empty"),
        ("reader.local\n", None, "does not contain an IP address"),
        ("192.168.8.23 junk", None, "does not contain an IP address"),
        ("999.1.1.1", None, "does not contain an IP address"),
        ("192.168.8.23\n", "192.168.8.23", None),
        ("  192.168.8.23  ", "192.168.8.23", None),
        ("fe80::1\n", "fe80::1", None),
    ],
)
def test_read_reader_address(tmp_path, content, expected, problem):
    path = tmp_path / "reader"
    if content is not None:
        path.write_text(content)
    address, reason = read_reader_address(str(path))
    assert address == expected
    assert (reason is None) if problem is None else (problem in reason)


def cfg_for(tmp_path, port, **reader):
    return make_config(tmp_path, port=port, reader={"host": None, "host_file": str(tmp_path / "reader"),
                                                    "host_file_poll_seconds": 0.2, **reader})


async def test_waits_for_valid_address_then_connects(tmp_path, sim, caplog):
    caplog.set_level(logging.WARNING, logger="app.llrp.client")
    mgr = ReaderManager(cfg_for(tmp_path, sim.port), lambda *a: None)
    await mgr.start()
    await wait_for(lambda: mgr.state.value == "waiting_for_address")
    await asyncio.sleep(1.0)  # several polls
    st = mgr.status()
    assert st["host"] is None and "does not exist yet" in st["address_problem"]
    assert st["address_source"].startswith("file:")
    assert sim.connections == 0 and mgr.reconnect_attempts == 0  # waiting is not a failed attempt
    warnings = [r for r in caplog.records if "Reader address not available" in r.getMessage()]
    assert len(warnings) == 1  # logged once, not every poll

    (tmp_path / "reader").write_text("not-an-ip\n")
    await wait_for(lambda: "does not contain an IP address" in (mgr.address_problem or ""))
    assert mgr.state.value == "waiting_for_address" and sim.connections == 0

    (tmp_path / "reader").write_text("127.0.0.1\n")
    await wait_for(lambda: mgr.state.value == "idle", 5)
    assert mgr.status()["host"] == "127.0.0.1" and mgr.address_problem is None
    await mgr.shutdown()


async def test_missing_file_does_not_drop_working_connection(tmp_path, sim):
    (tmp_path / "reader").write_text("127.0.0.1")
    mgr = ReaderManager(cfg_for(tmp_path, sim.port), lambda *a: None)
    await mgr.start()
    await wait_for(lambda: mgr.state.value == "idle", 5)
    (tmp_path / "reader").unlink()
    await asyncio.sleep(1.5)  # several polls (and monitor ticks)
    assert mgr.connected and sim.connections == 1 and mgr.host == "127.0.0.1"
    assert "does not exist yet" in mgr.address_problem
    await mgr.shutdown()


async def test_address_change_moves_to_new_reader_and_resumes(tmp_path, sim):
    second = SimulatedReader(host="127.0.0.2", port=sim.port)
    await second.start()
    synced_hosts = []

    async def syncer():
        synced_hosts.append(mgr.host)
        return TimeSyncResult(success=True, host=mgr.host, started_at="x", attempts=[{"command": "c"}])

    try:
        (tmp_path / "reader").write_text("127.0.0.1\n")
        mgr = ReaderManager(cfg_for(tmp_path, sim.port, time_sync={"enabled": True}), lambda *a: None,
                            time_syncer=syncer)
        await mgr.start()
        await wait_for(lambda: mgr.state.value == "idle", 5)
        await mgr.start_inventory()
        assert sim.rospec_state[ROSPEC_ID_FINISH_LINE] == P.ROSpecState.Active

        (tmp_path / "reader").write_text("127.0.0.2\n")
        await wait_for(lambda: mgr.host == "127.0.0.2" and mgr.inventorying, 10)
        assert second.connections == 1 and second.rospec_state[ROSPEC_ID_FINISH_LINE] == P.ROSpecState.Active
        assert not sim.client_connected  # left the old reader cleanly
        assert synced_hosts == ["127.0.0.1", "127.0.0.2"]  # new reader's clock set too
        await mgr.shutdown()
    finally:
        await second.stop()


async def test_reconnect_rereads_file(tmp_path, sim):
    """Reader reboots onto a new address: the connection drops and the next attempt uses the file."""
    second = SimulatedReader(host="127.0.0.2", port=sim.port)
    await second.start()
    try:
        (tmp_path / "reader").write_text("127.0.0.1")
        mgr = ReaderManager(cfg_for(tmp_path, sim.port), lambda *a: None)
        await mgr.start()
        await wait_for(lambda: mgr.state.value == "idle", 5)
        await sim.stop()  # old address gone
        (tmp_path / "reader").write_text("127.0.0.2")
        await wait_for(lambda: mgr.host == "127.0.0.2" and mgr.state.value == "idle", 10)
        await mgr.shutdown()
    finally:
        await second.stop()


async def test_api_while_waiting_for_address(tmp_path, sim):
    config = cfg_for(tmp_path, sim.port)
    svc = Services(config)
    await svc.start()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(config, svc)), base_url="http://t")
    try:
        await wait_for(lambda: svc.reader.state.value == "waiting_for_address")
        r = await client.post("/api/v1/reader/connect")
        assert r.status_code == 503 and "reader address not available yet" in r.json()["error"]["message"]
        st = (await client.get("/api/v1/reader/status")).json()
        assert st["connection_state"] == "waiting_for_address" and st["host"] is None
        r = await client.get("/ready")
        assert r.status_code == 503 and "waiting_for_address" in r.json()["reasons"][0]
        (tmp_path / "reader").write_text("127.0.0.1")
        await wait_for(lambda: svc.reader.state.value == "idle", 5)
        assert (await client.get("/ready")).status_code == 200
    finally:
        await client.aclose()
        await svc.stop()
