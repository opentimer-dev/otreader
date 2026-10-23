"""Hardware integration tests - require a physical LLRP reader.

Not part of the default run.  Execute with:

    OTREADER_HARDWARE_HOST=192.168.8.100 pytest -m hardware -s tests/hardware

Optional: OTREADER_HARDWARE_PORT (default 5084), OTREADER_HARDWARE_SECONDS (read time
per mode, default 5).  Place a few tags near the antennas.  The tests print what the
reader reported and accepted so vendor-specific behaviour can be verified and
recorded; they stop and delete their ROSpecs on exit.
"""

from __future__ import annotations

import asyncio
import json
import os

import pytest

from app.config import OperatingMode, parse_config
from app.llrp.capabilities import IMPINJ_PEN
from app.llrp.client import ReaderManager
from app.models import ReportPurpose

HOST = os.environ.get("OTREADER_HARDWARE_HOST")
PORT = int(os.environ.get("OTREADER_HARDWARE_PORT", "5084"))
SECONDS = float(os.environ.get("OTREADER_HARDWARE_SECONDS", "5"))

pytestmark = [
    pytest.mark.hardware,
    pytest.mark.skipif(not HOST, reason="set OTREADER_HARDWARE_HOST to run hardware tests"),
]


@pytest.fixture
async def manager():
    cfg = parse_config({"reader": {"host": HOST, "port": PORT, "auto_connect": False}})
    seen: list = []
    mgr = ReaderManager(cfg, lambda obs, purpose, mode: seen.extend((o, purpose, mode) for o in obs))
    mgr.seen = seen
    await mgr.start()
    await mgr.connect()
    yield mgr
    await mgr.shutdown()


async def test_discovery(manager):
    caps = manager.caps
    print(json.dumps(caps.model_dump(mode="json"), indent=2)[:6000])
    assert caps.manufacturer and caps.max_antennas and caps.max_antennas > 0
    assert caps.transmit_power_levels, "reader advertised no transmit power table"
    if caps.manufacturer_pen == IMPINJ_PEN:
        assert caps.impinj.extensions_enabled is not None
    print("applied configuration:", json.dumps(manager.applied_configuration, indent=2))


@pytest.mark.parametrize("mode", [OperatingMode.START_LINE, OperatingMode.FINISH_LINE])
async def test_mode_inventory(manager, mode):
    await manager.set_mode(mode)
    manager.seen.clear()
    await manager.start_inventory()
    assert manager.inventory_verified in (True, None)
    await asyncio.sleep(SECONDS)
    await manager.stop_inventory()
    applied = manager.applied_configuration[mode.value]
    print(f"{mode.value}: applied={json.dumps(applied)}")
    obs = [o for o, purpose, _ in manager.seen if purpose == ReportPurpose.TIMING]
    print(f"{mode.value}: {len(obs)} observations, {len({o.epc for o in obs})} unique tags")
    if obs:
        o = obs[0]
        print("sample:", o.to_dict())
        print("timestamp sources:", {str(x.timestamp_source) for x in obs})
        print("rssi sources:", {x.rssi_source for x in obs})
    print("clock:", manager.clock_mapper.status())


async def test_antenna_test_mode(manager):
    manager.seen.clear()
    await manager.start_test()
    await asyncio.sleep(SECONDS)
    await manager.stop_test()
    purposes = {p for _, p, _ in manager.seen}
    print(f"test mode: {len(manager.seen)} observations; purposes={purposes}")
    # Timing inventory was not running before the test, so every report must be routed to test.
    assert purposes <= {ReportPurpose.TEST}


async def test_reader_clock_offset(manager):
    manager.seen.clear()
    await manager.start_inventory()
    await asyncio.sleep(2)
    await manager.stop_inventory()
    st = manager.clock_mapper.status()
    print("reader clock:", st)
    if st["reader_minus_host_ms"] is not None and abs(st["reader_minus_host_ms"]) > 1000:
        pytest.fail(f"reader clock differs from host by {st['reader_minus_host_ms']} ms - configure NTP")


async def test_reader_time_sync_over_ssh(tmp_path):
    """Sets the reader clock over SSH/RShell and checks it with the LLRP connection timestamp.
    Credentials: OTREADER_HARDWARE_SSH_USER / OTREADER_HARDWARE_SSH_PASSWORD (default root / impinj)."""
    from app.clock import host_clock_status
    from app.llrp.events import parse_timestamp_param
    from app.llrp.rshell import HostKeyStore, sync_reader_time
    from app.llrp.transport import LLRPConnection

    res = await sync_reader_time(
        HOST, username=os.environ.get("OTREADER_HARDWARE_SSH_USER", "root"),
        password=os.environ.get("OTREADER_HARDWARE_SSH_PASSWORD", "impinj"),
        host_keys=HostKeyStore(tmp_path / "keys.json"), host_clock_check=host_clock_status,
    )
    print("time sync:", json.dumps(res.to_dict(), indent=2))
    assert res.success, res.error
    offsets = []
    for _ in range(3):
        conn = LLRPConnection(HOST, PORT, 5, lambda *a: None)
        msg, received_us = await conn.open()
        offsets.append((parse_timestamp_param(msg)[1] - received_us) / 1000)
        await conn.close()
    print("LLRP-measured reader - host (ms):", offsets)
    assert abs(max(offsets)) < 60  # tolerance + reader clock wander + network latency
