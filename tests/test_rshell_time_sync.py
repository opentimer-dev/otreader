"""Reader clock setting over SSH/RShell, against an in-process SSH server emulating Impinj RShell."""

from __future__ import annotations

import asyncio
import calendar
import re
import socket
import time

import asyncssh
import httpx
import pytest

from app.llrp.client import ReaderManager
from app.llrp.rshell import HostKeyStore, TimeSyncResult, _legacy_hosts, format_rshell_time, sync_reader_time
from app.main import create_app
from app.services import Services
from conftest import make_config, wait_for

_TIME_RE = re.compile(r"^config system time (\d{4})\.(\d{2})\.(\d{2})-(\d{2}):(\d{2}):(\d{2})$")


class FakeRShell:
    """SSH server behaving like Impinj RShell for the commands OTReader uses."""

    def __init__(self, legacy_only=True, processing_ms=80.0, password="impinj", reject_time=False):
        self.legacy_only = legacy_only
        self.processing_ms = processing_ms
        self.password = password
        self.reject_time = reject_time
        self.sets: list[dict] = []  # each: target second, host time at which the clock was set
        self.logins = 0
        self.key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
        self.server = None

    @property
    def port(self) -> int:
        return self.server.sockets[0].getsockname()[1]

    @property
    def offsets_ms(self) -> list[float]:
        return [(s["target"] - s["set_at"]) * 1000 for s in self.sets]

    async def start(self):
        outer = self

        class Server(asyncssh.SSHServer):
            def begin_auth(self, username):
                return True

            def password_auth_supported(self):
                return True

            def validate_password(self, username, password):
                ok = username == "root" and password == outer.password
                if ok:
                    outer.logins += 1
                return ok

        async def handle(process):
            process.stdout.write("Welcome\r\n > ")
            try:
                while True:
                    line = (await process.stdin.readuntil("\r")).strip()
                    if line == "":
                        process.stdout.write("\r\n > ")
                        continue
                    m = _TIME_RE.match(line)
                    if m:
                        await asyncio.sleep(outer.processing_ms / 1000)  # RShell work before the clock is set
                        if outer.reject_time:
                            process.stdout.write(f"{line}\r\nStatus='5,Incomplete-Parameter-List'\r\n > ")
                            continue
                        target = calendar.timegm(time.strptime(line.split()[-1], "%Y.%m.%d-%H:%M:%S"))
                        outer.sets.append({"target": target, "set_at": time.time(), "command": line})
                        process.stdout.write(f"{line}\r\nStatus='0,Success'\r\n > ")
                    else:
                        process.stdout.write(f"{line}\r\nStatus='1,Unknown-Command'\r\n > ")
            except (asyncssh.BreakReceived, asyncssh.TerminalSizeChanged, asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                process.exit(0)

        keypair = asyncssh.load_keypairs([self.key])[0]
        if self.legacy_only:  # like OpenSSH 4.x on older Impinj firmware: SHA-1 "ssh-rsa" only
            keypair.host_key_algorithms = (b"ssh-rsa",)
        self.server = await asyncssh.create_server(
            Server, "127.0.0.1", 0, server_host_keys=[keypair], process_factory=handle, line_editor=False,
        )

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()


@pytest.fixture
async def rshell():
    _legacy_hosts.clear()
    srv = FakeRShell()
    await srv.start()
    yield srv
    await srv.stop()


def store(tmp_path):
    return HostKeyStore(tmp_path / "keys.json")


def test_time_format():
    assert format_rshell_time(1790775015) == time.strftime("%Y.%m.%d-%H:%M:%S", time.gmtime(1790775015))
    assert format_rshell_time(1759239015) == "2025.09.30-13:30:15"


async def test_sets_time_at_second_boundary_with_legacy_fallback(tmp_path, rshell):
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=store(tmp_path), tolerance_ms=20,
                                 processing_estimate_ms=60)
    assert res.success and res.error is None
    assert res.legacy_ssh is True and res.host_key_type == "ssh-rsa"
    assert rshell.sets, "no time command received"
    cmd = rshell.sets[-1]["command"]
    assert re.fullmatch(r"config system time \d{4}\.\d{2}\.\d{2}-\d{2}:\d{2}:\d{2}", cmd)
    actual = rshell.offsets_ms[-1]
    # The reply-timing estimate matches what the (fake) reader actually did...
    assert abs(res.estimated_offset_ms - actual) < 15
    # ...and the final set landed within tolerance (+ scheduling slack).
    assert abs(actual) < 30, rshell.offsets_ms
    # Host key pinned (trust on first use).
    assert (tmp_path / "keys.json").read_text().count("ssh-rsa") == 1


async def test_retries_using_observed_processing_time(tmp_path, rshell):
    rshell.processing_ms = 150  # much slower than the 60 ms initial estimate
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=store(tmp_path), tolerance_ms=20,
                                 processing_estimate_ms=0, max_attempts=3)
    assert len(res.attempts) >= 2
    assert abs(rshell.offsets_ms[0]) > 100  # first attempt, uncompensated
    assert abs(rshell.offsets_ms[-1]) < 30  # corrected on a later attempt
    assert res.attempts[0]["processing_ms"] > 120


async def test_modern_server_needs_no_fallback(tmp_path):
    _legacy_hosts.clear()
    srv = FakeRShell(legacy_only=False)
    await srv.start()
    try:
        res = await sync_reader_time("127.0.0.1", port=srv.port, host_keys=store(tmp_path))
        assert res.success and res.legacy_ssh is False and res.host_key_type == "ssh-rsa"
    finally:
        await srv.stop()


async def test_legacy_fallback_can_be_disabled(tmp_path, rshell):
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=store(tmp_path), legacy_fallback=False)
    assert not res.success and res.error_kind == "protocol" and rshell.sets == []


async def test_wrong_password(tmp_path, rshell):
    res = await sync_reader_time("127.0.0.1", port=rshell.port, password="nope", host_keys=store(tmp_path))
    assert not res.success and res.error_kind == "auth" and "reader.ssh" in res.error


async def test_pinned_host_key_mismatch_is_refused(tmp_path, rshell):
    other = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    s = store(tmp_path)
    s.put("127.0.0.1", rshell.port, other.export_public_key("openssh").decode().strip())
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=s)
    assert not res.success and res.error_kind == "host_key_mismatch"
    assert rshell.logins == 0  # password never sent to an unverified host
    assert rshell.sets == []


async def test_second_run_uses_pinned_legacy_key_directly(tmp_path, rshell):
    s = store(tmp_path)
    assert (await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=s)).success
    _legacy_hosts.clear()  # simulate a fresh process: only the pinned key file remains
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=s)
    assert res.success and res.legacy_ssh is True


async def test_legacy_fallback_after_connection_reset(tmp_path, rshell):
    """Real OpenSSH 4.x readers reset the TCP connection on the modern attempt instead of
    sending an SSH disconnect; that must still trigger the legacy retry."""
    connections = 0

    async def handle(reader, writer):
        nonlocal connections
        connections += 1
        if connections == 1:  # modern attempt: read the client banner, then RST
            await reader.read(64)
            sock = writer.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
            writer.transport.abort()
            return
        up_r, up_w = await asyncio.open_connection("127.0.0.1", rshell.port)

        async def pipe(src, dst):
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            except ConnectionError:
                pass
            finally:
                dst.close()

        await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer))

    proxy = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = proxy.sockets[0].getsockname()[1]
    try:
        res = await sync_reader_time("127.0.0.1", port=port, host_keys=store(tmp_path))
        assert res.success and res.legacy_ssh is True, res.error
        assert connections >= 2
    finally:
        proxy.close()


async def test_unreachable(tmp_path):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    res = await sync_reader_time("127.0.0.1", port=port, host_keys=store(tmp_path), timeout=3)
    assert not res.success and res.error_kind == "unreachable"


async def test_rejected_command(tmp_path):
    _legacy_hosts.clear()
    srv = FakeRShell(reject_time=True)
    await srv.start()
    try:
        res = await sync_reader_time("127.0.0.1", port=srv.port, host_keys=store(tmp_path))
        assert not res.success and res.error_kind == "command" and "Incomplete-Parameter-List" in res.error
    finally:
        await srv.stop()


# ----------------------------------------------------------------- reader manager integration


class Syncer:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    async def __call__(self):
        self.calls += 1
        kind = self.results.pop(0) if self.results else None
        if kind is None:
            return TimeSyncResult(success=True, host="h", started_at="2026-09-30T13:00:00Z", estimated_offset_ms=3.0,
                                  attempts=[{"command": "config system time x"}])
        return TimeSyncResult(success=False, host="h", started_at="2026-09-30T13:00:00Z", error_kind=kind,
                              error=f"{kind} failure")


async def test_manager_syncs_once_before_first_llrp_connection(tmp_path, sim):
    cfg = make_config(tmp_path, port=sim.port, reader={"time_sync": {"enabled": True}})
    syncer = Syncer([None])
    order = []
    mgr = ReaderManager(cfg, lambda *a: None, time_syncer=syncer)
    orig = syncer.__call__

    async def tracking():
        order.append(("sync", sim.connections))
        return await orig()

    mgr._time_syncer = tracking
    await mgr.start()
    await wait_for(lambda: mgr.state.value == "idle", 10)  # initialisation complete
    assert order == [("sync", 0)]  # before any LLRP connection
    st = mgr.status()
    assert st["time_sync"]["success"] and st["time_sync"]["measured_offset_ms"] is not None
    await sim.drop_connection()
    await wait_for(lambda: sim.connections == 2 and mgr.state.value == "idle", 10)
    assert syncer.calls == 1  # not repeated on reconnect
    await mgr.shutdown()


async def test_manager_retries_sync_while_reader_unreachable_then_gives_up_after_llrp_connects(tmp_path, sim):
    cfg = make_config(tmp_path, port=sim.port, reader={"time_sync": {"enabled": True}})
    syncer = Syncer(["unreachable"])
    mgr = ReaderManager(cfg, lambda *a: None, time_syncer=syncer)
    await mgr.start()
    await wait_for(lambda: mgr.state.value == "idle", 10)  # initialisation complete
    # SSH unreachable but LLRP up: timing proceeds and the failure is reported, no endless retries.
    assert syncer.calls == 1 and mgr.status()["time_sync"]["error_kind"] == "unreachable"
    await sim.drop_connection()
    await wait_for(lambda: sim.connections == 2 and mgr.state.value == "idle", 10)
    assert syncer.calls == 1
    await mgr.shutdown()


async def test_disabled_sync_is_never_attempted(tmp_path, sim):
    syncer = Syncer([None])
    mgr = ReaderManager(make_config(tmp_path, port=sim.port), lambda *a: None, time_syncer=syncer)
    await mgr.start()
    await wait_for(lambda: mgr.state.value == "idle", 10)  # initialisation complete
    assert syncer.calls == 0 and mgr.status()["time_sync"] is None
    await mgr.shutdown()


async def test_sync_time_api_refused_while_reading(tmp_path, sim):
    config = make_config(tmp_path, port=sim.port)
    svc = Services(config)
    svc.reader._time_syncer = Syncer([None, None])
    await svc.start()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(config, svc)), base_url="http://t")
    try:
        await wait_for(lambda: svc.reader.connected, 10)
        r = await client.post("/api/v1/reader/sync-time")
        assert r.status_code == 200 and r.json()["success"] is True
        await client.post("/api/v1/reader/start")
        r = await client.post("/api/v1/reader/sync-time")
        assert r.status_code == 409 and "stop reading" in r.json()["error"]["message"]
        cfg = (await client.get("/api/v1/system/config")).json()
        assert cfg["reader"]["ssh"]["password"] == "***"
    finally:
        await client.aclose()
        await svc.stop()


async def test_unsynchronised_host_clock_does_not_set_reader(tmp_path, rshell):
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=store(tmp_path),
                                 host_clock_check=lambda: {"synchronized": False, "estimated_error_ms": 16000.0})
    assert not res.success and res.error_kind == "host_clock_unsynchronized"
    assert rshell.logins == 0 and rshell.sets == []  # never even logged in
    assert res.host_clock["synchronized"] is False


@pytest.mark.parametrize("status, require", [({"synchronized": None}, True), ({"synchronized": False}, False),
                                             ({"synchronized": True, "estimated_error_ms": 0.5}, True)])
async def test_host_clock_check_allows_when_ok_unknown_or_not_required(tmp_path, rshell, status, require):
    res = await sync_reader_time("127.0.0.1", port=rshell.port, host_keys=store(tmp_path),
                                 host_clock_check=lambda: status, require_host_clock_synchronized=require)
    assert res.success and rshell.sets and res.host_clock == status


def test_host_clock_status_reports_linux_kernel_state():
    from app.clock import host_clock_status

    st = host_clock_status()
    assert set(st) == {"synchronized", "estimated_error_ms", "max_error_ms"}
    assert st["synchronized"] in (True, False, None)
