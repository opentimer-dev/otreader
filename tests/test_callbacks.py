import asyncio
import hashlib
import hmac
import json

import httpx
import pytest

from app.callbacks.dispatcher import CallbackDispatcher
from app.callbacks.url_validation import CallbackURLError, redact_url, validate_callback_url
from app.clock import SystemClock, TimestampSource
from app.config import CallbackConfig, OperatingMode
from app.models import Confidence, EstimationMethod, TimingEvent
from app.persistence.repository import Repository
from conftest import wait_for

T0 = 1_790_000_000_000_000


def ev(i):
    return TimingEvent(event_id=f"e{i}", epc=f"EPC{i:04d}", mode=OperatingMode.FINISH_LINE, timestamp_us=T0 + i,
                       antenna=1, antennas=[1], first_seen_us=T0, last_seen_us=T0 + i, peak_rssi=-40.0,
                       observation_count=3, estimation_method=EstimationMethod.RSSI_QUADRATIC_FIT,
                       confidence=Confidence.HIGH, estimated=True, timestamp_source=TimestampSource.READER_UTC,
                       uncertainty_ms=3.0, created_at_us=T0 + i)


class Endpoint:
    def __init__(self, fail_first=0, status=200, delay=0.0, always_fail=False):
        self.fail_first = fail_first
        self.status = status
        self.delay = delay
        self.always_fail = always_fail
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.always_fail or len(self.requests) <= self.fail_first:
            return httpx.Response(503, text="busy")
        return httpx.Response(self.status, json={"ok": True})

    @property
    def bodies(self):
        return [json.loads(r.content) for r in self.requests]


@pytest.fixture
async def repo(tmp_path):
    r = Repository(tmp_path / "cb.db", SystemClock().now_us)
    await r.open()
    yield r
    await r.close()


async def make(repo, endpoint, **cfg):
    config = CallbackConfig(**({"retry_delay_seconds": 0.0, "retry_max_delay_seconds": 0.0} | cfg))
    d = CallbackDispatcher(config, repo, SystemClock(),
                           lambda: httpx.AsyncClient(transport=httpx.MockTransport(endpoint)))
    await d.start(None)
    await d.set_url("http://127.0.0.1:9/hook")
    return d


async def add(repo, d, i):
    seq = await repo.insert_event(ev(i), [], None, create_callback=True)
    d.notify(f"e{i}")
    return seq


async def test_successful_delivery_and_payload_schema(repo):
    ep = Endpoint()
    d = await make(repo, ep, secret="s3cret")
    await add(repo, d, 1)
    await wait_for(lambda: len(ep.requests) == 1)
    await wait_for(lambda: repo.callback_counts())
    body = ep.bodies[0]
    for key in ("event_id", "epc", "timestamp", "mode", "antenna", "peak_rssi", "first_seen", "last_seen",
                "observation_count", "estimation_method", "confidence", "sequence"):
        assert key in body
    assert body["event_id"] == "e1" and body["sequence"] == 1 and body["timestamp"].endswith("Z")
    req = ep.requests[0]
    assert req.headers["X-OTReader-Event-Id"] == "e1"
    expected = "sha256=" + hmac.new(b"s3cret", req.content, hashlib.sha256).hexdigest()
    assert req.headers["X-OTReader-Signature"] == expected
    await wait_for(lambda: d.delivered == 1)
    assert (await repo.callback_counts()) == {"delivered": 1}
    await d.stop()


async def test_retries_then_succeeds(repo):
    ep = Endpoint(fail_first=2)
    d = await make(repo, ep, max_retries=3)
    await add(repo, d, 1)
    await wait_for(lambda: d.delivered == 1, timeout=10)
    assert len(ep.requests) == 3
    assert (await repo.callback_queue(["delivered"], 10, 0))[1][0]["attempts"] == 3
    await d.stop()


async def test_exhausted_retries_mark_failed_and_manual_retry(repo):
    ep = Endpoint(always_fail=True)
    d = await make(repo, ep, max_retries=2)
    await add(repo, d, 1)

    async def failed():
        return (await repo.callback_counts()).get("failed") == 1

    await wait_for(failed, timeout=10)
    assert len(ep.requests) == 3  # 1 + max_retries
    total, items = await repo.callback_queue(["failed"], 10, 0)
    assert items[0]["last_error"].startswith("HTTP 503")
    # Event remains in the event log regardless.
    assert await repo.event_count() == 1
    ep.always_fail = False
    assert await d.retry(["e1"], include_pending=False) == 1
    await wait_for(lambda: d.delivered == 1, timeout=10)
    await d.stop()


async def test_bounded_queue_spills_to_disk_and_catches_up(repo):
    ep = Endpoint(delay=0.01)
    d = await make(repo, ep, queue_max_size=3)
    for i in range(1, 21):
        await add(repo, d, i)
    assert d._queue.qsize() <= 3
    assert d.overflowed > 0
    await wait_for(lambda: d.delivered == 20, timeout=15)
    assert sorted(b["event_id"] for b in ep.bodies) == sorted(f"e{i}" for i in range(1, 21))
    await d.stop()


async def test_disabled_callbacks_keep_pending_until_reenabled(repo):
    ep = Endpoint()
    d = await make(repo, ep)
    d.disable()
    await repo.insert_event(ev(1), [], None, create_callback=True)
    d.notify("e1")
    await asyncio.sleep(0.3)
    assert ep.requests == []
    await d.set_url("http://127.0.0.1:9/hook")
    await wait_for(lambda: d.delivered == 1, timeout=5)
    await d.stop()


async def test_slow_endpoint_does_not_block_notify(repo):
    ep = Endpoint(delay=2.0)
    d = await make(repo, ep, timeout_seconds=5)
    loop = asyncio.get_running_loop()
    for i in range(1, 6):
        await repo.insert_event(ev(i), [], None, True)
    t = loop.time()
    for i in range(1, 6):
        d.notify(f"e{i}")  # must return immediately
    assert loop.time() - t < 0.05
    await d.stop()


async def test_timeout_is_recorded(repo):
    # A real TCP server that accepts but never answers (MockTransport ignores timeouts).
    async def hang(reader, writer):
        await asyncio.sleep(30)

    server = await asyncio.start_server(hang, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    d = CallbackDispatcher(CallbackConfig(timeout_seconds=0.2, max_retries=0), repo, SystemClock())
    await d.start(None)
    await d.set_url(f"http://127.0.0.1:{port}/hook")
    await add(repo, d, 1)

    async def failed():
        return (await repo.callback_counts()).get("failed") == 1

    await wait_for(failed, timeout=5)
    _, items = await repo.callback_queue(["failed"], 10, 0)
    assert "timeout" in items[0]["last_error"].lower()
    await d.stop()
    server.close()


@pytest.mark.parametrize(
    "url, cfg, ok",
    [
        ("http://192.168.8.20:8080/x", {}, True),
        ("https://127.0.0.1/x", {}, True),
        ("ftp://192.168.1.1/x", {}, False),
        ("http:///nohost", {}, False),
        ("http://169.254.169.254/latest/meta-data", {}, False),
        ("http://224.0.0.1/x", {}, False),
        ("http://127.0.0.1/x", {"allow_loopback": False}, False),
        ("http://10.1.2.3/x", {"allow_private_networks": False}, False),
        ("http://8.8.8.8/x", {"allow_public_networks": False}, False),
        ("http://192.168.8.20/x", {"allowed_networks": ["192.168.8.0/24"]}, True),
        ("http://192.168.9.20/x", {"allowed_networks": ["192.168.8.0/24"]}, False),
        ("http://[::1]:8000/x", {}, True),
        ("http://[fe80::1]/x", {}, False),
    ],
)
async def test_url_validation(url, cfg, ok):
    config = CallbackConfig(**cfg)
    if ok:
        assert await validate_callback_url(url, config) == url
    else:
        with pytest.raises(CallbackURLError):
            await validate_callback_url(url, config)


def test_redact_url():
    assert redact_url("http://u:p@host:81/a?b") == "http://***@host:81/a?b"
    assert redact_url("http://host/a") == "http://host/a"
