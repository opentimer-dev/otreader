import json
import sqlite3

import pytest

from app.clock import ManualClock, TimestampSource
from app.config import OperatingMode
from app.models import Confidence, EstimationMethod, TimingEvent
from app.persistence.recovery import prepare_database, recover_state
from app.persistence.repository import NotFoundError, Repository
from app.timing.cooldown import CooldownTracker
from app.timing.inventory import Inventory

T0 = 1_790_000_000_000_000


def ev(eid, epc, t, mode=OperatingMode.FINISH_LINE, antennas=(1,), created=None):
    return TimingEvent(event_id=eid, epc=epc, mode=mode, timestamp_us=t, antenna=antennas[0], antennas=list(antennas),
                       first_seen_us=t - 100, last_seen_us=t + 100, peak_rssi=-41.0, observation_count=5,
                       estimation_method=EstimationMethod.RSSI_QUADRATIC_FIT, confidence=Confidence.HIGH,
                       estimated=True, timestamp_source=TimestampSource.READER_UTC, uncertainty_ms=4.0,
                       created_at_us=created or t)


@pytest.fixture
async def repo(tmp_path):
    clock = ManualClock(start_us=T0)
    r = Repository(tmp_path / "t.db", clock.now_us)
    await r.open()
    r.test_clock = clock
    yield r
    await r.close()


async def seed(repo):
    # Creation (sequence) order deliberately differs from timestamp order.
    events = [
        ev("e1", "A", T0 + 3_000_000),
        ev("e2", "B", T0 + 1_000_000, antennas=(2,)),
        ev("e3", "C", T0 + 2_000_000, mode=OperatingMode.START_LINE, antennas=(1, 2)),
        ev("e4", "D", T0 + 5_000_000),
        ev("e5", "E", T0 + 4_000_000),
    ]
    for e in events:
        assert await repo.insert_event(e, [{"t": e.timestamp_us}], None, create_callback=True) is not None
    return events


async def test_insert_assigns_sequence_and_ignores_duplicates(repo):
    e = ev("x", "A", T0)
    assert await repo.insert_event(e, [], None, True) == 1
    assert await repo.insert_event(e, [], None, True) is None  # same event_id
    dup = ev("y", "A", T0)  # same epc/mode/timestamp => duplicate processing
    assert await repo.insert_event(dup, [], None, True) is None
    assert await repo.event_count() == 1
    got = await repo.get_event("x")
    assert got["sequence"] == 1 and got["timestamp"].endswith("Z")


async def test_event_ordering_and_filters(repo):
    await seed(repo)
    _, by_ts = await repo.query_events(order_by="timestamp")
    assert [e["event_id"] for e in by_ts] == ["e2", "e3", "e1", "e5", "e4"]
    _, by_seq = await repo.query_events(order_by="sequence")
    assert [e["event_id"] for e in by_seq] == ["e1", "e2", "e3", "e4", "e5"]
    _, desc = await repo.query_events(order_by="timestamp", descending=True, limit=2)
    assert [e["event_id"] for e in desc] == ["e4", "e5"]
    total, ant2 = await repo.query_events(antenna=2)
    assert total == 2 and {e["event_id"] for e in ant2} == {"e2", "e3"}
    total, _ = await repo.query_events(mode="start_line")
    assert total == 1
    total, _ = await repo.query_events(epc="a")
    assert total == 1
    # start inclusive, end exclusive
    total, rng = await repo.query_events(start_us=T0 + 2_000_000, end_us=T0 + 4_000_000)
    assert [e["event_id"] for e in rng] == ["e3", "e1"]
    total, after = await repo.query_events(after_seq=3, order_by="sequence")
    assert [e["event_id"] for e in after] == ["e4", "e5"]


async def test_rewind_by_event_id_is_inclusive_in_sequence_order(repo):
    await seed(repo)
    s = await repo.create_replay("c1", "e2", None, 3600)
    assert (s["start_sequence"], s["end_sequence"], s["total_events"]) == (2, 5, 4)
    page = await repo.replay_page("c1", 2, None)
    assert [e["event_id"] for e in page["events"]] == ["e2", "e3"] and page["has_more"]
    # New live events after the rewind are not part of the replay.
    await repo.insert_event(ev("e6", "F", T0 + 9_000_000), [], None, True)
    page2 = await repo.replay_page("c1", 10, None)
    assert [e["event_id"] for e in page2["events"]] == ["e4", "e5"] and not page2["has_more"]
    # Re-read from an earlier position (lost response recovery).
    again = await repo.replay_page("c1", 10, page["position"])
    assert [e["event_id"] for e in again["events"]] == ["e4", "e5"]
    # Replay did not alter history.
    assert await repo.event_count() == 6


async def test_rewind_by_timestamp_is_inclusive(repo):
    await seed(repo)
    s = await repo.create_replay("c2", None, T0 + 3_000_000, 3600)
    assert s["total_events"] == 3
    page = await repo.replay_page("c2", 100, None)
    # timestamp >= T0+3s, delivered in original sequence order
    assert [e["event_id"] for e in page["events"]] == ["e1", "e4", "e5"]


async def test_rewind_independent_cursors_and_errors(repo):
    await seed(repo)
    await repo.create_replay("a", "e1", None, 3600)
    await repo.create_replay("b", "e1", None, 3600)
    await repo.replay_page("a", 5, None)
    pb = await repo.replay_page("b", 1, None)
    assert pb["events"][0]["event_id"] == "e1"  # cursor b unaffected by a
    with pytest.raises(NotFoundError):
        await repo.create_replay("c", "nope", None, 3600)
    with pytest.raises(NotFoundError):
        await repo.replay_page("zzz", 1, None)
    assert await repo.delete_replay("a")
    empty = await repo.create_replay("d", None, T0 + 99_000_000, 3600)
    assert empty["total_events"] == 0
    assert (await repo.replay_page("d", 5, None))["events"] == []


async def test_replay_expiry(repo):
    await seed(repo)
    await repo.create_replay("c", "e1", None, 10)
    repo.test_clock.advance(11)
    with pytest.raises(NotFoundError):
        await repo.replay_page("c", 5, None)


async def test_export_json_and_csv(repo):
    await seed(repo)
    data = b"".join(repo.iter_export("json"))
    rows = json.loads(data)
    assert [r["event_id"] for r in rows] == ["e1", "e2", "e3", "e4", "e5"]
    csv_text = b"".join(repo.iter_export("csv", mode="start_line")).decode()
    lines = csv_text.strip().splitlines()
    assert lines[0].startswith("sequence,event_id,epc") and len(lines) == 2 and "e3" in lines[1]
    assert json.loads(b"".join(repo.iter_export("json", epc="ZZZ"))) == []


async def test_inventory_snapshot_is_replaced_atomically(repo):
    await repo.save_inventory_snapshot([{"epc": "A", "x": 1}, {"epc": "B", "x": 2}])
    await repo.save_inventory_snapshot([{"epc": "C", "x": 3}])
    rows = await repo.load_inventory()
    assert [r["epc"] for r in rows] == ["C"]
    assert repo.last_snapshot_us == T0


async def test_callback_queue_lifecycle(repo):
    await seed(repo)
    due = await repo.due_callbacks(T0, 10, set())
    assert due == ["e1", "e2", "e3", "e4", "e5"]
    assert await repo.due_callbacks(T0, 2, {"e1"}) == ["e2", "e3"]
    await repo.update_callback("e1", status="delivered", attempts=1, delivered_us=T0)
    await repo.update_callback("e2", status="failed", attempts=4, last_error="HTTP 500")
    await repo.update_callback("e3", status="pending", attempts=1, next_attempt_us=T0 + 60_000_000)
    assert await repo.callback_counts() == {"delivered": 1, "failed": 1, "pending": 3}
    assert await repo.due_callbacks(T0, 10, set()) == ["e4", "e5"]
    total, items = await repo.callback_queue(["failed"], 10, 0)
    assert total == 1 and items[0]["last_error"] == "HTTP 500" and items[0]["epc"] == "B"
    assert await repo.retry_callbacks(None, False, T0) == 1
    assert await repo.retry_callbacks(["e3"], True, T0) == 1
    assert await repo.due_callbacks(T0, 10, set()) == ["e2", "e3", "e4", "e5"]


async def test_recovery_restores_cooldowns_and_respects_resets(tmp_path):
    clock = ManualClock(start_us=T0)
    repo = Repository(tmp_path / "r.db", clock.now_us)
    await repo.open()
    inv = Inventory(CooldownTracker(300, clock), clock)
    for eid, epc in (("e1", "A"), ("e2", "B"), ("e3", "C")):
        e = ev(eid, epc, clock.now_us(), created=clock.now_us())
        inv.accept_event(e)
        await repo.insert_event(e, [], inv.row(epc), True)
    await repo.remove_inventory_tag("B")  # operator made B eligible
    await repo.save_inventory_snapshot([])  # simulate a lost/empty snapshot
    await repo.close()

    clock.advance(60)
    repo2 = Repository(tmp_path / "r.db", clock.now_us)
    await repo2.open()
    inv2 = Inventory(CooldownTracker(300, clock), clock)
    report = await recover_state(repo2, inv2, 300, clock.now_us())
    assert report["cooldowns_reinstated_from_event_log"] == 2
    assert inv2.in_cooldown("A") and inv2.in_cooldown("C") and not inv2.in_cooldown("B")
    assert 239 < inv2.cooldown.remaining("A") <= 240
    await repo2.clear_inventory()
    inv3 = Inventory(CooldownTracker(300, clock), clock)
    await recover_state(repo2, inv3, 300, clock.now_us())
    assert not inv3.in_cooldown("A")
    await repo2.close()


async def test_corrupt_database_is_quarantined_and_salvaged(tmp_path):
    path = tmp_path / "otreader.db"
    clock = ManualClock(start_us=T0)
    repo = Repository(path, clock.now_us)
    await repo.open()
    for i in range(50):
        await repo.insert_event(ev(f"e{i}", f"T{i}", T0 + i), [{"t": i}], None, True)
    await repo.close()
    # Corrupt a page in the middle of the file (index/table pages).
    raw = bytearray(path.read_bytes())
    for off in range(len(raw) // 2, len(raw) // 2 + 2048):
        raw[off] = 0xFF
    path.write_bytes(bytes(raw))

    report = prepare_database(path)
    assert report["status"] == "recovered_from_corruption"
    assert (tmp_path / report["quarantined_to"].split("/")[-1]).exists()
    conn = sqlite3.connect(path)
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    conn.close()
    repo2 = Repository(path, clock.now_us)
    await repo2.open()  # usable afterwards
    await repo2.insert_event(ev("new", "Z", T0 + 999), [], None, True)
    assert await repo2.event_count() >= 1
    await repo2.close()


def test_non_sqlite_file_is_quarantined(tmp_path):
    path = tmp_path / "otreader.db"
    path.write_bytes(b"this is not a database" * 100)
    report = prepare_database(path)
    assert report["status"] == "recovered_from_corruption"
    assert sqlite3.connect(path).execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_healthy_database_passes_check(tmp_path):
    assert prepare_database(tmp_path / "none.db")["status"] == "new"


async def test_commit_racing_tag_removal_does_not_resurrect_cooldown(repo):
    # Event accepted at T0, operator removes the tag at T0+1s, then the commit lands.
    repo.test_clock.advance(1)
    await repo.remove_inventory_tag("A")
    e = ev("late", "A", T0, created=T0)
    await repo.insert_event(e, [], {"epc": "A", "cooldown_until_us": T0 + 300_000_000}, True)
    assert await repo.load_inventory() == []  # cooldown row not re-written
    assert await repo.event_count() == 1  # the event itself is still recorded
    # A later event for the same tag is persisted normally.
    e2 = ev("next", "A", T0 + 5_000_000, created=repo.test_clock.now_us() + 1)
    await repo.insert_event(e2, [], {"epc": "A", "cooldown_until_us": 1}, True)
    assert [r["epc"] for r in await repo.load_inventory()] == ["A"]
