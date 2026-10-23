from app.clock import ManualClock, TimestampSource
from app.config import OperatingMode
from app.models import Confidence, EstimationMethod, Observation, TimingEvent
from app.timing.cooldown import CooldownTracker
from app.timing.inventory import Inventory


def obs(epc="E1", t=1_790_000_000_000_000, antenna=1, rssi=-50.0):
    return Observation(epc=epc, antenna=antenna, rssi=rssi, rssi_source="llrp_peak_rssi", timestamp_us=t,
                       timestamp_source=TimestampSource.READER_UTC, first_seen_us=t, last_seen_us=t, seen_count=1,
                       channel_index=None, phase_angle_deg=None, doppler_hz=None, rospec_id=None,
                       host_received_us=t, host_received_mono=0.0)


def event(epc="E1", t=1_790_000_000_000_000, eid="ev1"):
    return TimingEvent(event_id=eid, epc=epc, mode=OperatingMode.FINISH_LINE, timestamp_us=t, antenna=1, antennas=[1],
                       first_seen_us=t, last_seen_us=t, peak_rssi=-40.0, observation_count=3,
                       estimation_method=EstimationMethod.RSSI_QUADRATIC_FIT, confidence=Confidence.HIGH,
                       estimated=True, timestamp_source=TimestampSource.READER_UTC, uncertainty_ms=5.0,
                       created_at_us=t)


def test_cooldown_expires_on_monotonic_clock():
    clock = ManualClock()
    cd = CooldownTracker(300, clock)
    cd.start("E1")
    assert cd.active("E1")
    clock.advance(299.9)
    assert cd.active("E1")
    clock.advance(0.2)
    assert not cd.active("E1")


def test_cooldown_restore_uses_wall_clock_and_never_extends():
    clock = ManualClock()
    cd = CooldownTracker(300, clock)
    now = clock.now_us()
    assert cd.restore("E1", now + 100_000_000)  # 100 s left
    assert 99.9 < cd.remaining("E1") <= 100.0
    assert not cd.restore("E2", now - 1)  # already expired
    assert cd.restore("E3", now + 10_000_000_000)  # wall clock jumped: cap at configured 300 s
    assert cd.remaining("E3") == 300.0


def test_inventory_accept_starts_cooldown_based_on_event_not_last_observation():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    inv.record_observation(obs(), OperatingMode.FINISH_LINE, suppressed=False)
    assert not inv.in_cooldown("E1")
    clock.advance(10)
    inv.accept_event(event())
    clock.advance(200)
    # Many later observations do not extend the cooldown.
    for _ in range(5):
        inv.record_observation(obs(), OperatingMode.FINISH_LINE, suppressed=True)
        clock.advance(10)
    assert inv.in_cooldown("E1")
    clock.advance(51)
    assert not inv.in_cooldown("E1")
    rec = inv.get("E1")
    assert rec.suppressed_count == 5 and rec.event_count == 1
    assert inv.view(rec)["status"] == "eligible_after_cooldown"


def test_remove_makes_tag_eligible_immediately():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    inv.accept_event(event())
    assert inv.in_cooldown("E1")
    assert inv.remove("E1")
    assert not inv.in_cooldown("E1") and inv.get("E1") is None
    assert not inv.remove("E1")


def test_clear_all():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    inv.accept_event(event("A"))
    inv.accept_event(event("B", eid="ev2"))
    assert inv.clear() == 2
    assert len(inv) == 0 and not inv.in_cooldown("A")


def test_snapshot_restore_roundtrip_preserves_cooldown():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    inv.record_observation(obs("A", antenna=2), OperatingMode.FINISH_LINE, False)
    inv.accept_event(event("A"))
    inv.record_observation(obs("B"), OperatingMode.START_LINE, False)
    rows = inv.snapshot()
    clock.advance(100)  # simulated downtime

    inv2 = Inventory(CooldownTracker(300, clock), clock)
    records, cooldowns = inv2.restore(rows)
    assert (records, cooldowns) == (2, 1)
    assert inv2.in_cooldown("A") and not inv2.in_cooldown("B")
    assert 199 < inv2.cooldown.remaining("A") <= 200
    assert inv2.get("A").antenna_counts == {"2": 1}


def test_query_filters_and_pagination():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    base = 1_790_000_000_000_000
    for i in range(10):
        inv.record_observation(obs(f"30080000{i:04d}", t=base + i * 1_000_000, antenna=1 + i % 2),
                               OperatingMode.START_LINE, False)
    total, items = inv.query(antenna=2)
    assert total == 5 and all(2 in t["antennas"] for t in items)
    total, items = inv.query(epc="0003")
    assert total == 1 and items[0]["epc"] == "300800000003"
    total, items = inv.query(seen_after_us=base + 5_000_000, limit=2)
    assert total == 5 and len(items) == 2
    total, _ = inv.query(status="eligible")
    assert total == 10


def test_snapshot_omits_cooldown_for_uncommitted_events():
    clock = ManualClock()
    inv = Inventory(CooldownTracker(300, clock), clock)
    inv.accept_event(event("A"))
    inv.accept_event(event("B", eid="ev2"))
    rows = {r["epc"]: r for r in inv.snapshot(uncommitted={"A"})}
    assert rows["A"]["cooldown_until_us"] is None
    assert rows["B"]["cooldown_until_us"] is not None
    assert inv.in_cooldown("A")  # in-memory cooldown unaffected
