"""Startup recovery.

1. :func:`prepare_database` runs before the repository opens.  It checks the
   SQLite file's integrity (``PRAGMA quick_check``).  A damaged or non-SQLite file
   is moved aside to ``<name>.corrupt-<timestamp>`` (with its -wal/-shm files), a
   fresh database is created, and every readable row of the event log, callback
   queue and inventory is salvaged into it.  The application then continues; the
   incident is reported through the health endpoint.
2. :func:`recover_state` restores the inventory and cooldowns from the last
   snapshot, then cross-checks the event log: any tag with an event more recent
   than the cooldown window whose cooldown is missing (e.g. salvaged database)
   gets its cooldown reinstated, unless the operator removed/cleared it after that
   event.  This prevents a restart from making recently timed tags eligible again.
"""

from __future__ import annotations

import logging
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from app.persistence.repository import SCHEMA, Repository
from app.timing.inventory import Inventory

log = logging.getLogger(__name__)

_SALVAGE_TABLES = ("events", "event_observations", "callbacks", "inventory", "tag_resets", "meta")


def check_integrity(path: Path) -> tuple[bool, str]:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    except sqlite3.Error as exc:
        return False, str(exc)
    try:
        rows = conn.execute("PRAGMA quick_check").fetchall()
        result = "; ".join(r[0] for r in rows)
        return result == "ok", result
    except sqlite3.DatabaseError as exc:
        return False, str(exc)
    finally:
        conn.close()


def prepare_database(path: Path) -> dict[str, Any]:
    path = Path(path)
    report: dict[str, Any] = {"database": str(path), "status": "new"}
    if not path.exists():
        return report
    ok, detail = check_integrity(path)
    if ok:
        report["status"] = "ok"
        return report
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    quarantine = path.with_name(f"{path.name}.corrupt-{stamp}")
    log.critical("Database %s failed integrity check (%s); moving it to %s and salvaging", path, detail, quarantine)
    shutil.move(str(path), quarantine)
    for suffix in ("-wal", "-shm"):
        side = Path(str(path) + suffix)
        if side.exists():
            shutil.move(str(side), str(quarantine) + suffix)
    salvaged = salvage(quarantine, path)
    report.update(
        {"status": "recovered_from_corruption", "integrity_error": detail, "quarantined_to": str(quarantine),
         "salvaged_rows": salvaged}
    )
    return report


def salvage(src: Path, dst: Path) -> dict[str, int]:
    out = sqlite3.connect(dst)
    out.executescript(SCHEMA)
    counts: dict[str, int] = {}
    try:
        old = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        log.error("Cannot open quarantined database for salvage: %s", exc)
        out.close()
        return counts
    for table in _SALVAGE_TABLES:
        n = 0
        try:
            cols = [r[1] for r in out.execute(f"PRAGMA table_info({table})")]
            cur = old.execute(f"SELECT {', '.join(cols)} FROM {table}")
            ph = ", ".join("?" for _ in cols)
            while True:
                try:
                    row = cur.fetchone()
                except sqlite3.DatabaseError as exc:
                    log.error("Salvage of %s stopped after %d rows: %s", table, n, exc)
                    break
                if row is None:
                    break
                try:
                    out.execute(f"INSERT OR IGNORE INTO {table} ({', '.join(cols)}) VALUES ({ph})", row)
                    n += 1
                except sqlite3.Error:
                    continue
        except sqlite3.DatabaseError as exc:
            log.error("Could not salvage table %s: %s", table, exc)
        counts[table] = n
    out.commit()
    out.close()
    old.close()
    log.warning("Salvaged rows: %s", counts)
    return counts


async def recover_state(repo: Repository, inventory: Inventory, cooldown_seconds: float, now_us: int) -> dict[str, Any]:
    rows = await repo.load_inventory()
    records, cooldowns = inventory.restore(rows)
    reinstated = 0
    if cooldown_seconds > 0:
        since = now_us - int(cooldown_seconds * 1_000_000)
        for epc, created in await repo.recent_event_epcs(since):
            if not inventory.in_cooldown(epc):
                if inventory.restore_cooldown(epc, created + int(cooldown_seconds * 1_000_000)):
                    reinstated += 1
    report = {
        "inventory_records": records,
        "cooldowns_restored": cooldowns,
        "cooldowns_reinstated_from_event_log": reinstated,
        "events_in_log": await repo.event_count(),
        "last_snapshot_us": repo.last_snapshot_us,
    }
    log.info("State recovered: %s", report)
    return report
