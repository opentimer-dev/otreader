"""SQLite repository.

Durability design:

* WAL journal + ``synchronous=FULL``: a committed transaction survives process
  crashes and power loss; an interrupted write is rolled back automatically on the
  next open, so previously committed events are never lost.
* All writes go through a single worker thread, in submission order.  Because
  in-memory state is serialised on the event loop immediately before submission,
  the on-disk order always matches the in-memory order (e.g. a snapshot can never
  overwrite a later tag removal).
* Accepting an event commits, in ONE transaction: the event row, its raw
  observations, the pending callback row and the tag's inventory/cooldown row.
  After a crash there is therefore never an event without its cooldown, nor a
  cooldown without its event, nor an event without its callback.
* The event log is append-only; removing tags or clearing the inventory never
  deletes events.
* Inventory snapshots replace the ``inventory`` table inside a transaction, so a
  reader always sees either the old or the new snapshot.
* Reads use separate read-only connections (WAL permits concurrent readers).
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import sqlite3
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, TypeVar

from app.models import TimingEvent

log = logging.getLogger(__name__)
T = TypeVar("T")

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    epc TEXT NOT NULL,
    mode TEXT NOT NULL,
    timestamp_us INTEGER NOT NULL,
    antenna INTEGER,
    antennas TEXT NOT NULL,
    confidence TEXT NOT NULL,
    estimation_method TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE (epc, mode, timestamp_us)
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp_us);
CREATE INDEX IF NOT EXISTS idx_events_epc ON events(epc, seq);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at_us);
CREATE TABLE IF NOT EXISTS event_observations (
    event_id TEXT PRIMARY KEY,
    observations TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS callbacks (
    event_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_us INTEGER NOT NULL,
    last_attempt_us INTEGER,
    delivered_us INTEGER,
    last_error TEXT,
    last_status_code INTEGER,
    created_us INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_callbacks_due ON callbacks(status, next_attempt_us, seq);
CREATE TABLE IF NOT EXISTS inventory (
    epc TEXT PRIMARY KEY,
    record TEXT NOT NULL,
    updated_us INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tag_resets (epc TEXT PRIMARY KEY, reset_us INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS replay_sessions (
    cursor_id TEXT PRIMARY KEY,
    created_us INTEGER NOT NULL,
    expires_us INTEGER NOT NULL,
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    position_seq INTEGER NOT NULL,
    from_timestamp_us INTEGER,
    from_event_id TEXT,
    total INTEGER NOT NULL
);
"""

EXPORT_COLUMNS = [
    "sequence", "event_id", "epc", "timestamp", "mode", "antenna", "antennas", "peak_rssi", "first_seen",
    "last_seen", "observation_count", "estimation_method", "confidence", "estimated", "timestamp_source",
    "uncertainty_ms", "created_at",
]


class NotFoundError(KeyError):
    pass


def _event_from_row(row: sqlite3.Row | tuple) -> dict[str, Any]:
    seq, payload = row[0], row[1]
    d = json.loads(payload)
    d["sequence"] = seq
    return d


class Repository:
    def __init__(self, path: Path, clock_now_us: Callable[[], int]) -> None:
        self.path = Path(path)
        self._now = clock_now_us
        self._writer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-writer")
        self._readers = ThreadPoolExecutor(max_workers=2, thread_name_prefix="db-reader")
        self._wconn: sqlite3.Connection | None = None
        self._local = threading.local()
        self._read_conns: list[sqlite3.Connection] = []
        self._read_lock = threading.Lock()
        self.last_write_us: int | None = None
        self.last_snapshot_us: int | None = None
        self.last_error: str | None = None
        self.write_failures = 0
        self._closed = False

    # ---------------------------------------------------------------- plumbing

    def _connect_writer(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(SCHEMA)
        cur = conn.execute("SELECT value FROM meta WHERE key='schema_version'")
        row = cur.fetchone()
        if row is None:
            conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', ?)", (json.dumps(SCHEMA_VERSION),))
        return conn

    def _reader_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, check_same_thread=False, timeout=30)
            conn.execute("PRAGMA query_only=ON")
            self._local.conn = conn
            with self._read_lock:
                self._read_conns.append(conn)
        return conn

    async def _write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        if self._closed:
            raise RuntimeError("repository closed")

        def run() -> T:
            assert self._wconn is not None
            conn = self._wconn
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = fn(conn)
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            return result

        try:
            result = await asyncio.get_running_loop().run_in_executor(self._writer, run)
        except Exception as exc:
            self.write_failures += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            raise
        self.last_write_us = self._now()
        self.last_error = None
        return result

    async def _read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.get_running_loop().run_in_executor(self._readers, lambda: fn(self._reader_conn()))

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._wconn = await asyncio.get_running_loop().run_in_executor(self._writer, self._connect_writer)
        snap = await self.get_meta("last_snapshot_us")
        self.last_snapshot_us = snap

    async def close(self) -> None:
        if self._closed:
            return

        def fin() -> None:
            if self._wconn is not None:
                try:
                    self._wconn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                self._wconn.close()
                self._wconn = None

        await asyncio.get_running_loop().run_in_executor(self._writer, fin)
        self._closed = True
        with self._read_lock:
            for c in self._read_conns:
                try:
                    c.close()
                except Exception:
                    pass
        self._writer.shutdown(wait=True)
        self._readers.shutdown(wait=False)

    # ---------------------------------------------------------------- meta

    async def get_meta(self, key: str, default: Any = None) -> Any:
        def q(c: sqlite3.Connection) -> Any:
            row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else default

        return await self._read(q)

    async def set_meta(self, key: str, value: Any) -> None:
        await self._write(
            lambda c: c.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value)),
            )
        )

    async def delete_meta(self, key: str) -> None:
        await self._write(lambda c: c.execute("DELETE FROM meta WHERE key=?", (key,)))

    # ---------------------------------------------------------------- events

    async def insert_event(
        self,
        event: TimingEvent,
        observations: list[dict[str, Any]],
        inventory_row: dict[str, Any] | None,
        create_callback: bool,
    ) -> int | None:
        """Commit an accepted event atomically.  Returns its sequence, or None if duplicate."""
        now = self._now()
        payload = event.payload()
        payload.pop("sequence", None)

        def tx(c: sqlite3.Connection) -> int | None:
            cur = c.execute(
                "INSERT OR IGNORE INTO events(event_id, epc, mode, timestamp_us, antenna, antennas, confidence,"
                " estimation_method, created_at_us, payload) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    event.event_id, event.epc, event.mode.value, event.timestamp_us, event.antenna,
                    "," + ",".join(str(a) for a in event.antennas) + ",", event.confidence.value,
                    event.estimation_method.value, event.created_at_us, json.dumps(payload),
                ),
            )
            if cur.rowcount == 0:
                return None
            seq = cur.lastrowid
            c.execute(
                "INSERT OR REPLACE INTO event_observations(event_id, observations) VALUES (?, ?)",
                (event.event_id, json.dumps(observations)),
            )
            if create_callback:
                c.execute(
                    "INSERT OR IGNORE INTO callbacks(event_id, seq, status, attempts, next_attempt_us, created_us)"
                    " VALUES (?, ?, 'pending', 0, ?, ?)",
                    (event.event_id, seq, now, now),
                )
            if inventory_row is not None and not self._reset_after(c, event.epc, event.created_at_us):
                c.execute(
                    "INSERT INTO inventory(epc, record, updated_us) VALUES (?,?,?)"
                    " ON CONFLICT(epc) DO UPDATE SET record=excluded.record, updated_us=excluded.updated_us",
                    (event.epc, json.dumps(inventory_row), now),
                )
            return seq

        return await self._write(tx)

    @staticmethod
    def _reset_after(c: sqlite3.Connection, epc: str, created_us: int) -> bool:
        """True if the operator removed this tag / cleared the inventory after the event was
        accepted (the commit raced the removal); the tag's cooldown row must not be re-written."""
        row = c.execute("SELECT reset_us FROM tag_resets WHERE epc=?", (epc,)).fetchone()
        if row and row[0] >= created_us:
            return True
        row = c.execute("SELECT value FROM meta WHERE key='inventory_cleared_us'").fetchone()
        return bool(row and json.loads(row[0]) >= created_us)

    async def get_event(self, event_id: str) -> dict[str, Any]:
        def q(c: sqlite3.Connection) -> dict[str, Any]:
            row = c.execute("SELECT seq, payload FROM events WHERE event_id=?", (event_id,)).fetchone()
            if row is None:
                raise NotFoundError(event_id)
            return _event_from_row(row)

        return await self._read(q)

    async def get_event_observations(self, event_id: str) -> list[dict[str, Any]]:
        def q(c: sqlite3.Connection) -> list[dict[str, Any]]:
            row = c.execute("SELECT observations FROM event_observations WHERE event_id=?", (event_id,)).fetchone()
            if row is None:
                raise NotFoundError(event_id)
            return json.loads(row[0])

        return await self._read(q)

    @staticmethod
    def _event_filters(
        epc: str | None, antenna: int | None, mode: str | None, start_us: int | None, end_us: int | None,
        after_seq: int | None,
    ) -> tuple[str, list[Any]]:
        where, args = [], []
        if epc:
            where.append("epc = ?")
            args.append(epc.upper())
        if antenna is not None:
            where.append("antennas LIKE ?")
            args.append(f"%,{int(antenna)},%")
        if mode:
            where.append("mode = ?")
            args.append(mode)
        if start_us is not None:
            where.append("timestamp_us >= ?")
            args.append(start_us)
        if end_us is not None:
            where.append("timestamp_us < ?")
            args.append(end_us)
        if after_seq is not None:
            where.append("seq > ?")
            args.append(after_seq)
        return (" WHERE " + " AND ".join(where)) if where else "", args

    async def query_events(
        self,
        epc: str | None = None,
        antenna: int | None = None,
        mode: str | None = None,
        start_us: int | None = None,
        end_us: int | None = None,
        after_seq: int | None = None,
        order_by: str = "timestamp",
        descending: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[int, list[dict[str, Any]]]:
        where, args = self._event_filters(epc, antenna, mode, start_us, end_us, after_seq)
        direction = "DESC" if descending else "ASC"
        order = f"timestamp_us {direction}, seq {direction}" if order_by == "timestamp" else f"seq {direction}"

        def q(c: sqlite3.Connection) -> tuple[int, list[dict[str, Any]]]:
            total = c.execute(f"SELECT COUNT(*) FROM events{where}", args).fetchone()[0]
            rows = c.execute(
                f"SELECT seq, payload FROM events{where} ORDER BY {order} LIMIT ? OFFSET ?", [*args, limit, offset]
            ).fetchall()
            return total, [_event_from_row(r) for r in rows]

        return await self._read(q)

    def iter_export(self, fmt: str, **filters: Any) -> Iterator[bytes]:
        """Synchronous generator (runs in a worker thread under Starlette)."""
        where, args = self._event_filters(
            filters.get("epc"), filters.get("antenna"), filters.get("mode"), filters.get("start_us"),
            filters.get("end_us"), None,
        )
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=30)
        try:
            last = 0
            first = True
            if fmt == "json":
                yield b"["
            else:
                buf = io.StringIO()
                csv.writer(buf).writerow(EXPORT_COLUMNS)
                yield buf.getvalue().encode()
            while True:
                clause = (where + " AND seq > ?") if where else " WHERE seq > ?"
                rows = conn.execute(
                    f"SELECT seq, payload FROM events{clause} ORDER BY seq LIMIT 1000", [*args, last]
                ).fetchall()
                if not rows:
                    break
                last = rows[-1][0]
                if fmt == "json":
                    chunk = ",".join(json.dumps(_event_from_row(r)) for r in rows)
                    yield ((b"" if first else b",") + chunk.encode())
                else:
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    for r in rows:
                        d = _event_from_row(r)
                        d["antennas"] = " ".join(str(a) for a in d.get("antennas") or [])
                        w.writerow([d.get(k) for k in EXPORT_COLUMNS])
                    yield buf.getvalue().encode()
                first = False
            if fmt == "json":
                yield b"]"
        finally:
            conn.close()

    async def event_count(self) -> int:
        return await self._read(lambda c: c.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    async def max_seq(self) -> int:
        return await self._read(lambda c: c.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0])

    # ---------------------------------------------------------------- inventory

    async def save_inventory_snapshot(self, rows: list[dict[str, Any]]) -> None:
        now = self._now()

        def tx(c: sqlite3.Connection) -> None:
            c.execute("DELETE FROM inventory")
            c.executemany(
                "INSERT INTO inventory(epc, record, updated_us) VALUES (?,?,?)",
                [(r["epc"], json.dumps(r), now) for r in rows],
            )
            c.execute(
                "INSERT INTO meta(key, value) VALUES('last_snapshot_us', ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (json.dumps(now),),
            )

        await self._write(tx)
        self.last_snapshot_us = now

    async def remove_inventory_tag(self, epc: str) -> None:
        now = self._now()

        def tx(c: sqlite3.Connection) -> None:
            c.execute("DELETE FROM inventory WHERE epc=?", (epc,))
            c.execute(
                "INSERT INTO tag_resets(epc, reset_us) VALUES (?, ?)"
                " ON CONFLICT(epc) DO UPDATE SET reset_us=excluded.reset_us",
                (epc, now),
            )

        await self._write(tx)

    async def clear_inventory(self) -> None:
        now = self._now()

        def tx(c: sqlite3.Connection) -> None:
            c.execute("DELETE FROM inventory")
            c.execute("DELETE FROM tag_resets")
            c.execute(
                "INSERT INTO meta(key, value) VALUES('inventory_cleared_us', ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (json.dumps(now),),
            )

        await self._write(tx)

    async def load_inventory(self) -> list[dict[str, Any]]:
        def q(c: sqlite3.Connection) -> list[dict[str, Any]]:
            out = []
            for (rec,) in c.execute("SELECT record FROM inventory"):
                try:
                    out.append(json.loads(rec))
                except (ValueError, TypeError):
                    log.warning("Skipping unreadable inventory row")
            return out

        return await self._read(q)

    async def recent_event_epcs(self, since_created_us: int) -> list[tuple[str, int]]:
        """(epc, latest created_at_us) for events created since *since_created_us*,
        excluding tags reset/cleared after that event."""

        def q(c: sqlite3.Connection) -> list[tuple[str, int]]:
            row = c.execute("SELECT value FROM meta WHERE key='inventory_cleared_us'").fetchone()
            cleared = json.loads(row[0]) if row else 0
            rows = c.execute(
                "SELECT e.epc, MAX(e.created_at_us) FROM events e WHERE e.created_at_us >= ? GROUP BY e.epc",
                (since_created_us,),
            ).fetchall()
            resets = dict(c.execute("SELECT epc, reset_us FROM tag_resets").fetchall())
            return [(epc, created) for epc, created in rows if created > cleared and created > resets.get(epc, 0)]

        return await self._read(q)

    # ---------------------------------------------------------------- callbacks

    async def due_callbacks(self, now_us: int, limit: int, exclude: set[str]) -> list[str]:
        def q(c: sqlite3.Connection) -> list[str]:
            rows = c.execute(
                "SELECT event_id FROM callbacks WHERE status='pending' AND next_attempt_us <= ? ORDER BY seq LIMIT ?",
                (now_us, limit + len(exclude)),
            ).fetchall()
            return [r[0] for r in rows if r[0] not in exclude][:limit]

        return await self._read(q)

    async def next_callback_due_us(self) -> int | None:
        return await self._read(
            lambda c: c.execute("SELECT MIN(next_attempt_us) FROM callbacks WHERE status='pending'").fetchone()[0]
        )

    async def callback_payload(self, event_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        def q(c: sqlite3.Connection) -> tuple[dict[str, Any], dict[str, Any]]:
            row = c.execute(
                "SELECT e.seq, e.payload, cb.attempts, cb.status FROM callbacks cb JOIN events e"
                " ON e.event_id = cb.event_id WHERE cb.event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(event_id)
            return _event_from_row(row[:2]), {"attempts": row[2], "status": row[3]}

        return await self._read(q)

    async def update_callback(self, event_id: str, **fields: Any) -> None:
        cols = ", ".join(f"{k}=?" for k in fields)
        await self._write(lambda c: c.execute(f"UPDATE callbacks SET {cols} WHERE event_id=?", [*fields.values(), event_id]))

    async def callback_counts(self) -> dict[str, int]:
        def q(c: sqlite3.Connection) -> dict[str, int]:
            return dict(c.execute("SELECT status, COUNT(*) FROM callbacks GROUP BY status").fetchall())

        return await self._read(q)

    async def callback_queue(self, statuses: list[str], limit: int, offset: int) -> tuple[int, list[dict[str, Any]]]:
        ph = ",".join("?" for _ in statuses)

        def q(c: sqlite3.Connection) -> tuple[int, list[dict[str, Any]]]:
            total = c.execute(f"SELECT COUNT(*) FROM callbacks WHERE status IN ({ph})", statuses).fetchone()[0]
            c.row_factory = sqlite3.Row
            try:
                rows = c.execute(
                    f"SELECT cb.*, e.epc FROM callbacks cb JOIN events e ON e.event_id=cb.event_id"
                    f" WHERE cb.status IN ({ph}) ORDER BY cb.seq LIMIT ? OFFSET ?",
                    [*statuses, limit, offset],
                ).fetchall()
                return total, [dict(r) for r in rows]
            finally:
                c.row_factory = None

        return await self._read(q)

    async def retry_callbacks(self, event_ids: list[str] | None, include_pending: bool, now_us: int) -> int:
        statuses = ["failed"] + (["pending"] if include_pending else [])
        ph = ",".join("?" for _ in statuses)

        def tx(c: sqlite3.Connection) -> int:
            sql = (
                f"UPDATE callbacks SET status='pending', attempts=0, next_attempt_us=?, last_error=NULL"
                f" WHERE status IN ({ph})"
            )
            args: list[Any] = [now_us, *statuses]
            if event_ids:
                sql += f" AND event_id IN ({','.join('?' for _ in event_ids)})"
                args += event_ids
            return c.execute(sql, args).rowcount

        return await self._write(tx)

    # ---------------------------------------------------------------- replay

    async def create_replay(
        self, cursor_id: str, event_id: str | None, from_us: int | None, ttl_s: float
    ) -> dict[str, Any]:
        now = self._now()

        def tx(c: sqlite3.Connection) -> dict[str, Any]:
            end_seq = c.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0]
            if event_id is not None:
                row = c.execute("SELECT seq FROM events WHERE event_id=?", (event_id,)).fetchone()
                if row is None:
                    raise NotFoundError(event_id)
                start_seq = row[0]
                total = c.execute(
                    "SELECT COUNT(*) FROM events WHERE seq >= ? AND seq <= ?", (start_seq, end_seq)
                ).fetchone()[0]
            else:
                row = c.execute("SELECT MIN(seq) FROM events WHERE timestamp_us >= ?", (from_us,)).fetchone()
                start_seq = row[0] if row[0] is not None else end_seq + 1
                total = c.execute(
                    "SELECT COUNT(*) FROM events WHERE seq >= ? AND seq <= ? AND timestamp_us >= ?",
                    (start_seq, end_seq, from_us),
                ).fetchone()[0]
            c.execute("DELETE FROM replay_sessions WHERE expires_us < ?", (now,))
            c.execute(
                "INSERT INTO replay_sessions(cursor_id, created_us, expires_us, start_seq, end_seq, position_seq,"
                " from_timestamp_us, from_event_id, total) VALUES (?,?,?,?,?,?,?,?,?)",
                (cursor_id, now, now + int(ttl_s * 1e6), start_seq, end_seq, start_seq - 1, from_us, event_id, total),
            )
            return {
                "cursor_id": cursor_id,
                "start_sequence": start_seq,
                "end_sequence": end_seq,
                "position": start_seq - 1,
                "total_events": total,
                "from_event_id": event_id,
                "from_timestamp_us": from_us,
                "created_us": now,
                "expires_us": now + int(ttl_s * 1e6),
            }

        return await self._write(tx)

    async def replay_page(self, cursor_id: str, limit: int, from_position: int | None) -> dict[str, Any]:
        now = self._now()

        def tx(c: sqlite3.Connection) -> dict[str, Any]:
            s = c.execute(
                "SELECT start_seq, end_seq, position_seq, from_timestamp_us, from_event_id, total, expires_us"
                " FROM replay_sessions WHERE cursor_id=?",
                (cursor_id,),
            ).fetchone()
            if s is None or s[6] < now:
                raise NotFoundError(cursor_id)
            start_seq, end_seq, pos, from_ts, from_eid, total, _ = s
            if from_position is not None:
                pos = max(from_position, start_seq - 1)
            sql = "SELECT seq, payload FROM events WHERE seq > ? AND seq <= ?"
            args: list[Any] = [pos, end_seq]
            if from_ts is not None:
                sql += " AND timestamp_us >= ?"
                args.append(from_ts)
            rows = c.execute(sql + " ORDER BY seq LIMIT ?", [*args, limit]).fetchall()
            new_pos = rows[-1][0] if rows else pos
            if rows and rows[-1][0] < end_seq:
                more_sql = "SELECT 1 FROM events WHERE seq > ? AND seq <= ?" + (
                    " AND timestamp_us >= ?" if from_ts is not None else ""
                )
                more_args = [new_pos, end_seq] + ([from_ts] if from_ts is not None else [])
                has_more = c.execute(more_sql + " LIMIT 1", more_args).fetchone() is not None
            else:
                has_more = False
            if not has_more:
                new_pos = max(new_pos, end_seq)
            c.execute(
                "UPDATE replay_sessions SET position_seq=?, expires_us=? WHERE cursor_id=?",
                (new_pos, max(s[6], now), cursor_id),
            )
            return {
                "events": [_event_from_row(r) for r in rows],
                "position": new_pos,
                "has_more": has_more,
                "start_sequence": start_seq,
                "end_sequence": end_seq,
                "total_events": total,
                "from_event_id": from_eid,
                "from_timestamp_us": from_ts,
            }

        return await self._write(tx)

    async def delete_replay(self, cursor_id: str) -> bool:
        return await self._write(
            lambda c: c.execute("DELETE FROM replay_sessions WHERE cursor_id=?", (cursor_id,)).rowcount > 0
        )

    # ---------------------------------------------------------------- health

    def health(self) -> dict[str, Any]:
        try:
            size = self.path.stat().st_size
        except OSError:
            size = None
        return {
            "path": str(self.path),
            "healthy": self.last_error is None,
            "last_error": self.last_error,
            "write_failures": self.write_failures,
            "last_write_us": self.last_write_us,
            "last_snapshot_us": self.last_snapshot_us,
            "size_bytes": size,
        }
