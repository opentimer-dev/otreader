"""Console client: show reader information and run antenna test mode.

Connects to the OTReader middleware over HTTP, prints the reader's identity and
antennas, starts antenna test mode and shows a live table of every antenna with
its connection status and whether a tag has been seen on it since the test
started.  Press Ctrl+C to stop; test mode is then stopped and timing inventory
is restored by the middleware (if this client started the test).

    python -m clients.antenna_test --url http://192.168.8.10:8000
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import httpx

API = "/api/v1"


class ClientError(Exception):
    pass


class MiddlewareClient:
    """Thin wrapper around the OTReader REST API."""

    def __init__(self, base_url: str, api_key: str | None = None, timeout: float = 70.0,
                 transport: httpx.BaseTransport | None = None) -> None:
        headers = {"X-API-Key": api_key} if api_key else {}
        # Reader operations (connect, test start/stop) can take a while; allow for it.
        self._http = httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=timeout,
                                  transport=transport)

    def close(self) -> None:
        self._http.close()

    def _request(self, method: str, path: str, **kw: Any) -> Any:
        try:
            r = self._http.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise ClientError(f"cannot reach middleware at {self._http.base_url}: {exc}") from None
        if r.status_code >= 400:
            try:
                err = r.json()["error"]
                msg = f"{err['message']} ({err['code']}, HTTP {r.status_code})"
            except Exception:
                msg = f"HTTP {r.status_code}: {r.text[:200]}"
            raise ClientError(msg)
        return r.json()

    def reader_status(self) -> dict[str, Any]:
        return self._request("GET", f"{API}/reader/status")

    def capabilities(self) -> dict[str, Any]:
        return self._request("GET", f"{API}/reader/capabilities")

    def connect(self) -> dict[str, Any]:
        return self._request("POST", f"{API}/reader/connect")

    def start_test(self) -> dict[str, Any]:
        return self._request("POST", f"{API}/test/start")

    def stop_test(self) -> dict[str, Any]:
        return self._request("POST", f"{API}/test/stop")

    def test_reports(self, after_sequence: int, limit: int = 1000) -> dict[str, Any]:
        return self._request("GET", f"{API}/test/reports", params={"after_sequence": after_sequence, "limit": limit})


# --------------------------------------------------------------------------- model


@dataclass
class AntennaRow:
    antenna: int
    connected: bool | None  # None = reader does not report connection state
    tag_seen: bool
    reads: int = 0
    unique_tags: int = 0
    rssi_max: float | None = None
    last_read_at: str | None = None


def antenna_rows(caps: dict[str, Any], summary: dict[str, Any]) -> list[AntennaRow]:
    """Merge the reader's antenna list with the test-mode per-antenna summary.

    Every antenna the reader has is listed, even if it has never seen a tag; antennas
    that report reads but are missing from the capability list are added too.
    """
    connected: dict[int, bool | None] = {a["antenna_id"]: a.get("connected") for a in caps.get("antennas") or []}
    for a in range(1, (caps.get("max_antennas") or 0) + 1):
        connected.setdefault(a, None)
    stats = {s["antenna"]: s for s in summary.get("per_antenna", []) if s.get("antenna") is not None}
    rows = []
    for ant in sorted(set(connected) | set(stats)):
        s = stats.get(ant, {})
        rows.append(
            AntennaRow(
                antenna=ant,
                connected=connected.get(ant),
                tag_seen=s.get("reads", 0) > 0,
                reads=s.get("reads", 0),
                unique_tags=s.get("unique_tags", 0),
                rssi_max=s.get("rssi_max"),
                last_read_at=s.get("last_read_at"),
            )
        )
    return rows


# --------------------------------------------------------------------------- rendering


class Style:
    def __init__(self, color: bool) -> None:
        self.color = color

    def _c(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def good(self, t: str) -> str:
        return self._c("32", t)

    def bad(self, t: str) -> str:
        return self._c("31", t)

    def warn(self, t: str) -> str:
        return self._c("33", t)

    def bold(self, t: str) -> str:
        return self._c("1", t)

    def dim(self, t: str) -> str:
        return self._c("2", t)


# --------------------------------------------------------------------------- peak meter

# 256-colour palette entries.
DARK_GREEN_BG = "48;5;22"  # track: antenna connected
GREY_BG = "48;5;237"  # track: antenna disconnected / unknown
BRIGHT_GREEN_FG = "38;5;46"  # current-read bar and peak mark
_EIGHTHS = " ▏▎▍▌▋▊▉"  # partial-cell blocks for smooth bar ends


class CurrentReads:
    """Most recent RSSI per antenna, held for *hold_s* seconds (like a meter's decay).

    Without the hold the bar would freeze at the last value after the tag has left
    the field, looking like a live signal.  Times are the client's monotonic clock
    at the poll that delivered the read.
    """

    def __init__(self, hold_s: float) -> None:
        self.hold_s = hold_s
        self._last: dict[int, tuple[float, float]] = {}  # antenna -> (rssi, received_mono)

    def update(self, reports: list[dict[str, Any]], now: float) -> None:
        for r in reports:  # reports arrive in sequence order; the last per antenna wins
            if r.get("antenna") is not None and r.get("rssi") is not None:
                self._last[r["antenna"]] = (float(r["rssi"]), now)

    def value(self, antenna: int, now: float) -> float | None:
        entry = self._last.get(antenna)
        if entry is None or now - entry[1] > self.hold_s:
            return None
        return entry[0]


def _fraction(rssi: float, lo: float, hi: float) -> float:
    return min(1.0, max(0.0, (rssi - lo) / (hi - lo)))


def meter_cells(connected: bool | None, current: float | None, peak: float | None,
                width: int, lo: float, hi: float) -> list[tuple[str, str]]:
    """Return the meter as a list of (kind, char) cells.

    kind is one of: 'track' (connected), 'off' (not connected/unknown), 'fill'
    (current-read bar), 'peak' (session-peak mark).
    """
    track = "track" if connected else "off"
    cells = [(track, " ") for _ in range(width)]
    if current is not None:
        exact = _fraction(current, lo, hi) * width
        full = int(exact)
        for i in range(min(full, width)):
            cells[i] = ("fill", "█")
        part = int((exact - full) * 8)
        if full < width and part:
            cells[full] = ("fill", _EIGHTHS[part])
        elif full == 0 and not part:
            cells[0] = ("fill", "▏")  # a read at/below the scale minimum is still visible
    if peak is not None:
        idx = min(width - 1, int(_fraction(peak, lo, hi) * width))
        cells[idx] = ("peak", "┃")
    return cells


def render_meter_bar(cells: list[tuple[str, str]], connected: bool | None, st: Style) -> str:
    """Colour: dark-green track when connected (grey otherwise), bright-green current bar
    and bright-green peak mark.  Without colour an ASCII equivalent is used."""
    if not st.color:
        ascii_map = {"track": "-", "off": ".", "fill": "#", "peak": "|"}
        return "[" + "".join(ascii_map[k] for k, _ in cells) + "]"
    bg = DARK_GREEN_BG if connected else GREY_BG
    out = []
    for kind, ch in cells:
        if kind == "fill":
            out.append(f"\033[{BRIGHT_GREEN_FG};{bg}m{ch}")
        elif kind == "peak":
            out.append(f"\033[1;{BRIGHT_GREEN_FG};{bg}m{ch}")
        else:
            out.append(f"\033[{bg}m{ch}")
    return "".join(out) + "\033[0m"


def render_scale(width: int, lo: float, hi: float, indent: int) -> str:
    line = [" "] * (width + 6)
    start = int(-(-lo // 10) * 10)  # first multiple of 10 >= lo
    next_free = 0
    for v in range(start, int(hi) + 1, 10):
        pos = min(width - 1, int(_fraction(v, lo, hi) * width))
        label = str(v)
        if pos < next_free:  # would overlap the previous label on a narrow meter
            continue
        for j, ch in enumerate(label):
            if pos + j < len(line):
                line[pos + j] = ch
        next_free = pos + len(label) + 1
    return " " * indent + "".join(line).rstrip() + "  dBm"


def render_meters(rows: list["AntennaRow"], current: CurrentReads, now: float, st: Style,
                  width: int, lo: float, hi: float) -> str:
    lines = []
    for r in rows:
        cur = current.value(r.antenna, now)
        bar = render_meter_bar(meter_cells(r.connected, cur, r.rssi_max, width, lo, hi), r.connected, st)
        cur_s = f"{cur:6.1f}" if cur is not None else "     -"
        peak_s = f"{r.rssi_max:6.1f}" if r.rssi_max is not None else "     -"
        state = {True: "", False: "  " + st.bad("DISCONNECTED"), None: "  " + st.dim("status unknown")}[r.connected]
        lines.append(f"  Ant {r.antenna:<3} {bar}  now {cur_s}  peak {peak_s}{state}")
    # The bar starts after "  Ant NNN " (plus "[" in ASCII mode).
    lines.append(render_scale(width, lo, hi, indent=10 + (0 if st.color else 1)))
    return "\n".join(lines)


def _pad(text: str, visible: str, width: int) -> str:
    """Pad *text* (which may contain colour codes) to *width* visible characters."""
    return text + " " * max(0, width - len(visible))


def render_reader_info(status: dict[str, Any], caps: dict[str, Any], st: Style) -> str:
    def v(x: Any) -> str:
        return "-" if x in (None, "") else str(x)

    conn = st.good("connected") if status.get("connected") else st.bad(status.get("connection_state", "disconnected"))
    impinj = status.get("impinj_extensions")
    lines = [
        st.bold("Reader"),
        (f"  Address          {status.get('host')}:{status.get('port')}  ({conn})" if status.get("host")
         else f"  Address          not available yet: {status.get('address_problem') or '-'}  ({conn})"),
        f"  Manufacturer     {v(status.get('manufacturer'))}",
        f"  Model            {v(status.get('model'))}",
        f"  Serial number    {v(status.get('serial_number'))}",
        f"  Firmware         {v(status.get('firmware_version'))}",
        f"  Impinj ext.      {'yes' if impinj else 'no' if impinj is False else 'not available'}",
        f"  Antennas         {v(caps.get('max_antennas'))} supported, "
        f"connected: {', '.join(map(str, caps.get('connected_antennas') or [])) or '-'}",
        f"  Mode             {v(status.get('mode'))}"
        + ("  [timing inventory RUNNING]" if status.get("inventorying") else ""),
    ]
    if status.get("test_mode_active"):
        lines.append(st.warn("  Antenna test mode is already active"))
    if status.get("last_error"):
        lines.append(st.dim(f"  Last error       {status['last_error'].get('message')}"))
    return "\n".join(lines)


def render_table(rows: list[AntennaRow], st: Style) -> str:
    header = f"  {'Antenna':<9}{'Connection':<16}{'Tag seen':<11}{'Reads':>7}{'Tags':>6}{'Peak RSSI':>11}  Last read (UTC)"
    out = [st.bold(header), "  " + "-" * (len(header) + 6)]
    for r in rows:
        if r.connected is True:
            conn_v, conn = "connected", st.good("connected")
        elif r.connected is False:
            conn_v, conn = "DISCONNECTED", st.bad("DISCONNECTED")
        else:
            conn_v, conn = "unknown", st.dim("unknown")
        seen_v = "YES" if r.tag_seen else "no"
        seen = st.good(seen_v) if r.tag_seen else st.warn(seen_v)
        rssi = f"{r.rssi_max:.1f} dBm" if r.rssi_max is not None else "-"
        last = r.last_read_at[11:23] if r.last_read_at else "-"
        out.append(
            f"  {r.antenna:<9}{_pad(conn, conn_v, 16)}{_pad(seen, seen_v, 11)}"
            f"{r.reads:>7}{r.unique_tags:>6}{rssi:>11}  {last}"
        )
    if not rows:
        out.append(st.warn("  The reader did not report any antennas."))
    return "\n".join(out)


def render_recent(reads: deque[dict[str, Any]], st: Style) -> str:
    if not reads:
        return st.dim("  (no tag reads yet - move a tag past each antenna)")
    lines = []
    for r in reads:
        rssi = f"{r['rssi']:.1f}" if r.get("rssi") is not None else "-"
        ts = (r.get("timestamp") or "")[11:23]
        lines.append(f"  {ts}  ant {r.get('antenna')!s:<3} {r.get('epc')}  {rssi} dBm")
    return "\n".join(lines)


# --------------------------------------------------------------------------- app


def confirm(question: str, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        return False
    try:
        return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def run(args: argparse.Namespace, client: MiddlewareClient, out=sys.stdout) -> int:
    tty = out.isatty() if hasattr(out, "isatty") else False
    st = Style(color=tty and not args.no_color)

    status = client.reader_status()
    caps = client.capabilities()
    print(render_reader_info(status, caps, st), file=out)
    print(file=out)

    if not status.get("connected"):
        if not confirm("The reader is not connected. Try to connect now?", args.yes):
            print("Cannot run antenna test mode without a reader connection.", file=out)
            return 1
        print("Connecting...", file=out)
        status = client.connect()
        caps = client.capabilities()

    started_by_us = False
    if status.get("test_mode_active"):
        print(st.warn("Test mode is already running; monitoring the existing test session."), file=out)
    else:
        if status.get("inventorying"):
            print(st.warn("WARNING: timing inventory is running. Test mode pauses it (no timing events are "
                          "recorded during the test) and resumes it when the test stops."), file=out)
        if not confirm("Start antenna test mode?", args.yes):
            print("Cancelled.", file=out)
            return 0
        client.start_test()
        started_by_us = True

    recent: deque[dict[str, Any]] = deque(maxlen=args.recent)
    current = CurrentReads(args.hold)
    after = 0
    started = time.monotonic()
    iterations = 0
    try:
        while True:
            data = client.test_reports(after)
            now = time.monotonic()
            current.update(data["reports"], now)
            for r in data["reports"]:
                recent.append(r)
            after = data["next_after_sequence"]
            if iterations % max(1, int(1.25 / args.interval)) == 0:
                caps = client.capabilities()  # picks up antenna connect/disconnect events (~1 s)
            rows = antenna_rows(caps, data["summary"])
            seen = sum(1 for r in rows if r.tag_seen)
            screen = [
                st.bold("OTReader antenna test") + st.dim(f"   {args.url}   (Ctrl+C to stop)"),
                f"Test running for {int(time.monotonic() - started)} s — "
                f"{seen}/{len(rows)} antennas have seen a tag, "
                f"{data['summary'].get('unique_tags', 0)} unique tag(s), {data['summary'].get('total_reads', 0)} reads",
                "",
                st.bold("Peak meter") + st.dim("   bright bar = current read, ┃ = session peak, "
                                             "green track = antenna connected"),
                render_meters(rows, current, now, st, args.meter_width, args.rssi_min, args.rssi_max),
                "",
                render_table(rows, st),
                "",
                st.bold("Recent reads"),
                render_recent(recent, st),
            ]
            if not data.get("test_mode_active"):
                screen += ["", st.warn("Test mode is no longer active on the middleware.")]
            if tty:
                # Clear once, then redraw in place (cursor home + clear-to-end-of-line) to avoid flicker.
                prefix = "\033[2J\033[H" if iterations == 0 else "\033[H"
                out.write(prefix + "\n".join(line + "\033[K" for line in screen) + "\n\033[J")
            else:
                print("\n".join(screen), file=out)
                print(file=out)
            out.flush()
            iterations += 1
            if not data.get("test_mode_active") or (args.duration and time.monotonic() - started >= args.duration):
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(file=out)
    finally:
        if started_by_us:
            print("Stopping antenna test mode...", file=out)
            try:
                result = client.stop_test()
                resumed = result.get("reader", {}).get("inventorying")
                print("Test mode stopped; timing inventory " + ("resumed." if resumed else "is idle."), file=out)
            except ClientError as exc:
                if "not active" not in str(exc):
                    print(st.bad(f"Failed to stop test mode: {exc}"), file=out)
        else:
            print("Leaving the existing test session running (it was not started by this client).", file=out)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OTReader antenna test console client")
    ap.add_argument("--url", default=os.environ.get("OTREADER_URL", "http://127.0.0.1:8000"),
                    help="middleware base URL (env OTREADER_URL)")
    ap.add_argument("--api-key", default=os.environ.get("OTREADER_API_KEY"), help="API key (env OTREADER_API_KEY)")
    ap.add_argument("--interval", type=float, default=0.25, help="refresh interval in seconds")
    ap.add_argument("--hold", type=float, default=1.0,
                    help="seconds the current-read bar holds after the last read on an antenna")
    ap.add_argument("--rssi-min", type=float, default=-85.0, help="meter scale minimum (dBm)")
    ap.add_argument("--rssi-max", type=float, default=-25.0, help="meter scale maximum (dBm)")
    ap.add_argument("--meter-width", type=int, default=50, help="meter width in characters")
    ap.add_argument("--duration", type=float, default=0, help="stop automatically after N seconds (0 = until Ctrl+C)")
    ap.add_argument("--recent", type=int, default=8, help="number of recent reads to show")
    ap.add_argument("--yes", "-y", action="store_true", help="answer yes to all prompts")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args(argv)
    if args.rssi_min >= args.rssi_max:
        ap.error("--rssi-min must be less than --rssi-max")
    if args.meter_width < 10:
        ap.error("--meter-width must be at least 10")
    client = MiddlewareClient(args.url, args.api_key)
    try:
        return run(args, client)
    except ClientError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
