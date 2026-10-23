"""Tests for the antenna-test console client (against a stubbed middleware API)."""

import argparse
import io
import json

import httpx
import pytest

from clients.antenna_test import ClientError, MiddlewareClient, Style, antenna_rows, render_table, run

CAPS = {"max_antennas": 4, "connected_antennas": [1, 2],
        "antennas": [{"antenna_id": a, "connected": a in (1, 2)} for a in range(1, 5)]}


def status(**kw):
    base = {"connected": True, "connection_state": "idle", "host": "10.0.0.5", "port": 5084,
            "manufacturer": "Impinj", "model": "R420", "serial_number": "S1", "firmware_version": "7.5",
            "impinj_extensions": True, "mode": "finish_line", "inventorying": False, "test_mode_active": False,
            "last_error": None}
    return base | kw


def summary(*per):
    return {"total_reads": sum(p["reads"] for p in per), "unique_tags": len(per), "per_antenna": list(per)}


class FakeMiddleware:
    def __init__(self, st=None, fail_start=False):
        self.st = st or status()
        self.fail_start = fail_start
        self.calls = []
        self.polls = 0

    def __call__(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path
        self.calls.append((req.method, path))
        if path.endswith("/reader/status"):
            return httpx.Response(200, json=self.st)
        if path.endswith("/reader/capabilities"):
            return httpx.Response(200, json=CAPS)
        if path.endswith("/reader/connect"):
            self.st = self.st | {"connected": True}
            return httpx.Response(200, json=self.st)
        if path.endswith("/test/start"):
            if self.fail_start:
                return httpx.Response(503, json={"error": {"code": "reader_unavailable", "message": "reader gone"}})
            self.st = self.st | {"test_mode_active": True}
            return httpx.Response(200, json={"test_mode_active": True})
        if path.endswith("/test/stop"):
            self.st = self.st | {"test_mode_active": False}
            return httpx.Response(200, json={"test_mode_active": False, "reader": {"inventorying": True}})
        if path.endswith("/test/reports"):
            self.polls += 1
            reports = [{"sequence": self.polls, "epc": "3008AA", "antenna": 2, "rssi": -45.0,
                        "timestamp": "2026-09-30T13:30:15.123456Z"}]
            return httpx.Response(200, json={
                "test_mode_active": self.st["test_mode_active"], "reports": reports,
                "next_after_sequence": self.polls,
                "summary": summary({"antenna": 2, "reads": self.polls, "unique_tags": 1, "rssi_max": -45.0,
                                    "last_read_at": "2026-09-30T13:30:15.123456Z"}),
            })
        return httpx.Response(404, json={"error": {"code": "not_found", "message": path}})


def args(**kw):
    return argparse.Namespace(**({"url": "http://mw", "interval": 0.01, "duration": 0.05, "recent": 5, "yes": True,
                                  "no_color": True, "hold": 1.0, "rssi_min": -85.0, "rssi_max": -25.0,
                                  "meter_width": 30} | kw))


def client_for(fake):
    return MiddlewareClient("http://mw", transport=httpx.MockTransport(fake))


def test_rows_list_every_antenna_with_connection_and_seen_state():
    rows = antenna_rows(CAPS, summary({"antenna": 2, "reads": 3, "unique_tags": 1, "rssi_max": -40.0,
                                       "last_read_at": None}))
    assert [(r.antenna, r.connected, r.tag_seen) for r in rows] == [
        (1, True, False), (2, True, True), (3, False, False), (4, False, False)]
    assert rows[1].reads == 3


def test_rows_without_connection_info_and_unexpected_antenna():
    rows = antenna_rows({"max_antennas": 2, "antennas": []},
                        summary({"antenna": 5, "reads": 1, "unique_tags": 1, "rssi_max": None, "last_read_at": None}))
    assert [(r.antenna, r.connected, r.tag_seen) for r in rows] == [(1, None, False), (2, None, False), (5, None, True)]


def test_render_table_plain():
    text = render_table(antenna_rows(CAPS, summary()), Style(color=False))
    assert "DISCONNECTED" in text and "connected" in text and "no" in text


def test_run_starts_monitors_and_stops_test_mode():
    fake = FakeMiddleware()
    out = io.StringIO()
    assert run(args(), client_for(fake), out) == 0
    text = out.getvalue()
    assert "Manufacturer     Impinj" in text and "Serial number    S1" in text
    assert "1/4 antennas have seen a tag" in text
    assert "3008AA" in text
    assert ("POST", "/api/v1/test/start") in fake.calls and ("POST", "/api/v1/test/stop") in fake.calls
    assert "timing inventory resumed" in text


def test_run_warns_when_timing_inventory_running():
    out = io.StringIO()
    run(args(), client_for(FakeMiddleware(status(inventorying=True))), out)
    assert "WARNING: timing inventory is running" in out.getvalue()


def test_existing_test_session_is_monitored_but_not_stopped():
    fake = FakeMiddleware(status(test_mode_active=True))
    out = io.StringIO()
    run(args(), client_for(fake), out)
    assert ("POST", "/api/v1/test/start") not in fake.calls
    assert ("POST", "/api/v1/test/stop") not in fake.calls
    assert "Leaving the existing test session running" in out.getvalue()


def test_disconnected_reader_declined_returns_error():
    fake = FakeMiddleware(status(connected=False, connection_state="reconnect_wait"))
    out = io.StringIO()
    assert run(args(yes=False), client_for(fake), out) == 1
    assert ("POST", "/api/v1/test/start") not in fake.calls


def test_disconnected_reader_connects_with_yes():
    fake = FakeMiddleware(status(connected=False))
    run(args(), client_for(fake), io.StringIO())
    assert ("POST", "/api/v1/reader/connect") in fake.calls


def test_api_errors_are_reported_readably():
    fake = FakeMiddleware(fail_start=True)
    with pytest.raises(ClientError, match="reader gone \\(reader_unavailable, HTTP 503\\)"):
        run(args(), client_for(fake), io.StringIO())


def test_unreachable_middleware():
    def boom(req):
        raise httpx.ConnectError("refused")

    with pytest.raises(ClientError, match="cannot reach middleware"):
        client_for(boom).reader_status()


def test_api_key_header_sent():
    seen = {}

    def handler(req):
        seen["key"] = req.headers.get("x-api-key")
        return httpx.Response(200, json=json.loads(json.dumps(status())))

    MiddlewareClient("http://mw", api_key="k", transport=httpx.MockTransport(handler)).reader_status()
    assert seen["key"] == "k"


# --------------------------------------------------------------------------- peak meter

from clients.antenna_test import (  # noqa: E402
    BRIGHT_GREEN_FG, DARK_GREEN_BG, GREY_BG, CurrentReads, meter_cells, render_meter_bar, render_meters,
    render_scale,
)


def kinds(cells):
    return "".join({"track": "-", "off": ".", "fill": "#", "peak": "|"}[k] for k, _ in cells)


def test_meter_connected_track_current_bar_and_peak_mark():
    # Scale -80..-30 over 10 cells: 5 dB per cell.
    cells = meter_cells(True, current=-55.0, peak=-40.0, width=10, lo=-80, hi=-30)
    assert kinds(cells) == "#####---|-"


def test_meter_disconnected_antenna_has_no_green_track():
    assert kinds(meter_cells(False, None, None, 10, -80, -30)) == ".........."
    assert kinds(meter_cells(None, None, None, 10, -80, -30)) == ".........."


def test_meter_partial_cell_and_clamping():
    cells = meter_cells(True, current=-77.5, peak=None, width=10, lo=-80, hi=-30)  # half a cell
    assert cells[0] == ("fill", "▌")
    assert kinds(meter_cells(True, -10.0, -10.0, 10, -80, -30)) == "#########|"  # above scale: full, mark at end
    assert kinds(meter_cells(True, -95.0, -95.0, 10, -80, -30)) == "|---------"  # below scale: mark at start
    assert meter_cells(True, -80.0, None, 10, -80, -30)[0] == ("fill", "▏")  # still visible at minimum


def test_meter_peak_only_after_signal_decays():
    assert kinds(meter_cells(True, None, -60.0, 10, -80, -30)) == "----|-----"


def test_meter_colours():
    st = Style(color=True)
    cells = meter_cells(True, -55.0, -40.0, 10, -80, -30)
    bar = render_meter_bar(cells, True, st)
    assert f"\033[{BRIGHT_GREEN_FG};{DARK_GREEN_BG}m█" in bar  # bright current bar on dark-green track
    assert f"\033[1;{BRIGHT_GREEN_FG};{DARK_GREEN_BG}m┃" in bar  # bright peak mark
    assert f"\033[{DARK_GREEN_BG}m " in bar  # dark-green connected track
    off = render_meter_bar(meter_cells(False, None, None, 10, -80, -30), False, st)
    assert DARK_GREEN_BG not in off and GREY_BG in off


def test_current_reads_hold_then_decay():
    cur = CurrentReads(hold_s=1.0)
    cur.update([{"antenna": 1, "rssi": -60.0}, {"antenna": 1, "rssi": -52.5}, {"antenna": 2, "rssi": None}], now=10.0)
    assert cur.value(1, 10.5) == -52.5  # latest read wins
    assert cur.value(2, 10.5) is None
    assert cur.value(1, 11.1) is None  # decayed after the hold time


def test_scale_labels_align_with_meter_positions():
    line = render_scale(10, -80, -30, indent=0)
    assert line.index("-80") == 0 and line.index("-40") == 8


def test_render_meters_rows():
    rows = antenna_rows(CAPS, summary({"antenna": 1, "reads": 4, "unique_tags": 1, "rssi_max": -40.0,
                                       "last_read_at": None}))
    cur = CurrentReads(1.0)
    cur.update([{"antenna": 1, "rssi": -55.0}], now=0.0)
    text = render_meters(rows, cur, 0.1, Style(color=False), 10, -80, -30)
    lines = text.splitlines()
    assert lines[0].startswith("  Ant 1   [#####---|-]  now  -55.0  peak  -40.0")
    assert "DISCONNECTED" in lines[2]
    # Scale label for -80 sits directly under the first meter cell.
    assert lines[-1].index("-80") == lines[0].index("[") + 1


def test_live_screen_includes_meter():
    out = io.StringIO()
    run(args(), client_for(FakeMiddleware()), out)
    assert "Peak meter" in out.getvalue() and "Ant 2" in out.getvalue()


def test_reader_info_while_waiting_for_address():
    from clients.antenna_test import render_reader_info

    text = render_reader_info(status(connected=False, connection_state="waiting_for_address", host=None,
                                     address_problem="/var/run/reader does not exist yet"), CAPS, Style(color=False))
    assert "not available yet: /var/run/reader does not exist yet" in text and "None:" not in text
