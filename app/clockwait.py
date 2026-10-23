"""Wait (bounded) for this host's clock to be NTP/GPS-synchronised.

Used by the systemd unit before starting OTReader, because the reader clock is set
from the host clock once at start-up and is skipped if the host is unsynchronised.
On a Raspberry Pi that boots at the race site, NTP typically syncs some seconds after
boot.  Waiting a bounded time (never forever: without a time source timing must still
start) lets the start-up reader clock sync succeed.

    python -m app.clockwait --timeout 60

Exit status is always 0 so the service starts regardless; the outcome is printed
(to the journal under systemd).
"""

from __future__ import annotations

import argparse
import sys
import time

from app.clock import host_clock_status


def wait_for_sync(timeout: float, interval: float = 2.0, check=host_clock_status, sleep=time.sleep,
                  monotonic=time.monotonic) -> dict:
    deadline = monotonic() + timeout
    while True:
        st = check()
        if st.get("synchronized") is not False:  # True, or None = cannot tell (non-Linux)
            return st
        if monotonic() >= deadline:
            return st
        sleep(interval)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--timeout", type=float, default=60.0, help="maximum seconds to wait (default 60)")
    args = ap.parse_args(argv)
    start = time.monotonic()
    st = wait_for_sync(args.timeout)
    waited = time.monotonic() - start
    if st.get("synchronized") is True:
        print(f"Host clock synchronised (estimated error {st.get('estimated_error_ms')} ms) after {waited:.0f}s")
    elif st.get("synchronized") is None:
        print("Host clock synchronisation status unknown; starting")
    else:
        print(f"Host clock still NOT synchronised after {waited:.0f}s; starting anyway. The reader clock "
              "will not be set at start-up; use 'Set reader clock' once the host has a time source.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
