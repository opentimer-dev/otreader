"""Impinj RShell over SSH: set the reader's clock at application start.

Impinj readers expose a management shell ("RShell") over SSH.  The command
``config system time YYYY.MM.DD-HH:mm:ss`` sets the reader's UTC clock with
whole-second resolution only, so the command is sent so that it takes effect as
close as possible to the start of the named second:

1. Open the session (see *Legacy SSH* below) and wait for the `` > `` prompt.
2. Measure the round-trip time (RTT) of an empty command a few times (minimum).
3. Pick a target second S and send the command at
   ``S - RTT/2 - processing_estimate``, spinning for the final milliseconds.
4. RShell sets the clock at the end of its processing, just before it replies,
   so the achieved error is estimated from the reply time:
   ``reader - host ≈ S - (reply_received - RTT/2)``.
   (On a Speedway R420 this estimate matched LLRP-measured offsets within ~4 ms.)
5. If the estimated error exceeds the tolerance, repeat using the observed
   processing time (it varies between ~50 and ~100 ms on an R420), up to
   ``max_attempts``.

The achieved offset is also measured independently from the LLRP connection
timestamp once the reader connects (``/reader/status`` → ``clock``).

Legacy SSH: older Impinj firmware runs OpenSSH 4.x offering only ``ssh-rsa`` /
``ssh-dss`` host keys, which modern clients refuse.  We first try modern host-key
algorithms only and fall back to allowing ``ssh-rsa``/``ssh-dss`` (the equivalent of
OpenSSH ``HostKeyAlgorithms=+ssh-rsa``).  paramiko >= 5 can no longer negotiate
``ssh-rsa`` at all, hence asyncssh.

Host keys are trusted on first use and pinned in ``<data_dir>/reader_host_keys.json``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import warnings
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message=".*Diffie-Hellman over finite fields.*")  # legacy readers use DH-GEX

import asyncssh  # noqa: E402

log = logging.getLogger(__name__)

MODERN_HOST_KEY_ALGS = [
    "ssh-ed25519", "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521",
    "rsa-sha2-512", "rsa-sha2-256",
]
LEGACY_HOST_KEY_ALGS = ["ssh-rsa", "ssh-dss"]
_STATUS_RE = re.compile(r"Status='(\d+),([^']*)'")
_legacy_hosts: set[tuple[str, int]] = set()  # hosts known to need the legacy fallback


class RShellError(Exception):
    """kind: unreachable | auth | host_key_mismatch | protocol | command | timeout"""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


@dataclass
class TimeSyncResult:
    success: bool
    host: str
    started_at: str
    attempts: list[dict[str, Any]] = field(default_factory=list)
    command: str | None = None
    estimated_offset_ms: float | None = None  # reader - host right after setting (from reply timing)
    rtt_ms: float | None = None
    host_key_type: str | None = None
    legacy_ssh: bool | None = None
    error_kind: str | None = None
    error: str | None = None
    measured_offset_ms: float | None = None  # filled in from the LLRP connection timestamp
    host_clock: dict[str, Any] | None = None  # this host's own sync status / estimated error

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ------------------------------------------------------------------------ host keys


class HostKeyStore:
    """Trust-on-first-use pinning of reader SSH host keys (JSON file)."""

    def __init__(self, path: Path | None) -> None:
        self.path = path

    def _load(self) -> dict[str, str]:
        if self.path is None or not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (OSError, ValueError):
            log.warning("Ignoring unreadable host key file %s", self.path)
            return {}

    def get(self, host: str, port: int) -> str | None:
        return self._load().get(f"{host}:{port}")

    def put(self, host: str, port: int, key_openssh: str) -> None:
        if self.path is None:
            return
        data = self._load()
        data[f"{host}:{port}"] = key_openssh
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        tmp.replace(self.path)


# ------------------------------------------------------------------------ session


class RShellSession:
    def __init__(self, conn: asyncssh.SSHClientConnection, proc: Any, legacy: bool) -> None:
        self.conn = conn
        self.proc = proc
        self.legacy = legacy
        key = conn.get_server_host_key()
        self.host_key_type = key.get_algorithm() if key is not None else None

    async def read_prompt(self, timeout: float) -> str:
        buf = ""
        deadline = asyncio.get_running_loop().time() + timeout
        while not buf.rstrip().endswith(">"):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise RShellError("timeout", f"no RShell prompt within {timeout}s (got {buf[-120:]!r})")
            chunk = await asyncio.wait_for(self.proc.stdout.read(4096), remaining)
            if not chunk:
                raise RShellError("protocol", f"RShell session closed (got {buf[-120:]!r})")
            buf += chunk
        return buf

    def send_line(self, line: str) -> None:
        self.proc.stdin.write(line + "\r")

    async def command(self, line: str, timeout: float = 10.0) -> tuple[int | None, str, str]:
        """Run a command; returns (status code, status text, full output)."""
        self.send_line(line)
        out = await self.read_prompt(timeout)
        m = _STATUS_RE.search(out)
        return (int(m.group(1)), m.group(2), out) if m else (None, "", out)

    async def measure_rtt(self, samples: int, timeout: float, now: Callable[[], float]) -> float:
        best = math.inf
        for _ in range(samples):
            t = now()
            self.send_line("")
            await self.read_prompt(timeout)
            best = min(best, now() - t)
        return best

    async def close(self) -> None:
        try:
            self.proc.close()
        except Exception:
            pass
        self.conn.close()
        try:
            await asyncio.wait_for(self.conn.wait_closed(), 3)
        except Exception:
            pass


async def open_rshell(
    host: str, port: int, username: str, password: str, timeout: float, legacy_fallback: bool,
    host_keys: HostKeyStore,
) -> RShellSession:
    pinned = host_keys.get(host, port)
    known_hosts: Any = None
    if pinned:
        known_hosts = ([asyncssh.import_public_key(pinned)], [], [])

    attempts = [False, True] if legacy_fallback else [False]
    pinned_legacy = bool(pinned) and pinned.split()[0] in LEGACY_HOST_KEY_ALGS
    if legacy_fallback and ((host, port) in _legacy_hosts or pinned_legacy):
        attempts = [True]  # known legacy server: skip the modern attempt it will drop
    last: Exception | None = None
    for legacy in attempts:
        algs = MODERN_HOST_KEY_ALGS + (LEGACY_HOST_KEY_ALGS if legacy else [])
        try:
            conn = await asyncio.wait_for(
                asyncssh.connect(
                    host, port=port, username=username, password=password, known_hosts=known_hosts,
                    server_host_key_algs=algs, client_keys=None, agent_path=None,
                    preferred_auth="password,keyboard-interactive", connect_timeout=timeout,
                ),
                timeout,
            )
        except asyncssh.PermissionDenied:
            raise RShellError("auth", f"SSH login to {host} as {username!r} was rejected; check reader.ssh "
                                      "username/password") from None
        except asyncssh.HostKeyNotVerifiable:
            raise RShellError(
                "host_key_mismatch",
                f"SSH host key of {host} does not match the pinned key. If the reader was replaced, delete its "
                f"entry from {host_keys.path}.",
            ) from None
        except asyncssh.Error as exc:
            # Typically host-key / key-exchange negotiation failure against a legacy server
            # (OpenSSH 4.x drops the connection when no host-key algorithm matches).
            last = exc
            log.debug("SSH to %s failed with %s host-key algorithms: %s", host, "legacy" if legacy else "modern", exc)
            continue
        except TimeoutError:  # (asyncio.TimeoutError is TimeoutError; must precede OSError)
            raise RShellError("timeout", f"SSH connection to {host}:{port} timed out after {timeout}s") from None
        except OSError as exc:
            raise RShellError("unreachable", f"cannot reach SSH on {host}:{port}: {exc}") from None
        if legacy and (host, port) not in _legacy_hosts:
            _legacy_hosts.add((host, port))
            if len(attempts) > 1:
                log.info("SSH to %s required legacy host-key algorithms (ssh-rsa/ssh-dss)", host)
        key = conn.get_server_host_key()
        if key is not None and not pinned:
            host_keys.put(host, port, key.export_public_key("openssh").decode().strip())
            log.info("Pinned SSH host key for %s (%s)", host, key.get_algorithm())
        try:
            proc = await conn.create_process(term_type="vt100", term_size=(200, 50))
            session = RShellSession(conn, proc, legacy)
            await session.read_prompt(timeout)
        except Exception:
            conn.close()
            raise
        return session
    raise RShellError("protocol", f"SSH negotiation with {host}:{port} failed: {last!r}")


# ------------------------------------------------------------------------ time sync


def format_rshell_time(epoch_second: int) -> str:
    return time.strftime("%Y.%m.%d-%H:%M:%S", time.gmtime(epoch_second))


async def _wait_until(target: float, now: Callable[[], float]) -> None:
    """Sleep until *target* (epoch seconds), spinning for the final ~3 ms for precision."""
    while True:
        remaining = target - now()
        if remaining <= 0:
            return
        if remaining > 0.004:
            await asyncio.sleep(remaining - 0.003)
        else:
            while now() < target:
                pass
            return


async def set_time_on_session(
    session: RShellSession,
    *,
    tolerance_ms: float,
    max_attempts: int,
    processing_estimate_ms: float,
    timeout: float,
    now: Callable[[], float] = time.time,
) -> tuple[bool, list[dict[str, Any]], float | None, float]:
    """Returns (within_tolerance, attempts, final estimated offset ms, rtt ms)."""
    rtt = await session.measure_rtt(5, timeout, now)
    processing = processing_estimate_ms / 1000.0
    attempts: list[dict[str, Any]] = []
    final: float | None = None
    for i in range(max_attempts):
        lead = rtt / 2 + processing
        target = math.ceil(now() + lead + 0.3)  # leave time to schedule precisely
        cmd = "config system time " + format_rshell_time(target)
        await _wait_until(target - lead, now)
        sent = now()
        session.send_line(cmd)
        out = await session.read_prompt(timeout)
        replied = now()
        m = _STATUS_RE.search(out)
        if not m or m.group(1) != "0":
            status = f"{m.group(1)},{m.group(2)}" if m else out.strip()[-120:]
            raise RShellError("command", f"reader rejected {cmd!r}: {status}")
        observed_processing = max(0.0, (replied - sent) - rtt)
        estimated = (target - (replied - rtt / 2)) * 1000.0  # reader - host, ms
        attempts.append({
            "command": cmd,
            "sent_vs_target_ms": round((sent - target) * 1000, 2),
            "reply_ms": round((replied - sent) * 1000, 1),
            "processing_ms": round(observed_processing * 1000, 1),
            "estimated_offset_ms": round(estimated, 1),
        })
        final = estimated
        log.info("Reader time set: %s (attempt %d, estimated reader-host %+.1f ms)", cmd, i + 1, estimated)
        if abs(estimated) <= tolerance_ms:
            return True, attempts, final, rtt * 1000
        processing = observed_processing
    return False, attempts, final, rtt * 1000


async def sync_reader_time(
    host: str,
    *,
    port: int = 22,
    username: str = "root",
    password: str = "impinj",
    timeout: float = 15.0,
    legacy_fallback: bool = True,
    host_keys: HostKeyStore | None = None,
    tolerance_ms: float = 20.0,
    max_attempts: int = 3,
    processing_estimate_ms: float = 60.0,
    now: Callable[[], float] = time.time,
    host_clock_check: Callable[[], dict[str, Any]] | None = None,
    require_host_clock_synchronized: bool = True,
) -> TimeSyncResult:
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now()))
    result = TimeSyncResult(success=False, host=host, started_at=started)
    if host_clock_check is not None:
        result.host_clock = host_clock_check()
        if require_host_clock_synchronized and result.host_clock.get("synchronized") is False:
            result.error_kind = "host_clock_unsynchronized"
            result.error = ("this host's clock is not synchronised (no NTP/GPS time source), so the reader was not "
                            "set; fix the host's time source, then set the reader clock from the web page or "
                            "POST /api/v1/reader/sync-time")
            log.warning("Reader time sync skipped: %s", result.error)
            return result
    session: RShellSession | None = None
    try:
        session = await open_rshell(host, port, username, password, timeout, legacy_fallback,
                                    host_keys or HostKeyStore(None))
        result.legacy_ssh = session.legacy
        result.host_key_type = session.host_key_type
        ok, attempts, final, rtt_ms = await set_time_on_session(
            session, tolerance_ms=tolerance_ms, max_attempts=max_attempts,
            processing_estimate_ms=processing_estimate_ms, timeout=timeout, now=now,
        )
        result.attempts = attempts
        result.command = attempts[-1]["command"] if attempts else None
        result.estimated_offset_ms = round(final, 1) if final is not None else None
        result.rtt_ms = round(rtt_ms, 2)
        # The clock was set in every case; "success" means it was set, and the
        # estimate tells how close it landed.
        result.success = bool(attempts)
        if not ok:
            log.warning("Reader time set, but estimated error %+.1f ms exceeds tolerance %.0f ms after %d attempts",
                        final, tolerance_ms, len(attempts))
    except RShellError as exc:
        result.error_kind, result.error = exc.kind, str(exc)
    except (asyncio.TimeoutError, TimeoutError):
        result.error_kind, result.error = "timeout", "reader time sync timed out"
    except Exception as exc:  # never let time sync break startup
        result.error_kind, result.error = "protocol", f"{type(exc).__name__}: {exc}"
    finally:
        if session is not None:
            await session.close()
    if result.error:
        log.warning("Reader time sync failed (%s): %s", result.error_kind, result.error)
    return result
