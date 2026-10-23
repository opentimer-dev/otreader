"""Logging configuration.

Log records are handed to a :class:`logging.handlers.QueueListener` so that file
I/O (including rotation) never blocks the asyncio event loop that processes RFID
reports.  Two rotation strategies are supported:

* ``internal`` - :class:`RotatingFileHandler` rotates by size.
* ``external`` - :class:`WatchedFileHandler` re-opens the file when an external
  tool such as ``logrotate`` moves it, so rotation never interrupts operation.
"""

from __future__ import annotations

import logging
import logging.handlers
import queue
import sys
from pathlib import Path

from app.config import ApplicationConfig

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-8s [%(name)s] %(message)s"
_DATEFMT = "%Y-%m-%dT%H:%M:%S"

_listener: logging.handlers.QueueListener | None = None


def setup_logging(cfg: ApplicationConfig, data_dir: Path) -> None:
    global _listener
    shutdown_logging()

    formatter = logging.Formatter(_FORMAT, _DATEFMT)
    formatter.converter = __import__("time").gmtime  # log timestamps in UTC
    handlers: list[logging.Handler] = []

    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(formatter)
    handlers.append(console)

    if cfg.log_file:
        log_path = Path(cfg.log_file)
        if not log_path.is_absolute():
            log_path = data_dir / log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if cfg.log_rotation == "internal":
            fh: logging.Handler = logging.handlers.RotatingFileHandler(
                log_path, maxBytes=cfg.log_max_bytes, backupCount=cfg.log_backup_count, encoding="utf-8"
            )
        else:
            fh = logging.handlers.WatchedFileHandler(log_path, encoding="utf-8")
        fh.setFormatter(formatter)
        handlers.append(fh)

    q: queue.Queue[logging.LogRecord] = queue.Queue(-1)
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(logging.handlers.QueueHandler(q))
    root.setLevel(cfg.log_level)
    # uvicorn installs its own handlers unless told otherwise; route through ours.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncssh").setLevel(logging.WARNING)

    _listener = logging.handlers.QueueListener(q, *handlers, respect_handler_level=True)
    _listener.start()


def shutdown_logging() -> None:
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None
