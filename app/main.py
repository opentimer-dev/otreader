"""Application entry point.

    python -m app.main --config config.yaml
"""

from __future__ import annotations

import argparse
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import __version__
from app.api.routes import health_router, router, web_router
from app.config import AppConfig, ConfigError, load_config
from app.llrp.client import ReaderOperationError
from app.persistence.repository import NotFoundError
from app.services import Services

log = logging.getLogger("app")

DESCRIPTION = """
Middleware between an LLRP RFID reader and race timing software.

* **Reader** - connection management, capability discovery, start/stop, operating mode.
* **Tags** - the active inventory with per-tag cooldown state.
* **Events** - the durable, append-only timing event log; filtering, export, rewind/replay.
* **Callbacks** - HTTP POST delivery of each accepted event with retries and a durable queue.
* **Test mode** - antenna verification without creating timing events.

All timestamps are ISO 8601 UTC (`Z`).  Timing event timestamps are *estimates*; see
`estimation_method`, `confidence` and `timestamp_source` on each event.

If `api.api_key` is configured, send it as `X-API-Key: <key>` or `Authorization: Bearer <key>`.
`/health` and `/ready` never require a key.
"""


def _error(status: int, code: str, message: str, details: Any = None) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message, "details": details}}, status_code=status)


class BodySizeLimitMiddleware:
    """Rejects request bodies larger than ``max_bytes`` (413), including chunked uploads."""

    def __init__(self, app: Any, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    if int(value) > self.max_bytes:
                        await _error(413, "request_too_large", f"request body exceeds {self.max_bytes} bytes")(
                            scope, receive, send
                        )
                        return
                except ValueError:
                    pass
        received = 0

        async def limited_receive() -> dict:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise HTTPException(413, f"request body exceeds {self.max_bytes} bytes")
            return message

        await self.app(scope, limited_receive, send)


def create_app(config: AppConfig, services: Services | None = None) -> FastAPI:
    svc = services or Services(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await svc.start()
        try:
            yield
        finally:
            await svc.stop()

    app = FastAPI(
        title="OTReader - LLRP Race Timing Middleware",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        openapi_tags=[
            {"name": "reader", "description": "Reader connection, capabilities and control"},
            {"name": "tags", "description": "Active tag inventory and cooldown"},
            {"name": "events", "description": "Timing event log, export, rewind/replay"},
            {"name": "callbacks", "description": "HTTP callback delivery"},
            {"name": "test mode", "description": "Antenna verification without timing"},
            {"name": "health", "description": "Health, readiness and status"},
        ],
    )
    app.state.services = svc

    @app.exception_handler(ReaderOperationError)
    async def _reader_error(_: Request, exc: ReaderOperationError) -> JSONResponse:
        return _error(exc.http_status, exc.code, exc.message, exc.details)

    @app.exception_handler(NotFoundError)
    async def _not_found(_: Request, exc: NotFoundError) -> JSONResponse:
        return _error(404, "not_found", str(exc.args[0]) if exc.args else "not found")

    @app.exception_handler(HTTPException)
    async def _http(_: Request, exc: HTTPException) -> JSONResponse:
        codes = {400: "bad_request", 401: "unauthorized", 404: "not_found", 409: "conflict", 413: "request_too_large",
                 422: "validation_error"}
        resp = _error(exc.status_code, codes.get(exc.status_code, "error"), str(exc.detail))
        if exc.headers:
            resp.headers.update(exc.headers)
        return resp

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"location": ".".join(str(p) for p in e.get("loc", ())), "message": e.get("msg")} for e in exc.errors()
        ]
        return _error(422, "validation_error", "request validation failed", errors)

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("Unhandled API error")
        return _error(500, "internal_error", "internal server error")

    if config.api.cors_origins:
        app.add_middleware(
            CORSMiddleware, allow_origins=config.api.cors_origins, allow_methods=["*"], allow_headers=["*"]
        )
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=config.api.max_request_bytes)
    app.include_router(web_router)
    app.include_router(health_router)
    app.include_router(router)
    return app


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="OTReader LLRP race timing middleware")
    ap.add_argument("--config", "-c", default="config.yaml", help="path to YAML configuration")
    ap.add_argument("--check-config", action="store_true", help="validate the configuration and exit")
    args = ap.parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    if args.check_config:
        print(f"Configuration {args.config} is valid.")
        return

    from app.logging_setup import setup_logging, shutdown_logging

    data_dir = Path(config.application.data_directory)
    data_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(config.application, data_dir)
    log.info("OTReader %s starting; reader %s:%d; mode %s", __version__,
             f"address from {config.reader.host_file}" if config.reader.host_file else config.reader.host,
             config.reader.port,
             config.application.mode.value)

    import uvicorn

    app = create_app(config)
    try:
        uvicorn.run(
            app,
            host=config.api.host,
            port=config.api.port,
            log_config=None,
            access_log=config.application.log_level == "DEBUG",
            timeout_graceful_shutdown=20,
        )
    finally:
        shutdown_logging()


if __name__ == "__main__":
    main()
