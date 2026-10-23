from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from app.clock import ManualClock
from app.config import AppConfig, parse_config
from app.llrp.simulator import SimulatedReader, SimulatorOptions


def make_config(tmp_path: Path, port: int = 5084, **sections: dict[str, Any]) -> AppConfig:
    data: dict[str, Any] = {
        "reader": {"host": "127.0.0.1", "host_file": None, "port": port, "reconnect_interval": 0.2, "reconnect_max_interval": 1.0,
                   "connection_timeout": 3, "keepalive_interval_ms": 0,
                   "time_sync": {"enabled": False}},  # never SSH to localhost from tests
        "application": {"data_directory": str(tmp_path / "data"), "log_file": None, "persistence_interval": 0.2},
        "timing": {"finish_estimation_window_seconds": 0.3, "cooldown_seconds": 300},
        "callbacks": {"retry_delay_seconds": 0.01, "retry_max_delay_seconds": 0.05},
    }
    for key, value in sections.items():
        data.setdefault(key, {}).update(value)
    return parse_config(data)


async def wait_for(predicate: Callable[[], Any], timeout: float = 5.0, interval: float = 0.02) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(interval)


@pytest.fixture
def manual_clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
async def sim():
    reader = SimulatedReader(options=SimulatorOptions())
    await reader.start()
    yield reader
    await reader.stop()


@pytest.fixture
async def generic_sim():
    reader = SimulatedReader(options=SimulatorOptions(impinj=False))
    await reader.start()
    yield reader
    await reader.stop()
