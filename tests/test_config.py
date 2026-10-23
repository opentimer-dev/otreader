from pathlib import Path

import pytest

from app.config import ConfigError, OperatingMode, load_config, parse_config

ROOT = Path(__file__).resolve().parent.parent


def test_minimal_config_uses_defaults():
    cfg = parse_config({"reader": {"host": "10.0.0.5"}})
    assert cfg.reader.port == 5084
    assert cfg.application.mode == OperatingMode.FINISH_LINE
    assert cfg.timing.cooldown_seconds == 300
    assert cfg.application.persistence_interval == 5
    assert cfg.callbacks.url is None
    assert cfg.reader.start_line.session == 2
    assert cfg.reader.finish_line.impinj_search_mode == "Dual_Target"


def test_example_config_file_is_valid():
    cfg = load_config(ROOT / "config.yaml")
    # config.yaml holds site-specific values (reader address, API port); only require that it validates.
    assert cfg.reader.host_file or cfg.reader.host


@pytest.mark.parametrize(
    "data, fragment",
    [
        ({}, "reader: Field required"),
        ({"reader": {"host": ""}}, "reader.host"),
        ({"reader": {"host": "h", "port": 70000}}, "reader.port"),
        ({"reader": {"host": "h", "bogus": 1}}, "reader.bogus"),
        ({"reader": {"host": "h"}, "application": {"mode": "middle"}}, "application.mode"),
        ({"reader": {"host": "h", "antennas": [0]}}, "antenna IDs"),
        ({"reader": {"host": "h", "antennas": [1, 1]}}, "unique"),
        ({"reader": {"host": "h", "reconnect_interval": 10, "reconnect_max_interval": 5}}, "reconnect_max_interval"),
        ({"reader": {"host": "h"}, "timing": {"cooldown_seconds": -1}}, "timing.cooldown_seconds"),
        ({"reader": {"host": "h"}, "callbacks": {"allowed_networks": ["nonsense"]}}, "invalid CIDR"),
        ({"reader": {"host": "h", "finish_line": {"session": 5, "tag_population": 1, "impinj_search_mode": "Dual_Target"}}},
         "session"),
    ],
)
def test_invalid_configs_give_meaningful_errors(data, fragment):
    with pytest.raises(ConfigError) as exc:
        parse_config(data)
    assert fragment in str(exc.value)


def test_log_level_is_case_insensitive():
    cfg = parse_config({"reader": {"host": "h"}, "application": {"log_level": "debug"}})
    assert cfg.application.log_level == "DEBUG"


def test_load_config_errors(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("reader: [unclosed")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)
    empty = tmp_path / "empty.yaml"
    empty.write_text("")
    with pytest.raises(ConfigError, match="empty"):
        load_config(empty)


def test_sanitized_masks_secrets():
    cfg = parse_config(
        {
            "reader": {"host": "h"},
            "api": {"api_key": "topsecret"},
            "callbacks": {"url": "http://user:pw@10.0.0.2/x", "secret": "s"},
        }
    )
    s = cfg.sanitized()
    assert s["api"]["api_key"] == "***"
    assert s["callbacks"]["secret"] == "***"
    assert "pw" not in s["callbacks"]["url"]


def test_rf_mode_accepts_int_or_keywords():
    cfg = parse_config({"reader": {"host": "h", "finish_line": {"session": 1, "tag_population": 4,
                                                                  "impinj_search_mode": "Dual_Target", "rf_mode": 1002}}})
    assert cfg.reader.finish_line.rf_mode == 1002


def test_reader_address_defaults_to_host_file():
    cfg = parse_config({"reader": {}})
    assert cfg.reader.host_file == "/var/run/reader" and cfg.reader.host is None
    assert cfg.reader.host_file_poll_seconds == 5.0


def test_reader_address_source_required():
    with pytest.raises(ConfigError, match="host_file"):
        parse_config({"reader": {"host_file": None}})
    cfg = parse_config({"reader": {"host_file": None, "host": "10.0.0.5"}})
    assert cfg.reader.host == "10.0.0.5"
