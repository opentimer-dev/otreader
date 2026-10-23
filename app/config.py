"""Configuration loading and validation.

The YAML file is validated with Pydantic at startup.  Invalid settings raise
:class:`ConfigError` with a human-readable list of problems, e.g.::

    Invalid configuration in config.yaml:
      - reader.port: Input should be less than or equal to 65535
"""

from __future__ import annotations

import ipaddress
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


class ConfigError(Exception):
    """Raised when the configuration file is missing or invalid."""


class OperatingMode(StrEnum):
    START_LINE = "start_line"
    FINISH_LINE = "finish_line"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ImpinjSearchMode(StrEnum):
    """Impinj inventory search modes defined in the Impinj LLRP 1.18 extensions
    (as shipped with pyllrp).  See README "Operating modes" for semantics."""

    READER_SELECTED = "Reader_Selected"
    SINGLE_TARGET = "Single_Target"
    DUAL_TARGET = "Dual_Target"
    SINGLE_TARGET_WITH_SUPPRESSION = "Single_Target_With_Suppression"


class InventoryProfileConfig(_Strict):
    """Per-mode reader inventory settings.

    ``rf_mode`` may be ``"auto"`` (select from the reader's advertised
    C1G2UHFRFModeTable using ``rf_mode_strategy``), ``"reader_default"`` (send no
    C1G2RFControl so the reader uses its own default), or an explicit integer
    ModeIdentifier from the reader's mode table.
    """

    session: int = Field(ge=0, le=3)
    tag_population: int = Field(ge=1, le=65535)
    tag_transit_time_ms: int = Field(default=0, ge=0)
    impinj_search_mode: ImpinjSearchMode
    rf_mode: Literal["auto", "reader_default"] | int = "auto"
    rf_mode_strategy: Literal["throughput", "robust"] = "robust"
    transmit_power_dbm: float | None = Field(default=None, description="None = reader default / max")
    receive_sensitivity_db: float | None = None
    enable_impinj_peak_rssi: bool = True
    enable_impinj_phase_angle: bool = False
    enable_impinj_doppler: bool = False


def _default_start_profile() -> InventoryProfileConfig:
    return InventoryProfileConfig(
        session=2,
        tag_population=256,
        tag_transit_time_ms=0,
        impinj_search_mode=ImpinjSearchMode.SINGLE_TARGET,
        rf_mode="auto",
        rf_mode_strategy="robust",
        enable_impinj_peak_rssi=True,
    )


def _default_finish_profile() -> InventoryProfileConfig:
    return InventoryProfileConfig(
        session=0,
        tag_population=32,
        tag_transit_time_ms=0,
        impinj_search_mode=ImpinjSearchMode.DUAL_TARGET,
        rf_mode="auto",
        rf_mode_strategy="throughput",
        enable_impinj_peak_rssi=True,
        enable_impinj_phase_angle=True,
        enable_impinj_doppler=True,
    )


def _default_test_profile() -> InventoryProfileConfig:
    return InventoryProfileConfig(
        session=0,
        tag_population=32,
        impinj_search_mode=ImpinjSearchMode.DUAL_TARGET,
        rf_mode="auto",
        rf_mode_strategy="throughput",
        enable_impinj_peak_rssi=True,
        enable_impinj_phase_angle=True,
        enable_impinj_doppler=True,
    )


class ReaderSSHConfig(_Strict):
    """SSH (Impinj RShell) access to the reader, used to set its clock."""

    username: str = "root"
    password: str = "impinj"
    port: int = Field(default=22, ge=1, le=65535)
    timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    legacy_host_keys: bool = Field(
        default=True, description="Fall back to ssh-rsa/ssh-dss host keys (OpenSSH HostKeyAlgorithms=+ssh-rsa)."
    )


class TimeSyncConfig(_Strict):
    enabled: bool = Field(default=True, description="Set the reader clock over SSH/RShell when the application starts.")
    tolerance_ms: float = Field(default=20.0, gt=0, le=1000)
    max_attempts: int = Field(default=3, ge=1, le=10)
    processing_estimate_ms: float = Field(
        default=60.0, ge=0, le=2000, description="Initial estimate of RShell's delay before the clock is set."
    )
    require_host_clock_synchronized: bool = Field(
        default=True,
        description="Only set the reader if this host's clock is NTP/GPS-synchronised (protects a Raspberry Pi "
        "without a time source from pushing a wrong time to the reader).",
    )


class ReaderConfig(_Strict):
    host_file: str | None = Field(
        default="/var/run/reader",
        description="File containing the reader's IP address (re-read every host_file_poll_seconds until valid). "
        "When set, reader.host is ignored.",
    )
    host_file_poll_seconds: float = Field(default=5.0, gt=0, le=3600)
    host: str | None = Field(
        default=None, min_length=1, description="Reader IP address or hostname; used only when host_file is null."
    )
    port: int = Field(default=5084, ge=1, le=65535)
    reconnect_interval: float = Field(default=5.0, gt=0, description="Initial reconnect delay (s).")
    reconnect_max_interval: float = Field(default=60.0, gt=0, description="Backoff ceiling (s).")
    connection_timeout: float = Field(default=10.0, gt=0, description="TCP connect / response timeout (s).")
    keepalive_interval_ms: int = Field(default=5000, ge=0, description="LLRP KEEPALIVE period; 0 disables.")
    keepalive_missed_limit: int = Field(default=3, ge=1)
    auto_connect: bool = True
    antennas: list[int] | None = Field(default=None, description="Antenna IDs to use; null = all (ID 0).")
    impinj_extensions: Literal["auto", "enabled", "disabled"] = "auto"
    reset_to_factory_default: bool = False
    delete_existing_rospecs: bool = True
    ssh: ReaderSSHConfig = Field(default_factory=ReaderSSHConfig)
    time_sync: TimeSyncConfig = Field(default_factory=TimeSyncConfig)
    start_line: InventoryProfileConfig = Field(default_factory=_default_start_profile)
    finish_line: InventoryProfileConfig = Field(default_factory=_default_finish_profile)
    test: InventoryProfileConfig = Field(default_factory=_default_test_profile)

    @field_validator("antennas")
    @classmethod
    def _check_antennas(cls, v: list[int] | None) -> list[int] | None:
        if v is None:
            return v
        if not v:
            raise ValueError("must be null (all antennas) or a non-empty list")
        if any(a < 1 or a > 65535 for a in v):
            raise ValueError("antenna IDs must be between 1 and 65535")
        if len(set(v)) != len(v):
            raise ValueError("antenna IDs must be unique")
        return sorted(v)

    @model_validator(mode="after")
    def _check_address_source(self) -> "ReaderConfig":
        if not self.host_file and not self.host:
            raise ValueError("set reader.host_file (file containing the reader IP) or reader.host")
        return self

    @model_validator(mode="after")
    def _check_backoff(self) -> "ReaderConfig":
        if self.reconnect_max_interval < self.reconnect_interval:
            raise ValueError("reconnect_max_interval must be >= reconnect_interval")
        return self


class ApplicationConfig(_Strict):
    mode: OperatingMode = OperatingMode.FINISH_LINE
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file: str | None = Field(default="otreader.log", description="Relative to data_directory; null = console only.")
    log_rotation: Literal["internal", "external"] = Field(
        default="internal",
        description="internal = RotatingFileHandler; external = WatchedFileHandler for logrotate.",
    )
    log_max_bytes: int = Field(default=10 * 1024 * 1024, ge=1024)
    log_backup_count: int = Field(default=10, ge=0)
    data_directory: str = "./data"
    persistence_interval: float = Field(default=5.0, gt=0, le=3600)
    auto_start_inventory: bool = Field(default=False, description="Start inventory on first boot once connected.")
    resume_inventory_on_restart: bool = Field(
        default=True, description="Restore the persisted 'inventory running' state after a restart/crash."
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper(cls, v: Any) -> Any:
        return v.upper() if isinstance(v, str) else v


class TimingConfig(_Strict):
    cooldown_seconds: float = Field(default=300.0, ge=0)
    finish_estimation_window_seconds: float = Field(
        default=2.0, gt=0, le=60, description="Quiet period after the last read before a pass is finalised."
    )
    finish_max_pass_seconds: float = Field(
        default=30.0, gt=0, description="Force-finalise a pass that lasts longer than this."
    )
    finish_max_observations_per_pass: int = Field(default=5000, ge=10)
    finish_cluster_gap_seconds: float = Field(default=1.0, gt=0)
    finish_top_window_db: float = Field(default=10.0, gt=0, le=40)
    finish_min_fit_points: int = Field(default=5, ge=3)
    finish_peak_tie_db: float = Field(default=3.0, ge=0)
    finish_time_offset_ms: float = Field(
        default=0.0, description="Calibration: constant added to every finish-line estimate."
    )
    clock_mode: Literal["auto", "reader", "reader_corrected", "host"] = "auto"
    max_reader_clock_offset_seconds: float = Field(
        default=2.0, gt=0, description="In 'auto' clock mode, correct reader UTC if it differs from host by more."
    )


class CallbackConfig(_Strict):
    url: str | None = None
    timeout_seconds: float = Field(default=3.0, gt=0, le=120)
    max_retries: int = Field(default=3, ge=0, le=1000)
    retry_delay_seconds: float = Field(default=1.0, ge=0)
    retry_max_delay_seconds: float = Field(default=60.0, ge=0)
    queue_max_size: int = Field(default=1000, ge=1)
    concurrency: int = Field(default=1, ge=1, le=16)
    max_response_bytes: int = Field(default=64 * 1024, ge=0)
    secret: str | None = Field(default=None, description="If set, requests carry an HMAC-SHA256 signature header.")
    allow_loopback: bool = True
    allow_private_networks: bool = True
    allow_public_networks: bool = True
    allowed_networks: list[str] | None = Field(
        default=None, description="Optional CIDR allow-list; when set, callback hosts must resolve inside it."
    )

    @field_validator("allowed_networks")
    @classmethod
    def _check_networks(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        for net in v:
            try:
                ipaddress.ip_network(net, strict=False)
            except ValueError as exc:
                raise ValueError(f"invalid CIDR network {net!r}: {exc}") from exc
        return v


class APIConfig(_Strict):
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    api_key: str | None = Field(default=None, description="Optional; when set, required via X-API-Key or Bearer.")
    cors_origins: list[str] = Field(default_factory=list)
    max_request_bytes: int = Field(default=64 * 1024, ge=1024)
    reader_operation_timeout_seconds: float = Field(default=60.0, gt=0)


class TestModeConfig(_Strict):
    __test__ = False  # not a pytest test class
    buffer_size: int = Field(default=5000, ge=10, le=1_000_000)


class AppConfig(_Strict):
    reader: ReaderConfig
    application: ApplicationConfig = Field(default_factory=ApplicationConfig)
    timing: TimingConfig = Field(default_factory=TimingConfig)
    callbacks: CallbackConfig = Field(default_factory=CallbackConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    test_mode: TestModeConfig = Field(default_factory=TestModeConfig)

    @property
    def data_path(self) -> Path:
        return Path(self.application.data_directory)

    def sanitized(self) -> dict[str, Any]:
        """Configuration safe to expose over the API (secrets masked)."""
        data = self.model_dump(mode="json")
        if data["api"].get("api_key"):
            data["api"]["api_key"] = "***"
        data["reader"]["ssh"]["password"] = "***"
        if data["callbacks"].get("secret"):
            data["callbacks"]["secret"] = "***"
        if data["callbacks"].get("url"):
            from app.callbacks.url_validation import redact_url

            data["callbacks"]["url"] = redact_url(data["callbacks"]["url"])
        return data


def _format_validation_error(exc: ValidationError) -> str:
    lines = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        lines.append(f"  - {loc}: {err['msg']}")
    return "\n".join(lines)


def parse_config(data: Any, source: str = "<config>") -> AppConfig:
    if data is None:
        raise ConfigError(f"Configuration {source} is empty")
    if not isinstance(data, dict):
        raise ConfigError(f"Configuration {source} must be a YAML mapping at the top level")
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration in {source}:\n{_format_validation_error(exc)}") from None


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found: {path}") from None
    except OSError as exc:
        raise ConfigError(f"Cannot read configuration file {path}: {exc}") from None
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Configuration file {path} is not valid YAML: {exc}") from None
    return parse_config(data, str(path))
