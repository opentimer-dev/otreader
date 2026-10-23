"""Reader capability discovery.

Parses pyllrp ``GET_READER_CAPABILITIES_RESPONSE`` / ``GET_READER_CONFIG_RESPONSE``
messages into a :class:`ReaderCapabilities` model.

Every discoverable item has an entry in ``field_status`` with one of:

* ``available``    - the reader reported it.
* ``unsupported``  - the reader (or protocol) does not provide it, e.g. serial
                     number on a reader without vendor extensions.
* ``unavailable``  - it should have been retrievable but the query failed.
* ``not_queried``  - discovery has not been performed (e.g. never connected).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field
from pyllrp import llrpdef
from pyllrp import pyllrp as P

IMPINJ_PEN = llrpdef.vendors.get("Impinj", 25882)


class Availability(StrEnum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    UNAVAILABLE = "unavailable"
    NOT_QUERIED = "not_queried"


class RFModeInfo(BaseModel):
    mode_id: int
    dr_value: str
    m_value: str
    m_factor: int  # 1 (FM0), 2, 4, 8
    forward_link_modulation: str
    spectral_mask: str
    bdr_bps: int
    pie: int
    min_tari_ns: int
    max_tari_ns: int
    step_tari_ns: int
    epc_hag_t_and_c_conformance: bool

    @property
    def link_rate(self) -> float:
        """Approximate effective reverse-link data rate (bps)."""
        return self.bdr_bps / self.m_factor


class PowerLevel(BaseModel):
    index: int
    dbm: float


class SensitivityLevel(BaseModel):
    index: int
    value_db: int = Field(description="As reported by the reader (vendor-defined reference).")


class AntennaInfo(BaseModel):
    antenna_id: int
    connected: bool | None = None
    gain_dbi_x100: int | None = None


class ImpinjInfo(BaseModel):
    extensions_enabled: bool | None = None
    extensions_error: str | None = None
    model_name: str | None = None
    serial_number: str | None = None
    software_version: str | None = None
    firmware_version: str | None = None
    fpga_version: str | None = None
    pcba_version: str | None = None
    temperature_c: int | None = None
    frequency_list_khz: list[int] | None = None
    search_mode_support: str = (
        "Impinj readers do not advertise supported search modes over LLRP; support is "
        "determined by attempting configuration (see applied_configuration)."
    )


class ReaderCapabilities(BaseModel):
    manufacturer_pen: int | None = None
    manufacturer: str | None = None
    model_code: int | None = None
    model_name: str | None = None
    firmware_version: str | None = None
    serial_number: str | None = None
    reader_id: str | None = None
    max_antennas: int | None = None
    antennas: list[AntennaInfo] = Field(default_factory=list)
    connected_antennas: list[int] | None = None
    can_set_antenna_properties: bool | None = None
    has_utc_clock: bool | None = None
    transmit_power_levels: list[PowerLevel] = Field(default_factory=list)
    receive_sensitivities: list[SensitivityLevel] = Field(default_factory=list)
    gpio: dict[str, int] | None = None
    regulatory: dict[str, Any] | None = None
    rf_modes: list[RFModeInfo] = Field(default_factory=list)
    llrp: dict[str, Any] | None = None
    c1g2: dict[str, Any] | None = None
    current_configuration: dict[str, Any] | None = None
    impinj: ImpinjInfo = Field(default_factory=ImpinjInfo)
    field_status: dict[str, Availability] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    discovered_at: str | None = None

    @property
    def impinj_extensions(self) -> bool:
        return bool(self.impinj.extensions_enabled)

    def hop_table_id(self) -> int:
        tables = (self.regulatory or {}).get("hop_tables") or []
        return int(tables[0]["hop_table_id"]) if tables else 1


_M_FACTOR = {0: 1, 1: 2, 2: 4, 3: 8}

_FIELDS = (
    "manufacturer",
    "model",
    "firmware_version",
    "serial_number",
    "max_antennas",
    "connected_antennas",
    "transmit_power_levels",
    "receive_sensitivities",
    "regulatory",
    "rf_modes",
    "llrp_capabilities",
    "impinj_extensions",
    "impinj_detailed_version",
    "current_configuration",
)


def empty_capabilities() -> ReaderCapabilities:
    caps = ReaderCapabilities()
    caps.field_status = {f: Availability.NOT_QUERIED for f in _FIELDS}
    return caps


def _enum_name(enum: Any, value: Any) -> str:
    try:
        return enum.getName(value)
    except Exception:
        return str(value)


def parse_rf_mode(entry: Any) -> RFModeInfo:
    return RFModeInfo(
        mode_id=entry.ModeIdentifier,
        dr_value=_enum_name(P.C1G2DRValue, int(entry.DRValue)),
        m_value=_enum_name(P.C1G2MValue, entry.MValue),
        m_factor=_M_FACTOR.get(entry.MValue, 1),
        forward_link_modulation=_enum_name(P.C1G2ForwardLinkModulation, entry.ForwardLinkModulation),
        spectral_mask=_enum_name(P.C1G2SpectralMaskIndicator, entry.SpectralMaskIndicator),
        bdr_bps=entry.BDRValue,
        pie=entry.PIEValue,
        min_tari_ns=entry.MinTariValue,
        max_tari_ns=entry.MaxTariValue,
        step_tari_ns=entry.StepTariValue,
        epc_hag_t_and_c_conformance=bool(entry.EPCHAGTCConformance),
    )


def apply_capabilities_response(caps: ReaderCapabilities, msg: Any) -> None:
    """Populate *caps* from a GET_READER_CAPABILITIES_RESPONSE."""
    fs = caps.field_status
    gdc = msg.getFirstParameterByClass(P.GeneralDeviceCapabilities_Parameter)
    if gdc is not None:
        caps.manufacturer_pen = gdc.DeviceManufacturerName
        caps.manufacturer = P.getVendorName(gdc.DeviceManufacturerName)
        if caps.manufacturer == str(gdc.DeviceManufacturerName):
            caps.manufacturer = f"IANA PEN {gdc.DeviceManufacturerName}"
        caps.model_code = gdc.ModelName
        caps.firmware_version = gdc.ReaderFirmwareVersion or None
        caps.max_antennas = gdc.MaxNumberOfAntennaSupported
        caps.can_set_antenna_properties = bool(gdc.CanSetAntennaProperties)
        caps.has_utc_clock = bool(gdc.HasUTCClockCapability)
        caps.receive_sensitivities = [
            SensitivityLevel(index=e.Index, value_db=e.ReceiveSensitivityValue)
            for e in gdc.getAllParametersByClass(P.ReceiveSensitivityTableEntry_Parameter)
        ]
        gpio = gdc.getFirstParameterByClass(P.GPIOCapabilities_Parameter)
        if gpio is not None:
            caps.gpio = {"num_gpis": gpio.NumGPIs, "num_gpos": gpio.NumGPOs}
        fs["manufacturer"] = Availability.AVAILABLE
        fs["model"] = Availability.AVAILABLE
        fs["firmware_version"] = Availability.AVAILABLE if caps.firmware_version else Availability.UNSUPPORTED
        fs["max_antennas"] = Availability.AVAILABLE
        fs["receive_sensitivities"] = (
            Availability.AVAILABLE if caps.receive_sensitivities else Availability.UNSUPPORTED
        )
    else:
        for f in ("manufacturer", "model", "firmware_version", "max_antennas", "receive_sensitivities"):
            fs[f] = Availability.UNAVAILABLE
        caps.errors.append("GeneralDeviceCapabilities missing from capabilities response")

    llrpc = msg.getFirstParameterByClass(P.LLRPCapabilities_Parameter)
    if llrpc is not None:
        caps.llrp = {
            "can_do_rf_survey": bool(llrpc.CanDoRFSurvey),
            "can_report_buffer_fill_warning": bool(llrpc.CanReportBufferFillWarning),
            "supports_client_request_opspec": bool(llrpc.SupportsClientRequestOpSpec),
            "can_do_tag_inventory_state_aware_singulation": bool(llrpc.CanDoTagInventoryStateAwareSingulation),
            "supports_event_and_report_holding": bool(llrpc.SupportsEventAndReportHolding),
            "max_priority_levels": llrpc.MaxNumPriorityLevelsSupported,
            "max_rospecs": llrpc.MaxNumROSpecs,
            "max_specs_per_rospec": llrpc.MaxNumSpecsPerROSpec,
            "max_inventory_parameter_specs_per_aispec": llrpc.MaxNumInventoryParameterSpecsPerAISpec,
            "max_access_specs": llrpc.MaxNumAccessSpecs,
        }
        fs["llrp_capabilities"] = Availability.AVAILABLE
    else:
        fs["llrp_capabilities"] = Availability.UNAVAILABLE

    reg = msg.getFirstParameterByClass(P.RegulatoryCapabilities_Parameter)
    if reg is not None:
        regulatory: dict[str, Any] = {
            "country_code": reg.CountryCode,
            "communications_standard": _enum_name(P.CommunicationsStandard, reg.CommunicationsStandard),
        }
        uhf = reg.getFirstParameterByClass(P.UHFBandCapabilities_Parameter)
        if uhf is not None:
            caps.transmit_power_levels = [
                PowerLevel(index=e.Index, dbm=e.TransmitPowerValue / 100.0)
                for e in uhf.getAllParametersByClass(P.TransmitPowerLevelTableEntry_Parameter)
            ]
            fi = uhf.getFirstParameterByClass(P.FrequencyInformation_Parameter)
            if fi is not None:
                regulatory["hopping"] = bool(fi.Hopping)
                regulatory["hop_tables"] = [
                    {"hop_table_id": t.HopTableID, "frequencies_khz": list(t.Frequency)}
                    for t in fi.getAllParametersByClass(P.FrequencyHopTable_Parameter)
                ]
                fft = fi.getFirstParameterByClass(P.FixedFrequencyTable_Parameter)
                regulatory["fixed_frequencies_khz"] = list(fft.Frequency) if fft is not None else []
            caps.rf_modes = [
                parse_rf_mode(e) for e in uhf.getAllParametersByClass(P.C1G2UHFRFModeTableEntry_Parameter)
            ]
        caps.regulatory = regulatory
        fs["regulatory"] = Availability.AVAILABLE
    else:
        fs["regulatory"] = Availability.UNAVAILABLE
    fs["transmit_power_levels"] = Availability.AVAILABLE if caps.transmit_power_levels else Availability.UNAVAILABLE
    fs["rf_modes"] = Availability.AVAILABLE if caps.rf_modes else Availability.UNAVAILABLE

    c1g2 = msg.getFirstParameterByClass(P.C1G2LLRPCapabilities_Parameter)
    if c1g2 is not None:
        caps.c1g2 = {
            "can_support_block_erase": bool(c1g2.CanSupportBlockErase),
            "can_support_block_write": bool(c1g2.CanSupportBlockWrite),
            "max_select_filters_per_query": c1g2.MaxNumSelectFiltersPerQuery,
        }

    apply_impinj_capabilities(caps, msg)


def apply_impinj_capabilities(caps: ReaderCapabilities, msg: Any) -> None:
    dv = msg.getFirstParameterByClass(P.ImpinjDetailedVersion_Parameter)
    if dv is not None:
        caps.impinj.model_name = dv.ModelName or None
        caps.impinj.serial_number = dv.SerialNumber or None
        caps.impinj.software_version = dv.SoftwareVersion or None
        caps.impinj.firmware_version = dv.FirmwareVersion or None
        caps.impinj.fpga_version = dv.FPGAVersion or None
        caps.impinj.pcba_version = dv.PCBAVersion or None
        if caps.impinj.model_name:
            caps.model_name = caps.impinj.model_name
        if caps.impinj.serial_number:
            caps.serial_number = caps.impinj.serial_number
            caps.field_status["serial_number"] = Availability.AVAILABLE
        if caps.impinj.software_version and not caps.firmware_version:
            caps.firmware_version = caps.impinj.software_version
        caps.field_status["impinj_detailed_version"] = Availability.AVAILABLE
    fc = msg.getFirstParameterByClass(P.ImpinjFrequencyCapabilities_Parameter)
    if fc is not None:
        caps.impinj.frequency_list_khz = list(fc.FrequencyList)


def apply_config_response(caps: ReaderCapabilities, msg: Any) -> None:
    """Populate antenna connection state and a summary of the current reader config."""
    props = list(msg.getAllParametersByClass(P.AntennaProperties_Parameter))
    if props:
        caps.antennas = [
            AntennaInfo(antenna_id=p.AntennaID, connected=bool(p.AntennaConnected), gain_dbi_x100=p.AntennaGain)
            for p in props
        ]
        caps.connected_antennas = sorted(p.AntennaID for p in props if p.AntennaConnected)
        caps.field_status["connected_antennas"] = Availability.AVAILABLE
    else:
        caps.field_status["connected_antennas"] = Availability.UNSUPPORTED

    ident = msg.getFirstParameterByClass(P.Identification_Parameter)
    if ident is not None and ident.ReaderID:
        caps.reader_id = "".join(f"{b:02X}" for b in ident.ReaderID)

    current: dict[str, Any] = {}
    ka = msg.getFirstParameterByClass(P.KeepaliveSpec_Parameter)
    if ka is not None:
        current["keepalive"] = {
            "trigger": _enum_name(P.KeepaliveTriggerType, ka.KeepaliveTriggerType),
            "period_ms": ka.PeriodicTriggerValue,
        }
    ants = []
    for ac in msg.getAllParametersByClass(P.AntennaConfiguration_Parameter):
        entry: dict[str, Any] = {"antenna_id": ac.AntennaID}
        rx = ac.getFirstParameterByClass(P.RFReceiver_Parameter)
        tx = ac.getFirstParameterByClass(P.RFTransmitter_Parameter)
        if rx is not None:
            entry["receiver_sensitivity_index"] = rx.ReceiverSensitivity
        if tx is not None:
            entry["transmit_power_index"] = tx.TransmitPower
            entry["hop_table_id"] = tx.HopTableID
        rfc = ac.getFirstParameterByClass(P.C1G2RFControl_Parameter)
        if rfc is not None:
            entry["rf_mode_index"] = rfc.ModeIndex
        sc = ac.getFirstParameterByClass(P.C1G2SingulationControl_Parameter)
        if sc is not None:
            entry["session"] = sc.Session
            entry["tag_population"] = sc.TagPopulation
        sm = ac.getFirstParameterByClass(P.ImpinjInventorySearchMode_Parameter)
        if sm is not None:
            entry["impinj_search_mode"] = _enum_name(P.ImpinjInventorySearchType, sm.InventorySearchMode)
        ants.append(entry)
    if ants:
        current["antenna_configuration"] = ants
    er = msg.getFirstParameterByClass(P.EventsAndReports_Parameter)
    if er is not None:
        current["hold_events_and_reports_upon_reconnect"] = bool(er.HoldEventsAndReportsUponReconnect)
    temp = msg.getFirstParameterByClass(P.ImpinjReaderTemperature_Parameter)
    if temp is not None:
        caps.impinj.temperature_c = temp.Temperature
    caps.current_configuration = current
    caps.field_status["current_configuration"] = Availability.AVAILABLE


def finalize_capabilities(caps: ReaderCapabilities) -> None:
    fs = caps.field_status
    if fs.get("serial_number") != Availability.AVAILABLE:
        # Standard LLRP has no serial-number field; only vendor extensions provide it.
        fs["serial_number"] = Availability.UNSUPPORTED
    if caps.impinj.extensions_enabled is None:
        fs["impinj_extensions"] = Availability.UNSUPPORTED
    elif caps.impinj.extensions_enabled:
        fs["impinj_extensions"] = Availability.AVAILABLE
        fs.setdefault("impinj_detailed_version", Availability.UNAVAILABLE)
        if fs.get("impinj_detailed_version") == Availability.NOT_QUERIED:
            fs["impinj_detailed_version"] = Availability.UNAVAILABLE
    else:
        fs["impinj_extensions"] = Availability.UNSUPPORTED
        fs["impinj_detailed_version"] = Availability.UNSUPPORTED
    if caps.model_name is None and caps.model_code is not None:
        caps.model_name = f"model code {caps.model_code}"
