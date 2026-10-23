from pyllrp import pyllrp as P

from app.llrp.capabilities import (
    Availability,
    apply_capabilities_response,
    apply_config_response,
    apply_impinj_capabilities,
    empty_capabilities,
    finalize_capabilities,
)
from app.llrp.simulator import SimulatedReader, SimulatorOptions
from app.llrp.transport import pack_message, unpack_message


def roundtrip(msg):
    return unpack_message(pack_message(msg))


def _caps_request(impinj_data=False):
    params = [P.ImpinjRequestedData_Parameter(RequestedData=1000)] if impinj_data else []
    return P.GET_READER_CAPABILITIES_Message(RequestedData=0, Parameters=params)


def test_parse_impinj_capabilities():
    sim = SimulatedReader(options=SimulatorOptions())
    sim.impinj_enabled = True
    resp = roundtrip(sim._capabilities(_caps_request(impinj_data=True)))
    caps = empty_capabilities()
    apply_capabilities_response(caps, resp)
    caps.impinj.extensions_enabled = True
    finalize_capabilities(caps)

    assert caps.manufacturer == "Impinj"
    assert caps.max_antennas == 4
    assert caps.firmware_version == "7.5.0.240"
    assert caps.model_name == "Speedway R420 (simulated)"
    assert caps.serial_number == "SIM-0001"
    assert caps.transmit_power_levels[0].dbm == 10.0
    assert caps.transmit_power_levels[-1].dbm == 32.5
    assert {m.mode_id for m in caps.rf_modes} == {0, 1, 2, 3, 4}
    m2 = next(m for m in caps.rf_modes if m.mode_id == 2)
    assert m2.m_factor == 4 and m2.spectral_mask == "DI"
    assert caps.regulatory["hop_tables"][0]["hop_table_id"] == 1
    assert caps.llrp["max_rospecs"] == 1
    assert caps.field_status["serial_number"] == Availability.AVAILABLE
    assert caps.field_status["impinj_extensions"] == Availability.AVAILABLE


def test_generic_reader_reports_unsupported_fields():
    sim = SimulatedReader(options=SimulatorOptions(impinj=False))
    resp = roundtrip(sim._capabilities(_caps_request()))
    caps = empty_capabilities()
    apply_capabilities_response(caps, resp)
    caps.impinj.extensions_enabled = False
    finalize_capabilities(caps)
    assert caps.manufacturer == "IANA PEN 99999"
    assert caps.model_name == "model code 2001002"
    assert caps.serial_number is None
    assert caps.field_status["serial_number"] == Availability.UNSUPPORTED
    assert caps.field_status["impinj_extensions"] == Availability.UNSUPPORTED
    assert caps.field_status["impinj_detailed_version"] == Availability.UNSUPPORTED


def test_missing_sections_marked_unavailable():
    resp = roundtrip(P.GET_READER_CAPABILITIES_RESPONSE_Message(MessageID=1, Parameters=[P.LLRPStatus_Parameter(StatusCode=0)]))
    caps = empty_capabilities()
    apply_capabilities_response(caps, resp)
    finalize_capabilities(caps)
    assert caps.field_status["manufacturer"] == Availability.UNAVAILABLE
    assert caps.field_status["rf_modes"] == Availability.UNAVAILABLE
    assert caps.field_status["transmit_power_levels"] == Availability.UNAVAILABLE
    assert caps.errors


def test_config_response_connected_antennas_and_current_config():
    sim = SimulatedReader(options=SimulatorOptions(connected_antennas=[1, 3]))
    sim.impinj_enabled = True
    req = P.GET_READER_CONFIG_Message(RequestedData=0, Parameters=[P.ImpinjRequestedData_Parameter(RequestedData=2000)])
    resp = roundtrip(sim._reader_config(req))
    caps = empty_capabilities()
    apply_config_response(caps, resp)
    assert caps.connected_antennas == [1, 3]
    assert [a.connected for a in caps.antennas] == [True, False, True, False]
    assert caps.reader_id == "001625123456"
    assert caps.impinj.temperature_c == 41
    assert caps.current_configuration["antenna_configuration"][0]["transmit_power_index"] == 91
    assert caps.field_status["connected_antennas"] == Availability.AVAILABLE


def test_impinj_detailed_version_only():
    resp = roundtrip(
        P.GET_READER_CAPABILITIES_RESPONSE_Message(
            MessageID=1,
            Parameters=[
                P.LLRPStatus_Parameter(StatusCode=0),
                P.ImpinjDetailedVersion_Parameter(ModelName="R700", SerialNumber="37020000", SoftwareVersion="8.0",
                                                  FirmwareVersion="8.0", FPGAVersion="", PCBAVersion=""),
            ],
        )
    )
    caps = empty_capabilities()
    apply_impinj_capabilities(caps, resp)
    assert caps.model_name == "R700" and caps.serial_number == "37020000"
