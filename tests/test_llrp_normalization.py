from pyllrp import pyllrp as P

from app.clock import ReaderClockMapper, TimestampSource
from app.llrp.events import llrp_status, normalize_epc, normalize_report, parse_reader_event, parse_timestamp_param
from app.llrp.transport import pack_message, unpack_message

HOST_US = 1_790_000_000_000_000


def report(*tags):
    msg = P.RO_ACCESS_REPORT_Message(MessageID=0, Parameters=[P.TagReportData_Parameter(Parameters=list(t)) for t in tags])
    return unpack_message(pack_message(msg))


def test_epc96_preserves_leading_zeros_and_prefers_impinj_rssi():
    msg = report(
        [
            P.EPC_96_Parameter(EPC=0x000000000000000000000ABC),
            P.ROSpecID_Parameter(ROSpecID=1102),
            P.AntennaID_Parameter(AntennaID=3),
            P.PeakRSSI_Parameter(PeakRSSI=-45),
            P.FirstSeenTimestampUTC_Parameter(Microseconds=HOST_US - 5000),
            P.TagSeenCount_Parameter(TagCount=1),
            P.ImpinjPeakRSSI_Parameter(RSSI=-4537),
            P.ImpinjRFPhaseAngle_Parameter(PhaseAngle=2048),
            P.ImpinjRFDopplerFrequency_Parameter(DopplerFrequency=-160),
        ]
    )
    obs, skipped = normalize_report(msg, ReaderClockMapper("reader"), HOST_US, 1.0)
    assert skipped == 0
    o = obs[0]
    assert o.epc == "000000000000000000000ABC"
    assert o.antenna == 3 and o.rospec_id == 1102
    assert o.rssi == -45.37 and o.rssi_source == "impinj_peak_rssi"
    assert o.phase_angle_deg == 180.0
    assert o.doppler_hz == -10.0
    assert o.timestamp_us == HOST_US - 5000
    assert o.timestamp_source == TimestampSource.READER_UTC


def test_epcdata_and_llrp_rssi():
    msg = report([P.EPCData_Parameter(EPC=bytes.fromhex("E28011700000020D1234ABCD5678")), P.PeakRSSI_Parameter(PeakRSSI=-60)])
    obs, _ = normalize_report(msg, ReaderClockMapper("reader"), HOST_US, 1.0)
    assert obs[0].epc == "E28011700000020D1234ABCD5678"
    assert obs[0].rssi == -60.0 and obs[0].rssi_source == "llrp_peak_rssi"
    # No reader timestamp -> host receive time, identified as such.
    assert obs[0].timestamp_us == HOST_US
    assert obs[0].timestamp_source == TimestampSource.HOST_RECEIVE


def test_aggregated_report_uses_midpoint():
    msg = report(
        [
            P.EPC_96_Parameter(EPC=1),
            P.FirstSeenTimestampUTC_Parameter(Microseconds=HOST_US - 100_000),
            P.LastSeenTimestampUTC_Parameter(Microseconds=HOST_US - 50_000),
            P.TagSeenCount_Parameter(TagCount=5),
        ]
    )
    o = normalize_report(msg, ReaderClockMapper("reader"), HOST_US, 1.0)[0][0]
    assert o.timestamp_us == HOST_US - 75_000
    assert o.timestamp_uncertainty_ms == 25.0


def test_uptime_timestamps_are_mapped_to_utc():
    mapper = ReaderClockMapper("auto")
    mapper.on_connect("uptime", 10_000_000, HOST_US)  # reader uptime 10 s at host time HOST_US
    msg = report([P.EPC_96_Parameter(EPC=1), P.FirstSeenTimestampUptime_Parameter(Microseconds=12_000_000)])
    o = normalize_report(msg, mapper, HOST_US + 2_001_000, 1.0)[0][0]
    assert o.timestamp_source == TimestampSource.READER_UPTIME_MAPPED
    assert o.timestamp_us == HOST_US + 2_000_000


def test_auto_clock_mode_corrects_unsynchronised_reader():
    mapper = ReaderClockMapper("auto", max_offset_s=2.0)
    reader_now = HOST_US - 3_600_000_000  # reader clock one hour behind
    mapper.on_connect("utc", reader_now, HOST_US)
    msg = report([P.EPC_96_Parameter(EPC=1), P.FirstSeenTimestampUTC_Parameter(Microseconds=reader_now + 1_000_000)])
    o = normalize_report(msg, mapper, HOST_US + 1_002_000, 1.0)[0][0]
    assert o.timestamp_source == TimestampSource.READER_UTC_CORRECTED
    assert abs(o.timestamp_us - (HOST_US + 1_000_000)) < 5_000
    assert mapper.status()["correction_active"] is True


def test_auto_clock_mode_trusts_synchronised_reader():
    mapper = ReaderClockMapper("auto", max_offset_s=2.0)
    mapper.on_connect("utc", HOST_US - 3000, HOST_US)
    msg = report([P.EPC_96_Parameter(EPC=1), P.FirstSeenTimestampUTC_Parameter(Microseconds=HOST_US + 500)])
    o = normalize_report(msg, mapper, HOST_US + 4000, 1.0)[0][0]
    assert o.timestamp_source == TimestampSource.READER_UTC
    assert o.timestamp_us == HOST_US + 500


def test_host_clock_mode_ignores_reader_time():
    msg = report([P.EPC_96_Parameter(EPC=1), P.FirstSeenTimestampUTC_Parameter(Microseconds=5)])
    o = normalize_report(msg, ReaderClockMapper("host"), HOST_US, 1.0)[0][0]
    assert o.timestamp_us == HOST_US and o.timestamp_source == TimestampSource.HOST_RECEIVE


def test_tag_without_epc_is_skipped():
    msg = report([P.EPC_96_Parameter(EPC=7)])
    msg.Parameters.append(P.TagReportData_Parameter(Parameters=[P.AntennaID_Parameter(AntennaID=1)]))
    obs, skipped = normalize_report(msg, ReaderClockMapper("reader"), HOST_US, 1.0)
    assert len(obs) == 1 and skipped == 1


def test_reader_event_parsing():
    msg = unpack_message(
        pack_message(
            P.READER_EVENT_NOTIFICATION_Message(
                MessageID=0,
                Parameters=[
                    P.ReaderEventNotificationData_Parameter(
                        Parameters=[
                            P.UTCTimestamp_Parameter(Microseconds=HOST_US),
                            P.ReaderExceptionEvent_Parameter(Message="overheat"),
                            P.AntennaEvent_Parameter(EventType=0, AntennaID=2),
                        ]
                    )
                ],
            )
        )
    )
    assert parse_timestamp_param(msg) == ("utc", HOST_US)
    events = parse_reader_event(msg)
    assert {"type": "reader_exception", "message": "overheat"} in events
    assert {"type": "antenna", "antenna": 2, "connected": False} in events


def test_llrp_status_details():
    ok, _ = llrp_status(P.ADD_ROSPEC_RESPONSE_Message(MessageID=1, Parameters=[P.LLRPStatus_Parameter(StatusCode=0)]))
    assert ok
    bad = P.ADD_ROSPEC_RESPONSE_Message(
        MessageID=1,
        Parameters=[
            P.LLRPStatus_Parameter(
                StatusCode=100, ErrorDescription="bad",
                Parameters=[P.ParameterError_Parameter(ParameterType=330, ErrorCode=207)],
            )
        ],
    )
    ok, detail = llrp_status(unpack_message(pack_message(bad)))
    assert not ok and "M_ParameterError" in detail and "bad" in detail and "330" in detail
    ok, detail = llrp_status(P.ERROR_MESSAGE_Message(MessageID=1, Parameters=[P.LLRPStatus_Parameter(StatusCode=109)]))
    assert not ok and "UnsupportedMessage" in detail


def test_unpack_normalises_pyllrp_bits_fields():
    msg = P.SET_READER_CONFIG_Message(
        Parameters=[
            P.AntennaConfiguration_Parameter(
                AntennaID=1,
                Parameters=[P.C1G2InventoryCommand_Parameter(Parameters=[
                    P.C1G2SingulationControl_Parameter(Session=2, TagPopulation=4, TagTransitTime=0)])],
            )
        ]
    )
    out = unpack_message(pack_message(msg))
    sc = out.getFirstParameterByClass(P.C1G2SingulationControl_Parameter)
    assert sc.Session == 2 and isinstance(sc.Session, int)
    pack_message(out)  # re-packs (validates) cleanly


def test_normalize_epc():
    assert normalize_epc(" 3008-33b2 ") == "300833B2"
    import pytest

    with pytest.raises(ValueError):
        normalize_epc("xyz")
