"""Normalisation of LLRP messages received from the reader.

Converts pyllrp ``RO_ACCESS_REPORT`` messages into :class:`Observation` objects and
``READER_EVENT_NOTIFICATION`` messages into simple event dictionaries.

Notes on pyllrp: its convenience ``RO_ACCESS_REPORT.getTagData()`` uses
``HexFormatToStr`` which strips leading zeros from EPCs and ignores Impinj
parameters, so we walk the parameters ourselves.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyllrp import pyllrp as P

from app.clock import MappedTime, ReaderClockMapper, TimestampSource
from app.models import Observation

log = logging.getLogger(__name__)

# Impinj phase angle is reported as 0..4095 over one full cycle.
_PHASE_SCALE = 360.0 / 4096.0
# Impinj Doppler is reported as a signed 16-bit value.  The Impinj Octane SDK
# divides by 16 to obtain Hz; we apply the same scaling.  Only the sign / zero
# crossing is used by the estimator, so the scale factor is not safety-critical.
_DOPPLER_SCALE = 1.0 / 16.0


def epc_to_hex(param: Any) -> str | None:
    if isinstance(param, P.EPC_96_Parameter):
        return f"{int(param.EPC):024X}"
    if isinstance(param, P.EPCData_Parameter):
        epc = param.EPC
        if isinstance(epc, (bytes, bytearray)):
            return epc.hex().upper()
        if isinstance(epc, int):
            return f"{epc:X}"
    return None


def normalize_epc(epc: str) -> str:
    """Canonical EPC form used as the inventory key: upper-case hex, no separators."""
    cleaned = epc.strip().replace(" ", "").replace("-", "").replace(":", "").upper()
    if not cleaned or any(c not in "0123456789ABCDEF" for c in cleaned):
        raise ValueError(f"invalid EPC {epc!r}: expected hexadecimal")
    return cleaned


@dataclass
class _TagFields:
    epc: str | None = None
    rospec_id: int | None = None
    antenna: int | None = None
    peak_rssi: int | None = None
    impinj_rssi: int | None = None
    channel: int | None = None
    first_utc: int | None = None
    first_uptime: int | None = None
    last_utc: int | None = None
    last_uptime: int | None = None
    seen_count: int | None = None
    phase: int | None = None
    doppler: int | None = None
    unknown: list[str] = field(default_factory=list)


def _extract(tag: Any) -> _TagFields:
    f = _TagFields()
    for p in tag.Parameters:
        if isinstance(p, (P.EPC_96_Parameter, P.EPCData_Parameter)):
            f.epc = epc_to_hex(p)
        elif isinstance(p, P.ROSpecID_Parameter):
            f.rospec_id = p.ROSpecID
        elif isinstance(p, P.AntennaID_Parameter):
            f.antenna = p.AntennaID
        elif isinstance(p, P.PeakRSSI_Parameter):
            f.peak_rssi = p.PeakRSSI
        elif isinstance(p, P.ChannelIndex_Parameter):
            f.channel = p.ChannelIndex
        elif isinstance(p, P.FirstSeenTimestampUTC_Parameter):
            f.first_utc = p.Microseconds
        elif isinstance(p, P.FirstSeenTimestampUptime_Parameter):
            f.first_uptime = p.Microseconds
        elif isinstance(p, P.LastSeenTimestampUTC_Parameter):
            f.last_utc = p.Microseconds
        elif isinstance(p, P.LastSeenTimestampUptime_Parameter):
            f.last_uptime = p.Microseconds
        elif isinstance(p, P.TagSeenCount_Parameter):
            f.seen_count = p.TagCount
        elif isinstance(p, P.ImpinjPeakRSSI_Parameter):
            f.impinj_rssi = p.RSSI
        elif isinstance(p, P.ImpinjRFPhaseAngle_Parameter):
            f.phase = p.PhaseAngle
        elif isinstance(p, P.ImpinjRFDopplerFrequency_Parameter):
            f.doppler = p.DopplerFrequency
        else:
            f.unknown.append(type(p).__name__)
    return f


def _map(mapper: ReaderClockMapper, utc: int | None, uptime: int | None, host_us: int) -> MappedTime | None:
    if utc is not None:
        return mapper.map_utc(utc, host_us)
    if uptime is not None:
        return mapper.map_uptime(uptime, host_us)
    return None


def normalize_report(
    message: Any, mapper: ReaderClockMapper, host_received_us: int, host_received_mono: float
) -> tuple[list[Observation], int]:
    """Return (observations, skipped_count) for an RO_ACCESS_REPORT message.

    Tag entries without an EPC are skipped (and counted) rather than raising.
    """
    observations: list[Observation] = []
    skipped = 0
    for tag in message.Parameters:
        if not isinstance(tag, P.TagReportData_Parameter):
            continue
        try:
            f = _extract(tag)
        except Exception:  # defensive: malformed parameter content
            log.warning("Skipping malformed TagReportData", exc_info=True)
            skipped += 1
            continue
        if not f.epc:
            skipped += 1
            continue

        first = _map(mapper, f.first_utc, f.first_uptime, host_received_us)
        last = _map(mapper, f.last_utc, f.last_uptime, host_received_us)
        if first is not None:
            if last is not None and f.seen_count is not None and f.seen_count > 1 and last.us > first.us:
                # Reader aggregated several reads into one report: the instant of the
                # reported peak RSSI is unknown within [first, last]; use the midpoint.
                ts = (first.us + last.us) // 2
                unc = (last.us - first.us) / 2000.0
            else:
                ts = first.us
                unc = first.uncertainty_ms
            source = first.source
        else:
            ts = host_received_us
            source = TimestampSource.HOST_RECEIVE
            unc = None

        if f.impinj_rssi is not None:
            rssi: float | None = f.impinj_rssi / 100.0
            rssi_source: str | None = "impinj_peak_rssi"
        elif f.peak_rssi is not None:
            rssi = float(f.peak_rssi)
            rssi_source = "llrp_peak_rssi"
        else:
            rssi, rssi_source = None, None

        observations.append(
            Observation(
                epc=f.epc,
                antenna=f.antenna,
                rssi=rssi,
                rssi_source=rssi_source,
                timestamp_us=ts,
                timestamp_source=source,
                first_seen_us=first.us if first else None,
                last_seen_us=last.us if last else None,
                seen_count=f.seen_count,
                channel_index=f.channel,
                phase_angle_deg=None if f.phase is None else round(f.phase * _PHASE_SCALE, 3),
                doppler_hz=None if f.doppler is None else f.doppler * _DOPPLER_SCALE,
                rospec_id=f.rospec_id,
                host_received_us=host_received_us,
                host_received_mono=host_received_mono,
                reader_first_seen_raw_us=f.first_utc if f.first_utc is not None else f.first_uptime,
                timestamp_uncertainty_ms=unc,
            )
        )
    return observations, skipped


def parse_timestamp_param(message: Any) -> tuple[str | None, int | None]:
    """Extract ('utc'|'uptime', microseconds) from a READER_EVENT_NOTIFICATION."""
    p = message.getFirstParameterByClass(P.UTCTimestamp_Parameter)
    if p is not None:
        return "utc", p.Microseconds
    p = message.getFirstParameterByClass(P.Uptime_Parameter)
    if p is not None:
        return "uptime", p.Microseconds
    return None, None


def parse_reader_event(message: Any) -> list[dict[str, Any]]:
    """Summarise a READER_EVENT_NOTIFICATION into a list of event dicts."""
    events: list[dict[str, Any]] = []
    data = message.getFirstParameterByClass(P.ReaderEventNotificationData_Parameter)
    if data is None:
        return events
    for p in data.Parameters:
        if isinstance(p, P.AntennaEvent_Parameter):
            events.append(
                {
                    "type": "antenna",
                    "antenna": p.AntennaID,
                    "connected": p.EventType == P.AntennaEventType.Antenna_Connected,
                }
            )
        elif isinstance(p, P.ConnectionAttemptEvent_Parameter):
            events.append(
                {
                    "type": "connection_attempt",
                    "status": P.ConnectionAttemptStatusType.getName(p.Status),
                    "success": p.Status == P.ConnectionAttemptStatusType.Success,
                }
            )
        elif isinstance(p, P.ConnectionCloseEvent_Parameter):
            events.append({"type": "connection_close"})
        elif isinstance(p, P.ReaderExceptionEvent_Parameter):
            events.append({"type": "reader_exception", "message": p.Message})
        elif isinstance(p, P.ROSpecEvent_Parameter):
            events.append(
                {"type": "rospec", "event": P.ROSpecEventType.getName(p.EventType), "rospec_id": p.ROSpecID}
            )
        elif isinstance(p, P.ReportBufferLevelWarningEvent_Parameter):
            events.append({"type": "report_buffer_warning", "percent_full": p.ReportBufferPercentageFull})
        elif isinstance(p, P.ReportBufferOverflowErrorEvent_Parameter):
            events.append({"type": "report_buffer_overflow"})
        elif isinstance(p, P.GPIEvent_Parameter):
            events.append({"type": "gpi", "port": p.GPIPortNumber, "state": bool(p.GPIEvent)})
        elif isinstance(p, (P.UTCTimestamp_Parameter, P.Uptime_Parameter, P.HoppingEvent_Parameter)):
            continue
        else:
            events.append({"type": "other", "parameter": type(p).__name__})
    return events


def llrp_status(message: Any) -> tuple[bool, str]:
    """Return (success, description) from a response's LLRPStatus parameter."""
    status = None
    for p in getattr(message, "Parameters", []):
        if isinstance(p, P.LLRPStatus_Parameter):
            status = p
            break
    if status is None:
        return False, f"{type(message).__name__} has no LLRPStatus"
    ok = status.StatusCode == P.StatusCode.M_Success
    name = P.StatusCode.getName(status.StatusCode)
    desc = status.ErrorDescription or ""
    detail = name + (f": {desc}" if desc else "")
    fe = status.getFirstParameterByClass(P.FieldError_Parameter)
    pe = status.getFirstParameterByClass(P.ParameterError_Parameter)
    if pe is not None:
        detail += f" (parameter type {pe.ParameterType}: {P.StatusCode.getName(pe.ErrorCode)})"
    if fe is not None:
        detail += f" (field {fe.FieldNum}: {P.StatusCode.getName(fe.ErrorCode)})"
    return ok, detail
