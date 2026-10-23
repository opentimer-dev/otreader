"""A simulated LLRP reader for tests, demos and development without hardware.

It speaks real LLRP over TCP, encoding and decoding with pyllrp, and implements
the subset of reader behaviour that OTReader relies on:

* connection notification (UTC or uptime timestamp; optional clock offset),
* GET_READER_CAPABILITIES (optionally Impinj-branded with detailed version),
* IMPINJ_ENABLE_EXTENSIONS (success, or ERROR_MESSAGE for a generic reader),
* GET/SET_READER_CONFIG, KEEPALIVE generation,
* ADD/DELETE/ENABLE/DISABLE/START/STOP ROSpec and GET_ROSPECS,
* tag report generation honouring the ROSpec's TagReportContentSelector and
  Impinj report selector,
* fault injection: refuse options, drop the connection, go silent.

Run standalone::

    python -m app.llrp.simulator --port 5084 --tags 20 --pass-interval 2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import struct
import time
from dataclasses import dataclass, field
from typing import Any

from pyllrp import pyllrp as P

from app.llrp.transport import pack_message, unpack_message

log = logging.getLogger(__name__)

IMPINJ_PEN = 25882
GENERIC_PEN = 99999


def default_rf_modes() -> list[dict[str, int]]:
    """A mode table resembling a typical reader (values are illustrative)."""
    return [
        # id, DR, M, FLM, mask, BDR, PIE, min/max/step Tari
        dict(ModeIdentifier=0, DRValue=True, MValue=0, ForwardLinkModulation=2, SpectralMaskIndicator=1,
             BDRValue=640000, PIEValue=1500, MinTariValue=6250, MaxTariValue=6250, StepTariValue=0),
        dict(ModeIdentifier=1, DRValue=True, MValue=1, ForwardLinkModulation=2, SpectralMaskIndicator=1,
             BDRValue=640000, PIEValue=1500, MinTariValue=6250, MaxTariValue=6250, StepTariValue=0),
        dict(ModeIdentifier=2, DRValue=True, MValue=2, ForwardLinkModulation=2, SpectralMaskIndicator=3,
             BDRValue=274000, PIEValue=2000, MinTariValue=20000, MaxTariValue=20000, StepTariValue=0),
        dict(ModeIdentifier=3, DRValue=True, MValue=3, ForwardLinkModulation=2, SpectralMaskIndicator=3,
             BDRValue=170600, PIEValue=2000, MinTariValue=20000, MaxTariValue=20000, StepTariValue=0),
        dict(ModeIdentifier=4, DRValue=True, MValue=2, ForwardLinkModulation=2, SpectralMaskIndicator=1,
             BDRValue=640000, PIEValue=1500, MinTariValue=7140, MaxTariValue=7140, StepTariValue=0),
    ]


@dataclass
class SimTagRead:
    epc: str
    antenna: int = 1
    rssi: float = -50.0
    timestamp_us: int | None = None  # reader-clock UTC; default = now
    phase: int | None = None
    doppler: int | None = None
    seen_count: int = 1
    last_seen_us: int | None = None


@dataclass
class SimulatorOptions:
    impinj: bool = True
    max_antennas: int = 4
    connected_antennas: list[int] = field(default_factory=lambda: [1, 2])
    firmware: str = "7.5.0.240"
    model_code: int = 2001002
    impinj_model_name: str = "Speedway R420 (simulated)"
    serial: str = "SIM-0001"
    rf_modes: list[dict[str, int]] = field(default_factory=default_rf_modes)
    max_rospecs: int = 1
    clock_offset_us: int = 0  # reader clock minus host clock
    uptime_timestamps: bool = False
    reject_impinj_search_mode: bool = False
    reject_impinj_report_selector: bool = False
    reject_rf_control: bool = False
    reject_rospec_ids: set[int] = field(default_factory=set)
    support_get_rospecs: bool = True
    send_keepalives: bool = True
    send_rospec_events: bool = True


class SimulatedReader:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, options: SimulatorOptions | None = None) -> None:
        self.host = host
        self._port = port
        self.opts = options or SimulatorOptions()
        self._server: asyncio.base_events.Server | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self.rospecs: dict[int, Any] = {}  # id -> ROSpec_Parameter
        self.rospec_state: dict[int, int] = {}
        self.impinj_enabled = False
        self.keepalive_ms = 0
        self.silent = False
        self.received: list[str] = []
        self.connections = 0
        self.boot_host_us = time.time_ns() // 1000
        self._msg_id = 1_000_000
        self.connected_event = asyncio.Event()

    # --------------------------------------------------------------- server

    @property
    def port(self) -> int:
        if self._server is not None and self._server.sockets:
            return self._server.sockets[0].getsockname()[1]
        return self._port

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self._port)
        log.info("Simulated LLRP reader listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        await self.drop_connection()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def reader_now_us(self) -> int:
        return time.time_ns() // 1000 + self.opts.clock_offset_us

    def uptime_us(self) -> int:
        return time.time_ns() // 1000 - self.boot_host_us

    def _uptime_for(self, reader_utc_us: int) -> int:
        return reader_utc_us - self.opts.clock_offset_us - self.boot_host_us

    def _timestamp_param(self) -> Any:
        if self.opts.uptime_timestamps:
            return P.Uptime_Parameter(Microseconds=self.uptime_us())
        return P.UTCTimestamp_Parameter(Microseconds=self.reader_now_us())

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def _send(self, msg: Any) -> None:
        if self._writer is None or self.silent:
            return
        try:
            self._writer.write(pack_message(msg))
            await self._writer.drain()
        except (ConnectionError, RuntimeError):
            pass

    @property
    def client_connected(self) -> bool:
        return self._writer is not None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._writer is not None:
            msg = P.READER_EVENT_NOTIFICATION_Message(
                MessageID=0,
                Parameters=[
                    P.ReaderEventNotificationData_Parameter(
                        Parameters=[
                            self._timestamp_param(),
                            P.ConnectionAttemptEvent_Parameter(
                                Status=P.ConnectionAttemptStatusType.Failed_A_Client_Initiated_Connection_Already_Exists
                            ),
                        ]
                    )
                ],
            )
            writer.write(pack_message(msg))
            await writer.drain()
            writer.close()
            return
        self._writer = writer
        self.connections += 1
        self.impinj_enabled = False
        self.silent = False
        await self._send(
            P.READER_EVENT_NOTIFICATION_Message(
                MessageID=0,
                Parameters=[
                    P.ReaderEventNotificationData_Parameter(
                        Parameters=[
                            self._timestamp_param(),
                            P.ConnectionAttemptEvent_Parameter(Status=P.ConnectionAttemptStatusType.Success),
                        ]
                    )
                ],
            )
        )
        self.connected_event.set()
        self._client_task = asyncio.current_task()
        try:
            while True:
                header = await reader.readexactly(10)
                _, length, _ = struct.unpack(">HII", header)
                body = await reader.readexactly(length - 10)
                try:
                    msg = unpack_message(header + body)
                except Exception as exc:
                    log.warning("simulator: cannot parse message: %r", exc)
                    continue
                self.received.append(type(msg).__name__.replace("_Message", ""))
                try:
                    close = await self._handle(msg)
                except Exception:
                    log.exception("simulator: error handling %s", type(msg).__name__)
                    close = False
                if close:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except asyncio.CancelledError:
            pass
        finally:
            if self._keepalive_task is not None:
                self._keepalive_task.cancel()
                self._keepalive_task = None
            if self._writer is writer:
                self._writer = None
            self.connected_event.clear()
            # A real reader keeps running ROSpecs only while a client is connected
            # (unless configured otherwise); disable to keep tests deterministic.
            for rid in self.rospec_state:
                self.rospec_state[rid] = P.ROSpecState.Disabled
            try:
                writer.close()
            except Exception:
                pass

    async def drop_connection(self) -> None:
        """Abruptly close the client socket (simulates network failure)."""
        w = self._writer
        self._writer = None
        if w is not None:
            try:
                w.transport.abort()
            except Exception:
                pass
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            self._keepalive_task = None
        await asyncio.sleep(0)

    # --------------------------------------------------------------- handlers

    def _status(self, code: int = 0, desc: str = "") -> Any:
        return P.LLRPStatus_Parameter(StatusCode=code, ErrorDescription=desc)

    async def _reply(self, request: Any, response_cls: Any, params: list[Any] | None = None, code: int = 0,
                     desc: str = "") -> None:
        await self._send(response_cls(MessageID=request.MessageID, Parameters=[self._status(code, desc)] + (params or [])))

    async def _handle(self, msg: Any) -> bool:  # noqa: C901 - dispatch table
        if isinstance(msg, P.GET_READER_CAPABILITIES_Message):
            await self._send(self._capabilities(msg))
        elif isinstance(msg, P.IMPINJ_ENABLE_EXTENSIONS_Message):
            if self.opts.impinj:
                self.impinj_enabled = True
                await self._reply(msg, P.IMPINJ_ENABLE_EXTENSIONS_RESPONSE_Message)
            else:
                await self._send(
                    P.ERROR_MESSAGE_Message(
                        MessageID=msg.MessageID,
                        Parameters=[self._status(P.StatusCode.M_UnsupportedMessage, "unsupported custom message")],
                    )
                )
        elif isinstance(msg, P.GET_READER_CONFIG_Message):
            await self._send(self._reader_config(msg))
        elif isinstance(msg, P.SET_READER_CONFIG_Message):
            if self._has_impinj(msg) and not self.impinj_enabled:
                await self._reply(msg, P.SET_READER_CONFIG_RESPONSE_Message, code=P.StatusCode.M_UnsupportedParameter,
                                  desc="custom parameter not supported")
                return False
            ka = msg.getFirstParameterByClass(P.KeepaliveSpec_Parameter)
            if ka is not None:
                self.keepalive_ms = ka.PeriodicTriggerValue if ka.KeepaliveTriggerType == 1 else 0
                self._restart_keepalive()
            await self._reply(msg, P.SET_READER_CONFIG_RESPONSE_Message)
        elif isinstance(msg, P.ADD_ROSPEC_Message):
            await self._add_rospec(msg)
        elif isinstance(msg, P.DELETE_ROSPEC_Message):
            rid = msg.ROSpecID
            if rid == 0:
                self.rospecs.clear()
                self.rospec_state.clear()
                await self._reply(msg, P.DELETE_ROSPEC_RESPONSE_Message)
            elif rid in self.rospecs:
                del self.rospecs[rid]
                self.rospec_state.pop(rid, None)
                await self._reply(msg, P.DELETE_ROSPEC_RESPONSE_Message)
            else:
                await self._reply(msg, P.DELETE_ROSPEC_RESPONSE_Message, code=P.StatusCode.A_Invalid,
                                  desc=f"no ROSpec {rid}")
        elif isinstance(msg, (P.ENABLE_ROSPEC_Message, P.START_ROSPEC_Message)):
            resp_cls = P.ENABLE_ROSPEC_RESPONSE_Message if isinstance(msg, P.ENABLE_ROSPEC_Message) else P.START_ROSPEC_RESPONSE_Message
            ids = list(self.rospecs) if msg.ROSpecID == 0 else [msg.ROSpecID]
            if not ids or any(i not in self.rospecs for i in ids):
                await self._reply(msg, resp_cls, code=P.StatusCode.A_Invalid, desc="no such ROSpec")
            else:
                for i in ids:
                    self.rospec_state[i] = P.ROSpecState.Active
                await self._reply(msg, resp_cls)
                for i in ids:
                    await self._rospec_event(0, i)
        elif isinstance(msg, (P.DISABLE_ROSPEC_Message, P.STOP_ROSPEC_Message)):
            resp_cls = P.DISABLE_ROSPEC_RESPONSE_Message if isinstance(msg, P.DISABLE_ROSPEC_Message) else P.STOP_ROSPEC_RESPONSE_Message
            ids = list(self.rospecs) if msg.ROSpecID == 0 else [msg.ROSpecID]
            if msg.ROSpecID != 0 and msg.ROSpecID not in self.rospecs:
                await self._reply(msg, resp_cls, code=P.StatusCode.A_Invalid, desc="no such ROSpec")
            else:
                for i in ids:
                    self.rospec_state[i] = (
                        P.ROSpecState.Disabled if isinstance(msg, P.DISABLE_ROSPEC_Message) else P.ROSpecState.Inactive
                    )
                await self._reply(msg, resp_cls)
                for i in ids:
                    await self._rospec_event(1, i)
        elif isinstance(msg, P.GET_ROSPECS_Message):
            if not self.opts.support_get_rospecs:
                await self._send(P.ERROR_MESSAGE_Message(MessageID=msg.MessageID, Parameters=[
                    self._status(P.StatusCode.M_UnsupportedMessage, "unsupported")]))
                return False
            specs = []
            for rid, spec in self.rospecs.items():
                spec.CurrentState = self.rospec_state.get(rid, P.ROSpecState.Disabled)
                specs.append(spec)
            await self._reply(msg, P.GET_ROSPECS_RESPONSE_Message, specs)
        elif isinstance(msg, P.DELETE_ACCESSSPEC_Message):
            await self._reply(msg, P.DELETE_ACCESSSPEC_RESPONSE_Message)
        elif isinstance(msg, P.KEEPALIVE_ACK_Message):
            pass
        elif isinstance(msg, P.CLOSE_CONNECTION_Message):
            await self._reply(msg, P.CLOSE_CONNECTION_RESPONSE_Message)
            return True
        else:
            await self._send(P.ERROR_MESSAGE_Message(MessageID=msg.MessageID, Parameters=[
                self._status(P.StatusCode.M_UnsupportedMessage, f"unsupported {type(msg).__name__}")]))
        return False

    @staticmethod
    def _has_impinj(msg: Any) -> bool:
        def walk(obj: Any) -> bool:
            for p in getattr(obj, "Parameters", []):
                if type(p).__name__.startswith("Impinj"):
                    return True
                if walk(p):
                    return True
            return False

        return walk(msg)

    async def _rospec_event(self, event_type: int, rid: int) -> None:
        if not self.opts.send_rospec_events:
            return
        await self._send(
            P.READER_EVENT_NOTIFICATION_Message(
                MessageID=0,
                Parameters=[
                    P.ReaderEventNotificationData_Parameter(
                        Parameters=[
                            self._timestamp_param(),
                            P.ROSpecEvent_Parameter(EventType=event_type, ROSpecID=rid, PreemptingROSpecID=0),
                        ]
                    )
                ],
            )
        )

    async def _add_rospec(self, msg: Any) -> None:
        spec = msg.getFirstParameterByClass(P.ROSpec_Parameter)
        rid = spec.ROSpecID
        err = None
        if rid in self.rospecs:
            err = (P.StatusCode.A_Invalid, f"ROSpec {rid} already exists")
        elif len(self.rospecs) >= self.opts.max_rospecs:
            err = (P.StatusCode.A_Invalid, f"maximum of {self.opts.max_rospecs} ROSpec(s)")
        elif rid in self.opts.reject_rospec_ids:
            err = (P.StatusCode.A_Invalid, f"ROSpec {rid} rejected (fault injection)")
        elif self._has_impinj(msg) and not self.impinj_enabled:
            err = (P.StatusCode.P_UnsupportedParameter, "custom parameter not supported")
        elif self.opts.reject_impinj_search_mode and msg.getFirstParameterByClass(P.ImpinjInventorySearchMode_Parameter):
            err = (P.StatusCode.P_UnsupportedParameter, "ImpinjInventorySearchMode not supported")
        elif self.opts.reject_impinj_report_selector and msg.getFirstParameterByClass(
            P.ImpinjTagReportContentSelector_Parameter
        ):
            err = (P.StatusCode.P_UnsupportedParameter, "ImpinjTagReportContentSelector not supported")
        else:
            rfc = msg.getFirstParameterByClass(P.C1G2RFControl_Parameter)
            if rfc is not None:
                if self.opts.reject_rf_control:
                    err = (P.StatusCode.P_UnsupportedParameter, "C1G2RFControl not supported")
                elif rfc.ModeIndex not in {m["ModeIdentifier"] for m in self.opts.rf_modes}:
                    err = (P.StatusCode.P_FieldError, f"invalid ModeIndex {rfc.ModeIndex}")
        if err is not None:
            await self._reply(msg, P.ADD_ROSPEC_RESPONSE_Message, code=err[0], desc=err[1])
            return
        self.rospecs[rid] = spec
        self.rospec_state[rid] = P.ROSpecState.Disabled
        await self._reply(msg, P.ADD_ROSPEC_RESPONSE_Message)

    def _capabilities(self, msg: Any) -> Any:
        o = self.opts
        gdc = P.GeneralDeviceCapabilities_Parameter(
            MaxNumberOfAntennaSupported=o.max_antennas,
            CanSetAntennaProperties=False,
            HasUTCClockCapability=not o.uptime_timestamps,
            DeviceManufacturerName=IMPINJ_PEN if o.impinj else GENERIC_PEN,
            ModelName=o.model_code,
            ReaderFirmwareVersion=o.firmware,
            Parameters=[P.ReceiveSensitivityTableEntry_Parameter(Index=1, ReceiveSensitivityValue=0)]
            + [P.ReceiveSensitivityTableEntry_Parameter(Index=i, ReceiveSensitivityValue=-80 + i) for i in range(2, 43)]
            + [P.GPIOCapabilities_Parameter(NumGPIs=4, NumGPOs=4)]
            + [P.PerAntennaAirProtocol_Parameter(AntennaID=a, ProtocolID=[1]) for a in range(1, o.max_antennas + 1)],
        )
        llrpc = P.LLRPCapabilities_Parameter(
            CanDoRFSurvey=False,
            CanReportBufferFillWarning=True,
            SupportsClientRequestOpSpec=False,
            CanDoTagInventoryStateAwareSingulation=True,
            SupportsEventAndReportHolding=True,
            MaxNumPriorityLevelsSupported=1,
            ClientRequestOpSpecTimeout=0,
            MaxNumROSpecs=o.max_rospecs,
            MaxNumSpecsPerROSpec=32,
            MaxNumInventoryParameterSpecsPerAISpec=1,
            MaxNumAccessSpecs=1508,
            MaxNumOpSpecsPerAccessSpec=8,
        )
        powers = [
            P.TransmitPowerLevelTableEntry_Parameter(Index=i + 1, TransmitPowerValue=1000 + i * 25) for i in range(91)
        ]
        uhf = P.UHFBandCapabilities_Parameter(
            Parameters=powers
            + [
                P.FrequencyInformation_Parameter(
                    Hopping=True,
                    Parameters=[
                        P.FrequencyHopTable_Parameter(HopTableID=1, Frequency=[902750 + 500 * i for i in range(50)])
                    ],
                ),
                P.C1G2UHFRFModeTable_Parameter(
                    Parameters=[P.C1G2UHFRFModeTableEntry_Parameter(EPCHAGTCConformance=False, **m) for m in o.rf_modes]
                ),
            ]
        )
        reg = P.RegulatoryCapabilities_Parameter(CountryCode=840, CommunicationsStandard=1, Parameters=[uhf])
        c1g2 = P.C1G2LLRPCapabilities_Parameter(CanSupportBlockErase=False, CanSupportBlockWrite=True,
                                                MaxNumSelectFiltersPerQuery=2)
        params: list[Any] = [self._status(), gdc, llrpc, reg, c1g2]
        if self.impinj_enabled and msg.getFirstParameterByClass(P.ImpinjRequestedData_Parameter) is not None:
            params.append(
                P.ImpinjDetailedVersion_Parameter(
                    ModelName=o.impinj_model_name,
                    SerialNumber=o.serial,
                    SoftwareVersion=o.firmware,
                    FirmwareVersion=o.firmware,
                    FPGAVersion="1.0",
                    PCBAVersion="1.0",
                )
            )
        return P.GET_READER_CAPABILITIES_RESPONSE_Message(MessageID=msg.MessageID, Parameters=params)

    def _reader_config(self, msg: Any) -> Any:
        o = self.opts
        params: list[Any] = [
            self._status(),
            P.Identification_Parameter(IDType=0, ReaderID=[0x00, 0x16, 0x25, 0x12, 0x34, 0x56]),
        ]
        params += [
            P.AntennaProperties_Parameter(AntennaConnected=a in o.connected_antennas, AntennaID=a, AntennaGain=600)
            for a in range(1, o.max_antennas + 1)
        ]
        params += [
            P.AntennaConfiguration_Parameter(
                AntennaID=a,
                Parameters=[
                    P.RFReceiver_Parameter(ReceiverSensitivity=1),
                    P.RFTransmitter_Parameter(HopTableID=1, ChannelIndex=1, TransmitPower=91),
                ],
            )
            for a in range(1, o.max_antennas + 1)
        ]
        params.append(P.KeepaliveSpec_Parameter(KeepaliveTriggerType=1 if self.keepalive_ms else 0,
                                                PeriodicTriggerValue=self.keepalive_ms))
        params.append(P.EventsAndReports_Parameter(HoldEventsAndReportsUponReconnect=False))
        if self.impinj_enabled and msg.getFirstParameterByClass(P.ImpinjRequestedData_Parameter) is not None:
            params.append(P.ImpinjReaderTemperature_Parameter(Temperature=41))
        return P.GET_READER_CONFIG_RESPONSE_Message(MessageID=msg.MessageID, Parameters=params)

    def _restart_keepalive(self) -> None:
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            self._keepalive_task = None
        if self.keepalive_ms > 0 and self.opts.send_keepalives:
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        while True:
            await asyncio.sleep(self.keepalive_ms / 1000.0)
            await self._send(P.KEEPALIVE_Message(MessageID=self._next_id()))

    # --------------------------------------------------------------- tag reads

    def active_rospec(self) -> Any | None:
        for rid, st in self.rospec_state.items():
            if st == P.ROSpecState.Active:
                return self.rospecs[rid]
        return None

    def _tag_report(self, spec: Any, read: SimTagRead) -> Any:
        sel = spec.getFirstParameterByClass(P.TagReportContentSelector_Parameter)
        isel = spec.getFirstParameterByClass(P.ImpinjTagReportContentSelector_Parameter)
        ts = read.timestamp_us if read.timestamp_us is not None else self.reader_now_us()
        last = read.last_seen_us if read.last_seen_us is not None else ts
        epc_int = int(read.epc, 16)
        params: list[Any] = []
        if len(read.epc) == 24:
            params.append(P.EPC_96_Parameter(EPC=epc_int))
        else:
            params.append(P.EPCData_Parameter(EPC=bytes.fromhex(read.epc)))
        if sel.EnableROSpecID:
            params.append(P.ROSpecID_Parameter(ROSpecID=spec.ROSpecID))
        if sel.EnableAntennaID:
            params.append(P.AntennaID_Parameter(AntennaID=read.antenna))
        if sel.EnablePeakRSSI:
            params.append(P.PeakRSSI_Parameter(PeakRSSI=int(round(read.rssi))))
        if sel.EnableChannelIndex:
            params.append(P.ChannelIndex_Parameter(ChannelIndex=random.randint(1, 50)))
        if sel.EnableFirstSeenTimestamp:
            if self.opts.uptime_timestamps:
                params.append(P.FirstSeenTimestampUptime_Parameter(Microseconds=self._uptime_for(ts)))
            else:
                params.append(P.FirstSeenTimestampUTC_Parameter(Microseconds=ts))
        if sel.EnableLastSeenTimestamp:
            if self.opts.uptime_timestamps:
                params.append(P.LastSeenTimestampUptime_Parameter(Microseconds=self._uptime_for(last)))
            else:
                params.append(P.LastSeenTimestampUTC_Parameter(Microseconds=last))
        if sel.EnableTagSeenCount:
            params.append(P.TagSeenCount_Parameter(TagCount=read.seen_count))
        if isel is not None and self.impinj_enabled:
            if isel.getFirstParameterByClass(P.ImpinjEnableRFPhaseAngle_Parameter):
                params.append(P.ImpinjRFPhaseAngle_Parameter(PhaseAngle=read.phase if read.phase is not None
                                                             else random.randint(0, 4095)))
            if isel.getFirstParameterByClass(P.ImpinjEnablePeakRSSI_Parameter):
                params.append(P.ImpinjPeakRSSI_Parameter(RSSI=int(round(read.rssi * 100))))
            if isel.getFirstParameterByClass(P.ImpinjEnableRFDopplerFrequency_Parameter):
                params.append(P.ImpinjRFDopplerFrequency_Parameter(DopplerFrequency=read.doppler or 0))
        return P.TagReportData_Parameter(Parameters=params)

    async def inject_reads(self, reads: list[SimTagRead], per_report: int = 1) -> int:
        """Send reads as RO_ACCESS_REPORTs if a ROSpec is active.  Returns reads sent."""
        spec = self.active_rospec()
        if spec is None or self._writer is None:
            return 0
        sent = 0
        for i in range(0, len(reads), per_report):
            chunk = reads[i : i + per_report]
            await self._send(
                P.RO_ACCESS_REPORT_Message(MessageID=0, Parameters=[self._tag_report(spec, r) for r in chunk])
            )
            sent += len(chunk)
        return sent

    async def inject_read(self, epc: str, **kw: Any) -> int:
        return await self.inject_reads([SimTagRead(epc=epc, **kw)])

    def make_pass(
        self,
        epc: str,
        peak_us: int,
        antenna: int = 1,
        duration_s: float = 1.2,
        interval_ms: float = 25.0,
        peak_rssi: float = -40.0,
        curvature_db_per_s2: float = 60.0,
        noise_db: float = 0.0,
        floor_dbm: float = -75.0,
        speed_mps: float = 4.0,
        rng: random.Random | None = None,
    ) -> list[SimTagRead]:
        """Synthesise reads for a tag passing over an antenna with RSSI peak at *peak_us*."""
        rng = rng or random.Random()
        reads = []
        n = int(duration_s * 1000 / interval_ms)
        start = peak_us - int(duration_s / 2 * 1e6)
        for k in range(n + 1):
            t = start + int(k * interval_ms * 1000)
            dt = (t - peak_us) / 1e6
            rssi = peak_rssi - curvature_db_per_s2 * dt * dt + rng.gauss(0, noise_db) if noise_db else (
                peak_rssi - curvature_db_per_s2 * dt * dt
            )
            if rssi < floor_dbm:
                continue
            # Radial velocity sign flips at closest approach (approaching => positive Doppler).
            doppler = int(round(-dt * speed_mps * 16 * 10))
            reads.append(SimTagRead(epc=epc, antenna=antenna, rssi=round(rssi, 2), timestamp_us=t, doppler=doppler))
        return reads


# ------------------------------------------------------------------ CLI demo


async def _demo(args: argparse.Namespace) -> None:
    opts = SimulatorOptions(impinj=not args.generic, clock_offset_us=int(args.clock_offset_ms * 1000))
    sim = SimulatedReader(args.host, args.port, opts)
    await sim.start()
    rng = random.Random(args.seed)
    epcs = [f"3008{i:020X}" for i in range(1, args.tags + 1)]
    try:
        while True:
            await asyncio.sleep(args.pass_interval)
            if sim.active_rospec() is None:
                continue
            epc = rng.choice(epcs)
            peak = sim.reader_now_us() - 600_000
            reads = sim.make_pass(epc, peak, antenna=rng.choice(opts.connected_antennas), noise_db=args.noise_db, rng=rng)
            n = await sim.inject_reads(reads)
            log.info("simulated pass for %s (%d reads, peak at reader time %d)", epc, n, peak)
    finally:
        await sim.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="Simulated LLRP reader for OTReader development")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5084)
    ap.add_argument("--tags", type=int, default=20)
    ap.add_argument("--pass-interval", type=float, default=2.0, help="seconds between simulated passes")
    ap.add_argument("--noise-db", type=float, default=1.5)
    ap.add_argument("--clock-offset-ms", type=float, default=0.0, help="reader clock minus host clock")
    ap.add_argument("--generic", action="store_true", help="simulate a non-Impinj reader")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(_demo(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
