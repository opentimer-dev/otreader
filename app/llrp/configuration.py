"""Reader configuration for each operating mode.

Mode selection is driven by the reader's *advertised* capabilities, never by mode
names:

* RF mode ("link profile") is chosen from the ``C1G2UHFRFModeTable`` entries using
  their physical parameters (backscatter data rate, Miller encoding, spectral mask).
  - ``throughput`` (finish line default): maximise effective reverse-link rate
    ``BDR / M`` so fast-moving tags are read as many times as possible while in
    the field, giving the estimator a dense RSSI profile.
  - ``robust`` (start line default): prefer Dense-Interrogator spectral mask and
    Miller-4 encoding, which trade raw speed for interference tolerance and
    sensitivity - appropriate when hundreds of tags (and bodies) crowd the field.
  Vendor "AutoSet" pseudo-modes (Impinj ModeIdentifier >= 1000) are only used
  when configured explicitly because their table parameters do not describe a
  single fixed link profile.

* Impinj inventory search mode (only when the Impinj extensions were negotiated):
  - start line: ``Single_Target`` with session 2 - each tag is inventoried once
    and then stays quiet (session-2 persistence) so the reader's time goes to tags
    not yet read: the classic dense-population strategy.
    ``Single_Target_With_Suppression`` (TagFocus) is available via config; it
    only benefits tags that support it.
  - finish line: ``Dual_Target`` with session 0 - tags are re-inventoried
    continuously (A->B->A...), producing the repeated reads the RSSI-peak
    estimator needs.
  For readers without Impinj extensions the same session/population settings are
  sent through standard ``C1G2SingulationControl``.

* Configuration is *negotiated*: if the reader rejects ADD_ROSPEC we retry with
  progressively fewer optional features (see :data:`FEATURE_LEVELS`) and record
  exactly what was applied.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyllrp import pyllrp as P

from app.config import InventoryProfileConfig
from app.llrp.capabilities import ReaderCapabilities, RFModeInfo

ROSPEC_ID_START_LINE = 1101
ROSPEC_ID_FINISH_LINE = 1102
ROSPEC_ID_TEST = 1199
INVENTORY_PARAMETER_SPEC_ID = 1


@dataclass(frozen=True)
class FeatureSet:
    impinj_search_mode: bool = True
    impinj_report_selector: bool = True
    rf_control: bool = True
    power_settings: bool = True
    extended_report_fields: bool = True  # LastSeen/TagSeenCount/ChannelIndex

    def describe(self) -> dict[str, bool]:
        return {
            "impinj_search_mode": self.impinj_search_mode,
            "impinj_report_selector": self.impinj_report_selector,
            "rf_control": self.rf_control,
            "power_settings": self.power_settings,
            "extended_report_fields": self.extended_report_fields,
        }


# Ordered from most to least capable.  Each subsequent level drops the option
# most likely to be rejected by a reader implementation.
FEATURE_LEVELS: tuple[FeatureSet, ...] = (
    FeatureSet(),
    FeatureSet(impinj_search_mode=False),
    FeatureSet(impinj_search_mode=False, impinj_report_selector=False),
    FeatureSet(impinj_search_mode=False, impinj_report_selector=False, rf_control=False),
    FeatureSet(impinj_search_mode=False, impinj_report_selector=False, rf_control=False, power_settings=False),
    FeatureSet(
        impinj_search_mode=False,
        impinj_report_selector=False,
        rf_control=False,
        power_settings=False,
        extended_report_fields=False,
    ),
)


@dataclass
class RFModeChoice:
    mode_id: int | None  # None => do not send C1G2RFControl
    reason: str


def select_rf_mode(modes: list[RFModeInfo], profile: InventoryProfileConfig) -> RFModeChoice:
    if isinstance(profile.rf_mode, int):
        known = {m.mode_id for m in modes}
        if modes and profile.rf_mode not in known:
            note = f"configured ModeIdentifier {profile.rf_mode} is not in the reader's advertised table"
        else:
            note = f"configured ModeIdentifier {profile.rf_mode}"
        return RFModeChoice(profile.rf_mode, note)
    if profile.rf_mode == "reader_default":
        return RFModeChoice(None, "configured to use reader default RF mode")
    candidates = [m for m in modes if m.mode_id < 1000]
    if not candidates:
        return RFModeChoice(None, "reader advertised no fixed RF modes; using reader default")

    if profile.rf_mode_strategy == "throughput":
        best = max(candidates, key=lambda m: (m.link_rate, -m.min_tari_ns, -m.mode_id))
        return RFModeChoice(
            best.mode_id,
            f"throughput: highest BDR/M ({best.bdr_bps}/{best.m_factor} = {best.link_rate:.0f} bps)",
        )

    m_pref = {4: 0, 8: 1, 2: 2, 1: 3}

    def robust_key(m: RFModeInfo) -> tuple:
        return (0 if m.spectral_mask == "DI" else 1, m_pref.get(m.m_factor, 9), -m.link_rate, m.mode_id)

    best = min(candidates, key=robust_key)
    return RFModeChoice(
        best.mode_id,
        f"robust: spectral mask {best.spectral_mask}, Miller {best.m_value}, BDR {best.bdr_bps} bps",
    )


def select_power_index(levels: list[Any], dbm: float | None) -> tuple[int | None, float | None]:
    """Largest table entry <= requested dBm (or the minimum if all exceed it)."""
    if dbm is None or not levels:
        return None, None
    below = [lv for lv in levels if lv.dbm <= dbm + 1e-9]
    chosen = max(below, key=lambda lv: lv.dbm) if below else min(levels, key=lambda lv: lv.dbm)
    return chosen.index, chosen.dbm


def select_sensitivity_index(levels: list[Any], value_db: float | None) -> tuple[int | None, int | None]:
    if value_db is None or not levels:
        return None, None
    chosen = min(levels, key=lambda lv: abs(lv.value_db - value_db))
    return chosen.index, chosen.value_db


@dataclass
class InventoryPlan:
    """Everything needed to build the ROSpec for a mode, plus the explanation."""

    rospec_id: int
    purpose: str
    antennas: list[int]
    profile: InventoryProfileConfig
    impinj: bool
    rf_mode: RFModeChoice
    tx_power_index: int | None
    tx_power_dbm: float | None
    rx_sensitivity_index: int | None
    rx_sensitivity_db: int | None
    hop_table_id: int
    notes: list[str] = field(default_factory=list)

    def describe(self, features: FeatureSet) -> dict[str, Any]:
        return {
            "rospec_id": self.rospec_id,
            "purpose": self.purpose,
            "antennas": self.antennas or "all",
            "session": self.profile.session,
            "tag_population": self.profile.tag_population,
            "impinj_search_mode": (
                str(self.profile.impinj_search_mode) if self.impinj and features.impinj_search_mode else None
            ),
            "rf_mode_id": self.rf_mode.mode_id if features.rf_control else None,
            "rf_mode_reason": self.rf_mode.reason if features.rf_control else "not sent (negotiated away)",
            "transmit_power_dbm": self.tx_power_dbm if features.power_settings else None,
            "receive_sensitivity_db": self.rx_sensitivity_db if features.power_settings else None,
            "impinj_report_fields": (
                {
                    "peak_rssi": self.profile.enable_impinj_peak_rssi,
                    "phase_angle": self.profile.enable_impinj_phase_angle,
                    "doppler": self.profile.enable_impinj_doppler,
                }
                if self.impinj and features.impinj_report_selector
                else None
            ),
            "features": features.describe(),
            "notes": self.notes,
        }


def make_plan(
    rospec_id: int,
    purpose: str,
    profile: InventoryProfileConfig,
    caps: ReaderCapabilities,
    antennas: list[int] | None,
) -> InventoryPlan:
    impinj = caps.impinj_extensions
    rf = select_rf_mode(caps.rf_modes, profile)
    tx_idx, tx_dbm = select_power_index(caps.transmit_power_levels, profile.transmit_power_dbm)
    rx_idx, rx_db = select_sensitivity_index(caps.receive_sensitivities, profile.receive_sensitivity_db)
    notes = []
    if profile.transmit_power_dbm is not None and tx_idx is None:
        notes.append("transmit power requested but reader advertised no power table; using reader default")
    if not impinj:
        notes.append("Impinj extensions not negotiated; using standard LLRP singulation settings only")
    ant = list(antennas) if antennas else []
    if ant and caps.max_antennas:
        bad = [a for a in ant if a > caps.max_antennas]
        if bad:
            notes.append(f"antennas {bad} exceed reader maximum {caps.max_antennas}")
    return InventoryPlan(
        rospec_id=rospec_id,
        purpose=purpose,
        antennas=ant,
        profile=profile,
        impinj=impinj,
        rf_mode=rf,
        tx_power_index=tx_idx,
        tx_power_dbm=tx_dbm,
        rx_sensitivity_index=rx_idx,
        rx_sensitivity_db=rx_db,
        hop_table_id=caps.hop_table_id(),
        notes=notes,
    )


def _antenna_configuration(plan: InventoryPlan, features: FeatureSet, antenna_id: int) -> Any:
    params: list[Any] = []
    if features.power_settings and plan.rx_sensitivity_index is not None:
        params.append(P.RFReceiver_Parameter(ReceiverSensitivity=plan.rx_sensitivity_index))
    if features.power_settings and plan.tx_power_index is not None:
        params.append(
            P.RFTransmitter_Parameter(
                HopTableID=plan.hop_table_id, ChannelIndex=1, TransmitPower=plan.tx_power_index
            )
        )
    inv_params: list[Any] = []
    if features.rf_control and plan.rf_mode.mode_id is not None:
        inv_params.append(P.C1G2RFControl_Parameter(ModeIndex=plan.rf_mode.mode_id, Tari=0))
    inv_params.append(
        P.C1G2SingulationControl_Parameter(
            Session=plan.profile.session,
            TagPopulation=plan.profile.tag_population,
            TagTransitTime=plan.profile.tag_transit_time_ms,
        )
    )
    if plan.impinj and features.impinj_search_mode:
        inv_params.append(
            P.ImpinjInventorySearchMode_Parameter(
                InventorySearchMode=getattr(P.ImpinjInventorySearchType, str(plan.profile.impinj_search_mode))
            )
        )
    params.append(P.C1G2InventoryCommand_Parameter(TagInventoryStateAware=False, Parameters=inv_params))
    return P.AntennaConfiguration_Parameter(AntennaID=antenna_id, Parameters=params)


def build_add_rospec(plan: InventoryPlan, features: FeatureSet) -> Any:
    antenna_ids = plan.antennas or [0]
    ant_configs = [_antenna_configuration(plan, features, a) for a in antenna_ids]

    selector = P.TagReportContentSelector_Parameter(
        EnableROSpecID=True,
        EnableSpecIndex=False,
        EnableInventoryParameterSpecID=False,
        EnableAntennaID=True,
        EnableChannelIndex=features.extended_report_fields,
        EnablePeakRSSI=True,
        EnableFirstSeenTimestamp=True,
        EnableLastSeenTimestamp=features.extended_report_fields,
        EnableTagSeenCount=features.extended_report_fields,
        EnableAccessSpecID=False,
    )
    report_params: list[Any] = [selector]
    if plan.impinj and features.impinj_report_selector:
        sub = []
        if plan.profile.enable_impinj_phase_angle:
            sub.append(P.ImpinjEnableRFPhaseAngle_Parameter(RFPhaseAngleMode=P.ImpinjRFPhaseAngleMode.Enabled))
        if plan.profile.enable_impinj_peak_rssi:
            sub.append(P.ImpinjEnablePeakRSSI_Parameter(PeakRSSIMode=P.ImpinjPeakRSSIMode.Enabled))
        if plan.profile.enable_impinj_doppler:
            sub.append(
                P.ImpinjEnableRFDopplerFrequency_Parameter(
                    RFDopplerFrequencyMode=P.ImpinjRFDopplerFrequencyMode.Enabled
                )
            )
        if sub:
            report_params.append(P.ImpinjTagReportContentSelector_Parameter(Parameters=sub))

    return P.ADD_ROSPEC_Message(
        Parameters=[
            P.ROSpec_Parameter(
                ROSpecID=plan.rospec_id,
                Priority=0,
                CurrentState=P.ROSpecState.Disabled,
                Parameters=[
                    P.ROBoundarySpec_Parameter(
                        Parameters=[
                            P.ROSpecStartTrigger_Parameter(ROSpecStartTriggerType=P.ROSpecStartTriggerType.Immediate),
                            P.ROSpecStopTrigger_Parameter(
                                ROSpecStopTriggerType=P.ROSpecStopTriggerType.Null, DurationTriggerValue=0
                            ),
                        ]
                    ),
                    P.AISpec_Parameter(
                        AntennaIDs=antenna_ids,
                        Parameters=[
                            P.AISpecStopTrigger_Parameter(
                                AISpecStopTriggerType=P.AISpecStopTriggerType.Null, DurationTrigger=0
                            ),
                            P.InventoryParameterSpec_Parameter(
                                InventoryParameterSpecID=INVENTORY_PARAMETER_SPEC_ID,
                                ProtocolID=P.AirProtocols.EPCGlobalClass1Gen2,
                                Parameters=ant_configs,
                            ),
                        ],
                    ),
                    # N=1: report every tag read immediately (lowest latency, per-read RSSI).
                    P.ROReportSpec_Parameter(
                        ROReportTrigger=P.ROReportTriggerType.Upon_N_Tags_Or_End_Of_ROSpec,
                        N=1,
                        Parameters=report_params,
                    ),
                ],
            )
        ]
    )


def build_reader_config(keepalive_ms: int, impinj: bool, low_latency_reports: bool = True) -> Any:
    """SET_READER_CONFIG establishing events, keepalives and (Impinj) low-latency reporting."""
    events = [
        P.EventNotificationState_Parameter(EventType=getattr(P.NotificationEventType, name), NotificationState=True)
        for name in (
            "ROSpec_Event",
            "Report_Buffer_Fill_Warning",
            "Reader_Exception_Event",
            "Antenna_Event",
        )
    ]
    params: list[Any] = [P.ReaderEventNotificationSpec_Parameter(Parameters=events)]
    if keepalive_ms > 0:
        params.append(
            P.KeepaliveSpec_Parameter(
                KeepaliveTriggerType=P.KeepaliveTriggerType.Periodic, PeriodicTriggerValue=keepalive_ms
            )
        )
    params.append(P.EventsAndReports_Parameter(HoldEventsAndReportsUponReconnect=False))
    if impinj and low_latency_reports:
        params.append(
            P.ImpinjReportBufferConfiguration_Parameter(ReportBufferMode=P.ImpinjReportBufferMode.Low_Latency)
        )
    return P.SET_READER_CONFIG_Message(ResetToFactoryDefault=False, Parameters=params)
