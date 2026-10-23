from pyllrp import pyllrp as P

from app.config import parse_config
from app.llrp.capabilities import PowerLevel, apply_capabilities_response, empty_capabilities
from app.llrp.configuration import (
    FEATURE_LEVELS,
    ROSPEC_ID_FINISH_LINE,
    ROSPEC_ID_START_LINE,
    build_add_rospec,
    build_reader_config,
    make_plan,
    select_power_index,
    select_rf_mode,
)
from app.llrp.simulator import SimulatedReader, SimulatorOptions, default_rf_modes
from app.llrp.transport import pack_message, unpack_message


def caps_for(impinj=True, rf_modes=None):
    sim = SimulatedReader(options=SimulatorOptions(impinj=impinj, rf_modes=rf_modes or default_rf_modes()))
    sim.impinj_enabled = impinj
    resp = unpack_message(pack_message(sim._capabilities(P.GET_READER_CAPABILITIES_Message(RequestedData=0))))
    caps = empty_capabilities()
    apply_capabilities_response(caps, resp)
    caps.impinj.extensions_enabled = impinj
    return caps


CFG = parse_config({"reader": {"host": "h"}})


def test_throughput_strategy_picks_fastest_link():
    caps = caps_for()
    choice = select_rf_mode(caps.rf_modes, CFG.reader.finish_line)
    assert choice.mode_id == 0  # FM0 @ 640 kbps
    assert "throughput" in choice.reason


def test_robust_strategy_prefers_dense_interrogator_miller4():
    caps = caps_for()
    choice = select_rf_mode(caps.rf_modes, CFG.reader.start_line)
    assert choice.mode_id == 2  # DI mask, M=4


def test_autoset_pseudo_modes_ignored_unless_explicit():
    modes = default_rf_modes() + [dict(ModeIdentifier=1002, DRValue=True, MValue=0, ForwardLinkModulation=2,
                                       SpectralMaskIndicator=3, BDRValue=999999, PIEValue=1500, MinTariValue=6250,
                                       MaxTariValue=6250, StepTariValue=0)]
    caps = caps_for(rf_modes=modes)
    assert select_rf_mode(caps.rf_modes, CFG.reader.finish_line).mode_id == 0
    explicit = CFG.reader.finish_line.model_copy(update={"rf_mode": 1002})
    assert select_rf_mode(caps.rf_modes, explicit).mode_id == 1002


def test_no_mode_table_uses_reader_default():
    choice = select_rf_mode([], CFG.reader.finish_line)
    assert choice.mode_id is None
    rd = CFG.reader.finish_line.model_copy(update={"rf_mode": "reader_default"})
    assert select_rf_mode(caps_for().rf_modes, rd).mode_id is None


def test_power_index_selection():
    levels = [PowerLevel(index=i, dbm=10 + i * 0.25) for i in range(1, 91)]
    assert select_power_index(levels, 30.0) == (80, 30.0)
    assert select_power_index(levels, 30.1) == (80, 30.0)  # never exceeds request
    assert select_power_index(levels, 1.0) == (1, 10.25)  # below table -> minimum
    assert select_power_index(levels, None) == (None, None)


def _param(msg, cls):
    return msg.getFirstParameterByClass(cls)


def test_start_line_rospec_for_impinj():
    caps = caps_for()
    plan = make_plan(ROSPEC_ID_START_LINE, "start_line", CFG.reader.start_line, caps, None)
    msg = unpack_message(pack_message(build_add_rospec(plan, FEATURE_LEVELS[0])))  # validates via pyllrp
    assert _param(msg, P.ROSpec_Parameter).ROSpecID == ROSPEC_ID_START_LINE
    assert _param(msg, P.C1G2SingulationControl_Parameter).Session == 2
    assert _param(msg, P.ImpinjInventorySearchMode_Parameter).InventorySearchMode == P.ImpinjInventorySearchType.Single_Target
    assert _param(msg, P.C1G2RFControl_Parameter).ModeIndex == 2
    sel = _param(msg, P.TagReportContentSelector_Parameter)
    assert sel.EnableROSpecID and sel.EnableAntennaID and sel.EnablePeakRSSI and sel.EnableFirstSeenTimestamp
    assert _param(msg, P.ImpinjEnablePeakRSSI_Parameter) is not None
    assert _param(msg, P.ROReportSpec_Parameter).N == 1


def test_finish_line_rospec_for_impinj():
    caps = caps_for()
    plan = make_plan(ROSPEC_ID_FINISH_LINE, "finish_line", CFG.reader.finish_line, caps, [1, 2])
    msg = unpack_message(pack_message(build_add_rospec(plan, FEATURE_LEVELS[0])))
    assert _param(msg, P.AISpec_Parameter).AntennaIDs == [1, 2]
    assert len(list(msg.getAllParametersByClass(P.AntennaConfiguration_Parameter))) == 2
    assert _param(msg, P.C1G2SingulationControl_Parameter).Session == 0
    assert _param(msg, P.ImpinjInventorySearchMode_Parameter).InventorySearchMode == P.ImpinjInventorySearchType.Dual_Target
    assert _param(msg, P.ImpinjEnableRFDopplerFrequency_Parameter) is not None
    assert _param(msg, P.ImpinjEnableRFPhaseAngle_Parameter) is not None


def test_generic_reader_rospec_has_no_vendor_parameters():
    caps = caps_for(impinj=False)
    plan = make_plan(ROSPEC_ID_FINISH_LINE, "finish_line", CFG.reader.finish_line, caps, None)
    msg = build_add_rospec(plan, FEATURE_LEVELS[0])
    assert not SimulatedReader._has_impinj(msg)
    assert _param(msg, P.C1G2SingulationControl_Parameter).Session == 0
    assert "not negotiated" in plan.notes[0]


def test_feature_levels_progressively_drop_options():
    caps = caps_for()
    prof = CFG.reader.finish_line.model_copy(update={"transmit_power_dbm": 30.0})
    plan = make_plan(ROSPEC_ID_FINISH_LINE, "finish_line", prof, caps, None)
    last = build_add_rospec(plan, FEATURE_LEVELS[-1])
    assert _param(last, P.ImpinjInventorySearchMode_Parameter) is None
    assert _param(last, P.ImpinjTagReportContentSelector_Parameter) is None
    assert _param(last, P.C1G2RFControl_Parameter) is None
    assert _param(last, P.RFTransmitter_Parameter) is None
    first = build_add_rospec(plan, FEATURE_LEVELS[0])
    assert _param(first, P.RFTransmitter_Parameter).TransmitPower == 81
    assert plan.describe(FEATURE_LEVELS[0])["transmit_power_dbm"] == 30.0


def test_reader_config_message_validates():
    for impinj in (True, False):
        msg = unpack_message(pack_message(build_reader_config(5000, impinj)))
        assert _param(msg, P.KeepaliveSpec_Parameter).PeriodicTriggerValue == 5000
        assert (_param(msg, P.ImpinjReportBufferConfiguration_Parameter) is not None) == impinj
