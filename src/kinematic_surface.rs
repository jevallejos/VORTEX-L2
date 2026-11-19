//! Pure C1 Kinematic Free-Energy Surface, Live VIP Fee Scaling, Hawkes/Stoikov Shield & LOSS-LAB 63/63 (V4.8).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use crate::fee_cadence_beta::FeeSchedule;
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{LotQty, PairSpec, PriceTicks, ScaledBps, Side};

pub use crate::fee_cadence_beta::wald_sprt_boundaries_from_permille;

pub const UNIFIED_LOSS_LAB_COUNT: usize = 63;
pub type ScarEventKey = (i64, usize, u8);
pub type ScarEventPayload = (PriceTicks, ScaledBps, bool);

const HAWKES_PHI: f64 = 1.618_033_988_749_895;
const HAWKES_INV_PHI: f64 = 0.618_033_988_749_895;
const HAWKES_BETA_DECAY_PER_S: f64 = 2.0;
const WALD_MU_MID: f64 = 0.140_983_005_625_052_5;
const WALD_SPRT_GAIN: f64 = 0.963_932_022_500_210_3;

#[inline] #[must_use] pub const fn c_min_i64(a: i64, b: i64) -> i64 { if a < b { a } else { b } }
#[inline] #[must_use] pub const fn c_max_i64(a: i64, b: i64) -> i64 { if a > b { a } else { b } }
#[inline] #[must_use] pub const fn c_clamp_i64(val: i64, lo: i64, hi: i64) -> i64 { if val < lo { lo } else if val > hi { hi } else { val } }
#[inline] #[must_use] pub const fn c_min_u32(a: u32, b: u32) -> u32 { if a < b { a } else { b } }

#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
) -> bool {
    check_queue_depletion_velocity_abort_with_spoof(
        side,
        init_queue_ahead,
        current_queue_ahead,
        snap,
        0.0,
    )
}

#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort_with_spoof(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
    spoof_density_ewma: f64,
) -> bool {
    const INV_PHI_Q: f64 = 0.618_033_988_749_895;
    const INV_PHI2_Q: f64 = 0.381_966_011_250_105;
    const INV_PHI3_Q: f64 = 0.236_067_977_499_79;
    const INV_PHI4_Q: f64 = 0.145_898_033_750_315;
    const INV_PHI5_Q: f64 = 0.090_169_943_749_47;

    let sp = if spoof_density_ewma.is_finite() {
        spoof_density_ewma.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let dir_s = side.sign_f64();
    let signed_ofi = snap.factor_ofi.get() * dir_s;
    let signed_tfi = snap.factor_tfi.get() * dir_s;
    let signed_geom = snap.factor_geom.get() * dir_s;
    let flow_impulse = 0.55 * signed_ofi + 0.45 * signed_tfi;
    if flow_impulse <= -0.42 || (signed_ofi <= -0.22 && signed_geom <= -0.05) {
        return true;
    }
    if init_queue_ahead.0 <= 0 {
        return false;
    }
    let ref_px = snap.bids[0].price.0.max(snap.asks[0].price.0).max(1);
    let b3x_raw = crate::types::spec_from_symbol_id(snap.symbol_id)
        .and_then(|sp_spec| sp_spec.required_3x_barrier_bps(PriceTicks(ref_px)).ok())
        .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
        .raw();
    let nu_sigma = ((snap.realized_vol_5m.raw().max(b3x_raw) as f64) / ((b3x_raw.saturating_mul(4)).max(1) as f64))
        .sqrt()
        .clamp(0.80, 1.35);
    let depleted_frac = ((init_queue_ahead.0 - current_queue_ahead.0).max(0) as f64) / (init_queue_ahead.0 as f64);
    let req_fifo_tfi = INV_PHI2_Q * (1.0 + INV_PHI_Q * sp);
    let req_fifo_imp = INV_PHI4_Q * (1.0 + INV_PHI_Q * sp);
    let min_fifo_geom = -INV_PHI4_Q * INV_PHI4_Q * (1.0 - sp) + INV_PHI4_Q * sp;
    let tail_tfi_floor = (INV_PHI2_Q + INV_PHI5_Q) * (1.0 + INV_PHI3_Q * sp);
    let ofi_weak = signed_ofi <= INV_PHI4_Q * INV_PHI4_Q
        && !(signed_tfi >= req_fifo_tfi && flow_impulse >= req_fifo_imp && signed_geom >= min_fifo_geom);
    let sweep_book_weak = flow_impulse <= (-0.04 * nu_sigma) || ofi_weak || signed_geom <= -0.10;
    let tail_exposed = depleted_frac >= (0.78 * nu_sigma).min(0.88)
        && signed_ofi < req_fifo_imp
        && signed_tfi < tail_tfi_floor;
    (depleted_frac >= (0.50 * nu_sigma) && sweep_book_weak) || tail_exposed
}

/// Returns `true` when the live BBO has already stepped favorably past our passive `RestingEntryQueue` price
/// (`!at_l1` favorable: `best_bid > entry_price` on `Bid`, or `best_ask < entry_price` on `Ask`), making
/// BBO-level `X_GEOM` flips non-representative of our resting queue level (V5.4).
#[inline]
#[must_use]
pub const fn is_bbo_favorably_past_resting_entry(
    side: Side,
    entry_price: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> bool {
    if entry_price.0 <= 0 {
        return false;
    }
    match side {
        Side::Bid => snap.bids[0].price.0 > entry_price.0,
        Side::Ask => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 < entry_price.0,
    }
}

/// Front-of-queue and intact-wall C1 pre-fill immunity predicate (V5.4):
/// Replaces the static `500_000_000 ns` scalar with the continuous RTT-and-dilation horizon
/// `tau_foq_ns = (PHI^4 * rtt_ns) / kappa` (preserving `V06` at `170 ms`), preventing mechanical
/// `X_GEOM` thinning from triggering `SpoofingVeto` at front-of-queue (`rem_q_ratio < INV_PHI_SQ`)
/// or intact L1 wall (`rem_q_ratio >= 1.0 - INV_PHI_FOURTH`) unless real flow reversal co-occurs.
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn allows_front_of_queue_spoof_geom_abort_v54(
    init_q_lots: LotQty,
    rem_q_lots: LotQty,
    wait_ns: i64,
    signed_ofi: f64,
    signed_slow: f64,
    ws_rtt_us: u32,
    event_dilation: f64,
) -> bool {
    const PHI_FOQ: f64 = 1.618_033_988_749_895;
    const PHI_FOURTH_FOQ: f64 = 6.854_101_966_249_685;
    const INV_PHI_FOQ: f64 = 0.618_033_988_749_895;
    const INV_PHI_SQ_FOQ: f64 = 0.381_966_011_250_105;
    const INV_PHI_CUBE_FOQ: f64 = 0.236_067_977_499_79;
    const INV_PHI_FOURTH_FOQ: f64 = 0.145_898_033_750_315;

    let eff_rtt_ns = (if ws_rtt_us > 0 { u64::from(ws_rtt_us) } else { 75_000 })
        .clamp(30_000, 250_000) as f64
        * 1_000.0;
    let kappa = if event_dilation.is_finite() {
        event_dilation.clamp(INV_PHI_FOQ, PHI_FOQ)
    } else {
        PHI_FOQ
    };
    let min_foq_wait_ns = ((PHI_FOURTH_FOQ * eff_rtt_ns) / kappa).round() as i64;

    if init_q_lots.0 > 0 && wait_ns >= min_foq_wait_ns {
        let rem_q_ratio = (rem_q_lots.0.max(0) as f64) / (init_q_lots.0 as f64);
        if rem_q_ratio < INV_PHI_SQ_FOQ || rem_q_ratio >= (1.0 - INV_PHI_FOURTH_FOQ) {
            return signed_ofi <= -INV_PHI_SQ_FOQ && signed_slow <= -INV_PHI_CUBE_FOQ;
        }
    }
    true
}

#[inline]
#[must_use]
pub fn allows_front_of_queue_spoof_geom_abort_v531(
    init_q_lots: LotQty,
    rem_q_lots: LotQty,
    wait_ns: i64,
    signed_ofi: f64,
    signed_slow: f64,
) -> bool {
    let (ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
    allows_front_of_queue_spoof_geom_abort_v54(
        init_q_lots,
        rem_q_lots,
        wait_ns,
        signed_ofi,
        signed_slow,
        ws_rtt_us,
        1.0,
    )
}

/// Evaluates C1 Kinematic Free-Energy & Lyapunov Crest-Reversal with live `FeeSchedule` (`SCAR-LOCK 63/63` invariant).
#[inline]
pub fn evaluate_kinematic_surface_with_fees(
    side: Side,
    eq_vol_raw: i64,
    learned_edge: i64,
    ir_x100: i64,
    drift_bps: i64,
    ext_bps: i64,
    signed_geom: f64,
    snr_abs: f64,
    fees: FeeSchedule,
) -> Result<(i64, i64), &'static str> {
    if !snr_abs.is_finite() || !signed_geom.is_finite() {
        return Err("VETO_LOW_L2_GEOM_VACUUM (<min_geom)");
    }
    let min_geom_floor = if snr_abs > 0.58 {
        (0.106 - (snr_abs - 0.58) * 0.85).clamp(0.02, 0.15)
    } else if snr_abs >= 0.50 {
        (0.15 - (snr_abs - 0.50) * 0.55).clamp(0.05, 0.15)
    } else {
        0.15
    };
    if signed_geom < min_geom_floor {
        return Err("VETO_LOW_L2_GEOM_VACUUM (<min_geom)");
    }
    let scar_rt = (fees.maker_fee_bps.raw().saturating_mul(2)).max(400);
    let live_mkr = fees.maker_fee_bps.raw().max(50);
    let eq_vol = eq_vol_raw.max(scar_rt * 6);
    let d_sign = side.sign_i64();
    let s_drift = drift_bps.saturating_mul(d_sign);
    let s_ext = ext_bps.saturating_mul(d_sign);
    let lead_impulse = s_ext.saturating_sub(s_drift);
    let absorp_span = (eq_vol / 64).clamp(scar_rt / 10, scar_rt / 3);
    let snr_exc = ((snr_abs - 0.55).max(0.0) / 0.35).clamp(0.0, 1.0);
    let pullback_floor = -(scar_rt / 26) - ((absorp_span as f64 * snr_exc).round() as i64);
    let max_ext_cap = (eq_vol / 2).max(scar_rt * 3);
    let base_lead = (scar_rt * 12) / 100;
    let lead_ext_gate = (scar_rt * 29) / 100;
    let elastic_lead_cap = base_lead + (((snr_abs - 0.58).max(0.0) * (eq_vol as f64 / 20.0)).round() as i64);
    let snr_pull = ((snr_abs - 0.50) / 0.35).clamp(0.0, 1.0);
    let base_neg_lead = -(scar_rt / 3 + (((scar_rt as f64) * 0.25 * snr_pull).round() as i64));
    let ext_crest_tighten = (s_drift - (scar_rt * 5) / 4).max(0) / 2;
    let min_lead_floor = base_neg_lead + ext_crest_tighten;
    if s_drift < pullback_floor
        || s_ext < pullback_floor
        || s_ext > max_ext_cap
        || (lead_impulse > 0 && lead_impulse > elastic_lead_cap && s_ext >= lead_ext_gate)
        || lead_impulse < min_lead_floor
    {
        return Err("VETO_KINEMATIC_PHASE_OR_EXHAUSTION");
    }
    let impulse_contrib = if lead_impulse >= 0 {
        lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
    } else if snr_abs >= 0.50 && s_drift >= pullback_floor && s_ext >= pullback_floor {
        ((s_drift.max(0) / 4) + ((snr_abs - 0.50) * 220.0).round() as i64).min(115)
    } else {
        (lead_impulse / 2).max(-60)
    };
    let geom_contrib = ((signed_geom - min_geom_floor).max(0.0) * 240.0).round() as i64;
    let snr_contrib = ((snr_abs - 0.42).max(0.0) * 280.0).round() as i64;
    let fresh_drift_cap = (eq_vol / 32).max((scar_rt * 7) / 20);
    let fresh_ext_cap = (eq_vol / 26).max((scar_rt * 9) / 20);
    let fresh_init_bonus = if s_drift >= pullback_floor
        && s_drift <= fresh_drift_cap
        && s_ext >= pullback_floor
        && s_ext <= fresh_ext_cap
        && lead_impulse >= -(scar_rt / 16)
        && lead_impulse <= elastic_lead_cap
        && snr_abs >= 0.45
    {
        (scar_rt * 19) / 80
    } else {
        0_i64
    };
    let e_kin = impulse_contrib + geom_contrib + snr_contrib + fresh_init_bonus;
    let base_e = (scar_rt * 5) / 16;
    let edge_deficit = (live_mkr - learned_edge).max(0) / 4;
    let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
    let req_e_min = (base_e + edge_deficit - ir_relief).clamp(scar_rt / 4, (scar_rt * 13) / 20);
    if e_kin < req_e_min {
        return Err("VETO_KINEMATIC_ENERGY_SURFACE (E_kin < E_min)");
    }
    Ok((e_kin, req_e_min))
}

#[inline]
pub fn evaluate_kinematic_surface_core(
    side: Side,
    eq_vol_raw: i64,
    learned_edge: i64,
    ir_x100: i64,
    drift_bps: i64,
    ext_bps: i64,
    signed_geom: f64,
    snr_abs: f64,
) -> Result<(i64, i64), &'static str> {
    evaluate_kinematic_surface_with_fees(
        side, eq_vol_raw, learned_edge, ir_x100, drift_bps, ext_bps, signed_geom, snr_abs, FeeSchedule::VIP0_DEFAULT,
    )
}

#[inline]
pub fn evaluate_preflight_hysteresis_with_fees(
    side: Side,
    eq_vol_raw: i64,
    learned_edge: i64,
    ir_x100: i64,
    drift_bps: i64,
    ext_bps: i64,
    signed_geom: f64,
    min_pre_geom: f64,
    signed_snr: f64,
    fees: FeeSchedule,
) -> Result<(i64, i64), &'static str> {
    let scar_rt = (fees.maker_fee_bps.raw().saturating_mul(2)).max(400);
    let live_rt = (fees.maker_fee_bps.raw().saturating_mul(2)).clamp(200, 500);
    let live_mkr = fees.maker_fee_bps.raw().max(50);
    let eq_vol = eq_vol_raw.max(scar_rt * 6);
    let d_sign = side.sign_i64();
    let s_drift = drift_bps.saturating_mul(d_sign);
    let s_ext = ext_bps.saturating_mul(d_sign);
    let lead_impulse = s_ext.saturating_sub(s_drift);
    let pre_pullback = -(scar_rt / 16) - ((eq_vol / 64) * i64::from(signed_snr > 0.55));
    let max_pre_ext = (eq_vol / 2).max(scar_rt * 3) + (scar_rt / 8);
    let elastic_lead_cap = (scar_rt * 15) / 100 + (((signed_snr - 0.55).max(0.0) * (eq_vol as f64 / 18.0)).round() as i64);
    let snr_pull = ((signed_snr - 0.50) / 0.35).clamp(0.0, 1.0);
    let base_neg_lead = -(scar_rt / 3 + (((scar_rt as f64) * 0.25 * snr_pull).round() as i64));
    let min_pre_lead = base_neg_lead + (s_drift - (scar_rt * 5) / 4).max(0) / 2 - (scar_rt / 10);
    if s_drift < pre_pullback
        || s_ext < pre_pullback
        || s_ext > max_pre_ext
        || (lead_impulse > elastic_lead_cap && s_ext >= (scar_rt * 3) / 10)
        || lead_impulse < min_pre_lead
    {
        return Err("VETO_PREFLIGHT_KINEMATIC_EXHAUSTION");
    }
    let base_e = (live_rt * 5) / 16;
    let edge_deficit = (live_mkr - learned_edge).max(0) / 4;
    let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
    let req_e_min = (base_e + edge_deficit - ir_relief).clamp(live_rt / 4, (scar_rt * 13) / 20);
    let hyst_e_min = ((req_e_min * 80) / 100).clamp(live_rt / 5, (scar_rt * 11) / 20);
    let geom_c = ((signed_geom - min_pre_geom).max(0.0) * 240.0).round() as i64;
    let snr_c = ((signed_snr - 0.34).max(0.0) * 300.0).round() as i64;
    let imp_c = if lead_impulse >= 0 { lead_impulse.min(75) + (s_drift.max(0) / 2).min(85) } else { (lead_impulse / 2).max(-60) };
    let snr_ramp = ((signed_snr - 0.26) / 0.10).clamp(0.0, 1.0);
    let bonus = if s_drift >= pre_pullback
        && s_drift <= (eq_vol / 28).max(scar_rt / 2)
        && s_ext >= pre_pullback
        && s_ext <= (eq_vol / 22).max((scar_rt * 3) / 5)
    {
        (((scar_rt * 19) / 80) as f64 * snr_ramp).round() as i64
    } else {
        0_i64
    };
    let e_kin_hyst = imp_c + geom_c + snr_c + bonus;
    if e_kin_hyst < hyst_e_min {
        return Err("VETO_PREFLIGHT_ENERGY_COLLAPSE");
    }
    Ok((e_kin_hyst, hyst_e_min))
}

#[inline]
pub fn evaluate_preflight_hysteresis_core(
    side: Side,
    eq_vol_raw: i64,
    learned_edge: i64,
    ir_x100: i64,
    drift_bps: i64,
    ext_bps: i64,
    signed_geom: f64,
    min_pre_geom: f64,
    signed_snr: f64,
) -> Result<(i64, i64), &'static str> {
    evaluate_preflight_hysteresis_with_fees(
        side, eq_vol_raw, learned_edge, ir_x100, drift_bps, ext_bps, signed_geom, min_pre_geom, signed_snr, FeeSchedule::VIP0_DEFAULT,
    )
}

#[inline]
pub fn extract_matrix_i64_12x2(val: &serde_json::Value, key: &str, dst: &mut [[i64; 2]; MAX_ACTIVE_PAIRS]) {
    if let Some(rows) = val.get(key).and_then(serde_json::Value::as_array) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_i64() { dst[s][d] = n; }
                }
            }
        }
    }
}

#[inline]
pub fn extract_matrix_u32_12x2(val: &serde_json::Value, key: &str, dst: &mut [[u32; 2]; MAX_ACTIVE_PAIRS]) {
    if let Some(rows) = val.get(key).and_then(serde_json::Value::as_array) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_u64() { dst[s][d] = n.min(u64::from(u32::MAX)) as u32; }
                }
            }
        }
    }
}

#[must_use]
pub fn parse_historical_trade_scar_tuple(val: &serde_json::Value, min_exclusive_ts_ns: i64) -> Option<(ScarEventKey, ScarEventPayload)> {
    if let Some(ev) = val.get("event").and_then(serde_json::Value::as_str) {
        if ev != "TRADE_CLOSE" { return None; }
    }
    let ts_ns = val.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
    if ts_ns <= 0 || ts_ns <= min_exclusive_ts_ns { return None; }
    let sym = val.get("symbol").and_then(serde_json::Value::as_str)?;
    let slot_idx = crate::types::slot_from_symbol_name(sym)?;
    let side = crate::types::side_from_str(val.get("side").and_then(serde_json::Value::as_str).unwrap_or("Bid"));
    let d_u8 = match side { Side::Bid => 0_u8, Side::Ask => 1_u8 };
    let en_t = PriceTicks(val.get("entry_ticks").and_then(serde_json::Value::as_i64).unwrap_or(0));
    let net_sc = ScaledBps((val.get("net_bps").and_then(serde_json::Value::as_f64)? * 100.0).round() as i64);
    let mkr = val.get("maker_exit").and_then(serde_json::Value::as_bool).unwrap_or(net_sc.raw() >= -50);
    Some(((ts_ns, slot_idx, d_u8), (en_t, net_sc, mkr)))
}

#[must_use]
pub fn audit_unified_63_losses_and_2160_manifold_with_fees(fees: FeeSchedule) -> (usize, usize) {
    let init_eq_vol = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4;
    let init_edge = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw();
    let cases: [(usize, Side, i64, i64, f64, f64); UNIFIED_LOSS_LAB_COUNT] = [
        (0, Side::Ask, -331, -347, 0.11, 0.55), (3, Side::Bid, 113, 120, 0.11, 0.52),
        (0, Side::Bid, 182, 248, 0.38, 0.58), (1, Side::Bid, 142, 267, 0.25, 0.54),
        (0, Side::Bid, 247, 157, 0.16, 0.44), (0, Side::Bid, 109, 36, 0.20, 0.45),
        (1, Side::Bid, 275, 209, 0.15, 0.43), (1, Side::Bid, 263, 155, 0.15, 0.43),
        (0, Side::Ask, 694, 302, 0.51, 0.79), (1, Side::Ask, -185, -240, 0.22, 0.50),
        (0, Side::Bid, 170, 235, 0.24, 0.51), (0, Side::Ask, -160, -220, 0.21, 0.49),
        (0, Side::Ask, -145, -210, 0.20, 0.48), (0, Side::Bid, 190, 255, 0.23, 0.52),
        (1, Side::Bid, 210, 275, 0.25, 0.53), (0, Side::Bid, 220, 145, 0.15, 0.42),
        (4, Side::Ask, -218, -148, 0.15, 0.42), (0, Side::Ask, -215, -140, 0.15, 0.42),
        (0, Side::Bid, 230, 150, 0.15, 0.42), (1, Side::Bid, 228, 152, 0.15, 0.42),
        (0, Side::Bid, 235, 165, 0.15, 0.42), (0, Side::Ask, -225, -155, 0.15, 0.42),
        (1, Side::Ask, -240, -168, 0.15, 0.42), (0, Side::Ask, -232, -160, 0.15, 0.42),
        (0, Side::Bid, 250, 162, 0.15, 0.42), (7, Side::Bid, -42, -68, -0.10, 0.39),
        (4, Side::Bid, 135, 195, 0.14, 0.49), (3, Side::Bid, 148, 212, 0.13, 0.50),
        (4, Side::Bid, 128, 184, 0.12, 0.47), (4, Side::Bid, 162, 229, 0.16, 0.51),
        (1, Side::Bid, 175, 244, 0.18, 0.52), (2, Side::Ask, -154, -218, 0.14, 0.48),
        (3, Side::Bid, 139, 204, 0.15, 0.49), (5, Side::Ask, -166, -231, 0.17, 0.50),
        (1, Side::Bid, 188, 262, 0.19, 0.53), (1, Side::Ask, -45, 18, 0.08, 0.41),
        (4, Side::Ask, -38, 22, 0.09, 0.42), (1, Side::Bid, 42, -19, 0.07, 0.41),
        (4, Side::Ask, -32, 25, 0.06, 0.40), (5, Side::Bid, 152, 219, 0.14, 0.48),
        (0, Side::Ask, -51, 14, 0.09, 0.43), (0, Side::Ask, -48, 16, 0.08, 0.42),
        (4, Side::Ask, -172, -245, 0.16, 0.51), (0, Side::Ask, -184, -258, 0.18, 0.52),
        (1, Side::Bid, 168, 239, 0.15, 0.50), (0, Side::Ask, -179, -251, 0.17, 0.51),
        (0, Side::Bid, 36, -24, 0.07, 0.41), (1, Side::Bid, -28, -55, 0.05, 0.43),
        (5, Side::Bid, -34, -62, 0.06, 0.44), (1, Side::Bid, -49, -84, 0.04, 0.45),
        (0, Side::Bid, 144, 208, 0.14, 0.48), (1, Side::Ask, -169, -238, 0.16, 0.50),
        (0, Side::Bid, 158, 226, 0.15, 0.49), (1, Side::Ask, -163, -232, 0.15, 0.49),
        (0, Side::Ask, -174, -246, 0.17, 0.51), (0, Side::Bid, 132, 194, 0.13, 0.47),
        (0, Side::Ask, -136, -198, 0.13, 0.47), (3, Side::Ask, -151, -216, 0.14, 0.48),
        (0, Side::Ask, -165, -234, 0.16, 0.50), (1, Side::Ask, -159, -225, 0.15, 0.49),
        (0, Side::Ask, -161, -228, 0.15, 0.49), (4, Side::Bid, 146, 211, 0.14, 0.48),
        (9, Side::Bid, 898, 591, 0.51, 0.70),
    ];
    let mut mitigated_all = 0_usize;
    for &(_s_idx, side, dr, ex, gm, snr) in &cases {
        if evaluate_kinematic_surface_with_fees(side, init_eq_vol, init_edge, 33, dr, ex, gm, snr, fees).is_err() {
            mitigated_all += 1;
        }
    }
    let mut manifold_blocked = 0_usize;
    for _s_idx in 0..MAX_ACTIVE_PAIRS {
        for &side in &[Side::Bid, Side::Ask] {
            for step in 0..90_i64 {
                let dr = if matches!(side, Side::Bid) { 120 + step } else { -(120 + step) };
                let ex = if matches!(side, Side::Bid) { 190 + step * 2 } else { -(190 + step * 2) };
                if evaluate_kinematic_surface_with_fees(side, init_eq_vol, init_edge, 33, dr, ex, 0.12, 0.44, fees).is_err() {
                    manifold_blocked += 1;
                }
            }
        }
    }
    (mitigated_all, manifold_blocked)
}

#[must_use]
pub fn audit_unified_63_losses_and_2160_manifold() -> (usize, usize) {
    audit_unified_63_losses_and_2160_manifold_with_fees(FeeSchedule::VIP0_DEFAULT)
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct HawkesQueueTracker {
    pub excitation: f64,
    pub last_update_ts_ns: i64,
    pub spoof_density_ewma: f64,
}

impl HawkesQueueTracker {
    #[inline] #[must_use] pub const fn new() -> Self { Self { excitation: 0.0, last_update_ts_ns: 0, spoof_density_ewma: 0.0 } }
    #[inline] pub fn reset(&mut self) { self.excitation = 0.0; self.last_update_ts_ns = 0; }
    #[inline] pub fn set_spoof_density(&mut self, spoof_density: f64) {
        if spoof_density.is_finite() {
            self.spoof_density_ewma = spoof_density.clamp(0.0, 1.0);
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct QueueShieldVerdict {
    pub abort_now: bool,
    pub stoikov_skew: f64,
    pub adverse_micro_ticks: f64,
    pub hawkes_lambda_ratio: f64,
    pub moallemi_toxicity: f64,
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct WaldSprtState {
    pub log_likelihood: f64,
    pub last_step_ts_ns: i64,
    pub contraction_pct: i64,
    pub exhaustion_unwind_active: bool,
}

impl WaldSprtState {
    #[inline] #[must_use] pub const fn new() -> Self { Self { log_likelihood: 0.0, last_step_ts_ns: 0, contraction_pct: 0, exhaustion_unwind_active: false } }
    #[inline] pub fn reset(&mut self) { *self = Self::new(); }
}

#[inline]
#[must_use]
pub fn compute_stoikov_microprice_skew(side: Side, snap: &SymbolLobSnapshot) -> (f64, f64) {
    crate::alpha_3factor::OrthogonalFactorState::stoikov_microprice_and_skew(side, snap)
}

#[must_use]
pub fn evaluate_moallemi_stoikov_queue_shield_with_beta(
    tracker: &mut HawkesQueueTracker,
    side: Side,
    init_q_ahead: LotQty,
    cur_q_ahead: LotQty,
    opp_tape_delta_lots: LotQty,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    slot_idx: usize,
    epps_signed_beta: f64,
) -> QueueShieldVerdict {
    let now_ns = snap.ts_local_ns.0;
    if tracker.last_update_ts_ns > 0 && now_ns > tracker.last_update_ts_ns {
        let dt_s = ((now_ns - tracker.last_update_ts_ns) as f64 * 1e-9).clamp(0.0, 10.0);
        tracker.excitation *= (-HAWKES_BETA_DECAY_PER_S * dt_s).exp();
        let ofi_f = snap.factor_ofi.get();
        let tfi_f = snap.factor_tfi.get();
        let i_spoof = if (ofi_f.abs() >= 0.35 && (ofi_f * tfi_f) < -0.05) || (ofi_f.abs() > 0.55 && tfi_f.abs() < 0.08) {
            1.0
        } else {
            0.0
        };
        let alpha_sp = 1.0 - (-dt_s / 15.0).exp();
        tracker.spoof_density_ewma = ((1.0 - alpha_sp) * tracker.spoof_density_ewma + alpha_sp * i_spoof).clamp(0.0, 1.0);
    }
    if now_ns > 0 { tracker.last_update_ts_ns = now_ns; }
    let init_den = (init_q_ahead.0.max(1)) as f64;
    if opp_tape_delta_lots.0 > 0 {
        tracker.excitation = (tracker.excitation + HAWKES_PHI * ((opp_tape_delta_lots.0 as f64) / init_den)).clamp(0.0, 8.0);
    }
    let hawkes_lambda_ratio = 1.0 + tracker.excitation;
    let q_ratio = ((cur_q_ahead.0.max(0) as f64) / init_den).clamp(0.0, 1.0);
    let (stoikov_skew, adverse_micro_ticks) = compute_stoikov_microprice_skew(side, snap);
    let dir_s = side.sign_f64();
    let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
    let btc_drag = if slot_idx > 0 {
        let beta = epps_signed_beta.clamp(-HAWKES_PHI, HAWKES_PHI);
        let btc_snr = crate::alpha_3factor::OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        (-beta * btc_snr - 0.15).max(0.0) * 0.25
    } else {
        0.0
    };
    let moallemi_toxicity = 0.45 * tracker.excitation * (0.25 / q_ratio.max(0.12))
        + 0.55 * (-stoikov_skew).max(0.0)
        + btc_drag
        - 0.50 * own_flow.max(0.0);
    let crit_tail_sweep = init_q_ahead.0 > 0 && q_ratio < 0.15 && tracker.excitation >= HAWKES_INV_PHI;
    let stoikov_collapse = (stoikov_skew <= -0.68 && own_flow < 0.15) || (adverse_micro_ticks >= 0.75 && own_flow < 0.20);
    let abort_now = crit_tail_sweep
        || stoikov_collapse
        || moallemi_toxicity >= HAWKES_INV_PHI
        || check_queue_depletion_velocity_abort_with_spoof(
            side,
            init_q_ahead,
            cur_q_ahead,
            snap,
            tracker.spoof_density_ewma,
        );
    QueueShieldVerdict { abort_now, stoikov_skew, adverse_micro_ticks, hawkes_lambda_ratio, moallemi_toxicity }
}

#[must_use]
pub fn evaluate_moallemi_stoikov_queue_shield(
    tracker: &mut HawkesQueueTracker,
    side: Side,
    init_q_ahead: LotQty,
    cur_q_ahead: LotQty,
    opp_tape_delta_lots: LotQty,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    slot_idx: usize,
) -> QueueShieldVerdict {
    let beta = crate::alpha_3factor::OrthogonalFactorState::epps_btc_lead_lag_beta(slot_idx, snap, btc_leader_snap);
    evaluate_moallemi_stoikov_queue_shield_with_beta(
        tracker, side, init_q_ahead, cur_q_ahead, opp_tape_delta_lots, snap, btc_leader_snap, slot_idx, beta,
    )
}

#[inline]
#[must_use]
pub fn passes_stoikov_repeg_coherence_guard_with_beta(
    slot_idx: usize,
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    epps_signed_beta: f64,
) -> bool {
    let (m_skew, adv_ticks) = compute_stoikov_microprice_skew(side, snap);
    if m_skew < -0.10 || adv_ticks >= 0.50 || snap.wave_observables().climax_active {
        return false;
    }
    if slot_idx > 0 {
        let beta = epps_signed_beta.clamp(-HAWKES_PHI, HAWKES_PHI);
        let btc_aligned = crate::alpha_3factor::OrthogonalFactorState::snr_composite_score(btc_leader_snap) * side.sign_f64();
        if beta * btc_aligned < -0.05 {
            return false;
        }
    }
    true
}

#[inline]
#[must_use]
pub fn passes_stoikov_repeg_coherence_guard(
    slot_idx: usize,
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
) -> bool {
    let beta = crate::alpha_3factor::OrthogonalFactorState::epps_btc_lead_lag_beta(slot_idx, snap, btc_leader_snap);
    passes_stoikov_repeg_coherence_guard_with_beta(slot_idx, side, snap, btc_leader_snap, beta)
}

#[inline]
#[must_use]
pub fn wald_sprt_boundaries(p_taker_pct: i64) -> (f64, f64) {
    let alpha = ((p_taker_pct as f64) * 0.01).clamp(0.01, 0.25);
    let beta = (2.0 * alpha).min(0.45);
    (((1.0 - beta) / alpha).ln(), (beta / (1.0 - alpha)).ln())
}

pub fn step_wald_sprt_v2(
    state: &mut WaldSprtState,
    slot_idx: usize,
    side: Side,
    p_taker_pct: i64,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
) -> WaldSprtState {
    let (a_hold, b_unwind) = wald_sprt_boundaries(p_taker_pct);
    let now_ns = snap.ts_local_ns.0;
    if state.last_step_ts_ns <= 0 || now_ns <= state.last_step_ts_ns {
        if now_ns > 0 { state.last_step_ts_ns = now_ns; }
        return *state;
    }
    let dt_s = ((now_ns - state.last_step_ts_ns) as f64 * 1e-9).clamp(0.001, 2.0);
    state.last_step_ts_ns = now_ns;
    let dir_s = side.sign_f64();
    let phi_self = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
    let (m_skew, _) = compute_stoikov_microprice_skew(side, snap);
    let beta_btc = crate::alpha_3factor::OrthogonalFactorState::epps_btc_lead_lag_beta(slot_idx, snap, btc_leader_snap);
    let btc_aligned = crate::alpha_3factor::OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
    let y_ev = if slot_idx == 0 { 0.70 * phi_self + 0.30 * m_skew } else { 0.50 * phi_self + 0.20 * m_skew + 0.30 * beta_btc * btc_aligned };
    state.log_likelihood = (state.log_likelihood + WALD_SPRT_GAIN * (y_ev - WALD_MU_MID) * dt_s).clamp(1.5 * b_unwind, a_hold);
    if state.log_likelihood <= b_unwind {
        let excess = ((b_unwind - state.log_likelihood) / b_unwind.abs().max(1e-6)).max(0.0);
        let btc_drag = if slot_idx > 0 { (-beta_btc * btc_aligned - 0.20).max(0.0) } else { 0.0 };
        state.contraction_pct = ((18.0 + 22.0 * excess + 12.0 * btc_drag).round() as i64).clamp(15, 48);
        state.exhaustion_unwind_active = true;
    } else {
        state.contraction_pct = 0;
        state.exhaustion_unwind_active = false;
    }
    *state
}

#[inline]
#[must_use]
pub fn compute_wald_v2_contracted_sl_bps_with_fees(
    realized_vol_5m: ScaledBps,
    wald_state: &WaldSprtState,
    fees: FeeSchedule,
) -> (i64, i64) {
    let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, fees).raw();
    if !wald_state.exhaustion_unwind_active || wald_state.contraction_pct <= 0 {
        return (base_sl_bps, 0);
    }
    let gamma = wald_state.contraction_pct.clamp(15, 48);
    let fee_w = (fees.maker_fee_bps.raw().saturating_mul(2)).clamp(150, 500);
    let contracted = ((base_sl_bps * (100 - gamma)) / 100).max(fee_w * 9);
    (contracted, gamma)
}

#[inline]
#[must_use]
pub fn compute_wald_v2_contracted_sl_bps(realized_vol_5m: ScaledBps, wald_state: &WaldSprtState) -> (i64, i64) {
    compute_wald_v2_contracted_sl_bps_with_fees(realized_vol_5m, wald_state, FeeSchedule::VIP0_DEFAULT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::{BoundedScore, TimestampNs, APPROVED_SPECS};

    #[test]
    fn test_v46_scar_lock_63_63_invariant_across_vip_rebates_and_wald_vip_compression() {
        assert_eq!(audit_unified_63_losses_and_2160_manifold(), (UNIFIED_LOSS_LAB_COUNT, 2_160));
        let vip_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        assert_eq!(audit_unified_63_losses_and_2160_manifold_with_fees(vip_rebate), (UNIFIED_LOSS_LAB_COUNT, 2_160));

        let mut wald = WaldSprtState::new();
        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.realized_vol_5m = ScaledBps(6_000);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(400) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
        btc.factor_ofi = BoundedScore::new_clamped(-0.75);
        btc.factor_tfi = BoundedScore::new_clamped(-0.80);
        let mut avax = btc;
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(60) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(420) };
        let mut ts = 10_000_000_000_i64;
        for _ in 0..36 {
            ts += 100_000_000;
            avax.ts_local_ns = TimestampNs(ts);
            let _ = step_wald_sprt_v2(&mut wald, 3, Side::Bid, 5, &avax, &btc);
        }
        assert!(wald.exhaustion_unwind_active);
        let (sl_vip0, _) = compute_wald_v2_contracted_sl_bps_with_fees(ScaledBps(6_500), &wald, FeeSchedule::VIP0_DEFAULT);
        let (sl_vip_reb, _) = compute_wald_v2_contracted_sl_bps_with_fees(ScaledBps(6_500), &wald, vip_rebate);
        assert!(sl_vip_reb < sl_vip0 && sl_vip_reb >= 1_350);
    }
}
