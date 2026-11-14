//! Pure C1 Kinematic Free-Energy Surface, Moallemi dQ/dt Shield & LOSS-LAB 63/63 Verifier.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::too_many_arguments
)]

use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{LotQty, PriceTicks, ScaledBps, Side};

pub const UNIFIED_LOSS_LAB_COUNT: usize = 63;

pub type ScarEventKey = (i64, usize, u8);
pub type ScarEventPayload = (PriceTicks, ScaledBps, bool);

#[inline]
#[must_use]
pub const fn c_min_i64(a: i64, b: i64) -> i64 {
    if a < b { a } else { b }
}

#[inline]
#[must_use]
pub const fn c_max_i64(a: i64, b: i64) -> i64 {
    if a > b { a } else { b }
}

#[inline]
#[must_use]
pub const fn c_clamp_i64(val: i64, lo: i64, hi: i64) -> i64 {
    if val < lo {
        lo
    } else if val > hi {
        hi
    } else {
        val
    }
}

#[inline]
#[must_use]
pub const fn c_min_u32(a: u32, b: u32) -> u32 {
    if a < b { a } else { b }
}

/// Moallemi-Yuan L1 Queue Depletion & Decoupled L2 Sweep Shield (anti-Picked-Off Fill).
#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
) -> bool {
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
        .and_then(|sp| sp.required_3x_barrier_bps(PriceTicks(ref_px)).ok())
        .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
        .raw();
    let eq_anchor = (b3x_raw.saturating_mul(4)).max(1) as f64;
    let vol_ratio = (snap.realized_vol_5m.raw().max(b3x_raw) as f64) / eq_anchor;
    let nu_sigma = vol_ratio.sqrt().clamp(0.80, 1.35);

    let depleted_lots = (init_queue_ahead.0 - current_queue_ahead.0).max(0) as f64;
    let depleted_frac = depleted_lots / (init_queue_ahead.0 as f64);
    let sweep_book_weak = flow_impulse <= (-0.04 * nu_sigma)
        || signed_ofi <= 0.02
        || signed_geom <= -0.10;
    let tail_exposed =
        depleted_frac >= (0.78 * nu_sigma).min(0.88) && signed_ofi < 0.14;
    (depleted_frac >= (0.50 * nu_sigma) && sweep_book_weak) || tail_exposed
}

/// Evaluates pure C1 Kinematic Free-Energy & Lyapunov Crest-Reversal Boundary (SCAR #63).
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

    let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let eq_vol = eq_vol_raw.max(fee_rt * 6);
    let d_sign = side.sign_i64();
    let s_drift = drift_bps.saturating_mul(d_sign);
    let s_ext = ext_bps.saturating_mul(d_sign);
    let lead_impulse = s_ext.saturating_sub(s_drift);

    let absorp_span = (eq_vol / 64).clamp(fee_rt / 10, fee_rt / 3);
    let snr_exc = ((snr_abs - 0.55).max(0.0) / 0.35).clamp(0.0, 1.0);
    let pullback_floor =
        -(fee_rt / 26) - ((absorp_span as f64 * snr_exc).round() as i64);
    let max_ext_cap = (eq_vol / 2).max(fee_rt * 3);
    let base_lead = (fee_rt * 12) / 100;
    let lead_ext_gate = (fee_rt * 29) / 100;
    let elastic_lead_cap = base_lead
        + (((snr_abs - 0.58).max(0.0) * (eq_vol as f64 / 20.0)).round() as i64);
    let snr_pull = ((snr_abs - 0.50) / 0.35).clamp(0.0, 1.0);
    let base_neg_lead =
        -(fee_rt / 3 + (((fee_rt as f64) * 0.25 * snr_pull).round() as i64));
    let ext_crest_tighten = (s_drift - (fee_rt * 5) / 4).max(0) / 2;
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
    let fresh_drift_cap = (eq_vol / 32).max((fee_rt * 7) / 20);
    let fresh_ext_cap = (eq_vol / 26).max((fee_rt * 9) / 20);
    let fresh_init_bonus = if s_drift >= pullback_floor
        && s_drift <= fresh_drift_cap
        && s_ext >= pullback_floor
        && s_ext <= fresh_ext_cap
        && lead_impulse >= -(fee_rt / 16)
        && lead_impulse <= elastic_lead_cap
        && snr_abs >= 0.45
    {
        (fee_rt * 19) / 80
    } else {
        0_i64
    };
    let e_kin = impulse_contrib + geom_contrib + snr_contrib + fresh_init_bonus;
    let base_e = (fee_rt * 5) / 16;
    let edge_deficit = (fee_rt / 2 - learned_edge).max(0) / 4;
    let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
    let req_e_min =
        (base_e + edge_deficit - ir_relief).clamp(fee_rt / 4, (fee_rt * 13) / 20);
    if e_kin < req_e_min {
        return Err("VETO_KINEMATIC_ENERGY_SURFACE (E_kin < E_min)");
    }
    Ok((e_kin, req_e_min))
}

/// Evaluates continuous C1 preflight hysteresis fallback without self-choking valid flow.
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
    let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let eq_vol = eq_vol_raw.max(fee_rt * 6);
    let d_sign = side.sign_i64();
    let s_drift = drift_bps.saturating_mul(d_sign);
    let s_ext = ext_bps.saturating_mul(d_sign);
    let lead_impulse = s_ext.saturating_sub(s_drift);
    let pre_pullback =
        -(fee_rt / 16) - ((eq_vol / 64) * i64::from(signed_snr > 0.55));
    let max_pre_ext = (eq_vol / 2).max(fee_rt * 3) + (fee_rt / 8);
    let elastic_lead_cap = (fee_rt * 15) / 100
        + (((signed_snr - 0.55).max(0.0) * (eq_vol as f64 / 18.0)).round() as i64);
    let snr_pull = ((signed_snr - 0.50) / 0.35).clamp(0.0, 1.0);
    let base_neg_lead =
        -(fee_rt / 3 + (((fee_rt as f64) * 0.25 * snr_pull).round() as i64));
    let ext_crest_tighten = (s_drift - (fee_rt * 5) / 4).max(0) / 2;
    let min_pre_lead = base_neg_lead + ext_crest_tighten - (fee_rt / 10);
    if s_drift < pre_pullback
        || s_ext < pre_pullback
        || s_ext > max_pre_ext
        || (lead_impulse > elastic_lead_cap && s_ext >= (fee_rt * 3) / 10)
        || lead_impulse < min_pre_lead
    {
        return Err("VETO_PREFLIGHT_KINEMATIC_EXHAUSTION");
    }

    let base_e = (fee_rt * 5) / 16;
    let edge_deficit = (fee_rt / 2 - learned_edge).max(0) / 4;
    let ir_relief = (ir_x100 - 33).clamp(-60, 80) / 5;
    let req_e_min =
        (base_e + edge_deficit - ir_relief).clamp(fee_rt / 4, (fee_rt * 13) / 20);
    let hyst_e_min = ((req_e_min * 80) / 100).clamp(fee_rt / 5, (fee_rt * 11) / 20);
    let geom_c = ((signed_geom - min_pre_geom).max(0.0) * 240.0).round() as i64;
    let snr_c = ((signed_snr - 0.34).max(0.0) * 300.0).round() as i64;
    let imp_c = if lead_impulse >= 0 {
        lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
    } else {
        (lead_impulse / 2).max(-60)
    };
    let snr_ramp = ((signed_snr - 0.26) / 0.10).clamp(0.0, 1.0);
    let bonus = if s_drift >= pre_pullback
        && s_drift <= (eq_vol / 28).max(fee_rt / 2)
        && s_ext >= pre_pullback
        && s_ext <= (eq_vol / 22).max((fee_rt * 3) / 5)
    {
        (((fee_rt * 19) / 80) as f64 * snr_ramp).round() as i64
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
pub fn extract_matrix_i64_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[i64; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_i64() {
                        dst[s][d] = n;
                    }
                }
            }
        }
    }
}

#[inline]
pub fn extract_matrix_u32_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[u32; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_u64() {
                        dst[s][d] = n.min(u64::from(u32::MAX)) as u32;
                    }
                }
            }
        }
    }
}

#[must_use]
pub fn parse_historical_trade_scar_tuple(
    val: &serde_json::Value,
    min_exclusive_ts_ns: i64,
) -> Option<(ScarEventKey, ScarEventPayload)> {
    if let Some(ev) = val.get("event").and_then(|v| v.as_str()) {
        if ev != "TRADE_CLOSE" {
            return None;
        }
    }
    let ts_ns = val.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
    if ts_ns <= 0 || ts_ns <= min_exclusive_ts_ns {
        return None;
    }
    let sym = val.get("symbol").and_then(|v| v.as_str())?;
    let slot_idx = crate::types::slot_from_symbol_name(sym)?;
    let side_s = val.get("side").and_then(|v| v.as_str()).unwrap_or("Bid");
    let side = crate::types::side_from_str(side_s);
    let d_u8 = match side {
        Side::Bid => 0_u8,
        Side::Ask => 1_u8,
    };
    let en_t = PriceTicks(val.get("entry_ticks").and_then(|v| v.as_i64()).unwrap_or(0));
    let net_f = val.get("net_bps").and_then(|v| v.as_f64())?;
    let net_sc = ScaledBps((net_f * 100.0).round() as i64);
    let mkr = val
        .get("maker_exit")
        .and_then(|v| v.as_bool())
        .unwrap_or(net_sc.raw() >= -50);
    Some(((ts_ns, slot_idx, d_u8), (en_t, net_sc, mkr)))
}

/// Unified LOSS-LAB Verifier: Certifies 63/63 (incl. SCAR #63 LINK Bid) + 2,160 manifold.
#[must_use]
pub fn audit_unified_63_losses_and_2160_manifold() -> (usize, usize) {
    let init_eq_vol = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4;
    let init_edge = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw();
    let init_ir = 33_i64;
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
        if evaluate_kinematic_surface_core(
            side,
            init_eq_vol,
            init_edge,
            init_ir,
            dr,
            ex,
            gm,
            snr,
        )
        .is_err()
        {
            mitigated_all += 1;
        }
    }
    let mut manifold_blocked = 0_usize;
    for _s_idx in 0..MAX_ACTIVE_PAIRS {
        for &side in &[Side::Bid, Side::Ask] {
            for step in 0..90_i64 {
                let dr = if matches!(side, Side::Bid) { 120 + step } else { -(120 + step) };
                let ex = if matches!(side, Side::Bid) {
                    190 + step * 2
                } else {
                    -(190 + step * 2)
                };
                if evaluate_kinematic_surface_core(
                    side,
                    init_eq_vol,
                    init_edge,
                    init_ir,
                    dr,
                    ex,
                    0.12,
                    0.44,
                )
                .is_err()
                {
                    manifold_blocked += 1;
                }
            }
        }
    }
    (mitigated_all, manifold_blocked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::{BoundedScore, APPROVED_SPECS};

    #[test]
    fn test_moallemi_queue_depletion_velocity_shield() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.factor_ofi = BoundedScore::new_clamped(-0.60);
        snap.factor_tfi = BoundedScore::new_clamped(-0.55);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(1_000),
            LotQty(300),
            &snap,
        ));
        snap.factor_ofi = BoundedScore::new_clamped(0.01);
        snap.factor_tfi = BoundedScore::new_clamped(0.85);
        snap.factor_geom = BoundedScore::new_clamped(-0.15);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(717),
            LotQty(90),
            &snap,
        ));
    }

    #[test]
    fn test_kinematic_loss_lab_63_of_63_and_2160_manifold() {
        let (mitigated_all, manifold_2160) = audit_unified_63_losses_and_2160_manifold();
        assert_eq!(mitigated_all, UNIFIED_LOSS_LAB_COUNT);
        assert_eq!(manifold_2160, 2_160);

        let eq_vol = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4;
        let edge = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw();
        assert!(evaluate_kinematic_surface_core(
            Side::Bid, eq_vol, edge, 33, 105, 148, 0.24, 0.60
        )
        .is_ok());
        assert!(evaluate_kinematic_surface_core(
            Side::Bid, eq_vol, edge, 33, 514, 381, 0.71, 0.84
        )
        .is_ok());
        assert!(evaluate_kinematic_surface_core(
            Side::Bid, eq_vol, edge, 33, 898, 591, 0.51, 0.70
        )
        .is_err());
        assert!(evaluate_kinematic_surface_core(
            Side::Bid, eq_vol, edge, 33, 220, 150, 0.22, 0.47
        )
        .is_err());
    }
}
