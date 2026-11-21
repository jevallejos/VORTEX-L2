//! Zero-Allocation 5-Horizon Post-Fill LOB-LVR Markout, Glosten-Milgrom Toxicity & Per-Veto Abort Auditor (+250ms..+60s, V4.9).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use crate::lob_shm::LastVetoCode;
use crate::types::{LotQty, PriceTicks, ScaledBps, Side, TimestampNs};

pub const MARKOUT_HORIZONS_V49: usize = 5;
pub const MARKOUT_HORIZONS_V49_NS: [i64; MARKOUT_HORIZONS_V49] = [
    250_000_000,
    1_000_000_000,
    5_000_000_000,
    15_000_000_000,
    60_000_000_000,
];
pub const MARKOUT_MASK_15S: u8 = 0x0F;
pub const MARKOUT_MASK_60S: u8 = 0x1F;
pub const VETO_CODE_CARDINALITY: usize = 13;

const PROBE_RING_CAP: usize = 12;
const DRAIN_ABORT_CAP: usize = 8;
const PENDING_LEARN_RING_CAP: usize = 4;
const LVR_EWMA_ETA_PM: i64 = 125;

const PHI_MK: f64 = 1.618_033_988_749_895;
const INV_PHI_MK: f64 = 0.618_033_988_749_895;
const INV_PHI_SQ_MK: f64 = 0.381_966_011_250_105;
const INV_PHI_CUBE_MK: f64 = 0.236_067_977_499_79;
const DIFFUSIVE_REF_WINDOW_SEC: f64 = 300.0;
const MARKOUT_HORIZONS_SEC: [f64; MARKOUT_HORIZONS_V49] = [0.25, 1.0, 5.0, 15.0, 60.0];
const HORIZON_WEIGHTS_4H: [f64; 4] = [INV_PHI_CUBE_MK, INV_PHI_SQ_MK, INV_PHI_MK, 1.0];

/// Legacy 4-horizon markout curve preserved 1:1 for backwards compatibility (`+250ms..+15s`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkoutCurveBps {
    pub samples_bps: [i64; 4],
    pub resolved_mask: u8,
    pub initial_edge_bps: i64,
}

/// Extended V4.9 5-horizon markout curve (`+250ms, +1s, +5s, +15s, +60s`) with explicit mask & peak toxicity.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkoutCurve5hBps {
    pub samples_bps: [i64; MARKOUT_HORIZONS_V49],
    pub resolved_mask: u8,
    pub initial_edge_bps: i64,
    pub peak_zeta_tox_pm: u16,
}

impl MarkoutCurve5hBps {
    #[inline]
    #[must_use]
    pub const fn to_legacy_4h(self) -> MarkoutCurveBps {
        MarkoutCurveBps {
            samples_bps: [
                self.samples_bps[0],
                self.samples_bps[1],
                self.samples_bps[2],
                self.samples_bps[3],
            ],
            resolved_mask: self.resolved_mask & MARKOUT_MASK_15S,
            initial_edge_bps: self.initial_edge_bps,
        }
    }
}

/// Completed counterfactual queue-abort record ready for off-thread `vault_io` persistence.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResolvedAbortSample {
    pub side: Side,
    pub veto_code: LastVetoCode,
    pub armed_ts_ns: TimestampNs,
    pub aborted_ts_ns: TimestampNs,
    pub limit_ticks: PriceTicks,
    pub init_q_lots: LotQty,
    pub rem_q_lots: LotQty,
    pub saved_tox_bps: [ScaledBps; MARKOUT_HORIZONS_V49],
    pub resolved_mask: u8,
    pub is_true_positive: bool,
}

/// Brownian diffusive Z-score Glosten-Milgrom toxicity functional (`+250ms..+15s`, V5.5.4)
/// with `sigma_h = max(INV_PHI * c_rt_mkr, vol5m_at_fill * sqrt(dt_h / 300.0))` and `C1` `+15s` equilibrium-reversion attenuation.
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta_diffusive(
    curve: MarkoutCurveBps,
    c_rt_mkr: ScaledBps,
    vol5m_at_fill: ScaledBps,
) -> f64 {
    let mask = curve.resolved_mask & MARKOUT_MASK_15S;
    if mask == 0 {
        return 0.0;
    }
    let c_rt_f = (c_rt_mkr.raw().max(100)) as f64;
    if vol5m_at_fill.raw() <= 0 {
        let mut weighted_adv = 0.0_f64;
        let mut weight_sum = 0.0_f64;
        let mut worst_adv = 0.0_f64;
        for h_idx in 0..4 {
            if (mask & (1_u8 << h_idx)) != 0 {
                let adv = (-curve.samples_bps[h_idx]).max(0) as f64;
                weighted_adv += HORIZON_WEIGHTS_4H[h_idx] * adv;
                weight_sum += HORIZON_WEIGHTS_4H[h_idx];
                if h_idx >= 1 && adv > worst_adv {
                    worst_adv = adv;
                }
            }
        }
        if weight_sum <= 1e-9 {
            return 0.0;
        }
        let mean_adv = weighted_adv / weight_sum;
        let eff_worst = if (mask & 0x0E) != 0 { worst_adv } else { mean_adv };
        let eff_adv = INV_PHI_MK * mean_adv + INV_PHI_SQ_MK * eff_worst;
        return (eff_adv / c_rt_f).clamp(0.0, 6.0);
    }
    let vol_f = vol5m_at_fill.raw() as f64;
    let fee_floor_sigma = INV_PHI_MK * c_rt_f;
    let mut weighted_z = 0.0_f64;
    let mut weight_sum = 0.0_f64;
    let mut worst_z = 0.0_f64;
    let mut peak_early_adv = 0.0_f64;
    let mut adv_15s = 0.0_f64;
    let mut z_15s = 0.0_f64;
    for h_idx in 0..4 {
        if (mask & (1_u8 << h_idx)) != 0 {
            let sigma_h = fee_floor_sigma
                .max(vol_f * (MARKOUT_HORIZONS_SEC[h_idx] / DIFFUSIVE_REF_WINDOW_SEC).sqrt())
                .max(1.0);
            let adv = (-curve.samples_bps[h_idx]).max(0) as f64;
            let z_h = adv / sigma_h;
            weighted_z += HORIZON_WEIGHTS_4H[h_idx] * z_h;
            weight_sum += HORIZON_WEIGHTS_4H[h_idx];
            if h_idx >= 1 && z_h > worst_z {
                worst_z = z_h;
            }
            if h_idx < 3 {
                if adv > peak_early_adv {
                    peak_early_adv = adv;
                }
            } else {
                adv_15s = adv;
                z_15s = z_h;
            }
        }
    }
    if weight_sum <= 1e-9 {
        return 0.0;
    }
    let mean_z = weighted_z / weight_sum;
    let eff_worst_z = if (mask & 0x0E) != 0 { worst_z } else { mean_z };
    let raw_zeta = PHI_MK * (INV_PHI_MK * mean_z + INV_PHI_SQ_MK * eff_worst_z);
    let zeta = if (mask & (1_u8 << 3)) != 0 && peak_early_adv > 0.0 {
        let rev_frac = (1.0 - adv_15s / peak_early_adv).clamp(0.0, 1.0);
        let s_rev = crate::wave_elliott::hermite_smoothstep_c1(rev_frac, INV_PHI_CUBE_MK, INV_PHI_MK);
        let s_eq15 = 1.0 - crate::wave_elliott::hermite_smoothstep_c1(z_15s, INV_PHI_CUBE_MK, INV_PHI_MK);
        let psi_rev = (s_rev * s_eq15).clamp(0.0, 1.0);
        (1.0 - psi_rev) * raw_zeta + psi_rev * (PHI_MK * z_15s)
    } else {
        raw_zeta
    };
    zeta.clamp(0.0, 6.0)
}

/// Extended 5-horizon diffusive Glosten-Milgrom post-fill toxicity functional (`+250ms..+60s`, V5.5.4).
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta_5h_diffusive(
    curve: MarkoutCurve5hBps,
    c_rt_mkr: ScaledBps,
    vol5m_at_fill: ScaledBps,
) -> f64 {
    let base_zeta = glosten_milgrom_toxicity_zeta_diffusive(curve.to_legacy_4h(), c_rt_mkr, vol5m_at_fill);
    if (curve.resolved_mask & 0x10) == 0 {
        return base_zeta;
    }
    let c_rt_f = (c_rt_mkr.raw().max(100)) as f64;
    if vol5m_at_fill.raw() <= 0 {
        let adv_60s = ((-curve.samples_bps[4]).max(0) as f64) / c_rt_f;
        return (INV_PHI_MK * base_zeta + INV_PHI_SQ_MK * adv_60s).clamp(0.0, 6.0);
    }
    let vol_f = vol5m_at_fill.raw() as f64;
    let sigma_60s = (INV_PHI_MK * c_rt_f)
        .max(vol_f * (MARKOUT_HORIZONS_SEC[4] / DIFFUSIVE_REF_WINDOW_SEC).sqrt())
        .max(1.0);
    let z_60s = ((-curve.samples_bps[4]).max(0) as f64) / sigma_60s;
    (INV_PHI_MK * base_zeta + INV_PHI_SQ_MK * (PHI_MK * z_60s)).clamp(0.0, 6.0)
}

/// Glosten-Milgrom multi-horizon post-fill adverse selection functional `zeta_tox >= 0.0` (4-horizon).
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta(curve: MarkoutCurveBps, expected_rt_cost: ScaledBps) -> f64 {
    glosten_milgrom_toxicity_zeta_diffusive(curve, expected_rt_cost, ScaledBps(0))
}

/// Extended 5-horizon Glosten-Milgrom post-fill toxicity functional (`+250ms..+60s`).
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta_5h(
    curve: MarkoutCurve5hBps,
    expected_rt_cost: ScaledBps,
) -> f64 {
    glosten_milgrom_toxicity_zeta_5h_diffusive(curve, expected_rt_cost, ScaledBps(0))
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct PendingMarkoutProbe {
    active: bool,
    is_abort_probe: bool,
    tp_counted_15s: bool,
    side: Side,
    veto_code: u8,
    ref_price: PriceTicks,
    mid_t0: PriceTicks,
    armed_ts_ns: i64,
    t0_ns: i64,
    init_q_lots: LotQty,
    rem_q_lots: LotQty,
    vol5m_at_fill: ScaledBps,
    c_rt_mkr: ScaledBps,
    peak_fav_sbps: i64,
    worst_adv_sbps: i64,
    curve: MarkoutCurve5hBps,
}

impl PendingMarkoutProbe {
    #[inline]
    const fn empty() -> Self {
        Self {
            active: false,
            is_abort_probe: false,
            tp_counted_15s: false,
            side: Side::Bid,
            veto_code: 0,
            ref_price: PriceTicks(0),
            mid_t0: PriceTicks(0),
            armed_ts_ns: 0,
            t0_ns: 0,
            init_q_lots: LotQty(0),
            rem_q_lots: LotQty(0),
            vol5m_at_fill: ScaledBps(0),
            c_rt_mkr: ScaledBps(400),
            peak_fav_sbps: 0,
            worst_adv_sbps: 0,
            curve: MarkoutCurve5hBps {
                samples_bps: [0; MARKOUT_HORIZONS_V49],
                resolved_mask: 0,
                initial_edge_bps: 0,
                peak_zeta_tox_pm: 0,
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SlotMarkoutRing {
    probes: [PendingMarkoutProbe; PROBE_RING_CAP],
    write_idx: usize,
    last_fill_curve: MarkoutCurve5hBps,
    last_fill_t0_ns: i64,
    last_fill_vol5m: ScaledBps,
    last_c_rt_mkr: ScaledBps,
    last_maker_fee_bps: i64,
    ewma_lvr_5s_bps: i64,
    ewma_markout_15s_bps: i64,
    ewma_markout_60s_bps: i64,
    pending_uncensored_peak_sbps: i64,
    pending_resolved_60s_fills: [(Side, i64, i64); PENDING_LEARN_RING_CAP],
    resolved_60s_read_idx: usize,
    resolved_60s_write_idx: usize,
    last_resolved_60s_peak_sbps: i64,
    pending_cf_aborts: [(Side, i64, i64); PENDING_LEARN_RING_CAP],
    cf_abort_read_idx: usize,
    cf_abort_write_idx: usize,
    tp_abort_count: u64,
    resolved_abort_count: u64,
    veto_tp_counts: [u32; VETO_CODE_CARDINALITY],
    veto_resolved_counts: [u32; VETO_CODE_CARDINALITY],
    completed_aborts: [ResolvedAbortSample; DRAIN_ABORT_CAP],
    abort_read_idx: usize,
    abort_write_idx: usize,
}

impl Default for SlotMarkoutRing {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SlotMarkoutRing {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            probes: [PendingMarkoutProbe::empty(); PROBE_RING_CAP],
            write_idx: 0,
            last_fill_curve: MarkoutCurve5hBps {
                samples_bps: [0; MARKOUT_HORIZONS_V49],
                resolved_mask: 0,
                initial_edge_bps: 0,
                peak_zeta_tox_pm: 0,
            },
            last_fill_t0_ns: 0,
            last_fill_vol5m: ScaledBps(0),
            last_c_rt_mkr: ScaledBps(400),
            last_maker_fee_bps: 200,
            ewma_lvr_5s_bps: 0,
            ewma_markout_15s_bps: 0,
            ewma_markout_60s_bps: 0,
            pending_uncensored_peak_sbps: 0,
            pending_resolved_60s_fills: [(Side::Bid, 0, 0); PENDING_LEARN_RING_CAP],
            resolved_60s_read_idx: 0,
            resolved_60s_write_idx: 0,
            last_resolved_60s_peak_sbps: 0,
            pending_cf_aborts: [(Side::Bid, 0, 0); PENDING_LEARN_RING_CAP],
            cf_abort_read_idx: 0,
            cf_abort_write_idx: 0,
            tp_abort_count: 0,
            resolved_abort_count: 0,
            veto_tp_counts: [0; VETO_CODE_CARDINALITY],
            veto_resolved_counts: [0; VETO_CODE_CARDINALITY],
            completed_aborts: [ResolvedAbortSample {
                side: Side::Bid,
                veto_code: LastVetoCode::None,
                armed_ts_ns: TimestampNs(0),
                aborted_ts_ns: TimestampNs(0),
                limit_ticks: PriceTicks(0),
                init_q_lots: LotQty(0),
                rem_q_lots: LotQty(0),
                saved_tox_bps: [ScaledBps(0); MARKOUT_HORIZONS_V49],
                resolved_mask: 0,
                is_true_positive: false,
            }; DRAIN_ABORT_CAP],
            abort_read_idx: 0,
            abort_write_idx: 0,
        }
    }

    #[inline]
    fn push_completed_abort(slot: &PendingMarkoutProbe, fee_bar: i64, buf: &mut [ResolvedAbortSample; DRAIN_ABORT_CAP], r_idx: &mut usize, w_idx: &mut usize) {
        let mut saved = [ScaledBps(0); MARKOUT_HORIZONS_V49];
        let mut max_saved = i64::MIN;
        for k in 0..MARKOUT_HORIZONS_V49 {
            saved[k] = ScaledBps(slot.curve.samples_bps[k]);
            if k >= 1 && (slot.curve.resolved_mask & (1_u8 << k)) != 0 {
                max_saved = max_saved.max(slot.curve.samples_bps[k]);
            }
        }
        let pos = *w_idx % DRAIN_ABORT_CAP;
        buf[pos] = ResolvedAbortSample {
            side: slot.side,
            veto_code: LastVetoCode::from_code(slot.veto_code),
            armed_ts_ns: TimestampNs(slot.armed_ts_ns),
            aborted_ts_ns: TimestampNs(slot.t0_ns),
            limit_ticks: slot.ref_price,
            init_q_lots: slot.init_q_lots,
            rem_q_lots: slot.rem_q_lots,
            saved_tox_bps: saved,
            resolved_mask: slot.curve.resolved_mask,
            is_true_positive: max_saved > fee_bar,
        };
        *w_idx = w_idx.wrapping_add(1);
        if w_idx.wrapping_sub(*r_idx) > DRAIN_ABORT_CAP {
            *r_idx = w_idx.wrapping_sub(DRAIN_ABORT_CAP);
        }
    }

    #[inline]
    pub fn arm_fill_probe(&mut self, side: Side, fill_px: PriceTicks, mid_t0: PriceTicks, ts_ns: TimestampNs) {
        let c_rt = ScaledBps((self.last_maker_fee_bps.max(0).saturating_mul(2)).max(100));
        self.arm_fill_probe_with_vol(side, fill_px, mid_t0, ts_ns, self.last_fill_vol5m, c_rt);
    }

    #[inline]
    pub fn arm_fill_probe_with_vol(
        &mut self,
        side: Side,
        fill_px: PriceTicks,
        mid_t0: PriceTicks,
        ts_ns: TimestampNs,
        vol5m_at_fill: ScaledBps,
        c_rt_mkr: ScaledBps,
    ) {
        if fill_px.0 <= 0 {
            return;
        }
        let eff_mid = if mid_t0.0 > 0 { mid_t0 } else { fill_px };
        let init_edge = ((eff_mid.0 - fill_px.0) * side.sign_i64()).saturating_mul(1_000_000) / fill_px.0.max(1);
        let eff_c_rt = ScaledBps(c_rt_mkr.raw().max(100));
        let idx = self.write_idx % PROBE_RING_CAP;
        self.write_idx = self.write_idx.wrapping_add(1);
        let curve = MarkoutCurve5hBps {
            samples_bps: [0; MARKOUT_HORIZONS_V49],
            resolved_mask: 0,
            initial_edge_bps: init_edge,
            peak_zeta_tox_pm: 0,
        };
        self.last_fill_curve = curve;
        self.last_fill_t0_ns = ts_ns.0;
        if vol5m_at_fill.raw() > 0 {
            self.last_fill_vol5m = vol5m_at_fill;
        }
        self.last_c_rt_mkr = eff_c_rt;
        self.probes[idx] = PendingMarkoutProbe {
            active: true,
            is_abort_probe: false,
            tp_counted_15s: false,
            side,
            veto_code: 0,
            ref_price: fill_px,
            mid_t0: eff_mid,
            armed_ts_ns: ts_ns.0,
            t0_ns: ts_ns.0,
            init_q_lots: LotQty(0),
            rem_q_lots: LotQty(0),
            vol5m_at_fill,
            c_rt_mkr: eff_c_rt,
            peak_fav_sbps: 0,
            worst_adv_sbps: 0,
            curve,
        };
    }

    #[inline]
    pub fn set_vol5m_at_fill(&mut self, vol5m_at_fill: ScaledBps) {
        self.last_fill_vol5m = vol5m_at_fill;
        for slot in &mut self.probes {
            if slot.active && !slot.is_abort_probe && slot.vol5m_at_fill.raw() <= 0 {
                slot.vol5m_at_fill = vol5m_at_fill;
            }
        }
    }

    #[inline]
    pub fn arm_abort_probe(&mut self, side: Side, abort_px: PriceTicks, veto: LastVetoCode, ts_ns: TimestampNs) {
        self.arm_abort_probe_v49(side, abort_px, veto, ts_ns, ts_ns, LotQty(0), LotQty(0));
    }

    #[inline]
    pub fn arm_abort_probe_v49(
        &mut self,
        side: Side,
        abort_px: PriceTicks,
        veto: LastVetoCode,
        armed_ts_ns: TimestampNs,
        aborted_ts_ns: TimestampNs,
        init_q_lots: LotQty,
        rem_q_lots: LotQty,
    ) {
        if abort_px.0 <= 0 {
            return;
        }
        let idx = self.write_idx % PROBE_RING_CAP;
        if self.probes[idx].active && self.probes[idx].is_abort_probe && self.probes[idx].tp_counted_15s {
            Self::push_completed_abort(&self.probes[idx], self.last_maker_fee_bps.max(0), &mut self.completed_aborts, &mut self.abort_read_idx, &mut self.abort_write_idx);
        }
        self.write_idx = self.write_idx.wrapping_add(1);
        self.probes[idx] = PendingMarkoutProbe {
            active: true,
            is_abort_probe: true,
            tp_counted_15s: false,
            side,
            veto_code: veto as u8,
            ref_price: abort_px,
            mid_t0: abort_px,
            armed_ts_ns: armed_ts_ns.0,
            t0_ns: aborted_ts_ns.0,
            init_q_lots,
            rem_q_lots,
            vol5m_at_fill: ScaledBps(0),
            c_rt_mkr: ScaledBps((self.last_maker_fee_bps.max(0).saturating_mul(2)).max(100)),
            peak_fav_sbps: 0,
            worst_adv_sbps: 0,
            curve: MarkoutCurve5hBps::default(),
        };
    }

    pub fn step_clock(&mut self, mid_now: PriceTicks, now_ns: TimestampNs, maker_fee_bps: i64) {
        if mid_now.0 <= 0 {
            return;
        }
        let fee_bar = maker_fee_bps.max(0);
        self.last_maker_fee_bps = fee_bar;
        let rt_cost_est = ScaledBps((fee_bar.saturating_mul(2)).max(100));
        self.last_c_rt_mkr = rt_cost_est;
        for slot in &mut self.probes {
            if !slot.active {
                continue;
            }
            let dt = now_ns.0.saturating_sub(slot.t0_ns);
            let denom = slot.ref_price.0.max(1);
            if dt >= 0 && dt <= MARKOUT_HORIZONS_V49_NS[MARKOUT_HORIZONS_V49 - 1] {
                let live_fav = ((mid_now.0 - slot.ref_price.0) * slot.side.sign_i64())
                    .saturating_mul(1_000_000)
                    / denom;
                if live_fav > slot.peak_fav_sbps {
                    slot.peak_fav_sbps = live_fav;
                }
                let live_adv = (-live_fav).max(0);
                if live_adv > slot.worst_adv_sbps {
                    slot.worst_adv_sbps = live_adv;
                }
                if !slot.is_abort_probe && live_fav > self.pending_uncensored_peak_sbps {
                    self.pending_uncensored_peak_sbps = live_fav;
                }
            }
            for h_idx in 0..MARKOUT_HORIZONS_V49 {
                let bit = 1_u8 << h_idx;
                if (slot.curve.resolved_mask & bit) == 0 && dt >= MARKOUT_HORIZONS_V49_NS[h_idx] {
                    let signed_diff = if slot.is_abort_probe {
                        -(mid_now.0 - slot.ref_price.0) * slot.side.sign_i64()
                    } else {
                        (mid_now.0 - slot.ref_price.0) * slot.side.sign_i64()
                    };
                    slot.curve.samples_bps[h_idx] = signed_diff.saturating_mul(1_000_000) / denom;
                    slot.curve.resolved_mask |= bit;
                    if !slot.is_abort_probe {
                        slot.c_rt_mkr = rt_cost_est;
                        let eff_vol = if slot.vol5m_at_fill.raw() > 0 {
                            slot.vol5m_at_fill
                        } else {
                            self.last_fill_vol5m
                        };
                        let z_pm = (glosten_milgrom_toxicity_zeta_5h_diffusive(
                            slot.curve,
                            rt_cost_est,
                            eff_vol,
                        ) * 1_000.0)
                            .round() as u16;
                        slot.curve.peak_zeta_tox_pm = slot.curve.peak_zeta_tox_pm.max(z_pm);
                        if h_idx == 2 {
                            let lvr_5s = ((slot.mid_t0.0 - mid_now.0) * slot.side.sign_i64()).saturating_mul(1_000_000) / denom;
                            self.ewma_lvr_5s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_lvr_5s_bps + LVR_EWMA_ETA_PM * lvr_5s) / 1_000;
                        } else if h_idx == 3 {
                            self.ewma_markout_15s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_markout_15s_bps + LVR_EWMA_ETA_PM * slot.curve.samples_bps[3]) / 1_000;
                        } else if h_idx == 4 {
                            self.ewma_markout_60s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_markout_60s_bps + LVR_EWMA_ETA_PM * slot.curve.samples_bps[4]) / 1_000;
                            let fav_60s_eff = slot
                                .curve
                                .samples_bps[4]
                                .max((slot.peak_fav_sbps * 618) / 1_000)
                                .max(0);
                            let peak_60s_item = slot.peak_fav_sbps.max(slot.curve.samples_bps[4]).max(0);
                            self.last_resolved_60s_peak_sbps = peak_60s_item;
                            let r_pos = self.resolved_60s_write_idx % PENDING_LEARN_RING_CAP;
                            self.pending_resolved_60s_fills[r_pos] = (slot.side, peak_60s_item, fav_60s_eff);
                            self.resolved_60s_write_idx = self.resolved_60s_write_idx.wrapping_add(1);
                            if self.resolved_60s_write_idx.wrapping_sub(self.resolved_60s_read_idx) > PENDING_LEARN_RING_CAP {
                                self.resolved_60s_read_idx = self.resolved_60s_write_idx.wrapping_sub(PENDING_LEARN_RING_CAP);
                            }
                        }
                        if slot.t0_ns >= self.last_fill_t0_ns {
                            self.last_fill_curve = slot.curve;
                            self.last_fill_t0_ns = slot.t0_ns;
                            if slot.vol5m_at_fill.raw() > 0 {
                                self.last_fill_vol5m = slot.vol5m_at_fill;
                            }
                        }
                        if slot.curve.samples_bps[h_idx] > self.pending_uncensored_peak_sbps {
                            self.pending_uncensored_peak_sbps = slot.curve.samples_bps[h_idx];
                        }
                    }
                }
            }
            if slot.is_abort_probe && !slot.tp_counted_15s && (slot.curve.resolved_mask & MARKOUT_MASK_15S) == MARKOUT_MASK_15S {
                slot.tp_counted_15s = true;
                self.resolved_abort_count = self.resolved_abort_count.saturating_add(1);
                let v_idx = usize::from(slot.veto_code).min(VETO_CODE_CARDINALITY - 1);
                self.veto_resolved_counts[v_idx] = self.veto_resolved_counts[v_idx].saturating_add(1);
                let max_15s = slot.curve.samples_bps[1].max(slot.curve.samples_bps[2]).max(slot.curve.samples_bps[3]);
                if max_15s > fee_bar {
                    self.tp_abort_count = self.tp_abort_count.saturating_add(1);
                    self.veto_tp_counts[v_idx] = self.veto_tp_counts[v_idx].saturating_add(1);
                }
            }
            if slot.curve.resolved_mask == MARKOUT_MASK_60S {
                if slot.is_abort_probe {
                    let max_adv_15s = slot.curve.samples_bps[0]
                        .max(slot.curve.samples_bps[1])
                        .max(slot.curve.samples_bps[2])
                        .max(slot.curve.samples_bps[3]);
                    let c_rt = slot.c_rt_mkr.raw().max(100);
                    let min_cf_mfe = (PHI_MK * (c_rt as f64)).round() as i64;
                    if slot.veto_code != (LastVetoCode::WarmupOrLock as u8)
                        && max_adv_15s <= fee_bar
                        && slot.worst_adv_sbps <= c_rt
                        && slot.peak_fav_sbps >= min_cf_mfe
                    {
                        let fav_60s_cf = (-slot.curve.samples_bps[4])
                            .max((slot.peak_fav_sbps * 618) / 1_000)
                            .max(0);
                        let cf_pos = self.cf_abort_write_idx % PENDING_LEARN_RING_CAP;
                        self.pending_cf_aborts[cf_pos] = (slot.side, slot.peak_fav_sbps, fav_60s_cf);
                        self.cf_abort_write_idx = self.cf_abort_write_idx.wrapping_add(1);
                        if self.cf_abort_write_idx.wrapping_sub(self.cf_abort_read_idx) > PENDING_LEARN_RING_CAP {
                            self.cf_abort_read_idx = self.cf_abort_write_idx.wrapping_sub(PENDING_LEARN_RING_CAP);
                        }
                    }
                    Self::push_completed_abort(slot, fee_bar, &mut self.completed_aborts, &mut self.abort_read_idx, &mut self.abort_write_idx);
                }
                slot.active = false;
            }
        }
    }

    #[inline]
    pub fn step_clock_with_vol(
        &mut self,
        mid_now: PriceTicks,
        now_ns: TimestampNs,
        maker_fee_bps: i64,
        vol5m_now: ScaledBps,
    ) {
        if vol5m_now.raw() > 0 && self.last_fill_vol5m.raw() <= 0 {
            self.set_vol5m_at_fill(vol5m_now);
        }
        self.step_clock(mid_now, now_ns, maker_fee_bps);
    }

    #[inline]
    #[must_use]
    pub fn has_armed_vol5m(&self, opened_ts: TimestampNs) -> bool {
        for slot in &self.probes {
            if slot.active && !slot.is_abort_probe && slot.ref_price.0 > 0 {
                let diff = slot.t0_ns.saturating_sub(opened_ts.0).saturating_abs();
                if diff <= 2_000_000_000 && slot.vol5m_at_fill.raw() > 0 {
                    return true;
                }
            }
        }
        self.last_fill_vol5m.raw() > 0
            && self.last_fill_t0_ns > 0
            && self.last_fill_t0_ns.saturating_sub(opened_ts.0).saturating_abs() <= 2_000_000_000
    }

    #[inline]
    #[must_use]
    pub fn active_fill_toxicity_zeta(&self, opened_ts: TimestampNs, expected_rt_cost: ScaledBps) -> f64 {
        self.active_fill_toxicity_zeta_with_vol(opened_ts, expected_rt_cost, ScaledBps(0))
    }

    #[inline]
    #[must_use]
    pub fn active_fill_toxicity_zeta_with_vol(
        &self,
        opened_ts: TimestampNs,
        c_rt_mkr: ScaledBps,
        vol5m_at_fill: ScaledBps,
    ) -> f64 {
        let mut best_probe: Option<(MarkoutCurve5hBps, ScaledBps, ScaledBps)> = None;
        let mut best_diff = i64::MAX;
        for slot in &self.probes {
            if slot.active && !slot.is_abort_probe && slot.ref_price.0 > 0 {
                let diff = slot.t0_ns.saturating_sub(opened_ts.0).saturating_abs();
                if diff <= 2_000_000_000 && diff < best_diff {
                    best_diff = diff;
                    best_probe = Some((slot.curve, slot.vol5m_at_fill, slot.c_rt_mkr));
                }
            }
        }
        let eff_c_rt = ScaledBps(c_rt_mkr.raw().max(100));
        if let Some((c5, slot_vol, _slot_crt)) = best_probe {
            let eff_vol = if slot_vol.raw() > 0 {
                slot_vol
            } else if vol5m_at_fill.raw() > 0 {
                vol5m_at_fill
            } else {
                self.last_fill_vol5m
            };
            return if (c5.resolved_mask & (1_u8 << 4)) != 0 {
                glosten_milgrom_toxicity_zeta_5h_diffusive(c5, eff_c_rt, eff_vol)
            } else {
                glosten_milgrom_toxicity_zeta_diffusive(c5.to_legacy_4h(), eff_c_rt, eff_vol)
            };
        }
        if self.last_fill_t0_ns > 0 && self.last_fill_t0_ns.saturating_sub(opened_ts.0).saturating_abs() <= 2_000_000_000 {
            let eff_vol = if self.last_fill_vol5m.raw() > 0 {
                self.last_fill_vol5m
            } else {
                vol5m_at_fill
            };
            return if (self.last_fill_curve.resolved_mask & (1_u8 << 4)) != 0 {
                glosten_milgrom_toxicity_zeta_5h_diffusive(self.last_fill_curve, eff_c_rt, eff_vol)
            } else {
                glosten_milgrom_toxicity_zeta_diffusive(
                    self.last_fill_curve.to_legacy_4h(),
                    eff_c_rt,
                    eff_vol,
                )
            };
        }
        0.0
    }

    #[inline]
    pub fn pop_resolved_abort(&mut self) -> Option<ResolvedAbortSample> {
        if self.abort_read_idx == self.abort_write_idx {
            return None;
        }
        let pos = self.abort_read_idx % DRAIN_ABORT_CAP;
        self.abort_read_idx = self.abort_read_idx.wrapping_add(1);
        Some(self.completed_aborts[pos])
    }

    #[inline]
    #[must_use]
    pub fn peak_favorable_fill_markout_sbps(&self, opened_ts: TimestampNs) -> i64 {
        let mut peak = 0_i64;
        for slot in &self.probes {
            if slot.active && !slot.is_abort_probe && slot.ref_price.0 > 0
                && slot.t0_ns.saturating_sub(opened_ts.0).saturating_abs() <= 2_000_000_000
            {
                for h_idx in 0..MARKOUT_HORIZONS_V49 {
                    if (slot.curve.resolved_mask & (1_u8 << h_idx)) != 0 {
                        peak = peak.max(slot.curve.samples_bps[h_idx]);
                    }
                }
            }
        }
        if self.last_fill_t0_ns > 0
            && (opened_ts.0 <= 0 || self.last_fill_t0_ns.saturating_sub(opened_ts.0).saturating_abs() <= 2_000_000_000)
        {
            for h_idx in 0..MARKOUT_HORIZONS_V49 {
                if (self.last_fill_curve.resolved_mask & (1_u8 << h_idx)) != 0 {
                    peak = peak.max(self.last_fill_curve.samples_bps[h_idx]);
                }
            }
        }
        peak
    }

    #[inline]
    pub fn take_uncensored_fill_peak_sbps(&mut self) -> Option<i64> {
        let pk = self.pending_uncensored_peak_sbps;
        self.pending_uncensored_peak_sbps = 0;
        (pk > 0).then_some(pk)
    }

    #[inline]
    pub fn take_resolved_60s_fill_with_peak(&mut self) -> Option<(Side, i64, i64)> {
        if self.resolved_60s_read_idx == self.resolved_60s_write_idx {
            return None;
        }
        let pos = self.resolved_60s_read_idx % PENDING_LEARN_RING_CAP;
        self.resolved_60s_read_idx = self.resolved_60s_read_idx.wrapping_add(1);
        let item = self.pending_resolved_60s_fills[pos];
        self.last_resolved_60s_peak_sbps = item.1;
        Some(item)
    }

    #[inline]
    pub fn take_resolved_60s_fill_markout(&mut self) -> Option<(Side, i64)> {
        let (side, _peak, fav_60s) = self.take_resolved_60s_fill_with_peak()?;
        Some((side, fav_60s))
    }

    #[inline]
    #[must_use]
    pub const fn last_resolved_60s_fill_peak_sbps(&self) -> i64 {
        self.last_resolved_60s_peak_sbps
    }

    #[inline]
    pub fn take_counterfactual_abort_learning(&mut self) -> Option<(Side, i64, i64)> {
        if self.cf_abort_read_idx == self.cf_abort_write_idx {
            return None;
        }
        let pos = self.cf_abort_read_idx % PENDING_LEARN_RING_CAP;
        self.cf_abort_read_idx = self.cf_abort_read_idx.wrapping_add(1);
        Some(self.pending_cf_aborts[pos])
    }

    #[inline]
    pub fn hydrate_veto_precision_counts(&mut self, veto: LastVetoCode, tp_count: u32, resolved_count: u32) {
        let idx = veto as usize;
        if idx < VETO_CODE_CARDINALITY {
            let eff_res = resolved_count.max(tp_count);
            self.veto_tp_counts[idx] = self.veto_tp_counts[idx].saturating_add(tp_count);
            self.veto_resolved_counts[idx] = self.veto_resolved_counts[idx].saturating_add(eff_res);
            self.tp_abort_count = self.tp_abort_count.saturating_add(u64::from(tp_count));
            self.resolved_abort_count = self.resolved_abort_count.saturating_add(u64::from(eff_res));
        }
    }

    /// Rehydrates per-veto `+15s` abort precision counts from `vortex_queue_aborts.jsonl` across restarts (`V5.5.10`, H-11).
    pub fn rehydrate_aborts_from_jsonl_path<const N: usize>(
        rings: &mut [Self; N],
        path: &std::path::Path,
    ) -> usize {
        use std::io::{BufRead, BufReader};
        let Ok(file) = std::fs::File::open(path) else {
            return 0;
        };
        let mut hydrated = 0_usize;
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            let Some(s_idx) = v
                .get("slot_idx")
                .and_then(serde_json::Value::as_u64)
                .and_then(|u| usize::try_from(u).ok())
                .filter(|&i| i < N)
            else {
                continue;
            };
            let code_u8 = v
                .get("veto_code")
                .and_then(serde_json::Value::as_u64)
                .and_then(|u| u8::try_from(u).ok())
                .unwrap_or(0);
            let veto = LastVetoCode::from_code(code_u8);
            let is_tp = v
                .get("true_positive")
                .and_then(serde_json::Value::as_bool)
                .or_else(|| v.get("is_true_positive").and_then(serde_json::Value::as_bool))
                .unwrap_or(false);
            rings[s_idx].hydrate_veto_precision_counts(veto, u32::from(is_tp), 1);
            hydrated = hydrated.saturating_add(1);
        }
        hydrated
    }

    /// Empirical Bayes `C1` L1 queue-patience multiplier in `[INV_PHI_MK, PHI_MK]` driven by `+15s`
    /// counterfactual precision of `HawkesStoikovQueueAbort` (`V5.5.8`; `1.0` for `< 4` resolved samples).
    #[inline]
    #[must_use]
    #[allow(clippy::suboptimal_flops)]
    pub fn hawkes_abort_precision_patience_mult_c1(&self) -> f64 {
        let (tp, res) = self.veto_precision_counts(LastVetoCode::HawkesStoikovQueueAbort);
        if res < 4 {
            return 1.0;
        }
        let prec = (f64::from(tp) + PHI_MK) / (f64::from(res) + 2.0 * PHI_MK);
        let s_n = crate::wave_elliott::hermite_smoothstep_c1(f64::from(res), 3.0, 8.0);
        let s_over_abort =
            crate::wave_elliott::hermite_smoothstep_c1(INV_PHI_MK - prec, 0.0, INV_PHI_SQ_MK);
        let s_high_tox =
            crate::wave_elliott::hermite_smoothstep_c1(prec - INV_PHI_MK, 0.0, INV_PHI_CUBE_MK);
        let delta = INV_PHI_SQ_MK * s_over_abort - INV_PHI_CUBE_MK * s_high_tox;
        (1.0 + s_n * delta).clamp(INV_PHI_MK, PHI_MK)
    }

    #[inline]
    #[must_use]
    pub const fn last_fill_curve(&self) -> MarkoutCurveBps {
        self.last_fill_curve.to_legacy_4h()
    }

    #[inline]
    #[must_use]
    pub const fn last_fill_curve_5h(&self) -> MarkoutCurve5hBps {
        self.last_fill_curve
    }

    #[inline]
    #[must_use]
    pub const fn ewma_lvr_5s_bps(&self) -> i64 {
        self.ewma_lvr_5s_bps
    }

    #[inline]
    #[must_use]
    pub const fn ewma_markout_15s_bps(&self) -> i64 {
        self.ewma_markout_15s_bps
    }

    #[inline]
    #[must_use]
    pub const fn ewma_markout_60s_bps(&self) -> i64 {
        self.ewma_markout_60s_bps
    }

    #[inline]
    #[must_use]
    pub const fn true_positive_aborts(&self) -> (u64, u64) {
        (self.tp_abort_count, self.resolved_abort_count)
    }

    #[inline]
    #[must_use]
    pub const fn veto_precision_counts(&self, veto: LastVetoCode) -> (u32, u32) {
        let idx = veto as usize;
        if idx < VETO_CODE_CARDINALITY {
            (self.veto_tp_counts[idx], self.veto_resolved_counts[idx])
        } else {
            (0, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_post_fill_lvr_markout_decomposition_and_15s_curve() {
        let mut ring = SlotMarkoutRing::new();
        ring.arm_fill_probe(Side::Bid, PriceTicks(10_000), PriceTicks(10_002), TimestampNs(1_000_000_000));
        ring.step_clock(PriceTicks(10_005), TimestampNs(1_300_000_000), 200);
        ring.step_clock(PriceTicks(10_008), TimestampNs(2_100_000_000), 200);
        ring.step_clock(PriceTicks(9_994), TimestampNs(6_200_000_000), 200);
        ring.step_clock(PriceTicks(10_012), TimestampNs(16_500_000_000), 200);
        let c = ring.last_fill_curve();
        assert_eq!(c.resolved_mask, 0x0F);
        assert_eq!(c.initial_edge_bps, 200);
        assert_eq!(c.samples_bps, [500, 800, -600, 1_200]);
        assert!(ring.ewma_lvr_5s_bps() > 0 && ring.ewma_markout_15s_bps() > 0);
        ring.step_clock(PriceTicks(10_025), TimestampNs(61_500_000_000), 200);
        let c5 = ring.last_fill_curve_5h();
        assert_eq!(c5.resolved_mask, MARKOUT_MASK_60S);
        assert_eq!(c5.samples_bps[4], 2_500);
        assert!(ring.ewma_markout_60s_bps() > 0);
    }

    #[test]
    fn test_post_abort_counterfactual_true_positive_saved_toxicity_and_v48_glosten_milgrom_zeta() {
        let mut ring = SlotMarkoutRing::new();
        ring.arm_abort_probe_v49(
            Side::Bid,
            PriceTicks(8_200),
            LastVetoCode::HawkesStoikovQueueAbort,
            TimestampNs(4_200_000_000),
            TimestampNs(5_000_000_000),
            LotQty(900),
            LotQty(210),
        );
        ring.step_clock(PriceTicks(8_195), TimestampNs(5_300_000_000), 200);
        ring.step_clock(PriceTicks(8_185), TimestampNs(6_200_000_000), 200);
        ring.step_clock(PriceTicks(8_172), TimestampNs(10_500_000_000), 200);
        ring.step_clock(PriceTicks(8_170), TimestampNs(20_500_000_000), 200);
        assert_eq!(ring.true_positive_aborts(), (1, 1));
        assert_eq!(ring.veto_precision_counts(LastVetoCode::HawkesStoikovQueueAbort), (1, 1));
        ring.step_clock(PriceTicks(8_160), TimestampNs(65_500_000_000), 200);
        let Some(drained) = ring.pop_resolved_abort() else { assert_eq!(ring.resolved_abort_count, 0); return; };
        assert!(drained.is_true_positive && drained.resolved_mask == MARKOUT_MASK_60S);
        assert_eq!(drained.veto_code, LastVetoCode::HawkesStoikovQueueAbort);
        assert!(ring.pop_resolved_abort().is_none());
    }

    #[test]
    fn test_v554_diffusive_brownian_zeta_tao4_reversion_vs_avax1_toxic_sweep() {
        let tao4 = MarkoutCurveBps {
            samples_bps: [-113, -339, -396, -56],
            resolved_mask: MARKOUT_MASK_15S,
            initial_edge_bps: 0,
        };
        let z_tao4 = glosten_milgrom_toxicity_zeta_diffusive(tao4, ScaledBps(400), ScaledBps(4_539));
        assert!(z_tao4 < INV_PHI_MK);

        let avax1 = MarkoutCurveBps {
            samples_bps: [-153, -153, -306, -1_226],
            resolved_mask: MARKOUT_MASK_15S,
            initial_edge_bps: 0,
        };
        let z_avax1 = glosten_milgrom_toxicity_zeta_diffusive(avax1, ScaledBps(400), ScaledBps(3_490));
        assert!(z_avax1 > 1.2);
    }
}

#[cfg(test)]
mod v5510_turno2_markout_tests {
    use super::*;

    #[test]
    fn test_v5510_h11_static_fifo_rings_and_veto_precision_rehydration() {
        let mut ring = SlotMarkoutRing::new();
        let t0 = 1_795_000_000_000_000_000_i64;
        ring.arm_fill_probe_with_vol(Side::Bid, PriceTicks(10_000), PriceTicks(10_000), TimestampNs(t0), ScaledBps(4_000), ScaledBps(400));
        ring.arm_fill_probe_with_vol(Side::Ask, PriceTicks(10_000), PriceTicks(10_000), TimestampNs(t0 + 1_000_000_000), ScaledBps(4_000), ScaledBps(400));
        ring.step_clock(PriceTicks(10_020), TimestampNs(t0 + 5_000_000_000), 200);
        ring.step_clock(PriceTicks(10_010), TimestampNs(t0 + 62_000_000_000), 200);
        let first = ring.take_resolved_60s_fill_with_peak();
        let second = ring.take_resolved_60s_fill_with_peak();
        assert!(first.is_some() && second.is_some(), "Ambos probes +60s deben preservarse en FIFO sin sobreescritura");
        assert!(ring.take_resolved_60s_fill_with_peak().is_none());

        let tmp = std::env::temp_dir().join(format!("vortex_h11_aborts_{}.jsonl", std::process::id()));
        let lines = concat!(
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n",
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n",
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n",
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n",
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n",
            "{\"slot_idx\":5,\"veto_code\":12,\"true_positive\":false}\n"
        );
        let _ = std::fs::write(&tmp, lines);
        let mut rings = [SlotMarkoutRing::new(); 12];
        let count = SlotMarkoutRing::rehydrate_aborts_from_jsonl_path(&mut rings, &tmp);
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(count, 6);
        assert!(rings[5].hawkes_abort_precision_patience_mult_c1() > 1.10, "Sobre-aborto rehidratado debe expandir paciencia Hawkes");
    }
}