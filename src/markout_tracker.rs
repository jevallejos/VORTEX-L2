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

pub const MARKOUT_HORIZONS_NS: [i64; 4] = [
    250_000_000,
    1_000_000_000,
    5_000_000_000,
    15_000_000_000,
];

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
const LVR_EWMA_ETA_PM: i64 = 125;

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

/// Glosten-Milgrom multi-horizon post-fill adverse selection functional `zeta_tox >= 0.0` (4-horizon).
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta(curve: MarkoutCurveBps, expected_rt_cost: ScaledBps) -> f64 {
    const WEIGHTS: [f64; 4] = [0.10, 0.20, 0.35, 0.35];
    let mask = curve.resolved_mask & MARKOUT_MASK_15S;
    if mask == 0 {
        return 0.0;
    }
    let cost_denom = (expected_rt_cost.raw().max(100)) as f64;
    let mut weighted_adv = 0.0_f64;
    let mut weight_sum = 0.0_f64;
    let mut worst_adv = 0.0_f64;
    for h_idx in 0..4 {
        if (mask & (1_u8 << h_idx)) != 0 {
            let adv = (-curve.samples_bps[h_idx]).max(0) as f64;
            weighted_adv += WEIGHTS[h_idx] * adv;
            weight_sum += WEIGHTS[h_idx];
            if h_idx >= 1 && adv > worst_adv {
                worst_adv = adv;
            }
        }
    }
    if weight_sum <= 1e-9 {
        return 0.0;
    }
    let mean_adv = weighted_adv / weight_sum;
    let eff_adv = 0.65 * mean_adv + 0.35 * worst_adv;
    (eff_adv / cost_denom).clamp(0.0, 6.0)
}

/// Extended 5-horizon Glosten-Milgrom post-fill toxicity functional (`+250ms..+60s`).
#[inline]
#[must_use]
pub fn glosten_milgrom_toxicity_zeta_5h(
    curve: MarkoutCurve5hBps,
    expected_rt_cost: ScaledBps,
) -> f64 {
    let base_zeta = glosten_milgrom_toxicity_zeta(curve.to_legacy_4h(), expected_rt_cost);
    if (curve.resolved_mask & 0x10) == 0 {
        return base_zeta;
    }
    let cost_denom = (expected_rt_cost.raw().max(100)) as f64;
    let adv_60s = ((-curve.samples_bps[4]).max(0) as f64) / cost_denom;
    (0.80 * base_zeta + 0.20 * adv_60s).clamp(0.0, 6.0)
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
    last_maker_fee_bps: i64,
    ewma_lvr_5s_bps: i64,
    ewma_markout_15s_bps: i64,
    ewma_markout_60s_bps: i64,
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
            last_maker_fee_bps: 200,
            ewma_lvr_5s_bps: 0,
            ewma_markout_15s_bps: 0,
            ewma_markout_60s_bps: 0,
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
        if fill_px.0 <= 0 {
            return;
        }
        let eff_mid = if mid_t0.0 > 0 { mid_t0 } else { fill_px };
        let init_edge = ((eff_mid.0 - fill_px.0) * side.sign_i64()).saturating_mul(1_000_000) / fill_px.0.max(1);
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
            curve,
        };
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
        for slot in &mut self.probes {
            if !slot.active {
                continue;
            }
            let dt = now_ns.0.saturating_sub(slot.t0_ns);
            let denom = slot.ref_price.0.max(1);
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
                        let z_pm = (glosten_milgrom_toxicity_zeta_5h(slot.curve, rt_cost_est) * 1_000.0).round() as u16;
                        slot.curve.peak_zeta_tox_pm = slot.curve.peak_zeta_tox_pm.max(z_pm);
                        if h_idx == 2 {
                            let lvr_5s = ((slot.mid_t0.0 - mid_now.0) * slot.side.sign_i64()).saturating_mul(1_000_000) / denom;
                            self.ewma_lvr_5s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_lvr_5s_bps + LVR_EWMA_ETA_PM * lvr_5s) / 1_000;
                        } else if h_idx == 3 {
                            self.ewma_markout_15s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_markout_15s_bps + LVR_EWMA_ETA_PM * slot.curve.samples_bps[3]) / 1_000;
                        } else if h_idx == 4 {
                            self.ewma_markout_60s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_markout_60s_bps + LVR_EWMA_ETA_PM * slot.curve.samples_bps[4]) / 1_000;
                        }
                        if slot.t0_ns >= self.last_fill_t0_ns {
                            self.last_fill_curve = slot.curve;
                            self.last_fill_t0_ns = slot.t0_ns;
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
                    Self::push_completed_abort(slot, fee_bar, &mut self.completed_aborts, &mut self.abort_read_idx, &mut self.abort_write_idx);
                }
                slot.active = false;
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn active_fill_toxicity_zeta(&self, opened_ts: TimestampNs, expected_rt_cost: ScaledBps) -> f64 {
        let mut best_curve: Option<MarkoutCurve5hBps> = None;
        let mut best_diff = i64::MAX;
        for slot in &self.probes {
            if slot.active && !slot.is_abort_probe && slot.ref_price.0 > 0 {
                let diff = slot.t0_ns.saturating_sub(opened_ts.0).saturating_abs();
                if diff <= 2_000_000_000 && diff < best_diff {
                    best_diff = diff;
                    best_curve = Some(slot.curve);
                }
            }
        }
        if let Some(c5) = best_curve {
            return if (c5.resolved_mask & (1_u8 << 4)) != 0 {
                glosten_milgrom_toxicity_zeta_5h(c5, expected_rt_cost)
            } else {
                glosten_milgrom_toxicity_zeta(c5.to_legacy_4h(), expected_rt_cost)
            };
        }
        if self.last_fill_t0_ns > 0 && self.last_fill_t0_ns.saturating_sub(opened_ts.0).saturating_abs() <= 2_000_000_000 {
            return if (self.last_fill_curve.resolved_mask & (1_u8 << 4)) != 0 {
                glosten_milgrom_toxicity_zeta_5h(self.last_fill_curve, expected_rt_cost)
            } else {
                glosten_milgrom_toxicity_zeta(self.last_fill_curve.to_legacy_4h(), expected_rt_cost)
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
}
