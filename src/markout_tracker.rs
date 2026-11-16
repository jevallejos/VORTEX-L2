//! Zero-Allocation Post-Fill LOB-LVR Markout, Glosten-Milgrom Toxicity & Counterfactual Tracker (V4.8).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::needless_range_loop,
    clippy::suboptimal_flops
)]

use crate::lob_shm::LastVetoCode;
use crate::types::{PriceTicks, ScaledBps, Side, TimestampNs};

pub const MARKOUT_HORIZONS_NS: [i64; 4] = [250_000_000, 1_000_000_000, 5_000_000_000, 15_000_000_000];
const PROBE_RING_CAP: usize = 8;
const LVR_EWMA_ETA_PM: i64 = 125;
const INV_PHI_MK: f64 = 0.618_033_988_749_895;
const INV_PHI2_MK: f64 = 0.381_966_011_250_105;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkoutCurveBps {
    pub samples_bps: [i64; 4],
    pub resolved_mask: u8,
    pub initial_edge_bps: i64,
}

impl MarkoutCurveBps {
    /// Dimensionless Glosten-Milgrom post-fill adverse selection toxicity `zeta_tox >= 0.0` once `+5s` resolves.
    #[inline]
    #[must_use]
    pub fn glosten_milgrom_toxicity_zeta(self, expected_rt_cost_bps: ScaledBps) -> f64 {
        if (self.resolved_mask & 0x07) != 0x07 {
            return 0.0;
        }
        let m250 = self.samples_bps[0] as f64;
        let m1s = self.samples_bps[1] as f64;
        let m5s = if (self.resolved_mask & 0x08) != 0 {
            self.samples_bps[2].min(self.samples_bps[3]) as f64
        } else {
            self.samples_bps[2] as f64
        };
        let weighted_neg = -(INV_PHI_MK * m250 + INV_PHI2_MK * m1s + m5s);
        let denom = 2.0 * (expected_rt_cost_bps.raw().max(50) as f64);
        (weighted_neg / denom).max(0.0)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct PendingMarkoutProbe {
    active: bool,
    is_abort_probe: bool,
    side: Side,
    veto_code: u8,
    ref_price: PriceTicks,
    mid_t0: PriceTicks,
    t0_ns: i64,
    curve: MarkoutCurveBps,
}

#[derive(Copy, Clone, Debug)]
pub struct SlotMarkoutRing {
    probes: [PendingMarkoutProbe; PROBE_RING_CAP],
    write_idx: usize,
    last_fill_t0_ns: i64,
    last_fill_curve: MarkoutCurveBps,
    last_abort_veto: LastVetoCode,
    ewma_lvr_5s_bps: i64,
    ewma_markout_15s_bps: i64,
    tp_abort_count: u64,
    resolved_abort_count: u64,
}

impl Default for SlotMarkoutRing {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl SlotMarkoutRing {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let empty_c = MarkoutCurveBps { samples_bps: [0; 4], resolved_mask: 0, initial_edge_bps: 0 };
        Self {
            probes: [PendingMarkoutProbe {
                active: false, is_abort_probe: false, side: Side::Bid, veto_code: 0,
                ref_price: PriceTicks(0), mid_t0: PriceTicks(0), t0_ns: 0, curve: empty_c,
            }; PROBE_RING_CAP],
            write_idx: 0,
            last_fill_t0_ns: 0,
            last_fill_curve: empty_c,
            last_abort_veto: LastVetoCode::None,
            ewma_lvr_5s_bps: 0,
            ewma_markout_15s_bps: 0,
            tp_abort_count: 0,
            resolved_abort_count: 0,
        }
    }

    #[inline]
    pub fn arm_fill_probe(&mut self, side: Side, fill_px: PriceTicks, mid_t0: PriceTicks, ts_ns: TimestampNs) {
        if fill_px.0 <= 0 { return; }
        let eff_mid = if mid_t0.0 > 0 { mid_t0 } else { fill_px };
        let init_edge = ((eff_mid.0 - fill_px.0) * side.sign_i64()).saturating_mul(1_000_000) / fill_px.0.max(1);
        let fresh_curve = MarkoutCurveBps { samples_bps: [0; 4], resolved_mask: 0, initial_edge_bps: init_edge };
        let idx = self.write_idx % PROBE_RING_CAP;
        self.write_idx = self.write_idx.wrapping_add(1);
        self.last_fill_t0_ns = ts_ns.0;
        self.last_fill_curve = fresh_curve;
        self.probes[idx] = PendingMarkoutProbe {
            active: true, is_abort_probe: false, side, veto_code: 0,
            ref_price: fill_px, mid_t0: eff_mid, t0_ns: ts_ns.0, curve: fresh_curve,
        };
    }

    #[inline]
    pub fn arm_abort_probe(&mut self, side: Side, abort_px: PriceTicks, veto: LastVetoCode, ts_ns: TimestampNs) {
        if abort_px.0 <= 0 { return; }
        let idx = self.write_idx % PROBE_RING_CAP;
        self.write_idx = self.write_idx.wrapping_add(1);
        self.probes[idx] = PendingMarkoutProbe {
            active: true, is_abort_probe: true, side, veto_code: veto as u8,
            ref_price: abort_px, mid_t0: abort_px, t0_ns: ts_ns.0,
            curve: MarkoutCurveBps { samples_bps: [0; 4], resolved_mask: 0, initial_edge_bps: 0 },
        };
    }

    pub fn step_clock(&mut self, mid_now: PriceTicks, now_ns: TimestampNs, maker_fee_bps: i64) {
        if mid_now.0 <= 0 { return; }
        let fee_bar = maker_fee_bps.max(0);
        for slot in &mut self.probes {
            if !slot.active { continue; }
            let dt = now_ns.0.saturating_sub(slot.t0_ns);
            let denom = slot.ref_price.0.max(1);
            for h_idx in 0..4 {
                let bit = 1_u8 << h_idx;
                if (slot.curve.resolved_mask & bit) == 0 && dt >= MARKOUT_HORIZONS_NS[h_idx] {
                    let signed_diff = if slot.is_abort_probe {
                        -(mid_now.0 - slot.ref_price.0) * slot.side.sign_i64()
                    } else {
                        (mid_now.0 - slot.ref_price.0) * slot.side.sign_i64()
                    };
                    slot.curve.samples_bps[h_idx] = signed_diff.saturating_mul(1_000_000) / denom;
                    slot.curve.resolved_mask |= bit;
                    if !slot.is_abort_probe && h_idx == 2 {
                        let lvr_5s = ((slot.mid_t0.0 - mid_now.0) * slot.side.sign_i64()).saturating_mul(1_000_000) / denom;
                        self.ewma_lvr_5s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_lvr_5s_bps + LVR_EWMA_ETA_PM * lvr_5s) / 1_000;
                    }
                    if !slot.is_abort_probe && slot.t0_ns == self.last_fill_t0_ns {
                        self.last_fill_curve = slot.curve;
                    }
                }
            }
            if slot.curve.resolved_mask == 0x0F {
                if slot.is_abort_probe {
                    self.last_abort_veto = LastVetoCode::from_code(slot.veto_code);
                    self.resolved_abort_count = self.resolved_abort_count.saturating_add(1);
                    if slot.curve.samples_bps[1].max(slot.curve.samples_bps[2]).max(slot.curve.samples_bps[3]) > fee_bar {
                        self.tp_abort_count = self.tp_abort_count.saturating_add(1);
                    }
                } else {
                    self.ewma_markout_15s_bps =
                        ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_markout_15s_bps + LVR_EWMA_ETA_PM * slot.curve.samples_bps[3]) / 1_000;
                }
                slot.active = false;
            }
        }
    }

    #[inline] #[must_use] pub const fn last_fill_ts_ns(&self) -> TimestampNs { TimestampNs(self.last_fill_t0_ns) }
    #[inline] #[must_use] pub const fn last_fill_curve(&self) -> MarkoutCurveBps { self.last_fill_curve }
    #[inline] #[must_use] pub const fn last_abort_veto(&self) -> LastVetoCode { self.last_abort_veto }
    #[inline] #[must_use] pub const fn ewma_lvr_5s_bps(&self) -> i64 { self.ewma_lvr_5s_bps }
    #[inline] #[must_use] pub const fn ewma_markout_15s_bps(&self) -> i64 { self.ewma_markout_15s_bps }
    #[inline] #[must_use] pub const fn true_positive_aborts(&self) -> (u64, u64) { (self.tp_abort_count, self.resolved_abort_count) }

    /// Returns live Glosten-Milgrom toxicity `zeta_tox` if the ring probe matches the open position's fill timestamp.
    #[inline]
    #[must_use]
    pub fn active_fill_toxicity_zeta(&self, opened_ts: TimestampNs, expected_rt_cost_bps: ScaledBps) -> f64 {
        if self.last_fill_t0_ns <= 0 || (self.last_fill_t0_ns - opened_ts.0).abs() > 5_000_000_000 {
            return 0.0;
        }
        self.last_fill_curve.glosten_milgrom_toxicity_zeta(expected_rt_cost_bps)
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
        assert_eq!((c.resolved_mask, c.initial_edge_bps, c.samples_bps), (0x0F, 200, [500, 800, -600, 1_200]));
        assert!(ring.ewma_lvr_5s_bps() > 0 && ring.ewma_markout_15s_bps() > 0);
    }

    #[test]
    fn test_post_abort_counterfactual_true_positive_saved_toxicity_and_v48_glosten_milgrom_zeta() {
        let mut ring = SlotMarkoutRing::new();
        ring.arm_abort_probe(Side::Bid, PriceTicks(8_200), LastVetoCode::HawkesStoikovQueueAbort, TimestampNs(5_000_000_000));
        for &(px, t) in &[(8_195, 5_300_000_000_i64), (8_185, 6_200_000_000), (8_172, 10_500_000_000), (8_170, 20_500_000_000)] {
            ring.step_clock(PriceTicks(px), TimestampNs(t), 200);
        }
        assert_eq!(ring.true_positive_aborts(), (1, 1));
        assert_eq!(ring.last_abort_veto(), LastVetoCode::HawkesStoikovQueueAbort);

        // Trade #11 benign dip vs Trade #13 toxic sweep
        let c_t11 = MarkoutCurveBps { samples_bps: [-20, -38, -199, 69], resolved_mask: 0x07, initial_edge_bps: 0 };
        let c_t13 = MarkoutCurveBps { samples_bps: [-314, -328, -693, -1_372], resolved_mask: 0x0F, initial_edge_bps: 0 };
        assert!(c_t11.glosten_milgrom_toxicity_zeta(ScaledBps(420)) < INV_PHI_MK);
        assert!(c_t13.glosten_milgrom_toxicity_zeta(ScaledBps(420)) > 1.80);
    }
}
