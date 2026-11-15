//! Zero-Allocation Post-Fill LOB-LVR Markout & Post-Abort Counterfactual Tracker (+250ms..+15s).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::needless_range_loop
)]

use crate::lob_shm::LastVetoCode;
use crate::types::{PriceTicks, Side, TimestampNs};

pub const MARKOUT_HORIZONS_NS: [i64; 4] = [
    250_000_000,
    1_000_000_000,
    5_000_000_000,
    15_000_000_000,
];
const PROBE_RING_CAP: usize = 8;
const LVR_EWMA_ETA_PM: i64 = 125;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkoutCurveBps {
    pub samples_bps: [i64; 4],
    pub resolved_mask: u8,
    pub initial_edge_bps: i64,
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
    last_fill_curve: MarkoutCurveBps,
    last_abort_veto: LastVetoCode,
    ewma_lvr_5s_bps: i64,
    ewma_markout_15s_bps: i64,
    tp_abort_count: u64,
    resolved_abort_count: u64,
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
            probes: [PendingMarkoutProbe {
                active: false,
                is_abort_probe: false,
                side: Side::Bid,
                veto_code: 0,
                ref_price: PriceTicks(0),
                mid_t0: PriceTicks(0),
                t0_ns: 0,
                curve: MarkoutCurveBps {
                    samples_bps: [0; 4],
                    resolved_mask: 0,
                    initial_edge_bps: 0,
                },
            }; PROBE_RING_CAP],
            write_idx: 0,
            last_fill_curve: MarkoutCurveBps {
                samples_bps: [0; 4],
                resolved_mask: 0,
                initial_edge_bps: 0,
            },
            last_abort_veto: LastVetoCode::None,
            ewma_lvr_5s_bps: 0,
            ewma_markout_15s_bps: 0,
            tp_abort_count: 0,
            resolved_abort_count: 0,
        }
    }

    #[inline]
    pub fn arm_fill_probe(
        &mut self,
        side: Side,
        fill_px: PriceTicks,
        mid_t0: PriceTicks,
        ts_ns: TimestampNs,
    ) {
        if fill_px.0 <= 0 {
            return;
        }
        let eff_mid = if mid_t0.0 > 0 { mid_t0 } else { fill_px };
        let init_edge = ((eff_mid.0 - fill_px.0) * side.sign_i64())
            .saturating_mul(1_000_000)
            / fill_px.0.max(1);
        let idx = self.write_idx % PROBE_RING_CAP;
        self.write_idx = self.write_idx.wrapping_add(1);
        self.probes[idx] = PendingMarkoutProbe {
            active: true,
            is_abort_probe: false,
            side,
            veto_code: 0,
            ref_price: fill_px,
            mid_t0: eff_mid,
            t0_ns: ts_ns.0,
            curve: MarkoutCurveBps {
                samples_bps: [0; 4],
                resolved_mask: 0,
                initial_edge_bps: init_edge,
            },
        };
    }

    #[inline]
    pub fn arm_abort_probe(
        &mut self,
        side: Side,
        abort_px: PriceTicks,
        veto: LastVetoCode,
        ts_ns: TimestampNs,
    ) {
        if abort_px.0 <= 0 {
            return;
        }
        let idx = self.write_idx % PROBE_RING_CAP;
        self.write_idx = self.write_idx.wrapping_add(1);
        self.probes[idx] = PendingMarkoutProbe {
            active: true,
            is_abort_probe: true,
            side,
            veto_code: veto as u8,
            ref_price: abort_px,
            mid_t0: abort_px,
            t0_ns: ts_ns.0,
            curve: MarkoutCurveBps {
                samples_bps: [0; 4],
                resolved_mask: 0,
                initial_edge_bps: 0,
            },
        };
    }

    pub fn step_clock(&mut self, mid_now: PriceTicks, now_ns: TimestampNs, maker_fee_bps: i64) {
        if mid_now.0 <= 0 {
            return;
        }
        let fee_bar = maker_fee_bps.max(0);
        for slot in &mut self.probes {
            if !slot.active {
                continue;
            }
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
                        let lvr_5s = ((slot.mid_t0.0 - mid_now.0) * slot.side.sign_i64())
                            .saturating_mul(1_000_000)
                            / denom;
                        self.ewma_lvr_5s_bps = ((1_000 - LVR_EWMA_ETA_PM) * self.ewma_lvr_5s_bps
                            + LVR_EWMA_ETA_PM * lvr_5s)
                            / 1_000;
                    }
                    if !slot.is_abort_probe {
                        self.last_fill_curve = slot.curve;
                    }
                }
            }
            if slot.curve.resolved_mask == 0x0F {
                if slot.is_abort_probe {
                    self.last_abort_veto = LastVetoCode::from_code(slot.veto_code);
                    self.resolved_abort_count = self.resolved_abort_count.saturating_add(1);
                    let max_saved = slot.curve.samples_bps[1]
                        .max(slot.curve.samples_bps[2])
                        .max(slot.curve.samples_bps[3]);
                    if max_saved > fee_bar {
                        self.tp_abort_count = self.tp_abort_count.saturating_add(1);
                    }
                } else {
                    self.ewma_markout_15s_bps = ((1_000 - LVR_EWMA_ETA_PM)
                        * self.ewma_markout_15s_bps
                        + LVR_EWMA_ETA_PM * slot.curve.samples_bps[3])
                        / 1_000;
                }
                slot.active = false;
            }
        }
    }

    #[inline]
    #[must_use]
    pub const fn last_fill_curve(&self) -> MarkoutCurveBps {
        self.last_fill_curve
    }

    #[inline]
    #[must_use]
    pub const fn last_abort_veto(&self) -> LastVetoCode {
        self.last_abort_veto
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
    pub const fn true_positive_aborts(&self) -> (u64, u64) {
        (self.tp_abort_count, self.resolved_abort_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_post_fill_lvr_markout_decomposition_and_15s_curve() {
        let mut ring = SlotMarkoutRing::new();
        ring.arm_fill_probe(
            Side::Bid,
            PriceTicks(10_000),
            PriceTicks(10_002),
            TimestampNs(1_000_000_000),
        );
        ring.step_clock(PriceTicks(10_005), TimestampNs(1_300_000_000), 200);
        ring.step_clock(PriceTicks(10_008), TimestampNs(2_100_000_000), 200);
        ring.step_clock(PriceTicks(9_994), TimestampNs(6_200_000_000), 200);
        ring.step_clock(PriceTicks(10_012), TimestampNs(16_500_000_000), 200);

        let c = ring.last_fill_curve();
        assert_eq!(c.resolved_mask, 0x0F);
        assert_eq!(c.initial_edge_bps, 200);
        assert_eq!(c.samples_bps, [500, 800, -600, 1_200]);
        assert!(ring.ewma_lvr_5s_bps() > 0);
        assert!(ring.ewma_markout_15s_bps() > 0);
    }

    #[test]
    fn test_post_abort_counterfactual_true_positive_saved_toxicity() {
        let mut ring = SlotMarkoutRing::new();
        ring.arm_abort_probe(
            Side::Bid,
            PriceTicks(8_200),
            LastVetoCode::HawkesStoikovQueueAbort,
            TimestampNs(5_000_000_000),
        );
        ring.step_clock(PriceTicks(8_195), TimestampNs(5_300_000_000), 200);
        ring.step_clock(PriceTicks(8_185), TimestampNs(6_200_000_000), 200);
        ring.step_clock(PriceTicks(8_172), TimestampNs(10_500_000_000), 200);
        ring.step_clock(PriceTicks(8_170), TimestampNs(20_500_000_000), 200);
        assert_eq!(ring.true_positive_aborts(), (1, 1));
        assert_eq!(ring.last_abort_veto(), LastVetoCode::HawkesStoikovQueueAbort);
    }
}
