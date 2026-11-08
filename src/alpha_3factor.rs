//! Orthogonal 3-Factor Alpha Model, Lead-Lag Propagator, and L2/L3 Vacuum Surf (LVDBS).

use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side};

pub const OFI_DEPTH: usize = 5;
pub const GEOM_DEPTH: usize = 10;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SignalVerdict {
    NeutralNoAction,
    SpoofingVeto,
    AuthorizeMakerEntry {
        side: Side,
        target_excursion_scaled_bps: ScaledBps,
    },
}

#[derive(Copy, Clone, Debug)]
pub struct OrthogonalFactorState {
    initialized: bool,
    prev_bids: [BookLevel; OFI_DEPTH],
    prev_asks: [BookLevel; OFI_DEPTH],
    ofi_pos_fast: f64,
    ofi_neg_fast: f64,
    ofi_pos_med: f64,
    ofi_neg_med: f64,
    ofi_pos_slow: f64,
    ofi_neg_slow: f64,
    tfi_buy_accum: f64,
    tfi_sell_accum: f64,
    last_ofi: BoundedScore,
    last_tfi: BoundedScore,
    last_geom: BoundedScore,
}

impl Default for OrthogonalFactorState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::needless_range_loop, clippy::manual_clamp)]
impl OrthogonalFactorState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            initialized: false,
            prev_bids: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; OFI_DEPTH],
            prev_asks: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; OFI_DEPTH],
            ofi_pos_fast: 1.0,
            ofi_neg_fast: 1.0,
            ofi_pos_med: 1.0,
            ofi_neg_med: 1.0,
            ofi_pos_slow: 1.0,
            ofi_neg_slow: 1.0,
            tfi_buy_accum: 1.0,
            tfi_sell_accum: 1.0,
            last_ofi: BoundedScore::ZERO,
            last_tfi: BoundedScore::ZERO,
            last_geom: BoundedScore::ZERO,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn on_l2_book_update(&mut self, snap: &mut SymbolLobSnapshot) {
        snap.refresh_deep_l1_l5_totals();
        if !self.initialized {
            let mut i = 0;
            while i < OFI_DEPTH {
                self.prev_bids[i] = snap.bids[i];
                self.prev_asks[i] = snap.asks[i];
                i += 1;
            }
            self.initialized = true;
            return;
        }
        let weights: [f64; OFI_DEPTH] = [1.0, 0.70, 0.45, 0.25, 0.12];
        let mut raw_ofi = 0.0_f64;
        for (i, &w) in weights.iter().enumerate() {
            let cur_b = snap.bids[i];
            let prv_b = self.prev_bids[i];
            let b_flow = if cur_b.price.0 > prv_b.price.0 {
                cur_b.qty.0 as f64
            } else if cur_b.price.0 == prv_b.price.0 {
                (cur_b.qty.0 - prv_b.qty.0) as f64
            } else {
                -(prv_b.qty.0 as f64)
            };
            let cur_a = snap.asks[i];
            let prv_a = self.prev_asks[i];
            let a_flow = if cur_a.price.0 > 0 && prv_a.price.0 > 0 {
                if cur_a.price.0 < prv_a.price.0 {
                    cur_a.qty.0 as f64
                } else if cur_a.price.0 == prv_a.price.0 {
                    (cur_a.qty.0 - prv_a.qty.0) as f64
                } else {
                    -(prv_a.qty.0 as f64)
                }
            } else {
                0.0
            };
            raw_ofi += w * (b_flow - a_flow);
            self.prev_bids[i] = cur_b;
            self.prev_asks[i] = cur_a;
        }
        let pos = raw_ofi.max(0.0);
        let neg = (-raw_ofi).max(0.0);
        self.ofi_pos_fast = self.ofi_pos_fast * 0.82 + pos;
        self.ofi_neg_fast = self.ofi_neg_fast * 0.82 + neg;
        self.ofi_pos_med = self.ofi_pos_med * 0.94 + pos;
        self.ofi_neg_med = self.ofi_neg_med * 0.94 + neg;
        self.ofi_pos_slow = self.ofi_pos_slow * 0.985 + pos;
        self.ofi_neg_slow = self.ofi_neg_slow * 0.985 + neg;

        let f_score = (self.ofi_pos_fast - self.ofi_neg_fast)
            / (self.ofi_pos_fast + self.ofi_neg_fast + 1e-6);
        let m_score = (self.ofi_pos_med - self.ofi_neg_med)
            / (self.ofi_pos_med + self.ofi_neg_med + 1e-6);
        let s_score = (self.ofi_pos_slow - self.ofi_neg_slow)
            / (self.ofi_pos_slow + self.ofi_neg_slow + 1e-6);
        let blended_ofi = 0.50 * f_score + 0.32 * m_score + 0.18 * s_score;
        self.last_ofi = BoundedScore::new_clamped(blended_ofi);

        self.tfi_buy_accum *= 0.992;
        self.tfi_sell_accum *= 0.992;
        let tfi_val = (self.tfi_buy_accum - self.tfi_sell_accum)
            / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6);
        self.last_tfi = BoundedScore::new_clamped(tfi_val);

        let mut bid_mass = 0.0_f64;
        let mut ask_mass = 0.0_f64;
        for i in 0..GEOM_DEPTH {
            let decay = 1.0 / ((i + 1) as f64);
            if snap.bids[i].price.0 > 0 && snap.bids[i].qty.0 > 0 {
                bid_mass += (snap.bids[i].qty.0 as f64) * decay;
            }
            if snap.asks[i].price.0 > 0 && snap.asks[i].qty.0 > 0 {
                ask_mass += (snap.asks[i].qty.0 as f64) * decay;
            }
        }
        let spread_ticks = if snap.asks[0].price.0 > snap.bids[0].price.0
            && snap.bids[0].price.0 > 0
        {
            (snap.asks[0].price.0 - snap.bids[0].price.0) as f64
        } else {
            1.0
        };
        let spread_penalty = 1.0 / spread_ticks.sqrt().max(1.0);
        let geom_raw = ((bid_mass - ask_mass) / (bid_mass + ask_mass + 1e-6)) * spread_penalty;
        self.last_geom = BoundedScore::new_clamped(geom_raw);

        snap.factor_ofi = self.last_ofi;
        snap.factor_tfi = self.last_tfi;
        snap.factor_geom = self.last_geom;
    }

    pub fn on_trade_print(
        &mut self,
        side: Side,
        qty: LotQty,
        snap: &mut SymbolLobSnapshot,
    ) {
        let q = (qty.0.max(0)) as f64;
        self.tfi_buy_accum *= 0.96;
        self.tfi_sell_accum *= 0.96;
        match side {
            Side::Bid => {
                self.tfi_buy_accum += q;
                snap.cum_buy_trade_lots = LotQty(snap.cum_buy_trade_lots.0.saturating_add(qty.0));
            }
            Side::Ask => {
                self.tfi_sell_accum += q;
                snap.cum_sell_trade_lots =
                    LotQty(snap.cum_sell_trade_lots.0.saturating_add(qty.0));
            }
        }
        let tfi_val = (self.tfi_buy_accum - self.tfi_sell_accum)
            / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6);
        self.last_tfi = BoundedScore::new_clamped(tfi_val);
        snap.factor_tfi = self.last_tfi;
    }

    #[inline]
    #[must_use]
    pub fn snr_composite_score(snap: &SymbolLobSnapshot) -> f64 {
        0.45 * snap.factor_ofi.get()
            + 0.35 * snap.factor_tfi.get()
            + 0.20 * snap.factor_geom.get()
    }


    #[must_use]
    pub fn evaluate_entry_signal_with_leader(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
    ) -> SignalVerdict {
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return SignalVerdict::NeutralNoAction;
        };
        let mid_ticks = PriceTicks((best_bid.price.0 + best_ask.price.0) / 2);
        if spec.validate_pair_eligibility(mid_ticks, snap.realized_vol_5m).is_err() {
            return SignalVerdict::NeutralNoAction;
        }
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        let geom = snap.factor_geom.get();

        if (ofi.abs() > 0.32 && tfi.abs() > 0.32 && (ofi * tfi) < -0.08)
            || (ofi.abs() > 0.55 && tfi.abs() < 0.08)
        {
            return SignalVerdict::SpoofingVeto;
        }

        let vol_ratio = (snap.realized_vol_5m.raw().clamp(3_500, 14_000) as f64) / 6_500.0;
        let nu_sigma = vol_ratio.sqrt().clamp(0.85, 1.25);
        let elastic_snr_floor = (0.40 * nu_sigma).clamp(0.38, 0.48);
        let composite = Self::snr_composite_score(snap);
        if composite.abs() < elastic_snr_floor {
            return SignalVerdict::NeutralNoAction;
        }

        let side = if composite > 0.0 {
            if ofi <= 0.15 || tfi <= 0.12 || geom < -0.05 {
                return SignalVerdict::NeutralNoAction;
            }
            Side::Bid
        } else {
            if ofi >= -0.15 || tfi >= -0.12 || geom > 0.05 {
                return SignalVerdict::NeutralNoAction;
            }
            Side::Ask
        };

        let dir_sign = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
        let leader_snr = Self::snr_composite_score(leader_btc_snap) * dir_sign;
        let leader_tfi = leader_btc_snap.factor_tfi.get() * dir_sign;
        let lead_lag_bonus_bps = if spec.symbol_id.0 >= 2
            && leader_snr >= 0.55
            && leader_tfi >= 0.60
        {
            ((leader_snr * 180.0).round() as i64).clamp(0, 180)
        } else {
            0
        };

        let flow_boost = 0.54 + 0.12 * tfi.abs() + 0.08 * geom.abs();
        let base_exc = ((composite.abs() * (snap.realized_vol_5m.raw() as f64) * flow_boost)
            .round() as i64)
            .saturating_add(lead_lag_bonus_bps);
        let expected_excursion = ScaledBps(base_exc);

        match spec.passes_3x_friction_barrier(mid_ticks, expected_excursion) {
            Ok(_) => SignalVerdict::AuthorizeMakerEntry {
                side,
                target_excursion_scaled_bps: expected_excursion,
            },
            Err(_) => SignalVerdict::NeutralNoAction,
        }
    }

    /// Innovation #2 (LVDBS): Computes continuous L2/L3 Liquidity Vacuum Index V_t.
    #[inline]
    #[must_use]
    pub fn compute_vacuum_index_l2_l3(open_side: Side, snap: &SymbolLobSnapshot) -> f64 {
        let mut own_q = 0_i64;
        let mut opp_q = 0_i64;
        let mut i = 0_usize;
        while i < 4 {
            match open_side {
                Side::Bid => {
                    if snap.bids[i].price.0 > 0 && snap.bids[i].qty.0 > 0 {
                        own_q = own_q.saturating_add(snap.bids[i].qty.0);
                    }
                    if snap.asks[i].price.0 > 0 && snap.asks[i].qty.0 > 0 {
                        opp_q = opp_q.saturating_add(snap.asks[i].qty.0);
                    }
                }
                Side::Ask => {
                    if snap.asks[i].price.0 > 0 && snap.asks[i].qty.0 > 0 {
                        own_q = own_q.saturating_add(snap.asks[i].qty.0);
                    }
                    if snap.bids[i].price.0 > 0 && snap.bids[i].qty.0 > 0 {
                        opp_q = opp_q.saturating_add(snap.bids[i].qty.0);
                    }
                }
            }
            i += 1;
        }
        let dir_s = if matches!(open_side, Side::Bid) { 1.0 } else { -1.0 };
        let signed_ofi = (snap.factor_ofi.get() * dir_s).max(0.0);
        let signed_tfi = (snap.factor_tfi.get() * dir_s).max(0.0);
        let depth_ratio = (own_q.max(1) as f64) / (opp_q.max(1) as f64);
        depth_ratio * (1.0 + signed_ofi + 1.25 * signed_tfi)
    }

    /// Innovation #2 (LVDBS): Selects optimal L1..L4 exit level and price.
    #[inline]
    #[must_use]
    pub fn compute_l2_l3_vacuum_surf_target(
        open_side: Side,
        snap: &SymbolLobSnapshot,
        quantum_floor_price: PriceTicks,
    ) -> (usize, PriceTicks, f64) {
        let vac_idx = Self::compute_vacuum_index_l2_l3(open_side, snap);
        let vol_ratio = (snap.realized_vol_5m.raw().clamp(4_000, 12_500) as f64) / 6_500.0;
        let nu_v = vol_ratio.sqrt().clamp(0.88, 1.15);
        let dir_s = if matches!(open_side, Side::Bid) { 1.0 } else { -1.0 };
        let signed_tfi = snap.factor_tfi.get() * dir_s;
        let level_idx = if vac_idx >= (2.15 * nu_v) && signed_tfi >= 0.42 {
            3_usize
        } else if vac_idx >= (1.42 * nu_v) && signed_tfi >= 0.18 {
            2_usize
        } else if vac_idx >= (1.04 * nu_v) {
            1_usize
        } else {
            0_usize
        };
        let book_px = match open_side {
            Side::Bid => {
                let p = snap.asks[level_idx].price.0;
                let fallback = snap.asks[0].price.0.max(quantum_floor_price.0);
                PriceTicks(if p > 0 { p.max(quantum_floor_price.0) } else { fallback })
            }
            Side::Ask => {
                let p = snap.bids[level_idx].price.0;
                let fallback = if snap.bids[0].price.0 > 0 {
                    snap.bids[0].price.0.min(quantum_floor_price.0)
                } else {
                    quantum_floor_price.0
                };
                PriceTicks(if p > 0 { p.min(quantum_floor_price.0).max(1) } else { fallback.max(1) })
            }
        };
        (level_idx + 1, book_px, vac_idx)
    }

    /// Innovation #1 (ZMPQR): Detects micro-rebound crest exhaustion for 25% Peristaltic Exhale.
    #[inline]
    #[must_use]
    pub fn detects_rebound_crest_exhaustion(
        open_side: Side,
        snap: &SymbolLobSnapshot,
    ) -> bool {
        let dir_s = if matches!(open_side, Side::Bid) { 1.0 } else { -1.0 };
        let signed_ofi = snap.factor_ofi.get() * dir_s;
        let signed_tfi = snap.factor_tfi.get() * dir_s;
        signed_ofi <= -0.25 && signed_tfi <= -0.30
    }
}

#[cfg(test)]
#[allow(clippy::needless_range_loop)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_lvdbs_vacuum_surf_levels_1_to_4_and_peristaltic_crest() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_500);
        let bid_px = [8_202, 8_201, 8_200, 8_199];
        let ask_px = [8_203, 8_205, 8_208, 8_211];
        for i in 0..4 {
            snap.bids[i] = BookLevel { price: PriceTicks(bid_px[i]), qty: LotQty(300) };
            snap.asks[i] = BookLevel { price: PriceTicks(ask_px[i]), qty: LotQty(250) };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.60);
        snap.factor_tfi = BoundedScore::new_clamped(0.72);
        let (lvl4, px4, vac4) = OrthogonalFactorState::compute_l2_l3_vacuum_surf_target(
            Side::Bid,
            &snap,
            PriceTicks(8_202),
        );
        assert_eq!(lvl4, 4);
        assert_eq!(px4, PriceTicks(8_211));
        assert!(vac4 >= 2.20);

        snap.asks[0].qty = LotQty(2_200);
        snap.factor_ofi = BoundedScore::new_clamped(0.10);
        snap.factor_tfi = BoundedScore::new_clamped(0.08);
        let (lvl1, px1, vac1) = OrthogonalFactorState::compute_l2_l3_vacuum_surf_target(
            Side::Bid,
            &snap,
            PriceTicks(8_202),
        );
        assert_eq!(lvl1, 1);
        assert_eq!(px1, PriceTicks(8_203));
        assert!(vac1 < 1.05);

        snap.factor_ofi = BoundedScore::new_clamped(-0.40);
        snap.factor_tfi = BoundedScore::new_clamped(-0.55);
        assert!(OrthogonalFactorState::detects_rebound_crest_exhaustion(Side::Bid, &snap));
    }
}
