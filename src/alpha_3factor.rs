//! Orthogonal 3-Factor Alpha Model, Lead-Lag Propagator, and L2/L3 Vacuum Surf (LVDBS).

use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{
    spec_from_symbol_id, BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
};

pub const OFI_DEPTH: usize = 5;
pub const GEOM_DEPTH: usize = 10;
const GAUSSIAN_MAD_TO_SIGMA: f64 = 1.253_314_14;

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
    last_l2_ts_ns: i64,
    last_tfi_ts_ns: i64,
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
            last_l2_ts_ns: 0,
            last_tfi_ts_ns: 0,
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

    /// Resets L2 book delta state on snapshot reload while preserving live TFI tape memory.
    #[inline]
    pub fn reset_l2_book_only(&mut self) {
        self.initialized = false;
        self.prev_bids = [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; OFI_DEPTH];
        self.prev_asks = [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; OFI_DEPTH];
        self.last_l2_ts_ns = 0;
        self.ofi_pos_fast = 1.0;
        self.ofi_neg_fast = 1.0;
        self.ofi_pos_med = 1.0;
        self.ofi_neg_med = 1.0;
        self.ofi_pos_slow = 1.0;
        self.ofi_neg_slow = 1.0;
        self.last_ofi = BoundedScore::ZERO;
        self.last_geom = BoundedScore::ZERO;
    }

    /// Computes contract-normalized neutral seed and silence floor in LotQty units.
    #[inline]
    #[must_use]
    pub fn flow_prior_bounds(snap: &SymbolLobSnapshot) -> (f64, f64) {
        let ref_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
        } else if snap.mark_price.0 > 0 {
            snap.mark_price
        } else if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            PriceTicks(0)
        };
        match spec_from_symbol_id(snap.symbol_id) {
            Some(spec) if ref_px.0 > 0 => {
                let q_scale = spec.characteristic_lot_scale(ref_px);
                let init_lots = q_scale.clamp(1.0, 250.0);
                let floor_lots = (0.25 * q_scale).clamp(0.25, 125.0);
                (init_lots, floor_lots)
            }
            _ => (1.0, 0.25),
        }
    }

    #[inline]
    fn decay_tfi_to_clock(&mut self, ts_ns: i64, floor_lots: f64) {
        if ts_ns <= 0 {
            return;
        }
        if self.last_tfi_ts_ns <= 0 {
            self.last_tfi_ts_ns = ts_ns;
            return;
        }
        if ts_ns <= self.last_tfi_ts_ns {
            return;
        }
        let dt_s = ((ts_ns - self.last_tfi_ts_ns) as f64 * 1e-9).clamp(0.0, 30.0);
        self.last_tfi_ts_ns = ts_ns;
        let a_tfi = (-dt_s / 4.0).exp();
        self.tfi_buy_accum = (self.tfi_buy_accum * a_tfi).max(floor_lots);
        self.tfi_sell_accum = (self.tfi_sell_accum * a_tfi).max(floor_lots);
    }

    #[inline]
    #[must_use]
    fn compute_book_geometry(snap: &SymbolLobSnapshot) -> BoundedScore {
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
        let geom_raw =
            ((bid_mass - ask_mass) / (bid_mass + ask_mass + 1e-6)) * spread_penalty;
        BoundedScore::new_clamped(geom_raw)
    }

    /// Unbiased Gaussian diffusion 5m volatility (zero artificial floor, decays to 0 on flat).
    #[inline]
    #[must_use]
    pub fn update_realized_vol_5m(
        prev_vol: ScaledBps,
        anchor_px: PriceTicks,
        cur_px: PriceTicks,
        dt_ns: i64,
    ) -> ScaledBps {
        let a_px = anchor_px.0.max(1);
        let dp = (cur_px.0 - a_px).abs();
        let ret_bps = (dp.saturating_mul(1_000_000)) / a_px;
        let dt_s = ((dt_ns.max(1_000_000_000) as f64) * 1e-9).clamp(1.0, 300.0);
        let scale = (300.0 / dt_s).sqrt() * GAUSSIAN_MAD_TO_SIGMA;
        let inst_v = (((ret_bps as f64) * scale).round() as i64).clamp(0, 24_000);
        let next_v = ((prev_vol.raw().max(0) * 88 + inst_v * 12) / 100).clamp(0, 24_000);
        ScaledBps(next_v)
    }

    pub fn on_l2_book_update(&mut self, snap: &mut SymbolLobSnapshot) {
        snap.refresh_deep_l1_l5_totals();
        let (init_lots, floor_lots) = Self::flow_prior_bounds(snap);
        if !self.initialized {
            for i in 0..OFI_DEPTH {
                self.prev_bids[i] = snap.bids[i];
                self.prev_asks[i] = snap.asks[i];
            }
            self.ofi_pos_fast = init_lots;
            self.ofi_neg_fast = init_lots;
            self.ofi_pos_med = init_lots;
            self.ofi_neg_med = init_lots;
            self.ofi_pos_slow = init_lots;
            self.ofi_neg_slow = init_lots;
            self.last_l2_ts_ns = snap.ts_local_ns.0;
            if self.last_tfi_ts_ns <= 0 {
                self.last_tfi_ts_ns = snap.ts_local_ns.0;
                self.tfi_buy_accum = init_lots;
                self.tfi_sell_accum = init_lots;
            } else {
                self.decay_tfi_to_clock(snap.ts_local_ns.0, floor_lots);
                let tfi_val = (self.tfi_buy_accum - self.tfi_sell_accum)
                    / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6);
                self.last_tfi = BoundedScore::new_clamped(tfi_val);
            }
            self.last_ofi = BoundedScore::ZERO;
            self.last_geom = Self::compute_book_geometry(snap);
            snap.factor_ofi = self.last_ofi;
            snap.factor_tfi = self.last_tfi;
            snap.factor_geom = self.last_geom;
            self.initialized = true;
            return;
        }

        let weights: [f64; OFI_DEPTH] = [1.0, 0.70, 0.45, 0.25, 0.12];
        let mut prev_worst_bid = 0_i64;
        let mut cur_worst_bid = 0_i64;
        let mut prev_worst_ask = 0_i64;
        let mut cur_worst_ask = 0_i64;
        for i in 0..OFI_DEPTH {
            let pb = self.prev_bids[i].price.0;
            if pb > 0 && (prev_worst_bid == 0 || pb < prev_worst_bid) {
                prev_worst_bid = pb;
            }
            let cb = snap.bids[i].price.0;
            if cb > 0 && (cur_worst_bid == 0 || cb < cur_worst_bid) {
                cur_worst_bid = cb;
            }
            let pa = self.prev_asks[i].price.0;
            if pa > prev_worst_ask {
                prev_worst_ask = pa;
            }
            let ca = snap.asks[i].price.0;
            if ca > cur_worst_ask {
                cur_worst_ask = ca;
            }
        }

        let mut b_flow = 0.0_f64;
        for i in 0..OFI_DEPTH {
            let cur_b = snap.bids[i];
            if cur_b.price.0 <= 0 {
                continue;
            }
            let mut matched_prev = None;
            for j in 0..OFI_DEPTH {
                if self.prev_bids[j].price.0 == cur_b.price.0 {
                    matched_prev = Some(self.prev_bids[j].qty.0);
                    break;
                }
            }
            if let Some(pq) = matched_prev {
                b_flow += weights[i] * ((cur_b.qty.0 - pq) as f64);
            } else if prev_worst_bid == 0 || cur_b.price.0 > prev_worst_bid {
                b_flow += weights[i] * (cur_b.qty.0 as f64);
            }
        }
        for j in 0..OFI_DEPTH {
            let prv_b = self.prev_bids[j];
            if prv_b.price.0 <= 0 {
                continue;
            }
            let mut still_present = false;
            for i in 0..OFI_DEPTH {
                if snap.bids[i].price.0 == prv_b.price.0 {
                    still_present = true;
                    break;
                }
            }
            if !still_present && (cur_worst_bid == 0 || prv_b.price.0 >= cur_worst_bid) {
                b_flow -= weights[j] * (prv_b.qty.0 as f64);
            }
        }

        let mut a_flow = 0.0_f64;
        for i in 0..OFI_DEPTH {
            let cur_a = snap.asks[i];
            if cur_a.price.0 <= 0 {
                continue;
            }
            let mut matched_prev = None;
            for j in 0..OFI_DEPTH {
                if self.prev_asks[j].price.0 == cur_a.price.0 {
                    matched_prev = Some(self.prev_asks[j].qty.0);
                    break;
                }
            }
            if let Some(pq) = matched_prev {
                a_flow += weights[i] * ((cur_a.qty.0 - pq) as f64);
            } else if prev_worst_ask == 0 || cur_a.price.0 < prev_worst_ask {
                a_flow += weights[i] * (cur_a.qty.0 as f64);
            }
        }
        for j in 0..OFI_DEPTH {
            let prv_a = self.prev_asks[j];
            if prv_a.price.0 <= 0 {
                continue;
            }
            let mut still_present = false;
            for i in 0..OFI_DEPTH {
                if snap.asks[i].price.0 == prv_a.price.0 {
                    still_present = true;
                    break;
                }
            }
            if !still_present && (cur_worst_ask == 0 || prv_a.price.0 <= cur_worst_ask) {
                a_flow -= weights[j] * (prv_a.qty.0 as f64);
            }
        }

        for i in 0..OFI_DEPTH {
            self.prev_bids[i] = snap.bids[i];
            self.prev_asks[i] = snap.asks[i];
        }

        let ts_now = snap.ts_local_ns.0;
        let dt_s = if ts_now > 0 && self.last_l2_ts_ns > 0 && ts_now > self.last_l2_ts_ns {
            ((ts_now - self.last_l2_ts_ns) as f64 * 1e-9).clamp(0.0, 30.0)
        } else {
            0.0
        };
        if ts_now > self.last_l2_ts_ns {
            self.last_l2_ts_ns = ts_now;
        }
        let a_fast = (-dt_s / 0.50).exp();
        let a_med = (-dt_s / 3.00).exp();
        let a_slow = (-dt_s / 15.00).exp();

        let raw_ofi = b_flow - a_flow;
        let pos = raw_ofi.max(0.0);
        let neg = (-raw_ofi).max(0.0);

        self.ofi_pos_fast = (self.ofi_pos_fast * a_fast).max(floor_lots) + pos;
        self.ofi_neg_fast = (self.ofi_neg_fast * a_fast).max(floor_lots) + neg;
        self.ofi_pos_med = (self.ofi_pos_med * a_med).max(floor_lots) + pos;
        self.ofi_neg_med = (self.ofi_neg_med * a_med).max(floor_lots) + neg;
        self.ofi_pos_slow = (self.ofi_pos_slow * a_slow).max(floor_lots) + pos;
        self.ofi_neg_slow = (self.ofi_neg_slow * a_slow).max(floor_lots) + neg;

        let f_score = (self.ofi_pos_fast - self.ofi_neg_fast)
            / (self.ofi_pos_fast + self.ofi_neg_fast + 1e-6);
        let m_score = (self.ofi_pos_med - self.ofi_neg_med)
            / (self.ofi_pos_med + self.ofi_neg_med + 1e-6);
        let s_score = (self.ofi_pos_slow - self.ofi_neg_slow)
            / (self.ofi_pos_slow + self.ofi_neg_slow + 1e-6);

        let blended_ofi = 0.50 * f_score + 0.32 * m_score + 0.18 * s_score;
        self.last_ofi = BoundedScore::new_clamped(blended_ofi);

        self.decay_tfi_to_clock(ts_now, floor_lots);
        let tfi_val = (self.tfi_buy_accum - self.tfi_sell_accum)
            / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6);
        self.last_tfi = BoundedScore::new_clamped(tfi_val);
        self.last_geom = Self::compute_book_geometry(snap);

        snap.factor_ofi = self.last_ofi;
        snap.factor_tfi = self.last_tfi;
        snap.factor_geom = self.last_geom;
    }

    pub fn on_trade_print(&mut self, side: Side, qty: LotQty, snap: &mut SymbolLobSnapshot) {
        let q = (qty.0.max(0)) as f64;
        let (_, floor_lots) = Self::flow_prior_bounds(snap);
        self.decay_tfi_to_clock(snap.ts_local_ns.0, floor_lots);
        match side {
            Side::Bid => {
                self.tfi_buy_accum += q;
                snap.cum_buy_trade_lots =
                    LotQty(snap.cum_buy_trade_lots.0.saturating_add(qty.0));
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

    /// Fully elastic, self-learned entry signal evaluator preserving LVDBS L2..L4 reach.
    #[must_use]
    pub fn evaluate_entry_signal_elastic(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
    ) -> SignalVerdict {
        let Some((best_bid, best_ask, spread_ticks)) = snap.bbo_and_spread_ticks() else {
            return SignalVerdict::NeutralNoAction;
        };
        let mid_ticks = PriceTicks(i64::midpoint(best_bid.price.0, best_ask.price.0));
        let Ok(b3x_barrier) = spec.required_3x_barrier_bps(mid_ticks) else {
            return SignalVerdict::NeutralNoAction;
        };
        if spread_ticks > 1 {
            let spread_scaled_bps =
                (spread_ticks.saturating_mul(1_000_000)) / mid_ticks.0.max(1);
            let rt_cost = b3x_barrier.raw() / 3;
            let max_elastic_spread_bps =
                (snap.realized_vol_5m.raw() / 24).clamp(rt_cost / 2, rt_cost);
            if spread_scaled_bps > max_elastic_spread_bps {
                return SignalVerdict::NeutralNoAction;
            }
        }
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

        let eq_vol_anchor = (b3x_barrier.raw().saturating_mul(4)).max(1) as f64;
        let vol_ratio = (snap.realized_vol_5m.raw().max(b3x_barrier.raw()) as f64)
            / eq_vol_anchor;
        let nu_sigma = vol_ratio.sqrt().clamp(0.82, 1.25);
        let edge_relief =
            ((learned_dir_edge_bps.raw().clamp(0, 450) as f64) / 10_000.0).min(0.04);
        let elastic_snr_floor = (0.40 * nu_sigma - edge_relief).clamp(0.34, 0.48);

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

        let dir_sign = side.sign_f64();
        let leader_snr = Self::snr_composite_score(leader_btc_snap) * dir_sign;
        let leader_tfi = leader_btc_snap.factor_tfi.get() * dir_sign;
        let is_follower = snap.symbol_id != leader_btc_snap.symbol_id;
        let lead_lag_bonus_bps = if is_follower && leader_snr >= 0.55 && leader_tfi >= 0.60 {
            let bonus_cap = b3x_barrier.raw() / 9;
            ((leader_snr * (bonus_cap as f64)).round() as i64).clamp(0, bonus_cap)
        } else {
            0
        };

        let raw_vol = snap.realized_vol_5m.raw() as f64;
        let learned_span = learned_mfe_bps
            .raw()
            .max(0)
            .saturating_add(learned_dir_edge_bps.raw().max(0));
        let eq_horizon = b3x_barrier
            .raw()
            .saturating_mul(3)
            .saturating_add(learned_span.saturating_mul(2)) as f64;
        let eff_vol = if raw_vol < eq_horizon {
            (raw_vol * eq_horizon).sqrt()
        } else {
            raw_vol
        };
        let flow_boost = 0.54 + 0.12 * tfi.abs() + 0.08 * geom.abs();
        let base_exc = ((composite.abs() * eff_vol * flow_boost).round() as i64)
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

    #[must_use]
    pub fn evaluate_entry_signal_with_leader(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
    ) -> SignalVerdict {
        let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
        } else {
            snap.mark_price
        };
        let b3x = spec
            .required_3x_barrier_bps(mid_px)
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS);
        let neutral_mfe = ScaledBps((b3x.raw() * 4) / 9);
        let neutral_edge = ScaledBps(b3x.raw() / 8);
        Self::evaluate_entry_signal_elastic(
            spec,
            snap,
            leader_btc_snap,
            neutral_mfe,
            neutral_edge,
        )
    }

    /// Computes continuous L2/L3 Liquidity Vacuum Index V_t with spatial tick-gap dilation.
    #[inline]
    #[must_use]
    pub fn compute_vacuum_index_l2_l3(open_side: Side, snap: &SymbolLobSnapshot) -> f64 {
        let mut own_q = 0_i64;
        let mut opp_q = 0_i64;
        let mut first_opp_px = 0_i64;
        let mut last_opp_px = 0_i64;
        let mut opp_populated = 0_usize;

        for i in 0..4 {
            let (own_lvl, opp_lvl) = match open_side {
                Side::Bid => (snap.bids[i], snap.asks[i]),
                Side::Ask => (snap.asks[i], snap.bids[i]),
            };
            if own_lvl.price.0 > 0 && own_lvl.qty.0 > 0 {
                own_q = own_q.saturating_add(own_lvl.qty.0);
            }
            if opp_lvl.price.0 > 0 && opp_lvl.qty.0 > 0 {
                opp_q = opp_q.saturating_add(opp_lvl.qty.0);
                if opp_populated == 0 {
                    first_opp_px = opp_lvl.price.0;
                }
                last_opp_px = opp_lvl.price.0;
                opp_populated += 1;
            }
        }
        let gap_mult = if opp_populated >= 2 {
            let span_ticks = (last_opp_px - first_opp_px).abs().max(1) as f64;
            let mean_step = span_ticks / ((opp_populated - 1) as f64);
            mean_step.sqrt().clamp(1.0, 1.50)
        } else {
            1.0
        };
        let dir_s = open_side.sign_f64();
        let signed_ofi = (snap.factor_ofi.get() * dir_s).max(0.0);
        let signed_tfi = (snap.factor_tfi.get() * dir_s).max(0.0);
        let depth_ratio = (own_q.max(1) as f64) / (opp_q.max(1) as f64);
        depth_ratio * gap_mult * (1.0 + signed_ofi + 1.25 * signed_tfi)
    }

    /// Selects L1..L4 exit level respecting structural floor & endogenous volatility cap.
    #[inline]
    #[must_use]
    pub fn compute_l2_l3_vacuum_surf_target_with_structural(
        open_side: Side,
        snap: &SymbolLobSnapshot,
        quantum_floor_price: PriceTicks,
        structural_l1_floor_price: PriceTicks,
    ) -> (usize, PriceTicks, f64) {
        let vac_idx = Self::compute_vacuum_index_l2_l3(open_side, snap);
        let ref_px = quantum_floor_price.0.max(structural_l1_floor_price.0).max(1);
        let b3x_raw = spec_from_symbol_id(snap.symbol_id)
            .and_then(|sp| sp.required_3x_barrier_bps(PriceTicks(ref_px)).ok())
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw();
        let vol_raw = snap.realized_vol_5m.raw();
        let eq_anchor = (b3x_raw.saturating_mul(4)).max(1) as f64;
        let vol_ratio = (vol_raw.max(b3x_raw) as f64) / eq_anchor;
        let nu_v = vol_ratio.sqrt().clamp(0.85, 1.15);
        let dir_s = open_side.sign_f64();
        let signed_tfi = snap.factor_tfi.get() * dir_s;

        let level_idx = if vac_idx >= (2.15 * nu_v) && signed_tfi >= 0.42 {
            3_usize
        } else if vac_idx >= (1.42 * nu_v) && signed_tfi >= 0.18 {
            2_usize
        } else {
            usize::from(vac_idx >= (1.04 * nu_v))
        };

        let eff_struct = match open_side {
            Side::Bid => structural_l1_floor_price.0.max(quantum_floor_price.0).max(1),
            Side::Ask => {
                if structural_l1_floor_price.0 > 0 {
                    structural_l1_floor_price.0.min(quantum_floor_price.0).max(1)
                } else {
                    quantum_floor_price.0.max(1)
                }
            }
        };

        let min_surf_cap = b3x_raw.saturating_mul(3);
        let max_surf_cap = b3x_raw.saturating_mul(7);
        let max_surf_bps = vol_raw.clamp(min_surf_cap, max_surf_cap);
        let anchor_t = eff_struct.max(quantum_floor_price.0).max(1);
        let max_offset_t = ((anchor_t.saturating_mul(max_surf_bps)) / 1_000_000).max(4);

        let book_px = match open_side {
            Side::Bid => {
                let p = snap.asks[level_idx].price.0;
                let fallback = snap.asks[0].price.0.max(eff_struct);
                let raw_t = if p > 0 { p.max(eff_struct) } else { fallback };
                let cap_t = eff_struct.saturating_add(max_offset_t);
                PriceTicks(raw_t.min(cap_t))
            }
            Side::Ask => {
                let p = snap.bids[level_idx].price.0;
                let fallback = if snap.bids[0].price.0 > 0 {
                    snap.bids[0].price.0.min(eff_struct)
                } else {
                    eff_struct
                };
                let raw_t = if p > 0 {
                    p.min(eff_struct).max(1)
                } else {
                    fallback.max(1)
                };
                let floor_cap_t = eff_struct.saturating_sub(max_offset_t).max(1);
                PriceTicks(raw_t.max(floor_cap_t))
            }
        };
        (level_idx + 1, book_px, vac_idx)
    }
}

#[cfg(test)]
#[allow(clippy::needless_range_loop)]
mod tests {
    use super::*;
    use crate::types::{TimestampNs, APPROVED_SPECS};

    #[test]
    fn test_lvdbs_vacuum_surf_levels_1_to_4() {
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
        let (lvl4, px4, vac4) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                Side::Bid,
                &snap,
                PriceTicks(8_202),
                PriceTicks(8_202),
            );
        assert_eq!(lvl4, 4);
        assert_eq!(px4, PriceTicks(8_211));
        assert!(vac4 >= 2.20);

        snap.asks[0].qty = LotQty(2_200);
        snap.factor_ofi = BoundedScore::new_clamped(0.10);
        snap.factor_tfi = BoundedScore::new_clamped(0.08);
        let (lvl1, px1, vac1) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                Side::Bid,
                &snap,
                PriceTicks(8_202),
                PriceTicks(8_202),
            );
        assert_eq!(lvl1, 1);
        assert_eq!(px1, PriceTicks(8_203));
        assert!(vac1 < 1.05);
    }

    #[test]
    fn test_price_matched_ofi_clock_decay_and_endogenous_vol_guard() {
        let mut st = OrthogonalFactorState::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[7].symbol_id;
        snap.realized_vol_5m = ScaledBps(6_500);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        for i in 0..5 {
            snap.bids[i] = BookLevel {
                price: PriceTicks(15_700 - (i as i64)),
                qty: LotQty(5_000),
            };
            snap.asks[i] = BookLevel {
                price: PriceTicks(15_702 + (i as i64)),
                qty: LotQty(5_000),
            };
        }
        st.on_l2_book_update(&mut snap);
        snap.ts_local_ns = TimestampNs(1_100_000_000);
        snap.apply_level_update(Side::Bid, PriceTicks(15_701), LotQty(1));
        st.on_l2_book_update(&mut snap);
        assert!(snap.factor_ofi.get() > 0.05 && snap.factor_ofi.get() < 0.45);

        snap.bids[0] = BookLevel { price: PriceTicks(15_693), qty: LotQty(500) };
        snap.asks[0] = BookLevel { price: PriceTicks(15_699), qty: LotQty(400) };
        snap.factor_ofi = BoundedScore::new_clamped(0.80);
        snap.factor_tfi = BoundedScore::new_clamped(0.85);
        snap.factor_geom = BoundedScore::new_clamped(0.45);
        let wide_v = OrthogonalFactorState::evaluate_entry_signal_with_leader(
            APPROVED_SPECS[7],
            &snap,
            &snap,
        );
        assert_eq!(wide_v, SignalVerdict::NeutralNoAction);

        snap.realized_vol_5m = ScaledBps(2_272);
        snap.bids[0] = BookLevel { price: PriceTicks(15_586), qty: LotQty(500) };
        snap.asks[0] = BookLevel { price: PriceTicks(15_587), qty: LotQty(400) };
        snap.factor_ofi = BoundedScore::new_clamped(-0.45);
        snap.factor_tfi = BoundedScore::new_clamped(-0.95);
        snap.factor_geom = BoundedScore::new_clamped(-0.24);
        let calm_ok = OrthogonalFactorState::evaluate_entry_signal_elastic(
            APPROVED_SPECS[7],
            &snap,
            &snap,
            ScaledBps(680),
            ScaledBps(178),
        );
        assert!(matches!(
            calm_ok,
            SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, .. }
        ));

        let mut v = ScaledBps(6_000);
        for _ in 0..25 {
            v = OrthogonalFactorState::update_realized_vol_5m(
                v,
                PriceTicks(15_700),
                PriceTicks(15_700),
                5_000_000_000,
            );
        }
        let endog_floor = APPROVED_SPECS[7]
            .endogenous_min_vol_5m_bps(PriceTicks(15_700))
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS);
        assert!(v.raw() < endog_floor.raw());
    }

    #[test]
    fn test_same_packet_trade_burst_zero_phantom_decay_and_lvdbs_cap() {
        let mut st = OrthogonalFactorState::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[5].symbol_id;
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.ts_local_ns = TimestampNs(2_000_000_000);
        st.on_trade_print(Side::Ask, LotQty(100), &mut snap);
        let sell_before = st.tfi_sell_accum;
        for _ in 0..50 {
            st.on_trade_print(Side::Bid, LotQty(10), &mut snap);
        }
        assert!((st.tfi_sell_accum - sell_before).abs() < 1e-12);
        assert!((st.tfi_buy_accum - 501.0).abs() < 1e-9);

        for i in 0..4 {
            snap.bids[i] = BookLevel {
                price: PriceTicks(8_500 - i as i64),
                qty: LotQty(400),
            };
        }
        snap.asks[0] = BookLevel { price: PriceTicks(8_505), qty: LotQty(100) };
        snap.asks[1] = BookLevel { price: PriceTicks(8_508), qty: LotQty(100) };
        snap.asks[2] = BookLevel { price: PriceTicks(8_512), qty: LotQty(100) };
        snap.asks[3] = BookLevel { price: PriceTicks(9_200), qty: LotQty(100) };
        snap.factor_ofi = BoundedScore::new_clamped(0.70);
        snap.factor_tfi = BoundedScore::new_clamped(0.80);
        let (lvl, capped_px, _) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                Side::Bid,
                &snap,
                PriceTicks(8_504),
                PriceTicks(8_504),
            );
        assert_eq!(lvl, 4);
        assert!(capped_px.0 > 8_512 && capped_px.0 < 9_200);
    }

    #[test]
    fn test_f11_f04_scale_priors_book_only_reset_and_structural_lvdbs() {
        let mut st_wif = OrthogonalFactorState::new();
        let mut wif = SymbolLobSnapshot::new_empty();
        wif.symbol_id = APPROVED_SPECS[8].symbol_id;
        wif.ts_local_ns = TimestampNs(1_000_000_000);
        wif.bids[0] = BookLevel { price: PriceTicks(1_600), qty: LotQty(800) };
        wif.asks[0] = BookLevel { price: PriceTicks(1_601), qty: LotQty(750) };
        st_wif.on_l2_book_update(&mut wif);
        wif.ts_local_ns = TimestampNs(26_000_000_000);
        st_wif.on_trade_print(Side::Bid, LotQty(1), &mut wif);
        assert!(wif.factor_tfi.get() < 0.20);
        st_wif.on_trade_print(Side::Bid, LotQty(120), &mut wif);
        assert!(wif.factor_tfi.get() > 0.85);

        st_wif.reset_l2_book_only();
        let preserved_tfi = wif.factor_tfi.get();
        wif.bids[0] = BookLevel { price: PriceTicks(1_602), qty: LotQty(1_200) };
        wif.asks[0] = BookLevel { price: PriceTicks(1_603), qty: LotQty(400) };
        st_wif.on_l2_book_update(&mut wif);
        assert!((wif.factor_tfi.get() - preserved_tfi).abs() < 1e-6);
        assert!(wif.factor_geom.get() > 0.25);

        let mut dense = SymbolLobSnapshot::new_empty();
        let mut gapped = SymbolLobSnapshot::new_empty();
        for i in 0..4 {
            dense.bids[i] = BookLevel {
                price: PriceTicks(8_202 - i as i64),
                qty: LotQty(250),
            };
            dense.asks[i] = BookLevel {
                price: PriceTicks(8_203 + i as i64),
                qty: LotQty(250),
            };
            gapped.bids[i] = dense.bids[i];
            gapped.asks[i] = BookLevel {
                price: PriceTicks(8_203 + (i as i64) * 3),
                qty: LotQty(250),
            };
        }
        let v_dense = OrthogonalFactorState::compute_vacuum_index_l2_l3(Side::Bid, &dense);
        let v_gap = OrthogonalFactorState::compute_vacuum_index_l2_l3(Side::Bid, &gapped);
        assert!(v_gap > v_dense * 1.40);

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.realized_vol_5m = ScaledBps(6_500);
        for i in 0..4 {
            btc.bids[i] = BookLevel {
                price: PriceTicks(759_000 - i as i64),
                qty: LotQty(100),
            };
            btc.asks[i] = BookLevel {
                price: PriceTicks(759_001 + i as i64),
                qty: LotQty(100),
            };
        }
        let (_, px_bid, _) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                Side::Bid,
                &btc,
                PriceTicks(759_334),
                PriceTicks(759_471),
            );
        assert_eq!(px_bid, PriceTicks(759_471));
        let (_, px_ask, _) =
            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                Side::Ask,
                &btc,
                PriceTicks(758_666),
                PriceTicks(758_529),
            );
        assert_eq!(px_ask, PriceTicks(758_529));
    }
}
