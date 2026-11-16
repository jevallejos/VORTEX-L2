//! Orthogonal 3-Factor Alpha, Anti-Spoof GEOM Weights, Dynamic 3x Barrier, Epps 5s & LVDBS (V4.6).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::needless_range_loop,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use crate::fee_cadence_beta::FeeSchedule;
use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{
    spec_from_symbol_id, BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs,
};

pub const OFI_DEPTH: usize = 5;
pub const GEOM_DEPTH: usize = 10;

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const BASE_VOL_WIN_NS: i64 = VOL_5M_HORIZON_NS / 60;
const GAUSSIAN_MAD_TO_SIGMA: f64 = 1.253_314_14;
const PARKINSON_HL_TO_SIGMA: f64 = 0.600_561_20;
const V45_PHI: f64 = 1.618_033_988_749_895;
const V45_INV_PHI: f64 = 0.618_033_988_749_895;
const V45_INV_PHI_SQ: f64 = 0.381_966_011_250_105;
const V45_EPPS_BUCKET_NS: i64 = 5_000_000_000;
const V45_EPPS_TAU_SECS: f64 = 120.0;
const V45_EPPS_PRIOR_VAR_BPS2: f64 = 25.0;
const V45_FAST_RATE_TAU_SECS: f64 = 2.0;
const V45_EQ_RATE_TAU_SECS: f64 = 60.0;
const V45_SPOOF_TAU_SECS: f64 = 15.0;
const V45_SEED_RATE_HZ: f64 = 10.0;

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
    vol_window_start_ts_ns: i64,
    vol_anchor_px: PriceTicks,
    vol_low_px: PriceTicks,
    vol_high_px: PriceTicks,
    ofi_pos_fast: f64,
    ofi_neg_fast: f64,
    ofi_pos_med: f64,
    ofi_neg_med: f64,
    ofi_pos_slow: f64,
    ofi_neg_slow: f64,
    tfi_buy_accum: f64,
    tfi_sell_accum: f64,
    spoof_density_ewma: f64,
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
            vol_window_start_ts_ns: 0,
            vol_anchor_px: PriceTicks(0),
            vol_low_px: PriceTicks(0),
            vol_high_px: PriceTicks(0),
            ofi_pos_fast: 1.0,
            ofi_neg_fast: 1.0,
            ofi_pos_med: 1.0,
            ofi_neg_med: 1.0,
            ofi_pos_slow: 1.0,
            ofi_neg_slow: 1.0,
            tfi_buy_accum: 1.0,
            tfi_sell_accum: 1.0,
            spoof_density_ewma: 0.0,
            last_ofi: BoundedScore::ZERO,
            last_tfi: BoundedScore::ZERO,
            last_geom: BoundedScore::ZERO,
        }
    }

    #[inline]
    #[must_use]
    pub const fn spoofing_density(&self) -> f64 {
        self.spoof_density_ewma
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

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

    #[inline]
    #[must_use]
    pub fn flow_prior_bounds(snap: &SymbolLobSnapshot) -> (f64, f64) {
        let ref_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
        } else if snap.mark_price.0 > 0 {
            snap.mark_price
        } else {
            snap.last_trade_price
        };
        match spec_from_symbol_id(snap.symbol_id) {
            Some(spec) if ref_px.0 > 0 => {
                let q_scale = spec.characteristic_lot_scale(ref_px);
                (q_scale.clamp(1.0, 250.0), (0.25 * q_scale).clamp(0.25, 125.0))
            }
            _ => (1.0, 0.25),
        }
    }

    #[inline]
    fn decay_tfi_to_clock(&mut self, ts_ns: i64, floor_lots: f64) {
        if ts_ns <= 0 {
            return;
        }
        if self.last_tfi_ts_ns <= 0 || ts_ns <= self.last_tfi_ts_ns {
            if self.last_tfi_ts_ns <= 0 { self.last_tfi_ts_ns = ts_ns; }
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
        let (mut bid_mass, mut ask_mass) = (0.0_f64, 0.0_f64);
        for i in 0..GEOM_DEPTH {
            let decay = 1.0 / ((i + 1) as f64);
            if snap.bids[i].price.0 > 0 && snap.bids[i].qty.0 > 0 {
                bid_mass += (snap.bids[i].qty.0 as f64) * decay;
            }
            if snap.asks[i].price.0 > 0 && snap.asks[i].qty.0 > 0 {
                ask_mass += (snap.asks[i].qty.0 as f64) * decay;
            }
        }
        let spread_ticks = if snap.asks[0].price.0 > snap.bids[0].price.0 && snap.bids[0].price.0 > 0 {
            (snap.asks[0].price.0 - snap.bids[0].price.0) as f64
        } else {
            1.0
        };
        let geom_raw = ((bid_mass - ask_mass) / (bid_mass + ask_mass + 1e-6)) / spread_ticks.sqrt().max(1.0);
        BoundedScore::new_clamped(geom_raw)
    }

    #[inline]
    #[must_use]
    pub fn update_realized_vol_5m_parkinson(
        prev_vol: ScaledBps,
        anchor_px: PriceTicks,
        low_px: PriceTicks,
        high_px: PriceTicks,
        cur_px: PriceTicks,
        dt_ns: i64,
    ) -> ScaledBps {
        let a_px = anchor_px.0.max(1);
        let eff_cur = if cur_px.0 > 0 { cur_px.0 } else { a_px };
        let eff_lo = if low_px.0 > 0 { low_px.0.min(a_px).min(eff_cur) } else { a_px.min(eff_cur) };
        let eff_hi = high_px.0.max(a_px).max(eff_cur);
        let net_bps = (((eff_cur - a_px).abs().saturating_mul(1_000_000)) as f64) / (a_px as f64);
        let hl_bps = (((eff_hi - eff_lo).max(0).saturating_mul(1_000_000)) as f64) / (a_px as f64);
        let horizon_s = (VOL_5M_HORIZON_NS as f64) * 1e-9;
        let dt_s = ((dt_ns.max(500_000_000) as f64) * 1e-9).clamp(0.5, horizon_s);
        let root_t = (horizon_s / dt_s).sqrt();
        let inst_v = (net_bps * GAUSSIAN_MAD_TO_SIGMA * root_t)
            .max(hl_bps * PARKINSON_HL_TO_SIGMA * root_t)
            .clamp(0.0, 24_000.0);
        let prev_f = (prev_vol.raw().max(0) as f64).clamp(0.0, 24_000.0);
        let tau_s = if inst_v > prev_f { horizon_s / 12.0 } else { horizon_s / 4.0 };
        let alpha = (1.0 - (-dt_s / tau_s).exp()).clamp(0.01, 1.0);
        ScaledBps(((prev_f + alpha * (inst_v - prev_f)).round() as i64).clamp(0, 24_000))
    }

    #[inline]
    #[must_use]
    pub fn update_realized_vol_5m(prev_vol: ScaledBps, anchor_px: PriceTicks, cur_px: PriceTicks, dt_ns: i64) -> ScaledBps {
        Self::update_realized_vol_5m_parkinson(
            prev_vol, anchor_px, PriceTicks(anchor_px.0.min(cur_px.0)), PriceTicks(anchor_px.0.max(cur_px.0)), cur_px, dt_ns,
        )
    }

    #[inline]
    pub fn observe_mid_and_step_elastic_vol(&mut self, spec: PairSpec, obs_px: PriceTicks, ts_ns: i64, current_vol: ScaledBps) -> ScaledBps {
        if obs_px.0 <= 0 || ts_ns <= 0 { return current_vol; }
        if self.vol_anchor_px.0 <= 0 || self.vol_window_start_ts_ns <= 0 {
            self.vol_anchor_px = obs_px;
            self.vol_low_px = obs_px;
            self.vol_high_px = obs_px;
            self.vol_window_start_ts_ns = ts_ns;
            return current_vol;
        }
        if obs_px.0 < self.vol_low_px.0 { self.vol_low_px = obs_px; }
        if obs_px.0 > self.vol_high_px.0 { self.vol_high_px = obs_px; }
        let dt_ns = ts_ns.saturating_sub(self.vol_window_start_ts_ns);
        let min_win_ns = (BASE_VOL_WIN_NS * 3) / 10;
        if dt_ns < min_win_ns { return current_vol; }
        let b3x = spec.required_3x_barrier_bps(self.vol_anchor_px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS).raw().max(1);
        let raw_win_ns = ((BASE_VOL_WIN_NS as i128 * (b3x.saturating_mul(4) as i128)) / (current_vol.raw().max(b3x) as i128)) as i64;
        if dt_ns < raw_win_ns.clamp(min_win_ns, BASE_VOL_WIN_NS * 2) { return current_vol; }
        let updated = Self::update_realized_vol_5m_parkinson(current_vol, self.vol_anchor_px, self.vol_low_px, self.vol_high_px, obs_px, dt_ns);
        self.vol_anchor_px = obs_px;
        self.vol_low_px = obs_px;
        self.vol_high_px = obs_px;
        self.vol_window_start_ts_ns = ts_ns;
        updated
    }

    pub fn on_l2_book_update(&mut self, snap: &mut SymbolLobSnapshot) {
        snap.refresh_deep_l1_l5_totals();
        let (init_lots, floor_lots) = Self::flow_prior_bounds(snap);
        if !self.initialized {
            for i in 0..OFI_DEPTH {
                self.prev_bids[i] = snap.bids[i];
                self.prev_asks[i] = snap.asks[i];
            }
            self.ofi_pos_fast = init_lots; self.ofi_neg_fast = init_lots;
            self.ofi_pos_med = init_lots; self.ofi_neg_med = init_lots;
            self.ofi_pos_slow = init_lots; self.ofi_neg_slow = init_lots;
            self.last_l2_ts_ns = snap.ts_local_ns.0;
            if self.last_tfi_ts_ns <= 0 {
                self.last_tfi_ts_ns = snap.ts_local_ns.0;
                self.tfi_buy_accum = init_lots;
                self.tfi_sell_accum = init_lots;
            } else {
                self.decay_tfi_to_clock(snap.ts_local_ns.0, floor_lots);
                self.last_tfi = BoundedScore::new_clamped((self.tfi_buy_accum - self.tfi_sell_accum) / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6));
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
        let (mut prev_wb, mut cur_wb, mut prev_wa, mut cur_wa) = (0_i64, 0_i64, 0_i64, 0_i64);
        for i in 0..OFI_DEPTH {
            let pb = self.prev_bids[i].price.0;
            if pb > 0 && (prev_wb == 0 || pb < prev_wb) { prev_wb = pb; }
            let cb = snap.bids[i].price.0;
            if cb > 0 && (cur_wb == 0 || cb < cur_wb) { cur_wb = cb; }
            if self.prev_asks[i].price.0 > prev_wa { prev_wa = self.prev_asks[i].price.0; }
            if snap.asks[i].price.0 > cur_wa { cur_wa = snap.asks[i].price.0; }
        }
        let mut b_flow = 0.0_f64;
        for i in 0..OFI_DEPTH {
            let cur_b = snap.bids[i];
            if cur_b.price.0 <= 0 { continue; }
            let mut matched = None;
            for j in 0..OFI_DEPTH {
                if self.prev_bids[j].price.0 == cur_b.price.0 { matched = Some(self.prev_bids[j].qty.0); break; }
            }
            if let Some(pq) = matched { b_flow += weights[i] * ((cur_b.qty.0 - pq) as f64); }
            else if prev_wb == 0 || cur_b.price.0 > prev_wb { b_flow += weights[i] * (cur_b.qty.0 as f64); }
        }
        for j in 0..OFI_DEPTH {
            let prv_b = self.prev_bids[j];
            if prv_b.price.0 <= 0 { continue; }
            let mut present = false;
            for i in 0..OFI_DEPTH { if snap.bids[i].price.0 == prv_b.price.0 { present = true; break; } }
            if !present && (cur_wb == 0 || prv_b.price.0 >= cur_wb) { b_flow -= weights[j] * (prv_b.qty.0 as f64); }
        }
        let mut a_flow = 0.0_f64;
        for i in 0..OFI_DEPTH {
            let cur_a = snap.asks[i];
            if cur_a.price.0 <= 0 { continue; }
            let mut matched = None;
            for j in 0..OFI_DEPTH {
                if self.prev_asks[j].price.0 == cur_a.price.0 { matched = Some(self.prev_asks[j].qty.0); break; }
            }
            if let Some(pq) = matched { a_flow += weights[i] * ((cur_a.qty.0 - pq) as f64); }
            else if prev_wa == 0 || cur_a.price.0 < prev_wa { a_flow += weights[i] * (cur_a.qty.0 as f64); }
        }
        for j in 0..OFI_DEPTH {
            let prv_a = self.prev_asks[j];
            if prv_a.price.0 <= 0 { continue; }
            let mut present = false;
            for i in 0..OFI_DEPTH { if snap.asks[i].price.0 == prv_a.price.0 { present = true; break; } }
            if !present && (cur_wa == 0 || prv_a.price.0 <= cur_wa) { a_flow -= weights[j] * (prv_a.qty.0 as f64); }
        }
        for i in 0..OFI_DEPTH {
            self.prev_bids[i] = snap.bids[i];
            self.prev_asks[i] = snap.asks[i];
        }
        let ts_now = snap.ts_local_ns.0;
        let dt_s = if ts_now > 0 && self.last_l2_ts_ns > 0 && ts_now > self.last_l2_ts_ns {
            ((ts_now - self.last_l2_ts_ns) as f64 * 1e-9).clamp(0.0, 30.0)
        } else { 0.0 };
        if ts_now > self.last_l2_ts_ns { self.last_l2_ts_ns = ts_now; }
        let (a_fast, a_med, a_slow) = ((-dt_s / 0.50).exp(), (-dt_s / 3.00).exp(), (-dt_s / 15.00).exp());
        let raw_ofi = b_flow - a_flow;
        let (pos, neg) = (raw_ofi.max(0.0), (-raw_ofi).max(0.0));
        self.ofi_pos_fast = (self.ofi_pos_fast * a_fast).max(floor_lots) + pos;
        self.ofi_neg_fast = (self.ofi_neg_fast * a_fast).max(floor_lots) + neg;
        self.ofi_pos_med = (self.ofi_pos_med * a_med).max(floor_lots) + pos;
        self.ofi_neg_med = (self.ofi_neg_med * a_med).max(floor_lots) + neg;
        self.ofi_pos_slow = (self.ofi_pos_slow * a_slow).max(floor_lots) + pos;
        self.ofi_neg_slow = (self.ofi_neg_slow * a_slow).max(floor_lots) + neg;
        let f_sc = (self.ofi_pos_fast - self.ofi_neg_fast) / (self.ofi_pos_fast + self.ofi_neg_fast + 1e-6);
        let m_sc = (self.ofi_pos_med - self.ofi_neg_med) / (self.ofi_pos_med + self.ofi_neg_med + 1e-6);
        let s_sc = (self.ofi_pos_slow - self.ofi_neg_slow) / (self.ofi_pos_slow + self.ofi_neg_slow + 1e-6);
        self.last_ofi = BoundedScore::new_clamped(0.50 * f_sc + 0.32 * m_sc + 0.18 * s_sc);
        self.decay_tfi_to_clock(ts_now, floor_lots);
        self.last_tfi = BoundedScore::new_clamped((self.tfi_buy_accum - self.tfi_sell_accum) / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6));
        self.last_geom = Self::compute_book_geometry(snap);
        if dt_s > 0.0 {
            let ofi_f = self.last_ofi.get();
            let tfi_f = self.last_tfi.get();
            let i_spoof = if (ofi_f.abs() >= 0.35 && (ofi_f * tfi_f) < -0.05) || (ofi_f.abs() > 0.55 && tfi_f.abs() < 0.08) {
                1.0
            } else {
                0.0
            };
            let alpha_sp = 1.0 - (-dt_s / V45_SPOOF_TAU_SECS).exp();
            self.spoof_density_ewma = ((1.0 - alpha_sp) * self.spoof_density_ewma + alpha_sp * i_spoof).clamp(0.0, 1.0);
        }
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
                snap.cum_buy_trade_lots = LotQty(snap.cum_buy_trade_lots.0.saturating_add(qty.0));
            }
            Side::Ask => {
                self.tfi_sell_accum += q;
                snap.cum_sell_trade_lots = LotQty(snap.cum_sell_trade_lots.0.saturating_add(qty.0));
            }
        }
        self.last_tfi = BoundedScore::new_clamped((self.tfi_buy_accum - self.tfi_sell_accum) / (self.tfi_buy_accum + self.tfi_sell_accum + 1e-6));
        snap.factor_tfi = self.last_tfi;
    }

    #[inline]
    #[must_use]
    pub fn snr_composite_score(snap: &SymbolLobSnapshot) -> f64 {
        0.45 * snap.factor_ofi.get() + 0.35 * snap.factor_tfi.get() + 0.20 * snap.factor_geom.get()
    }

    #[inline]
    #[must_use]
    pub fn adaptive_weights_from_spoof_density(spoof_density: f64) -> (f64, f64, f64) {
        let excess = ((spoof_density.clamp(0.0, 1.0) - 0.15) / 0.55).clamp(0.0, 1.0);
        let w_geom = 0.20 * (1.0 - V45_INV_PHI * excess);
        let dw = 0.20 - w_geom;
        let w_tfi = 0.35 + V45_INV_PHI * dw;
        let w_ofi = 1.0 - w_tfi - w_geom;
        (w_ofi, w_tfi, w_geom)
    }

    #[inline]
    #[must_use]
    pub fn adaptive_snr_from_snapshot(snap: &SymbolLobSnapshot, spoof_density: f64) -> BoundedScore {
        let (w_ofi, w_tfi, w_geom) = Self::adaptive_weights_from_spoof_density(spoof_density);
        BoundedScore::new_clamped(
            w_ofi * snap.factor_ofi.get() + w_tfi * snap.factor_tfi.get() + w_geom * snap.factor_geom.get(),
        )
    }

    /// V4.6 Island 1A: Entry evaluator coupled to anti-spoof GEOM weights, dynamic 3x VIP barrier & signed Epps beta.
    #[must_use]
    pub fn evaluate_entry_signal_v46(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> SignalVerdict {
        let Some((best_bid, best_ask, spread_ticks)) = snap.bbo_and_spread_ticks() else {
            return SignalVerdict::NeutralNoAction;
        };
        let mid_ticks = PriceTicks(i64::midpoint(best_bid.price.0, best_ask.price.0));
        let Ok(raw_tick_bps) = spec.tick_to_price_scaled_bps(mid_ticks) else {
            return SignalVerdict::NeutralNoAction;
        };
        let b3x_barrier = spec.three_x_friction_barrier_bps_dynamic(mid_ticks, taker_prob_pm, fees);
        if spread_ticks > 1 {
            let spread_scaled_bps = (spread_ticks.saturating_mul(1_000_000)) / mid_ticks.0.max(1);
            let rt_cost = (b3x_barrier.raw() / 3).max(50);
            let max_elastic_spread_bps = (snap.realized_vol_5m.raw() / 24).clamp(rt_cost / 2, rt_cost);
            if spread_scaled_bps > max_elastic_spread_bps {
                return SignalVerdict::NeutralNoAction;
            }
        }
        if snap.realized_vol_5m.raw() < b3x_barrier.raw() {
            return SignalVerdict::NeutralNoAction;
        }
        let p_exc_pm = (taker_prob_pm.clamp(20, 450) - 50).max(0);
        let tox_div_x10 = 100 + p_exc_pm * 4;
        let elastic_max_tick = (snap.realized_vol_5m.raw().saturating_mul(10)) / tox_div_x10;
        if raw_tick_bps.raw() > elastic_max_tick {
            return SignalVerdict::NeutralNoAction;
        }
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        let geom = snap.factor_geom.get();
        if (ofi.abs() > 0.32 && tfi.abs() > 0.32 && (ofi * tfi) < -0.08) || (ofi.abs() > 0.55 && tfi.abs() < 0.08) {
            return SignalVerdict::SpoofingVeto;
        }
        let eq_vol_anchor = (b3x_barrier.raw().saturating_mul(4)).max(1) as f64;
        let nu_sigma = ((snap.realized_vol_5m.raw().max(b3x_barrier.raw()) as f64) / eq_vol_anchor).sqrt().clamp(0.82, 1.25);
        let edge_relief = ((learned_dir_edge_bps.raw().clamp(0, 450) as f64) / 10_000.0).min(0.04);
        let elastic_snr_floor = (0.40 * nu_sigma - edge_relief).clamp(0.34, 0.48);
        let composite = Self::adaptive_snr_from_snapshot(snap, spoof_density).get();
        if composite.abs() < elastic_snr_floor {
            return SignalVerdict::NeutralNoAction;
        }
        let side = if composite > 0.0 {
            if ofi <= 0.15 || tfi <= 0.12 || geom < -0.05 { return SignalVerdict::NeutralNoAction; }
            Side::Bid
        } else {
            if ofi >= -0.15 || tfi >= -0.12 || geom > 0.05 { return SignalVerdict::NeutralNoAction; }
            Side::Ask
        };
        let dir_sign = side.sign_f64();
        let is_follower = snap.symbol_id != leader_btc_snap.symbol_id;
        let beta_c = epps_signed_beta.clamp(-V45_PHI, V45_PHI);
        let aligned_leader = beta_c * (Self::snr_composite_score(leader_btc_snap) * dir_sign);
        let beta_sign = if beta_c >= 0.0 { 1.0 } else { -1.0 };
        let leader_tfi_al = leader_btc_snap.factor_tfi.get() * dir_sign * beta_sign;
        let lead_lag_adj_bps = if !is_follower || beta_c.abs() < 1e-6 {
            0
        } else if aligned_leader >= 0.55 && leader_tfi_al >= 0.55 {
            let bonus_cap = b3x_barrier.raw() / 9;
            ((aligned_leader * (bonus_cap as f64)).round() as i64).clamp(0, bonus_cap)
        } else if aligned_leader <= -0.35 {
            let drag_cap = b3x_barrier.raw() / 6;
            -((((-aligned_leader) - 0.35) * (drag_cap as f64)).round() as i64).clamp(0, drag_cap)
        } else {
            0
        };
        let raw_vol = snap.realized_vol_5m.raw() as f64;
        let learned_span = learned_mfe_bps.raw().max(0).saturating_add(learned_dir_edge_bps.raw().max(0));
        let eq_horizon = b3x_barrier.raw().saturating_mul(3).saturating_add(learned_span.saturating_mul(2)) as f64;
        let eff_vol = if raw_vol < eq_horizon { (raw_vol * eq_horizon).sqrt() } else { raw_vol };
        let flow_boost = 0.54 + 0.12 * tfi.abs() + 0.08 * geom.abs();
        let expected_excursion = ScaledBps(((composite.abs() * eff_vol * flow_boost).round() as i64).saturating_add(lead_lag_adj_bps));
        if expected_excursion.raw() >= b3x_barrier.raw() {
            SignalVerdict::AuthorizeMakerEntry { side, target_excursion_scaled_bps: expected_excursion }
        } else {
            SignalVerdict::NeutralNoAction
        }
    }

    #[must_use]
    pub fn evaluate_entry_signal_elastic(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
    ) -> SignalVerdict {
        let beta_default = if snap.symbol_id == leader_btc_snap.symbol_id { 0.0 } else { 1.0 };
        Self::evaluate_entry_signal_v46(
            spec, snap, leader_btc_snap, learned_mfe_bps, learned_dir_edge_bps,
            50, FeeSchedule::VIP0_DEFAULT, 0.0, beta_default,
        )
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
        let b3x = spec.required_3x_barrier_bps(mid_px).unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS);
        Self::evaluate_entry_signal_elastic(spec, snap, leader_btc_snap, ScaledBps((b3x.raw() * 4) / 9), ScaledBps(b3x.raw() / 8))
    }

    #[inline]
    #[must_use]
    pub fn compute_vacuum_index_l2_l3(open_side: Side, snap: &SymbolLobSnapshot) -> f64 {
        let (mut own_q, mut opp_q, mut first_px, mut last_px, mut pop) = (0_i64, 0_i64, 0_i64, 0_i64, 0_usize);
        for i in 0..4 {
            let (own_l, opp_l) = match open_side { Side::Bid => (snap.bids[i], snap.asks[i]), Side::Ask => (snap.asks[i], snap.bids[i]) };
            if own_l.price.0 > 0 && own_l.qty.0 > 0 { own_q = own_q.saturating_add(own_l.qty.0); }
            if opp_l.price.0 > 0 && opp_l.qty.0 > 0 {
                opp_q = opp_q.saturating_add(opp_l.qty.0);
                if pop == 0 { first_px = opp_l.price.0; }
                last_px = opp_l.price.0;
                pop += 1;
            }
        }
        let gap_mult = if pop >= 2 { (((last_px - first_px).abs().max(1) as f64) / ((pop - 1) as f64)).sqrt().clamp(1.0, 1.50) } else { 1.0 };
        let dir_s = open_side.sign_f64();
        ((own_q.max(1) as f64) / (opp_q.max(1) as f64))
            * gap_mult
            * (1.0 + (snap.factor_ofi.get() * dir_s).max(0.0) + 1.25 * (snap.factor_tfi.get() * dir_s).max(0.0))
    }

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
        let nu_v = ((vol_raw.max(b3x_raw) as f64) / ((b3x_raw.saturating_mul(4)).max(1) as f64)).sqrt().clamp(0.85, 1.15);
        let signed_tfi = snap.factor_tfi.get() * open_side.sign_f64();
        let level_idx = if vac_idx >= (2.15 * nu_v) && signed_tfi >= 0.42 {
            3_usize
        } else if vac_idx >= (1.42 * nu_v) && signed_tfi >= 0.18 {
            2_usize
        } else {
            usize::from(vac_idx >= (1.04 * nu_v))
        };
        let eff_struct = match open_side {
            Side::Bid => structural_l1_floor_price.0.max(quantum_floor_price.0).max(1),
            Side::Ask => if structural_l1_floor_price.0 > 0 { structural_l1_floor_price.0.min(quantum_floor_price.0).max(1) } else { quantum_floor_price.0.max(1) },
        };
        let max_surf_bps = vol_raw.clamp(b3x_raw.saturating_mul(3), b3x_raw.saturating_mul(7));
        let max_offset_t = ((eff_struct.max(quantum_floor_price.0).max(1).saturating_mul(max_surf_bps)) / 1_000_000).max(4);
        let book_px = match open_side {
            Side::Bid => {
                let p = snap.asks[level_idx].price.0;
                let raw_t = if p > 0 { p.max(eff_struct) } else { snap.asks[0].price.0.max(eff_struct) };
                PriceTicks(raw_t.min(eff_struct.saturating_add(max_offset_t)))
            }
            Side::Ask => {
                let p = snap.bids[level_idx].price.0;
                let fb = if snap.bids[0].price.0 > 0 { snap.bids[0].price.0.min(eff_struct) } else { eff_struct };
                let raw_t = if p > 0 { p.min(eff_struct).max(1) } else { fb.max(1) };
                PriceTicks(raw_t.max(eff_struct.saturating_sub(max_offset_t).max(1)))
            }
        };
        (level_idx + 1, book_px, vac_idx)
    }

    #[inline]
    #[must_use]
    pub fn stoikov_microprice_and_skew(side: Side, snap: &SymbolLobSnapshot) -> (f64, f64) {
        let (b0, a0) = (snap.bids[0], snap.asks[0]);
        if b0.price.0 <= 0 || a0.price.0 <= b0.price.0 || (b0.qty.0 <= 0 && a0.qty.0 <= 0) {
            return (0.0, 0.0);
        }
        let dir_s = side.sign_f64();
        let spread_ticks = (a0.price.0 - b0.price.0) as f64;
        let (b_l1, a_l1) = (b0.qty.0.max(0), a0.qty.0.max(0));
        let i_bbo = dir_s * ((b_l1 - a_l1) as f64) / ((b_l1.saturating_add(a_l1)).max(1) as f64);
        let eff_db = if snap.deep_bid_lots_l1_l5.0 > 0 { snap.deep_bid_lots_l1_l5.0 } else { b_l1 };
        let eff_da = if snap.deep_ask_lots_l1_l5.0 > 0 { snap.deep_ask_lots_l1_l5.0 } else { a_l1 };
        let i_deep = dir_s * ((eff_db - eff_da) as f64) / ((eff_db.saturating_add(eff_da)).max(1) as f64);
        let m_skew = (V45_INV_PHI * i_bbo + V45_INV_PHI_SQ * i_deep).clamp(-1.0, 1.0);
        (m_skew, 0.5 * spread_ticks * (-m_skew).max(0.0))
    }

    #[inline]
    #[must_use]
    pub fn epps_btc_lead_lag_beta(slot_idx: usize, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot) -> f64 {
        if slot_idx == 0 { return 0.0; }
        ((snap.realized_vol_5m.raw().max(1_200) as f64) / (btc_leader_snap.realized_vol_5m.raw().max(1_200) as f64)).clamp(0.75, V45_PHI)
    }

    #[inline]
    #[must_use]
    pub fn epps_correlated_lead_lag_beta(
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        tracker: &EppsBtcCorrelationTracker,
    ) -> f64 {
        if slot_idx == 0 { 0.0 } else { tracker.correlated_beta(false, snap.realized_vol_5m, btc_leader_snap.realized_vol_5m) }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EppsBtcCorrelationTracker {
    last_bucket_ts_ns: i64,
    prev_own_mid_ticks: i64,
    prev_btc_mid_ticks: i64,
    var_own_bps2: f64,
    var_btc_bps2: f64,
    cov_own_btc_bps2: f64,
}

impl Default for EppsBtcCorrelationTracker {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl EppsBtcCorrelationTracker {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { last_bucket_ts_ns: 0, prev_own_mid_ticks: 0, prev_btc_mid_ticks: 0, var_own_bps2: 0.0, var_btc_bps2: 0.0, cov_own_btc_bps2: 0.0 }
    }

    #[inline]
    pub fn observe_5s_pair(&mut self, ts_ns: TimestampNs, is_btc_slot: bool, own_mid: PriceTicks, btc_mid: PriceTicks) {
        if own_mid.0 <= 0 || btc_mid.0 <= 0 || ts_ns.0 <= 0 { return; }
        if is_btc_slot || self.last_bucket_ts_ns <= 0 || ts_ns.0 < self.last_bucket_ts_ns || self.prev_own_mid_ticks <= 0 || self.prev_btc_mid_ticks <= 0 {
            self.last_bucket_ts_ns = ts_ns.0;
            self.prev_own_mid_ticks = own_mid.0;
            self.prev_btc_mid_ticks = btc_mid.0;
            return;
        }
        let dt_ns = ts_ns.0 - self.last_bucket_ts_ns;
        if dt_ns < V45_EPPS_BUCKET_NS { return; }
        let alpha = 1.0 - (-(((dt_ns as f64) * 1e-9).clamp(5.0, 60.0)) / V45_EPPS_TAU_SECS).exp();
        let r_own = (((own_mid.0 - self.prev_own_mid_ticks) as f64) * 10_000.0 / (self.prev_own_mid_ticks as f64)).clamp(-500.0, 500.0);
        let r_btc = (((btc_mid.0 - self.prev_btc_mid_ticks) as f64) * 10_000.0 / (self.prev_btc_mid_ticks as f64)).clamp(-500.0, 500.0);
        self.var_own_bps2 = (1.0 - alpha) * self.var_own_bps2 + alpha * (r_own * r_own);
        self.var_btc_bps2 = (1.0 - alpha) * self.var_btc_bps2 + alpha * (r_btc * r_btc);
        self.cov_own_btc_bps2 = (1.0 - alpha) * self.cov_own_btc_bps2 + alpha * (r_own * r_btc);
        self.last_bucket_ts_ns = ts_ns.0;
        self.prev_own_mid_ticks = own_mid.0;
        self.prev_btc_mid_ticks = btc_mid.0;
    }

    #[inline]
    #[must_use]
    pub fn correlation(&self, is_btc_slot: bool) -> f64 {
        if is_btc_slot { return 1.0; }
        let num = self.cov_own_btc_bps2 + V45_INV_PHI * V45_EPPS_PRIOR_VAR_BPS2;
        let denom = ((self.var_own_bps2 + V45_EPPS_PRIOR_VAR_BPS2).max(1e-6) * (self.var_btc_bps2 + V45_EPPS_PRIOR_VAR_BPS2).max(1e-6)).sqrt();
        (num / denom).clamp(-1.0, 1.0)
    }

    #[inline]
    #[must_use]
    pub fn correlated_beta(&self, is_btc_slot: bool, own_vol: ScaledBps, btc_vol: ScaledBps) -> f64 {
        if is_btc_slot { 1.0 } else {
            (self.correlation(false) * ((own_vol.raw().max(100) as f64) / (btc_vol.raw().max(100) as f64))).clamp(-V45_PHI, V45_PHI)
        }
    }

    #[inline]
    #[must_use]
    pub fn correlated_beta_x1000(&self, is_btc_slot: bool, own_vol: ScaledBps, btc_vol: ScaledBps) -> i64 {
        (self.correlated_beta(is_btc_slot, own_vol, btc_vol) * 1_000.0).round().clamp(-1_618.0, 1_618.0) as i64
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EventClockAlphaModulator {
    last_real_ts_ns: i64,
    virtual_clock_ns: i64,
    fast_ewma_events: f64,
    eq_ewma_events: f64,
    spoof_density_ewma: f64,
}


impl Default for EventClockAlphaModulator {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl EventClockAlphaModulator {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last_real_ts_ns: 0, virtual_clock_ns: 0,
            fast_ewma_events: V45_SEED_RATE_HZ * V45_FAST_RATE_TAU_SECS,
            eq_ewma_events: V45_SEED_RATE_HZ * V45_EQ_RATE_TAU_SECS,
            spoof_density_ewma: 0.0,
        }
    }
    #[inline] #[must_use] pub const fn spoofing_density(&self) -> f64 { self.spoof_density_ewma }
    #[inline] #[must_use]
    pub fn event_clock_dilation(&self) -> f64 {
        (((self.fast_ewma_events / V45_FAST_RATE_TAU_SECS).max(0.05)) / ((self.eq_ewma_events / V45_EQ_RATE_TAU_SECS).max(1.0))).sqrt().clamp(V45_INV_PHI, V45_PHI)
    }
    #[inline] #[must_use]
    pub fn adaptive_factor_weights(&self) -> (f64, f64, f64) {
        OrthogonalFactorState::adaptive_weights_from_spoof_density(self.spoof_density_ewma)
    }
    #[inline]
    pub fn step_event_clock_and_spoof(&mut self, real_ts_ns: TimestampNs, ofi: BoundedScore, tfi: BoundedScore) -> TimestampNs {
        if real_ts_ns.0 <= 0 { return real_ts_ns; }
        if self.last_real_ts_ns <= 0 || real_ts_ns.0 < self.last_real_ts_ns {
            self.last_real_ts_ns = real_ts_ns.0;
            self.virtual_clock_ns = real_ts_ns.0;
            return TimestampNs(self.virtual_clock_ns);
        }
        let dt_ns = real_ts_ns.0 - self.last_real_ts_ns;
        if dt_ns == 0 { return TimestampNs(self.virtual_clock_ns); }
        let dt_s = ((dt_ns as f64) * 1e-9).clamp(1e-6, 30.0);
        self.fast_ewma_events = self.fast_ewma_events * (-dt_s / V45_FAST_RATE_TAU_SECS).exp() + 1.0;
        self.eq_ewma_events = self.eq_ewma_events * (-dt_s / V45_EQ_RATE_TAU_SECS).exp() + 1.0;
        let i_spoof = if ofi.get().abs() >= 0.35 && (ofi.get() * tfi.get()) < -0.05 { 1.0 } else { 0.0 };
        let alpha_sp = 1.0 - (-dt_s / V45_SPOOF_TAU_SECS).exp();
        self.spoof_density_ewma = ((1.0 - alpha_sp) * self.spoof_density_ewma + alpha_sp * i_spoof).clamp(0.0, 1.0);
        self.virtual_clock_ns = self.virtual_clock_ns.saturating_add((((dt_ns as f64) * self.event_clock_dilation()).round() as i64).max(1));
        self.last_real_ts_ns = real_ts_ns.0;
        TimestampNs(self.virtual_clock_ns)
    }
    #[inline] #[must_use]
    pub fn adaptive_composite_snr(&self, ofi: BoundedScore, tfi: BoundedScore, geom: BoundedScore) -> BoundedScore {
        let (w_o, w_t, w_g) = self.adaptive_factor_weights();
        BoundedScore::new_clamped(w_o * ofi.get() + w_t * tfi.get() + w_g * geom.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_lvdbs_stoikov_parkinson_and_event_clock_invariants() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_500);
        for (i, &(bp, ap)) in [(8_202_i64, 8_203_i64), (8_201, 8_205), (8_200, 8_208), (8_199, 8_211)].iter().enumerate() {
            snap.bids[i] = BookLevel { price: PriceTicks(bp), qty: LotQty(300) };
            snap.asks[i] = BookLevel { price: PriceTicks(ap), qty: LotQty(250) };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.60);
        snap.factor_tfi = BoundedScore::new_clamped(0.72);
        let (lvl4, px4, _) = OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(Side::Bid, &snap, PriceTicks(8_202), PriceTicks(8_202));
        assert_eq!((lvl4, px4), (4, PriceTicks(8_211)));
        let mut clock = EventClockAlphaModulator::new();
        let mut ts = 1_794_700_000_000_000_000_i64;
        for _ in 0..300 { ts += 5_000_000; let _ = clock.step_event_clock_and_spoof(TimestampNs(ts), snap.factor_ofi, snap.factor_tfi); }
        assert!((clock.event_clock_dilation() - V45_PHI).abs() < 1e-6);
    }

    #[test]
    fn test_v46_island1a_spoof_density_dynamic_3x_vip_fees_and_signed_epps() {
        let mut st = OrthogonalFactorState::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[7].symbol_id;
        snap.realized_vol_5m = ScaledBps(2_272);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        for i in 0..5 {
            snap.bids[i] = BookLevel { price: PriceTicks(15_586 - i as i64), qty: LotQty(400) };
            snap.asks[i] = BookLevel { price: PriceTicks(15_587 + i as i64), qty: LotQty(400) };
        }
        st.on_l2_book_update(&mut snap);
        st.on_trade_print(Side::Ask, LotQty(900), &mut snap);
        for step in 1..=25_i64 {
            snap.ts_local_ns = TimestampNs(1_000_000_000 + step * 1_000_000_000);
            snap.bids[0].qty = LotQty(400 + step * 500);
            st.on_l2_book_update(&mut snap);
        }
        assert!(st.spoofing_density() > 0.65);
        let (w_o, w_t, w_g) = OrthogonalFactorState::adaptive_weights_from_spoof_density(st.spoofing_density());
        assert!((w_o + w_t + w_g - 1.0).abs() < 1e-12 && w_g < 0.09);

        // VIP rebate vs high Taker toxicity on marginal excursion + inverse Epps beta hedge
        snap.factor_ofi = BoundedScore::new_clamped(0.26);
        snap.factor_tfi = BoundedScore::new_clamped(0.25);
        snap.factor_geom = BoundedScore::new_clamped(0.95);
        let mut btc = snap;
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.factor_ofi = BoundedScore::new_clamped(-0.75);
        btc.factor_tfi = BoundedScore::new_clamped(-0.80);
        btc.factor_geom = BoundedScore::new_clamped(-0.50);
        // Spoofed GEOM=0.95 blocked when spoof_density=0.85, and toxic taker_prob_pm=350 blocks low vol
        assert_eq!(
            OrthogonalFactorState::evaluate_entry_signal_v46(
                APPROVED_SPECS[7], &snap, &btc, ScaledBps(680), ScaledBps(150),
                50, FeeSchedule::VIP0_DEFAULT, 0.85, 1.0,
            ),
            SignalVerdict::NeutralNoAction
        );
        snap.factor_ofi = BoundedScore::new_clamped(0.52);
        snap.factor_tfi = BoundedScore::new_clamped(0.58);
        snap.factor_geom = BoundedScore::new_clamped(0.30);
        let mut inv_trk = EppsBtcCorrelationTracker::new();
        let mut t_e = 10_000_000_000_i64;
        for k in 0..=36_i64 {
            let up = (k % 2) == 1;
            inv_trk.observe_5s_pair(TimestampNs(t_e), false, PriceTicks(if up { 15_560 } else { 15_586 }), PriceTicks(if up { 680_700 } else { 680_000 }));
            t_e += 5_000_000_000;
        }
        let inv_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(7, &snap, &btc, &inv_trk);
        assert!(inv_beta < -0.50);
        let vip_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        assert!(matches!(
            OrthogonalFactorState::evaluate_entry_signal_v46(
                APPROVED_SPECS[7], &snap, &btc, ScaledBps(680), ScaledBps(150),
                35, vip_rebate, 0.0, inv_beta,
            ),
            SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
        ));
    }
}
