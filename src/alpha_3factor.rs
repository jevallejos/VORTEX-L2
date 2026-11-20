//! Orthogonal 3-Factor Alpha, Anti-Spoof GEOM Weights, Dynamic 3x Barrier, Epps 5s & LVDBS (V4.8).
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

pub const PHI: f64 = 1.618_033_988_749_895;
pub const PHI_SQ: f64 = 2.618_033_988_749_895;
pub const PHI_CUBE: f64 = 4.236_067_977_499_79;
pub const PHI_FOURTH: f64 = 6.854_101_966_249_685;
pub const INV_PHI: f64 = 0.618_033_988_749_895;
pub const INV_PHI_SQ: f64 = 0.381_966_011_250_105;
pub const INV_PHI_CUBE: f64 = 0.236_067_977_499_79;
pub const INV_PHI_FOURTH: f64 = 0.145_898_033_750_315;
pub const INV_PHI_FIFTH: f64 = 0.090_169_943_749_47;
pub const ONE_MINUS_INV_PHI_FOURTH: f64 = 0.854_101_966_249_685;

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const BASE_VOL_WIN_NS: i64 = VOL_5M_HORIZON_NS / 60;
const GAUSSIAN_MAD_TO_SIGMA: f64 = 1.253_314_14;
const PARKINSON_HL_TO_SIGMA: f64 = 0.600_561_20;
const V45_PHI: f64 = PHI;
const V45_INV_PHI: f64 = INV_PHI;
const V45_INV_PHI_SQ: f64 = INV_PHI_SQ;
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
    learned_l1_depth_lots: f64,
    learned_trade_lots: f64,
    learned_flow_lots: f64,
    fast_ewma_events: f64,
    eq_ewma_events: f64,
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
            learned_l1_depth_lots: 0.0,
            learned_trade_lots: 0.0,
            learned_flow_lots: 0.0,
            fast_ewma_events: V45_SEED_RATE_HZ * V45_FAST_RATE_TAU_SECS,
            eq_ewma_events: V45_SEED_RATE_HZ * V45_EQ_RATE_TAU_SECS,
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
    #[must_use]
    pub fn event_clock_dilation(&self) -> f64 {
        (((self.fast_ewma_events / V45_FAST_RATE_TAU_SECS).max(0.05))
            / ((self.eq_ewma_events / V45_EQ_RATE_TAU_SECS).max(1.0)))
        .sqrt()
        .clamp(V45_INV_PHI, V45_PHI)
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
        self.learned_l1_depth_lots = 0.0;
        self.fast_ewma_events = V45_SEED_RATE_HZ * V45_FAST_RATE_TAU_SECS;
        self.eq_ewma_events = V45_SEED_RATE_HZ * V45_EQ_RATE_TAU_SECS;
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
    #[must_use]
    pub fn tfi_endogenous_prior_lots(&self, snap: &SymbolLobSnapshot, floor_lots: f64) -> f64 {
        if snap.bids[0].qty.0 <= 0 && snap.asks[0].qty.0 <= 0 {
            return 1e-6;
        }
        let q_ref = self.learned_trade_lots.max(PHI * floor_lots).max(PHI);
        let kappa_spatial = INV_PHI * (self.learned_l1_depth_lots.max(0.0) * q_ref).sqrt();
        let kappa_flow = INV_PHI_SQ * self.learned_flow_lots.max(0.0) + INV_PHI * q_ref;
        kappa_spatial.max(kappa_flow).max(PHI * floor_lots) * (1.0 + INV_PHI * self.spoof_density_ewma)
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
            let (b0, a0) = (snap.bids[0].price.0, snap.asks[0].price.0);
            if b0 > 0 && a0 > 0 {
                self.learned_l1_depth_lots =
                    (snap.bids[0].qty.0.min(snap.asks[0].qty.0).max(0) as f64).max(init_lots);
                if self.learned_trade_lots <= 0.0 {
                    self.learned_trade_lots = init_lots;
                }
                if self.learned_flow_lots <= 0.0 {
                    self.learned_flow_lots = init_lots;
                }
            }
            self.last_l2_ts_ns = snap.ts_local_ns.0;
            if self.last_tfi_ts_ns <= 0 {
                self.last_tfi_ts_ns = snap.ts_local_ns.0;
                self.tfi_buy_accum = init_lots;
                self.tfi_sell_accum = init_lots;
            } else {
                self.decay_tfi_to_clock(snap.ts_local_ns.0, floor_lots);
                let tfi_prior = self.tfi_endogenous_prior_lots(snap, floor_lots);
                self.last_tfi = BoundedScore::new_clamped(
                    (self.tfi_buy_accum - self.tfi_sell_accum)
                        / (self.tfi_buy_accum + self.tfi_sell_accum + tfi_prior),
                );
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
        if dt_s > 0.0 {
            let thin_bbo = snap.bids[0].qty.0.min(snap.asks[0].qty.0).max(0) as f64;
            let tau_l1_s = V45_EQ_RATE_TAU_SECS * (1.0 + PHI * self.spoof_density_ewma);
            let alpha_l1 = 1.0 - (-dt_s / tau_l1_s).exp();
            self.learned_l1_depth_lots =
                (1.0 - alpha_l1) * self.learned_l1_depth_lots + alpha_l1 * thin_bbo;
        }
        self.decay_tfi_to_clock(ts_now, floor_lots);
        if dt_s > 0.0 {
            let active_vol = self.tfi_buy_accum + self.tfi_sell_accum;
            let alpha_fl = 1.0 - (-dt_s / V45_EQ_RATE_TAU_SECS).exp();
            self.learned_flow_lots += alpha_fl * (active_vol - self.learned_flow_lots);
        }
        let tfi_prior = self.tfi_endogenous_prior_lots(snap, floor_lots);
        self.last_tfi = BoundedScore::new_clamped(
            (self.tfi_buy_accum - self.tfi_sell_accum)
                / (self.tfi_buy_accum + self.tfi_sell_accum + tfi_prior),
        );
        self.last_geom = Self::compute_book_geometry(snap);
        if dt_s > 0.0 {
            self.fast_ewma_events =
                self.fast_ewma_events * (-dt_s / V45_FAST_RATE_TAU_SECS).exp() + 1.0;
            self.eq_ewma_events =
                self.eq_ewma_events * (-dt_s / V45_EQ_RATE_TAU_SECS).exp() + 1.0;
            let ofi_f = self.last_ofi.get();
            let tfi_f = self.last_tfi.get();
            let i_spoof = if ofi_f.abs() >= 0.35 && (ofi_f * tfi_f) < -0.05 {
                1.0
            } else if ofi_f.abs() > 0.55 && tfi_f.abs() < 0.08 {
                if (ofi_f * tfi_f) < 0.0 {
                    1.0
                } else {
                    let eta_lat =
                        compute_tick_lattice_granularity_eta(snap, FeeSchedule::VIP0_DEFAULT);
                    let kappa_evt = self.event_clock_dilation();
                    evaluate_silent_tape_spoof_weight_c1(eta_lat, kappa_evt)
                }
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
        let (init_lots, floor_lots) = Self::flow_prior_bounds(snap);
        if q > 0.0 {
            let alpha_tr = INV_PHI_FOURTH;
            let base_tr = if self.learned_trade_lots > 0.0 {
                self.learned_trade_lots
            } else {
                init_lots
            };
            self.learned_trade_lots = (1.0 - alpha_tr) * base_tr + alpha_tr * q;
        }
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
        let tfi_prior = self.tfi_endogenous_prior_lots(snap, floor_lots);
        self.last_tfi = BoundedScore::new_clamped(
            (self.tfi_buy_accum - self.tfi_sell_accum)
                / (self.tfi_buy_accum + self.tfi_sell_accum + tfi_prior),
        );
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
    pub fn adaptive_snr_from_snapshot_with_fees(
        snap: &SymbolLobSnapshot,
        spoof_density: f64,
        fees: FeeSchedule,
    ) -> BoundedScore {
        let (mut w_ofi, mut w_tfi, w_geom) = Self::adaptive_weights_from_spoof_density(spoof_density);
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        if (ofi * tfi) >= 0.0 {
            let eta_lat = compute_tick_lattice_granularity_eta(snap, fees);
            let s_lat = crate::wave_elliott::hermite_smoothstep_c1(eta_lat, INV_PHI_SQ, PHI);
            if s_lat > 0.0 {
                let s_ofi = crate::wave_elliott::hermite_smoothstep_c1(
                    ofi.abs(),
                    INV_PHI,
                    1.0 - INV_PHI_CUBE,
                );
                let s_pause =
                    1.0 - crate::wave_elliott::hermite_smoothstep_c1(tfi.abs(), 0.0, INV_PHI_CUBE);
                let sp_c = if spoof_density.is_finite() {
                    spoof_density.clamp(0.0, 1.0)
                } else {
                    1.0
                };
                let s_clean =
                    1.0 - crate::wave_elliott::hermite_smoothstep_c1(sp_c, INV_PHI_FOURTH, INV_PHI);
                let dw_lat = INV_PHI * w_tfi * s_lat * s_ofi * s_pause * s_clean;
                w_ofi += dw_lat;
                w_tfi -= dw_lat;
            }
        }
        BoundedScore::new_clamped(w_ofi * ofi + w_tfi * tfi + w_geom * snap.factor_geom.get())
    }

    #[inline]
    #[must_use]
    pub fn adaptive_snr_from_snapshot(snap: &SymbolLobSnapshot, spoof_density: f64) -> BoundedScore {
        Self::adaptive_snr_from_snapshot_with_fees(snap, spoof_density, FeeSchedule::VIP0_DEFAULT)
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
        let is_follower = snap.symbol_id != leader_btc_snap.symbol_id;
        let implied_corr = if is_follower {
            let own_v = snap.realized_vol_5m.raw().max(100) as f64;
            let btc_v = leader_btc_snap.realized_vol_5m.raw().max(100) as f64;
            (epps_signed_beta * (btc_v / own_v)).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        Self::evaluate_entry_signal_v54(
            spec,
            snap,
            leader_btc_snap,
            learned_mfe_bps,
            learned_dir_edge_bps,
            taker_prob_pm,
            fees,
            spoof_density,
            epps_signed_beta,
            implied_corr,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_entry_signal_v54(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
    ) -> SignalVerdict {
        Self::evaluate_entry_signal_v542(
            spec,
            snap,
            leader_btc_snap,
            learned_mfe_bps,
            learned_dir_edge_bps,
            taker_prob_pm,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            None,
            1.0,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_entry_signal_v542(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
    ) -> SignalVerdict {
        Self::evaluate_entry_signal_v55(
            spec,
            snap,
            leader_btc_snap,
            learned_mfe_bps,
            learned_dir_edge_bps,
            taker_prob_pm,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            psi_health,
            0.0,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_entry_signal_v55(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
        lead_lag_ignition_bonus: f64,
    ) -> SignalVerdict {
        Self::evaluate_entry_signal_v553(
            spec,
            snap,
            leader_btc_snap,
            learned_mfe_bps,
            learned_dir_edge_bps,
            taker_prob_pm,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            psi_health,
            lead_lag_ignition_bonus,
            PHI,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_entry_signal_v553(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
        lead_lag_ignition_bonus: f64,
        kappa_evt: f64,
    ) -> SignalVerdict {
        let sigma_est = (learned_mfe_bps.raw().max(200) / 2)
            .max(200)
            .saturating_add(fees.round_trip_maker_exit_bps().raw().max(0))
            .max(1);
        let implied_ir_x100 = (learned_dir_edge_bps.raw().saturating_mul(100)) / sigma_est;
        let tau_mkt_s =
            crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(snap.realized_vol_5m);
        Self::evaluate_entry_signal_v554(
            spec,
            snap,
            leader_btc_snap,
            learned_mfe_bps,
            learned_dir_edge_bps,
            taker_prob_pm,
            implied_ir_x100.clamp(-60, 120),
            tau_mkt_s,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            psi_health,
            lead_lag_ignition_bonus,
            kappa_evt,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_entry_signal_v554(
        spec: PairSpec,
        snap: &SymbolLobSnapshot,
        leader_btc_snap: &SymbolLobSnapshot,
        learned_mfe_bps: ScaledBps,
        learned_dir_edge_bps: ScaledBps,
        taker_prob_pm: i64,
        learned_ir_x100: i64,
        tau_mkt_s: f64,
        fees: FeeSchedule,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
        lead_lag_ignition_bonus: f64,
        kappa_evt: f64,
    ) -> SignalVerdict {
        let Some((best_bid, best_ask, spread_ticks)) = snap.bbo_and_spread_ticks() else {
            return SignalVerdict::NeutralNoAction;
        };
        let mid_ticks = PriceTicks(i64::midpoint(best_bid.price.0, best_ask.price.0));
        let Ok(raw_tick_bps) = spec.tick_to_price_scaled_bps(mid_ticks) else {
            return SignalVerdict::NeutralNoAction;
        };
        let fric_barrier = spec.learned_friction_barrier_bps_c1(
            mid_ticks,
            taker_prob_pm,
            learned_ir_x100,
            psi_health,
            fees,
        );
        if spread_ticks > 1 {
            let spread_scaled_bps = (spread_ticks.saturating_mul(1_000_000)) / mid_ticks.0.max(1);
            let rt_cost = spec
                .expected_round_trip_cost_bps_dynamic(mid_ticks, taker_prob_pm, fees)
                .raw()
                .max(50);
            let max_elastic_spread_bps = (snap.realized_vol_5m.raw() / 24).clamp(rt_cost / 2, rt_cost);
            if spread_scaled_bps > max_elastic_spread_bps {
                return SignalVerdict::NeutralNoAction;
            }
        }
        if !passes_intrinsic_diffusive_reachability_c1(snap.realized_vol_5m, tau_mkt_s, fees) {
            return SignalVerdict::NeutralNoAction;
        }
        let c_mkr_rt_sbps = fees.round_trip_maker_exit_bps().raw().max(1) as f64;
        let eta_lat = (raw_tick_bps.raw().max(0) as f64) / c_mkr_rt_sbps;
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        let geom = snap.factor_geom.get();
        let composite = Self::adaptive_snr_from_snapshot_with_fees(snap, spoof_density, fees).get();
        let cand_side = if composite > 0.0 {
            Side::Bid
        } else if composite < 0.0 {
            Side::Ask
        } else if ofi >= 0.0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let s_cand = cand_side.sign_f64();
        let ofi_al = ofi * s_cand;
        let tfi_al = tfi * s_cand;
        let geom_al = geom * s_cand;
        let is_follower = snap.symbol_id != leader_btc_snap.symbol_id;
        let eff_epps_corr = if is_follower { epps_corr } else { 0.0 };
        let btc_w3_aligned = compute_btc_leader_aligned_score(
            cand_side,
            snap,
            leader_btc_snap,
            eff_epps_corr,
            btc_lead_aligned_override,
        );
        let s_preprint =
            compute_preprint_alignment_bridge_c1(ofi_al, geom_al, btc_w3_aligned, spoof_density);
        if (ofi * tfi) < 0.0 {
            if is_opposing_or_silent_spoof_veto_c1(ofi, tfi, eta_lat, kappa_evt) {
                return SignalVerdict::SpoofingVeto;
            }
        } else if ofi.abs() > 0.55
            && tfi.abs() < 0.08
            && (1.0 - s_preprint) * evaluate_silent_tape_spoof_weight_c1(eta_lat, kappa_evt)
                >= INV_PHI
        {
            return SignalVerdict::SpoofingVeto;
        }
        let bonus = if lead_lag_ignition_bonus.is_finite() {
            lead_lag_ignition_bonus.clamp(0.0, INV_PHI_CUBE)
        } else {
            0.0
        };
        let eq_vol_anchor = (fric_barrier.raw().saturating_mul(4)).max(1) as f64;
        let nu_sigma = ((snap.realized_vol_5m.raw().max(fric_barrier.raw()) as f64) / eq_vol_anchor)
            .sqrt()
            .clamp(0.82, 1.25);
        let edge_relief = ((learned_dir_edge_bps.raw().clamp(0, 450) as f64) / 10_000.0).min(0.04);
        let elastic_snr_floor = (0.40 * nu_sigma - edge_relief).clamp(0.34, 0.48);
        let sp_c = if spoof_density.is_finite() {
            spoof_density.clamp(0.0, 1.0)
        } else {
            1.0
        };
        let preprint_min = -0.02 * (1.0 - sp_c);
        let s_tfi_pause =
            1.0 - crate::wave_elliott::hermite_smoothstep_c1(tfi_al.max(0.0), 0.0, INV_PHI_CUBE);
        let preprint_snr_lift = if s_preprint > 0.0 && tfi_al >= preprint_min {
            INV_PHI_CUBE * s_preprint * s_tfi_pause * (ofi_al - tfi_al).max(0.0)
        } else {
            0.0
        };
        let bridged_composite_abs = (composite.abs() + preprint_snr_lift).min(1.0);
        if bridged_composite_abs + bonus < elastic_snr_floor {
            return SignalVerdict::NeutralNoAction;
        }
        let min_ofi_dir = 0.15 * (1.0 - bonus);
        let min_tfi_dir = 0.12 * (1.0 - bonus);
        let s_lat = crate::wave_elliott::hermite_smoothstep_c1(eta_lat, INV_PHI_SQ, PHI);
        let s_ofi_lat =
            crate::wave_elliott::hermite_smoothstep_c1(ofi_al, INV_PHI, 1.0 - INV_PHI_CUBE);
        let relaxed_tfi_floor = min_tfi_dir * (1.0 - s_lat * s_ofi_lat);
        let bridged_tfi_floor =
            relaxed_tfi_floor * (1.0 - s_preprint) + preprint_min * s_preprint;
        let tfi_ok = if s_preprint > 0.0 {
            tfi_al >= preprint_min && tfi_al >= bridged_tfi_floor
        } else if s_lat * s_ofi_lat > 0.0 {
            tfi_al >= 0.0 && tfi_al >= relaxed_tfi_floor
        } else {
            tfi_al > min_tfi_dir
        };
        let s_geom_relax = s_lat.max(s_preprint);
        let min_geom_al = if ofi_al >= INV_PHI {
            -INV_PHI_FOURTH * s_geom_relax
        } else {
            -INV_PHI_FOURTH
                * s_geom_relax
                * crate::wave_elliott::hermite_smoothstep_c1(ofi_al, INV_PHI_SQ, INV_PHI)
        };
        if ofi_al <= min_ofi_dir || !tfi_ok || geom_al < min_geom_al {
            return SignalVerdict::NeutralNoAction;
        }
        let side = cand_side;
        let dir_sign = side.sign_f64();
        let corr_sign = if eff_epps_corr < 0.0 { -1.0 } else { 1.0 };
        let inst_btc_aligned = if is_follower {
            corr_sign * Self::snr_composite_score(leader_btc_snap) * dir_sign
        } else {
            0.0
        };
        let btc_sync_aligned = if is_follower {
            btc_w3_aligned
        } else {
            0.0
        };
        if is_opposing_impulse_wave3_entry_hazard_v554(
            side,
            snap,
            btc_w3_aligned,
            psi_health,
            0.0,
            tau_mkt_s,
            0,
            taker_prob_pm,
            fees,
        ) {
            return SignalVerdict::NeutralNoAction;
        }
        if !passes_elastic_anti_spoof_ofi_guard(side, snap, spoof_density, taker_prob_pm, 0, fees) {
            return SignalVerdict::SpoofingVeto;
        }
        let beta_c = epps_signed_beta.clamp(-V45_PHI, V45_PHI);
        let aligned_leader = beta_c * (Self::snr_composite_score(leader_btc_snap) * dir_sign);
        let beta_sign = if beta_c >= 0.0 { 1.0 } else { -1.0 };
        let leader_tfi_al = leader_btc_snap.factor_tfi.get() * dir_sign * beta_sign;
        let lead_lag_adj_bps = if !is_follower || beta_c.abs() < 1e-6 {
            0
        } else if aligned_leader >= 0.55 && leader_tfi_al >= 0.55 {
            let bonus_cap = fric_barrier.raw() / 9;
            ((aligned_leader * (bonus_cap as f64)).round() as i64).clamp(0, bonus_cap)
        } else if aligned_leader <= -0.35 {
            let drag_cap = fric_barrier.raw() / 6;
            -((((-aligned_leader) - 0.35) * (drag_cap as f64)).round() as i64).clamp(0, drag_cap)
        } else {
            0
        };
        let w_entry = snap.wave_observables();
        let (al_ob_pm, op_ob_pm) = match side {
            Side::Bid => (snap.ob_bull_efficacy_pm, snap.ob_bear_efficacy_pm),
            Side::Ask => (snap.ob_bear_efficacy_pm, snap.ob_bull_efficacy_pm),
        };
        if !passes_corrective_abc_snapshot_guard_with_health_c1(
            side,
            snap,
            eff_epps_corr,
            btc_sync_aligned,
            psi_health,
        ) {
            return SignalVerdict::NeutralNoAction;
        }
        let s_iceberg = compute_passive_iceberg_absorption_penalty_c1(
            ofi * dir_sign,
            tfi * dir_sign,
            inst_btc_aligned,
        );
        let eff_composite_abs = (bridged_composite_abs * (1.0 - INV_PHI * s_iceberg) + bonus).min(1.0);
        if eff_composite_abs < elastic_snr_floor {
            return SignalVerdict::NeutralNoAction;
        }
        let raw_vol = snap.realized_vol_5m.raw() as f64;
        let learned_span = learned_mfe_bps.raw().max(0).saturating_add(learned_dir_edge_bps.raw().max(0));
        let eq_horizon = fric_barrier.raw().saturating_mul(3).saturating_add(learned_span.saturating_mul(2)) as f64;
        let eff_vol = if matches!(w_entry.regime, crate::wave_elliott::WaveRegime::CorrectiveAbc) && raw_vol < eq_horizon {
            let ob_tot = (al_ob_pm.max(382) + op_ob_pm.max(382)) as f64;
            let rho_ob_v = if al_ob_pm > 0 || op_ob_pm > 0 { (al_ob_pm.max(382) as f64) / ob_tot } else { 0.50 };
            let s_opp_w = compute_s_opp_ob_wall_with_target_c1(
                side,
                snap,
                learned_mfe_bps.raw(),
                taker_prob_pm,
                fees,
            );
            let chi_threat = compute_corrective_abc_structural_threat_c1(
                s_opp_w,
                dir_sign * w_entry.macro_phase.get(),
            );
            let delta_h_clear = compute_corrective_abc_clear_lift_c1(
                psi_health,
                (ofi * dir_sign).min(tfi * dir_sign),
                chi_threat,
                s_iceberg,
            );
            let delta_h_tick = compute_corrective_abc_tick_hurst_lift_c1(
                psi_health,
                ofi * dir_sign,
                tfi * dir_sign,
                btc_sync_aligned,
                eff_epps_corr,
                chi_threat,
                s_iceberg,
            );
            let s_sync = ((compute_corrective_abc_epps_sync_lift_c1(
                eff_epps_corr,
                btc_sync_aligned,
                ofi * dir_sign,
                tfi * dir_sign,
                s_opp_w,
            ) + delta_h_clear
                + delta_h_tick)
                / V45_INV_PHI_SQ)
                .clamp(0.0, 1.0);
            let s_ob = crate::wave_elliott::hermite_smoothstep_c1(rho_ob_v, 0.50, V45_INV_PHI).max(s_sync);
            let h_drag = chi_threat
                * crate::wave_elliott::hermite_smoothstep_c1(w_entry.hurst_exponent(), 0.50, V45_INV_PHI)
                * (1.0 - V45_INV_PHI * s_ob);
            let geom_mean = (raw_vol * eq_horizon).sqrt();
            geom_mean - h_drag * (geom_mean - raw_vol).max(0.0)
        } else if raw_vol < eq_horizon {
            (raw_vol * eq_horizon).sqrt()
        } else {
            raw_vol
        };
        let flow_boost = (0.54 + 0.12 * tfi.abs() + 0.08 * geom.abs()) * (1.0 - INV_PHI_SQ * s_iceberg);
        let expected_excursion = ScaledBps(((eff_composite_abs * eff_vol * flow_boost).round() as i64).saturating_add(lead_lag_adj_bps));
        if expected_excursion.raw() >= fric_barrier.raw() {
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
    pub fn seed_correlation_permille(&mut self, is_btc_slot: bool, corr_pm: i16) {
        if is_btc_slot {
            self.var_own_bps2 = PHI_CUBE * V45_EPPS_PRIOR_VAR_BPS2;
            self.var_btc_bps2 = PHI_CUBE * V45_EPPS_PRIOR_VAR_BPS2;
            self.cov_own_btc_bps2 = PHI_CUBE * V45_EPPS_PRIOR_VAR_BPS2;
            return;
        }
        let rho = (f64::from(corr_pm) * 1e-3).clamp(-0.98, 0.98);
        let v_seed = PHI_CUBE * V45_EPPS_PRIOR_VAR_BPS2;
        self.var_own_bps2 = v_seed;
        self.var_btc_bps2 = v_seed;
        self.cov_own_btc_bps2 = rho * (v_seed + V45_EPPS_PRIOR_VAR_BPS2)
            - V45_INV_PHI * V45_EPPS_PRIOR_VAR_BPS2;
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
        let ofi_f = ofi.get();
        let tfi_f = tfi.get();
        let i_spoof = if (ofi_f.abs() >= 0.35 && (ofi_f * tfi_f) < -0.05) || (ofi_f.abs() > 0.55 && tfi_f.abs() < 0.08) { 1.0 } else { 0.0 };
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

impl OrthogonalFactorState {
    /// V4.8 P2-3: Elastic Parkinson 5m volatility window coupled to live VIP `FeeSchedule` & `taker_prob_pm`.
    #[inline]
    pub fn observe_mid_and_step_elastic_vol_with_fees(
        &mut self,
        spec: PairSpec,
        obs_px: PriceTicks,
        ts_ns: i64,
        current_vol: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> ScaledBps {
        if obs_px.0 <= 0 || ts_ns <= 0 {
            return current_vol;
        }
        if self.vol_anchor_px.0 <= 0 || self.vol_window_start_ts_ns <= 0 {
            self.vol_anchor_px = obs_px;
            self.vol_low_px = obs_px;
            self.vol_high_px = obs_px;
            self.vol_window_start_ts_ns = ts_ns;
            return current_vol;
        }
        if obs_px.0 < self.vol_low_px.0 {
            self.vol_low_px = obs_px;
        }
        if obs_px.0 > self.vol_high_px.0 {
            self.vol_high_px = obs_px;
        }
        let dt_ns = ts_ns.saturating_sub(self.vol_window_start_ts_ns);
        let min_win_ns = (BASE_VOL_WIN_NS * 3) / 10;
        if dt_ns < min_win_ns {
            return current_vol;
        }
        let b3x = spec
            .three_x_friction_barrier_bps_dynamic(self.vol_anchor_px, taker_prob_pm, fees)
            .raw()
            .max(1);
        let raw_win_ns = ((BASE_VOL_WIN_NS as i128 * (b3x.saturating_mul(4) as i128))
            / (current_vol.raw().max(b3x) as i128)) as i64;
        if dt_ns < raw_win_ns.clamp(min_win_ns, BASE_VOL_WIN_NS * 2) {
            return current_vol;
        }
        let updated = Self::update_realized_vol_5m_parkinson(
            current_vol,
            self.vol_anchor_px,
            self.vol_low_px,
            self.vol_high_px,
            obs_px,
            dt_ns,
        );
        self.vol_anchor_px = obs_px;
        self.vol_low_px = obs_px;
        self.vol_high_px = obs_px;
        self.vol_window_start_ts_ns = ts_ns;
        updated
    }
}

#[cfg(test)]
mod v48_parkinson_vip_tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_v48_elastic_parkinson_window_scales_with_live_vip_fee_schedule() {
        let avax = APPROVED_SPECS[3];
        let mut st_vip0 = OrthogonalFactorState::new();
        let mut st_reb = OrthogonalFactorState::new();
        let vip_rebate = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        let seed_vol = ScaledBps(2_500);

        let _ = st_vip0.observe_mid_and_step_elastic_vol_with_fees(
            avax, PriceTicks(8_200), 1_000_000_000, seed_vol, 50, FeeSchedule::VIP0_DEFAULT,
        );
        let _ = st_reb.observe_mid_and_step_elastic_vol_with_fees(
            avax, PriceTicks(8_200), 1_000_000_000, seed_vol, 35, vip_rebate,
        );

        let v0_2s = st_vip0.observe_mid_and_step_elastic_vol_with_fees(
            avax, PriceTicks(8_240), 3_000_000_000, seed_vol, 50, FeeSchedule::VIP0_DEFAULT,
        );
        let vreb_2s = st_reb.observe_mid_and_step_elastic_vol_with_fees(
            avax, PriceTicks(8_240), 3_000_000_000, seed_vol, 35, vip_rebate,
        );
        assert_eq!(v0_2s, seed_vol);
        assert!(vreb_2s.raw() > seed_vol.raw());
    }
}

/// Continuous C1 GEOM book-support surface & strict opposing `ImpulseWave3` veto (Law II & VI, V4.9).
///
/// # Errors
/// Returns `LastVetoCode::MacroElliottContradiction` on opposing `ImpulseWave3` or `SpoofingVeto` on hollow book.
#[allow(
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::suboptimal_flops
)]
pub fn passes_elastic_geom_and_macro_impulse_guard(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    side: crate::types::Side,
    spoof_density: f64,
) -> Result<(), crate::lob_shm::LastVetoCode> {
    passes_elastic_geom_and_macro_impulse_guard_with_sync(snap, snap, side, spoof_density, 0.0)
}

/// Continuous C1 GEOM book-support surface + `Epps + BTC` synchronized `CorrectiveAbc` unlock (V5.4).
///
/// # Errors
/// Returns `LastVetoCode::MacroElliottContradiction` on opposing `ImpulseWave3` / unsynchronized `CorrectiveAbc` or `SpoofingVeto` on hollow book.
#[allow(
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::suboptimal_flops
)]
pub fn passes_elastic_geom_and_macro_impulse_guard_with_sync(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_leader_snap: &crate::lob_shm::SymbolLobSnapshot,
    side: crate::types::Side,
    spoof_density: f64,
    epps_corr: f64,
) -> Result<(), crate::lob_shm::LastVetoCode> {
    passes_elastic_geom_and_macro_impulse_guard_with_health(
        snap,
        btc_leader_snap,
        side,
        spoof_density,
        epps_corr,
        None,
        1.0,
    )
}

#[allow(
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]
pub fn passes_elastic_geom_and_macro_impulse_guard_with_health(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_leader_snap: &crate::lob_shm::SymbolLobSnapshot,
    side: crate::types::Side,
    spoof_density: f64,
    epps_corr: f64,
    btc_lead_aligned_override: Option<f64>,
    psi_health: f64,
) -> Result<(), crate::lob_shm::LastVetoCode> {
    const INV_PHI: f64 = 0.618_033_988_749_895;
    const INV_PHI2: f64 = 0.381_966_011_250_105;
    const INV_PHI3: f64 = 0.236_067_977_499_790;
    const INV_PHI4: f64 = 0.145_898_033_750_315;

    let dir_s = side.sign_f64();
    let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
    let eff_corr = if is_follower { epps_corr } else { 0.0 };
    let btc_sync_aligned = if is_follower {
        compute_btc_leader_aligned_score(
            side,
            snap,
            btc_leader_snap,
            eff_corr,
            btc_lead_aligned_override,
        )
    } else {
        0.0
    };
    let btc_w3_aligned = compute_btc_leader_aligned_score(
        side,
        snap,
        btc_leader_snap,
        eff_corr,
        btc_lead_aligned_override,
    );
    let w_obs = snap.wave_observables();
    if is_opposing_impulse_wave3_entry_hazard_v552(
        side,
        snap,
        btc_w3_aligned,
        psi_health,
        0,
        50,
        FeeSchedule::VIP0_DEFAULT,
    ) || !passes_corrective_abc_snapshot_guard_with_health_c1(
        side,
        snap,
        eff_corr,
        btc_sync_aligned,
        psi_health,
    ) {
        return Err(crate::lob_shm::LastVetoCode::MacroElliottContradiction);
    }

    let bull_pm = snap.ob_bull_efficacy_pm as f64;
    let bear_pm = snap.ob_bear_efficacy_pm as f64;
    let is_synthetic_neutral = matches!(w_obs.regime, crate::wave_elliott::WaveRegime::NeutralWarmup)
        && bull_pm <= 0.0
        && bear_pm <= 0.0
        && spoof_density <= 1e-6
        && w_obs.liq_imbalance.get().abs() <= 1e-6;
    if is_synthetic_neutral {
        return Ok(());
    }

    let (al_f, op_f) = match side {
        crate::types::Side::Bid => (bull_pm, bear_pm),
        crate::types::Side::Ask => (bear_pm, bull_pm),
    };
    let rho_ob = if al_f > 0.0 || op_f > 0.0 {
        let a = al_f.max(382.0);
        let b = op_f.max(382.0);
        (a / (a + b)).clamp(0.0, 1.0)
    } else {
        0.5
    };

    let sp_clean = if spoof_density.is_finite() { spoof_density.clamp(0.0, 1.0) } else { 1.0 };
    let h_sp = crate::wave_elliott::hermite_smoothstep_c1(sp_clean, 0.05, 0.75);
    let h_ob_adv = crate::wave_elliott::hermite_smoothstep_c1((1.0 - 2.0 * rho_ob).max(0.0), 0.0, 0.35);
    let h_ob_fav = crate::wave_elliott::hermite_smoothstep_c1((2.0 * rho_ob - 1.0).max(0.0), 0.0, 0.35);
    let liq_adv = (-dir_s * w_obs.liq_imbalance.get()).clamp(0.0, 1.0);
    let h_liq = crate::wave_elliott::hermite_smoothstep_c1(liq_adv, 0.10, 0.80);
    let ofi_al = dir_s * snap.factor_ofi.get();
    let tfi_al = dir_s * snap.factor_tfi.get();
    let s_ice = compute_passive_iceberg_absorption_penalty_c1(ofi_al, tfi_al, btc_sync_aligned);
    let s_flow_btc_sync = if psi_health.is_finite() && psi_health > 0.0 {
        psi_health.clamp(0.0, 1.0)
            * crate::wave_elliott::hermite_smoothstep_c1(
                ofi_al.min(tfi_al),
                INV_PHI2,
                INV_PHI2 + INV_PHI_FIFTH,
            )
            * crate::wave_elliott::hermite_smoothstep_c1(
                btc_sync_aligned,
                INV_PHI3,
                INV_PHI2,
            )
            * crate::wave_elliott::hermite_smoothstep_c1(
                eff_corr.abs(),
                INV_PHI,
                INV_PHI + INV_PHI_FIFTH,
            )
            * (1.0 - s_ice)
    } else {
        0.0
    };

    let eta_lat = compute_tick_lattice_granularity_eta(snap, FeeSchedule::VIP0_DEFAULT);
    let s_lat_geom = if tfi_al >= 0.0 {
        crate::wave_elliott::hermite_smoothstep_c1(eta_lat, INV_PHI2, PHI)
            * crate::wave_elliott::hermite_smoothstep_c1(ofi_al, INV_PHI2, INV_PHI)
            * (1.0 - h_sp)
            * (1.0 - h_liq)
    } else {
        0.0
    };
    let min_geom_floor = 0.05 - (0.05 + INV_PHI4) * s_lat_geom;
    let req_geom = (INV_PHI4 + INV_PHI3 * h_sp + INV_PHI3 * h_ob_adv + INV_PHI2 * h_liq
        - 0.5 * INV_PHI4 * h_ob_fav.max(s_flow_btc_sync)
        - 2.0 * INV_PHI4 * s_lat_geom)
        .clamp(min_geom_floor, INV_PHI);
    let signed_geom = dir_s * snap.factor_geom.get();
    if !signed_geom.is_finite() || signed_geom < req_geom {
        return Err(crate::lob_shm::LastVetoCode::SpoofingVeto);
    }
    Ok(())
}

/// Dimensionless tick-lattice granularity `eta_lat = tau_tick_sbps / max(c_mkr_rt_sbps, 1)` (`V5.5.3`).
#[inline]
#[must_use]
pub fn compute_tick_lattice_granularity_eta(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    fees: FeeSchedule,
) -> f64 {
    let ref_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
        PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
    } else if snap.mark_price.0 > 0 {
        snap.mark_price
    } else {
        snap.last_trade_price
    };
    let Some(spec) = spec_from_symbol_id(snap.symbol_id) else {
        return 0.0;
    };
    let Ok(tau_tick) = spec.tick_to_price_scaled_bps(ref_px) else {
        return 0.0;
    };
    let c_mkr_rt = fees.round_trip_maker_exit_bps().raw().max(1) as f64;
    (tau_tick.raw().max(0) as f64) / c_mkr_rt
}

/// Continuous `C1` silent-tape spoofing weight `(1 - S_3(eta_lat; INV_PHI_SQ, PHI)) * S_3(kappa_evt; 1.0, PHI)` (`V5.5.3`).
#[inline]
#[must_use]
pub fn evaluate_silent_tape_spoof_weight_c1(eta_lat: f64, kappa_evt: f64) -> f64 {
    let eta_c = if eta_lat.is_finite() { eta_lat.max(0.0) } else { 0.0 };
    let kap_c = if kappa_evt.is_finite() { kappa_evt.max(0.0) } else { PHI };
    (1.0 - crate::wave_elliott::hermite_smoothstep_c1(eta_c, INV_PHI_SQ, PHI))
        * crate::wave_elliott::hermite_smoothstep_c1(kap_c, 1.0, PHI)
}

/// Unified `C1` opposing-tape or fine-tick rapid silent-tape `SpoofingVeto` predicate (`V5.5.3`).
#[inline]
#[must_use]
pub fn is_opposing_or_silent_spoof_veto_c1(
    ofi: f64,
    tfi: f64,
    eta_lat: f64,
    kappa_evt: f64,
) -> bool {
    if ofi.abs() > 0.32 && tfi.abs() > 0.32 && (ofi * tfi) < -0.08 {
        return true;
    }
    if ofi.abs() > 0.55 && tfi.abs() < 0.08 {
        if (ofi * tfi) < 0.0 {
            return true;
        }
        return evaluate_silent_tape_spoof_weight_c1(eta_lat, kappa_evt) >= INV_PHI;
    }
    false
}

/// Aborts a resting passive order when its supporting BBO wall evaporates with adverse OFI flip.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn should_abort_collapsed_resting_l1_wall(
    spec: crate::types::PairSpec,
    side: crate::types::Side,
    entry_price: crate::types::PriceTicks,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    init_q_lots: crate::types::LotQty,
) -> bool {
    should_abort_collapsed_resting_l1_wall_dynamic(
        spec,
        side,
        entry_price,
        snap,
        init_q_lots,
        50,
        FeeSchedule::VIP0_DEFAULT,
    )
}

/// Self-learning C1 resting L1 wall-collapse shield coupled to live `p_Taker` & VIP `FeeSchedule`.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn should_abort_collapsed_resting_l1_wall_dynamic(
    spec: crate::types::PairSpec,
    side: crate::types::Side,
    entry_price: crate::types::PriceTicks,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    init_q_lots: crate::types::LotQty,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> bool {
    const PHI: f64 = 1.618_033_988_749_895;
    const INV_PHI: f64 = 0.618_033_988_749_895;
    const INV_PHI2: f64 = 0.381_966_011_250_105;

    let (bbo_px, live_q) = match side {
        crate::types::Side::Bid => (snap.bids[0].price, snap.bids[0].qty),
        crate::types::Side::Ask => (snap.asks[0].price, snap.asks[0].qty),
    };
    if init_q_lots.0 <= 0 || bbo_px.0 <= 0 || bbo_px != entry_price {
        return false;
    }
    let p_tkr = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let ofi_gate = -(0.12 - 0.55 * (p_tkr - 0.05)).clamp(0.05, 0.18);
    let signed_ofi = side.sign_f64() * snap.factor_ofi.get();
    if signed_ofi >= ofi_gate {
        return false;
    }
    let min_l1 = spec
        .min_resting_l1_lots_dynamic(entry_price, snap.realized_vol_5m, taker_prob_pm, fees)
        .0
        .max(1) as f64;
    let tox_shift = (1.0 + PHI * (p_tkr - 0.05)).clamp(0.85, PHI);
    let keep_min = (INV_PHI * tox_shift).clamp(INV_PHI2, 0.85);
    let signed_tfi = side.sign_f64() * snap.factor_tfi.get();
    let s_tfi_fav = if signed_ofi > -INV_PHI {
        crate::wave_elliott::hermite_smoothstep_c1(signed_tfi, INV_PHI2, INV_PHI)
    } else {
        0.0
    };
    let keep_init = ((INV_PHI2 * tox_shift).clamp(0.25, INV_PHI)) * (1.0 - 0.854_101_966_249_685 * s_tfi_fav);
    let init_f = (init_q_lots.0.max(0)) as f64;
    let live_f = (live_q.0.max(0)) as f64;
    live_f < keep_min * min_l1 || (init_f >= min_l1 && live_f < keep_init * init_f)
}

#[cfg(test)]
mod v49_geom_macro_guard_tests {
    use super::*;
    use crate::lob_shm::{BookLevel, LastVetoCode, SymbolLobSnapshot};
    use crate::types::{BoundedScore, LotQty, PriceTicks, ScaledBps, Side, APPROVED_SPECS};
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_v49_elastic_geom_and_macro_guard_blocks_tao17_eth18_and_preserves_wins() {
        // 1. Real TAO #17 LOSS (Ask into bullish ImpulseWave3 macro_phase = +0.90) -> VETOED.
        let mut tao17 = SymbolLobSnapshot::new_empty();
        tao17.factor_geom = BoundedScore::new_clamped(-0.21);
        tao17.ob_bull_efficacy_pm = 459;
        tao17.ob_bear_efficacy_pm = 464;
        tao17.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.515),
            macro_phase: BoundedScore::new_clamped(0.90),
            oi_impulse: BoundedScore::new_clamped(0.08),
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(5_514),
        });
        assert_eq!(
            passes_elastic_geom_and_macro_impulse_guard(&tao17, Side::Ask, 0.0),
            Err(LastVetoCode::MacroElliottContradiction)
        );

        // 2. Real ETH #18 LOSS (Bid with hollow geom = +0.09 & adverse liq_imbalance = -0.67) -> VETOED.
        let mut eth18 = SymbolLobSnapshot::new_empty();
        eth18.factor_geom = BoundedScore::new_clamped(0.09);
        eth18.ob_bull_efficacy_pm = 438;
        eth18.ob_bear_efficacy_pm = 415;
        eth18.set_wave_observables(WaveObservables {
            regime: WaveRegime::CorrectiveAbc,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.696),
            macro_phase: BoundedScore::new_clamped(0.45),
            oi_impulse: BoundedScore::new_clamped(0.09),
            liq_imbalance: BoundedScore::new_clamped(-0.67),
            fib_proj_bps: ScaledBps(0),
        });
        assert_eq!(
            passes_elastic_geom_and_macro_impulse_guard(&eth18, Side::Bid, 0.10),
            Err(LastVetoCode::MacroElliottContradiction)
        );

        // 3. Real AVAX #5 WIN (Bid, geom = +0.23, aligned ImpulseWave3 +0.73, OB+ 742 > OB- 675) -> OK.
        let mut avax5 = SymbolLobSnapshot::new_empty();
        avax5.factor_geom = BoundedScore::new_clamped(0.23);
        avax5.ob_bull_efficacy_pm = 742;
        avax5.ob_bear_efficacy_pm = 675;
        avax5.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.578),
            macro_phase: BoundedScore::new_clamped(0.73),
            oi_impulse: BoundedScore::new_clamped(0.01),
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(2_063),
        });
        assert!(passes_elastic_geom_and_macro_impulse_guard(&avax5, Side::Bid, 0.0).is_ok());

        // 4. Collapsed resting L1 wall abort check (TAO 74 lots collapsing to 8 lots under OFI flip).
        let mut q_snap = SymbolLobSnapshot::new_empty();
        q_snap.realized_vol_5m = ScaledBps(1_809);
        q_snap.asks[0] = BookLevel { price: PriceTicks(19_336), qty: LotQty(8) };
        q_snap.factor_ofi = BoundedScore::new_clamped(0.35);
        assert!(should_abort_collapsed_resting_l1_wall(
            APPROVED_SPECS[4], Side::Ask, PriceTicks(19_336), &q_snap, LotQty(740),
        ));
    }
}

/// Continuous C1 passive Iceberg absorption / exhaustion penalty in `[0.0, 1.0]` across all contracts (V5.5.2).
/// Activates strictly when aggressive tape hits without displacing the book (`tfi_al - ofi_al > INV_PHI_SQ`),
/// book orderflow is genuinely weak (`ofi_al < INV_PHI_CUBE`, `< 0.236`), and BTC leader lacks positive confirmation (`btc_al <= 0.0`),
/// while remaining identically `0.0` on all co-directional book+tape impulses (`ofi_al >= INV_PHI_CUBE`, including all `>= INV_PHI_SQ`).
#[inline]
#[must_use]
pub fn compute_passive_iceberg_absorption_penalty_c1(
    ofi_aligned: f64,
    tfi_aligned: f64,
    btc_aligned: f64,
) -> f64 {
    const INV_PHI_7: f64 = INV_PHI_CUBE * INV_PHI_FOURTH;
    const INV_PHI_8: f64 = INV_PHI_FOURTH * INV_PHI_FOURTH;
    const INV_PHI_9: f64 = INV_PHI_FOURTH * INV_PHI_FIFTH;
    if !ofi_aligned.is_finite()
        || !tfi_aligned.is_finite()
        || !btc_aligned.is_finite()
        || ofi_aligned >= INV_PHI_CUBE
        || tfi_aligned - ofi_aligned <= INV_PHI_SQ
        || btc_aligned >= INV_PHI_7
    {
        return 0.0;
    }
    let s_div = crate::wave_elliott::hermite_smoothstep_c1(
        tfi_aligned - ofi_aligned,
        INV_PHI_SQ,
        INV_PHI_SQ + INV_PHI_8,
    );
    let s_ofi_weak = crate::wave_elliott::hermite_smoothstep_c1(
        INV_PHI_CUBE - ofi_aligned,
        0.0,
        INV_PHI_9,
    );
    let s_btc_nonpos = crate::wave_elliott::hermite_smoothstep_c1(
        INV_PHI_7 - btc_aligned,
        0.0,
        INV_PHI_7,
    );
    (s_div * s_ofi_weak * s_btc_nonpos).clamp(0.0, 1.0)
}

/// Self-learned `C1` inter-asset synchronized breakout lift `delta_h_sync` in `[0.0, INV_PHI_SQ]` for `CorrectiveAbc` (V5.4).
#[inline]
#[must_use]
#[allow(clippy::manual_clamp, clippy::suboptimal_flops)]
pub fn compute_corrective_abc_epps_sync_lift_c1(
    epps_corr: f64,
    btc_aligned: f64,
    ofi_aligned: f64,
    tfi_aligned: f64,
    s_opp_ob_wall: f64,
) -> f64 {
    const INV_PHI_S: f64 = 0.618_033_988_749_895;
    const INV_PHI2_S: f64 = 0.381_966_011_250_105;
    const INV_PHI3_S: f64 = 0.236_067_977_499_79;
    if !epps_corr.is_finite() || !btc_aligned.is_finite() || !ofi_aligned.is_finite() || !tfi_aligned.is_finite() {
        return 0.0;
    }
    let s_epps = crate::wave_elliott::hermite_smoothstep_c1(
        epps_corr.abs(),
        INV_PHI_S,
        1.0 - INV_PHI3_S,
    );
    let s_btc = crate::wave_elliott::hermite_smoothstep_c1(btc_aligned, INV_PHI2_S, INV_PHI_S);
    let s_flow = crate::wave_elliott::hermite_smoothstep_c1(
        ofi_aligned.min(tfi_aligned),
        INV_PHI2_S,
        INV_PHI_S,
    );
    let opp_clear = (1.0 - s_opp_ob_wall.clamp(0.0, 1.0)).clamp(0.0, 1.0);
    INV_PHI2_S * s_epps * s_btc * s_flow * opp_clear
}

/// Zero-allocation unified-horizon lead-lag memory (`BtcLeadLagMemory`, V5.4.2 M1 + V5.5 O2):
/// Tracks `BTC` SNR memory (`\tau = \varphi^4` s) AND short exponential price return `\Delta p_{\tau_{ll}}`
/// across `BTCUSDT` (`slot 0`) and all slots `0..MAX_ACTIVE_PAIRS` using the exact same continuous
/// semigroup decay `\alpha_{ll}` coupled to `\tau_{ll} = (\varphi^2 \cdot rtt_{eff}).clamp(\varphi^{-2}, \varphi)`.
#[derive(Copy, Clone, Debug, Default)]
pub struct BtcLeadLagMemory {
    snr_ema: f64,
    last_ts_ns: i64,
    initialized: bool,
    ema_mid_ticks: [f64; crate::lob_shm::MAX_ACTIVE_PAIRS],
    dp_tau_bps: [f64; crate::lob_shm::MAX_ACTIVE_PAIRS],
    slot_last_ts_ns: [i64; crate::lob_shm::MAX_ACTIVE_PAIRS],
    slot_initialized: [bool; crate::lob_shm::MAX_ACTIVE_PAIRS],
}

impl BtcLeadLagMemory {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            snr_ema: 0.0,
            last_ts_ns: 0,
            initialized: false,
            ema_mid_ticks: [0.0; crate::lob_shm::MAX_ACTIVE_PAIRS],
            dp_tau_bps: [0.0; crate::lob_shm::MAX_ACTIVE_PAIRS],
            slot_last_ts_ns: [0; crate::lob_shm::MAX_ACTIVE_PAIRS],
            slot_initialized: [false; crate::lob_shm::MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    #[must_use]
    pub fn unified_horizon_tau_ll_s(ws_rtt_us: u32) -> f64 {
        let rtt_eff_s = (f64::from(if ws_rtt_us > 0 { ws_rtt_us } else { 70_000 })) * 1e-6;
        (PHI_SQ * rtt_eff_s).clamp(INV_PHI_SQ, PHI)
    }

    #[inline]
    #[must_use]
    fn snapshot_mid_f64(snap: &SymbolLobSnapshot) -> Option<f64> {
        if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            Some(0.5 * ((snap.bids[0].price.0 + snap.asks[0].price.0) as f64))
        } else if snap.mark_price.0 > 0 {
            Some(snap.mark_price.0 as f64)
        } else {
            None
        }
    }

    /// Updates short exponential return `\Delta p_{\tau_{ll}}` (in bps, `1.0 = 1 bp`) for `slot_idx`
    /// using unified decay `\alpha_{ll} = 1 - \exp(-\Delta t / \tau_{ll})`.
    #[inline]
    pub fn observe_slot_snapshot(
        &mut self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
        ws_rtt_us: u32,
    ) -> f64 {
        if slot_idx >= crate::lob_shm::MAX_ACTIVE_PAIRS {
            return 0.0;
        }
        let Some(mid_px) = Self::snapshot_mid_f64(snap) else {
            return self.dp_tau_bps[slot_idx];
        };
        if !mid_px.is_finite() || mid_px <= 0.0 {
            return self.dp_tau_bps[slot_idx];
        }
        let ts = snap.ts_local_ns.0;
        if !self.slot_initialized[slot_idx]
            || ts <= 0
            || self.slot_last_ts_ns[slot_idx] <= 0
            || ts < self.slot_last_ts_ns[slot_idx]
        {
            self.ema_mid_ticks[slot_idx] = mid_px;
            self.dp_tau_bps[slot_idx] = 0.0;
            self.slot_last_ts_ns[slot_idx] = ts;
            self.slot_initialized[slot_idx] = true;
            return 0.0;
        }
        if ts == self.slot_last_ts_ns[slot_idx] {
            let ema = self.ema_mid_ticks[slot_idx].max(1e-9);
            self.dp_tau_bps[slot_idx] = 10_000.0 * (mid_px - ema) / ema;
            return self.dp_tau_bps[slot_idx];
        }
        let dt_s = ((ts - self.slot_last_ts_ns[slot_idx]) as f64) * 1e-9;
        let tau_ll = Self::unified_horizon_tau_ll_s(ws_rtt_us);
        let alpha_ll = if dt_s >= 30.0 * tau_ll {
            1.0
        } else {
            (1.0 - (-dt_s / tau_ll).exp()).clamp(0.0, 1.0)
        };
        let prev_ema = self.ema_mid_ticks[slot_idx];
        let next_ema = (prev_ema + alpha_ll * (mid_px - prev_ema)).max(1e-9);
        self.ema_mid_ticks[slot_idx] = next_ema;
        self.dp_tau_bps[slot_idx] = 10_000.0 * (mid_px - next_ema) / next_ema;
        self.slot_last_ts_ns[slot_idx] = ts;
        self.dp_tau_bps[slot_idx]
    }

    #[inline]
    #[must_use]
    pub fn slot_dp_tau_bps_at(
        &self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
        eval_ts_ns: i64,
        ws_rtt_us: u32,
    ) -> f64 {
        if slot_idx >= crate::lob_shm::MAX_ACTIVE_PAIRS || !self.slot_initialized[slot_idx] {
            return 0.0;
        }
        let Some(cur_mid) = Self::snapshot_mid_f64(snap) else {
            return self.dp_tau_bps[slot_idx];
        };
        let prev_ema = self.ema_mid_ticks[slot_idx].max(1e-9);
        let last_ts = self.slot_last_ts_ns[slot_idx];
        if eval_ts_ns <= last_ts || last_ts <= 0 {
            return 10_000.0 * (cur_mid - prev_ema) / prev_ema;
        }
        let dt_s = ((eval_ts_ns - last_ts) as f64) * 1e-9;
        let tau_ll = Self::unified_horizon_tau_ll_s(ws_rtt_us);
        let alpha_ll = if dt_s >= 30.0 * tau_ll {
            1.0
        } else {
            (1.0 - (-dt_s / tau_ll).exp()).clamp(0.0, 1.0)
        };
        let ema_eval = (prev_ema + alpha_ll * (cur_mid - prev_ema)).max(1e-9);
        10_000.0 * (cur_mid - ema_eval) / ema_eval
    }

    /// Computes undiscounted directional lead-lag `lag_bps = dir_sign * (epps_beta * dp_btc_bps - dp_own_bps)`
    /// on the exact unified EMA horizon `\tau_{ll}` (V5.5 O2).
    #[inline]
    #[must_use]
    pub fn undiscounted_lead_lag_bps(
        &self,
        slot_idx: usize,
        side: Side,
        snap: &SymbolLobSnapshot,
        btc_snap: &SymbolLobSnapshot,
        ws_rtt_us: u32,
        epps_beta: f64,
    ) -> f64 {
        if slot_idx == 0
            || slot_idx >= crate::lob_shm::MAX_ACTIVE_PAIRS
            || snap.symbol_id == btc_snap.symbol_id
            || !self.slot_initialized[0]
            || !self.slot_initialized[slot_idx]
            || !epps_beta.is_finite()
        {
            return 0.0;
        }
        let eval_ts = snap.ts_local_ns.0.max(btc_snap.ts_local_ns.0);
        let dp_btc_bps = self.slot_dp_tau_bps_at(0, btc_snap, eval_ts, ws_rtt_us);
        let dp_own_bps = self.slot_dp_tau_bps_at(slot_idx, snap, eval_ts, ws_rtt_us);
        let beta_c = epps_beta.clamp(-PHI, PHI);
        side.sign_f64() * (beta_c * dp_btc_bps - dp_own_bps)
    }

    /// Bounded `C1` lead-lag ignition bonus `psi_health * INV_PHI_CUBE * S_3(lag_bps; INV_PHI * fee_rt_mkr_bps, PHI * fee_rt_mkr_bps)`
    /// strictly when `btc_ofi_al >= INV_PHI_SQ && btc_tfi_al >= INV_PHI_SQ`, `|epps_corr| >= INV_PHI`,
    /// `!iceberg_trap` and `lag_bps >= INV_PHI * fee_rt_mkr_bps` (V5.5 O2).
    #[inline]
    #[must_use]
    pub fn compute_lead_lag_ignition_bonus_c1(
        &self,
        slot_idx: usize,
        side: Side,
        snap: &SymbolLobSnapshot,
        btc_snap: &SymbolLobSnapshot,
        ws_rtt_us: u32,
        epps_beta: f64,
        epps_corr: f64,
        psi_health: f64,
        fees: FeeSchedule,
    ) -> f64 {
        if slot_idx == 0
            || slot_idx >= crate::lob_shm::MAX_ACTIVE_PAIRS
            || snap.symbol_id == btc_snap.symbol_id
            || !epps_corr.is_finite()
            || epps_corr.abs() < INV_PHI
            || !psi_health.is_finite()
            || psi_health <= 0.0
        {
            return 0.0;
        }
        let dir_s = side.sign_f64();
        let corr_sign = if epps_corr < 0.0 { -1.0 } else { 1.0 };
        let btc_ofi_al = corr_sign * btc_snap.factor_ofi.get() * dir_s;
        let btc_tfi_al = corr_sign * btc_snap.factor_tfi.get() * dir_s;
        if btc_ofi_al < INV_PHI_SQ || btc_tfi_al < INV_PHI_SQ {
            return 0.0;
        }
        let ofi_al = snap.factor_ofi.get() * dir_s;
        let tfi_al = snap.factor_tfi.get() * dir_s;
        let iceberg_trap = ofi_al < 0.0
            || tfi_al < 0.0
            || compute_passive_iceberg_absorption_penalty_c1(ofi_al, tfi_al, 0.0) > 0.0;
        if iceberg_trap {
            return 0.0;
        }
        let fee_rt_mkr_bps = ((fees.round_trip_maker_exit_bps().raw().max(100)) as f64) / 100.0;
        let lo_lag = INV_PHI * fee_rt_mkr_bps;
        let hi_lag = PHI * fee_rt_mkr_bps;
        let lag_bps =
            self.undiscounted_lead_lag_bps(slot_idx, side, snap, btc_snap, ws_rtt_us, epps_beta);
        if lag_bps < lo_lag {
            return 0.0;
        }
        let s_lag = crate::wave_elliott::hermite_smoothstep_c1(lag_bps, lo_lag, hi_lag);
        (psi_health.clamp(0.0, 1.0) * INV_PHI_CUBE * s_lag).clamp(0.0, INV_PHI_CUBE)
    }

    #[inline]
    pub fn observe_btc_snapshot(&mut self, btc_snap: &SymbolLobSnapshot) -> f64 {
        self.observe_btc_snapshot_with_rtt(
            btc_snap,
            crate::execution_fsm::read_live_ws_pulse().0,
        )
    }

    #[inline]
    pub fn observe_btc_snapshot_with_rtt(
        &mut self,
        btc_snap: &SymbolLobSnapshot,
        ws_rtt_us: u32,
    ) -> f64 {
        let _ = self.observe_slot_snapshot(0, btc_snap, ws_rtt_us);
        if btc_snap.bids[0].price.0 <= 0 || btc_snap.asks[0].price.0 <= 0 {
            return self.snr_ema;
        }
        let inst_snr = OrthogonalFactorState::snr_composite_score(btc_snap);
        if !inst_snr.is_finite() {
            return self.snr_ema;
        }
        let ts = btc_snap.ts_local_ns.0;
        if !self.initialized || ts <= 0 || self.last_ts_ns <= 0 || ts < self.last_ts_ns {
            self.snr_ema = inst_snr;
            self.last_ts_ns = ts;
            self.initialized = true;
        } else if ts > self.last_ts_ns {
            let dt_s = ((ts - self.last_ts_ns) as f64) * 1e-9;
            let alpha = if dt_s >= 60.0 {
                1.0
            } else {
                (1.0 - (-dt_s / PHI_FOURTH).exp()).clamp(0.0, 1.0)
            };
            self.snr_ema += alpha * (inst_snr - self.snr_ema);
            self.last_ts_ns = ts;
        }
        self.snr_ema
    }

    #[inline]
    #[must_use]
    pub fn effective_aligned_impulse(
        &self,
        btc_snap: &SymbolLobSnapshot,
        corr_sign: f64,
        side: Side,
    ) -> f64 {
        let dir_s = side.sign_f64();
        let inst = corr_sign * OrthogonalFactorState::snr_composite_score(btc_snap) * dir_s;
        if self.initialized {
            inst.max(corr_sign * self.snr_ema * dir_s)
        } else {
            inst
        }
    }
}

/// Real structural threat `chi_threat = s_opp_ob_wall.max(S_3(-macro_aligned; INV_PHI_CUBE, 0.50))` in `[0.0, 1.0]` (V5.4.2 M1).
#[inline]
#[must_use]
pub fn compute_corrective_abc_structural_threat_c1(
    s_opp_ob_wall: f64,
    macro_aligned: f64,
) -> f64 {
    let s_opp = if s_opp_ob_wall.is_finite() {
        s_opp_ob_wall.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let s_macro_adv =
        crate::wave_elliott::hermite_smoothstep_c1(-macro_aligned, INV_PHI_CUBE, 0.50);
    s_opp.max(s_macro_adv).clamp(0.0, 1.0)
}

/// Clear-runway `CorrectiveAbc` Hurst lift `delta_h_clear` in `[0.0, INV_PHI_SQ]` gated by `psi_health` (V5.4.2 M1).
#[inline]
#[must_use]
pub fn compute_corrective_abc_clear_lift_c1(
    psi_health: f64,
    flow_aligned: f64,
    chi_threat: f64,
    s_iceberg_pen: f64,
) -> f64 {
    if !psi_health.is_finite()
        || !flow_aligned.is_finite()
        || !chi_threat.is_finite()
        || !s_iceberg_pen.is_finite()
    {
        return 0.0;
    }
    let psi = psi_health.clamp(0.0, 1.0);
    let s_flow = crate::wave_elliott::hermite_smoothstep_c1(flow_aligned, INV_PHI_CUBE, INV_PHI);
    psi * INV_PHI_SQ
        * s_flow
        * (1.0 - chi_threat.clamp(0.0, 1.0))
        * (1.0 - s_iceberg_pen.clamp(0.0, 1.0))
}

/// Physical `CorrectiveAbc` Order-Block shot reach `reach_bps` in `ScaledBps` (`1 bp = 100`, `[1_800.0, 6_200.0]`, V5.5.2).
/// `reach_bps = (PHI_SQ * target_or_b3x_bps).max(realized_vol_5m_bps * INV_PHI).clamp(18.0, 62.0)`.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn compute_corrective_abc_ob_reach_sbps(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    target_or_b3x_override_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    let (b3x_sbps, _) = endogenous_b3x_and_eq_vol_sbps(snap, 0, taker_prob_pm, fees);
    let target_or_b3x_bps = (target_or_b3x_override_sbps.max(b3x_sbps) as f64) / 100.0;
    let realized_vol_5m_bps = (snap.realized_vol_5m.raw().max(0) as f64) / 100.0;
    let reach_bps = (PHI_SQ * target_or_b3x_bps)
        .max(realized_vol_5m_bps * INV_PHI)
        .clamp(18.0, 62.0);
    100.0 * reach_bps
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
fn compute_corrective_abc_ob_wall_pair_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    target_or_b3x_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> (f64, f64) {
    let (own_sc, opp_sc, own_dist, opp_dist) = match side {
        crate::types::Side::Bid => (
            snap.ob_bull_score.get(),
            snap.ob_bear_score.get(),
            snap.ob_dist_bull_bps.raw(),
            snap.ob_dist_bear_bps.raw(),
        ),
        crate::types::Side::Ask => (
            snap.ob_bear_score.get(),
            snap.ob_bull_score.get(),
            snap.ob_dist_bear_bps.raw(),
            snap.ob_dist_bull_bps.raw(),
        ),
    };
    let reach_sbps =
        compute_corrective_abc_ob_reach_sbps(snap, target_or_b3x_sbps, taker_prob_pm, fees);
    let dir_s = side.sign_f64();
    let flow_min = (snap.factor_ofi.get() * dir_s).min(snap.factor_tfi.get() * dir_s);
    let s_flow_sup = crate::wave_elliott::hermite_smoothstep_c1(flow_min, INV_PHI_CUBE, INV_PHI);
    let s_own_ob = if own_dist > 0 && own_sc > 0.0 {
        crate::wave_elliott::hermite_smoothstep_c1(own_sc, INV_PHI_FIFTH, INV_PHI_SQ)
    } else {
        0.0
    };
    let own_eff = if own_dist > 0 && own_sc > 0.0 {
        own_sc / (1.0 + (own_dist as f64) / (PHI * reach_sbps))
    } else {
        0.0
    };
    let opp_eff = if opp_dist > 0 && opp_sc > 0.0 {
        let eff_reach_sbps = reach_sbps * (1.0 + INV_PHI_CUBE * (1.0 - s_own_ob));
        let d_ratio = (opp_dist as f64) / eff_reach_sbps.max(100.0);
        let far_tail = INV_PHI_SQ * (d_ratio - 2.15).max(0.0).powi(2);
        let hi_far_sbps = (PHI * reach_sbps).clamp(6_500.0, 9_000.0);
        let lo_far_sbps = reach_sbps.clamp(3_500.0, 6_200.0).min(hi_far_sbps - 500.0);
        let s_far_65 =
            crate::wave_elliott::hermite_smoothstep_c1(opp_dist as f64, lo_far_sbps, hi_far_sbps);
        (opp_sc / (1.0 + d_ratio + far_tail)) * (1.0 - s_flow_sup * s_own_ob * s_far_65)
    } else {
        0.0
    };
    let s_opp_wall =
        crate::wave_elliott::hermite_smoothstep_c1(opp_eff - own_eff, INV_PHI_FIFTH, INV_PHI_FOURTH);
    let s_own_wall =
        crate::wave_elliott::hermite_smoothstep_c1(own_eff - opp_eff, INV_PHI_FIFTH, INV_PHI_CUBE);
    (s_opp_wall, s_own_wall)
}

#[inline]
#[must_use]
pub fn compute_s_opp_ob_wall_with_target_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    target_or_b3x_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    compute_corrective_abc_ob_wall_pair_c1(side, snap, target_or_b3x_sbps, taker_prob_pm, fees).0
}

#[inline]
#[must_use]
pub fn compute_s_opp_ob_wall_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    _eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    compute_s_opp_ob_wall_with_target_c1(side, snap, 0, taker_prob_pm, fees)
}

/// Tick-level L2 (`200 Hz`) `CorrectiveAbc` Hurst persistence lift `delta_h_tick_l2` in `[0.0, INV_PHI_SQ]` (`V5.5.4`).
/// Eliminates the `|epps_corr| >= INV_PHI` step via continuous convex synergy:
/// `|epps_corr| * S_3(btc_al; INV_PHI_CUBE, INV_PHI_SQ) + (1 - |epps_corr|) * S_3(min(ofi_al, tfi_al); INV_PHI_SQ, INV_PHI)`
/// strictly gated by `chi_threat < INV_PHI` (preserving `0.0` whenever `s_opp_ob_wall == 1.0`) and passive iceberg absence.
#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn compute_corrective_abc_tick_hurst_lift_c1(
    psi_health: f64,
    ofi_aligned: f64,
    tfi_aligned: f64,
    btc_aligned: f64,
    epps_corr: f64,
    chi_threat: f64,
    s_iceberg_pen: f64,
) -> f64 {
    if !psi_health.is_finite()
        || psi_health <= 0.0
        || !ofi_aligned.is_finite()
        || !tfi_aligned.is_finite()
        || !btc_aligned.is_finite()
        || !epps_corr.is_finite()
        || !chi_threat.is_finite()
        || !s_iceberg_pen.is_finite()
        || chi_threat >= INV_PHI
    {
        return 0.0;
    }
    let flow_min = ofi_aligned.min(tfi_aligned);
    if flow_min < INV_PHI_SQ {
        return 0.0;
    }
    let s_threat_ok =
        crate::wave_elliott::hermite_smoothstep_c1(INV_PHI - chi_threat, 0.0, INV_PHI_FOURTH);
    let s_flow_gate = crate::wave_elliott::hermite_smoothstep_c1(
        flow_min,
        INV_PHI_SQ,
        INV_PHI_SQ + INV_PHI_FIFTH,
    );
    let s_flow_own =
        crate::wave_elliott::hermite_smoothstep_c1(flow_min, INV_PHI_SQ, INV_PHI);
    let s_btc = crate::wave_elliott::hermite_smoothstep_c1(
        btc_aligned,
        INV_PHI_CUBE,
        INV_PHI_SQ,
    );
    let abs_corr = epps_corr.abs().clamp(0.0, 1.0);
    let s_synergy = abs_corr * s_btc + (1.0 - abs_corr) * s_flow_own;
    let s_no_ice = (1.0 - s_iceberg_pen.clamp(0.0, 1.0)).clamp(0.0, 1.0);
    (psi_health.clamp(0.0, 1.0)
        * INV_PHI_SQ
        * s_threat_ok
        * s_flow_gate
        * s_synergy
        * s_no_ice)
        .clamp(0.0, INV_PHI_SQ)
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn passes_corrective_abc_fractal_guard_with_health_c1(
    side: crate::types::Side,
    wave: crate::wave_elliott::WaveObservables,
    ob_align_pm: i64,
    ob_opp_pm: i64,
    epps_corr: f64,
    btc_aligned: f64,
    ofi_aligned: f64,
    tfi_aligned: f64,
    s_opp_ob_wall: f64,
    psi_health: f64,
) -> bool {
    const INV_PHI_C: f64 = 0.618_033_988_749_895;
    const INV_PHI2_C: f64 = 0.381_966_011_250_105;
    const INV_PHI3_C: f64 = 0.236_067_977_499_79;
    if !matches!(wave.regime, crate::wave_elliott::WaveRegime::CorrectiveAbc) {
        return true;
    }
    let aligned_macro = side.sign_f64() * wave.macro_phase.get();
    let has_btc_backing = btc_aligned >= INV_PHI3_C && epps_corr.abs() >= INV_PHI_C;
    if aligned_macro <= -INV_PHI2_C && !has_btc_backing {
        return false;
    }
    let rho_ob = if ob_align_pm > 0 || ob_opp_pm > 0 {
        let a = (ob_align_pm.max(382)) as f64;
        let b = (ob_opp_pm.max(382)) as f64;
        (a / (a + b)).clamp(0.0, 1.0)
    } else {
        0.50
    };
    let chi_threat = compute_corrective_abc_structural_threat_c1(s_opp_ob_wall, aligned_macro);
    let s_threat_gate =
        1.0 - crate::wave_elliott::hermite_smoothstep_c1(chi_threat, INV_PHI_C - INV_PHI_FIFTH, INV_PHI_C);
    let s_iceberg_pen =
        compute_passive_iceberg_absorption_penalty_c1(ofi_aligned, tfi_aligned, btc_aligned);
    let delta_h_clear = s_threat_gate
        * compute_corrective_abc_clear_lift_c1(
            psi_health,
            ofi_aligned.min(tfi_aligned),
            chi_threat,
            s_iceberg_pen,
        );
    let delta_h_sync = s_threat_gate
        * compute_corrective_abc_epps_sync_lift_c1(
            epps_corr,
            btc_aligned,
            ofi_aligned,
            tfi_aligned,
            s_opp_ob_wall,
        );
    let delta_h_tick = compute_corrective_abc_tick_hurst_lift_c1(
        psi_health,
        ofi_aligned,
        tfi_aligned,
        btc_aligned,
        epps_corr,
        chi_threat,
        s_iceberg_pen,
    );
    let s_sync = ((delta_h_sync + delta_h_clear + delta_h_tick) / INV_PHI2_C).clamp(0.0, 1.0);
    let s_ob = crate::wave_elliott::hermite_smoothstep_c1(rho_ob, INV_PHI2_C, INV_PHI_C);
    let s_adv_macro = crate::wave_elliott::hermite_smoothstep_c1(-aligned_macro, 0.0, INV_PHI_C);
    let macro_floor = -INV_PHI3_C * (0.50 + s_ob + s_sync);
    if aligned_macro < macro_floor {
        return false;
    }
    let s_pos_m = crate::wave_elliott::hermite_smoothstep_c1(aligned_macro, INV_PHI3_C, INV_PHI_C);
    let base_h_crit = 0.50 + INV_PHI3_C * s_ob + 0.090_169_943_749_47 * s_pos_m
        - INV_PHI3_C * s_adv_macro
        + delta_h_sync
        + delta_h_clear;
    let h_crit = (base_h_crit + delta_h_tick).min(0.80);
    wave.hurst_exponent() <= h_crit
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn passes_corrective_abc_fractal_guard_with_sync_c1(
    side: crate::types::Side,
    wave: crate::wave_elliott::WaveObservables,
    ob_align_pm: i64,
    ob_opp_pm: i64,
    epps_corr: f64,
    btc_aligned: f64,
    ofi_aligned: f64,
    tfi_aligned: f64,
    s_opp_ob_wall: f64,
) -> bool {
    passes_corrective_abc_fractal_guard_with_health_c1(
        side,
        wave,
        ob_align_pm,
        ob_opp_pm,
        epps_corr,
        btc_aligned,
        ofi_aligned,
        tfi_aligned,
        s_opp_ob_wall,
        1.0,
    )
}

#[inline]
#[must_use]
pub fn passes_corrective_abc_fractal_guard_c1(
    side: crate::types::Side,
    wave: crate::wave_elliott::WaveObservables,
    ob_align_pm: i64,
    ob_opp_pm: i64,
) -> bool {
    passes_corrective_abc_fractal_guard_with_sync_c1(
        side,
        wave,
        ob_align_pm,
        ob_opp_pm,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    )
}

/// Resolves directional `BTC` leader alignment combining composite SNR, co-directional `min(OFI, TFI)` flow, and optional lead-lag memory (`V5.5.2`).
#[inline]
#[must_use]
pub fn compute_btc_leader_aligned_score(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_leader_snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_lead_aligned_override: Option<f64>,
) -> f64 {
    let dir_s = side.sign_f64();
    let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
    let corr_sign = if is_follower && epps_corr < 0.0 {
        -1.0
    } else {
        1.0
    };
    let inst_comp = corr_sign * OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
    let flow_min = (corr_sign * btc_leader_snap.factor_ofi.get() * dir_s)
        .min(corr_sign * btc_leader_snap.factor_tfi.get() * dir_s);
    let inst = inst_comp.max(flow_min);
    if is_follower {
        btc_lead_aligned_override.map_or(inst, |m| inst.max(m))
    } else {
        inst
    }
}

///Resolves endogenous `(B_3x, eq_vol_5m_bps)` in `ScaledBps` (`1 bp = 100`) from live snapshot & fees.
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn endogenous_b3x_and_eq_vol_sbps(
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_override_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> (i64, i64) {
    let spec = spec_from_symbol_id(snap.symbol_id).unwrap_or(crate::types::APPROVED_SPECS[0]);
    let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
        PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
    } else if snap.mark_price.0 > 0 {
        snap.mark_price
    } else {
        PriceTicks(10_000)
    };
    let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
    let b3x_sbps = spec
        .three_x_friction_barrier_bps_dynamic(mid_px, taker_prob_pm, fees)
        .raw()
        .max(min_v);
    let phi_sq_b3x = (PHI_SQ * (b3x_sbps as f64)).round() as i64;
    let eq_vol_sbps = eq_vol_override_sbps
        .max(snap.realized_vol_5m.raw())
        .max(phi_sq_b3x);
    (b3x_sbps, eq_vol_sbps)
}

/// Immediate hazard detector for opposing `ImpulseWave3` sweeps (preflight, `RepegEntry`, exit & `0 ms` resting queue abort).
/// Strictly independent of `wave_hurst` and anchored to `-max(PHI * B_3x(t), INV_PHI * eq_vol_5m_bps(t))`.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn is_opposing_impulse_wave3_hazard_dynamic(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> bool {
    let w = snap.wave_observables();
    if !matches!(w.regime, crate::wave_elliott::WaveRegime::ImpulseWave3) {
        return false;
    }
    let (b3x_sbps, eq_vol_sbps) =
        endogenous_b3x_and_eq_vol_sbps(snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    let fib_cut_sbps = (PHI * (b3x_sbps as f64)).max(INV_PHI * (eq_vol_sbps as f64));
    let dir_s = side.sign_f64();
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_sbps = (w.fib_proj_bps.raw() as f64) * dir_s;
    macro_aligned < -INV_PHI || (macro_aligned < -INV_PHI_SQ && fib_aligned_sbps <= -fib_cut_sbps)
}

#[inline]
#[must_use]
pub fn is_opposing_impulse_wave3_hazard(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
) -> bool {
    is_opposing_impulse_wave3_hazard_dynamic(side, snap, 0, 50, FeeSchedule::VIP0_DEFAULT)
}

/// Intrinsic diffusive reachability (`V5.5.4`):
/// `sigma_5m * sqrt(clamp(tau_mkt / 300.0, INV_PHI_SQ, PHI)) >= PHI * c_rt_mkr` (all in `ScaledBps`, `1 bp = 100`).
#[inline]
#[must_use]
pub fn passes_intrinsic_diffusive_reachability_c1(
    realized_vol_5m: ScaledBps,
    tau_mkt_s: f64,
    fees: FeeSchedule,
) -> bool {
    let sigma_5m_sbps = realized_vol_5m.raw().max(0) as f64;
    let tau_s = if tau_mkt_s.is_finite() && tau_mkt_s > 0.0 {
        tau_mkt_s
    } else {
        300.0
    };
    let tau_scale = (tau_s / 300.0).clamp(INV_PHI_SQ, PHI).sqrt();
    let c_rt_mkr_sbps = (fees.round_trip_maker_exit_bps().raw().max(50)) as f64;
    sigma_5m_sbps * tau_scale >= PHI * c_rt_mkr_sbps
}

/// Continuous `C1` pre-print alignment bridge in `[0.0, 1.0]` (`V5.5.4`):
/// Active strictly when `spoof_density < INV_PHI_SQ` and `OFI_al`, `GEOM_al`, and `BTC_al` are strongly aligned.
#[inline]
#[must_use]
pub fn compute_preprint_alignment_bridge_c1(
    ofi_aligned: f64,
    geom_aligned: f64,
    btc_aligned: f64,
    spoof_density: f64,
) -> f64 {
    let sp_c = if spoof_density.is_finite() {
        spoof_density.clamp(0.0, 1.0)
    } else {
        1.0
    };
    if sp_c >= INV_PHI_SQ
        || !ofi_aligned.is_finite()
        || !geom_aligned.is_finite()
        || !btc_aligned.is_finite()
    {
        return 0.0;
    }
    let s_sp = 1.0 - crate::wave_elliott::hermite_smoothstep_c1(sp_c, INV_PHI_FOURTH, INV_PHI_SQ);
    let s_ofi = crate::wave_elliott::hermite_smoothstep_c1(ofi_aligned, INV_PHI_CUBE, INV_PHI);
    let s_geom =
        crate::wave_elliott::hermite_smoothstep_c1(geom_aligned, INV_PHI_FOURTH, INV_PHI_SQ);
    let s_btc = crate::wave_elliott::hermite_smoothstep_c1(btc_aligned, INV_PHI_CUBE, INV_PHI_SQ);
    (s_sp * s_ofi * s_geom * s_btc).clamp(0.0, 1.0)
}

/// Continuous `C1` own `[OB+, OB-]` support weight in `[0.0, 1.0]` (`V5.5.4`).
/// Strictly `0.0` when neither an active own Order-Block wall (`own_sc >= INV_PHI_FOURTH`)
/// nor net positive directional `[OB+, OB-]` efficacy (`own_pm > 500 && own_pm > opp_pm`) is present.
#[inline]
#[must_use]
pub fn compute_own_ob_support_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    let (own_pm, opp_pm, own_sc, own_dist) = match side {
        crate::types::Side::Bid => (
            snap.ob_bull_efficacy_pm,
            snap.ob_bear_efficacy_pm,
            snap.ob_bull_score.get(),
            snap.ob_dist_bull_bps.raw(),
        ),
        crate::types::Side::Ask => (
            snap.ob_bear_efficacy_pm,
            snap.ob_bull_efficacy_pm,
            snap.ob_bear_score.get(),
            snap.ob_dist_bear_bps.raw(),
        ),
    };
    let has_own_wall = own_dist > 0 && own_sc >= INV_PHI_FOURTH;
    let has_own_efficacy = own_pm > 500 && own_pm > opp_pm;
    if !has_own_wall && !has_own_efficacy {
        return 0.0;
    }
    let rho_ob = compute_ob_alignment_ratio_c1(side, snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    let (_, s_own_wall) =
        compute_corrective_abc_ob_wall_pair_c1(side, snap, 0, taker_prob_pm, fees);
    crate::wave_elliott::hermite_smoothstep_c1(rho_ob, 0.50, INV_PHI)
        .max(s_own_wall)
        .clamp(0.0, 1.0)
}

/// Continuous `C1` entry block attenuation in `[0.0, 1.0]` for an aged opposing `ImpulseWave3` (`V5.5.4`),
/// modulated by `pivot_age_s / tau_mkt_s` (and `WaveObservables` aged phase), own `[OB+, OB-]` support,
/// and `min(ofi_al, tfi_al) >= INV_PHI_SQ`.
#[inline]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_aged_impulse_wave3_entry_relief_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    _btc_aligned: f64,
    psi_health: f64,
    pivot_age_s: f64,
    tau_mkt_s: f64,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    if !psi_health.is_finite() || psi_health <= 0.0 {
        return 0.0;
    }
    let w = snap.wave_observables();
    if w.climax_active
        || !matches!(w.regime, crate::wave_elliott::WaveRegime::ImpulseWave3)
    {
        return 0.0;
    }
    let dir_s = side.sign_f64();
    let ofi_al = snap.factor_ofi.get() * dir_s;
    let tfi_al = snap.factor_tfi.get() * dir_s;
    if !ofi_al.is_finite() || !tfi_al.is_finite() {
        return 0.0;
    }
    let flow_min = ofi_al.min(tfi_al);
    if flow_min < INV_PHI_SQ {
        return 0.0;
    }
    let s_own_ob = compute_own_ob_support_c1(side, snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    if s_own_ob <= 0.0 {
        return 0.0;
    }
    let macro_aligned = w.macro_phase.get() * dir_s;
    let s_age_direct =
        crate::wave_elliott::compute_impulse_wave3_age_decay_c1(pivot_age_s, tau_mkt_s);
    let s_age_obs = crate::wave_elliott::hermite_smoothstep_c1(
        0.68 - (-macro_aligned),
        0.0,
        0.68 - crate::wave_elliott::STALE_IMPULSE_WAVE3_PHASE_FLOOR,
    );
    let s_age = s_age_direct.max(s_age_obs);
    if s_age <= 0.0 {
        return 0.0;
    }
    let s_flow = crate::wave_elliott::hermite_smoothstep_c1(flow_min, INV_PHI_SQ, INV_PHI);
    let s_psi = crate::wave_elliott::hermite_smoothstep_c1(psi_health, INV_PHI_CUBE, INV_PHI);
    (s_psi * s_age * s_own_ob * s_flow).clamp(0.0, 1.0)
}

/// Clean intraday reversal predicate against stale opposing `ImpulseWave3`, strictly for NEW entry admission (`psi_health > INV_PHI_SQ`, `V5.5.4`).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn qualifies_clean_intraday_wave3_reversal_v552(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_aligned: f64,
    psi_health: f64,
) -> bool {
    let aged_relief = compute_aged_impulse_wave3_entry_relief_c1(
        side,
        snap,
        btc_aligned,
        psi_health,
        0.0,
        300.0,
        0,
        50,
        FeeSchedule::VIP0_DEFAULT,
    );
    if aged_relief >= INV_PHI_SQ {
        return true;
    }
    if !psi_health.is_finite() || psi_health <= INV_PHI_SQ || !btc_aligned.is_finite() {
        return false;
    }
    let w = snap.wave_observables();
    if w.climax_active || matches!(w.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion) {
        return false;
    }
    let dir_s = side.sign_f64();
    let ofi_al = snap.factor_ofi.get() * dir_s;
    let tfi_al = snap.factor_tfi.get() * dir_s;
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_bps = ((w.fib_proj_bps.raw() as f64) * dir_s) / 100.0;
    ofi_al.is_finite()
        && tfi_al.is_finite()
        && ofi_al >= INV_PHI
        && tfi_al >= INV_PHI
        && btc_aligned >= INV_PHI_SQ
        && macro_aligned > -0.75
        && fib_aligned_bps > -45.0
}

/// Continuous `C1` intraday reversal relief in `[0.0, 1.0]` attenuating stale `ImpulseWave3` headwind strictly on NEW entry (`V5.5.4`).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn compute_intraday_wave3_reversal_relief_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_aligned: f64,
    psi_health: f64,
) -> f64 {
    let aged_relief = compute_aged_impulse_wave3_entry_relief_c1(
        side,
        snap,
        btc_aligned,
        psi_health,
        0.0,
        300.0,
        0,
        50,
        FeeSchedule::VIP0_DEFAULT,
    );
    if !qualifies_clean_intraday_wave3_reversal_v552(side, snap, btc_aligned, psi_health) {
        return aged_relief;
    }
    let w = snap.wave_observables();
    let dir_s = side.sign_f64();
    let ofi_al = snap.factor_ofi.get() * dir_s;
    let tfi_al = snap.factor_tfi.get() * dir_s;
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_bps = ((w.fib_proj_bps.raw() as f64) * dir_s) / 100.0;
    let s_psi =
        crate::wave_elliott::hermite_smoothstep_c1(psi_health, INV_PHI_SQ, INV_PHI_SQ + 0.02);
    let s_ofi = crate::wave_elliott::hermite_smoothstep_c1(ofi_al, INV_PHI, INV_PHI + 0.005);
    let s_tfi = crate::wave_elliott::hermite_smoothstep_c1(tfi_al, INV_PHI, INV_PHI + 0.005);
    let s_btc = if btc_aligned.is_finite() {
        crate::wave_elliott::hermite_smoothstep_c1(btc_aligned, INV_PHI_SQ, INV_PHI_SQ + 0.005)
    } else {
        0.0
    };
    let s_macro = crate::wave_elliott::hermite_smoothstep_c1(macro_aligned, -0.75, -0.73);
    let s_fib = crate::wave_elliott::hermite_smoothstep_c1(fib_aligned_bps, -45.0, -43.0);
    (s_psi * s_ofi * s_tfi * s_btc * s_macro * s_fib)
        .max(aged_relief)
        .clamp(0.0, 1.0)
}

/// Opposing `ImpulseWave3` hazard for NEW entry admission with `C1` pivot-age / `tau_mkt` attenuation (`V5.5.4`).
#[inline]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn is_opposing_impulse_wave3_entry_hazard_v554(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_aligned: f64,
    psi_health: f64,
    pivot_age_s: f64,
    tau_mkt_s: f64,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> bool {
    let w = snap.wave_observables();
    if !matches!(w.regime, crate::wave_elliott::WaveRegime::ImpulseWave3) {
        return false;
    }
    if qualifies_clean_intraday_wave3_reversal_v552(side, snap, btc_aligned, psi_health) {
        return false;
    }
    let s_aged = compute_aged_impulse_wave3_entry_relief_c1(
        side,
        snap,
        btc_aligned,
        psi_health,
        pivot_age_s,
        tau_mkt_s,
        eq_vol_5m_sbps,
        taker_prob_pm,
        fees,
    );
    let (b3x_sbps, eq_vol_sbps) =
        endogenous_b3x_and_eq_vol_sbps(snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    let fib_cut_sbps = (PHI * (b3x_sbps as f64))
        .max(INV_PHI * (eq_vol_sbps as f64))
        * (1.0 + PHI_SQ * s_aged);
    let dir_s = side.sign_f64();
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_sbps = (w.fib_proj_bps.raw() as f64) * dir_s;
    let macro_cut = -INV_PHI - (1.0 - INV_PHI) * s_aged;
    let fib_phase_cut = -INV_PHI_SQ - (1.0 - INV_PHI_SQ) * s_aged;
    macro_aligned < macro_cut || (macro_aligned < fib_phase_cut && fib_aligned_sbps <= -fib_cut_sbps)
}

/// Opposing `ImpulseWave3` hazard strictly for NEW entry admission (`V5.5.2` / `V5.5.4`).
#[inline]
#[must_use]
pub fn is_opposing_impulse_wave3_entry_hazard_v552(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    btc_aligned: f64,
    psi_health: f64,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> bool {
    let tau_mkt_s =
        crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(snap.realized_vol_5m);
    is_opposing_impulse_wave3_entry_hazard_v554(
        side,
        snap,
        btc_aligned,
        psi_health,
        0.0,
        tau_mkt_s,
        eq_vol_5m_sbps,
        taker_prob_pm,
        fees,
    )
}

/// Continuous `C1` Order-Block alignment ratio `[0.0, 1.0]` combining Bayesian efficacy & physical `reach_bps` proximity.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn compute_ob_alignment_ratio_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    let (ob_align_pm, ob_opp_pm, own_sc, opp_sc, own_dist, opp_dist) = match side {
        crate::types::Side::Bid => (
            snap.ob_bull_efficacy_pm,
            snap.ob_bear_efficacy_pm,
            snap.ob_bull_score.get(),
            snap.ob_bear_score.get(),
            snap.ob_dist_bull_bps.raw(),
            snap.ob_dist_bear_bps.raw(),
        ),
        crate::types::Side::Ask => (
            snap.ob_bear_efficacy_pm,
            snap.ob_bull_efficacy_pm,
            snap.ob_bear_score.get(),
            snap.ob_bull_score.get(),
            snap.ob_dist_bear_bps.raw(),
            snap.ob_dist_bull_bps.raw(),
        ),
    };
    let (b3x_sbps, eq_vol_sbps) =
        endogenous_b3x_and_eq_vol_sbps(snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    let prox_div_sbps = (PHI * (eq_vol_sbps.max(b3x_sbps) as f64)).max(100.0);
    let own_eff = if own_dist > 0 && own_sc > 0.0 {
        own_sc / (1.0 + (own_dist as f64) / prox_div_sbps)
    } else {
        0.0
    };
    let opp_eff = if opp_dist > 0 && opp_sc > 0.0 {
        opp_sc / (1.0 + (opp_dist as f64) / prox_div_sbps)
    } else {
        0.0
    };
    let rho_pm = if ob_align_pm > 0 || ob_opp_pm > 0 {
        let a = (ob_align_pm.max(382)) as f64;
        let b = (ob_opp_pm.max(382)) as f64;
        (a / (a + b)).clamp(0.0, 1.0)
    } else {
        0.50
    };
    let tot_wall = own_eff + opp_eff;
    if tot_wall > INV_PHI_FOURTH {
        let rho_wall = (own_eff / tot_wall).clamp(0.0, 1.0);
        let s_opp_dom = crate::wave_elliott::hermite_smoothstep_c1(
            opp_eff - own_eff,
            INV_PHI_FOURTH,
            INV_PHI_SQ,
        );
        ((1.0 - s_opp_dom) * rho_pm.max(rho_wall) + s_opp_dom * rho_pm.min(rho_wall))
            .clamp(0.0, 1.0)
    } else {
        rho_pm
    }
}

/// Elastic anti-spoof minimum own-OFI coherence floor `Phi_ofi_min(t)` (`C1`).
/// `Phi_ofi_min(t) = INV_PHI_CUBE * (1 + INV_PHI * spoof_density + INV_PHI * p_eff) * (1 - INV_PHI_SQ * S_3(rho_ob_align; INV_PHI, 0.8541))`
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_elastic_anti_spoof_ofi_floor(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    spoof_density: f64,
    taker_prob_pm: i64,
    eq_vol_5m_sbps: i64,
    fees: FeeSchedule,
) -> f64 {
    let sp = if spoof_density.is_finite() {
        spoof_density.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let p_eff = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let rho_ob_align =
        compute_ob_alignment_ratio_c1(side, snap, eq_vol_5m_sbps, taker_prob_pm, fees);
    let s_ob = crate::wave_elliott::hermite_smoothstep_c1(
        rho_ob_align,
        INV_PHI,
        ONE_MINUS_INV_PHI_FOURTH,
    );
    INV_PHI_CUBE * (1.0 + INV_PHI * sp + INV_PHI * p_eff) * (1.0 - INV_PHI_SQ * s_ob)
}

#[inline]
#[must_use]
pub fn passes_elastic_anti_spoof_ofi_guard(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    spoof_density: f64,
    taker_prob_pm: i64,
    eq_vol_5m_sbps: i64,
    fees: FeeSchedule,
) -> bool {
    let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
    signed_ofi.is_finite()
        && signed_ofi
            >= compute_elastic_anti_spoof_ofi_floor(
                side,
                snap,
                spoof_density,
                taker_prob_pm,
                eq_vol_5m_sbps,
                fees,
            )
}

/// Continuous `C1` `CorrectiveAbc` fractal + physical `reach_bps` Order-Block threat `chi_threat` + tick-L2 Hurst (`0.65..0.80`) surface in `[0.0, 1.0]` (`V5.5.2`).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn evaluate_corrective_abc_fractal_penalty_with_health_c1_dynamic(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    _eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
    epps_corr: f64,
    btc_aligned: f64,
    psi_health: f64,
) -> f64 {
    let wave = snap.wave_observables();
    if !matches!(wave.regime, crate::wave_elliott::WaveRegime::CorrectiveAbc) {
        return 0.0;
    }
    let dir_s = side.sign_f64();
    let (ob_align_pm, ob_opp_pm) = match side {
        crate::types::Side::Bid => (snap.ob_bull_efficacy_pm, snap.ob_bear_efficacy_pm),
        crate::types::Side::Ask => (snap.ob_bear_efficacy_pm, snap.ob_bull_efficacy_pm),
    };
    let (s_opp_wall, s_own_wall) =
        compute_corrective_abc_ob_wall_pair_c1(side, snap, 0, taker_prob_pm, fees);
    let ofi_aligned = snap.factor_ofi.get() * dir_s;
    let tfi_aligned = snap.factor_tfi.get() * dir_s;
    let aligned_macro = dir_s * wave.macro_phase.get();
    let chi_threat = compute_corrective_abc_structural_threat_c1(s_opp_wall, aligned_macro);
    let s_threat_gate =
        1.0 - crate::wave_elliott::hermite_smoothstep_c1(chi_threat, INV_PHI - INV_PHI_FIFTH, INV_PHI);
    let s_iceberg_pen =
        compute_passive_iceberg_absorption_penalty_c1(ofi_aligned, tfi_aligned, btc_aligned);
    let delta_h_clear = s_threat_gate
        * compute_corrective_abc_clear_lift_c1(
            psi_health,
            ofi_aligned.min(tfi_aligned),
            chi_threat,
            s_iceberg_pen,
        );
    let delta_h_sync = s_threat_gate
        * compute_corrective_abc_epps_sync_lift_c1(
            epps_corr,
            btc_aligned,
            ofi_aligned,
            tfi_aligned,
            s_opp_wall,
        );
    let delta_h_tick = compute_corrective_abc_tick_hurst_lift_c1(
        psi_health,
        ofi_aligned,
        tfi_aligned,
        btc_aligned,
        epps_corr,
        chi_threat,
        s_iceberg_pen,
    );
    let s_sync = ((delta_h_sync + delta_h_clear + delta_h_tick) / INV_PHI_SQ).clamp(0.0, 1.0);
    let rho_ob = if ob_align_pm > 0 || ob_opp_pm > 0 {
        let a = (ob_align_pm.max(382)) as f64;
        let b = (ob_opp_pm.max(382)) as f64;
        (a / (a + b)).clamp(0.0, 1.0)
    } else {
        0.50
    };
    let s_ob = crate::wave_elliott::hermite_smoothstep_c1(rho_ob, INV_PHI_SQ, INV_PHI);
    let s_ob_eff = (s_ob * (1.0 - s_opp_wall)).max(s_own_wall);
    let s_adv_macro = crate::wave_elliott::hermite_smoothstep_c1(-aligned_macro, 0.0, INV_PHI);
    let macro_floor = -INV_PHI_CUBE * (0.50 + s_ob_eff + s_sync);
    let s_pos_m = crate::wave_elliott::hermite_smoothstep_c1(aligned_macro, INV_PHI_CUBE, INV_PHI);
    let base_h_crit = 0.50 + INV_PHI_CUBE * s_ob_eff + INV_PHI_FIFTH * s_pos_m
        - INV_PHI_CUBE * s_adv_macro
        - INV_PHI_FOURTH * s_opp_wall
        + delta_h_sync
        + delta_h_clear;
    let h_crit = (base_h_crit + delta_h_tick).min(0.80);
    let s_btc_backing = crate::wave_elliott::hermite_smoothstep_c1(
        btc_aligned,
        INV_PHI_CUBE,
        INV_PHI_CUBE + INV_PHI_FIFTH,
    ) * crate::wave_elliott::hermite_smoothstep_c1(
        epps_corr.abs(),
        INV_PHI,
        INV_PHI + INV_PHI_FIFTH,
    );
    let pen_unbacked_macro = crate::wave_elliott::hermite_smoothstep_c1(
        -INV_PHI_SQ - aligned_macro,
        -0.023_144,
        0.016_856,
    ) * (1.0 - s_btc_backing);
    let pen_macro = crate::wave_elliott::hermite_smoothstep_c1(
        macro_floor - aligned_macro,
        -0.023_144,
        0.016_856,
    );
    let pen_hurst = crate::wave_elliott::hermite_smoothstep_c1(
        wave.hurst_exponent() - h_crit,
        -0.023_144,
        0.016_856,
    );
    pen_macro
        .max(pen_unbacked_macro)
        .max(pen_hurst)
        .clamp(0.0, 1.0)
}

#[inline]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_corrective_abc_fractal_penalty_with_sync_c1_dynamic(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
    epps_corr: f64,
    btc_aligned: f64,
) -> f64 {
    evaluate_corrective_abc_fractal_penalty_with_health_c1_dynamic(
        side,
        snap,
        eq_vol_5m_sbps,
        taker_prob_pm,
        fees,
        epps_corr,
        btc_aligned,
        1.0,
    )
}

#[inline]
#[must_use]
pub fn evaluate_corrective_abc_fractal_penalty_c1_dynamic(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    eq_vol_5m_sbps: i64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> f64 {
    evaluate_corrective_abc_fractal_penalty_with_health_c1_dynamic(
        side,
        snap,
        eq_vol_5m_sbps,
        taker_prob_pm,
        fees,
        0.0,
        0.0,
        0.0,
    )
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_corrective_abc_snapshot_sync_with_health_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
    psi_health: f64,
) -> f64 {
    let wave = snap.wave_observables();
    if !matches!(
        wave.regime,
        crate::wave_elliott::WaveRegime::CorrectiveAbc
    ) {
        return 0.0;
    }
    let dir_s = side.sign_f64();
    let s_opp_wall = compute_s_opp_ob_wall_c1(side, snap, 0, 50, FeeSchedule::VIP0_DEFAULT);
    let ofi_aligned = snap.factor_ofi.get() * dir_s;
    let tfi_aligned = snap.factor_tfi.get() * dir_s;
    let chi_threat =
        compute_corrective_abc_structural_threat_c1(s_opp_wall, dir_s * wave.macro_phase.get());
    let s_threat_gate =
        1.0 - crate::wave_elliott::hermite_smoothstep_c1(chi_threat, INV_PHI - INV_PHI_FIFTH, INV_PHI);
    let s_iceberg_pen =
        compute_passive_iceberg_absorption_penalty_c1(ofi_aligned, tfi_aligned, btc_aligned);
    let delta_h_clear = s_threat_gate
        * compute_corrective_abc_clear_lift_c1(
            psi_health,
            ofi_aligned.min(tfi_aligned),
            chi_threat,
            s_iceberg_pen,
        );
    let delta_h_sync = s_threat_gate
        * compute_corrective_abc_epps_sync_lift_c1(
            epps_corr,
            btc_aligned,
            ofi_aligned,
            tfi_aligned,
            s_opp_wall,
        );
    let delta_h_tick = compute_corrective_abc_tick_hurst_lift_c1(
        psi_health,
        ofi_aligned,
        tfi_aligned,
        btc_aligned,
        epps_corr,
        chi_threat,
        s_iceberg_pen,
    );
    ((delta_h_sync + delta_h_clear + delta_h_tick) / INV_PHI_SQ).clamp(0.0, 1.0)
}

#[inline]
#[must_use]
pub fn compute_corrective_abc_snapshot_sync_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
) -> f64 {
    compute_corrective_abc_snapshot_sync_with_health_c1(side, snap, epps_corr, btc_aligned, 1.0)
}

#[inline]
#[must_use]
pub fn evaluate_corrective_abc_fractal_penalty_with_health_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
    psi_health: f64,
) -> f64 {
    evaluate_corrective_abc_fractal_penalty_with_health_c1_dynamic(
        side,
        snap,
        0,
        50,
        FeeSchedule::VIP0_DEFAULT,
        epps_corr,
        btc_aligned,
        psi_health,
    )
}

#[inline]
#[must_use]
pub fn evaluate_corrective_abc_fractal_penalty_with_sync_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
) -> f64 {
    evaluate_corrective_abc_fractal_penalty_with_health_c1(side, snap, epps_corr, btc_aligned, 1.0)
}

#[inline]
#[must_use]
pub fn evaluate_corrective_abc_fractal_penalty_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
) -> f64 {
    evaluate_corrective_abc_fractal_penalty_c1_dynamic(
        side,
        snap,
        0,
        50,
        FeeSchedule::VIP0_DEFAULT,
    )
}

#[inline]
#[must_use]
pub fn passes_corrective_abc_snapshot_guard_with_health_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
    psi_health: f64,
) -> bool {
    evaluate_corrective_abc_fractal_penalty_with_health_c1(
        side,
        snap,
        epps_corr,
        btc_aligned,
        psi_health,
    ) < 0.618_033_988_749_895
}

#[inline]
#[must_use]
pub fn passes_corrective_abc_snapshot_guard_with_sync_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
) -> bool {
    passes_corrective_abc_snapshot_guard_with_health_c1(side, snap, epps_corr, btc_aligned, 1.0)
}

#[inline]
#[must_use]
pub fn passes_corrective_abc_snapshot_guard_c1(
    side: crate::types::Side,
    snap: &crate::lob_shm::SymbolLobSnapshot,
) -> bool {
    passes_corrective_abc_snapshot_guard_with_health_c1(side, snap, 0.0, 0.0, 0.0)
}

#[cfg(test)]
mod v51_f5_abc_fractal_and_1lot_ev_tests {
    use super::*;
    use crate::types::{passes_asymmetry_ev_gate_v51, APPROVED_SPECS};
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_v51_f5_blocks_high_hurst_abc_losses_and_artificial_1lot_ev() {
        let w_avax3 = WaveObservables {
            regime: WaveRegime::CorrectiveAbc, climax_active: false, hurst: WaveObservables::encode_hurst(0.674),
            macro_phase: BoundedScore::new_clamped(-0.10), oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
        };
        assert!(!passes_corrective_abc_fractal_guard_c1(Side::Ask, w_avax3, 490, 510));
        let w_ob_backed = WaveObservables { hurst: WaveObservables::encode_hurst(0.575), macro_phase: BoundedScore::new_clamped(0.35), ..w_avax3 };
        assert!(passes_corrective_abc_fractal_guard_c1(Side::Bid, w_ob_backed, 780, 420));
        let eth = APPROVED_SPECS[1];
        assert!(!passes_asymmetry_ev_gate_v51(eth, PriceTicks(179_961), LotQty(1), ScaledBps(560), ScaledBps(580), ScaledBps(1_850), 50, BoundedScore::new_clamped(0.55), FeeSchedule::VIP0_DEFAULT));
        assert!(passes_asymmetry_ev_gate_v51(eth, PriceTicks(182_033), LotQty(1), ScaledBps(920), ScaledBps(820), ScaledBps(1_600), 45, BoundedScore::new_clamped(0.95), FeeSchedule::VIP0_DEFAULT));
    }
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn should_abort_l1_cancellation_collapse_v51(
    spec: PairSpec,
    side: Side,
    prev_l1_qty: LotQty,
    cur_l1_qty: LotQty,
    opp_trade_lots: LotQty,
    bbo_px: PriceTicks,
    signed_flow_slow: BoundedScore,
    fees: FeeSchedule,
) -> bool {
    const INV_PHI_Q: f64 = 0.618_033_988_749_895;
    const INV_PHI2_Q: f64 = 0.381_966_011_250_105;
    const INV_PHI3_Q: f64 = 0.236_067_977_499_79;
    const INV_PHI5_Q: f64 = 0.090_169_943_749_47;
    let _ = side;
    if bbo_px.0 <= 0 || prev_l1_qty.0 <= cur_l1_qty.0 || prev_l1_qty.0 <= 0 {
        return false;
    }
    let drop_lots = prev_l1_qty.0.saturating_sub(cur_l1_qty.0.max(0));
    let consumed_by_trades = opp_trade_lots.0.max(0).min(drop_lots);
    let pure_cancel_lots = drop_lots.saturating_sub(consumed_by_trades);
    if pure_cancel_lots <= 0 {
        return false;
    }
    let rho_cancel = (pure_cancel_lots as f64) / (prev_l1_qty.0.max(1) as f64);
    let fl = signed_flow_slow.get();
    let s_fav = crate::wave_elliott::hermite_smoothstep_c1(fl, 0.0, INV_PHI_Q);
    let s_adv = crate::wave_elliott::hermite_smoothstep_c1(-fl, 0.0, INV_PHI_Q);
    let cancel_thresh = INV_PHI2_Q * (1.0 + INV_PHI_Q * s_fav - INV_PHI2_Q * s_adv);
    if rho_cancel < cancel_thresh {
        return false;
    }
    let min_l1 = spec
        .min_resting_l1_lots_dynamic(bbo_px, ScaledBps::MIN_REQUIRED_VOL_5M_BPS, 50, fees)
        .0
        .max(1);
    let wall_thin = cur_l1_qty.0 < min_l1 || cur_l1_qty.0.saturating_mul(1_000) < prev_l1_qty.0.saturating_mul(618);
    let s_sev = crate::wave_elliott::hermite_smoothstep_c1(rho_cancel, INV_PHI2_Q, 0.854_101_966_249_685);
    let flow_ceil = -INV_PHI5_Q + INV_PHI3_Q * s_sev;
    wall_thin && fl < flow_ceil
}

#[cfg(test)]
mod v51_f4_l1_depletion_vs_cancel_tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_v51_f4_separates_fifo_trade_consumption_from_whale_cancellation() {
        let tao = APPROVED_SPECS[4];
        let fees = FeeSchedule::VIP0_DEFAULT;
        assert!(!should_abort_l1_cancellation_collapse_v51(
            tao, Side::Bid, LotQty(797), LotQty(118), LotQty(650),
            PriceTicks(19_505), BoundedScore::new_clamped(0.42), fees,
        ));
        assert!(should_abort_l1_cancellation_collapse_v51(
            tao, Side::Bid, LotQty(797), LotQty(118), LotQty(12),
            PriceTicks(19_505), BoundedScore::new_clamped(-0.18), fees,
        ));
    }

    #[test]
    fn test_h1_endogenous_dirichlet_laplace_tfi_prior_spoof_immunity_and_reset() {
        let mut st = OrthogonalFactorState::new();
        let empty = SymbolLobSnapshot::new_empty();
        assert!((st.tfi_endogenous_prior_lots(&empty, 1.0) - 1e-6).abs() < 1e-12);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        for i in 0..5 {
            snap.bids[i] = BookLevel { price: PriceTicks(8_200 - i as i64), qty: LotQty(1_200) };
            snap.asks[i] = BookLevel { price: PriceTicks(8_201 + i as i64), qty: LotQty(1_200) };
        }
        st.on_l2_book_update(&mut snap);
        assert!(st.learned_l1_depth_lots >= 1_200.0);
        // Dust 1-lot print on deep L1 book must not trigger a spurious TFI spike
        st.on_trade_print(Side::Bid, LotQty(1), &mut snap);
        assert!(snap.factor_tfi.get().abs() < 0.05);

        // One-sided spoof wall on Bid must not inflate learned_l1_depth_lots past thin Ask BBO
        snap.ts_local_ns = TimestampNs(5_000_000_000);
        snap.bids[0].qty = LotQty(80_000);
        snap.asks[0].qty = LotQty(900);
        st.on_l2_book_update(&mut snap);
        assert!(st.learned_l1_depth_lots <= 1_200.0);

        st.on_trade_print(Side::Bid, LotQty(600), &mut snap);
        snap.ts_local_ns = TimestampNs(15_000_000_000);
        st.on_l2_book_update(&mut snap);
        let tr_saved = st.learned_trade_lots;
        let fl_saved = st.learned_flow_lots;
        assert!(fl_saved > 0.0);
        st.reset_l2_book_only();
        assert!(st.learned_l1_depth_lots.abs() < 1e-12);
        assert!((st.learned_trade_lots - tr_saved).abs() < 1e-12);
        assert!((st.learned_flow_lots - fl_saved).abs() < 1e-12);
        st.reset();
        assert!(st.learned_trade_lots.abs() < 1e-12);
        assert!(st.learned_flow_lots.abs() < 1e-12);

        // 2-lot retail print vs 500-lot L1 stays < 0.15, while deep 25k-lot L1 book does not crush real flow
        let mut st_ret = OrthogonalFactorState::new();
        let mut s_ret = SymbolLobSnapshot::new_empty();
        s_ret.symbol_id = APPROVED_SPECS[3].symbol_id;
        s_ret.ts_local_ns = TimestampNs(1_000_000_000);
        s_ret.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(500) };
        s_ret.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(500) };
        st_ret.on_l2_book_update(&mut s_ret);
        st_ret.on_trade_print(Side::Bid, LotQty(2), &mut s_ret);
        assert!(s_ret.factor_tfi.get() < 0.15);

        let mut st_deep = OrthogonalFactorState::new();
        let mut s_deep = SymbolLobSnapshot::new_empty();
        s_deep.symbol_id = APPROVED_SPECS[8].symbol_id; // WIF
        s_deep.ts_local_ns = TimestampNs(1_000_000_000);
        s_deep.bids[0] = BookLevel { price: PriceTicks(1_600), qty: LotQty(25_000) };
        s_deep.asks[0] = BookLevel { price: PriceTicks(1_601), qty: LotQty(25_000) };
        st_deep.on_l2_book_update(&mut s_deep);
        for _ in 0..4 {
            st_deep.on_trade_print(Side::Bid, LotQty(80), &mut s_deep);
        }
        assert!(s_deep.factor_tfi.get() > 0.20);
    }

    #[test]
    fn test_v554_c1_friction_diffusive_preprint_aged_w3_and_abc_synergy() {
        let fees = FeeSchedule::VIP0_DEFAULT;
        assert!(passes_intrinsic_diffusive_reachability_c1(ScaledBps(600), 485.0, fees));
        assert!(!passes_intrinsic_diffusive_reachability_c1(ScaledBps(450), 300.0, fees));

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.realized_vol_5m = ScaledBps(2_200);
        btc.bids[0] = BookLevel { price: PriceTicks(680_000), qty: LotQty(500) };
        btc.asks[0] = BookLevel { price: PriceTicks(680_001), qty: LotQty(500) };
        btc.factor_ofi = BoundedScore::new_clamped(0.72);
        btc.factor_tfi = BoundedScore::new_clamped(0.65);
        btc.factor_geom = BoundedScore::new_clamped(0.55);

        let mut eth = SymbolLobSnapshot::new_empty();
        eth.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth.realized_vol_5m = ScaledBps(1_800);
        eth.bids[0] = BookLevel { price: PriceTicks(180_000), qty: LotQty(800) };
        eth.asks[0] = BookLevel { price: PriceTicks(180_001), qty: LotQty(800) };
        eth.factor_ofi = BoundedScore::new_clamped(0.68);
        eth.factor_tfi = BoundedScore::new_clamped(0.02);
        eth.factor_geom = BoundedScore::new_clamped(0.48);
        assert!(matches!(
            OrthogonalFactorState::evaluate_entry_signal_v554(
                APPROVED_SPECS[1], &eth, &btc, ScaledBps(850), ScaledBps(220),
                45, 75, 360.0, fees, 0.05, 1.0, 0.80, None, 1.0, 0.0, PHI,
            ),
            SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }
        ));
        eth.factor_tfi = BoundedScore::new_clamped(-0.04);
        assert_eq!(
            OrthogonalFactorState::evaluate_entry_signal_v554(
                APPROVED_SPECS[1], &eth, &btc, ScaledBps(850), ScaledBps(220),
                45, 75, 360.0, fees, 0.05, 1.0, 0.80, None, 1.0, 0.0, PHI,
            ),
            SignalVerdict::SpoofingVeto
        );

        // Continuous CorrectiveAbc synergy without |epps_corr| >= INV_PHI step + s_opp_ob_wall == 1.0 when ob_own == 0
        let lift_low_corr =
            compute_corrective_abc_tick_hurst_lift_c1(1.0, 0.72, 0.70, 0.10, 0.15, 0.10, 0.0);
        assert!(lift_low_corr > 0.10);
        let mut s_ob_opp = eth;
        s_ob_opp.ob_bull_score = BoundedScore::ZERO;
        s_ob_opp.ob_dist_bull_bps = ScaledBps(0);
        s_ob_opp.ob_bear_score = BoundedScore::new_clamped(0.85);
        s_ob_opp.ob_dist_bear_bps = ScaledBps(800);
        assert!((compute_s_opp_ob_wall_c1(Side::Bid, &s_ob_opp, 0, 50, fees) - 1.0).abs() < 1e-12);

        // Aged ImpulseWave3 C1 relief with own [OB+, OB-] support and min(ofi_al, tfi_al) >= INV_PHI_SQ
        let mut s_w3 = eth;
        s_w3.factor_ofi = BoundedScore::new_clamped(0.55);
        s_w3.factor_tfi = BoundedScore::new_clamped(0.52);
        s_w3.ob_bull_efficacy_pm = 720;
        s_w3.ob_bear_efficacy_pm = 430;
        s_w3.set_wave_observables(crate::wave_elliott::WaveObservables {
            regime: crate::wave_elliott::WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.56),
            macro_phase: BoundedScore::new_clamped(-0.65),
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(-1_800),
        });
        assert!(!is_opposing_impulse_wave3_entry_hazard_v554(
            Side::Bid, &s_w3, 0.30, 1.0, 500.0, 300.0, 1_800, 50, fees,
        ));
    }
}
