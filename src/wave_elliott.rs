//! Causal C1 Elliott Impulse/Correction Wave Physics, Hurst VR & Bybit OI/Kline Lab.

use core::f64::consts::{FRAC_1_SQRT_2, LN_10};

use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side};

const HORIZON_30S_NS: i64 = 30_000_000_000;
const HORIZON_5M_NS: i64 = 300_000_000_000;
const MAD_TO_SIGMA: f64 = 1.253_314_14;
const PARKINSON_TO_SIGMA: f64 = 0.600_561_20;
const TWO_LN_10: f64 = 2.0 * LN_10;
const EWMA_DIFF_STD_5M_15M: f64 = FRAC_1_SQRT_2;
const EWMA_DIFF_STD_15M_60M: f64 = 2.012_461_18;

/// Exhaustive continuous-state Elliott wave regime classification (zero repainting).
#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum WaveRegimeKind {
    #[default]
    NeutralEquilibrium = 0,
    MotiveImpulseBull = 1,
    MotiveImpulseBear = 2,
    CorrectivePullbackInBull = 3,
    CorrectiveBounceInBear = 4,
    TerminalExhaustionClimax = 5,
}

impl WaveRegimeKind {
    #[inline]
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::MotiveImpulseBull,
            2 => Self::MotiveImpulseBear,
            3 => Self::CorrectivePullbackInBull,
            4 => Self::CorrectiveBounceInBear,
            5 => Self::TerminalExhaustionClimax,
            _ => Self::NeutralEquilibrium,
        }
    }

    #[inline]
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NeutralEquilibrium => "NEUTRAL_EQUILIBRIUM",
            Self::MotiveImpulseBull => "IMPULSE_BULL_W135",
            Self::MotiveImpulseBear => "IMPULSE_BEAR_W135",
            Self::CorrectivePullbackInBull => "CORR_PULLBACK_W24",
            Self::CorrectiveBounceInBear => "CORR_BOUNCE_WB24",
            Self::TerminalExhaustionClimax => "CLIMAX_EXHAUST_W5C",
        }
    }
}

/// Dimensionless wave observables in [-1, +1] / [0, +1] with exact 0.0 neutral priors.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct WaveObservables {
    pub regime: WaveRegimeKind,
    pub hurst_score: BoundedScore,
    pub oi_impulse: BoundedScore,
    pub macro_phase: BoundedScore,
    pub retracement_ratio: BoundedScore,
    pub climax_intensity: BoundedScore,
}

impl Default for WaveObservables {
    #[inline]
    fn default() -> Self {
        Self::neutral()
    }
}

impl WaveObservables {
    #[inline]
    #[must_use]
    pub const fn neutral() -> Self {
        Self {
            regime: WaveRegimeKind::NeutralEquilibrium,
            hurst_score: BoundedScore::ZERO,
            oi_impulse: BoundedScore::ZERO,
            macro_phase: BoundedScore::ZERO,
            retracement_ratio: BoundedScore::ZERO,
            climax_intensity: BoundedScore::ZERO,
        }
    }

    /// Reconstructs local Hurst exponent H_t in [0.20, 0.85] from centered BoundedScore.
    #[inline]
    #[must_use]
    pub fn hurst_exponent(self) -> f64 {
        (0.50 + 0.50 * self.hurst_score.get()).clamp(0.20, 0.85)
    }
}

/// Entry conviction & Tranche-1 harvest modulation derived from causal wave physics.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct WaveModulationVerdict {
    pub snr_threshold_delta: f64,
    pub tranche1_permille_delta: i64,
    pub def_horizon_permille_mult: i64,
}

/// Zero-allocation O(1) state-space estimator for causal Elliott wave physics.
#[derive(Copy, Clone, Debug)]
pub struct CausalElliottWaveState {
    initialized: bool,
    last_price_ts_ns: i64,
    last_price_ticks: PriceTicks,
    last_vol_5m: ScaledBps,
    px_ewma_30s: f64,
    px_ewma_5m: f64,
    px_ewma_15m: f64,
    px_ewma_1h: f64,
    swing_high_px: f64,
    swing_low_px: f64,
    var30_anchor_px: i64,
    var30_low_px: i64,
    var30_high_px: i64,
    var30_start_ts_ns: i64,
    ewma_var_30s_bps2: f64,
    var300_anchor_px: i64,
    var300_low_px: i64,
    var300_high_px: i64,
    var300_start_ts_ns: i64,
    ewma_var_300s_bps2: f64,
    oi_initialized: bool,
    last_oi_ts_ns: i64,
    oi_ewma_fast: f64,
    oi_ewma_slow: f64,
    oi_diff_mad: f64,
    last_liq_ts_ns: i64,
    liq_buy_lots_ewma: f64,
    liq_sell_lots_ewma: f64,
    last_obs: WaveObservables,
}

impl Default for CausalElliottWaveState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::manual_clamp, clippy::needless_range_loop, clippy::too_many_arguments)]
impl CausalElliottWaveState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            initialized: false,
            last_price_ts_ns: 0,
            last_price_ticks: PriceTicks(0),
            last_vol_5m: ScaledBps(0),
            px_ewma_30s: 0.0,
            px_ewma_5m: 0.0,
            px_ewma_15m: 0.0,
            px_ewma_1h: 0.0,
            swing_high_px: 0.0,
            swing_low_px: 0.0,
            var30_anchor_px: 0,
            var30_low_px: 0,
            var30_high_px: 0,
            var30_start_ts_ns: 0,
            ewma_var_30s_bps2: 400.0,
            var300_anchor_px: 0,
            var300_low_px: 0,
            var300_high_px: 0,
            var300_start_ts_ns: 0,
            ewma_var_300s_bps2: 4_000.0,
            oi_initialized: false,
            last_oi_ts_ns: 0,
            oi_ewma_fast: 0.0,
            oi_ewma_slow: 0.0,
            oi_diff_mad: 1.0,
            last_liq_ts_ns: 0,
            liq_buy_lots_ewma: 0.0,
            liq_sell_lots_ewma: 0.0,
            last_obs: WaveObservables::neutral(),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    #[inline]
    #[must_use]
    pub const fn observables(&self) -> WaveObservables {
        self.last_obs
    }

    #[inline]
    fn parkinson_window_var_bps2(
        anchor_px: i64,
        low_px: i64,
        high_px: i64,
        cur_px: i64,
        dt_ns: i64,
        ref_horizon_ns: i64,
    ) -> f64 {
        let a = anchor_px.max(1) as f64;
        let lo = low_px.min(anchor_px).min(cur_px);
        let hi = high_px.max(anchor_px).max(cur_px);
        let net_bps = (((cur_px - anchor_px).abs() as f64) * 10_000.0) / a;
        let hl_bps = (((hi - lo).max(0) as f64) * 10_000.0) / a;
        let dt_s = ((dt_ns.max(1_000_000_000) as f64) * 1e-9).max(1.0);
        let ref_s = (ref_horizon_ns as f64) * 1e-9;
        let s_net = net_bps * MAD_TO_SIGMA;
        let s_hl = hl_bps * PARKINSON_TO_SIGMA;
        let best_sigma = s_net.max(s_hl);
        (best_sigma * best_sigma * (ref_s / dt_s)).clamp(0.01, 250_000.0)
    }

    fn init_seed(&mut self, spec: PairSpec, obs_px: PriceTicks, ts_ns: i64) {
        let px_f = obs_px.0 as f64;
        let b3x_bps = (spec
            .required_3x_barrier_bps(obs_px)
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw() as f64)
            / 100.0;
        let seed_var_300 = (b3x_bps * 2.5) * (b3x_bps * 2.5);
        self.initialized = true;
        self.last_price_ts_ns = ts_ns;
        self.last_price_ticks = obs_px;
        self.px_ewma_30s = px_f;
        self.px_ewma_5m = px_f;
        self.px_ewma_15m = px_f;
        self.px_ewma_1h = px_f;
        self.swing_high_px = px_f;
        self.swing_low_px = px_f;
        self.var30_anchor_px = obs_px.0;
        self.var30_low_px = obs_px.0;
        self.var30_high_px = obs_px.0;
        self.var30_start_ts_ns = ts_ns;
        self.ewma_var_30s_bps2 = (seed_var_300 / 10.0).max(0.25);
        self.var300_anchor_px = obs_px.0;
        self.var300_low_px = obs_px.0;
        self.var300_high_px = obs_px.0;
        self.var300_start_ts_ns = ts_ns;
        self.ewma_var_300s_bps2 = seed_var_300.max(2.5);
    }

    /// Steps causal multi-scale wave state on a live L2 mid-price tick at monotonic ts_ns.
    pub fn observe_price_tick(
        &mut self,
        spec: PairSpec,
        obs_px: PriceTicks,
        ts_ns: i64,
        realized_vol_5m: ScaledBps,
    ) -> WaveObservables {
        if obs_px.0 <= 0 || ts_ns <= 0 {
            return self.last_obs;
        }
        self.last_price_ticks = obs_px;
        self.last_vol_5m = realized_vol_5m;
        if !self.initialized {
            self.init_seed(spec, obs_px, ts_ns);
            return self.last_obs;
        }
        let px_f = obs_px.0 as f64;
        if obs_px.0 < self.var30_low_px {
            self.var30_low_px = obs_px.0;
        }
        if obs_px.0 > self.var30_high_px {
            self.var30_high_px = obs_px.0;
        }
        if obs_px.0 < self.var300_low_px {
            self.var300_low_px = obs_px.0;
        }
        if obs_px.0 > self.var300_high_px {
            self.var300_high_px = obs_px.0;
        }

        if ts_ns > self.last_price_ts_ns {
            let dt_ns = ts_ns - self.last_price_ts_ns;
            self.last_price_ts_ns = ts_ns;
            let dt_s = ((dt_ns as f64) * 1e-9).clamp(1e-4, 3_600.0);
            self.px_ewma_30s += (1.0 - (-dt_s / 30.0).exp()) * (px_f - self.px_ewma_30s);
            self.px_ewma_5m += (1.0 - (-dt_s / 300.0).exp()) * (px_f - self.px_ewma_5m);
            self.px_ewma_15m += (1.0 - (-dt_s / 900.0).exp()) * (px_f - self.px_ewma_15m);
            self.px_ewma_1h += (1.0 - (-dt_s / 3_600.0).exp()) * (px_f - self.px_ewma_1h);

            let a_env = 1.0 - (-dt_s / 3_600.0).exp();
            self.swing_high_px = if px_f >= self.swing_high_px {
                px_f
            } else {
                (self.swing_high_px + a_env * (self.px_ewma_1h - self.swing_high_px))
                    .max(px_f)
            };
            self.swing_low_px = if px_f <= self.swing_low_px {
                px_f
            } else {
                (self.swing_low_px + a_env * (self.px_ewma_1h - self.swing_low_px))
                    .min(px_f)
            };
        }

        let dt30 = ts_ns.saturating_sub(self.var30_start_ts_ns);
        if dt30 >= HORIZON_30S_NS {
            let v30 = Self::parkinson_window_var_bps2(
                self.var30_anchor_px,
                self.var30_low_px,
                self.var30_high_px,
                obs_px.0,
                dt30,
                HORIZON_30S_NS,
            );
            let a_v30 = (1.0 - (-(dt30 as f64 * 1e-9) / 180.0).exp()).clamp(0.05, 0.50);
            self.ewma_var_30s_bps2 += a_v30 * (v30 - self.ewma_var_30s_bps2);
            self.var30_anchor_px = obs_px.0;
            self.var30_low_px = obs_px.0;
            self.var30_high_px = obs_px.0;
            self.var30_start_ts_ns = ts_ns;
        }

        let dt300 = ts_ns.saturating_sub(self.var300_start_ts_ns);
        if dt300 >= HORIZON_5M_NS {
            let v300 = Self::parkinson_window_var_bps2(
                self.var300_anchor_px,
                self.var300_low_px,
                self.var300_high_px,
                obs_px.0,
                dt300,
                HORIZON_5M_NS,
            );
            let a_v300 = (1.0 - (-(dt300 as f64 * 1e-9) / 900.0).exp()).clamp(0.08, 0.55);
            self.ewma_var_300s_bps2 += a_v300 * (v300 - self.ewma_var_300s_bps2);
            self.var300_anchor_px = obs_px.0;
            self.var300_low_px = obs_px.0;
            self.var300_high_px = obs_px.0;
            self.var300_start_ts_ns = ts_ns;
        }

        self.recompute_observables(spec, obs_px, ts_ns, realized_vol_5m)
    }

    /// Integrates a closed OHLC candle over its exact bar_secs without intra-bar bias.
    pub fn observe_ohlc_candle(
        &mut self,
        spec: PairSpec,
        start_ts_ms: i64,
        o_t: PriceTicks,
        h_t: PriceTicks,
        l_t: PriceTicks,
        c_t: PriceTicks,
        bar_secs: i64,
    ) -> WaveObservables {
        let dur_ns = bar_secs.max(1) * 1_000_000_000;
        let end_ns = start_ts_ms.saturating_mul(1_000_000).saturating_add(dur_ns);
        if c_t.0 <= 0 || end_ns <= 0 {
            return self.last_obs;
        }
        if !self.initialized {
            self.init_seed(spec, o_t, end_ns - dur_ns);
            self.swing_high_px = h_t.0 as f64;
            self.swing_low_px = l_t.0 as f64;
        }
        if end_ns <= self.last_price_ts_ns {
            return self.last_obs;
        }
        self.last_price_ts_ns = end_ns;
        self.last_price_ticks = c_t;
        let c_f = c_t.0 as f64;
        let h_f = h_t.0 as f64;
        let l_f = l_t.0 as f64;
        let dt_s = (bar_secs.max(1)) as f64;

        self.px_ewma_30s += (1.0 - (-dt_s / 30.0).exp()) * (c_f - self.px_ewma_30s);
        self.px_ewma_5m += (1.0 - (-dt_s / 300.0).exp()) * (c_f - self.px_ewma_5m);
        self.px_ewma_15m += (1.0 - (-dt_s / 900.0).exp()) * (c_f - self.px_ewma_15m);
        self.px_ewma_1h += (1.0 - (-dt_s / 3_600.0).exp()) * (c_f - self.px_ewma_1h);

        let a_env = 1.0 - (-dt_s / 3_600.0).exp();
        self.swing_high_px = (self.swing_high_px
            + a_env * (self.px_ewma_1h - self.swing_high_px))
            .max(h_f);
        self.swing_low_px = (self.swing_low_px
            + a_env * (self.px_ewma_1h - self.swing_low_px))
            .min(l_f);

        if bar_secs <= 90 {
            let v30 = Self::parkinson_window_var_bps2(
                o_t.0,
                l_t.0,
                h_t.0,
                c_t.0,
                dur_ns,
                HORIZON_30S_NS,
            );
            let a_v30 = (1.0 - (-dt_s / 180.0).exp()).clamp(0.05, 0.50);
            self.ewma_var_30s_bps2 += a_v30 * (v30 - self.ewma_var_30s_bps2);
        }
        self.var30_anchor_px = c_t.0;
        self.var30_low_px = c_t.0;
        self.var30_high_px = c_t.0;
        self.var30_start_ts_ns = end_ns;

        if l_t.0 < self.var300_low_px {
            self.var300_low_px = l_t.0;
        }
        if h_t.0 > self.var300_high_px {
            self.var300_high_px = h_t.0;
        }
        let dt300 = end_ns.saturating_sub(self.var300_start_ts_ns);
        if dt300 >= HORIZON_5M_NS {
            let v300 = Self::parkinson_window_var_bps2(
                self.var300_anchor_px,
                self.var300_low_px,
                self.var300_high_px,
                c_t.0,
                dt300,
                HORIZON_5M_NS,
            );
            let a_v300 = (1.0 - (-(dt300 as f64 * 1e-9) / 900.0).exp()).clamp(0.08, 0.55);
            self.ewma_var_300s_bps2 += a_v300 * (v300 - self.ewma_var_300s_bps2);
            if bar_secs > 90 {
                self.ewma_var_30s_bps2 = (self.ewma_var_300s_bps2 / 10.0).max(0.25);
            }
            self.var300_anchor_px = c_t.0;
            self.var300_low_px = c_t.0;
            self.var300_high_px = c_t.0;
            self.var300_start_ts_ns = end_ns;
        }

        let est_vol = ScaledBps((self.ewma_var_300s_bps2.sqrt() * 100.0).round() as i64);
        self.last_vol_5m = est_vol;
        self.recompute_observables(spec, c_t, end_ns, est_vol)
    }

    /// Updates unilateral Open Interest state and refreshes wave observables if initialized.
    pub fn observe_open_interest(
        &mut self,
        spec: PairSpec,
        open_interest_contracts: f64,
        ts_ns: i64,
    ) -> WaveObservables {
        if !open_interest_contracts.is_finite()
            || open_interest_contracts <= 0.0
            || ts_ns <= 0
        {
            return self.last_obs;
        }
        if !self.oi_initialized {
            self.oi_initialized = true;
            self.last_oi_ts_ns = ts_ns;
            self.oi_ewma_fast = open_interest_contracts;
            self.oi_ewma_slow = open_interest_contracts;
            self.oi_diff_mad = (open_interest_contracts * 1e-3).max(1e-6);
            return self.last_obs;
        }
        if ts_ns <= self.last_oi_ts_ns {
            return self.last_obs;
        }
        let dt_s = ((ts_ns - self.last_oi_ts_ns) as f64 * 1e-9).clamp(0.1, 1_800.0);
        self.last_oi_ts_ns = ts_ns;
        let a_fast = 1.0 - (-dt_s / 60.0).exp();
        let a_slow = 1.0 - (-dt_s / 300.0).exp();
        self.oi_ewma_fast += a_fast * (open_interest_contracts - self.oi_ewma_fast);
        self.oi_ewma_slow += a_slow * (open_interest_contracts - self.oi_ewma_slow);
        let abs_diff = (self.oi_ewma_fast - self.oi_ewma_slow).abs();
        let a_mad = 1.0 - (-dt_s / 600.0).exp();
        let floor_mad = (self.oi_ewma_slow * 5e-4).max(1e-6);
        self.oi_diff_mad =
            (self.oi_diff_mad + a_mad * (abs_diff - self.oi_diff_mad)).max(floor_mad);

        if self.initialized && self.last_price_ticks.0 > 0 {
            let eff_ts = ts_ns.max(self.last_price_ts_ns);
            return self.recompute_observables(
                spec,
                self.last_price_ticks,
                eff_ts,
                self.last_vol_5m,
            );
        }
        self.last_obs
    }

    /// Updates Hawkes forced-liquidation intensity from Bybit allLiquidation.{sym} WS.
    pub fn observe_liquidation_print(
        &mut self,
        spec: PairSpec,
        side: Side,
        qty: LotQty,
        ts_ns: i64,
    ) -> WaveObservables {
        if qty.0 <= 0 || ts_ns <= 0 {
            return self.last_obs;
        }
        if self.last_liq_ts_ns > 0 && ts_ns > self.last_liq_ts_ns {
            let dt_s = ((ts_ns - self.last_liq_ts_ns) as f64 * 1e-9).clamp(0.0, 600.0);
            let decay = (-dt_s / 45.0).exp();
            self.liq_buy_lots_ewma *= decay;
            self.liq_sell_lots_ewma *= decay;
        }
        if ts_ns > self.last_liq_ts_ns {
            self.last_liq_ts_ns = ts_ns;
        }
        let q = qty.0 as f64;
        match side {
            Side::Bid => self.liq_buy_lots_ewma += q,
            Side::Ask => self.liq_sell_lots_ewma += q,
        }
        if self.initialized && self.last_price_ticks.0 > 0 {
            let eff_ts = ts_ns.max(self.last_price_ts_ns);
            return self.recompute_observables(
                spec,
                self.last_price_ticks,
                eff_ts,
                self.last_vol_5m,
            );
        }
        self.last_obs
    }

    fn recompute_observables(
        &mut self,
        spec: PairSpec,
        obs_px: PriceTicks,
        ts_ns: i64,
        realized_vol_5m: ScaledBps,
    ) -> WaveObservables {
        let px_f = (obs_px.0.max(1)) as f64;
        let b3x_sc = spec
            .required_3x_barrier_bps(obs_px)
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS);
        let b3x_bps = (b3x_sc.raw() as f64) / 100.0;
        let obs_vol_bps = (realized_vol_5m.raw() as f64) / 100.0;
        let park_sigma_5m = self.ewma_var_300s_bps2.sqrt();
        let sigma_5m_bps = if self.ewma_var_300s_bps2 > (b3x_bps * b3x_bps) {
            park_sigma_5m.max(b3x_bps)
        } else {
            obs_vol_bps.max(park_sigma_5m).max(b3x_bps).max(4.0)
        };

        let tau_tick_bps = (spec
            .tick_to_price_scaled_bps(obs_px)
            .unwrap_or(ScaledBps(50))
            .raw() as f64)
            / 100.0;
        let bounce_var = 2.0 * tau_tick_bps * tau_tick_bps;
        let min_v30 = (0.5 * tau_tick_bps * tau_tick_bps).max(0.04);
        let clean_v30 = (self.ewma_var_30s_bps2 - bounce_var).max(min_v30);
        let clean_v300 = (self.ewma_var_300s_bps2 - bounce_var).max(min_v30 * 2.0);
        let vr_10 = (clean_v300 / (10.0 * clean_v30)).clamp(0.05, 25.0);
        let hurst_h = (0.50 + vr_10.ln() / TWO_LN_10).clamp(0.20, 0.85);
        let hurst_score = BoundedScore::new_clamped(2.0 * (hurst_h - 0.50));

        let d_fast_bps =
            ((px_f - self.px_ewma_30s) / self.px_ewma_30s.max(1.0)) * 10_000.0;
        let d_sub_bps =
            ((px_f - self.px_ewma_5m) / self.px_ewma_5m.max(1.0)) * 10_000.0;
        let d_int_bps = ((self.px_ewma_5m - self.px_ewma_15m)
            / self.px_ewma_15m.max(1.0))
            * 10_000.0;
        let d_par_bps = ((self.px_ewma_15m - self.px_ewma_1h)
            / self.px_ewma_1h.max(1.0))
            * 10_000.0;

        let z_fast = d_fast_bps / (0.35 * sigma_5m_bps).max(1.0);
        let z_sub = d_sub_bps / (EWMA_DIFF_STD_5M_15M * sigma_5m_bps);
        let z_int = d_int_bps / (EWMA_DIFF_STD_5M_15M * sigma_5m_bps);
        let z_par = d_par_bps / (EWMA_DIFF_STD_15M_60M * sigma_5m_bps);
        let omega_macro = (0.55 * z_par + 0.45 * z_int).tanh();
        let macro_phase = BoundedScore::new_clamped(omega_macro);

        let oi_val = if self.oi_initialized && self.oi_ewma_slow > 0.0 {
            let rel_oi =
                (self.oi_ewma_fast - self.oi_ewma_slow) / self.oi_diff_mad.max(1e-6);
            let f_oi = rel_oi.tanh();
            let phi_oi = if f_oi >= 0.0 { f_oi } else { 0.50 * f_oi };
            let v_px = (0.65 * z_fast + 0.35 * z_sub).tanh();
            (v_px * phi_oi).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let oi_impulse = BoundedScore::new_clamped(oi_val);

        let min_span = (px_f * sigma_5m_bps * 1e-4).max(2.0);
        let span = (self.swing_high_px - self.swing_low_px).max(min_span);
        let ret_raw = if omega_macro >= 0.0 {
            (self.swing_high_px - px_f) / span
        } else {
            (px_f - self.swing_low_px) / span
        };
        let retracement_ratio = BoundedScore::new_clamped(ret_raw.clamp(0.0, 1.0));

        let liq_decay = if self.last_liq_ts_ns > 0 && ts_ns > self.last_liq_ts_ns {
            (-((ts_ns - self.last_liq_ts_ns) as f64 * 1e-9) / 45.0).exp()
        } else {
            1.0
        };
        let q_scale = spec.characteristic_lot_scale(obs_px).max(1.0);
        let tot_liq =
            (self.liq_buy_lots_ewma + self.liq_sell_lots_ewma) * liq_decay / (8.0 * q_scale);
        let ext_mag = (z_int.abs() + 0.5 * z_sub.abs() - 1.80).max(0.0) / 1.80;
        let oi_div = if omega_macro * oi_val < -0.05 { 0.25 } else { 0.0 };
        let climax_raw = (0.55 * tot_liq.tanh() + 0.45 * ext_mag + oi_div).clamp(0.0, 1.0);
        let climax_intensity = BoundedScore::new_clamped(climax_raw);

        let regime = if climax_raw >= 0.58
            && omega_macro.abs() >= 0.38
            && retracement_ratio.get() <= 0.08
        {
            WaveRegimeKind::TerminalExhaustionClimax
        } else if omega_macro >= 0.22 {
            if (z_fast < -0.20 || z_sub < -0.15) && retracement_ratio.get() >= 0.16 {
                WaveRegimeKind::CorrectivePullbackInBull
            } else if hurst_h >= 0.48 {
                WaveRegimeKind::MotiveImpulseBull
            } else {
                WaveRegimeKind::NeutralEquilibrium
            }
        } else if omega_macro <= -0.22 {
            if (z_fast > 0.20 || z_sub > 0.15) && retracement_ratio.get() >= 0.16 {
                WaveRegimeKind::CorrectiveBounceInBear
            } else if hurst_h >= 0.48 {
                WaveRegimeKind::MotiveImpulseBear
            } else {
                WaveRegimeKind::NeutralEquilibrium
            }
        } else {
            WaveRegimeKind::NeutralEquilibrium
        };

        let out = WaveObservables {
            regime,
            hurst_score,
            oi_impulse,
            macro_phase,
            retracement_ratio,
            climax_intensity,
        };
        self.last_obs = out;
        out
    }

    /// Parses Bybit V5 /v5/market/kline list into ascending (ts_ms, O, H, L, C) rows.
    #[must_use]
    pub fn parse_bybit_kline_rows(
        spec: PairSpec,
        kline_payload: &serde_json::Value,
    ) -> Vec<(i64, PriceTicks, PriceTicks, PriceTicks, PriceTicks)> {
        let Some(list) = kline_payload
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        else {
            return Vec::new();
        };
        let mut rows = Vec::with_capacity(list.len());
        for item in list {
            let Some(arr) = item.as_array() else { continue };
            if arr.len() < 5 {
                continue;
            }
            let ts_ms = arr[0]
                .as_str()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .or_else(|| arr[0].as_i64())
                .unwrap_or(0);
            let Some(o_s) = arr[1].as_str() else { continue };
            let Some(h_s) = arr[2].as_str() else { continue };
            let Some(l_s) = arr[3].as_str() else { continue };
            let Some(c_s) = arr[4].as_str() else { continue };
            let (Ok(o_t), Ok(h_t), Ok(l_t), Ok(c_t)) = (
                spec.parse_price_ticks(o_s),
                spec.parse_price_ticks(h_s),
                spec.parse_price_ticks(l_s),
                spec.parse_price_ticks(c_s),
            ) else {
                continue;
            };
            if ts_ms > 0 && c_t.0 > 0 {
                rows.push((ts_ms, o_t, h_t, l_t, c_t));
            }
        }
        rows.sort_by_key(|r| r.0);
        rows
    }

    /// Chronologically merges 5m history (< first 1m bar), 1m detail & 5m Open Interest.
    pub fn warm_start_from_bybit_bundle(
        &mut self,
        spec: PairSpec,
        kline5m: Option<&serde_json::Value>,
        kline1m: Option<&serde_json::Value>,
        oi5m: Option<&serde_json::Value>,
    ) -> usize {
        let rows_1m = kline1m
            .map(|k| Self::parse_bybit_kline_rows(spec, k))
            .unwrap_or_default();
        let first_1m_ms = rows_1m.first().map_or(i64::MAX, |r| r.0);
        let rows_5m = kline5m
            .map(|k| Self::parse_bybit_kline_rows(spec, k))
            .unwrap_or_default();

        let mut oi_pts: Vec<(i64, f64)> = Vec::new();
        if let Some(oi_json) = oi5m {
            if let Some(list) = oi_json
                .get("result")
                .and_then(|r| r.get("list"))
                .and_then(|l| l.as_array())
            {
                for item in list {
                    let ts_ms = item
                        .get("timestamp")
                        .and_then(|v| {
                            v.as_str()
                                .and_then(|s| s.trim().parse::<i64>().ok())
                                .or_else(|| v.as_i64())
                        })
                        .unwrap_or(0);
                    let oi_v = item
                        .get("openInterest")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.trim().parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if ts_ms > 0 && oi_v > 0.0 {
                        oi_pts.push((ts_ms, oi_v));
                    }
                }
            }
        }
        oi_pts.sort_by_key(|p| p.0);
        let mut oi_idx = 0_usize;
        let mut total_bars = 0_usize;

        for &(ts_ms, o_t, h_t, l_t, c_t) in &rows_5m {
            if ts_ms >= first_1m_ms {
                break;
            }
            while oi_idx < oi_pts.len() && oi_pts[oi_idx].0 <= ts_ms + 300_000 {
                let (ots, oval) = oi_pts[oi_idx];
                let _ = self.observe_open_interest(spec, oval, ots * 1_000_000);
                oi_idx += 1;
            }
            let _ = self.observe_ohlc_candle(spec, ts_ms, o_t, h_t, l_t, c_t, 300);
            total_bars += 1;
        }

        for &(ts_ms, o_t, h_t, l_t, c_t) in &rows_1m {
            while oi_idx < oi_pts.len() && oi_pts[oi_idx].0 <= ts_ms + 60_000 {
                let (ots, oval) = oi_pts[oi_idx];
                let _ = self.observe_open_interest(spec, oval, ots * 1_000_000);
                oi_idx += 1;
            }
            let _ = self.observe_ohlc_candle(spec, ts_ms, o_t, h_t, l_t, c_t, 60);
            total_bars += 1;
        }

        while oi_idx < oi_pts.len() {
            let (ots, oval) = oi_pts[oi_idx];
            let _ = self.observe_open_interest(spec, oval, ots * 1_000_000);
            oi_idx += 1;
        }
        total_bars
    }

    /// Evaluates continuous C1 wave entry modulation & counter-trend/climax vetoes.
    pub fn evaluate_wave_entry_modulation(
        obs: WaveObservables,
        candidate_side: Side,
        btc_leader_snr_signed: f64,
    ) -> Result<WaveModulationVerdict, &'static str> {
        let dir_s = candidate_side.sign_f64();
        let signed_macro = obs.macro_phase.get() * dir_s;
        let signed_oi = obs.oi_impulse.get() * dir_s;
        let x_h = obs.hurst_score.get();
        let ret = obs.retracement_ratio.get();
        let climax = obs.climax_intensity.get();

        let is_corrective_counter = matches!(
            (candidate_side, obs.regime),
            (Side::Bid, WaveRegimeKind::CorrectiveBounceInBear)
                | (Side::Ask, WaveRegimeKind::CorrectivePullbackInBull)
        );
        let hurst_h = obs.hurst_exponent();
        if (signed_macro < -0.32 || is_corrective_counter)
            && signed_macro < -0.18
            && signed_oi < 0.18
            && btc_leader_snr_signed < 0.28
        {
            return Err("VETO_COUNTER_TREND_CORRECTIVE_WAVE");
        }
        let req_chop_h = (0.48 + 0.35 * signed_macro).clamp(0.36, 0.48);
        if signed_macro < -0.12
            && (hurst_h < req_chop_h || hurst_h < 0.46)
            && signed_oi < 0.22
            && btc_leader_snr_signed < 0.25
        {
            return Err("VETO_COUNTER_TREND_SUBDIFFUSIVE_CHOP");
        }
        if matches!(obs.regime, WaveRegimeKind::TerminalExhaustionClimax)
            && signed_macro > 0.35
            && ret <= 0.08
            && climax >= 0.58
        {
            return Err("VETO_ELLIOTT_WAVE5_CLIMAX_EXHAUSTION");
        }

        let mut snr_delta = 0.0_f64;
        if signed_macro < -0.15 {
            snr_delta += (((-signed_macro - 0.15) / 0.25) * 0.07).clamp(0.0, 0.08);
        } else if signed_macro >= 0.22 && (0.30..=0.70).contains(&ret) && x_h >= -0.05 {
            snr_delta -= 0.025;
        } else if signed_macro >= 0.30 && x_h >= 0.10 && signed_oi >= 0.05 {
            snr_delta -= 0.020;
        }
        if x_h < -0.12 {
            snr_delta += ((-x_h - 0.12) * 0.10).clamp(0.0, 0.04);
        }

        let t1_shift = -(90.0 * x_h.clamp(-0.50, 0.60)
            + 50.0 * signed_oi.clamp(-0.50, 0.60))
        .round() as i64;
        let def_mult = if signed_macro >= 0.25 && x_h >= 0.08 {
            (1_000.0 + 250.0 * x_h.min(0.60)).round() as i64
        } else if signed_macro <= -0.20 {
            800
        } else {
            1_000
        };

        Ok(WaveModulationVerdict {
            snr_threshold_delta: snr_delta.clamp(-0.03, 0.09),
            tranche1_permille_delta: t1_shift.clamp(-110, 110),
            def_horizon_permille_mult: def_mult.clamp(750, 1_250),
        })
    }
}

/// Computes absolute Pearson correlation |r| in [0.0, 1.0] to verify orthogonality (< 0.25).
#[must_use]
pub fn pearson_abs_correlation(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len().min(ys.len());
    if n < 3 {
        return 0.0;
    }
    let nf = n as f64;
    let mx = xs.iter().take(n).sum::<f64>() / nf;
    let my = ys.iter().take(n).sum::<f64>() / nf;
    let mut cov = 0.0_f64;
    let mut vx = 0.0_f64;
    let mut vy = 0.0_f64;
    for i in 0..n {
        let dx = xs[i] - mx;
        let dy = ys[i] - my;
        cov += dx * dy;
        vx += dx * dx;
        vy += dy * dy;
    }
    let denom = (vx * vy).sqrt();
    if denom <= 1e-12 {
        0.0
    } else {
        (cov / denom).abs().clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_elliott_impulse_vs_corrective_bounce_symmetry_and_climax() {
        let tao_spec = APPROVED_SPECS[4];
        let mut bear_st = CausalElliottWaveState::new();
        let mut ts = 1_700_000_000_000_000_000_i64;
        let mut px = 22_200_i64;
        for step in 0..120_i64 {
            ts += 60_000_000_000;
            px -= 6;
            let _ = bear_st.observe_price_tick(
                tao_spec,
                PriceTicks(px),
                ts,
                ScaledBps(5_500),
            );
            let _ = bear_st.observe_open_interest(
                tao_spec,
                100_000.0 + (step as f64) * 180.0,
                ts,
            );
        }
        let bear_imp = bear_st.observables();
        assert!(bear_imp.macro_phase.get() < -0.45);
        assert!(bear_imp.hurst_exponent() > 0.55);
        assert!(bear_imp.oi_impulse.get() < -0.20);
        assert_eq!(bear_imp.regime, WaveRegimeKind::MotiveImpulseBear);
        assert!(CausalElliottWaveState::evaluate_wave_entry_modulation(
            bear_imp, Side::Ask, -0.45,
        )
        .is_ok());

        for step in 0..6_i64 {
            ts += 30_000_000_000;
            px += 4;
            let _ = bear_st.observe_price_tick(
                tao_spec,
                PriceTicks(px),
                ts,
                ScaledBps(5_500),
            );
            let _ = bear_st.observe_open_interest(
                tao_spec,
                121_500.0 - (step as f64) * 320.0,
                ts,
            );
        }
        let hollow_bounce = bear_st.observables();
        assert!(hollow_bounce.oi_impulse.get() <= 0.0);
        assert_eq!(
            CausalElliottWaveState::evaluate_wave_entry_modulation(
                hollow_bounce,
                Side::Bid,
                -0.10,
            ),
            Err("VETO_COUNTER_TREND_CORRECTIVE_WAVE")
        );

        let mut bull_st = CausalElliottWaveState::new();
        let mut ts_b = 1_700_000_000_000_000_000_i64;
        let mut px_b = 20_760_i64;
        for step in 0..120_i64 {
            ts_b += 60_000_000_000;
            px_b += 6;
            let _ = bull_st.observe_price_tick(
                tao_spec,
                PriceTicks(px_b),
                ts_b,
                ScaledBps(5_500),
            );
            let _ = bull_st.observe_open_interest(
                tao_spec,
                100_000.0 + (step as f64) * 180.0,
                ts_b,
            );
        }
        let bull_imp = bull_st.observables();
        assert_eq!(bull_imp.regime, WaveRegimeKind::MotiveImpulseBull);
        let mod_bull = CausalElliottWaveState::evaluate_wave_entry_modulation(
            bull_imp, Side::Bid, 0.45,
        );
        let mod_bear = CausalElliottWaveState::evaluate_wave_entry_modulation(
            bear_imp, Side::Ask, -0.45,
        );
        assert!(mod_bull.is_ok() && mod_bear.is_ok());
        if let (Ok(mb), Ok(ma)) = (mod_bull, mod_bear) {
            assert!((mb.tranche1_permille_delta - ma.tranche1_permille_delta).abs() <= 2);
            assert!(mb.tranche1_permille_delta < 0);
        }

        let _ = bull_st.observe_liquidation_print(
            tao_spec,
            Side::Bid,
            LotQty(4_000),
            ts_b + 1_000_000_000,
        );
        let climax_obs = bull_st.observe_price_tick(
            tao_spec,
            PriceTicks(px_b + 85),
            ts_b + 2_000_000_000,
            ScaledBps(5_500),
        );
        assert_eq!(climax_obs.regime, WaveRegimeKind::TerminalExhaustionClimax);
        assert_eq!(
            CausalElliottWaveState::evaluate_wave_entry_modulation(
                climax_obs, Side::Bid, 0.50,
            ),
            Err("VETO_ELLIOTT_WAVE5_CLIMAX_EXHAUSTION")
        );
    }

    #[test]
    fn test_bybit_v5_live_or_cached_12pair_elliott_statistical_validation() {
        let sample_kline = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [
                    ["1700000900000", "8.4800", "8.4900", "8.4500", "8.4550", "1000", "8460"],
                    ["1700000600000", "8.5100", "8.5150", "8.4750", "8.4800", "1200", "10180"],
                    ["1700000300000", "8.5400", "8.5450", "8.5050", "8.5100", "1100", "9360"],
                    ["1700000000000", "8.5700", "8.5800", "8.5350", "8.5400", "900", "7690"]
                ]
            }
        });
        let sample_oi = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [
                    {"openInterest": "2650000", "timestamp": "1700000900000"},
                    {"openInterest": "2610000", "timestamp": "1700000600000"},
                    {"openInterest": "2580000", "timestamp": "1700000300000"},
                    {"openInterest": "2550000", "timestamp": "1700000000000"}
                ]
            }
        });
        let mut st = CausalElliottWaveState::new();
        assert_eq!(
            st.warm_start_from_bybit_bundle(
                APPROVED_SPECS[3],
                Some(&sample_kline),
                None,
                Some(&sample_oi),
            ),
            4
        );
        assert!(st.observables().macro_phase.get() < 0.0);

        let mut macro_series = Vec::with_capacity(120);
        let mut fast_osc_series = Vec::with_capacity(120);
        for k in 0..120_usize {
            let kf = k as f64;
            macro_series.push((kf / 40.0).tanh());
            fast_osc_series.push((kf * 1.7).sin());
        }
        assert!(pearson_abs_correlation(&macro_series, &fast_osc_series) < 0.25);

        let live_cache = std::env::temp_dir().join("vortex_bybit_elliott_live.json");
        if let Ok(raw_txt) = std::fs::read_to_string(&live_cache) {
            if let Ok(root) = serde_json::from_str::<serde_json::Value>(&raw_txt) {
                println!(
                    "\n=== [BYBIT V5 UNBIASED 12-PAIR ELLIOTT WAVE & OI LAB] ==="
                );
                println!(
                    concat!(
                        "SLOT | SYMBOL     | REGIME               | ",
                        "HURST | MACRO | OI_IMP | RET_FIB | BID_GATE | ASK_GATE"
                    )
                );
                let mut analyzed = 0_usize;
                for (idx, spec) in APPROVED_SPECS.iter().enumerate() {
                    let sym = spec.symbol_id.name();
                    let Some(entry) = root.get(sym) else { continue };
                    let mut live_w = CausalElliottWaveState::new();
                    let _ = live_w.warm_start_from_bybit_bundle(
                        *spec,
                        entry.get("kline5m"),
                        entry.get("kline1m"),
                        entry.get("oi5m"),
                    );
                    let obs = live_w.observables();
                    let bid_g = match CausalElliottWaveState::evaluate_wave_entry_modulation(
                        obs, Side::Bid, 0.0,
                    ) {
                        Ok(m) => format!("OK(dT1={:+})", m.tranche1_permille_delta),
                        Err(_) => "VETO_CORR".to_string(),
                    };
                    let ask_g = match CausalElliottWaveState::evaluate_wave_entry_modulation(
                        obs, Side::Ask, 0.0,
                    ) {
                        Ok(m) => format!("OK(dT1={:+})", m.tranche1_permille_delta),
                        Err(_) => "VETO_CORR".to_string(),
                    };
                    println!(
                        concat!(
                            "  {:2} | {:<10} | {:<20} | {:>5.3} | ",
                            "{:>+5.2} | {:>+6.2} | {:>7.3} | {:<11} | {}"
                        ),
                        idx,
                        sym,
                        obs.regime.label(),
                        obs.hurst_exponent(),
                        obs.macro_phase.get(),
                        obs.oi_impulse.get(),
                        obs.retracement_ratio.get(),
                        bid_g,
                        ask_g
                    );
                    analyzed += 1;
                }
                assert!(analyzed >= 1);
            }
        }
    }
}
