//! Causal 6-Pivot Elliott Wave v2.0 Automaton, Signed Liquidations & Fibonacci Projections.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs};

const PHI: f64 = 1.618_033_988_749_895;
const PHI_SQ: f64 = 2.618_033_988_749_895;
const INV_PHI: f64 = 0.618_033_988_749_895;
const LIQ_FLOOR_USDT: f64 = 500.0;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum WaveRegime {
    #[default]
    NeutralWarmup = 0,
    ImpulseWave3 = 1,
    CorrectiveWave4 = 2,
    ImpulseWave5 = 3,
    CorrectiveAbc = 4,
    ClimaxExhaustion = 5,
}

impl WaveRegime {
    #[inline]
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::ImpulseWave3,
            2 => Self::CorrectiveWave4,
            3 => Self::ImpulseWave5,
            4 => Self::CorrectiveAbc,
            5 => Self::ClimaxExhaustion,
            _ => Self::NeutralWarmup,
        }
    }

    #[inline]
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NeutralWarmup => "NeutralWarmup",
            Self::ImpulseWave3 => "ImpulseWave3",
            Self::CorrectiveWave4 => "CorrectiveWave4",
            Self::ImpulseWave5 => "ImpulseWave5",
            Self::CorrectiveAbc => "CorrectiveAbc",
            Self::ClimaxExhaustion => "ClimaxExhaustion",
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct WavePivot {
    pub kind_sign: i8,
    pub price_ticks: PriceTicks,
    pub ts_ns: TimestampNs,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct WaveObservables {
    pub regime: WaveRegime,
    pub climax_active: bool,
    pub hurst: BoundedScore,
    pub macro_phase: BoundedScore,
    pub oi_impulse: BoundedScore,
    pub liq_imbalance: BoundedScore,
    pub fib_proj_bps: ScaledBps,
}

impl WaveObservables {
    #[inline]
    #[must_use]
    pub fn hurst_exponent(self) -> f64 {
        (0.50 + 0.35 * self.hurst.get()).clamp(0.25, 0.85)
    }

    #[inline]
    #[must_use]
    pub fn encode_hurst(h: f64) -> BoundedScore {
        BoundedScore::new_clamped((h.clamp(0.25, 0.85) - 0.50) / 0.35)
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct WaveEntryModulation {
    pub snr_threshold_delta: f64,
    pub def_horizon_permille_mult: i64,
    pub tranche1_permille_delta: i64,
    pub fib_target_bps: ScaledBps,
}

impl Default for WaveEntryModulation {
    #[inline]
    fn default() -> Self {
        Self {
            snr_threshold_delta: 0.0,
            def_horizon_permille_mult: 1_000,
            tranche1_permille_delta: 0,
            fib_target_bps: ScaledBps(0),
        }
    }
}

/// Parses Bybit V5 `allLiquidation` JSON item using uppercase `"S"` / `"side"`.
/// `"Sell"` (Short liquidated => Forced Market Buy) -> `Side::Bid` (+1).
/// `"Buy"`  (Long liquidated  => Forced Market Sell) -> `Side::Ask` (-1).
#[must_use]
pub fn parse_bybit_all_liquidation_event(
    spec: PairSpec,
    item: &serde_json::Value,
) -> Option<(Side, LotQty, PriceTicks)> {
    let raw_pos_side = item
        .get("S")
        .or_else(|| item.get("side"))
        .and_then(|v| v.as_str())?;
    let forced_book_side = match raw_pos_side {
        "Sell" => Side::Bid,
        "Buy" => Side::Ask,
        _ => return None,
    };
    let q_str = item
        .get("v")
        .or_else(|| item.get("size"))
        .and_then(|v| v.as_str())?;
    let qty = spec.parse_lot_qty(q_str);
    if qty.0 <= 0 {
        return None;
    }
    let px = item
        .get("p")
        .or_else(|| item.get("price"))
        .and_then(|v| v.as_str())
        .and_then(|s| spec.parse_price_ticks(s).ok())
        .unwrap_or(PriceTicks(0));
    Some((forced_book_side, qty, px))
}

#[derive(Copy, Clone, Debug)]
pub struct CausalElliottWaveState {
    pivots: [WavePivot; 6],
    pivot_len: u8,
    zig_dir: i8,
    anchor_px: PriceTicks,
    anchor_ts: TimestampNs,
    last_mid_px: PriceTicks,
    last_var_sample_px: PriceTicks,
    last_var_sample_ts_ns: i64,
    ring_ret4_bps: [f64; 4],
    ring_idx: u8,
    ewma_var1_bps2: f64,
    ewma_var4_bps2: f64,
    ewma_oi_fast: f64,
    ewma_oi_slow: f64,
    last_oi_ts_ns: i64,
    ewma_liq_signed_usdt: f64,
    ewma_liq_abs_fast_usdt: f64,
    ewma_liq_abs_slow_usdt: f64,
    last_liq_ts_ns: i64,
    cached_obs: WaveObservables,
}

impl Default for CausalElliottWaveState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl CausalElliottWaveState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pivots: [WavePivot {
                kind_sign: 0,
                price_ticks: PriceTicks(0),
                ts_ns: TimestampNs(0),
            }; 6],
            pivot_len: 0,
            zig_dir: 0,
            anchor_px: PriceTicks(0),
            anchor_ts: TimestampNs(0),
            last_mid_px: PriceTicks(0),
            last_var_sample_px: PriceTicks(0),
            last_var_sample_ts_ns: 0,
            ring_ret4_bps: [0.0; 4],
            ring_idx: 0,
            ewma_var1_bps2: 400.0,
            ewma_var4_bps2: 1_600.0,
            ewma_oi_fast: 0.0,
            ewma_oi_slow: 0.0,
            last_oi_ts_ns: 0,
            ewma_liq_signed_usdt: 0.0,
            ewma_liq_abs_fast_usdt: 0.0,
            ewma_liq_abs_slow_usdt: LIQ_FLOOR_USDT,
            last_liq_ts_ns: 0,
            cached_obs: WaveObservables {
                regime: WaveRegime::NeutralWarmup,
                climax_active: false,
                hurst: BoundedScore::ZERO,
                macro_phase: BoundedScore::ZERO,
                oi_impulse: BoundedScore::ZERO,
                liq_imbalance: BoundedScore::ZERO,
                fib_proj_bps: ScaledBps(0),
            },
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    #[inline]
    #[must_use]
    pub const fn observables(&self) -> WaveObservables {
        self.cached_obs
    }

    #[inline]
    #[must_use]
    pub const fn pivot_count(&self) -> usize {
        self.pivot_len as usize
    }

    #[inline]
    fn reversal_threshold_bps(spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> i64 {
        let b3x = spec
            .required_3x_barrier_bps(mid_px)
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw()
            .max(900);
        let base_v = vol_5m.raw().max(b3x) as f64;
        let raw = (INV_PHI * base_v).round() as i64;
        raw.clamp(b3x, b3x.saturating_mul(8))
    }

    #[inline]
    fn push_pivot(&mut self, pivot: WavePivot) {
        let len = self.pivot_len as usize;
        if len < 6 {
            self.pivots[len] = pivot;
            self.pivot_len += 1;
        } else {
            for i in 0..5 {
                self.pivots[i] = self.pivots[i + 1];
            }
            self.pivots[5] = pivot;
        }
    }

    fn step_zigzag(&mut self, spec: PairSpec, px: PriceTicks, ts_ns: i64, vol_5m: ScaledBps) {
        if px.0 <= 0 {
            return;
        }
        let ts = TimestampNs(ts_ns);
        let rev_bps = Self::reversal_threshold_bps(spec, px, vol_5m);
        if self.zig_dir == 0 {
            if self.anchor_px.0 <= 0 {
                self.anchor_px = px;
                self.anchor_ts = ts;
                return;
            }
            let move_bps =
                ((px.0 - self.anchor_px.0).saturating_mul(1_000_000)) / self.anchor_px.0.max(1);
            if move_bps >= rev_bps {
                self.push_pivot(WavePivot {
                    kind_sign: -1,
                    price_ticks: self.anchor_px,
                    ts_ns: self.anchor_ts,
                });
                self.push_pivot(WavePivot {
                    kind_sign: 1,
                    price_ticks: px,
                    ts_ns: ts,
                });
                self.zig_dir = 1;
            } else if move_bps <= -rev_bps {
                self.push_pivot(WavePivot {
                    kind_sign: 1,
                    price_ticks: self.anchor_px,
                    ts_ns: self.anchor_ts,
                });
                self.push_pivot(WavePivot {
                    kind_sign: -1,
                    price_ticks: px,
                    ts_ns: ts,
                });
                self.zig_dir = -1;
            }
            return;
        }
        let last_idx = (self.pivot_len as usize).saturating_sub(1);
        let last_px = self.pivots[last_idx].price_ticks;
        if self.zig_dir > 0 {
            if px.0 >= last_px.0 {
                self.pivots[last_idx].price_ticks = px;
                self.pivots[last_idx].ts_ns = ts;
            } else {
                let drop_bps = ((last_px.0 - px.0).saturating_mul(1_000_000)) / last_px.0.max(1);
                if drop_bps >= rev_bps {
                    self.push_pivot(WavePivot {
                        kind_sign: -1,
                        price_ticks: px,
                        ts_ns: ts,
                    });
                    self.zig_dir = -1;
                }
            }
        } else if px.0 <= last_px.0 {
            self.pivots[last_idx].price_ticks = px;
            self.pivots[last_idx].ts_ns = ts;
        } else {
            let rise_bps = ((px.0 - last_px.0).saturating_mul(1_000_000)) / last_px.0.max(1);
            if rise_bps >= rev_bps {
                self.push_pivot(WavePivot {
                    kind_sign: 1,
                    price_ticks: px,
                    ts_ns: ts,
                });
                self.zig_dir = 1;
            }
        }
    }

    fn update_hurst_vr(&mut self, px: PriceTicks, ts_ns: i64) -> f64 {
        if self.last_var_sample_px.0 <= 0 {
            self.last_var_sample_px = px;
            self.last_var_sample_ts_ns = ts_ns;
            return self.cached_obs.hurst_exponent();
        }
        let dt_ns = ts_ns.saturating_sub(self.last_var_sample_ts_ns);
        if ts_ns > 0 && self.last_var_sample_ts_ns > 0 && dt_ns < 500_000_000 {
            return self.cached_obs.hurst_exponent();
        }
        let prev = self.last_var_sample_px.0.max(1) as f64;
        let ret_bps = (((px.0 - self.last_var_sample_px.0) as f64) * 10_000.0) / prev;
        self.last_var_sample_px = px;
        self.last_var_sample_ts_ns = ts_ns;
        if ret_bps.abs() < 1e-6 {
            return self.cached_obs.hurst_exponent();
        }
        let idx = (self.ring_idx as usize) & 3;
        self.ring_ret4_bps[idx] = ret_bps;
        self.ring_idx = self.ring_idx.wrapping_add(1);
        let sum4 = self.ring_ret4_bps[0]
            + self.ring_ret4_bps[1]
            + self.ring_ret4_bps[2]
            + self.ring_ret4_bps[3];
        let sq1 = (ret_bps * ret_bps).clamp(0.01, 250_000.0);
        let sq4 = (sum4 * sum4).clamp(0.04, 1_000_000.0);
        self.ewma_var1_bps2 = 0.92 * self.ewma_var1_bps2 + 0.08 * sq1;
        self.ewma_var4_bps2 = 0.92 * self.ewma_var4_bps2 + 0.08 * sq4;
        let vr4 = (self.ewma_var4_bps2 / (4.0 * self.ewma_var1_bps2).max(0.04)).clamp(0.35, 2.65);
        (0.50 + vr4.ln() / (2.0 * 4.0_f64.ln())).clamp(0.25, 0.85)
    }

    fn decay_liquidation_state(&mut self, ts_ns: i64) {
        if self.last_liq_ts_ns <= 0 || ts_ns <= self.last_liq_ts_ns {
            if self.last_liq_ts_ns <= 0 && ts_ns > 0 {
                self.last_liq_ts_ns = ts_ns;
            }
            return;
        }
        let dt_s = ((ts_ns - self.last_liq_ts_ns) as f64 * 1e-9).clamp(0.0, 600.0);
        self.last_liq_ts_ns = ts_ns;
        let d_fast = (-dt_s / 15.0).exp();
        let d_slow = (-dt_s / 300.0).exp();
        self.ewma_liq_signed_usdt *= d_fast;
        self.ewma_liq_abs_fast_usdt *= d_fast;
        self.ewma_liq_abs_slow_usdt =
            (self.ewma_liq_abs_slow_usdt * d_slow).max(LIQ_FLOOR_USDT);
    }

    #[inline]
    fn liq_imbalance_and_z(&self) -> (f64, f64) {
        let denom = (self.ewma_liq_abs_fast_usdt + LIQ_FLOOR_USDT).max(1.0);
        let i_liq = (self.ewma_liq_signed_usdt / denom).clamp(-1.0, 1.0);
        let z_liq = self.ewma_liq_abs_fast_usdt / self.ewma_liq_abs_slow_usdt.max(LIQ_FLOOR_USDT);
        (i_liq, z_liq)
    }

    fn recompute_observables(&mut self, hurst_val: f64) -> WaveObservables {
        let px = self.last_mid_px.0;
        let (i_liq, z_liq) = self.liq_imbalance_and_z();
        let i_oi = self.cached_obs.oi_impulse.get();
        let len = self.pivot_len as usize;

        if px <= 0 || len < 3 {
            let obs = WaveObservables {
                regime: WaveRegime::NeutralWarmup,
                climax_active: false,
                hurst: WaveObservables::encode_hurst(hurst_val),
                macro_phase: BoundedScore::ZERO,
                oi_impulse: self.cached_obs.oi_impulse,
                liq_imbalance: BoundedScore::new_clamped(i_liq),
                fib_proj_bps: ScaledBps(0),
            };
            self.cached_obs = obs;
            return obs;
        }

        let mut regime = WaveRegime::CorrectiveAbc;
        let mut phase = 0.0_f64;
        let mut fib_proj_bps = 0_i64;
        let mut climax_active = false;
        let mut matched_higher_order = false;

        if len == 6 {
            let p0 = self.pivots[0].price_ticks.0;
            let p1 = self.pivots[1].price_ticks.0;
            let p2 = self.pivots[2].price_ticks.0;
            let p3 = self.pivots[3].price_ticks.0;
            let p4 = self.pivots[4].price_ticks.0;
            let s = i64::from(-self.pivots[0].kind_sign);
            let sf = s as f64;
            let w1 = (p1 - p0) * s;
            let w2 = (p1 - p2) * s;
            let w3 = (p3 - p2) * s;
            let w4 = (p3 - p4) * s;
            let e1_ok = (p2 - p0) * s > 0 && w1 > 0 && w2 > 0 && (w2 as f64) < 0.99 * (w1 as f64);
            let e2_w3_valid = w3 >= w1;
            let e3_non_overlap = (p4 - p1) * s > 0
                && w3 > 0
                && w4 > 0
                && (w4 as f64) <= (INV_PHI + 0.06) * (w3 as f64);

            if e1_ok && e3_non_overlap && (px - p4) * s > 0 {
                let w5_cur = (px - p4) * s;
                let e2_ok = e2_w3_valid || w5_cur < w3;
                if e2_ok {
                    matched_higher_order = true;
                    let fuel = (0.60 * (sf * i_liq).max(0.0) * (0.45 * z_liq).tanh()
                        + 0.40 * i_oi.max(0.0))
                        .clamp(0.0, 1.0);
                    let blowoff = z_liq >= 3.5 && (sf * i_liq) >= 0.55 && i_oi <= -0.12;
                    if blowoff {
                        regime = WaveRegime::ClimaxExhaustion;
                        climax_active = true;
                        phase = 0.92 * sf;
                        fib_proj_bps = 0;
                    } else {
                        regime = WaveRegime::ImpulseWave5;
                        let k5 = 1.0 + (PHI - 1.0) * (1.25 * fuel - 0.25).clamp(0.0, 1.0);
                        let base_amp = (w1 as f64).max(INV_PHI * ((p3 - p0).abs() as f64));
                        let raw_w5 = (k5 * base_amp).round() as i64;
                        let cap_w5 = if e2_w3_valid { raw_w5 } else { raw_w5.min(w3 - 1) };
                        let p_fib5 = p4 + s * cap_w5.max(1);
                        let rem_ticks = (p_fib5 - px) * s;
                        if rem_ticks > 0 {
                            fib_proj_bps = ((p_fib5 - px).saturating_mul(1_000_000)) / px.max(1);
                        }
                        phase = 0.68 * sf;
                    }
                }
            } else if e1_ok && !e3_non_overlap {
                matched_higher_order = true;
                regime = WaveRegime::CorrectiveAbc;
                phase = -0.45 * sf;
            }
        }

        if !matched_higher_order && len >= 5 {
            let b = len - 5;
            let p0 = self.pivots[b].price_ticks.0;
            let p1 = self.pivots[b + 1].price_ticks.0;
            let p2 = self.pivots[b + 2].price_ticks.0;
            let p3 = self.pivots[b + 3].price_ticks.0;
            let s = i64::from(-self.pivots[b].kind_sign);
            let sf = s as f64;
            let w1 = (p1 - p0) * s;
            let w2 = (p1 - p2) * s;
            let w3 = (p3 - p2) * s;
            let w4_cur = (p3 - px) * s;
            if (p2 - p0) * s > 0
                && w1 > 0
                && w2 > 0
                && w3 >= w1
                && w4_cur > 0
                && (px - p1) * s > 0
                && (w4_cur as f64) <= (INV_PHI + 0.06) * (w3 as f64)
            {
                matched_higher_order = true;
                regime = WaveRegime::CorrectiveWave4;
                phase = 0.25 * sf;
            }
        }

        if !matched_higher_order && len >= 3 {
            let b = if len >= 4 && self.pivots[len - 1].kind_sign != self.pivots[len - 4].kind_sign
            {
                len - 4
            } else {
                len - 3
            };
            let p0 = self.pivots[b].price_ticks.0;
            let p1 = self.pivots[b + 1].price_ticks.0;
            let p2 = self.pivots[b + 2].price_ticks.0;
            let s = i64::from(-self.pivots[b].kind_sign);
            let sf = s as f64;
            let w1 = (p1 - p0) * s;
            let w2 = (p1 - p2) * s;
            let r2 = if w1 > 0 { (w2 as f64) / (w1 as f64) } else { 0.0 };
            let e1_ok = (p2 - p0) * s > 0 && (0.236..0.990).contains(&r2);
            if e1_ok && (px - p2) * s > 0 {
                let fuel = (0.60 * (sf * i_liq).max(0.0) * (0.45 * z_liq).tanh()
                    + 0.40 * i_oi.max(0.0))
                    .clamp(0.0, 1.0);
                let blowoff = z_liq >= 4.2 && (sf * i_liq) >= 0.65 && i_oi <= -0.18;
                if blowoff {
                    regime = WaveRegime::ClimaxExhaustion;
                    climax_active = true;
                    phase = 0.90 * sf;
                } else {
                    regime = WaveRegime::ImpulseWave3;
                    let h_boost = ((hurst_val - 0.54) / 0.14).clamp(0.0, 1.0);
                    let blend = (0.55 * h_boost + 0.45 * fuel).clamp(0.0, 1.0);
                    let k3 = PHI + (PHI_SQ - PHI) * blend;
                    let p_fib3 = p2 + s * ((k3 * (w1 as f64)).round() as i64).max(1);
                    if (p_fib3 - px) * s > 0 {
                        fib_proj_bps = ((p_fib3 - px).saturating_mul(1_000_000)) / px.max(1);
                    }
                    let golden_pocket = (0.382..=0.786).contains(&r2);
                    phase = (if golden_pocket { 0.85 } else { 0.70 }) * sf;
                }
            } else {
                let last_s = f64::from(self.zig_dir);
                phase = 0.30 * last_s;
            }
        }

        let obs = WaveObservables {
            regime,
            climax_active,
            hurst: WaveObservables::encode_hurst(hurst_val),
            macro_phase: BoundedScore::new_clamped(phase),
            oi_impulse: self.cached_obs.oi_impulse,
            liq_imbalance: BoundedScore::new_clamped(i_liq),
            fib_proj_bps: ScaledBps(fib_proj_bps),
        };
        self.cached_obs = obs;
        obs
    }

    pub fn observe_price_tick(
        &mut self,
        spec: PairSpec,
        mid_px: PriceTicks,
        ts_ns: i64,
        realized_vol_5m: ScaledBps,
    ) -> WaveObservables {
        if mid_px.0 <= 0 {
            return self.cached_obs;
        }
        self.last_mid_px = mid_px;
        self.decay_liquidation_state(ts_ns);
        let h_val = self.update_hurst_vr(mid_px, ts_ns);
        self.step_zigzag(spec, mid_px, ts_ns, realized_vol_5m);
        self.recompute_observables(h_val)
    }

    pub fn observe_open_interest(
        &mut self,
        _spec: PairSpec,
        open_interest: f64,
        ts_ns: i64,
    ) -> WaveObservables {
        if !open_interest.is_finite() || open_interest <= 0.0 {
            return self.cached_obs;
        }
        if self.ewma_oi_fast <= 0.0 || self.ewma_oi_slow <= 0.0 {
            self.ewma_oi_fast = open_interest;
            self.ewma_oi_slow = open_interest;
            self.last_oi_ts_ns = ts_ns;
            return self.cached_obs;
        }
        let dt_s = if ts_ns > self.last_oi_ts_ns && self.last_oi_ts_ns > 0 {
            ((ts_ns - self.last_oi_ts_ns) as f64 * 1e-9).clamp(1.0, 300.0)
        } else {
            5.0
        };
        self.last_oi_ts_ns = ts_ns;
        let a_f = (1.0 - (-dt_s / 30.0).exp()).clamp(0.02, 1.0);
        let a_s = (1.0 - (-dt_s / 300.0).exp()).clamp(0.005, 1.0);
        self.ewma_oi_fast += a_f * (open_interest - self.ewma_oi_fast);
        self.ewma_oi_slow += a_s * (open_interest - self.ewma_oi_slow);
        let rel_oi = (self.ewma_oi_fast - self.ewma_oi_slow) / self.ewma_oi_slow.max(1e-6);
        self.cached_obs.oi_impulse = BoundedScore::new_clamped(rel_oi * 120.0);
        let h_val = self.cached_obs.hurst_exponent();
        self.recompute_observables(h_val)
    }

    pub fn observe_liquidation_with_price(
        &mut self,
        spec: PairSpec,
        forced_book_side: Side,
        qty: LotQty,
        liq_price: PriceTicks,
        ts_ns: i64,
    ) -> WaveObservables {
        if qty.0 <= 0 {
            return self.cached_obs;
        }
        self.decay_liquidation_state(ts_ns);
        let ref_px = if liq_price.0 > 0 {
            liq_price
        } else if self.last_mid_px.0 > 0 {
            self.last_mid_px
        } else {
            PriceTicks(1_000)
        };
        let notional_usdt = (spec.lot_notional_1e8(qty, ref_px) as f64) * 1e-8;
        let signed_usdt = notional_usdt * forced_book_side.sign_f64();
        self.ewma_liq_signed_usdt += signed_usdt;
        self.ewma_liq_abs_fast_usdt += notional_usdt;
        self.ewma_liq_abs_slow_usdt =
            0.96 * self.ewma_liq_abs_slow_usdt + 0.04 * notional_usdt.max(LIQ_FLOOR_USDT);
        let h_val = self.cached_obs.hurst_exponent();
        self.recompute_observables(h_val)
    }

    #[inline]
    pub fn observe_liquidation_print(
        &mut self,
        spec: PairSpec,
        forced_book_side: Side,
        qty: LotQty,
        ts_ns: i64,
    ) -> WaveObservables {
        self.observe_liquidation_with_price(spec, forced_book_side, qty, self.last_mid_px, ts_ns)
    }

    pub fn observe_bybit_liquidation_json(
        &mut self,
        spec: PairSpec,
        item: &serde_json::Value,
        ts_ns: i64,
    ) -> Option<WaveObservables> {
        let (forced_side, qty, px) = parse_bybit_all_liquidation_event(spec, item)?;
        Some(self.observe_liquidation_with_price(spec, forced_side, qty, px, ts_ns))
    }

    fn ingest_kline_bundle(&mut self, spec: PairSpec, kline_json: &serde_json::Value) -> usize {
        let Some(rows) = kline_json
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        else {
            return 0;
        };
        let mut count = 0_usize;
        for row in rows.iter().rev() {
            let Some(arr) = row.as_array() else { continue };
            if arr.len() < 5 {
                continue;
            }
            let ts_ms = arr[0]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let hi_opt = arr[2].as_str().and_then(|s| spec.parse_price_ticks(s).ok());
            let lo_opt = arr[3].as_str().and_then(|s| spec.parse_price_ticks(s).ok());
            let cl_opt = arr[4].as_str().and_then(|s| spec.parse_price_ticks(s).ok());
            if let (Some(hi), Some(lo), Some(cl)) = (hi_opt, lo_opt, cl_opt) {
                let vol_est = ScaledBps(
                    (((hi.0 - lo.0).abs().saturating_mul(1_000_000)) / cl.0.max(1)).max(2_500),
                );
                let ts_ns = ts_ms.saturating_mul(1_000_000);
                let _ = self.observe_price_tick(spec, cl, ts_ns, vol_est);
                count += 1;
            }
        }
        count
    }

    pub fn warm_start_from_bybit_bundle(
        &mut self,
        spec: PairSpec,
        kline_5m: Option<&serde_json::Value>,
        kline_1m: Option<&serde_json::Value>,
        oi_5m: Option<&serde_json::Value>,
    ) -> usize {
        let mut total = 0_usize;
        if let Some(k5) = kline_5m {
            total += self.ingest_kline_bundle(spec, k5);
        }
        if let Some(k1) = kline_1m {
            total += self.ingest_kline_bundle(spec, k1);
        }
        if let Some(oi_val) = oi_5m {
            if let Some(rows) = oi_val
                .get("result")
                .and_then(|r| r.get("list"))
                .and_then(|l| l.as_array())
            {
                for item in rows.iter().rev() {
                    let oi_f = item
                        .get("openInterest")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let ts_ms = item
                        .get("timestamp")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    if oi_f > 0.0 {
                        let _ = self.observe_open_interest(
                            spec,
                            oi_f,
                            ts_ms.saturating_mul(1_000_000),
                        );
                    }
                }
            }
        }
        total
    }

    pub fn evaluate_wave_entry_modulation(
        obs: WaveObservables,
        side: Side,
        btc_signed_snr: f64,
    ) -> Result<WaveEntryModulation, &'static str> {
        let dir_s = side.sign_f64();
        let signed_phase = obs.macro_phase.get() * dir_s;
        if obs.climax_active && signed_phase > 0.0 {
            return Err("VETO_WAVE5_LIQ_CLIMAX_EXHAUSTION");
        }
        let aligned_fib = obs.fib_proj_bps.raw().saturating_mul(side.sign_i64()).max(0);
        match obs.regime {
            WaveRegime::NeutralWarmup => Ok(WaveEntryModulation::default()),
            WaveRegime::ClimaxExhaustion => {
                if signed_phase > 0.0 {
                    Err("VETO_WAVE5_LIQ_CLIMAX_EXHAUSTION")
                } else {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: -0.010,
                        def_horizon_permille_mult: 850,
                        tranche1_permille_delta: 60,
                        fib_target_bps: ScaledBps(0),
                    })
                }
            }
            WaveRegime::ImpulseWave3 => {
                if signed_phase > 0.15 {
                    let h_bonus = ((obs.hurst_exponent() - 0.52) * 0.08).clamp(0.0, 0.015);
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: (-0.015 - h_bonus).clamp(-0.030, -0.010),
                        def_horizon_permille_mult: 1_180,
                        tranche1_permille_delta: -60,
                        fib_target_bps: ScaledBps(aligned_fib),
                    })
                } else if signed_phase < -0.15 && btc_signed_snr < 0.20 {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: 0.045,
                        def_horizon_permille_mult: 820,
                        tranche1_permille_delta: 90,
                        fib_target_bps: ScaledBps(0),
                    })
                } else {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: 0.010,
                        def_horizon_permille_mult: 1_000,
                        tranche1_permille_delta: 0,
                        fib_target_bps: ScaledBps(aligned_fib),
                    })
                }
            }
            WaveRegime::ImpulseWave5 => {
                if signed_phase > 0.15 {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: -0.008,
                        def_horizon_permille_mult: 1_050,
                        tranche1_permille_delta: 40,
                        fib_target_bps: ScaledBps(aligned_fib),
                    })
                } else if signed_phase < -0.15 {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: 0.025,
                        def_horizon_permille_mult: 860,
                        tranche1_permille_delta: 80,
                        fib_target_bps: ScaledBps(0),
                    })
                } else {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: 0.0,
                        def_horizon_permille_mult: 1_000,
                        tranche1_permille_delta: 20,
                        fib_target_bps: ScaledBps(aligned_fib),
                    })
                }
            }
            WaveRegime::CorrectiveWave4 => Ok(WaveEntryModulation {
                snr_threshold_delta: if signed_phase >= 0.0 { 0.005 } else { 0.020 },
                def_horizon_permille_mult: 900,
                tranche1_permille_delta: 50,
                fib_target_bps: ScaledBps(0),
            }),
            WaveRegime::CorrectiveAbc => {
                if signed_phase < -0.35 && btc_signed_snr < 0.15 {
                    Err("VETO_WAVE_ABC_STRUCTURAL_HEADWIND")
                } else {
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: if signed_phase < 0.0 { 0.025 } else { 0.0 },
                        def_horizon_permille_mult: 880,
                        tranche1_permille_delta: 60,
                        fib_target_bps: ScaledBps(0),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_elliott_impulse_vs_corrective_bounce_symmetry_and_climax() {
        let avax = APPROVED_SPECS[3];
        let mut bull = CausalElliottWaveState::new();
        let vol = ScaledBps(5_000);
        for &(px, step) in &[(10_000_i64, 1_i64), (10_100, 2), (10_038, 3), (10_080, 4)] {
            let _ = bull.observe_price_tick(avax, PriceTicks(px), step * 1_000_000_000, vol);
        }
        let obs_w3 = bull.observables();
        assert_eq!(obs_w3.regime, WaveRegime::ImpulseWave3);
        assert!(obs_w3.fib_proj_bps.raw() > 800);
        let mod_w3 = CausalElliottWaveState::evaluate_wave_entry_modulation(obs_w3, Side::Bid, 0.4);
        assert!(mod_w3.is_ok_and(|m| m.def_horizon_permille_mult == 1_180 && m.tranche1_permille_delta == -60));

        let mut bear = CausalElliottWaveState::new();
        for &(px, step) in &[(10_000_i64, 1_i64), (9_900, 2), (9_962, 3), (9_920, 4)] {
            let _ = bear.observe_price_tick(avax, PriceTicks(px), step * 1_000_000_000, vol);
        }
        assert_eq!(bear.observables().regime, WaveRegime::ImpulseWave3);
        assert!(bear.observables().fib_proj_bps.raw() < -800);

        let _ = bull.observe_price_tick(avax, PriceTicks(10_200), 5_000_000_000, vol);
        let _ = bull.observe_price_tick(avax, PriceTicks(10_090), 6_000_000_000, vol);
        let _ = bull.observe_price_tick(avax, PriceTicks(10_150), 7_000_000_000, vol);
        assert_eq!(bull.observables().regime, WaveRegime::CorrectiveAbc);

        let mut w5 = CausalElliottWaveState::new();
        for &(px, st) in &[
            (10_000_i64, 1_i64),
            (10_100, 2),
            (10_038, 3),
            (10_210, 4),
            (10_145, 5),
            (10_220, 6),
        ] {
            let _ = w5.observe_price_tick(avax, PriceTicks(px), st * 2_000_000_000, vol);
        }
        assert_eq!(w5.observables().regime, WaveRegime::ImpulseWave5);
        let _ = w5.observe_open_interest(avax, 1_000_000.0, 12_000_000_000);
        let _ = w5.observe_open_interest(avax, 970_000.0, 14_000_000_000);
        let liq_short_json = serde_json::json!({
            "s": "AVAXUSDT",
            "S": "Sell",
            "v": "500.0",
            "p": "10.220"
        });
        assert_eq!(
            parse_bybit_all_liquidation_event(avax, &liq_short_json),
            Some((Side::Bid, LotQty(5_000), PriceTicks(10_220)))
        );
        let climax_obs = w5.observe_bybit_liquidation_json(avax, &liq_short_json, 15_000_000_000);
        assert!(climax_obs.is_some_and(|o| o.regime == WaveRegime::ClimaxExhaustion && o.climax_active));
        assert!(CausalElliottWaveState::evaluate_wave_entry_modulation(
            w5.observables(),
            Side::Bid,
            0.50,
        )
        .is_err());
    }

    #[test]
    fn test_bybit_v5_live_or_cached_12pair_elliott_statistical_validation() {
        let k5 = serde_json::json!({
            "result": {
                "list": [
                    ["1700001200000", "8.300", "8.340", "8.290", "8.330"],
                    ["1700000900000", "8.240", "8.260", "8.230", "8.245"],
                    ["1700000600000", "8.290", "8.305", "8.235", "8.240"],
                    ["1700000300000", "8.200", "8.300", "8.195", "8.295"],
                    ["1700000000000", "8.200", "8.210", "8.190", "8.200"]
                ]
            }
        });
        let oi = serde_json::json!({
            "result": {
                "list": [
                    {"openInterest": "1050000", "timestamp": "1700001200000"},
                    {"openInterest": "1000000", "timestamp": "1700000000000"}
                ]
            }
        });
        for spec in &APPROVED_SPECS {
            let mut st = CausalElliottWaveState::new();
            let bars = st.warm_start_from_bybit_bundle(*spec, Some(&k5), None, Some(&oi));
            assert_eq!(bars, 5);
            assert!(st.observables().oi_impulse.get() > 0.0);
            assert!((0.25..=0.85).contains(&st.observables().hurst_exponent()));
        }
    }
}
