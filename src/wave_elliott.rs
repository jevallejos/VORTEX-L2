//! Causal 6-Pivot Elliott Wave v2.0 Automaton, Endogenous Liquidation Floor & C1 Hermite Surface (V4.6).
#![allow(
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use crate::types::{BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs};

const PHI: f64 = 1.618_033_988_749_895;
const PHI_SQ: f64 = 2.618_033_988_749_895;
const PHI_CU: f64 = 4.236_067_977_499_79;
const INV_PHI: f64 = 0.618_033_988_749_895;
const GP_STRICT_HI: f64 = 0.650;
const GP_CENTER: f64 = f64::midpoint(INV_PHI, GP_STRICT_HI);
const GP_HALF_WIDTH: f64 = 0.5 * (GP_STRICT_HI - INV_PHI);
const GP_SIGMA_OUTER: f64 = 0.115;
const FLOW_TAU_SECS: f64 = 300.0;
const LIQ_DECAY_TAU_SECS: f64 = 15.0;

/// Tick-dilated C^1 Golden Pocket [0.618, 0.650] resonance kernel in [0.0, 1.0].
#[inline]
#[must_use]
pub fn compute_golden_pocket_resonance(w1_ticks: i64, w2_ticks: i64) -> f64 {
    if w1_ticks <= 0 || w2_ticks <= 0 || w2_ticks >= w1_ticks {
        return 0.0;
    }
    let r2 = (w2_ticks as f64) / (w1_ticks as f64);
    if !(0.236..0.990).contains(&r2) {
        return 0.0;
    }
    let delta_core = GP_HALF_WIDTH.max(0.55 / (w1_ticks as f64));
    let dist = ((r2 - GP_CENTER).abs() - delta_core).max(0.0);
    if dist <= 0.0 {
        1.0
    } else {
        let z = dist / GP_SIGMA_OUTER;
        (-0.5 * z * z).exp()
    }
}

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

    #[inline]
    #[must_use]
    pub fn golden_pocket_resonance(self, side: Side) -> f64 {
        if !matches!(self.regime, WaveRegime::ImpulseWave3) {
            return 0.0;
        }
        let signed_phase = self.macro_phase.get() * side.sign_f64();
        if signed_phase <= 0.68 {
            0.0
        } else {
            ((signed_phase - 0.68) / 0.27).clamp(0.0, 1.0)
        }
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

/// Parses Bybit V5 `allLiquidation` item (`"Sell"` Short Liq -> `Bid`, `"Buy"` Long Liq -> `Ask`).
#[must_use]
pub fn parse_bybit_all_liquidation_event(
    spec: PairSpec,
    item: &serde_json::Value,
) -> Option<(Side, LotQty, PriceTicks)> {
    let raw_pos_side = item.get("S").or_else(|| item.get("side")).and_then(serde_json::Value::as_str)?;
    let forced_book_side = match raw_pos_side {
        "Sell" => Side::Bid,
        "Buy" => Side::Ask,
        _ => return None,
    };
    let q_str = item.get("v").or_else(|| item.get("size")).and_then(serde_json::Value::as_str)?;
    let qty = spec.parse_lot_qty(q_str);
    if qty.0 <= 0 {
        return None;
    }
    let px = item
        .get("p")
        .or_else(|| item.get("price"))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| spec.parse_price_ticks(s).ok())
        .unwrap_or(PriceTicks(0));
    Some((forced_book_side, qty, px))
}

/// O(1) Zero-Alloc endogenous liquidation denominator anchored to each pair's own trade notional.
#[derive(Copy, Clone, Debug)]
pub struct EndogenousLiquidationFloor {
    ewma_trade_notional: f64,
    seed_floor: f64,
    forced_buy_accum: f64,
    forced_sell_accum: f64,
    last_ts_ns: i64,
}

impl Default for EndogenousLiquidationFloor {
    #[inline]
    fn default() -> Self {
        Self::new_const()
    }
}

impl EndogenousLiquidationFloor {
    #[inline]
    #[must_use]
    pub const fn new_const() -> Self {
        Self {
            ewma_trade_notional: 50.0,
            seed_floor: 50.0 * PHI_SQ,
            forced_buy_accum: 0.0,
            forced_sell_accum: 0.0,
            last_ts_ns: 0,
        }
    }

    #[inline]
    #[must_use]
    pub fn new_for_spec(spec: PairSpec, ref_mid: PriceTicks) -> Self {
        let px_usdt = ((ref_mid.0.max(1) as f64) * (spec.tick_size_1e8.max(1) as f64)) * 1e-8;
        let step_qty = (spec.lot_step_1e8.max(1) as f64) * 1e-8;
        let min_notional = (px_usdt * step_qty).max(5.0);
        let seed_trade = (PHI_CU * min_notional).clamp(20.0, 9_500.0);
        let init_floor = (PHI_SQ * seed_trade).clamp(50.0, 25_000.0);
        Self {
            ewma_trade_notional: seed_trade,
            seed_floor: init_floor,
            forced_buy_accum: 0.0,
            forced_sell_accum: 0.0,
            last_ts_ns: 0,
        }
    }

    #[inline]
    fn decay_to_timestamp(&mut self, ts_ns: TimestampNs) -> f64 {
        if ts_ns.0 <= 0 {
            return 0.0;
        }
        if self.last_ts_ns <= 0 || ts_ns.0 <= self.last_ts_ns {
            if self.last_ts_ns <= 0 {
                self.last_ts_ns = ts_ns.0;
            }
            return 0.0;
        }
        let dt_s = (((ts_ns.0 - self.last_ts_ns) as f64) * 1e-9).clamp(0.0, 120.0);
        let liq_decay = (-dt_s / LIQ_DECAY_TAU_SECS).exp();
        self.forced_buy_accum *= liq_decay;
        self.forced_sell_accum *= liq_decay;
        self.last_ts_ns = ts_ns.0;
        dt_s
    }

    #[inline]
    #[must_use]
    pub fn print_notional_usdt(spec: PairSpec, price: PriceTicks, qty: LotQty) -> f64 {
        let px_usdt = ((price.0.max(0) as f64) * (spec.tick_size_1e8.max(1) as f64)) * 1e-8;
        let units = ((qty.0.max(0) as f64) * (spec.lot_step_1e8.max(1) as f64)) * 1e-8;
        (px_usdt * units).max(0.0)
    }

    #[inline]
    pub fn observe_trade_print(&mut self, spec: PairSpec, ts_ns: TimestampNs, price: PriceTicks, qty: LotQty) {
        let dt_s = self.decay_to_timestamp(ts_ns);
        let notional = Self::print_notional_usdt(spec, price, qty);
        if notional <= 0.0 {
            return;
        }
        let alpha = (1.0 - (-dt_s.max(0.05) / FLOW_TAU_SECS).exp()).clamp(0.002, 0.25);
        self.ewma_trade_notional =
            (1.0 - alpha) * self.ewma_trade_notional + alpha * notional.clamp(5.0, 100_000.0);
    }

    #[inline]
    #[must_use]
    pub fn floor_usdt(&self) -> f64 {
        if self.ewma_trade_notional <= 0.0 {
            self.seed_floor.clamp(50.0, 25_000.0)
        } else {
            (PHI_SQ * self.ewma_trade_notional).clamp(50.0, 25_000.0)
        }
    }

    #[inline]
    pub fn observe_liquidation_print(
        &mut self,
        spec: PairSpec,
        ts_ns: TimestampNs,
        forced_book_side: Side,
        price: PriceTicks,
        qty: LotQty,
    ) -> BoundedScore {
        let _ = self.decay_to_timestamp(ts_ns);
        let notional = Self::print_notional_usdt(spec, price, qty);
        match forced_book_side {
            Side::Bid => self.forced_buy_accum += notional,
            Side::Ask => self.forced_sell_accum += notional,
        }
        self.current_imbalance()
    }

    #[inline]
    #[must_use]
    pub fn current_imbalance(&self) -> BoundedScore {
        let denom = self.floor_usdt() + self.forced_buy_accum + self.forced_sell_accum;
        let z = ((self.forced_buy_accum - self.forced_sell_accum) / denom.max(1.0)).tanh();
        BoundedScore::new_clamped(z)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct CausalElliottWaveState {
    pivots: [WavePivot; 6],
    pivot_len: u8,
    zig_dir: i8,
    floor_seeded: bool,
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
    liq_floor: EndogenousLiquidationFloor,
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
        let init_fl = EndogenousLiquidationFloor::new_const();
        Self {
            pivots: [WavePivot { kind_sign: 0, price_ticks: PriceTicks(0), ts_ns: TimestampNs(0) }; 6],
            pivot_len: 0,
            zig_dir: 0,
            floor_seeded: false,
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
            liq_floor: init_fl,
            ewma_liq_signed_usdt: 0.0,
            ewma_liq_abs_fast_usdt: 0.0,
            ewma_liq_abs_slow_usdt: 50.0 * PHI_SQ,
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
    pub fn reset(&mut self) { *self = Self::new(); }
    #[inline]
    #[must_use]
    pub const fn observables(&self) -> WaveObservables { self.cached_obs }
    #[inline]
    #[must_use]
    pub const fn pivot_count(&self) -> usize { self.pivot_len as usize }
    #[inline]
    #[must_use]
    pub fn active_liquidation_floor_usdt(&self) -> f64 { self.liq_floor.floor_usdt() }

    #[inline]
    fn ensure_floor_seeded(&mut self, spec: PairSpec, ref_px: PriceTicks) {
        if !self.floor_seeded && ref_px.0 > 0 {
            self.liq_floor = EndogenousLiquidationFloor::new_for_spec(spec, ref_px);
            self.ewma_liq_abs_slow_usdt = self.liq_floor.floor_usdt();
            self.floor_seeded = true;
        }
    }

    #[inline]
    pub fn observe_public_trade_notional(&mut self, spec: PairSpec, ts_ns: TimestampNs, price: PriceTicks, qty: LotQty) {
        self.ensure_floor_seeded(spec, price);
        self.liq_floor.observe_trade_print(spec, ts_ns, price, qty);
        let fl = self.liq_floor.floor_usdt();
        if self.ewma_liq_abs_slow_usdt < fl {
            self.ewma_liq_abs_slow_usdt = fl;
        }
    }

    #[inline]
    #[must_use]
    pub fn reversal_threshold_bps_dynamic(
        spec: PairSpec,
        mid_px: PriceTicks,
        vol_5m: ScaledBps,
        taker_prob_pm: i64,
        fees: crate::fee_cadence_beta::FeeSchedule,
    ) -> i64 {
        let b3x = spec.three_x_friction_barrier_bps_dynamic(mid_px, taker_prob_pm, fees).raw().max(450);
        let raw = (INV_PHI * (vol_5m.raw().max(b3x) as f64)).round() as i64;
        raw.clamp(b3x, b3x.saturating_mul(8))
    }

    #[inline]
    fn reversal_threshold_bps(spec: PairSpec, mid_px: PriceTicks, vol_5m: ScaledBps) -> i64 {
        Self::reversal_threshold_bps_dynamic(
            spec, mid_px, vol_5m, 50, crate::fee_cadence_beta::FeeSchedule::VIP0_DEFAULT,
        )
    }

    #[inline]
    fn push_pivot(&mut self, pivot: WavePivot) {
        let len = self.pivot_len as usize;
        if len < 6 {
            self.pivots[len] = pivot;
            self.pivot_len += 1;
        } else {
            for i in 0..5 { self.pivots[i] = self.pivots[i + 1]; }
            self.pivots[5] = pivot;
        }
    }

    fn step_zigzag(&mut self, spec: PairSpec, px: PriceTicks, ts_ns: i64, vol_5m: ScaledBps) {
        if px.0 <= 0 { return; }
        let ts = TimestampNs(ts_ns);
        let rev_bps = Self::reversal_threshold_bps(spec, px, vol_5m);
        if self.zig_dir == 0 {
            if self.anchor_px.0 <= 0 {
                self.anchor_px = px;
                self.anchor_ts = ts;
                return;
            }
            let move_bps = ((px.0 - self.anchor_px.0).saturating_mul(1_000_000)) / self.anchor_px.0.max(1);
            if move_bps >= rev_bps {
                self.push_pivot(WavePivot { kind_sign: -1, price_ticks: self.anchor_px, ts_ns: self.anchor_ts });
                self.push_pivot(WavePivot { kind_sign: 1, price_ticks: px, ts_ns: ts });
                self.zig_dir = 1;
            } else if move_bps <= -rev_bps {
                self.push_pivot(WavePivot { kind_sign: 1, price_ticks: self.anchor_px, ts_ns: self.anchor_ts });
                self.push_pivot(WavePivot { kind_sign: -1, price_ticks: px, ts_ns: ts });
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
            } else if ((last_px.0 - px.0).saturating_mul(1_000_000)) / last_px.0.max(1) >= rev_bps {
                self.push_pivot(WavePivot { kind_sign: -1, price_ticks: px, ts_ns: ts });
                self.zig_dir = -1;
            }
        } else if px.0 <= last_px.0 {
            self.pivots[last_idx].price_ticks = px;
            self.pivots[last_idx].ts_ns = ts;
        } else if ((px.0 - last_px.0).saturating_mul(1_000_000)) / last_px.0.max(1) >= rev_bps {
            self.push_pivot(WavePivot { kind_sign: 1, price_ticks: px, ts_ns: ts });
            self.zig_dir = 1;
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
        if ret_bps.abs() < 1e-6 { return self.cached_obs.hurst_exponent(); }
        let idx = (self.ring_idx as usize) & 3;
        self.ring_ret4_bps[idx] = ret_bps;
        self.ring_idx = self.ring_idx.wrapping_add(1);
        let sum4 = self.ring_ret4_bps[0] + self.ring_ret4_bps[1] + self.ring_ret4_bps[2] + self.ring_ret4_bps[3];
        self.ewma_var1_bps2 = 0.92 * self.ewma_var1_bps2 + 0.08 * (ret_bps * ret_bps).clamp(0.01, 250_000.0);
        self.ewma_var4_bps2 = 0.92 * self.ewma_var4_bps2 + 0.08 * (sum4 * sum4).clamp(0.04, 1_000_000.0);
        let vr4 = (self.ewma_var4_bps2 / (4.0 * self.ewma_var1_bps2).max(0.04)).clamp(0.35, 2.65);
        (0.50 + vr4.ln() / (2.0 * 4.0_f64.ln())).clamp(0.25, 0.85)
    }

    fn decay_liquidation_state(&mut self, ts_ns: i64) {
        if self.last_liq_ts_ns <= 0 || ts_ns <= self.last_liq_ts_ns {
            if self.last_liq_ts_ns <= 0 && ts_ns > 0 { self.last_liq_ts_ns = ts_ns; }
            return;
        }
        let dt_s = ((ts_ns - self.last_liq_ts_ns) as f64 * 1e-9).clamp(0.0, 600.0);
        self.last_liq_ts_ns = ts_ns;
        let d_fast = (-dt_s / LIQ_DECAY_TAU_SECS).exp();
        let d_slow = (-dt_s / FLOW_TAU_SECS).exp();
        let fl = self.liq_floor.floor_usdt();
        self.ewma_liq_signed_usdt *= d_fast;
        self.ewma_liq_abs_fast_usdt *= d_fast;
        self.ewma_liq_abs_slow_usdt = (self.ewma_liq_abs_slow_usdt * d_slow).max(fl);
    }

    #[inline]
    fn liq_imbalance_and_z(&self) -> (f64, f64) {
        let fl = self.liq_floor.floor_usdt();
        let denom = (self.ewma_liq_abs_fast_usdt + fl).max(1.0);
        let i_liq = (self.ewma_liq_signed_usdt / denom).clamp(-1.0, 1.0);
        let z_liq = self.ewma_liq_abs_fast_usdt / self.ewma_liq_abs_slow_usdt.max(fl);
        (i_liq, z_liq)
    }

    fn recompute_observables(&mut self, hurst_val: f64) -> WaveObservables {
        let px = self.last_mid_px.0;
        let (i_liq, z_liq) = self.liq_imbalance_and_z();
        let i_oi = self.cached_obs.oi_impulse.get();
        let len = self.pivot_len as usize;
        if px <= 0 || len < 3 {
            let obs = WaveObservables {
                regime: WaveRegime::NeutralWarmup, climax_active: false, hurst: WaveObservables::encode_hurst(hurst_val),
                macro_phase: BoundedScore::ZERO, oi_impulse: self.cached_obs.oi_impulse,
                liq_imbalance: BoundedScore::new_clamped(i_liq), fib_proj_bps: ScaledBps(0),
            };
            self.cached_obs = obs;
            return obs;
        }
        let (mut regime, mut phase, mut fib_proj_bps, mut climax_active, mut matched) =
            (WaveRegime::CorrectiveAbc, 0.0_f64, 0_i64, false, false);
        if len == 6 {
            let (p0, p1, p2, p3, p4) = (
                self.pivots[0].price_ticks.0, self.pivots[1].price_ticks.0, self.pivots[2].price_ticks.0,
                self.pivots[3].price_ticks.0, self.pivots[4].price_ticks.0,
            );
            let s = i64::from(-self.pivots[0].kind_sign);
            let sf = s as f64;
            let (w1, w2, w3, w4) = ((p1 - p0) * s, (p1 - p2) * s, (p3 - p2) * s, (p3 - p4) * s);
            let e1_ok = (p2 - p0) * s > 0 && w1 > 0 && w2 > 0 && (w2 as f64) < 0.99 * (w1 as f64);
            let e2_w3_valid = w3 >= w1;
            let e3_ok = (p4 - p1) * s > 0 && w3 > 0 && w4 > 0 && (w4 as f64) <= (INV_PHI + 0.06) * (w3 as f64);
            if e1_ok && e3_ok && (px - p4) * s > 0 {
                let w5_cur = (px - p4) * s;
                if e2_w3_valid || w5_cur < w3 {
                    matched = true;
                    let fuel = (0.60 * (sf * i_liq).max(0.0) * (0.45 * z_liq).tanh() + 0.40 * i_oi.max(0.0)).clamp(0.0, 1.0);
                    if z_liq >= 3.5 && (sf * i_liq) >= 0.55 && i_oi <= -0.12 {
                        regime = WaveRegime::ClimaxExhaustion;
                        climax_active = true;
                        phase = 0.92 * sf;
                    } else {
                        regime = WaveRegime::ImpulseWave5;
                        let k5 = 1.0 + (PHI - 1.0) * (1.25 * fuel - 0.25).clamp(0.0, 1.0);
                        let raw_w5 = (k5 * (w1 as f64).max(INV_PHI * ((p3 - p0).abs() as f64))).round() as i64;
                        let cap_w5 = if e2_w3_valid { raw_w5 } else { raw_w5.min(w3 - 1) };
                        let p_fib5 = p4 + s * cap_w5.max(1);
                        if (p_fib5 - px) * s > 0 {
                            fib_proj_bps = ((p_fib5 - px).saturating_mul(1_000_000)) / px.max(1);
                        }
                        phase = 0.68 * sf;
                    }
                }
            } else if e1_ok && !e3_ok {
                matched = true;
                regime = WaveRegime::CorrectiveAbc;
                phase = -0.45 * sf;
            }
        }
        if !matched && len >= 5 {
            let b = len - 5;
            let (p0, p1, p2, p3) = (
                self.pivots[b].price_ticks.0, self.pivots[b + 1].price_ticks.0,
                self.pivots[b + 2].price_ticks.0, self.pivots[b + 3].price_ticks.0,
            );
            let s = i64::from(-self.pivots[b].kind_sign);
            let (w1, w2, w3, w4_cur) = ((p1 - p0) * s, (p1 - p2) * s, (p3 - p2) * s, (p3 - px) * s);
            if (p2 - p0) * s > 0 && w1 > 0 && w2 > 0 && w3 >= w1 && w4_cur > 0
                && (px - p1) * s > 0 && (w4_cur as f64) <= (INV_PHI + 0.06) * (w3 as f64)
            {
                matched = true;
                regime = WaveRegime::CorrectiveWave4;
                phase = 0.25 * (s as f64);
            }
        }
        if !matched && len >= 3 {
            let b = if len >= 4 && self.pivots[len - 1].kind_sign != self.pivots[len - 4].kind_sign { len - 4 } else { len - 3 };
            let (p0, p1, p2) = (self.pivots[b].price_ticks.0, self.pivots[b + 1].price_ticks.0, self.pivots[b + 2].price_ticks.0);
            let s = i64::from(-self.pivots[b].kind_sign);
            let sf = s as f64;
            let (w1, w2) = ((p1 - p0) * s, (p1 - p2) * s);
            let r2 = if w1 > 0 { (w2 as f64) / (w1 as f64) } else { 0.0 };
            if (p2 - p0) * s > 0 && (0.236..0.990).contains(&r2) && (px - p2) * s > 0 {
                let fuel = (0.60 * (sf * i_liq).max(0.0) * (0.45 * z_liq).tanh() + 0.40 * i_oi.max(0.0)).clamp(0.0, 1.0);
                if z_liq >= 4.2 && (sf * i_liq) >= 0.65 && i_oi <= -0.18 {
                    regime = WaveRegime::ClimaxExhaustion;
                    climax_active = true;
                    phase = 0.90 * sf;
                } else {
                    regime = WaveRegime::ImpulseWave3;
                    let gp_res = compute_golden_pocket_resonance(w1, w2);
                    let h_boost = ((hurst_val - 0.54) / 0.14).clamp(0.0, 1.0);
                    let k3 = PHI + (PHI_SQ - PHI) * (0.45 * h_boost + 0.35 * fuel + 0.20 * gp_res).clamp(0.0, 1.0);
                    let p_fib3 = p2 + s * ((k3 * (w1 as f64)).round() as i64).max(1);
                    if (p_fib3 - px) * s > 0 {
                        fib_proj_bps = ((p_fib3 - px).saturating_mul(1_000_000)) / px.max(1);
                    }
                    phase = (0.68 + 0.27 * gp_res) * sf;
                }
            } else {
                phase = 0.30 * f64::from(self.zig_dir);
            }
        }
        let obs = WaveObservables {
            regime, climax_active, hurst: WaveObservables::encode_hurst(hurst_val),
            macro_phase: BoundedScore::new_clamped(phase), oi_impulse: self.cached_obs.oi_impulse,
            liq_imbalance: BoundedScore::new_clamped(i_liq), fib_proj_bps: ScaledBps(fib_proj_bps),
        };
        self.cached_obs = obs;
        obs
    }

    pub fn observe_price_tick(&mut self, spec: PairSpec, mid_px: PriceTicks, ts_ns: i64, realized_vol_5m: ScaledBps) -> WaveObservables {
        if mid_px.0 <= 0 { return self.cached_obs; }
        self.ensure_floor_seeded(spec, mid_px);
        self.last_mid_px = mid_px;
        self.decay_liquidation_state(ts_ns);
        let h_val = self.update_hurst_vr(mid_px, ts_ns);
        self.step_zigzag(spec, mid_px, ts_ns, realized_vol_5m);
        self.recompute_observables(h_val)
    }

    pub fn observe_open_interest(&mut self, spec: PairSpec, open_interest: f64, ts_ns: i64) -> WaveObservables {
        if !open_interest.is_finite() || open_interest <= 0.0 { return self.cached_obs; }
        self.ensure_floor_seeded(spec, self.last_mid_px);
        if self.ewma_oi_fast <= 0.0 || self.ewma_oi_slow <= 0.0 {
            self.ewma_oi_fast = open_interest;
            self.ewma_oi_slow = open_interest;
            self.last_oi_ts_ns = ts_ns;
            return self.cached_obs;
        }
        let dt_s = if ts_ns > self.last_oi_ts_ns && self.last_oi_ts_ns > 0 {
            ((ts_ns - self.last_oi_ts_ns) as f64 * 1e-9).clamp(1.0, 300.0)
        } else { 5.0 };
        self.last_oi_ts_ns = ts_ns;
        self.ewma_oi_fast += (1.0 - (-dt_s / 30.0).exp()).clamp(0.02, 1.0) * (open_interest - self.ewma_oi_fast);
        self.ewma_oi_slow += (1.0 - (-dt_s / 300.0).exp()).clamp(0.005, 1.0) * (open_interest - self.ewma_oi_slow);
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
        if qty.0 <= 0 { return self.cached_obs; }
        let ref_px = if liq_price.0 > 0 { liq_price } else if self.last_mid_px.0 > 0 { self.last_mid_px } else { PriceTicks(1_000) };
        self.ensure_floor_seeded(spec, ref_px);
        self.decay_liquidation_state(ts_ns);
        let _ = self.liq_floor.observe_liquidation_print(spec, TimestampNs(ts_ns), forced_book_side, ref_px, qty);
        let notional_usdt = (spec.lot_notional_1e8(qty, ref_px) as f64) * 1e-8;
        let fl = self.liq_floor.floor_usdt();
        self.ewma_liq_signed_usdt += notional_usdt * forced_book_side.sign_f64();
        self.ewma_liq_abs_fast_usdt += notional_usdt;
        self.ewma_liq_abs_slow_usdt = 0.96 * self.ewma_liq_abs_slow_usdt + 0.04 * notional_usdt.max(fl);
        let h_val = self.cached_obs.hurst_exponent();
        self.recompute_observables(h_val)
    }

    #[inline]
    pub fn observe_liquidation_print(&mut self, spec: PairSpec, forced_book_side: Side, qty: LotQty, ts_ns: i64) -> WaveObservables {
        self.observe_liquidation_with_price(spec, forced_book_side, qty, self.last_mid_px, ts_ns)
    }

    pub fn observe_bybit_liquidation_json(&mut self, spec: PairSpec, item: &serde_json::Value, ts_ns: i64) -> Option<WaveObservables> {
        let (forced_side, qty, px) = parse_bybit_all_liquidation_event(spec, item)?;
        Some(self.observe_liquidation_with_price(spec, forced_side, qty, px, ts_ns))
    }

    fn ingest_kline_bundle(&mut self, spec: PairSpec, kline_json: &serde_json::Value) -> usize {
        let Some(rows) = kline_json.get("result").and_then(|r| r.get("list")).and_then(serde_json::Value::as_array) else { return 0 };
        let mut count = 0_usize;
        for row in rows.iter().rev() {
            let Some(arr) = row.as_array() else { continue };
            if arr.len() < 5 { continue; }
            let ts_ms = arr[0].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let (Some(hi), Some(lo), Some(cl)) = (
                arr[2].as_str().and_then(|s| spec.parse_price_ticks(s).ok()),
                arr[3].as_str().and_then(|s| spec.parse_price_ticks(s).ok()),
                arr[4].as_str().and_then(|s| spec.parse_price_ticks(s).ok()),
            ) else { continue };
            let vol_est = ScaledBps((((hi.0 - lo.0).abs().saturating_mul(1_000_000)) / cl.0.max(1)).max(2_500));
            let _ = self.observe_price_tick(spec, cl, ts_ms.saturating_mul(1_000_000), vol_est);
            count += 1;
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
        if let Some(k5) = kline_5m { total += self.ingest_kline_bundle(spec, k5); }
        if let Some(k1) = kline_1m { total += self.ingest_kline_bundle(spec, k1); }
        if let Some(rows) = oi_5m.and_then(|v| v.get("result")).and_then(|r| r.get("list")).and_then(serde_json::Value::as_array) {
            for item in rows.iter().rev() {
                let oi_f = item.get("openInterest").and_then(serde_json::Value::as_str).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let ts_ms = item.get("timestamp").and_then(serde_json::Value::as_str).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                if oi_f > 0.0 { let _ = self.observe_open_interest(spec, oi_f, ts_ms.saturating_mul(1_000_000)); }
            }
        }
        total
    }

    pub fn evaluate_wave_entry_modulation(obs: WaveObservables, side: Side, btc_signed_snr: f64) -> Result<WaveEntryModulation, &'static str> {
        let signed_phase = obs.macro_phase.get() * side.sign_f64();
        if obs.climax_active && signed_phase > 0.0 {
            return Err("VETO_WAVE5_LIQ_CLIMAX_EXHAUSTION");
        }
        let aligned_fib = obs.fib_proj_bps.raw().saturating_mul(side.sign_i64()).max(0);
        match obs.regime {
            WaveRegime::NeutralWarmup => Ok(WaveEntryModulation::default()),
            WaveRegime::ClimaxExhaustion => {
                if signed_phase > 0.0 { Err("VETO_WAVE5_LIQ_CLIMAX_EXHAUSTION") } else {
                    Ok(WaveEntryModulation { snr_threshold_delta: -0.010, def_horizon_permille_mult: 850, tranche1_permille_delta: 60, fib_target_bps: ScaledBps(0) })
                }
            }
            WaveRegime::ImpulseWave3 => {
                if signed_phase > 0.15 {
                    let h_val = obs.hurst_exponent();
                    let h_bonus = ((h_val - 0.52) * 0.08).clamp(0.0, 0.015);
                    let gp_res = obs.golden_pocket_resonance(side);
                    let sniper_gp = gp_res >= 0.85 && h_val >= 0.54;
                    let gp_snr_bonus = if h_val >= 0.52 { 0.010 * gp_res } else { 0.0 };
                    Ok(WaveEntryModulation {
                        snr_threshold_delta: (-0.015 - h_bonus - gp_snr_bonus).clamp(-0.038, -0.010),
                        def_horizon_permille_mult: if sniper_gp { 1_180 + (70.0 * gp_res).round() as i64 } else { 1_180 },
                        tranche1_permille_delta: if sniper_gp { -60 - (25.0 * gp_res).round() as i64 } else { -60 },
                        fib_target_bps: ScaledBps(aligned_fib),
                    })
                } else if signed_phase < -0.15 && btc_signed_snr < 0.20 {
                    Ok(WaveEntryModulation { snr_threshold_delta: 0.045, def_horizon_permille_mult: 820, tranche1_permille_delta: 90, fib_target_bps: ScaledBps(0) })
                } else {
                    Ok(WaveEntryModulation { snr_threshold_delta: 0.010, def_horizon_permille_mult: 1_000, tranche1_permille_delta: 0, fib_target_bps: ScaledBps(aligned_fib) })
                }
            }
            WaveRegime::ImpulseWave5 => {
                if signed_phase > 0.15 {
                    Ok(WaveEntryModulation { snr_threshold_delta: -0.008, def_horizon_permille_mult: 1_050, tranche1_permille_delta: 40, fib_target_bps: ScaledBps(aligned_fib) })
                } else if signed_phase < -0.15 {
                    Ok(WaveEntryModulation { snr_threshold_delta: 0.025, def_horizon_permille_mult: 860, tranche1_permille_delta: 80, fib_target_bps: ScaledBps(0) })
                } else {
                    Ok(WaveEntryModulation { snr_threshold_delta: 0.0, def_horizon_permille_mult: 1_000, tranche1_permille_delta: 20, fib_target_bps: ScaledBps(aligned_fib) })
                }
            }
            WaveRegime::CorrectiveWave4 => Ok(WaveEntryModulation {
                snr_threshold_delta: if signed_phase >= 0.0 { 0.005 } else { 0.020 },
                def_horizon_permille_mult: 900, tranche1_permille_delta: 50, fib_target_bps: ScaledBps(0),
            }),
            WaveRegime::CorrectiveAbc => {
                if signed_phase < -0.35 && btc_signed_snr < 0.15 { Err("VETO_WAVE_ABC_STRUCTURAL_HEADWIND") } else {
                    Ok(WaveEntryModulation { snr_threshold_delta: if signed_phase < 0.0 { 0.025 } else { 0.0 }, def_horizon_permille_mult: 880, tranche1_permille_delta: 60, fib_target_bps: ScaledBps(0) })
                }
            }
        }
    }
}

/// Cubic Hermite C1 smoothstep `S_3(u) = u^2 * (3 - 2u)` with zero slope at both boundaries.
#[inline]
#[must_use]
pub fn hermite_smoothstep_c1(x: f64, edge0: f64, edge1: f64) -> f64 {
    let span = edge1 - edge0;
    if span.abs() <= 1e-12 {
        return if x >= edge1 { 1.0 } else { 0.0 };
    }
    let u = ((x - edge0) / span).clamp(0.0, 1.0);
    u * u * (3.0 - 2.0 * u)
}

/// Continuous C1 Elliott-Hurst macro contradiction penalty in `[0.0, 1.0]` (replaces H=0.500 step).
#[inline]
#[must_use]
pub fn evaluate_elliott_hurst_coherence_penalty_c1(side: Side, wave: WaveObservables) -> f64 {
    let s_dir = side.sign_f64();
    let h_t = wave.hurst_exponent();
    let phi_al = s_dir * (f64::from(wave.macro_phase.to_bps_i32()) / 10_000.0);
    let oi_raw = f64::from(wave.oi_impulse.to_bps_i32()) / 10_000.0;
    let fib_al_bps = s_dir * ((wave.fib_proj_bps.raw() as f64) / 100.0);
    let psi_h = hermite_smoothstep_c1(h_t, 0.480, 0.525);
    let psi_phi = hermite_smoothstep_c1(-0.18 - phi_al, 0.0, 0.14);
    let psi_oi = hermite_smoothstep_c1(oi_raw, -0.26, -0.04);
    let branch_oi = psi_h * psi_phi * psi_oi;
    let branch_deep = hermite_smoothstep_c1(-0.55 - phi_al, 0.0, 0.14) * hermite_smoothstep_c1(h_t, 0.515, 0.590);
    let branch_fib = if matches!(wave.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5) {
        hermite_smoothstep_c1(-30.0 - fib_al_bps, 0.0, 35.0) * psi_h
    } else {
        0.0
    };
    branch_oi.max(branch_deep).max(branch_fib).clamp(0.0, 1.0)
}

#[inline]
#[must_use]
pub fn passes_elliott_hurst_coherence_c1(side: Side, wave: WaveObservables) -> bool {
    evaluate_elliott_hurst_coherence_penalty_c1(side, wave) < INV_PHI
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_elliott_impulse_climax_and_golden_pocket_lattice_invariance() {
        let avax = APPROVED_SPECS[3];
        let vol = ScaledBps(5_000);
        let mut w5 = CausalElliottWaveState::new();
        for &(px, st) in &[(10_000_i64, 1_i64), (10_100, 2), (10_038, 3), (10_210, 4), (10_145, 5), (10_220, 6)] {
            let _ = w5.observe_price_tick(avax, PriceTicks(px), st * 2_000_000_000, vol);
        }
        assert_eq!(w5.pivot_count(), 6);
        assert_eq!(w5.observables().regime, WaveRegime::ImpulseWave5);
        let _ = w5.observe_open_interest(avax, 1_000_000.0, 12_000_000_000);
        let _ = w5.observe_open_interest(avax, 970_000.0, 14_000_000_000);
        let liq_short = serde_json::json!({ "s": "AVAXUSDT", "S": "Sell", "v": "500.0", "p": "10.220" });
        let climax = w5.observe_bybit_liquidation_json(avax, &liq_short, 15_000_000_000);
        assert!(climax.is_some_and(|o| o.regime == WaveRegime::ClimaxExhaustion && o.climax_active));
        assert!(CausalElliottWaveState::evaluate_wave_entry_modulation(w5.observables(), Side::Bid, 0.50).is_err());

        for w1 in 3..=200_i64 {
            let mut best = 0.0_f64;
            for w2 in 1..w1 { best = best.max(compute_golden_pocket_resonance(w1, w2)); }
            assert!((best - 1.0).abs() < 1e-12);
        }
    }

    #[test]
    fn test_v46_internal_endogenous_liq_floor_btc_vs_wif_and_c1_trades_45_to_58() {
        let btc = APPROVED_SPECS[0];
        let wif = APPROVED_SPECS[8];
        let vol = ScaledBps(5_000);
        let mut btc_w = CausalElliottWaveState::new();
        let mut wif_w = CausalElliottWaveState::new();
        for &(b_px, w_px, st) in &[
            (680_000_i64, 1_600_i64, 1_i64), (686_800, 1_618, 2), (682_500, 1_606, 3),
            (694_000, 1_638, 4), (689_500, 1_626, 5), (695_000, 1_642, 6),
        ] {
            let _ = btc_w.observe_price_tick(btc, PriceTicks(b_px), st * 2_000_000_000, vol);
            let _ = wif_w.observe_price_tick(wif, PriceTicks(w_px), st * 2_000_000_000, vol);
        }
        let mut ts = 15_000_000_000_i64;
        for _ in 0..60 {
            ts += 5_000_000_000;
            btc_w.observe_public_trade_notional(btc, TimestampNs(ts), PriceTicks(695_000), LotQty(50));
            wif_w.observe_public_trade_notional(wif, TimestampNs(ts), PriceTicks(1_642), LotQty(150));
        }
        assert!(btc_w.active_liquidation_floor_usdt() > 3_500.0);
        assert!(wif_w.active_liquidation_floor_usdt() < 250.0);

        let _ = btc_w.observe_open_interest(btc, 100_000.0, ts + 1_000_000_000);
        let _ = btc_w.observe_open_interest(btc, 97_000.0, ts + 3_000_000_000);
        let _ = wif_w.observe_open_interest(wif, 100_000.0, ts + 1_000_000_000);
        let _ = wif_w.observe_open_interest(wif, 97_000.0, ts + 3_000_000_000);
        // Same ~$410 forced buy triggers ClimaxExhaustion on WIF while BTC stays ImpulseWave5!
        let o_btc = btc_w.observe_liquidation_with_price(btc, Side::Bid, LotQty(6), PriceTicks(695_000), ts + 4_000_000_000);
        let o_wif = wif_w.observe_liquidation_with_price(wif, Side::Bid, LotQty(2_500), PriceTicks(1_642), ts + 4_000_000_000);
        assert_eq!(o_btc.regime, WaveRegime::ImpulseWave5);
        assert_eq!(o_wif.regime, WaveRegime::ClimaxExhaustion);

        let mk_wave = |h: f64, m: f64, oi: f64| WaveObservables {
            regime: WaveRegime::NeutralWarmup, climax_active: false, hurst: WaveObservables::encode_hurst(h),
            macro_phase: BoundedScore::new_clamped(m), oi_impulse: BoundedScore::new_clamped(oi),
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
        };
        assert!(!passes_elliott_hurst_coherence_c1(Side::Bid, mk_wave(0.605, -0.69, -0.52)));
        assert!(passes_elliott_hurst_coherence_c1(Side::Bid, mk_wave(0.481, -0.45, 0.05)));
        assert!(passes_elliott_hurst_coherence_c1(Side::Ask, mk_wave(0.774, 0.45, -0.29)));
        assert!(passes_elliott_hurst_coherence_c1(Side::Ask, mk_wave(0.418, 0.85, -0.05)));
        assert!(passes_elliott_hurst_coherence_c1(Side::Ask, mk_wave(0.495, 0.70, 0.12)));
        assert!(!passes_elliott_hurst_coherence_c1(Side::Ask, mk_wave(0.560, 0.70, 0.05)));
        assert!(!passes_elliott_hurst_coherence_c1(Side::Ask, mk_wave(0.674, 0.30, 0.13)));
    }
}
