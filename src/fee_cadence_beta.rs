//! Dynamic VIP FeeSchedule, Beta-Binomial Taker Prior, Civil DST + Live Funding Cadence & Dollar Beta Guard.
#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::needless_range_loop,
    clippy::similar_names,
    clippy::too_many_arguments
)]

use core::f64::consts::TAU;

use crate::types::{
    PairSpec, PriceTicks, ScaledBps, Side, TemporalCadenceModulation,
    TimestampNs,
};

pub const MAX_PORTFOLIO_RISK_PPM: i64 = 14_400;
pub const MAX_DIRECTIONAL_BETA_RISK_PPM: i64 = 10_800;
pub const MAX_SIDE_OPEN_SLOTS: usize = 3;
pub const MAX_TOTAL_OPEN_SLOTS: usize = 6;

const PHI_F64: f64 = 1.618_033_988_749_895;
const INV_PHI_F64: f64 = 0.618_033_988_749_894_9;
const INV_PHI2_F64: f64 = 0.381_966_011_250_105_1;
const INV_PHI_PM: i64 = 618;
const INV_PHI2_PM: i64 = 382;
const INV_PHI3_PM: i64 = 236;
const REAL_UTC_MIN_EPOCH_SECS: i64 = 1_577_836_800; // 2020-01-01T00:00:00Z

/// Self-learned `C1` friction barrier multiplier in `[PHI, 3.0]` (`V5.5.4`):
/// `m_fric = PHI + (3.0 - PHI) * (1.0 - INV_PHI * psi_health * S_3(1.0 - p_T; 0.84, 0.95) * (INV_PHI + INV_PHI_SQ * iota))`
#[inline]
#[must_use]
pub fn compute_learned_friction_multiplier_c1(
    taker_prob_pm: i64,
    learned_ir_x100: i64,
    psi_health: f64,
) -> f64 {
    let psi_c = if psi_health.is_finite() {
        psi_health.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let p_t = ((taker_prob_pm as f64) * 1e-3).clamp(0.0, 1.0);
    let iota = ((learned_ir_x100 as f64) * 1e-2).clamp(0.0, 1.0);
    let s_maker = crate::wave_elliott::hermite_smoothstep_c1(1.0 - p_t, 0.84, 0.95);
    let relief = INV_PHI_F64 * psi_c * s_maker * (INV_PHI_F64 + INV_PHI2_F64 * iota);
    (PHI_F64 + (3.0 - PHI_F64) * (1.0 - relief)).clamp(PHI_F64, 3.0)
}

/// Live exchange fee schedule in ScaledBps (1 bp = 100), updated via /v5/account/fee-rate.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FeeSchedule {
    pub maker_fee_bps: ScaledBps,
    pub taker_fee_bps: ScaledBps,
}

impl Default for FeeSchedule {
    #[inline]
    fn default() -> Self {
        Self::VIP0_DEFAULT
    }
}

impl FeeSchedule {
    pub const VIP0_DEFAULT: Self = Self {
        maker_fee_bps: ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS,
        taker_fee_bps: ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS,
    };

    #[inline]
    #[must_use]
    pub const fn from_live_rates_clamped(maker: ScaledBps, taker: ScaledBps) -> Self {
        let m = if maker.0 < -50 {
            -50
        } else if maker.0 > 250 {
            250
        } else {
            maker.0
        };
        let t = if taker.0 < 100 {
            100
        } else if taker.0 > 1_000 {
            1_000
        } else {
            taker.0
        };
        Self {
            maker_fee_bps: ScaledBps(m),
            taker_fee_bps: ScaledBps(t),
        }
    }

    #[inline]
    #[must_use]
    pub const fn round_trip_maker_exit_bps(self) -> ScaledBps {
        ScaledBps(self.maker_fee_bps.0.saturating_mul(2))
    }

    #[inline]
    #[must_use]
    pub const fn round_trip_taker_exit_bps(self) -> ScaledBps {
        ScaledBps(self.maker_fee_bps.0.saturating_add(self.taker_fee_bps.0))
    }
}

/// Online conjugate Beta-Binomial estimator with exponential forgetting (gamma = 0.960, N_eff = 25).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BetaBinomialTakerPrior {
    pub prior_alpha_x1000: i64,
    pub prior_beta_x1000: i64,
    pub obs_alpha_x1000: i64,
    pub obs_beta_x1000: i64,
}

impl Default for BetaBinomialTakerPrior {
    #[inline]
    fn default() -> Self {
        Self::new_seeded(5)
    }
}

impl BetaBinomialTakerPrior {
    #[inline]
    #[must_use]
    pub const fn new_seeded(seed_pct: u8) -> Self {
        let raw_pm = (seed_pct as i64) * 10;
        let seed_pm = if raw_pm < 20 {
            20
        } else if raw_pm > 450 {
            450
        } else {
            raw_pm
        };
        Self {
            prior_alpha_x1000: 20 * seed_pm,
            prior_beta_x1000: 20 * (1_000 - seed_pm),
            obs_alpha_x1000: 0,
            obs_beta_x1000: 0,
        }
    }

    #[inline]
    pub fn observe_exit(&mut self, is_adverse_taker_sl: bool) {
        let decayed_a = (self.obs_alpha_x1000.saturating_mul(960)) / 1_000;
        let decayed_b = (self.obs_beta_x1000.saturating_mul(960)) / 1_000;
        if is_adverse_taker_sl {
            self.obs_alpha_x1000 = decayed_a.saturating_add(1_000);
            self.obs_beta_x1000 = decayed_b;
        } else {
            self.obs_alpha_x1000 = decayed_a;
            self.obs_beta_x1000 = decayed_b.saturating_add(1_000);
        }
    }

    #[inline]
    #[must_use]
    pub const fn posterior_permille(self) -> i64 {
        let a_tot = self.prior_alpha_x1000.saturating_add(self.obs_alpha_x1000);
        let b_tot = self.prior_beta_x1000.saturating_add(self.obs_beta_x1000);
        let denom = a_tot.saturating_add(b_tot);
        if denom <= 0 {
            return 50;
        }
        let pm = (a_tot.saturating_mul(1_000).saturating_add(denom / 2)) / denom;
        if pm < 20 {
            20
        } else if pm > 450 {
            450
        } else {
            pm
        }
    }
}

/// Monotonic Wald SPRT v2.0 boundaries (A_hold > 0, B_unwind < 0) coupled to learned p_Taker permille.
#[inline]
#[must_use]
pub fn wald_sprt_boundaries_from_permille(taker_prob_pm: i64) -> (f64, f64) {
    let p = (taker_prob_pm.clamp(20, 450) as f64) / 1_000.0;
    let alpha = (INV_PHI_F64 * (0.140 - 0.220 * p)).clamp(0.018, 0.100);
    let beta = (INV_PHI_F64 * (0.240 + 0.850 * p)).clamp(0.120, 0.420);
    let a_hold = ((1.0 - beta) / alpha).ln();
    let b_unwind = (beta / (1.0 - alpha)).ln();
    (a_hold, b_unwind)
}

#[inline]
const fn civil_ymd_dow_hour_from_epoch(epoch_secs: i64) -> (i64, i64, i64, i64, i64) {
    let days = epoch_secs.div_euclid(86_400);
    let sec_of_day = epoch_secs.rem_euclid(86_400);
    let hour_utc = sec_of_day / 3_600;
    let dow = (days + 4).rem_euclid(7); // 0 = Sunday .. 6 = Saturday

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as i64;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as i64;
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day, dow, hour_utc)
}

/// O(1) UK/London British Summer Time (BST: last Sunday of March 01:00 UTC -> last Sunday of Oct 01:00 UTC).
#[inline]
#[must_use]
pub const fn is_eu_london_bst(epoch_secs: i64) -> bool {
    if epoch_secs <= 0 {
        return false;
    }
    let (_, month, day, dow, hour_utc) = civil_ymd_dow_hour_from_epoch(epoch_secs);
    if month < 3 || month > 10 {
        return false;
    }
    if month > 3 && month < 10 {
        return true;
    }
    let dow_1st = (dow - (day - 1)).rem_euclid(7);
    let first_sun = 1 + (7 - dow_1st) % 7;
    let last_sun_31 = if first_sun + 28 <= 31 {
        first_sun + 28
    } else {
        first_sun + 21
    };
    if month == 3 {
        day > last_sun_31 || (day == last_sun_31 && hour_utc >= 1)
    } else {
        day < last_sun_31 || (day == last_sun_31 && hour_utc < 1)
    }
}

/// O(1) US/NYSE Eastern Daylight Time (EDT: 2nd Sunday of March 07:00 UTC -> 1st Sunday of Nov 06:00 UTC).
#[inline]
#[must_use]
pub const fn is_us_nyse_edt(epoch_secs: i64) -> bool {
    if epoch_secs <= 0 {
        return false;
    }
    let (_, month, day, dow, hour_utc) = civil_ymd_dow_hour_from_epoch(epoch_secs);
    if month < 3 || month > 11 {
        return false;
    }
    if month > 3 && month < 11 {
        return true;
    }
    let dow_1st = (dow - (day - 1)).rem_euclid(7);
    let first_sun = 1 + (7 - dow_1st) % 7;
    if month == 3 {
        let second_sun = first_sun + 7;
        day > second_sun || (day == second_sun && hour_utc >= 7)
    } else {
        day < first_sun || (day == first_sun && hour_utc < 6)
    }
}

/// Continuous C^inf UTC + DST harmonic liquidity kernel coupled with directional live Bybit funding carry.
#[must_use]
pub fn evaluate_utc_dst_and_live_funding_cadence(
    ts_ns: TimestampNs,
    realized_vol_5m: ScaledBps,
    eq_vol_5m: ScaledBps,
    candidate_side: Option<Side>,
    funding_rate_scaled_bps: ScaledBps,
    next_funding_ts_ns: TimestampNs,
) -> TemporalCadenceModulation {
    let epoch_secs = ts_ns.0.div_euclid(1_000_000_000);
    if epoch_secs < REAL_UTC_MIN_EPOCH_SECS {
        return TemporalCadenceModulation {
            intraday_percentile: 0.50,
            snr_threshold_delta: 0.0,
            kin_energy_min_delta_bps: 0,
            kelly_lot_mult_permille: 1_000,
        };
    }

    let sec_of_day = (epoch_secs.rem_euclid(86_400)) as f64;
    let theta = TAU * (sec_of_day / 86_400.0);
    let ldn_pole_h = if is_eu_london_bst(epoch_secs) {
        7.0
    } else {
        8.0
    };
    let ny_pole_h = if is_us_nyse_edt(epoch_secs) {
        14.25
    } else {
        15.25
    };

    let theta_asia = TAU * (1.5 / 24.0);
    let theta_ldn = TAU * (ldn_pole_h / 24.0);
    let theta_ny = TAU * (ny_pole_h / 24.0);

    let k_asia = 0.24 * (3.2 * ((theta - theta_asia).cos() - 1.0)).exp();
    let k_ldn = 0.30 * (3.8 * ((theta - theta_ldn).cos() - 1.0)).exp();
    let k_ny = 0.42 * (3.5 * ((theta - theta_ny).cos() - 1.0)).exp();
    let h_utc = (0.16 + k_asia + k_ldn + k_ny).clamp(0.05, 0.99);

    let vol_cur = realized_vol_5m.0.max(100) as f64;
    let vol_eq = eq_vol_5m.0.max(100) as f64;
    let vol_ratio = vol_cur / vol_eq;
    let phi_vol = (0.50 * vol_ratio).clamp(0.05, 0.99);
    let p_day = (0.62 * h_utc + 0.38 * phi_vol).clamp(0.05, 0.99);

    let breakout_release = ((vol_ratio - 1.75) / 0.65).clamp(0.0, 1.0);
    let trough_depth = ((0.45 - p_day) / 0.40).clamp(0.0, 1.0) * (1.0 - breakout_release);
    let peak_expansion = ((p_day - 0.60) / 0.39).clamp(0.0, 1.0);

    let mut snr_delta = 0.08 * trough_depth - 0.03 * peak_expansion;
    let mut e_min_delta = (52.0 * trough_depth - 14.0 * peak_expansion).round() as i64;
    let mut kelly_pm =
        (1_000.0 - 180.0 * trough_depth + 90.0 * peak_expansion).round() as i64;

    let rem_8h = epoch_secs.rem_euclid(28_800);
    let dt_since_cut_s = rem_8h as f64;
    let dt_to_cut_s = if next_funding_ts_ns.0 > ts_ns.0
        && (next_funding_ts_ns.0 - ts_ns.0) <= 28_800_000_000_000
    {
        ((next_funding_ts_ns.0 - ts_ns.0) as f64) / 1_000_000_000.0
    } else {
        (28_800 - rem_8h) as f64
    };

    let k_post = (-0.5 * (dt_since_cut_s / 30.0).powi(2)).exp();
    e_min_delta += (28.0 * k_post).round() as i64;
    snr_delta += 0.04 * k_post;

    match candidate_side {
        Some(side) if funding_rate_scaled_bps.0 != 0 => {
            let s_dir = if side == Side::Bid { 1_i64 } else { -1_i64 };
            let carry_bps = -s_dir.saturating_mul(funding_rate_scaled_bps.0);
            if carry_bps < 0 {
                let abs_c = carry_bps.unsigned_abs() as f64;
                let k_cut = (-0.5 * (dt_to_cut_s / 120.0).powi(2)).exp();
                let pen_e = (k_cut * (45.0 + 60.0 * (abs_c / 200.0)))
                    .round()
                    .clamp(0.0, 180.0) as i64;
                let pen_snr = (0.12 * k_cut * (abs_c / 100.0).min(2.0)).clamp(0.0, 0.24);
                let pen_k = (k_cut * (abs_c / 2.0)).round().clamp(0.0, 250.0) as i64;
                e_min_delta += pen_e;
                snr_delta += pen_snr;
                kelly_pm -= pen_k;
            } else if dt_to_cut_s < 20.0 {
                e_min_delta += 25;
                snr_delta += 0.04;
            } else if dt_to_cut_s <= 900.0 {
                let pos_c = carry_bps as f64;
                let k_carry = (-0.5 * ((dt_to_cut_s - 180.0) / 240.0).powi(2)).exp();
                let cred_e = (INV_PHI2_F64 * (pos_c / 100.0) * k_carry * 20.0)
                    .round()
                    .clamp(0.0, 30.0) as i64;
                let cred_snr = (0.04 * k_carry * (pos_c / 100.0).min(2.0)).clamp(0.0, 0.08);
                let cred_k = ((pos_c / 5.0) * k_carry).round().clamp(0.0, 120.0) as i64;
                e_min_delta -= cred_e;
                snr_delta -= cred_snr;
                kelly_pm += cred_k;
            }
        }
        _ => {
            let k_cut = (-0.5 * (dt_to_cut_s / 90.0).powi(2)).exp();
            e_min_delta += (60.0 * k_cut).round() as i64;
            snr_delta += 0.08 * k_cut;
            kelly_pm -= (140.0 * k_cut).round() as i64;
        }
    }

    TemporalCadenceModulation {
        intraday_percentile: p_day,
        snr_threshold_delta: snr_delta.clamp(-0.10, 0.32),
        kin_energy_min_delta_bps: e_min_delta.clamp(-35, 220),
        kelly_lot_mult_permille: kelly_pm.clamp(550, 1_200),
    }
}

impl PairSpec {
    #[inline]
    #[must_use]
    pub const fn quantum_tick_scaled_bps(self, mid: PriceTicks) -> ScaledBps {
        let _ = self.symbol_id;
        if mid.0 <= 0 {
            ScaledBps(10_000)
        } else {
            ScaledBps((1_000_000 + mid.0 - 1) / mid.0)
        }
    }

    #[inline]
    #[must_use]
    pub const fn expected_round_trip_cost_bps_dynamic(
        self,
        mid: PriceTicks,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> ScaledBps {
        let p_pm = if taker_prob_pm < 20 {
            20
        } else if taker_prob_pm > 450 {
            450
        } else {
            taker_prob_pm
        };
        let tau_tick = self.quantum_tick_scaled_bps(mid).0;
        let f_m = fees.maker_fee_bps.0;
        let f_t = fees.taker_fee_bps.0;
        let num = (1_000 - p_pm)
            .saturating_mul(f_m)
            .saturating_add(p_pm.saturating_mul(f_t.saturating_add(tau_tick)));
        let exit_leg = (num + 999) / 1_000;
        let rt = f_m.saturating_add(exit_leg);
        ScaledBps(if rt < 50 { 50 } else { rt })
    }

    #[inline]
    #[must_use]
    pub const fn three_x_friction_barrier_bps_dynamic(
        self,
        mid: PriceTicks,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> ScaledBps {
        ScaledBps(
            self.expected_round_trip_cost_bps_dynamic(mid, taker_prob_pm, fees)
                .0
                .saturating_mul(3),
        )
    }

    /// Self-learned `C1` friction barrier in `ScaledBps` (`1 bp = 100`, `V5.5.4`),
    /// continuously modulating `m_fric` in `[PHI, 3.0]` via `taker_prob_pm`, `learned_ir_x100`, and `psi_health`.
    #[inline]
    #[must_use]
    pub fn learned_friction_barrier_bps_c1(
        self,
        mid: PriceTicks,
        taker_prob_pm: i64,
        learned_ir_x100: i64,
        psi_health: f64,
        fees: FeeSchedule,
    ) -> ScaledBps {
        let rt = self
            .expected_round_trip_cost_bps_dynamic(mid, taker_prob_pm, fees)
            .0 as f64;
        let m_fric =
            compute_learned_friction_multiplier_c1(taker_prob_pm, learned_ir_x100, psi_health);
        ScaledBps((rt * m_fric).round().max(50.0) as i64)
    }

    /// Intrinsic diffusive reachability & `C1` friction eligibility (`V5.5.4`).
    #[inline]
    #[must_use]
    pub fn validate_pair_eligibility_c1(
        self,
        realized_vol_5m: ScaledBps,
        tau_mkt_s: f64,
        fees: FeeSchedule,
    ) -> bool {
        let sigma_5m = realized_vol_5m.0.max(0) as f64;
        let tau_s = if tau_mkt_s.is_finite() && tau_mkt_s > 0.0 {
            tau_mkt_s
        } else {
            300.0
        };
        let tau_scale = (tau_s / 300.0).clamp(INV_PHI2_F64, PHI_F64).sqrt();
        let c_rt_mkr = (fees.round_trip_maker_exit_bps().0.max(50)) as f64;
        sigma_5m * tau_scale >= PHI_F64 * c_rt_mkr
    }

    #[inline]
    #[must_use]
    pub const fn validate_pair_eligibility_dynamic(
        self,
        mid: PriceTicks,
        realized_vol_5m: ScaledBps,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> bool {
        let tau_tick = self.quantum_tick_scaled_bps(mid).0;
        if tau_tick > 150 {
            return false;
        }
        let rt = self
            .expected_round_trip_cost_bps_dynamic(mid, taker_prob_pm, fees)
            .0;
        let b3x = rt.saturating_mul(3);
        let phi_rt = (rt.saturating_mul(1_000 + INV_PHI_PM) + 999) / 1_000;
        let req_vol = if b3x > phi_rt { b3x } else { phi_rt };
        realized_vol_5m.0 >= req_vol
    }

    #[inline]
    #[must_use]
    pub const fn max_preflight_drift_ticks(
        self,
        mid: PriceTicks,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> i64 {
        let b3x = self
            .three_x_friction_barrier_bps_dynamic(mid, taker_prob_pm, fees)
            .0;
        let raw_tau = self.quantum_tick_scaled_bps(mid).0;
        let tau_tick = if raw_tau > 1 { raw_tau } else { 1 };
        let raw = (INV_PHI3_PM.saturating_mul(b3x)) / (1_000_i64.saturating_mul(tau_tick));
        if raw < 1 {
            1
        } else if raw > 24 {
            24
        } else {
            raw
        }
    }

    #[inline]
    #[must_use]
    pub const fn max_repeg_chase_ticks(
        self,
        mid: PriceTicks,
        taker_prob_pm: i64,
        fees: FeeSchedule,
    ) -> i64 {
        let b3x = self
            .three_x_friction_barrier_bps_dynamic(mid, taker_prob_pm, fees)
            .0;
        let raw_tau = self.quantum_tick_scaled_bps(mid).0;
        let tau_tick = if raw_tau > 1 { raw_tau } else { 1 };
        let raw = (INV_PHI2_PM.saturating_mul(b3x)) / (1_000_i64.saturating_mul(tau_tick));
        if raw < 2 {
            2
        } else if raw > 48 {
            48
        } else {
            raw
        }
    }

    #[inline]
    #[must_use]
    pub const fn min_exit_amend_step_ticks(self, mid: PriceTicks, fees: FeeSchedule) -> i64 {
        let raw_tau = self.quantum_tick_scaled_bps(mid).0;
        let tau_tick = if raw_tau > 1 { raw_tau } else { 1 };
        let half_maker = if fees.maker_fee_bps.0 / 2 > 75 {
            fees.maker_fee_bps.0 / 2
        } else {
            75
        };
        let raw = half_maker / tau_tick;
        if raw < 1 {
            1
        } else if raw > 16 {
            16
        } else {
            raw
        }
    }

    #[inline]
    #[must_use]
    pub const fn compute_position_sl_risk_ppm(
        self,
        wallet_usdt_1e8: i64,
        notional_1e8: i64,
        entry_price: PriceTicks,
        realized_vol_5m: ScaledBps,
    ) -> i64 {
        if notional_1e8 <= 0 {
            return 1;
        }
        let raw_sl = self.all_in_sl_loss_ppm(entry_price, realized_vol_5m);
        let sl_ppm = (if raw_sl > 1 { raw_sl } else { 1 }) as i128;
        let eq = (if wallet_usdt_1e8 > 1 { wallet_usdt_1e8 } else { 1 }) as i128;
        let num = (notional_1e8 as i128).saturating_mul(sl_ppm);
        let ceil_ppm = ((num + eq - 1) / eq) as i64;
        if ceil_ppm < 1 {
            1
        } else if ceil_ppm > MAX_PORTFOLIO_RISK_PPM {
            MAX_PORTFOLIO_RISK_PPM
        } else {
            ceil_ppm
        }
    }
}



/// Law VII Dollar-Weighted Portfolio Risk & Directional Beta Guard (<= 14,400 ppm total, <= 9,000 ppm beta).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MonetaryPortfolioBetaGuard {
    active: [bool; 12],
    free_ride: [bool; 12],
    side: [Side; 12],
    risk_ppm: [i64; 12],
    beta_x1000: [i64; 12],
}

impl Default for MonetaryPortfolioBetaGuard {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl MonetaryPortfolioBetaGuard {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active: [false; 12],
            free_ride: [false; 12],
            side: [Side::Bid; 12],
            risk_ppm: [0; 12],
            beta_x1000: [1_000; 12],
        }
    }

    #[inline]
    #[must_use]
    pub const fn is_slot_free_ride(&self, slot_idx: usize) -> bool {
        slot_idx < 12 && self.active[slot_idx] && self.free_ride[slot_idx]
    }

    #[inline]
    #[must_use]
    pub fn open_counts(&self) -> (usize, usize, usize) {
        let mut bids = 0_usize;
        let mut asks = 0_usize;
        for i in 0..12 {
            if self.active[i] {
                match self.side[i] {
                    Side::Bid => bids += 1,
                    Side::Ask => asks += 1,
                }
            }
        }
        (bids, asks, bids + asks)
    }

    #[inline]
    #[must_use]
    fn risk_bearing_side_counts(&self) -> (usize, usize) {
        let mut bids = 0_usize;
        let mut asks = 0_usize;
        for i in 0..12 {
            if self.active[i] && !self.free_ride[i] {
                match self.side[i] {
                    Side::Bid => bids += 1,
                    Side::Ask => asks += 1,
                }
            }
        }
        (bids, asks)
    }

    #[inline]
    #[must_use]
    pub fn total_open_risk_ppm(&self) -> i64 {
        let mut sum = 0_i64;
        for i in 0..12 {
            if self.active[i] {
                sum = sum.saturating_add(self.risk_ppm[i]);
            }
        }
        sum
    }

    #[inline]
    #[must_use]
    pub fn net_directional_beta_risk_ppm(&self) -> i64 {
        let mut net = 0_i64;
        for i in 0..12 {
            if self.active[i] {
                let s = if self.side[i] == Side::Bid {
                    1_i64
                } else {
                    -1_i64
                };
                let term = (s
                    .saturating_mul(self.beta_x1000[i])
                    .saturating_mul(self.risk_ppm[i]))
                    / 1_000;
                net = net.saturating_add(term);
            }
        }
        net
    }

    #[inline]
    #[must_use]
    pub fn can_open_with_risk_ppm(
        &self,
        slot_idx: usize,
        side: Side,
        risk_ppm: i64,
        beta_x1000: i64,
    ) -> bool {
        if slot_idx >= 12 || self.active[slot_idx] {
            return false;
        }
        let (_, _, tot) = self.open_counts();
        if tot >= MAX_TOTAL_OPEN_SLOTS {
            return false;
        }
        let (risk_bids, risk_asks) = self.risk_bearing_side_counts();
        match side {
            Side::Bid if risk_bids >= MAX_SIDE_OPEN_SLOTS => return false,
            Side::Ask if risk_asks >= MAX_SIDE_OPEN_SLOTS => return false,
            _ => {}
        }
        let eff_risk = risk_ppm.clamp(1, MAX_PORTFOLIO_RISK_PPM);
        if self.total_open_risk_ppm().saturating_add(eff_risk) > MAX_PORTFOLIO_RISK_PPM {
            return false;
        }
        if tot == 0 || (risk_bids + risk_asks) == 0 {
            return true;
        }
        let eff_beta = beta_x1000.clamp(-1_618, 1_618);
        let s = if side == Side::Bid { 1_i64 } else { -1_i64 };
        let cand_beta_risk = (s.saturating_mul(eff_beta).saturating_mul(eff_risk)) / 1_000;
        let cur_net = self.net_directional_beta_risk_ppm();
        let next_net = cur_net.saturating_add(cand_beta_risk);
        next_net.abs() <= MAX_DIRECTIONAL_BETA_RISK_PPM || next_net.abs() < cur_net.abs()
    }

    #[inline]
    pub fn register_slot_open(
        &mut self,
        slot_idx: usize,
        side: Side,
        risk_ppm: i64,
        beta_x1000: i64,
    ) {
        if slot_idx < 12 {
            self.active[slot_idx] = true;
            self.free_ride[slot_idx] = false;
            self.side[slot_idx] = side;
            self.risk_ppm[slot_idx] = risk_ppm.clamp(1, MAX_PORTFOLIO_RISK_PPM);
            self.beta_x1000[slot_idx] = beta_x1000.clamp(-1_618, 1_618);
        }
    }

    /// Registers an open position scaling `eff_risk_ppm` and directional beta load by
    /// `S_3(residual_risk_ratio; 0.0, INV_PHI_SQ)` (V5.5 O4 House-Money Free-Ride).
    /// When `residual_s3_scale <= 0.0` (`residual_risk_1e8 == 0`), marks the slot `free_ride`
    /// (`eff_risk_ppm = 0`, freeing risk budget and same-side slot cap while enforcing `<= 6` total slots).
    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn register_slot_open_house_money(
        &mut self,
        slot_idx: usize,
        side: Side,
        base_risk_ppm: i64,
        beta_x1000: i64,
        residual_s3_scale: f64,
    ) {
        if slot_idx >= 12 {
            return;
        }
        let scale = if residual_s3_scale.is_finite() {
            residual_s3_scale.clamp(0.0, 1.0)
        } else {
            1.0
        };
        self.active[slot_idx] = true;
        self.side[slot_idx] = side;
        if scale <= 1e-9 {
            self.free_ride[slot_idx] = true;
            self.risk_ppm[slot_idx] = 0;
            self.beta_x1000[slot_idx] = 0;
        } else {
            let base_c = base_risk_ppm.clamp(1, MAX_PORTFOLIO_RISK_PPM);
            let scaled = ((base_c as f64) * scale).round() as i64;
            self.free_ride[slot_idx] = false;
            self.risk_ppm[slot_idx] = scaled.clamp(1, MAX_PORTFOLIO_RISK_PPM);
            self.beta_x1000[slot_idx] = beta_x1000.clamp(-1_618, 1_618);
        }
    }

    #[inline]
    pub fn release_slot(&mut self, slot_idx: usize) {
        if slot_idx < 12 {
            self.active[slot_idx] = false;
            self.free_ride[slot_idx] = false;
            self.risk_ppm[slot_idx] = 0;
            self.beta_x1000[slot_idx] = 1_000;
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_beta_binomial_taker_prior_convergence_and_wald_monotonicity() {
        let mut prior = BetaBinomialTakerPrior::new_seeded(5);
        assert_eq!(prior.posterior_permille(), 50);

        let (a_base, b_base) = wald_sprt_boundaries_from_permille(prior.posterior_permille());
        assert!(a_base > 0.0 && b_base < 0.0);

        for _ in 0..30 {
            prior.observe_exit(false);
        }
        let pm_clean = prior.posterior_permille();
        assert!((20..50).contains(&pm_clean));
        let (a_clean, b_clean) = wald_sprt_boundaries_from_permille(pm_clean);

        let mut toxic = BetaBinomialTakerPrior::new_seeded(5);
        for _ in 0..25 {
            toxic.observe_exit(true);
        }
        let pm_toxic = toxic.posterior_permille();
        assert!(pm_toxic > 250);
        let (a_toxic, b_toxic) = wald_sprt_boundaries_from_permille(pm_toxic);

        // Higher adverse Taker SL frequency raises A_hold and contracts |B_unwind| toward 0
        assert!(a_toxic > a_base && a_base > a_clean);
        assert!(b_toxic.abs() < b_base.abs() && b_base.abs() < b_clean.abs());

        let sol = APPROVED_SPECS[2];
        let mid = PriceTicks(18_000);
        let vip0 = FeeSchedule::VIP0_DEFAULT;
        let vip3 = FeeSchedule::from_live_rates_clamped(ScaledBps(120), ScaledBps(380));
        let b3x_clean = sol.three_x_friction_barrier_bps_dynamic(mid, pm_clean, vip0);
        let b3x_toxic = sol.three_x_friction_barrier_bps_dynamic(mid, pm_toxic, vip0);
        let b3x_vip3 = sol.three_x_friction_barrier_bps_dynamic(mid, pm_clean, vip3);
        assert!(b3x_toxic.0 > b3x_clean.0);
        assert!(b3x_vip3.0 < b3x_clean.0);

        let m_clean = compute_learned_friction_multiplier_c1(pm_clean, 80, 1.0);
        let m_mid = compute_learned_friction_multiplier_c1(90, 40, 0.85);
        let m_toxic = compute_learned_friction_multiplier_c1(pm_toxic, 0, 1.0);
        let m_sick = compute_learned_friction_multiplier_c1(pm_clean, 100, 0.0);
        assert!((PHI_F64..=3.0).contains(&m_clean));
        assert!(m_clean < m_mid && m_mid < m_toxic);
        assert!((m_toxic - 3.0).abs() < 1e-12 && (m_sick - 3.0).abs() < 1e-12);
        let b_c1_clean = sol.learned_friction_barrier_bps_c1(mid, pm_clean, 80, 1.0, vip0);
        assert!(b_c1_clean.0 < b3x_clean.0 && b_c1_clean.0 > vip0.round_trip_maker_exit_bps().0);
    }

    #[test]
    fn test_civil_dst_2026_transitions_and_directional_funding_carry() {
        // US NYSE EDT 2026: starts 2026-03-08 07:00:00 UTC (1_772_953_200), ends 2026-11-01 06:00:00 UTC (1_793_512_800)
        assert!(!is_us_nyse_edt(1_772_953_140));
        assert!(is_us_nyse_edt(1_772_953_200));
        assert!(is_us_nyse_edt(1_793_512_740));
        assert!(!is_us_nyse_edt(1_793_512_800));

        // UK London BST 2026: starts 2026-03-29 01:00:00 UTC (1_774_746_000), ends 2026-10-25 01:00:00 UTC (1_792_890_000)
        assert!(!is_eu_london_bst(1_774_745_940));
        assert!(is_eu_london_bst(1_774_746_000));
        assert!(is_eu_london_bst(1_792_889_940));
        assert!(!is_eu_london_bst(1_792_890_000));

        // 150s before 16:00 UTC settlement on 2026-11-14 with +2.00 bps funding (Long pays Short)
        let cut_ns = 1_794_672_000_000_000_000_i64;
        let pre_cut_ts = TimestampNs(cut_ns - 150_000_000_000);
        let mod_long_payer = evaluate_utc_dst_and_live_funding_cadence(
            pre_cut_ts,
            ScaledBps(6_500),
            ScaledBps(6_500),
            Some(Side::Bid),
            ScaledBps(200),
            TimestampNs(cut_ns),
        );
        let mod_short_receiver = evaluate_utc_dst_and_live_funding_cadence(
            pre_cut_ts,
            ScaledBps(6_500),
            ScaledBps(6_500),
            Some(Side::Ask),
            ScaledBps(200),
            TimestampNs(cut_ns),
        );
        assert!(mod_long_payer.kin_energy_min_delta_bps > mod_short_receiver.kin_energy_min_delta_bps + 25);
        assert!(mod_long_payer.snr_threshold_delta > mod_short_receiver.snr_threshold_delta + 0.05);
        assert!(mod_short_receiver.kelly_lot_mult_permille > mod_long_payer.kelly_lot_mult_permille);
    }

    #[test]
    fn test_scale_invariant_tick_tolerances_and_monetary_beta_guard() {
        let fees = FeeSchedule::VIP0_DEFAULT;
        let btc = APPROVED_SPECS[0];
        let wif = APPROVED_SPECS[8];
        let btc_repeg = btc.max_repeg_chase_ticks(PriceTicks(680_000), 50, fees);
        let wif_repeg = wif.max_repeg_chase_ticks(PriceTicks(1_600), 50, fees);
        assert_eq!(btc_repeg, 48);
        assert_eq!(wif_repeg, 2);

        let mut guard = MonetaryPortfolioBetaGuard::new();
        // Oversized BTC lot consuming 12,800 ppm leaves < 2,400 ppm of the 14,400 ppm total budget
        assert!(guard.can_open_with_risk_ppm(0, Side::Bid, 12_800, 1_000));
        guard.register_slot_open(0, Side::Bid, 12_800, 1_000);
        assert!(!guard.can_open_with_risk_ppm(2, Side::Bid, 2_400, 1_150));
        assert!(!guard.can_open_with_risk_ppm(2, Side::Ask, 2_400, 1_150));
        // Smaller 1,500 ppm opposite-side Ask fits <= 14,400 ppm total AND reduces net directional beta
        assert!(guard.can_open_with_risk_ppm(2, Side::Ask, 1_500, 1_150));
        guard.release_slot(0);
        assert_eq!(guard.total_open_risk_ppm(), 0);
    }
}
