//! Zero-Alloc 12x2 Beta-Binomial Taker Priors, Live FeeSchedule & Regime-Shift Shadow-Decay State.
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use crate::alpha_3factor::OrthogonalFactorState;
use crate::fee_cadence_beta::{BetaBinomialTakerPrior, FeeSchedule};
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::Side;

const INV_PHI_PM: i64 = 618;
const INV_PHI2_PM: i64 = 382;
const MIN_REHAB_SNR: f64 = 0.72;
const SHADOW_CREDITS_REQUIRED: u8 = 3;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct GovernorPlasticityState {
    active_fees: FeeSchedule,
    taker_priors: [[BetaBinomialTakerPrior; 2]; MAX_ACTIVE_PAIRS],
    scar_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    scar_eq_vol_bps: [[i64; 2]; MAX_ACTIVE_PAIRS],
    shadow_credits: [[u8; 2]; MAX_ACTIVE_PAIRS],
    last_credit_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
}

impl Default for GovernorPlasticityState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl GovernorPlasticityState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let seed_prior = BetaBinomialTakerPrior::new_seeded(5);
        Self {
            active_fees: FeeSchedule::VIP0_DEFAULT,
            taker_priors: [[seed_prior; 2]; MAX_ACTIVE_PAIRS],
            scar_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            scar_eq_vol_bps: [[4_800; 2]; MAX_ACTIVE_PAIRS],
            shadow_credits: [[0; 2]; MAX_ACTIVE_PAIRS],
            last_credit_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    const fn dir_idx(side: Side) -> usize {
        match side {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }

    #[inline]
    #[must_use]
    pub const fn active_fee_schedule(&self) -> FeeSchedule {
        self.active_fees
    }

    #[inline]
    pub fn set_active_fee_schedule(&mut self, fees: FeeSchedule) {
        self.active_fees = fees;
    }

    #[inline]
    #[must_use]
    pub const fn maker_round_trip_fee_bps(&self) -> i64 {
        let rt = self.active_fees.round_trip_maker_exit_bps().raw();
        if rt > 100 {
            rt
        } else {
            100
        }
    }

    #[inline]
    #[must_use]
    pub const fn slot_taker_prob_permille(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 50;
        }
        let d = Self::dir_idx(side);
        self.taker_priors[slot_idx][d].posterior_permille()
    }

    #[inline]
    #[must_use]
    pub const fn shadow_credits_for(&self, slot_idx: usize, side: Side) -> u8 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 0;
        }
        self.shadow_credits[slot_idx][Self::dir_idx(side)]
    }

    /// Updates conjugate Beta-Binomial Taker prior; non-punitive wins (Chandelier) pass `false`.
    #[inline]
    pub fn observe_exit_outcome(
        &mut self,
        slot_idx: usize,
        side: Side,
        is_adverse_taker_sl: bool,
        ts_ns: i64,
        eq_vol_bps: i64,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        self.taker_priors[slot_idx][d].observe_exit(is_adverse_taker_sl);
        if is_adverse_taker_sl {
            self.scar_ts_ns[slot_idx][d] = ts_ns;
            self.scar_eq_vol_bps[slot_idx][d] = eq_vol_bps.max(1_200);
            self.shadow_credits[slot_idx][d] = 0;
            self.last_credit_ts_ns[slot_idx][d] = ts_ns;
        } else {
            self.shadow_credits[slot_idx][d] = 0;
        }
    }

    /// Contracts negative local scar debt by `phi^-2` (`38.2%`) upon 3/3 regime-shift shadow credits.
    #[inline]
    #[must_use]
    pub const fn contract_local_debt_golden(local_cum_bps_x100: i64) -> i64 {
        if local_cum_bps_x100 < 0 {
            (local_cum_bps_x100.saturating_mul(INV_PHI_PM)) / 1_000
        } else {
            local_cum_bps_x100
        }
    }

    /// Law III Anti-Smoothing-by-Silence: awards shadow credit ONLY after verified regime mutation.
    pub fn evaluate_shadow_rehab_step(
        &mut self,
        slot_idx: usize,
        side: Side,
        sl_streak: u32,
        tau_mkt_ns: i64,
        cur_eq_vol_bps: i64,
        b3x_bps: i64,
        drift_bps: i64,
        snap: &SymbolLobSnapshot,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS || sl_streak == 0 || snap.ts_local_ns.0 <= 0 {
            return false;
        }
        let d = Self::dir_idx(side);
        let now_ns = snap.ts_local_ns.0;
        let t_scar = self.scar_ts_ns[slot_idx][d];
        if t_scar <= 0 {
            self.scar_ts_ns[slot_idx][d] = now_ns;
            self.scar_eq_vol_bps[slot_idx][d] = cur_eq_vol_bps.max(1_200);
            return false;
        }
        let tau_safe = tau_mkt_ns.max(90_000_000_000);
        let req_elapsed = tau_safe
            .saturating_mul(6)
            .saturating_mul(i64::from(sl_streak.min(4)));
        if now_ns.saturating_sub(t_scar) < req_elapsed {
            return false;
        }

        let scar_v = self.scar_eq_vol_bps[slot_idx][d];
        let denom_v = scar_v.max(b3x_bps).max(1_200);
        let vol_shift_pm = ((cur_eq_vol_bps - scar_v).abs().saturating_mul(1_000)) / denom_v;
        let w = snap.wave_observables();
        let macro_aligned = w.macro_phase.get() * side.sign_f64();
        let regime_shifted =
            vol_shift_pm >= INV_PHI2_PM || (macro_aligned >= 0.35 && w.hurst_exponent() >= 0.54);
        if !regime_shifted {
            return false;
        }

        let prev_c_ts = self.last_credit_ts_ns[slot_idx][d];
        if prev_c_ts > 0 && now_ns.saturating_sub(prev_c_ts) < tau_safe {
            return false;
        }
        let signed_snr = OrthogonalFactorState::snr_composite_score(snap) * side.sign_f64();
        let signed_drift = drift_bps.saturating_mul(side.sign_i64());
        if signed_snr < MIN_REHAB_SNR || signed_drift <= 0 {
            return false;
        }

        self.last_credit_ts_ns[slot_idx][d] = now_ns;
        let next_c = self.shadow_credits[slot_idx][d].saturating_add(1);
        if next_c >= SHADOW_CREDITS_REQUIRED {
            self.shadow_credits[slot_idx][d] = 0;
            self.scar_ts_ns[slot_idx][d] = now_ns;
            self.scar_eq_vol_bps[slot_idx][d] = cur_eq_vol_bps.max(1_200);
            true
        } else {
            self.shadow_credits[slot_idx][d] = next_c;
            false
        }
    }

    pub fn write_to_scar_json_map(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        let mut obs_a = [[0_i64; 2]; MAX_ACTIVE_PAIRS];
        let mut obs_b = [[0_i64; 2]; MAX_ACTIVE_PAIRS];
        for s in 0..MAX_ACTIVE_PAIRS {
            for d in 0..2 {
                obs_a[s][d] = self.taker_priors[s][d].obs_alpha_x1000;
                obs_b[s][d] = self.taker_priors[s][d].obs_beta_x1000;
            }
        }
        map.insert(
            "active_maker_fee_bps".to_string(),
            serde_json::json!(self.active_fees.maker_fee_bps.raw()),
        );
        map.insert(
            "active_taker_fee_bps".to_string(),
            serde_json::json!(self.active_fees.taker_fee_bps.raw()),
        );
        map.insert("taker_obs_alpha_x1000".to_string(), serde_json::json!(obs_a));
        map.insert("taker_obs_beta_x1000".to_string(), serde_json::json!(obs_b));
        map.insert("scar_ts_ns".to_string(), serde_json::json!(self.scar_ts_ns));
        map.insert(
            "scar_eq_vol_bps".to_string(),
            serde_json::json!(self.scar_eq_vol_bps),
        );
    }

    pub fn apply_from_scar_json(&mut self, val: &serde_json::Value) {
        let m_opt = val.get("active_maker_fee_bps").and_then(|v| v.as_i64());
        let t_opt = val.get("active_taker_fee_bps").and_then(|v| v.as_i64());
        if let (Some(m), Some(t)) = (m_opt, t_opt) {
            self.active_fees = FeeSchedule::from_live_rates_clamped(
                crate::types::ScaledBps(m),
                crate::types::ScaledBps(t),
            );
        }
        if let Some(rows_a) = val.get("taker_obs_alpha_x1000").and_then(|v| v.as_array()) {
            for (s, r) in rows_a.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(cols) = r.as_array() {
                    for (d, c) in cols.iter().take(2).enumerate() {
                        if let Some(n) = c.as_i64() {
                            self.taker_priors[s][d].obs_alpha_x1000 = n.max(0);
                        }
                    }
                }
            }
        }
        if let Some(rows_b) = val.get("taker_obs_beta_x1000").and_then(|v| v.as_array()) {
            for (s, r) in rows_b.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(cols) = r.as_array() {
                    for (d, c) in cols.iter().take(2).enumerate() {
                        if let Some(n) = c.as_i64() {
                            self.taker_priors[s][d].obs_beta_x1000 = n.max(0);
                        }
                    }
                }
            }
        }
        crate::kinematic_surface::extract_matrix_i64_12x2(val, "scar_ts_ns", &mut self.scar_ts_ns);
        crate::kinematic_surface::extract_matrix_i64_12x2(
            val,
            "scar_eq_vol_bps",
            &mut self.scar_eq_vol_bps,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BoundedScore, ScaledBps, TimestampNs};

    #[test]
    fn test_governor_plasticity_12x2_taker_prior_and_json_roundtrip() {
        let mut st = GovernorPlasticityState::new();
        assert_eq!(st.slot_taker_prob_permille(0, Side::Bid), 50);
        st.set_active_fee_schedule(FeeSchedule::from_live_rates_clamped(
            ScaledBps(120),
            ScaledBps(380),
        ));
        assert_eq!(st.maker_round_trip_fee_bps(), 240);

        for k in 0..10_i64 {
            st.observe_exit_outcome(3, Side::Ask, true, (k + 1) * 1_000_000_000, 5_200);
        }
        let tox_pm = st.slot_taker_prob_permille(3, Side::Ask);
        assert!(tox_pm > 250);
        assert_eq!(st.slot_taker_prob_permille(3, Side::Bid), 50);

        let mut map = serde_json::Map::new();
        st.write_to_scar_json_map(&mut map);
        let json_val = serde_json::Value::Object(map);

        let mut restored = GovernorPlasticityState::new();
        restored.apply_from_scar_json(&json_val);
        assert_eq!(restored.slot_taker_prob_permille(3, Side::Ask), tox_pm);
        assert_eq!(restored.maker_round_trip_fee_bps(), 240);
    }

    #[test]
    fn test_shadow_rehab_blocks_silence_smoothing_and_unlocks_btc_after_regime_shift() {
        let mut st = GovernorPlasticityState::new();
        let t0 = 1_794_700_000_000_000_000_i64;
        let tau_ns = 300_000_000_000_i64; // 300s
        st.observe_exit_outcome(0, Side::Bid, true, t0, 4_500);

        let mut snap = SymbolLobSnapshot::new_empty();
        snap.factor_ofi = BoundedScore::new_clamped(0.85);
        snap.factor_tfi = BoundedScore::new_clamped(0.85);
        snap.factor_geom = BoundedScore::new_clamped(0.65);

        // 1. Law III: 24 hours elapsed WITHOUT structural regime shift -> 0 shadow credits!
        for step in 1..=12_i64 {
            snap.ts_local_ns = TimestampNs(t0 + 86_400_000_000_000 + step * tau_ns);
            assert!(!st.evaluate_shadow_rehab_step(
                0, Side::Bid, 1, tau_ns, 4_600, 1_300, 85, &snap
            ));
        }
        assert_eq!(st.shadow_credits_for(0, Side::Bid), 0);

        // 2. True volatility regime shift (4_500 -> 7_200, +60% >= 38.2%) awards 3 spaced credits
        let t_shift = t0 + 7 * tau_ns;
        snap.ts_local_ns = TimestampNs(t_shift);
        assert!(!st.evaluate_shadow_rehab_step(
            0, Side::Bid, 1, tau_ns, 7_200, 1_300, 90, &snap
        ));
        assert_eq!(st.shadow_credits_for(0, Side::Bid), 1);

        // Anti-burst spacing (< tau_ns) blocks premature 2nd credit
        snap.ts_local_ns = TimestampNs(t_shift + 15_000_000_000);
        assert!(!st.evaluate_shadow_rehab_step(
            0, Side::Bid, 1, tau_ns, 7_200, 1_300, 90, &snap
        ));
        assert_eq!(st.shadow_credits_for(0, Side::Bid), 1);

        snap.ts_local_ns = TimestampNs(t_shift + tau_ns);
        assert!(!st.evaluate_shadow_rehab_step(
            0, Side::Bid, 1, tau_ns, 7_200, 1_300, 90, &snap
        ));
        assert_eq!(st.shadow_credits_for(0, Side::Bid), 2);

        snap.ts_local_ns = TimestampNs(t_shift + 2 * tau_ns);
        assert!(st.evaluate_shadow_rehab_step(
            0, Side::Bid, 1, tau_ns, 7_200, 1_300, 90, &snap
        ));
        assert_eq!(st.shadow_credits_for(0, Side::Bid), 0);
        assert_eq!(GovernorPlasticityState::contract_local_debt_golden(-2_000), -1_236);
    }
}

pub const DAILY_GOVERNOR_VAULT_PATH: &str = "data/vault/vortex_daily_governor.json";
pub const DAILY_GOVERNOR_SCHEMA_TAG: &str = "DAILY_SHIELD_V48";

const BASE_SLOT_RISK_PPM_F64: f64 = 2_400.0;
const PHI_DAILY: f64 = 1.618_033_988_749_895;
const PHI_SQ_DAILY: f64 = 2.618_033_988_749_895;
const INV_PHI_DAILY: f64 = 0.618_033_988_749_895;
const INV_PHI2_DAILY: f64 = 0.381_966_011_250_105;
const UTC_DAY_SPAN_NS: i64 = 86_400_000_000_000;
const MIN_CIVIL_UTC_NS: i64 = 1_577_836_800_000_000_000;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum DailyShieldPhase {
    #[default]
    ActiveHunting = 0,
    ProfitLockArmed = 1,
    HibernatingLossCap = 2,
    HibernatingProfitLocked = 3,
}

impl DailyShieldPhase {
    #[inline]
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::ProfitLockArmed,
            2 => Self::HibernatingLossCap,
            3 => Self::HibernatingProfitLocked,
            _ => Self::ActiveHunting,
        }
    }

    #[inline]
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ActiveHunting => "ACTIVE_HUNTING",
            Self::ProfitLockArmed => "PROFIT_RATCHET_ARMED",
            Self::HibernatingLossCap => "SLEEP_DAILY_LOSS_CAP",
            Self::HibernatingProfitLocked => "SLEEP_DAILY_GOAL_LOCKED",
        }
    }
}

/// Zero-Alloc O(1) Elastic UTC Daily High-Water-Mark & Drawdown Governor (V4.8).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DailySessionGovernor {
    pub utc_day_idx: i64,
    pub day_open_equity_1e8: i64,
    pub day_realized_pnl_1e8: i64,
    pub day_peak_ppm: i64,
    pub learned_pos_day_ema_ppm: i64,
    pub day_closed_trades: u32,
    pub day_wins: u32,
    pub day_losses: u32,
    pub phase: DailyShieldPhase,
}

impl Default for DailySessionGovernor {
    #[inline]
    fn default() -> Self {
        Self::new(6_500_000_000)
    }
}

#[allow(clippy::suboptimal_flops)]
impl DailySessionGovernor {
    #[inline]
    #[must_use]
    pub const fn new(initial_equity_1e8: i64) -> Self {
        let eq = if initial_equity_1e8 > 100_000_000 { initial_equity_1e8 } else { 100_000_000 };
        Self {
            utc_day_idx: 0,
            day_open_equity_1e8: eq,
            day_realized_pnl_1e8: 0,
            day_peak_ppm: 0,
            learned_pos_day_ema_ppm: 6_283,
            day_closed_trades: 0,
            day_wins: 0,
            day_losses: 0,
            phase: DailyShieldPhase::ActiveHunting,
        }
    }

    #[inline]
    #[must_use]
    pub const fn utc_day_from_ns(ts_ns: crate::types::TimestampNs) -> i64 {
        if ts_ns.0 < MIN_CIVIL_UTC_NS { 0 } else { ts_ns.0.div_euclid(UTC_DAY_SPAN_NS) }
    }

    #[inline]
    #[must_use]
    pub const fn is_hibernating(&self) -> bool {
        matches!(self.phase, DailyShieldPhase::HibernatingLossCap | DailyShieldPhase::HibernatingProfitLocked)
    }

    #[inline]
    #[must_use]
    pub const fn day_pnl_ppm(&self) -> i64 {
        let denom = if self.day_open_equity_1e8 > 100_000_000 { self.day_open_equity_1e8 } else { 100_000_000 };
        ((self.day_realized_pnl_1e8 as i128 * 1_000_000) / (denom as i128)) as i64
    }

    /// Synchronizes UTC civil day boundary (`00:00:00 UTC`), waking up autonomously on rollover.
    pub fn sync_utc_day(&mut self, ts_ns: crate::types::TimestampNs, live_equity_1e8: i64) -> bool {
        let cur_day = Self::utc_day_from_ns(ts_ns);
        if cur_day <= 0 {
            return false;
        }
        let clean_eq = live_equity_1e8.max(100_000_000);
        if self.utc_day_idx == 0 {
            self.utc_day_idx = cur_day;
            self.day_open_equity_1e8 = clean_eq;
            return false;
        }
        if cur_day <= self.utc_day_idx {
            return false;
        }
        let end_ppm = self.day_pnl_ppm();
        if end_ppm > 0 {
            self.learned_pos_day_ema_ppm =
                ((864 * self.learned_pos_day_ema_ppm + 136 * end_ppm) / 1_000).clamp(2_400, 35_000);
        }
        self.utc_day_idx = cur_day;
        self.day_open_equity_1e8 = clean_eq;
        self.day_realized_pnl_1e8 = 0;
        self.day_peak_ppm = 0;
        self.day_closed_trades = 0;
        self.day_wins = 0;
        self.day_losses = 0;
        self.phase = DailyShieldPhase::ActiveHunting;
        true
    }

    #[inline]
    #[must_use]
    pub fn elastic_loss_stop_ppm(&self, sinking_reserve_bps_x100: i64, fee_rt_bps: i64) -> i64 {
        let reserve_pos = (sinking_reserve_bps_x100.max(0)) as f64;
        let denom = (4 * fee_rt_bps.max(100)) as f64;
        let mult = PHI_DAILY + (PHI_SQ_DAILY - PHI_DAILY) * (reserve_pos / denom).tanh();
        -((BASE_SLOT_RISK_PPM_F64 * mult).round() as i64).clamp(3_883, 6_283)
    }

    #[inline]
    #[must_use]
    pub fn elastic_goal_arm_ppm(
        &self,
        sinking_reserve_bps_x100: i64,
        fee_rt_bps: i64,
        eq_vol_bps: i64,
        port_ir_x100: i64,
    ) -> i64 {
        let abs_stop = self.elastic_loss_stop_ppm(sinking_reserve_bps_x100, fee_rt_bps).abs() as f64;
        let vol_ratio = ((eq_vol_bps.max(600) as f64) / ((6 * fee_rt_bps.max(100)) as f64)).sqrt();
        let ir_boost = 1.0 + INV_PHI2_DAILY * ((port_ir_x100 as f64) / 100.0).tanh();
        let nu_reg = (vol_ratio * ir_boost).clamp(INV_PHI_DAILY, PHI_DAILY);
        let dyn_arm = (PHI_DAILY * abs_stop * nu_reg).round() as i64;
        let learned_arm = ((INV_PHI_DAILY * (self.learned_pos_day_ema_ppm as f64)).round() as i64).max(3_883);
        dyn_arm.max(learned_arm).clamp(3_883, 25_000)
    }

    #[inline]
    #[must_use]
    pub fn locked_profit_floor_ppm(&self, goal_arm_ppm: i64) -> i64 {
        let arm = goal_arm_ppm.max(1);
        if self.day_peak_ppm < arm {
            return 0;
        }
        let h = self.day_peak_ppm as f64;
        let g = arm as f64;
        let keep_frac = 1.0 - INV_PHI2_DAILY * (-(h - g) / g).exp();
        ((h * keep_frac).round() as i64).clamp(1, self.day_peak_ppm)
    }

    pub fn observe_realized_cashflow(
        &mut self,
        ts_ns: crate::types::TimestampNs,
        delta_pnl_1e8: i64,
        is_trade_close: bool,
        is_win: bool,
        is_loss: bool,
        live_equity_1e8: i64,
        sinking_reserve_bps_x100: i64,
        fee_rt_bps: i64,
        eq_vol_bps: i64,
        port_ir_x100: i64,
    ) -> DailyShieldPhase {
        let _ = self.sync_utc_day(ts_ns, live_equity_1e8.saturating_sub(delta_pnl_1e8));
        self.day_realized_pnl_1e8 = self.day_realized_pnl_1e8.saturating_add(delta_pnl_1e8);
        if is_trade_close {
            self.day_closed_trades = self.day_closed_trades.saturating_add(1);
            if is_win { self.day_wins = self.day_wins.saturating_add(1); }
            if is_loss { self.day_losses = self.day_losses.saturating_add(1); }
        }
        let cur_ppm = self.day_pnl_ppm();
        if cur_ppm > self.day_peak_ppm {
            self.day_peak_ppm = cur_ppm;
        }
        self.evaluate_intraday_gate(
            ts_ns, live_equity_1e8, sinking_reserve_bps_x100, fee_rt_bps, eq_vol_bps, port_ir_x100, 0.70,
        )
    }

    pub fn evaluate_intraday_gate(
        &mut self,
        ts_ns: crate::types::TimestampNs,
        live_equity_1e8: i64,
        sinking_reserve_bps_x100: i64,
        fee_rt_bps: i64,
        eq_vol_bps: i64,
        port_ir_x100: i64,
        intraday_percentile: f64,
    ) -> DailyShieldPhase {
        let _ = self.sync_utc_day(ts_ns, live_equity_1e8);
        if self.is_hibernating() {
            return self.phase;
        }
        let cur_ppm = self.day_pnl_ppm();
        if cur_ppm > self.day_peak_ppm {
            self.day_peak_ppm = cur_ppm;
        }
        let stop_ppm = self.elastic_loss_stop_ppm(sinking_reserve_bps_x100, fee_rt_bps);
        if cur_ppm <= stop_ppm {
            self.phase = DailyShieldPhase::HibernatingLossCap;
            return self.phase;
        }
        let arm_ppm = self.elastic_goal_arm_ppm(sinking_reserve_bps_x100, fee_rt_bps, eq_vol_bps, port_ir_x100);
        if self.day_peak_ppm >= arm_ppm {
            self.phase = DailyShieldPhase::ProfitLockArmed;
            let floor_ppm = self.locked_profit_floor_ppm(arm_ppm);
            if (self.day_closed_trades >= 2 && cur_ppm <= floor_ppm)
                || (cur_ppm >= floor_ppm && intraday_percentile < INV_PHI2_DAILY)
            {
                self.phase = DailyShieldPhase::HibernatingProfitLocked;
            }
        }
        self.phase
    }

    #[inline]
    #[must_use]
    pub fn kelly_boundary_damper_permille(
        &self,
        sinking_reserve_bps_x100: i64,
        fee_rt_bps: i64,
        eq_vol_bps: i64,
        port_ir_x100: i64,
    ) -> i64 {
        if self.is_hibernating() {
            return 0;
        }
        let cur_ppm = self.day_pnl_ppm();
        let stop_ppm = self.elastic_loss_stop_ppm(sinking_reserve_bps_x100, fee_rt_bps);
        let arm_ppm = self.elastic_goal_arm_ppm(sinking_reserve_bps_x100, fee_rt_bps, eq_vol_bps, port_ir_x100);
        let active_floor = if matches!(self.phase, DailyShieldPhase::ProfitLockArmed) {
            self.locked_profit_floor_ppm(arm_ppm)
        } else {
            stop_ppm
        };
        let span = stop_ppm.abs().max(1) as f64;
        let u = (((cur_ppm - active_floor).max(0) as f64) / span).clamp(0.0, 1.0);
        let s3 = u * u * (3.0 - 2.0 * u);
        let mult = INV_PHI_DAILY + (1.0 - INV_PHI_DAILY) * s3;
        ((mult * 1_000.0).round() as i64).clamp(618, 1_000)
    }

    #[must_use]
    pub fn to_json_value(&self, ts_ns: crate::types::TimestampNs) -> serde_json::Value {
        serde_json::json!({
            "event": DAILY_GOVERNOR_SCHEMA_TAG,
            "ts_ns": ts_ns.0,
            "utc_day_idx": self.utc_day_idx,
            "day_open_equity_1e8": self.day_open_equity_1e8,
            "day_realized_pnl_1e8": self.day_realized_pnl_1e8,
            "day_peak_ppm": self.day_peak_ppm,
            "learned_pos_day_ema_ppm": self.learned_pos_day_ema_ppm,
            "day_closed_trades": self.day_closed_trades,
            "day_wins": self.day_wins,
            "day_losses": self.day_losses,
            "phase_code": self.phase as u8,
        })
    }

    pub fn apply_from_json(&mut self, val: &serde_json::Value) {
        if val.get("event").and_then(serde_json::Value::as_str) != Some(DAILY_GOVERNOR_SCHEMA_TAG) {
            return;
        }
        if let Some(d) = val.get("utc_day_idx").and_then(serde_json::Value::as_i64) { self.utc_day_idx = d.max(0); }
        if let Some(eq) = val.get("day_open_equity_1e8").and_then(serde_json::Value::as_i64) {
            self.day_open_equity_1e8 = eq.max(100_000_000);
        }
        if let Some(p) = val.get("day_realized_pnl_1e8").and_then(serde_json::Value::as_i64) { self.day_realized_pnl_1e8 = p; }
        if let Some(pk) = val.get("day_peak_ppm").and_then(serde_json::Value::as_i64) { self.day_peak_ppm = pk.max(0); }
        if let Some(em) = val.get("learned_pos_day_ema_ppm").and_then(serde_json::Value::as_i64) {
            self.learned_pos_day_ema_ppm = em.clamp(2_400, 35_000);
        }
        if let Some(ct) = val.get("day_closed_trades").and_then(serde_json::Value::as_u64) { self.day_closed_trades = ct as u32; }
        if let Some(w) = val.get("day_wins").and_then(serde_json::Value::as_u64) { self.day_wins = w as u32; }
        if let Some(l) = val.get("day_losses").and_then(serde_json::Value::as_u64) { self.day_losses = l as u32; }
        if let Some(ph) = val.get("phase_code").and_then(serde_json::Value::as_u64) {
            self.phase = DailyShieldPhase::from_code((ph.min(255)) as u8);
        }
    }
}

#[cfg(test)]
mod daily_shield_v48_tests {
    use super::*;
    use crate::types::TimestampNs;

    #[test]
    fn test_v48_elastic_daily_session_governor_ratchet_scars_and_utc_rollover() {
        let eq0 = 6_513_770_000_i64; // 65.1377 USDT
        let mut dg = DailySessionGovernor::new(eq0);
        let day1_ns = TimestampNs(1_794_787_200_000_000_000 + 3_600_000_000_000);
        assert!(!dg.sync_utc_day(day1_ns, eq0));
        // Empty sinking reserve tightens loss stop to -3,883 ppm; full reserve expands toward -6,283 ppm
        let stop_empty = dg.elastic_loss_stop_ppm(0, 400);
        let stop_rich = dg.elastic_loss_stop_ppm(4_000, 400);
        assert_eq!(stop_empty, -3_883);
        assert!(stop_rich < -5_800 && stop_rich >= -6_283);

        // Surpassing daily goal arms ratchet without cutting right-tail trend, then pullback locks green day
        let ph1 = dg.observe_realized_cashflow(day1_ns, 70_000_000, true, true, false, eq0 + 70_000_000, 381, 400, 3_200, 45);
        assert_eq!(ph1, DailyShieldPhase::ProfitLockArmed);
        assert!(dg.kelly_boundary_damper_permille(381, 400, 3_200, 45) >= 618);
        let ph2 = dg.observe_realized_cashflow(day1_ns, -26_000_000, true, false, true, eq0 + 44_000_000, 381, 400, 3_200, 45);
        assert_eq!(ph2, DailyShieldPhase::HibernatingProfitLocked);
        assert!(dg.is_hibernating() && dg.day_pnl_ppm() > 0);

        // Next UTC day 00:00:05 wakes autonomously and updates learned positive day EMA
        let day2_ns = TimestampNs(day1_ns.0 + UTC_DAY_SPAN_NS);
        assert!(dg.sync_utc_day(day2_ns, eq0 + 44_000_000));
        assert!(!dg.is_hibernating() && dg.phase == DailyShieldPhase::ActiveHunting);
    }
}
