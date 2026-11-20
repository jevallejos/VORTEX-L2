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
use crate::types::{ScaledBps, Side};

const INV_PHI_PM: i64 = 618;
const INV_PHI2_PM: i64 = 382;
const MIN_REHAB_SNR: f64 = 0.72;
const SHADOW_CREDITS_REQUIRED: u8 = 3;
pub const DEFAULT_FILL_PRIOR_X1000: i64 = 1_000;
pub const DEFAULT_CONT_RATIO_X1000: i64 = 1_000;
pub const MIN_CONT_RATIO_X1000: i64 = 618;
pub const MAX_CONT_RATIO_X1000: i64 = 2_618;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct GovernorPlasticityState {
    active_fees: FeeSchedule,
    portfolio_taker_prior: BetaBinomialTakerPrior,
    taker_priors: [[BetaBinomialTakerPrior; 2]; MAX_ACTIVE_PAIRS],
    scar_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    scar_eq_vol_bps: [[i64; 2]; MAX_ACTIVE_PAIRS],
    shadow_credits: [[u8; 2]; MAX_ACTIVE_PAIRS],
    last_credit_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    pub fill_obs_alpha_x1000: [i64; MAX_ACTIVE_PAIRS],
    pub fill_obs_beta_x1000: [i64; MAX_ACTIVE_PAIRS],
    pub cont_ratio_x1000: [[i64; 2]; MAX_ACTIVE_PAIRS],
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
            portfolio_taker_prior: seed_prior,
            taker_priors: [[seed_prior; 2]; MAX_ACTIVE_PAIRS],
            scar_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            scar_eq_vol_bps: [[4_800; 2]; MAX_ACTIVE_PAIRS],
            shadow_credits: [[0; 2]; MAX_ACTIVE_PAIRS],
            last_credit_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            fill_obs_alpha_x1000: [DEFAULT_FILL_PRIOR_X1000; MAX_ACTIVE_PAIRS],
            fill_obs_beta_x1000: [DEFAULT_FILL_PRIOR_X1000; MAX_ACTIVE_PAIRS],
            cont_ratio_x1000: [[DEFAULT_CONT_RATIO_X1000; 2]; MAX_ACTIVE_PAIRS],
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
    const fn hermite_s3_const(x: f64, lo: f64, hi: f64) -> f64 {
        if hi <= lo {
            return if x >= hi { 1.0 } else { 0.0 };
        }
        let raw_u = (x - lo) / (hi - lo);
        let u = if raw_u < 0.0 {
            0.0
        } else if raw_u > 1.0 {
            1.0
        } else {
            raw_u
        };
        u * u * (3.0 - 2.0 * u)
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub const fn slot_taker_prob_permille(&self, slot_idx: usize, side: Side) -> i64 {
        const PHI_P1: f64 = 1.618_033_988_749_895;
        const PHI_CUBE_P1: f64 = 4.236_067_977_499_79;
        const INV_PHI2_P1: f64 = 0.381_966_011_250_105;

        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 50;
        }
        let d = Self::dir_idx(side);
        let cell_pr = self.taker_priors[slot_idx][d];
        let p_cell = cell_pr.posterior_permille();
        let mut scar_slots = 0_i64;
        let mut i = 0_usize;
        while i < MAX_ACTIVE_PAIRS {
            if self.taker_priors[i][0].obs_alpha_x1000 >= 800
                || self.taker_priors[i][1].obs_alpha_x1000 >= 800
            {
                scar_slots += 1;
            }
            i += 1;
        }
        if scar_slots <= 1 {
            return if p_cell < 20 { 20 } else if p_cell > 450 { 450 } else { p_cell };
        }
        let sys_df = if scar_slots >= 3 { 1.0 } else { 0.50 };
        let p_port = self.portfolio_taker_prior.posterior_permille() as f64;
        let raw_pt = cell_pr.prior_alpha_x1000.saturating_add(cell_pr.prior_beta_x1000);
        let prior_tot = if raw_pt > 1 { raw_pt } else { 1 };
        let p_0 = ((cell_pr.prior_alpha_x1000 as f64) * 1_000.0) / (prior_tot as f64);
        let p_cell_f = p_cell as f64;
        let raw_n = cell_pr.obs_alpha_x1000.saturating_add(cell_pr.obs_beta_x1000);
        let n_cell = ((if raw_n > 0 { raw_n } else { 0 }) as f64) * 1e-3;
        let s_n = Self::hermite_s3_const(n_cell, PHI_P1, PHI_CUBE_P1);
        let s_clean = Self::hermite_s3_const(p_0 - p_cell_f, 0.0, 0.5 * p_0);
        let raw_imm = 1.0 - s_n * s_clean;
        let clean_immunity = if raw_imm < 0.0 { 0.0 } else if raw_imm > 1.0 { 1.0 } else { raw_imm };
        let shrink_w = (PHI_CUBE_P1 / (PHI_CUBE_P1 + n_cell)) * clean_immunity;
        let ex_port = if p_port > p_cell_f { p_port - p_cell_f } else { 0.0 };
        let lift_pm = ex_port * INV_PHI2_P1 * shrink_w * sys_df;
        let surv_raw = 1_000 - p_cell;
        let surv_cell = ((if surv_raw > 0 { surv_raw } else { 0 }) as f64) * 1e-3;
        let eff_pm = (p_cell_f + surv_cell * lift_pm + 0.5) as i64;
        if eff_pm < 20 { 20 } else if eff_pm > 450 { 450 } else { eff_pm }
    }

    #[inline]
    #[must_use]
    pub const fn portfolio_taker_prob_permille(&self) -> i64 {
        self.portfolio_taker_prior.posterior_permille()
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn slot_fill_rate_eta(&self, slot_idx: usize) -> f64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 0.50;
        }
        let a = self.fill_obs_alpha_x1000[slot_idx].max(1) as f64;
        let b = self.fill_obs_beta_x1000[slot_idx].max(1) as f64;
        (a / (a + b)).clamp(0.0, 1.0)
    }

    #[inline]
    pub fn observe_passive_entry_fill(&mut self, slot_idx: usize) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let next_a = self.fill_obs_alpha_x1000[slot_idx].saturating_add(1_000);
        let cur_b = self.fill_obs_beta_x1000[slot_idx].max(250);
        if next_a.saturating_add(cur_b) > 24_000 {
            self.fill_obs_alpha_x1000[slot_idx] = ((next_a.saturating_mul(938)) / 1_000).max(250);
            self.fill_obs_beta_x1000[slot_idx] = ((cur_b.saturating_mul(938)) / 1_000).max(250);
        } else {
            self.fill_obs_alpha_x1000[slot_idx] = next_a;
        }
    }

    #[inline]
    pub fn observe_favorable_entry_miss(&mut self, slot_idx: usize) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let cur_a = self.fill_obs_alpha_x1000[slot_idx].max(250);
        let next_b = self.fill_obs_beta_x1000[slot_idx].saturating_add(1_000);
        if cur_a.saturating_add(next_b) > 24_000 {
            self.fill_obs_alpha_x1000[slot_idx] = ((cur_a.saturating_mul(938)) / 1_000).max(250);
            self.fill_obs_beta_x1000[slot_idx] = ((next_b.saturating_mul(938)) / 1_000).max(250);
        } else {
            self.fill_obs_beta_x1000[slot_idx] = next_b;
        }
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
        self.portfolio_taker_prior.observe_exit(is_adverse_taker_sl);
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
            self.taker_priors[slot_idx][d].obs_alpha_x1000 =
                (self.taker_priors[slot_idx][d].obs_alpha_x1000.saturating_mul(INV_PHI_PM)) / 1_000;
            self.portfolio_taker_prior.obs_alpha_x1000 =
                (self.portfolio_taker_prior.obs_alpha_x1000.saturating_mul(854)) / 1_000;
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
        map.insert(
            "portfolio_obs_alpha_x1000".to_string(),
            serde_json::json!(self.portfolio_taker_prior.obs_alpha_x1000),
        );
        map.insert(
            "portfolio_obs_beta_x1000".to_string(),
            serde_json::json!(self.portfolio_taker_prior.obs_beta_x1000),
        );
        map.insert("scar_ts_ns".to_string(), serde_json::json!(self.scar_ts_ns));
        map.insert(
            "scar_eq_vol_bps".to_string(),
            serde_json::json!(self.scar_eq_vol_bps),
        );
        map.insert(
            "fill_obs_alpha_x1000".to_string(),
            serde_json::json!(self.fill_obs_alpha_x1000),
        );
        map.insert(
            "fill_obs_beta_x1000".to_string(),
            serde_json::json!(self.fill_obs_beta_x1000),
        );
        map.insert(
            "cont_ratio_x1000".to_string(),
            serde_json::json!(self.cont_ratio_x1000),
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
        if let Some(pa) = val.get("portfolio_obs_alpha_x1000").and_then(|v| v.as_i64()) {
            self.portfolio_taker_prior.obs_alpha_x1000 = pa.clamp(0, 500_000);
        }
        if let Some(pb) = val.get("portfolio_obs_beta_x1000").and_then(|v| v.as_i64()) {
            self.portfolio_taker_prior.obs_beta_x1000 = pb.clamp(0, 500_000);
        }
        crate::kinematic_surface::extract_matrix_i64_12x2(val, "scar_ts_ns", &mut self.scar_ts_ns);
        crate::kinematic_surface::extract_matrix_i64_12x2(
            val,
            "scar_eq_vol_bps",
            &mut self.scar_eq_vol_bps,
        );
        for row in &mut self.scar_eq_vol_bps {
            for cell in row.iter_mut() {
                if *cell != 0 {
                    *cell = (*cell).clamp(1_200, 24_000);
                }
            }
        }
        self.fill_obs_alpha_x1000 = [DEFAULT_FILL_PRIOR_X1000; MAX_ACTIVE_PAIRS];
        self.fill_obs_beta_x1000 = [DEFAULT_FILL_PRIOR_X1000; MAX_ACTIVE_PAIRS];
        if let Some(arr_a) = val.get("fill_obs_alpha_x1000").and_then(|v| v.as_array()) {
            for (s, c) in arr_a.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = c.as_i64() {
                    self.fill_obs_alpha_x1000[s] = n.clamp(100, 500_000);
                }
            }
        }
        if let Some(arr_b) = val.get("fill_obs_beta_x1000").and_then(|v| v.as_array()) {
            for (s, c) in arr_b.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = c.as_i64() {
                    self.fill_obs_beta_x1000[s] = n.clamp(100, 500_000);
                }
            }
        }
        self.cont_ratio_x1000 = [[DEFAULT_CONT_RATIO_X1000; 2]; MAX_ACTIVE_PAIRS];
        crate::kinematic_surface::extract_matrix_i64_12x2(
            val,
            "cont_ratio_x1000",
            &mut self.cont_ratio_x1000,
        );
        for row in &mut self.cont_ratio_x1000 {
            for cell in row.iter_mut() {
                *cell = if *cell <= 0 {
                    DEFAULT_CONT_RATIO_X1000
                } else {
                    (*cell).clamp(MIN_CONT_RATIO_X1000, MAX_CONT_RATIO_X1000)
                };
            }
        }
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
    pub armed_goal_ppm: i64,
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
            armed_goal_ppm: 0,
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
        self.armed_goal_ppm = 0;
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
    #[allow(clippy::cast_precision_loss)]
    pub fn daily_loss_used_ratio(&self, sinking_reserve_bps_x100: i64, fee_rt_bps: i64) -> f64 {
        if matches!(self.phase, DailyShieldPhase::HibernatingLossCap) {
            return 1.0;
        }
        let cur_ppm = self.day_pnl_ppm();
        if cur_ppm >= 0 {
            return 0.0;
        }
        let abs_stop = self
            .elastic_loss_stop_ppm(sinking_reserve_bps_x100, fee_rt_bps)
            .abs()
            .max(1) as f64;
        (((-cur_ppm) as f64) / abs_stop).clamp(0.0, 2.0)
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
        if self.day_peak_ppm >= arm_ppm || matches!(self.phase, DailyShieldPhase::ProfitLockArmed) {
            self.phase = DailyShieldPhase::ProfitLockArmed;
            let eff_arm = if self.armed_goal_ppm > 0 {
                self.armed_goal_ppm.min(arm_ppm)
            } else {
                arm_ppm.min(self.day_peak_ppm.max(3_883))
            };
            self.armed_goal_ppm = eff_arm;
            let floor_ppm = self.locked_profit_floor_ppm(eff_arm);
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
            let eff_arm = if self.armed_goal_ppm > 0 {
                self.armed_goal_ppm.min(arm_ppm)
            } else {
                arm_ppm.min(self.day_peak_ppm.max(3_883))
            };
            self.locked_profit_floor_ppm(eff_arm)
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
            "armed_goal_ppm": self.armed_goal_ppm,
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
        if let Some(ag) = val.get("armed_goal_ppm").and_then(serde_json::Value::as_i64) { self.armed_goal_ppm = ag.max(0); }
        if let Some(em) = val.get("learned_pos_day_ema_ppm").and_then(serde_json::Value::as_i64) {
            self.learned_pos_day_ema_ppm = em.clamp(2_400, 35_000);
        }
        if let Some(ct) = val.get("day_closed_trades").and_then(serde_json::Value::as_u64) { self.day_closed_trades = ct as u32; }
        if let Some(w) = val.get("day_wins").and_then(serde_json::Value::as_u64) { self.day_wins = w as u32; }
        if let Some(l) = val.get("day_losses").and_then(serde_json::Value::as_u64) { self.day_losses = l as u32; }
        if let Some(ph) = val.get("phase_code").and_then(serde_json::Value::as_u64) {
            self.phase = DailyShieldPhase::from_code((ph.min(255)) as u8);
        }
        if matches!(self.phase, DailyShieldPhase::ProfitLockArmed) && self.armed_goal_ppm <= 0 && self.day_peak_ppm > 0 {
            self.armed_goal_ppm = self.day_peak_ppm.max(3_883);
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

#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn observe_outcome_surprise_weighted(
    prior: &mut BetaBinomialTakerPrior,
    adverse_sl: bool,
    mfe_bps: ScaledBps,
    fee_rt_mkr: ScaledBps,
) -> i64 {
    const PHI_S: f64 = 1.618_033_988_749_895;
    const INV_PHI2_S: f64 = 0.381_966_011_250_105;
    let p_t = (prior.posterior_permille().clamp(20, 450) as f64) * 1e-3;
    let fee_f = (fee_rt_mkr.raw().max(100)) as f64;
    let s3_zero = crate::wave_elliott::hermite_smoothstep_c1(
        fee_f - (mfe_bps.raw().max(0) as f64),
        0.0,
        fee_f,
    );
    if adverse_sl {
        let s_rec = crate::wave_elliott::hermite_smoothstep_c1(
            (prior.obs_alpha_x1000.max(0) as f64) * 1e-3,
            0.25,
            1.25,
        );
        let gamma_eff = (960 - (236.0 * (1.0 - p_t) * s3_zero * (0.50 + 0.50 * s_rec)).floor() as i64).clamp(724, 960);
        let shock_mult = 1.0 + INV_PHI2_S * (1.0 - p_t) * s3_zero * (1.0 + PHI_S * s_rec);
        let delta_a = (1_000.0 * shock_mult).round() as i64;
        prior.obs_alpha_x1000 = ((prior.obs_alpha_x1000.saturating_mul(960)) / 1_000).saturating_add(delta_a);
        prior.obs_beta_x1000 = (prior.obs_beta_x1000.saturating_mul(gamma_eff)) / 1_000;
    } else {
        prior.obs_alpha_x1000 = (prior.obs_alpha_x1000.saturating_mul(960)) / 1_000;
        prior.obs_beta_x1000 = ((prior.obs_beta_x1000.saturating_mul(960)) / 1_000).saturating_add(1_000);
    }
    prior.posterior_permille()
}

/// Continuous `C1` fractional adverse evidence weight in `[0.0, 1.0]` for Maker Bailouts closing with `net_bps < 0`.
/// Anchored to `S_3(-net_bps; 0.5 * C_RT_mkr, SL_bps)` with smooth `C1` sub-fee tail `(0, 0.5 * C_RT_mkr]`.
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_fractional_maker_bailout_scar_weight_c1(
    net_bps: ScaledBps,
    fee_rt_mkr: ScaledBps,
    sl_bps: ScaledBps,
) -> f64 {
    const INV_PHI4_B: f64 = 0.145_898_033_750_315;
    if net_bps.raw() >= 0 {
        return 0.0;
    }
    let loss_sbps = (-net_bps.raw()) as f64;
    let half_fee = 0.5 * (fee_rt_mkr.raw().max(100) as f64);
    let sl_hi = (sl_bps.raw() as f64).max(half_fee + 100.0);
    let s_core = crate::wave_elliott::hermite_smoothstep_c1(loss_sbps, half_fee, sl_hi);
    let s_tail = crate::wave_elliott::hermite_smoothstep_c1(loss_sbps, 0.0, half_fee);
    (INV_PHI4_B * s_tail + (1.0 - INV_PHI4_B) * s_core).clamp(0.0, 1.0)
}

impl GovernorPlasticityState {
    #[inline]
    #[must_use]
    pub const fn slot_cont_ratio_x1000(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return DEFAULT_CONT_RATIO_X1000;
        }
        let raw = self.cont_ratio_x1000[slot_idx][Self::dir_idx(side)];
        if raw < MIN_CONT_RATIO_X1000 {
            MIN_CONT_RATIO_X1000
        } else if raw > MAX_CONT_RATIO_X1000 {
            MAX_CONT_RATIO_X1000
        } else {
            raw
        }
    }

    /// EWMA (`eta = INV_PHI_CUBE`) continuation ratio observer updating `cont_ratio_x1000` in `[618, 2618]`
    /// (`[INV_PHI, PHI^2]`) from `max(mfe_bps, markout_60s_favorable_bps) / t1_floor_bps` (V5.5 O3).
    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn observe_continuation_sample(
        &mut self,
        slot_idx: usize,
        side: Side,
        mfe_sbps: i64,
        markout_60s_fav_sbps: i64,
        t1_floor_sbps: i64,
    ) -> i64 {
        const ETA_INV_PHI3: f64 = 0.236_067_977_499_79;
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return DEFAULT_CONT_RATIO_X1000;
        }
        let d = Self::dir_idx(side);
        let denom = (t1_floor_sbps.max(100)) as f64;
        let peak_fav = (mfe_sbps.max(markout_60s_fav_sbps).max(0)) as f64;
        let sample_x1000 = (((peak_fav / denom) * 1_000.0).round() as i64)
            .clamp(MIN_CONT_RATIO_X1000, MAX_CONT_RATIO_X1000);
        let prev = (self.slot_cont_ratio_x1000(slot_idx, side)) as f64;
        let updated = ((1.0 - ETA_INV_PHI3) * prev + ETA_INV_PHI3 * (sample_x1000 as f64))
            .round() as i64;
        let clamped = updated.clamp(MIN_CONT_RATIO_X1000, MAX_CONT_RATIO_X1000);
        self.cont_ratio_x1000[slot_idx][d] = clamped;
        clamped
    }

    #[inline]
    pub fn observe_exit_outcome_with_mfe(
        &mut self,
        slot_idx: usize,
        side: Side,
        is_adverse_taker_sl: bool,
        mfe_bps: ScaledBps,
        ts_ns: i64,
        eq_vol_bps: i64,
    ) {
        let synth_net = if is_adverse_taker_sl { ScaledBps(-2_000) } else { ScaledBps(200) };
        self.observe_exit_outcome_with_net_and_mfe(
            slot_idx,
            side,
            is_adverse_taker_sl,
            synth_net,
            mfe_bps,
            ts_ns,
            eq_vol_bps,
        );
    }

    #[inline]
    pub fn observe_exit_outcome_with_net_and_mfe(
        &mut self,
        slot_idx: usize,
        side: Side,
        is_adverse_taker_sl: bool,
        net_bps: ScaledBps,
        mfe_bps: ScaledBps,
        ts_ns: i64,
        eq_vol_bps: i64,
    ) {
        self.observe_exit_outcome_core(
            slot_idx,
            side,
            is_adverse_taker_sl,
            net_bps,
            mfe_bps,
            true,
            ts_ns,
            eq_vol_bps,
        );
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
    pub(crate) fn observe_exit_outcome_core(
        &mut self,
        slot_idx: usize,
        side: Side,
        is_adverse_taker_sl: bool,
        net_bps: ScaledBps,
        mfe_bps: ScaledBps,
        has_uncensored_mfe: bool,
        ts_ns: i64,
        eq_vol_bps: i64,
    ) {
        const INV_PHI2_S: f64 = 0.381_966_011_250_105;
        const INV_PHI3_S: f64 = 0.236_067_977_499_79;
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let d = Self::dir_idx(side);
        let fee_rt_raw = self.maker_round_trip_fee_bps();
        let fee_rt = ScaledBps(fee_rt_raw);
        if !is_adverse_taker_sl && net_bps.raw() < 0 {
            let sl_bps = crate::types::PairSpec::elastic_server_sl_distance_bps_with_fees(
                ScaledBps(eq_vol_bps.max(1_200)),
                self.active_fees,
            );
            let w_scar = compute_fractional_maker_bailout_scar_weight_c1(net_bps, fee_rt, sl_bps);
            let a_add = (1_000.0 * w_scar).round() as i64;
            let b_add = (1_000.0 * (1.0 - w_scar) * INV_PHI2_S).round() as i64;
            let pr = &mut self.taker_priors[slot_idx][d];
            pr.obs_alpha_x1000 = ((pr.obs_alpha_x1000.saturating_mul(960)) / 1_000).saturating_add(a_add);
            pr.obs_beta_x1000 = ((pr.obs_beta_x1000.saturating_mul(960)) / 1_000).saturating_add(b_add);
            self.portfolio_taker_prior.obs_alpha_x1000 =
                ((self.portfolio_taker_prior.obs_alpha_x1000.saturating_mul(985)) / 1_000).saturating_add(a_add / 2);
            self.portfolio_taker_prior.obs_beta_x1000 =
                ((self.portfolio_taker_prior.obs_beta_x1000.saturating_mul(985)) / 1_000).saturating_add(b_add);
            self.scar_ts_ns[slot_idx][d] = ts_ns;
            self.scar_eq_vol_bps[slot_idx][d] = eq_vol_bps.max(1_200);
            self.shadow_credits[slot_idx][d] = 0;
            self.last_credit_ts_ns[slot_idx][d] = ts_ns;
            return;
        }
        if has_uncensored_mfe && !is_adverse_taker_sl && net_bps.raw() > 0 && mfe_bps.raw() > 0 {
            let t1_floor_sbps = crate::exit_quoter::compute_elastic_offensive_win_floor_bps(
                fee_rt_raw,
                ScaledBps(eq_vol_bps.max(1_200)),
            )
            .raw();
            let _ = self.observe_continuation_sample(
                slot_idx,
                side,
                mfe_bps.raw(),
                0,
                t1_floor_sbps,
            );
        }
        let is_stressed_scratch = !is_adverse_taker_sl
            && net_bps.raw() >= 0
            && net_bps.raw() < fee_rt_raw / 4
            && mfe_bps.raw() < (fee_rt_raw * 13) / 10;
        if is_stressed_scratch {
            let gap = ((fee_rt_raw * 13) / 10 - mfe_bps.raw().max(0)).max(0) as f64;
            let s_stress = crate::wave_elliott::hermite_smoothstep_c1(gap, 0.0, fee_rt_raw.max(100) as f64);
            let a_add = (1_000.0 * INV_PHI3_S * s_stress).round() as i64;
            let b_add = (1_000 - a_add).max(382);
            let pr = &mut self.taker_priors[slot_idx][d];
            pr.obs_alpha_x1000 = ((pr.obs_alpha_x1000.saturating_mul(960)) / 1_000).saturating_add(a_add);
            pr.obs_beta_x1000 = ((pr.obs_beta_x1000.saturating_mul(960)) / 1_000).saturating_add(b_add);
            self.portfolio_taker_prior.obs_alpha_x1000 =
                ((self.portfolio_taker_prior.obs_alpha_x1000.saturating_mul(985)) / 1_000).saturating_add(a_add / 2);
            self.portfolio_taker_prior.obs_beta_x1000 =
                ((self.portfolio_taker_prior.obs_beta_x1000.saturating_mul(985)) / 1_000).saturating_add(b_add);
            self.shadow_credits[slot_idx][d] = 0;
            return;
        }
        let _ = observe_outcome_surprise_weighted(&mut self.taker_priors[slot_idx][d], is_adverse_taker_sl, mfe_bps, fee_rt);
        self.portfolio_taker_prior.obs_alpha_x1000 =
            (self.portfolio_taker_prior.obs_alpha_x1000.saturating_mul(985)) / 1_000
                + if is_adverse_taker_sl { 1_000 } else { 0 };
        self.portfolio_taker_prior.obs_beta_x1000 =
            (self.portfolio_taker_prior.obs_beta_x1000.saturating_mul(985)) / 1_000
                + if is_adverse_taker_sl { 0 } else { 1_000 };
        if is_adverse_taker_sl {
            self.scar_ts_ns[slot_idx][d] = ts_ns;
            self.scar_eq_vol_bps[slot_idx][d] = eq_vol_bps.max(1_200);
            self.shadow_credits[slot_idx][d] = 0;
            self.last_credit_ts_ns[slot_idx][d] = ts_ns;
        } else {
            self.shadow_credits[slot_idx][d] = 0;
        }
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn observe_sympathetic_chop_coupling(
        &mut self,
        slot_idx: usize,
        closed_side: Side,
        mfe_bps: ScaledBps,
    ) {
        const INV_PHI_S: f64 = 0.618_033_988_749_895;
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let fee_f = (self.maker_round_trip_fee_bps().max(100)) as f64;
        let s_z = crate::wave_elliott::hermite_smoothstep_c1(fee_f - (mfe_bps.raw().max(0) as f64), 0.0, fee_f);
        if s_z <= 1e-6 { return; }
        let opp_d = 1 - Self::dir_idx(closed_side);
        let opp_pr = &mut self.taker_priors[slot_idx][opp_d];
        let p_opp = (opp_pr.posterior_permille().clamp(20, 450) as f64) * 1e-3;
        let sym_a = (1_000.0 * INV_PHI_S * (1.0 - p_opp) * s_z).round() as i64;
        let sym_g = (960 - (146.0 * (1.0 - p_opp) * s_z).floor() as i64).clamp(814, 960);
        opp_pr.obs_alpha_x1000 = ((opp_pr.obs_alpha_x1000.saturating_mul(960)) / 1_000).saturating_add(sym_a);
        opp_pr.obs_beta_x1000 = (opp_pr.obs_beta_x1000.saturating_mul(sym_g)) / 1_000;
    }
}

/// Self-braking centrifugal operational health index `psi_health` in `[0.0, 1.0]` (`O(1)`, `C1` smooth) (V5.4.2 M4).
#[inline]
#[must_use]
pub fn compute_psi_health_c1(
    portfolio_taker_prob: f64,
    portfolio_ewma_net_bps: f64,
    fee_rt_mkr_bps: f64,
    daily_loss_used_ratio: f64,
    epoch_loss_5u_ratio: f64,
) -> f64 {
    const PHI_H: f64 = 1.618_033_988_749_895;
    const INV_PHI_H: f64 = 0.618_033_988_749_895;
    const INV_PHI_SQ_H: f64 = 0.381_966_011_250_105;

    let f_tox = 1.0 - crate::wave_elliott::hermite_smoothstep_c1(portfolio_taker_prob, 0.08, 0.22);
    let eff_fee_bps = fee_rt_mkr_bps.abs().max(1.0);
    let f_ewma = crate::wave_elliott::hermite_smoothstep_c1(
        portfolio_ewma_net_bps,
        -PHI_H * eff_fee_bps,
        0.0,
    );
    let f_daily = 1.0
        - crate::wave_elliott::hermite_smoothstep_c1(
            daily_loss_used_ratio,
            INV_PHI_SQ_H,
            INV_PHI_H,
        );
    let f_epoch = 1.0
        - crate::wave_elliott::hermite_smoothstep_c1(
            epoch_loss_5u_ratio,
            INV_PHI_SQ_H,
            INV_PHI_H,
        );
    (f_tox * f_ewma * f_daily * f_epoch).clamp(0.0, 1.0)
}

/// Smooth `C1` pre-limit lot damper `1.0 - INV_PHI_SQ * S_3(epoch_loss_5u_ratio; INV_PHI, 1.0)` (V5.4.2 M5).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_epoch_5u_loss_lot_damper_c1(epoch_loss_1e8: i64) -> f64 {
    const INV_PHI_D: f64 = 0.618_033_988_749_895;
    const INV_PHI_SQ_D: f64 = 0.381_966_011_250_105;
    let ratio = (epoch_loss_1e8.max(0) as f64)
        / (crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 as f64);
    (1.0 - INV_PHI_SQ_D * crate::wave_elliott::hermite_smoothstep_c1(ratio, INV_PHI_D, 1.0))
        .clamp(INV_PHI_D, 1.0)
}

/// Continuous `C1` Momentum Chain Re-Entry directional cooldown compressor (V5.5 O5).
/// Compresses post-win cooldown by `1.0 - S_3(aligned_propulsion; INV_PHI_SQ, INV_PHI)` down to
/// settlement floor `round(PHI^2 * ws_rtt_ns)` (`~185 ms` at `70 ms` RTT) strictly on clean
/// offensive Maker wins (`is_maker_exit && net >= round(INV_PHI_SQ * active_fee_rt_mkr_sbps)`),
/// `!climax_active`, and `psi_health > INV_PHI_SQ`. Preserves 100% of loss, scratch, and `F6` cooldowns.
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_momentum_chain_win_cooldown_ns_c1(
    base_win_cd_ns: i64,
    is_maker_exit: bool,
    net_scaled_bps: ScaledBps,
    active_fee_rt_mkr_sbps: i64,
    climax_active: bool,
    psi_health: f64,
    aligned_propulsion: f64,
    ws_rtt_us: u32,
) -> i64 {
    const PHI_SQ_M: f64 = 2.618_033_988_749_895;
    const INV_PHI_M: f64 = 0.618_033_988_749_895;
    const INV_PHI_SQ_M: f64 = 0.381_966_011_250_105;

    let base_pos = base_win_cd_ns.max(0);
    let min_clean_win_sbps =
        (INV_PHI_SQ_M * (active_fee_rt_mkr_sbps.max(100) as f64)).round() as i64;
    if !is_maker_exit
        || net_scaled_bps.raw() < min_clean_win_sbps
        || climax_active
        || psi_health <= INV_PHI_SQ_M
    {
        return base_pos;
    }
    let rtt_us_eff = if ws_rtt_us > 0 { ws_rtt_us } else { 70_000 };
    let ws_rtt_ns = i64::from(rtt_us_eff).saturating_mul(1_000);
    let settle_floor_ns = ((PHI_SQ_M * (ws_rtt_ns as f64)).round() as i64).max(1_000_000);
    if base_pos <= settle_floor_ns {
        return base_pos;
    }
    let s_prop = crate::wave_elliott::hermite_smoothstep_c1(
        aligned_propulsion,
        INV_PHI_SQ_M,
        INV_PHI_M,
    );
    let compressed = ((base_pos as f64) * (1.0 - s_prop)).round() as i64;
    compressed.clamp(settle_floor_ns, base_pos)
}
