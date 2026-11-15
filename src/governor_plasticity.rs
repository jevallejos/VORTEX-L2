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
