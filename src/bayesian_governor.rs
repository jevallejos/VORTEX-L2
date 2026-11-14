//! 12x2 Online Bayesian Governor, Welford Var, UTC/Funding Cadence & Risk-Parity Sizer.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::too_many_arguments
)]

use crate::alpha_3factor::OrthogonalFactorState;
use crate::kinematic_surface::{
    audit_unified_63_losses_and_2160_manifold, c_clamp_i64, c_max_i64, c_min_i64, c_min_u32,
    evaluate_kinematic_surface_core, evaluate_preflight_hysteresis_core,
    extract_matrix_i64_12x2, extract_matrix_u32_12x2,
    parse_historical_trade_scar_tuple as parse_scar_tuple_core, ScarEventKey,
    ScarEventPayload,
};
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{
    evaluate_utc_and_funding_cadence, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TemporalCadenceModulation, TimestampNs, APPROVED_SPECS,
};

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const INIT_RETURN_VAR_BPS2: i64 = 40_000;
const MIN_VALID_UTC_EPOCH_NS: i64 = 1_577_836_800_000_000_000;

/// 12x2 Online Bayesian Governor with Lyapunov Crest Shield, Welford Var & UTC Cadence.
#[derive(Clone, Debug)]
pub struct OnlineAdaptiveGovernor {
    ewma_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    ewma_var_bps2: [[i64; 2]; MAX_ACTIVE_PAIRS],
    eq_vol_5m_bps: [i64; MAX_ACTIVE_PAIRS],
    cum_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    sl_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    win_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    directional_cooldown_until_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    global_dir_debt_bps_x100: [i64; 2],
    global_dir_sl_streak: [u32; 2],
    global_dir_win_streak: [u32; 2],
    global_dir_cooldown_until_ns: [i64; 2],
    last_global_win_dir: u8,
    ewma_fast_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    ewma_slow_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    last_price_sample_ts_ns: [i64; MAX_ACTIVE_PAIRS],
    wave_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    wave_anchor_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    abort_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_last_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
}

impl Default for OnlineAdaptiveGovernor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        let init_eq_vol = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4;
        Self {
            ewma_net_bps_x100: [[ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw(); 2]; MAX_ACTIVE_PAIRS],
            ewma_var_bps2: [[INIT_RETURN_VAR_BPS2; 2]; MAX_ACTIVE_PAIRS],
            eq_vol_5m_bps: [init_eq_vol; MAX_ACTIVE_PAIRS],
            cum_net_bps_x100: [[0; 2]; MAX_ACTIVE_PAIRS],
            sl_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            win_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            directional_cooldown_until_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            global_dir_debt_bps_x100: [0; 2],
            global_dir_sl_streak: [0; 2],
            global_dir_win_streak: [0; 2],
            global_dir_cooldown_until_ns: [0; 2],
            last_global_win_dir: 255,
            ewma_fast_px_1e4: [0; MAX_ACTIVE_PAIRS],
            ewma_slow_px_1e4: [0; MAX_ACTIVE_PAIRS],
            last_price_sample_ts_ns: [0; MAX_ACTIVE_PAIRS],
            wave_anchor_px_1e4: [[0; 2]; MAX_ACTIVE_PAIRS],
            wave_anchor_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_anchor_px_1e4: [[i64::MIN; 2]; MAX_ACTIVE_PAIRS],
            abort_last_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    #[must_use]
    pub const fn dir_idx(side: Side) -> usize {
        match side {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }

    #[inline]
    #[must_use]
    pub const fn slot_return_std_bps(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 200;
        }
        let d = Self::dir_idx(side);
        let var_u = if self.ewma_var_bps2[slot_idx][d] > 1 {
            self.ewma_var_bps2[slot_idx][d] as u64
        } else {
            1_u64
        };
        var_u.isqrt() as i64
    }

    #[inline]
    #[must_use]
    pub const fn slot_information_ratio_x100(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 0;
        }
        let d = Self::dir_idx(side);
        let mu = self.ewma_net_bps_x100[slot_idx][d];
        let sigma = self.slot_return_std_bps(slot_idx, side);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        (mu.saturating_mul(100)) / c_max_i64(sigma.saturating_add(fee_rt), 1)
    }

    /// Evaluates continuous UTC & 8h funding cadence when ts_local_ns is a real epoch (>= 2020).
    #[inline]
    #[must_use]
    pub fn slot_temporal_cadence(
        &self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
    ) -> TemporalCadenceModulation {
        if snap.ts_local_ns.0 < MIN_VALID_UTC_EPOCH_NS {
            return TemporalCadenceModulation::neutral();
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6)
        } else {
            fee_rt * 12
        };
        evaluate_utc_and_funding_cadence(
            snap.ts_local_ns,
            snap.realized_vol_5m,
            ScaledBps(eq_v),
        )
    }

    /// SSOT Risk-Parity + Bayesian Kelly + UTC Cadence lot calculator shared 1:1 across Exec & Replay.
    #[inline]
    #[must_use]
    pub fn compute_slot_entry_lots(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
    ) -> LotQty {
        let s_clamped = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_clamped];
        let has_bbo = snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0;
        let entry_px = if has_bbo {
            side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price)
        } else {
            snap.mark_price
        };
        let cadence = self.slot_temporal_cadence(s_clamped, snap);
        let (edge_bps, _, _) = self.slot_direction_diagnostics(s_clamped, side);
        let ir_x100 = self.slot_information_ratio_x100(s_clamped, side);
        let in_debt = self.is_in_scar_debt(s_clamped, side);
        spec.compute_risk_parity_kelly_lots(
            wallet_usdt_1e8,
            entry_px,
            snap.realized_vol_5m,
            edge_bps,
            ir_x100,
            cadence.kelly_lot_mult_permille,
            in_debt,
        )
    }

    #[inline]
    #[must_use]
    pub const fn intrinsic_diffusion_clock_ns(&self, slot_idx: usize) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let ref_v = min_v * 4;
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS && self.eq_vol_5m_bps[slot_idx] > 0 {
            c_max_i64(self.eq_vol_5m_bps[slot_idx], min_v)
        } else {
            ref_v
        };
        let raw_ns = ((VOL_5M_HORIZON_NS as i128) * (ref_v as i128)) / (eq_v as i128);
        let lo = VOL_5M_HORIZON_NS / 3;
        let hi = VOL_5M_HORIZON_NS * 3;
        if raw_ns < (lo as i128) {
            lo
        } else if raw_ns > (hi as i128) {
            hi
        } else {
            raw_ns as i64
        }
    }

    #[inline]
    #[must_use]
    pub const fn learned_tranche1_fraction_permille(
        &self,
        slot_idx: usize,
        side: Side,
        learned_mfe_bps: i64,
    ) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 500;
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let ir_x100 = c_clamp_i64(self.slot_information_ratio_x100(slot_idx, side), -60, 120);
        let mfe_c = c_clamp_i64(learned_mfe_bps, fee_rt, fee_rt * 4);
        let ir_shift = (ir_x100 - 40) * 2;
        let mfe_shift = ((mfe_c - 2 * fee_rt) * 125) / c_max_i64(fee_rt, 1);
        let scar_add = if self.is_in_scar_debt(slot_idx, side) { 120 } else { 0 };
        c_clamp_i64(500 - ir_shift - mfe_shift + scar_add, 350, 650)
    }

    #[inline]
    #[must_use]
    pub fn elastic_mae_trigger_and_def_horizon_s(
        &self,
        slot_idx: usize,
        side: Side,
        avg_price_ticks: PriceTicks,
        realized_vol_5m: ScaledBps,
        learned_mfe_bps: i64,
    ) -> (i64, i64) {
        let s_clamped = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_clamped];
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let tau_bps = spec
            .tick_to_price_scaled_bps(avg_price_ticks)
            .unwrap_or(ScaledBps(fee_rt / 8))
            .raw();
        let sl_dist_bps = PairSpec::elastic_server_sl_distance_bps(realized_vol_5m).raw();
        let ir_x100 = self.slot_information_ratio_x100(s_clamped, side).clamp(-60, 120);
        let ret_std = self.slot_return_std_bps(s_clamped, side);
        let mfe_c = learned_mfe_bps.clamp((fee_rt * 7) / 5, fee_rt * 4);
        let gamma_permille =
            (220 + (ir_x100 * 5) / 6 + ((mfe_c - 2 * fee_rt) * 100) / (10 * fee_rt).max(1))
                .clamp(180, 340);
        let sl_cushion_bps = (sl_dist_bps.saturating_mul(gamma_permille)) / 1_000;
        let mae_mag = (4 * tau_bps).max(sl_cushion_bps).max(ret_std / 6);

        let tau_mkt_s = (self.intrinsic_diffusion_clock_ns(s_clamped) / 1_000_000_000).max(90);
        let init_mfe = (fee_rt * 17) / 10;
        let ir_mult_permille = 1_000 + (ir_x100.clamp(-40, 80) * 10) / 3;
        let raw_def_s =
            ((((tau_mkt_s / 4) * mfe_c) / init_mfe.max(1)) * ir_mult_permille) / 1_000;
        let t_def_s = raw_def_s.clamp(tau_mkt_s / 6, (tau_mkt_s * 2) / 5);
        (-mae_mag, t_def_s)
    }

    #[inline]
    #[must_use]
    const fn elastic_wave_anchor_ttl_ns(&self, slot_idx: usize, d: usize) -> i64 {
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let edge_pos = if self.ewma_net_bps_x100[slot_idx][d] > 0 {
            c_min_i64(self.ewma_net_bps_x100[slot_idx][d], fee_rt * 2)
        } else {
            0
        };
        (tau_ns.saturating_mul(2 * fee_rt + edge_pos)) / fee_rt
    }

    #[inline]
    pub fn observe_market_price(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let mid_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        if mid_ticks <= 0 {
            return;
        }
        let px_1e4 = mid_ticks.saturating_mul(10_000);
        let b3x_floor = APPROVED_SPECS[slot_idx]
            .required_3x_barrier_bps(PriceTicks(mid_ticks))
            .unwrap_or(ScaledBps::MIN_REQUIRED_VOL_5M_BPS)
            .raw();
        let active_regime_floor = b3x_floor.saturating_mul(2);
        let obs_vol = snap.realized_vol_5m.raw();
        let fast_prev = self.ewma_fast_px_1e4[slot_idx];
        let slow_prev = self.ewma_slow_px_1e4[slot_idx];
        if fast_prev <= 0 || slow_prev <= 0 {
            self.ewma_fast_px_1e4[slot_idx] = px_1e4;
            self.ewma_slow_px_1e4[slot_idx] = px_1e4;
            if obs_vol > 0 {
                self.eq_vol_5m_bps[slot_idx] = obs_vol.max(active_regime_floor);
            }
            self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
            return;
        }
        let sample_dt = snap.ts_local_ns.0.saturating_sub(self.last_price_sample_ts_ns[slot_idx]);
        if sample_dt < 1_000_000_000 {
            return;
        }
        self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
        let dt_s = ((sample_dt as f64) * 1e-9).clamp(1.0, 30.0);
        let a_fast = (1.0 - (-dt_s / 20.0).exp()).clamp(0.02, 1.0);
        let a_slow = (1.0 - (-dt_s / 300.0).exp()).clamp(0.002, 1.0);
        self.ewma_fast_px_1e4[slot_idx] =
            (fast_prev as f64 + a_fast * ((px_1e4 - fast_prev) as f64)).round() as i64;
        self.ewma_slow_px_1e4[slot_idx] =
            (slow_prev as f64 + a_slow * ((px_1e4 - slow_prev) as f64)).round() as i64;
        if obs_vol > 0 {
            let prev_eq = self.eq_vol_5m_bps[slot_idx].max(active_regime_floor) as f64;
            let obs_f = (obs_vol.max(active_regime_floor)) as f64;
            let tau_eq = if obs_f > prev_eq { 120.0 } else { 900.0 };
            let a_eq = (1.0 - (-dt_s / tau_eq).exp()).clamp(0.001, 1.0);
            let next_eq = (prev_eq + a_eq * (obs_f - prev_eq)).round() as i64;
            self.eq_vol_5m_bps[slot_idx] = next_eq.clamp(active_regime_floor, 24_000);
        }
    }

    #[inline]
    #[must_use]
    pub const fn price_drift_and_extension_scaled_bps(
        &self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
    ) -> (i64, i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0);
        }
        let fast = self.ewma_fast_px_1e4[slot_idx];
        let slow = self.ewma_slow_px_1e4[slot_idx];
        if fast <= 0 || slow <= 0 {
            return (0, 0);
        }
        let drift_bps = ((fast - slow).saturating_mul(1_000_000)) / slow;
        let cur_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        let cur_1e4 = cur_ticks.saturating_mul(10_000);
        let ext_bps = if cur_1e4 > 0 {
            ((cur_1e4 - slow).saturating_mul(1_000_000)) / slow
        } else {
            0
        };
        (drift_bps, ext_bps)
    }

    #[inline]
    #[must_use]
    pub const fn slot_direction_diagnostics(&self, slot_idx: usize, side: Side) -> (i64, u32, u32) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0, 0);
        }
        let d = Self::dir_idx(side);
        (
            self.ewma_net_bps_x100[slot_idx][d],
            self.sl_streak[slot_idx][d],
            self.win_streak[slot_idx][d],
        )
    }

    #[inline]
    #[must_use]
    pub const fn is_in_local_slot_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        let local_floor = -(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 8);
        self.cum_net_bps_x100[slot_idx][d] <= local_floor || self.sl_streak[slot_idx][d] > 0
    }

    #[inline]
    #[must_use]
    pub const fn is_in_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        let global_floor = -(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 30);
        self.is_in_local_slot_scar_debt(slot_idx, side)
            || self.global_dir_debt_bps_x100[d] < global_floor
    }

    #[inline]
    #[must_use]
    pub const fn audit_historical_entry_veto(
        &self,
        slot_idx: usize,
        side: Side,
        entry_ticks: i64,
        check_ts_ns: i64,
    ) -> Option<&'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Some("VETO_INVALID_SLOT");
        }
        let d = Self::dir_idx(side);
        if check_ts_ns > 0 && check_ts_ns < self.directional_cooldown_until_ns[slot_idx][d] {
            return Some("VETO_SLOT_COOLDOWN");
        }
        if check_ts_ns > 0 && check_ts_ns < self.global_dir_cooldown_until_ns[d] {
            return Some("VETO_GLOBAL_WAVE_OR_SL_BREATHER");
        }
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let anchor_1e4 = self.wave_anchor_px_1e4[slot_idx][d];
        let anchor_ts = self.wave_anchor_ts_ns[slot_idx][d];
        let wave_fresh = check_ts_ns <= 0
            || anchor_ts <= 0
            || check_ts_ns.saturating_sub(anchor_ts)
                <= self.elastic_wave_anchor_ttl_ns(slot_idx, d);
        let cur_1e4 = entry_ticks.saturating_mul(10_000);
        let wave_active = self.win_streak[slot_idx][d] >= 3
            || (self.win_streak[slot_idx][d] >= 1 && self.global_dir_win_streak[d] >= 3);
        if wave_active && wave_fresh && anchor_1e4 > 0 && cur_1e4 > 0 {
            let chased_bps = match side {
                Side::Bid => ((cur_1e4 - anchor_1e4).saturating_mul(1_000_000)) / anchor_1e4,
                Side::Ask => ((anchor_1e4 - cur_1e4).saturating_mul(1_000_000)) / anchor_1e4,
            };
            let edge_adj = c_clamp_i64(self.ewma_net_bps_x100[slot_idx][d], -fee_rt, fee_rt) / 4;
            let stair_min = fee_rt * 6 + edge_adj;
            let stair_max = self.eq_vol_5m_bps[slot_idx].saturating_mul(2) + fee_rt * 8;
            if chased_bps > stair_min && chased_bps < stair_max {
                return Some("VETO_WAVE_STAIRCASE_25BPS");
            }
        }
        let abort_breather_ns = tau_ns / 20;
        if self.abort_streak[slot_idx][d] >= 4
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && check_ts_ns > 0
            && check_ts_ns.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < abort_breather_ns
        {
            return Some("VETO_ABORT_STREAK_BREATHER");
        }
        let deep_debt_floor = -(fee_rt * 11);
        if self.sl_streak[slot_idx][d] >= 1
            && self.cum_net_bps_x100[slot_idx][d] <= deep_debt_floor
        {
            let slot_cd = self.directional_cooldown_until_ns[slot_idx][d];
            let debt_mult = c_clamp_i64((-self.cum_net_bps_x100[slot_idx][d]) / fee_rt, 2, 16);
            let sl_m = c_min_u32(self.sl_streak[slot_idx][d], 4) as i64;
            let deep_q_ns = (tau_ns.saturating_mul(debt_mult).saturating_mul(sl_m)) / 2;
            if check_ts_ns <= 0 || slot_cd <= 0 || check_ts_ns < slot_cd.saturating_add(deep_q_ns)
            {
                return Some("VETO_DEEP_SCAR_DEBT_QUARANTINE");
            }
        }
        if slot_idx > 0 {
            let glob_cd = self.global_dir_cooldown_until_ns[d];
            if self.global_dir_sl_streak[d] >= 1 && self.last_global_win_dir == ((1 - d) as u8) {
                let g_sl_m = c_min_u32(self.global_dir_sl_streak[d], 4) as i64;
                let dom_ns = tau_ns.saturating_mul(g_sl_m) * 2;
                if check_ts_ns <= 0 || glob_cd <= 0 || check_ts_ns < glob_cd.saturating_add(dom_ns)
                {
                    return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
                }
            }
            let symbol_total_net =
                self.cum_net_bps_x100[slot_idx][0] + self.cum_net_bps_x100[slot_idx][1];
            if symbol_total_net <= -(fee_rt * 6)
                && self.global_dir_debt_bps_x100[d] <= -(fee_rt * 25)
                && self.cum_net_bps_x100[slot_idx][d] <= 0
            {
                let glob_mult =
                    c_clamp_i64((-self.global_dir_debt_bps_x100[d]) / (fee_rt * 4), 4, 16);
                let contagion_ns = tau_ns.saturating_mul(glob_mult);
                if check_ts_ns <= 0
                    || glob_cd <= 0
                    || check_ts_ns < glob_cd.saturating_add(contagion_ns)
                {
                    return Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION");
                }
            }
        }
        None
    }

    #[inline]
    pub fn evaluate_kinematic_energy_surface(
        &self,
        slot_idx: usize,
        side: Side,
        drift_bps: i64,
        ext_bps: i64,
        signed_geom: f64,
        snr_abs: f64,
    ) -> Result<(i64, i64), &'static str> {
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let eq_vol = if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6)
        } else {
            fee_rt * 12
        };
        let d = Self::dir_idx(side);
        let (learned_edge, ir_x100) = if slot_idx < MAX_ACTIVE_PAIRS {
            (
                self.ewma_net_bps_x100[slot_idx][d],
                self.slot_information_ratio_x100(slot_idx, side),
            )
        } else {
            (fee_rt / 2, 33)
        };
        evaluate_kinematic_surface_core(
            side, eq_vol, learned_edge, ir_x100, drift_bps, ext_bps, signed_geom, snr_abs,
        )
    }

    pub fn observe_closed_trade_at_entry(
        &mut self,
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        net_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let prev_mean = self.ewma_net_bps_x100[slot_idx][d];
        let diff = net_scaled_bps.raw().saturating_sub(prev_mean);
        let sq_clamped = diff.saturating_mul(diff).min(25_000_000);
        self.ewma_var_bps2[slot_idx][d] =
            ((self.ewma_var_bps2[slot_idx][d] * 80 + sq_clamped * 20) / 100).max(2_500);
        self.ewma_net_bps_x100[slot_idx][d] = (prev_mean * 75 + net_scaled_bps.raw() * 25) / 100;
        self.cum_net_bps_x100[slot_idx][d] = self.cum_net_bps_x100[slot_idx][d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-(fee_rt * 60), fee_rt * 60);
        self.global_dir_debt_bps_x100[d] = self.global_dir_debt_bps_x100[d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-(fee_rt * 75), fee_rt * 30);

        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let ir_x100 = self.slot_information_ratio_x100(slot_idx, side).clamp(-50, 100);

        if !is_maker_exit || net_scaled_bps.raw() < -(fee_rt / 8) {
            let s = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            self.sl_streak[slot_idx][d] = s;
            self.win_streak[slot_idx][d] = 0;
            self.global_dir_win_streak[d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            self.wave_anchor_ts_ns[slot_idx][d] = 0;
            if ts_ns.0 > 0 {
                let backoff_ns = (tau_ns / 2).saturating_mul(1_i64 << s.min(4));
                self.directional_cooldown_until_ns[slot_idx][d] =
                    ts_ns.0.saturating_add(backoff_ns);
            }
            let gs = self.global_dir_sl_streak[d].saturating_add(1).min(4);
            self.global_dir_sl_streak[d] = gs;
            if ts_ns.0 > 0 {
                let global_pause_ns = ((tau_ns * 3) / 5).saturating_mul(i64::from(gs));
                self.global_dir_cooldown_until_ns[d] = ts_ns.0.saturating_add(global_pause_ns);
            }
        } else if net_scaled_bps.raw() < fee_rt / 4 {
            if ts_ns.0 > 0 {
                let scratch_ns = (tau_ns * (120 - ir_x100 / 2)) / 360;
                let scratch_cd = ts_ns.0.saturating_add(scratch_ns);
                if scratch_cd > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = scratch_cd;
                }
            }
        } else {
            self.clear_abort_streak(slot_idx, side);
            let prev_anchor_ts = self.wave_anchor_ts_ns[slot_idx][d];
            let anchor_expired = ts_ns.0 > 0
                && prev_anchor_ts > 0
                && ts_ns.0.saturating_sub(prev_anchor_ts)
                    > self.elastic_wave_anchor_ttl_ns(slot_idx, d);
            if entry_price.0 > 0
                && (self.wave_anchor_px_1e4[slot_idx][d] <= 0 || anchor_expired)
            {
                self.wave_anchor_px_1e4[slot_idx][d] = entry_price.0.saturating_mul(10_000);
            }
            if ts_ns.0 > 0 {
                self.wave_anchor_ts_ns[slot_idx][d] = ts_ns.0;
            }
            let w = if anchor_expired {
                1_u32
            } else {
                self.win_streak[slot_idx][d].saturating_add(1)
            };
            self.win_streak[slot_idx][d] = w;
            self.win_streak[slot_idx][1 - d] = 0;
            self.wave_anchor_px_1e4[slot_idx][1 - d] = 0;
            self.wave_anchor_ts_ns[slot_idx][1 - d] = 0;
            let gw = self.global_dir_win_streak[d].saturating_add(1);
            self.global_dir_win_streak[d] = gw;
            self.global_dir_win_streak[1 - d] = 0;
            self.last_global_win_dir = d as u8;
            if gw >= 6 {
                self.global_dir_win_streak[d] = 0;
            }
            if ts_ns.0 > 0 {
                let win_ns = ((tau_ns * (100 - ir_x100 / 3)) / 1_500) * (w.min(2) as i64);
                let win_cd = ts_ns.0.saturating_add(win_ns);
                if win_cd > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = win_cd;
                }
            }
            if self.sl_streak[slot_idx][d] > 0
                && w >= 3
                && self.cum_net_bps_x100[slot_idx][d] >= -(fee_rt * 8)
            {
                self.sl_streak[slot_idx][d] -= 1;
                self.win_streak[slot_idx][d] = 0;
            }
            if self.global_dir_sl_streak[d] > 0
                && self.global_dir_debt_bps_x100[d] >= -(fee_rt * 15)
            {
                self.global_dir_sl_streak[d] -= 1;
            }
        }
    }

    #[inline]
    pub fn clear_abort_streak(&mut self, slot_idx: usize, side: Side) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        self.abort_streak[slot_idx][d] = 0;
        self.abort_anchor_px_1e4[slot_idx][d] = i64::MIN;
        self.abort_last_ts_ns[slot_idx][d] = 0;
    }

    #[inline]
    pub fn observe_entry_abort(
        &mut self,
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let decay_win_ns = self.intrinsic_diffusion_clock_ns(slot_idx) / 5;
        let prev_ts = self.abort_last_ts_ns[slot_idx][d];
        if prev_ts > 0 && ts_ns.0 > prev_ts && ts_ns.0.saturating_sub(prev_ts) >= decay_win_ns {
            self.abort_streak[slot_idx][d] = 0;
        }
        self.abort_streak[slot_idx][d] = self.abort_streak[slot_idx][d].saturating_add(1).min(8);
        self.abort_anchor_px_1e4[slot_idx][d] = aborted_price.0.saturating_mul(10_000);
        self.abort_last_ts_ns[slot_idx][d] = ts_ns.0;
    }

    pub fn audit_scar64_entry_and_sweep_shield(
        &self,
        slot_idx: usize,
        side: Side,
        snap: &SymbolLobSnapshot,
        btc_signed_snr: f64,
    ) -> Result<(), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Ok(());
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Ok(());
        };
        let d = Self::dir_idx(side);
        let d_sign = side.sign_i64();
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if self.abort_streak[slot_idx][d] >= 1
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && snap.ts_local_ns.0 > 0
        {
            let vol_raw = snap.realized_vol_5m.raw().clamp(1_200, 15_000);
            let sweep_ttl_ns =
                ((25_000_000_000_i64 * 6_000) / vol_raw).clamp(18_000_000_000, 45_000_000_000);
            let since_abort_ns = snap.ts_local_ns.0.saturating_sub(self.abort_last_ts_ns[slot_idx][d]);
            let cur_1e4 = entry_px.0.saturating_mul(10_000);
            let move_1e4 = (cur_1e4 - self.abort_anchor_px_1e4[slot_idx][d]).saturating_mul(d_sign);
            if since_abort_ns < sweep_ttl_ns && move_1e4 < 0 {
                return Err("VETO_POST_ABORT_ADVERSE_SWEEP");
            }
        }
        if slot_idx > 0 {
            let glob_debt = (-self.global_dir_debt_bps_x100[d]).max(0) as f64;
            let opp_macro = (-snap.wave_macro_phase.get() * side.sign_f64()).max(0.0);
            if glob_debt >= 8_000.0 || self.global_dir_sl_streak[d] >= 1 || opp_macro > 0.05 {
                let min_btc_snr = (-0.04
                    + 0.04 * (glob_debt / 10_000.0).tanh()
                    + 0.85 * (opp_macro - 0.05).max(0.0))
                    .clamp(-0.04, 0.28);
                if btc_signed_snr < min_btc_snr {
                    return Err("VETO_SCAR64_BTC_DEBT_HEADWIND");
                }
            }
            let fast_1e4 = self.ewma_fast_px_1e4[slot_idx];
            let slow_1e4 = self.ewma_slow_px_1e4[slot_idx];
            if fast_1e4 > 0 && slow_1e4 > 0 {
                let mid_t = i64::midpoint(best_bid.price.0, best_ask.price.0);
                let px_1e4 = mid_t.saturating_mul(10_000);
                let s_drift =
                    (((fast_1e4 - slow_1e4).saturating_mul(1_000_000)) / slow_1e4).saturating_mul(d_sign);
                let s_ext =
                    (((px_1e4 - slow_1e4).saturating_mul(1_000_000)) / slow_1e4).saturating_mul(d_sign);
                let lead_impulse = s_ext.saturating_sub(s_drift);
                let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
                let sym_sl_tot =
                    self.sl_streak[slot_idx][0].saturating_add(self.sl_streak[slot_idx][1]);
                if sym_sl_tot >= 1 && (signed_ofi < 0.34 || lead_impulse < 0) {
                    return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE");
                }
                let low_vol_decel = snap.realized_vol_5m.raw() < 2_500
                    || (snap.realized_vol_5m.raw() < 3_000 && signed_ofi < 0.35);
                if s_drift > 0
                    && lead_impulse < -15
                    && (low_vol_decel || btc_signed_snr < 0.08)
                {
                    return Err("VETO_FOLLOWER_DECEL_UNDER_COMPRESSION");
                }
            } else {
                let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
                let sym_sl_tot =
                    self.sl_streak[slot_idx][0].saturating_add(self.sl_streak[slot_idx][1]);
                if sym_sl_tot >= 1 && signed_ofi < 0.34 {
                    return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE");
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn to_scar_snapshot_json(&self, ts_ns: TimestampNs) -> serde_json::Value {
        serde_json::json!({
            "event": "GOV_SCAR_SNAPSHOT",
            "ts_ns": ts_ns.0,
            "ewma_net_bps_x100": self.ewma_net_bps_x100,
            "ewma_var_bps2": self.ewma_var_bps2,
            "eq_vol_5m_bps": self.eq_vol_5m_bps,
            "cum_net_bps_x100": self.cum_net_bps_x100,
            "sl_streak": self.sl_streak,
            "win_streak": self.win_streak,
            "directional_cooldown_until_ns": self.directional_cooldown_until_ns,
            "wave_anchor_px_1e4": self.wave_anchor_px_1e4,
            "wave_anchor_ts_ns": self.wave_anchor_ts_ns,
            "global_dir_debt_bps_x100": self.global_dir_debt_bps_x100,
            "global_dir_sl_streak": self.global_dir_sl_streak,
            "global_dir_win_streak": self.global_dir_win_streak,
            "global_dir_cooldown_until_ns": self.global_dir_cooldown_until_ns,
            "last_global_win_dir": self.last_global_win_dir,
        })
    }

    pub fn apply_scar_snapshot_json(&mut self, val: &serde_json::Value) -> Option<TimestampNs> {
        if val.get("event").and_then(|v| v.as_str()) != Some("GOV_SCAR_SNAPSHOT") {
            return None;
        }
        let snap_ts = val.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
        extract_matrix_i64_12x2(val, "ewma_net_bps_x100", &mut self.ewma_net_bps_x100);
        self.ewma_var_bps2 = [[INIT_RETURN_VAR_BPS2; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "ewma_var_bps2", &mut self.ewma_var_bps2);
        let min_regime_floor = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 2;
        if let Some(eq_arr) = val.get("eq_vol_5m_bps").and_then(|v| v.as_array()) {
            for (s, cell) in eq_arr.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = cell.as_i64() {
                    if n > 0 {
                        self.eq_vol_5m_bps[s] = n.max(min_regime_floor);
                    }
                }
            }
        }
        extract_matrix_i64_12x2(val, "cum_net_bps_x100", &mut self.cum_net_bps_x100);
        extract_matrix_u32_12x2(val, "sl_streak", &mut self.sl_streak);
        extract_matrix_u32_12x2(val, "win_streak", &mut self.win_streak);
        extract_matrix_i64_12x2(
            val,
            "directional_cooldown_until_ns",
            &mut self.directional_cooldown_until_ns,
        );
        extract_matrix_i64_12x2(val, "wave_anchor_px_1e4", &mut self.wave_anchor_px_1e4);
        self.wave_anchor_ts_ns = [[0; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "wave_anchor_ts_ns", &mut self.wave_anchor_ts_ns);
        for s in 0..MAX_ACTIVE_PAIRS {
            for d in 0..2 {
                if self.wave_anchor_px_1e4[s][d] > 0 && self.wave_anchor_ts_ns[s][d] <= 0 {
                    let fallback_ts = if self.directional_cooldown_until_ns[s][d] > 0 {
                        self.directional_cooldown_until_ns[s][d]
                    } else {
                        snap_ts
                    };
                    self.wave_anchor_ts_ns[s][d] = fallback_ts;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_debt_bps_x100").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_debt_bps_x100[d] = n;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_sl_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_sl_streak[d] = n.min(4) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_win_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_win_streak[d] = n.min(6) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_cooldown_until_ns").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_cooldown_until_ns[d] = n;
                }
            }
        }
        if let Some(ld) = val.get("last_global_win_dir").and_then(|v| v.as_u64()) {
            self.last_global_win_dir = ld.min(255) as u8;
        }
        Some(TimestampNs(snap_ts))
    }

    #[must_use]
    pub fn authorizes_adaptive_entry_with_wallet(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let has_bbo = candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0;
        let entry_px = if has_bbo {
            side.select_entry_bbo_price(candidate_snap.bids[0].price, candidate_snap.asks[0].price)
        } else {
            candidate_snap.mark_price
        };
        if entry_px.0 <= 0 {
            return false;
        }
        if self
            .audit_historical_entry_veto(slot_idx, side, entry_px.0, candidate_snap.ts_local_ns.0)
            .is_some()
        {
            return false;
        }
        let d = Self::dir_idx(side);
        let spec = APPROVED_SPECS[slot_idx];
        let (own_q, opp_q, own_deep) = match side {
            Side::Bid => (
                candidate_snap.bids[0].qty,
                candidate_snap.asks[0].qty,
                candidate_snap.deep_bid_lots_l1_l5,
            ),
            Side::Ask => (
                candidate_snap.asks[0].qty,
                candidate_snap.bids[0].qty,
                candidate_snap.deep_ask_lots_l1_l5,
            ),
        };
        if !spec.passes_l1_queue_depth_and_wall_guard(
            entry_px,
            candidate_snap.realized_vol_5m,
            self.ewma_net_bps_x100[slot_idx][d],
            own_q,
            opp_q,
            own_deep,
            candidate_snap.factor_ofi.get().abs(),
            candidate_snap.factor_tfi.get().abs(),
        ) {
            return false;
        }
        let (self_drift_bps, self_ext_bps) =
            self.price_drift_and_extension_scaled_bps(slot_idx, candidate_snap);
        let dir_s = side.sign_f64();
        let signed_geom = candidate_snap.factor_geom.get() * dir_s;
        let cand_comp = OrthogonalFactorState::snr_composite_score(candidate_snap).abs();
        let in_local_scar = self.is_in_local_slot_scar_debt(slot_idx, side);
        let in_scar = self.is_in_scar_debt(slot_idx, side);

        if !spec.passes_oversized_lot_conviction_guard_with_vol(
            wallet_usdt_1e8,
            entry_px,
            candidate_snap.realized_vol_5m,
            cand_comp,
            in_local_scar,
        ) {
            return false;
        }
        let Ok((e_kin, base_e_min)) = self.evaluate_kinematic_energy_surface(
            slot_idx,
            side,
            self_drift_bps,
            self_ext_bps,
            signed_geom,
            cand_comp,
        ) else {
            return false;
        };
        let cadence = self.slot_temporal_cadence(slot_idx, candidate_snap);
        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let eff_e_min = (base_e_min + cadence.kin_energy_min_delta_bps)
            .clamp(fee_rt / 5, (fee_rt * 4) / 5);
        if e_kin < eff_e_min {
            return false;
        }

        let sl_s = self.sl_streak[slot_idx][d] as f64;
        let win_s = (self.win_streak[slot_idx][d].min(4)) as f64;
        let debt_bps = self.cum_net_bps_x100[slot_idx][d] as f64;
        let debt_penalty = if debt_bps < 0.0 { ((-debt_bps) / 16_000.0).min(0.28) } else { 0.0 };
        let global_debt_bps = self.global_dir_debt_bps_x100[d] as f64;
        let global_penalty = if global_debt_bps < -2_000.0 {
            ((-global_debt_bps - 2_000.0) / 35_000.0).min(0.14)
        } else {
            0.0
        };
        let staircase_penalty = if win_s >= 2.0 { 0.03 * (win_s - 1.0) } else { 0.0 };
        let ir_adj =
            (self.slot_information_ratio_x100(slot_idx, side) as f64 / 2_000.0).clamp(-0.04, 0.05);
        let req_scar_tfi = if in_local_scar { 0.45 } else { 0.38 };
        if in_scar && candidate_snap.factor_tfi.get().abs() < req_scar_tfi {
            return false;
        }

        if slot_idx > 0 {
            let btc_comp = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
            let btc_ofi = btc_leader_snap.factor_ofi.get() * dir_s;
            let btc_tfi = btc_leader_snap.factor_tfi.get() * dir_s;
            let strong_independent = cand_comp >= 0.50 && !in_local_scar;
            if strong_independent {
                if btc_comp < -0.08 || (btc_ofi < -0.18 && btc_tfi < -0.18) {
                    return false;
                }
            } else {
                let min_req_btc = 0.08 + 0.08 * sl_s + 0.45 * (debt_penalty + global_penalty);
                if btc_ofi < -0.05 || btc_tfi < -0.05 || btc_comp < min_req_btc {
                    return false;
                }
            }
        }
        let btc_signed_snr = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        let Ok(wave_mod) =
            crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
                candidate_snap.wave_observables(),
                side,
                btc_signed_snr,
            )
        else {
            return false;
        };
        if self
            .audit_scar64_entry_and_sweep_shield(slot_idx, side, candidate_snap, btc_signed_snr)
            .is_err()
        {
            return false;
        }
        let adaptive_threshold = (0.42
            + 0.08 * sl_s
            + debt_penalty
            + global_penalty
            + staircase_penalty
            + wave_mod.snr_threshold_delta
            + cadence.snr_threshold_delta
            - ir_adj)
            .clamp(0.36, 0.84);
        cand_comp >= adaptive_threshold
    }

    #[inline]
    #[must_use]
    pub fn max_total_entry_chase_ns(&self, slot_idx: usize, realized_vol_5m: ScaledBps) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx].max(min_v * 2)
        } else {
            min_v * 4
        };
        let vol_c = realized_vol_5m.raw().max(min_v);
        let base_flow_ns = VOL_5M_HORIZON_NS / 60;
        let ir_best = c_max_i64(
            self.slot_information_ratio_x100(slot_idx, Side::Bid),
            self.slot_information_ratio_x100(slot_idx, Side::Ask),
        )
        .clamp(-40, 80);
        let ir_permille = 1_000 + ir_best * 2;
        let raw_ns = (((base_flow_ns as i128 * eq_v as i128) / (vol_c as i128))
            * (ir_permille as i128))
            / 1_000;
        let lo = (base_flow_ns * 16) / 25;
        let hi = (base_flow_ns * 28) / 25;
        (raw_ns as i64).clamp(lo, hi)
    }

    pub fn confirm_preflight_kinematics_non_choking(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> Result<(i64, i64), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Err("VETO_INVALID_SLOT");
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err("VETO_PREFLIGHT_INVALID_BBO");
        };
        let ofi = snap.factor_ofi.get();
        let tfi = snap.factor_tfi.get();
        if (ofi.abs() > 0.32 && tfi.abs() > 0.32 && (ofi * tfi) < -0.08)
            || (ofi.abs() > 0.55 && tfi.abs() < 0.08)
        {
            return Err("VETO_PREFLIGHT_SPOOFING");
        }
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if let Some(veto) =
            self.audit_historical_entry_veto(slot_idx, side, entry_px.0, snap.ts_local_ns.0)
        {
            return Err(veto);
        }
        let dir_s = side.sign_f64();
        if (ofi * dir_s) < 0.08 {
            return Err("VETO_PREFLIGHT_OFI_DECAY");
        }
        let signed_snr = OrthogonalFactorState::snr_composite_score(snap) * dir_s;
        if signed_snr < 0.30 {
            return Err("VETO_PREFLIGHT_SNR_COLLAPSE");
        }
        let spec = APPROVED_SPECS[slot_idx];
        let in_local_scar = self.is_in_local_slot_scar_debt(slot_idx, side);
        if !spec.passes_oversized_lot_conviction_guard_with_vol(
            wallet_usdt_1e8,
            entry_px,
            snap.realized_vol_5m,
            signed_snr,
            in_local_scar,
        ) {
            return Err("VETO_PREFLIGHT_OVERSIZED_LOT");
        }
        let signed_geom = snap.factor_geom.get() * dir_s;
        let (own_q_pre, opp_q_pre) = match side {
            Side::Bid => (best_bid.qty.0, best_ask.qty.0),
            Side::Ask => (best_ask.qty.0, best_bid.qty.0),
        };
        let geom_floor_lo = if signed_snr >= 0.68 && own_q_pre >= opp_q_pre { -0.06 } else { 0.00 };
        let min_pre_geom = (0.06 - (signed_snr - 0.42) * 0.45).clamp(geom_floor_lo, 0.06);
        if signed_geom < min_pre_geom {
            return Err("VETO_PREFLIGHT_GEOM_REVERSAL");
        }
        let btc_snr = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        if slot_idx > 0 && btc_snr < -0.08 && signed_snr < 0.62 {
            return Err("VETO_PREFLIGHT_BTC_HEADWIND");
        }
        self.audit_scar64_entry_and_sweep_shield(slot_idx, side, snap, btc_snr)?;
        let _ = crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
            snap.wave_observables(),
            side,
            btc_snr,
        )?;

        let (drift_bps, ext_bps) = self.price_drift_and_extension_scaled_bps(slot_idx, snap);
        if let Ok(ok_pair) = self.evaluate_kinematic_energy_surface(
            slot_idx, side, drift_bps, ext_bps, signed_geom, signed_snr,
        ) {
            return Ok(ok_pair);
        }

        let d = Self::dir_idx(side);
        evaluate_preflight_hysteresis_core(
            side,
            self.eq_vol_5m_bps[slot_idx],
            self.ewma_net_bps_x100[slot_idx][d],
            self.slot_information_ratio_x100(slot_idx, side),
            drift_bps,
            ext_bps,
            signed_geom,
            min_pre_geom,
            signed_snr,
        )
    }

    #[inline]
    #[must_use]
    pub fn parse_historical_trade_scar_tuple(
        val: &serde_json::Value,
        min_exclusive_ts_ns: i64,
    ) -> Option<(ScarEventKey, ScarEventPayload)> {
        parse_scar_tuple_core(val, min_exclusive_ts_ns)
    }

    pub fn rehydrate_from_scar_and_loss_files(
        &mut self,
        scar_path: &std::path::Path,
        red_loss_path: &std::path::Path,
        session_vault_path: Option<&std::path::Path>,
    ) -> (i64, usize) {
        use std::io::BufRead;
        let mut snap_ts_ns = 0_i64;
        if let Ok(s_file) = std::fs::File::open(scar_path) {
            let reader = std::io::BufReader::new(s_file);
            let mut latest_snap: Option<serde_json::Value> = None;
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    if val.get("event").and_then(|v| v.as_str()) == Some("GOV_SCAR_SNAPSHOT") {
                        latest_snap = Some(val);
                    }
                }
            }
            if let Some(ref snap_v) = latest_snap {
                if let Some(restored) = self.apply_scar_snapshot_json(snap_v) {
                    snap_ts_ns = restored.0;
                }
            }
        }
        let mut dedup_events: std::collections::BTreeMap<ScarEventKey, ScarEventPayload> =
            std::collections::BTreeMap::new();
        let mut paths: [&std::path::Path; 3] = [scar_path, red_loss_path, scar_path];
        let path_count = if let Some(vp) = session_vault_path {
            paths[2] = vp;
            3_usize
        } else {
            2_usize
        };
        for p in &paths[..path_count] {
            let Ok(file) = std::fs::File::open(p) else { continue };
            let reader = std::io::BufReader::new(file);
            for line in reader.lines().map_while(Result::ok) {
                let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
                if let Some((key, payload)) =
                    Self::parse_historical_trade_scar_tuple(&val, snap_ts_ns)
                {
                    dedup_events.entry(key).or_insert(payload);
                }
            }
        }
        let mut max_ts = snap_ts_ns;
        let ingested = dedup_events.len();
        for (&(ts_ns, slot_idx, d_u8), &(en_t, net_sc, mkr)) in &dedup_events {
            let side = if d_u8 == 1 { Side::Ask } else { Side::Bid };
            self.observe_closed_trade_at_entry(
                slot_idx,
                side,
                en_t,
                net_sc,
                mkr,
                TimestampNs(ts_ns),
            );
            if ts_ns > max_ts {
                max_ts = ts_ns;
            }
        }
        (max_ts, ingested)
    }

    #[inline]
    #[must_use]
    pub fn audit_unified_62_losses_and_2160_manifold() -> (usize, usize) {
        audit_unified_63_losses_and_2160_manifold()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kinematic_surface::UNIFIED_LOSS_LAB_COUNT;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_governor_12x2_loss_lab_staircase_abort_and_scar_json() {
        let (mitigated_all, manifold_2160) =
            OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!(mitigated_all, UNIFIED_LOSS_LAB_COUNT);
        assert_eq!(manifold_2160, 2_160);

        let mut gov = OnlineAdaptiveGovernor::new();
        let chase_ns = gov.max_total_entry_chase_ns(9, ScaledBps(2_090));
        assert!((3_200_000_000..=5_600_000_000).contains(&chase_ns));

        let t0 = 1_700_000_000_000_000_000_i64;
        for step in 0..3_i64 {
            gov.observe_closed_trade_at_entry(
                3, Side::Bid, PriceTicks(8_200 + step * 5), ScaledBps(220),
                true, TimestampNs(t0 + step * 120_000_000_000),
            );
        }
        gov.observe_closed_trade_at_entry(
            3, Side::Ask, PriceTicks(8_212), ScaledBps(45),
            true, TimestampNs(t0 + 400_000_000_000),
        );
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_225, t0 + 600_000_000_000),
            Some("VETO_WAVE_STAIRCASE_25BPS")
        );
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_225, t0 + 1_600_000_000_000),
            None
        );

        for a in 0..4_i64 {
            gov.observe_entry_abort(
                3, Side::Bid, PriceTicks(8_205),
                TimestampNs(t0 + 700_000_000_000 + a * 1_000_000_000),
            );
        }
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            Some("VETO_ABORT_STREAK_BREATHER")
        );
        gov.clear_abort_streak(3, Side::Bid);
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            None
        );
    }

    #[test]
    fn test_scar64_scar65_dedup_rehydrate_and_preflight_non_choking() {
        let tmp_scar = std::env::temp_dir().join("vortex_gov_scar_dedup_23a.jsonl");
        let tmp_red = std::env::temp_dir().join("vortex_gov_red_loss_23a.jsonl");
        let scar_lines = concat!(
            "{\"ts_ns\":1793979746124021595,\"symbol\":\"TAOUSDT\",",
            "\"side\":\"Bid\",\"entry_ticks\":22413,\"net_bps\":0.46,\"maker_exit\":true}\n",
            "{\"ts_ns\":1793979746124021595,\"symbol\":\"TAOUSDT\",",
            "\"side\":\"Bid\",\"entry_ticks\":22413,\"net_bps\":0.46,\"maker_exit\":true}\n"
        );
        let red_line = concat!(
            "{\"event\":\"TRADE_CLOSE\",\"ts_ns\":1794242514128285563,",
            "\"symbol\":\"PENDLEUSDT\",\"side\":\"Bid\",\"entry_ticks\":16411,",
            "\"net_bps\":-327.4,\"maker_exit\":false}\n"
        );
        assert!(std::fs::write(&tmp_scar, scar_lines).is_ok());
        assert!(std::fs::write(&tmp_red, red_line).is_ok());
        let mut gov = OnlineAdaptiveGovernor::new();
        let (_, unique_cnt) =
            gov.rehydrate_from_scar_and_loss_files(&tmp_scar, &tmp_red, Some(&tmp_red));
        let _ = std::fs::remove_file(&tmp_scar);
        let _ = std::fs::remove_file(&tmp_red);
        assert_eq!(unique_cnt, 2);
        assert!(gov.is_in_scar_debt(7, Side::Bid));

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = TimestampNs(1_800_000_000_000_000_000);
        btc.realized_vol_5m = ScaledBps(6_500);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(400) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
        btc.factor_ofi = BoundedScore::new_clamped(0.55);
        btc.factor_tfi = BoundedScore::new_clamped(0.60);
        btc.factor_geom = BoundedScore::new_clamped(0.25);

        let mut link = btc;
        link.symbol_id = APPROVED_SPECS[9].symbol_id;
        link.bids[0] = BookLevel { price: PriceTicks(10_500), qty: LotQty(320) };
        link.asks[0] = BookLevel { price: PriceTicks(10_501), qty: LotQty(240) };
        link.factor_ofi = BoundedScore::new_clamped(0.42);
        link.factor_tfi = BoundedScore::new_clamped(0.44);
        link.factor_geom = BoundedScore::new_clamped(0.08);
        assert!(gov
            .confirm_preflight_kinematics_non_choking(9, Side::Bid, 6_628_080_000, &link, &btc)
            .is_ok());
    }

    #[test]
    fn test_utc_funding_entry_filter_and_ssot_risk_parity_sizer() {
        let gov = OnlineAdaptiveGovernor::new();
        let w_live = 6_628_080_000_i64;
        // Exact UTC midnight >= 2020: 1_762_992_000 = 20,405 * 86,400s (00:00:00 UTC)
        let day0_ns = 1_762_992_000_000_000_000_i64;
        let prime_1430_ns = TimestampNs(day0_ns + 14 * 3_600_000_000_000 + 1_800_000_000_000);
        let trough_2030_ns = TimestampNs(day0_ns + 20 * 3_600_000_000_000 + 1_800_000_000_000);
        let fund_whip_ns = TimestampNs(day0_ns + 8 * 3_600_000_000_000 - 75_000_000_000);

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = prime_1430_ns;
        btc.realized_vol_5m = ScaledBps(5_200);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(450) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(350) };
        btc.refresh_deep_l1_l5_totals();
        btc.factor_ofi = BoundedScore::new_clamped(0.60);
        btc.factor_tfi = BoundedScore::new_clamped(0.65);
        btc.factor_geom = BoundedScore::new_clamped(0.30);

        // Marginal AVAX setup: SNR = 0.45*0.47 + 0.35*0.48 + 0.20*0.37 = 0.4535, E_kin = 157
        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.ts_local_ns = prime_1430_ns;
        avax.realized_vol_5m = ScaledBps(4_800);
        avax.bids[0] = BookLevel { price: PriceTicks(8_170), qty: LotQty(500) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_171), qty: LotQty(380) };
        avax.refresh_deep_l1_l5_totals();
        avax.factor_ofi = BoundedScore::new_clamped(0.47);
        avax.factor_tfi = BoundedScore::new_clamped(0.48);
        avax.factor_geom = BoundedScore::new_clamped(0.37);

        // Passes in 14:30 UTC overlap; blocked in 20:30 UTC calm trough & -75s funding cut
        assert!(gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, w_live, &avax, &btc));
        avax.ts_local_ns = trough_2030_ns;
        avax.realized_vol_5m = ScaledBps(2_800);
        assert!(!gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, w_live, &avax, &btc));
        avax.ts_local_ns = fund_whip_ns;
        avax.realized_vol_5m = ScaledBps(3_200);
        assert!(!gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, w_live, &avax, &btc));

        // Real structural breakout (vol_5m = 12_000 > 1.10x eq_vol) melts off-peak penalty
        avax.ts_local_ns = trough_2030_ns;
        avax.realized_vol_5m = ScaledBps(12_000);
        assert!(gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, w_live, &avax, &btc));

        // SSOT compute_slot_entry_lots enforces constant-dollar risk parity & even split lots
        avax.ts_local_ns = prime_1430_ns;
        avax.realized_vol_5m = ScaledBps(2_500);
        let lots_calm = gov.compute_slot_entry_lots(3, Side::Bid, w_live, &avax);
        avax.realized_vol_5m = ScaledBps(14_500);
        let lots_storm = gov.compute_slot_entry_lots(3, Side::Bid, w_live, &avax);
        assert!(lots_calm.0 > lots_storm.0);
        assert_eq!(lots_calm.0 % 2, 0);
        assert_eq!(lots_storm.0 % 2, 0);
    }
}
