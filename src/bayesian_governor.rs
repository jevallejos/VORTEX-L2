//! 12x2 Online Bayesian Governor, Anti-Spoof SNR, Signed Epps Beta, [OB+, OB-] & Regime Rehab (V4.8).
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::suboptimal_flops,
    clippy::too_many_arguments
)]

use std::sync::mpsc::SyncSender;

use crate::alpha_3factor::OrthogonalFactorState;
use crate::fee_cadence_beta::{evaluate_utc_dst_and_live_funding_cadence, FeeSchedule};
pub use crate::governor_plasticity::{observe_outcome_surprise_weighted, GovernorPlasticityState};
use crate::kinematic_surface::{
    audit_unified_63_losses_and_2160_manifold, c_clamp_i64, c_max_i64, c_min_i64, c_min_u32,
    evaluate_kinematic_surface_with_fees, evaluate_preflight_hysteresis_with_fees,
    extract_matrix_i64_12x2, extract_matrix_u32_12x2,
    parse_historical_trade_scar_tuple as parse_scar_tuple_core, ScarEventKey, ScarEventPayload,
};
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::order_block::OrderBlockObservables;
use crate::types::{
    evaluate_utc_and_funding_cadence, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TemporalCadenceModulation, TimestampNs, APPROVED_SPECS,
};
use crate::vault_io::VaultIoCommand;

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const INIT_RETURN_VAR_BPS2: i64 = 40_000;
const MIN_VALID_UTC_EPOCH_NS: i64 = 1_577_836_800_000_000_000;
const EPPS_PHI_CAP: f64 = 1.618_033_988_749_895;
pub const EMA_PREWARM_PATH: &str = "data/vault/vortex_ema_prewarm.json";
const MAX_EMA_PREWARM_AGE_NS: i64 = 900_000_000_000;

#[inline]
#[must_use]
fn opposing_ob_for_side(side: Side, snap: &SymbolLobSnapshot) -> OrderBlockObservables {
    let mut obs = snap.order_block_observables();
    let opp_pm = match side {
        Side::Bid => snap.ob_bear_efficacy_pm,
        Side::Ask => snap.ob_bull_efficacy_pm,
    };
    if opp_pm > 0 {
        obs.learned_efficacy_pm = opp_pm;
    }
    obs
}

#[inline]
#[must_use]
fn aligned_ob_for_side(side: Side, snap: &SymbolLobSnapshot) -> OrderBlockObservables {
    let mut obs = snap.order_block_observables();
    let al_pm = match side {
        Side::Bid => snap.ob_bull_efficacy_pm,
        Side::Ask => snap.ob_bear_efficacy_pm,
    };
    if al_pm > 0 {
        obs.learned_efficacy_pm = al_pm;
    }
    obs
}

#[repr(transparent)]
struct SharedAtomicI64(std::sync::atomic::AtomicI64);

impl SharedAtomicI64 {
    #[inline]
    const fn new(val: i64) -> Self {
        Self(std::sync::atomic::AtomicI64::new(val))
    }

    #[inline]
    fn load(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[inline]
    fn store(&self, val: i64) {
        self.0.store(val, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clone for SharedAtomicI64 {
    #[inline]
    fn clone(&self) -> Self {
        Self::new(self.load())
    }
}

impl Default for SharedAtomicI64 {
    #[inline]
    fn default() -> Self {
        Self::new(0)
    }
}

impl core::fmt::Debug for SharedAtomicI64 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(&self.load(), f)
    }
}

#[derive(Clone, Debug)]
pub struct OnlineAdaptiveGovernor {
    ewma_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    ewma_var_bps2: [[i64; 2]; MAX_ACTIVE_PAIRS],
    eq_vol_5m_bps: [i64; MAX_ACTIVE_PAIRS],
    cum_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    sl_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    win_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    directional_cooldown_until_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    global_dir_debt_bps_x100: [SharedAtomicI64; 2],
    global_dir_sl_streak: [u32; 2],
    global_dir_win_streak: [u32; 2],
    global_dir_cooldown_until_ns: [SharedAtomicI64; 2],
    global_debt_decay_ts_ns: [SharedAtomicI64; 2],
    last_global_win_dir: u8,
    ewma_fast_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    ewma_slow_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    last_price_sample_ts_ns: [i64; MAX_ACTIVE_PAIRS],
    wave_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    wave_anchor_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    abort_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_last_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    cross_chop_cooldown_until_ns: [i64; MAX_ACTIVE_PAIRS],
    abc_chop_last_sl_ts_ns: i64,
    abc_chop_sl_score_pm: i64,
    entry_warmup_until_ns: i64,
    portfolio_ewma_net_bps_x100: i64,
    last_daily_loss_used_ratio: f64,
    last_epoch_loss_5u_ratio: f64,
    cached_psi_health: f64,
    last_obs_propulsion: [f64; MAX_ACTIVE_PAIRS],
    last_obs_climax_active: [bool; MAX_ACTIVE_PAIRS],
    last_ws_rtt_us: u32,
    plasticity: GovernorPlasticityState,
}

impl Default for OnlineAdaptiveGovernor {
    #[inline]
    fn default() -> Self { Self::new() }
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
            global_dir_debt_bps_x100: [SharedAtomicI64::new(0), SharedAtomicI64::new(0)],
            global_dir_sl_streak: [0; 2],
            global_dir_win_streak: [0; 2],
            global_dir_cooldown_until_ns: [SharedAtomicI64::new(0), SharedAtomicI64::new(0)],
            global_debt_decay_ts_ns: [SharedAtomicI64::new(0), SharedAtomicI64::new(0)],
            last_global_win_dir: 255,
            ewma_fast_px_1e4: [0; MAX_ACTIVE_PAIRS],
            ewma_slow_px_1e4: [0; MAX_ACTIVE_PAIRS],
            last_price_sample_ts_ns: [0; MAX_ACTIVE_PAIRS],
            wave_anchor_px_1e4: [[0; 2]; MAX_ACTIVE_PAIRS],
            wave_anchor_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_anchor_px_1e4: [[i64::MIN; 2]; MAX_ACTIVE_PAIRS],
            abort_last_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            cross_chop_cooldown_until_ns: [0; MAX_ACTIVE_PAIRS],
            abc_chop_last_sl_ts_ns: 0,
            abc_chop_sl_score_pm: 0,
            entry_warmup_until_ns: 0,
            portfolio_ewma_net_bps_x100: ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw(),
            last_daily_loss_used_ratio: 0.0,
            last_epoch_loss_5u_ratio: 0.0,
            cached_psi_health: 1.0,
            last_obs_propulsion: [0.0; MAX_ACTIVE_PAIRS],
            last_obs_climax_active: [false; MAX_ACTIVE_PAIRS],
            last_ws_rtt_us: 0,
            plasticity: GovernorPlasticityState::new(),
        }
    }

    #[inline] #[must_use] pub const fn dir_idx(side: Side) -> usize { match side { Side::Bid => 0, Side::Ask => 1 } }
    #[inline] #[must_use] pub const fn active_fee_schedule(&self) -> FeeSchedule { self.plasticity.active_fee_schedule() }
    #[inline] pub fn set_active_fee_schedule(&mut self, fees: FeeSchedule) { self.plasticity.set_active_fee_schedule(fees); }
    #[inline] #[must_use] pub const fn active_maker_rt_fee_bps(&self) -> i64 { self.plasticity.maker_round_trip_fee_bps() }
    #[inline] #[must_use] pub const fn slot_taker_prob_permille(&self, slot_idx: usize, side: Side) -> i64 { self.plasticity.slot_taker_prob_permille(slot_idx, side) }
    #[inline] #[must_use] pub const fn portfolio_taker_prob_permille(&self) -> i64 { self.plasticity.portfolio_taker_prob_permille() }
    #[inline] #[must_use] pub const fn psi_health(&self) -> f64 { self.cached_psi_health }
    #[inline] pub fn set_psi_health(&mut self, psi: f64) { self.cached_psi_health = psi.clamp(0.0, 1.0); }
    #[inline] #[must_use] pub fn slot_fill_rate_eta(&self, slot_idx: usize) -> f64 { self.plasticity.slot_fill_rate_eta(slot_idx) }
    #[inline] #[must_use] pub const fn slot_fill_obs_alpha_beta(&self, slot_idx: usize) -> (i64, i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (1_000, 1_000);
        }
        (self.plasticity.fill_obs_alpha_x1000[slot_idx], self.plasticity.fill_obs_beta_x1000[slot_idx])
    }
    #[inline] pub fn observe_passive_entry_fill(&mut self, slot_idx: usize) { self.plasticity.observe_passive_entry_fill(slot_idx); }
    #[inline] pub fn observe_favorable_entry_miss(&mut self, slot_idx: usize) { self.plasticity.observe_favorable_entry_miss(slot_idx); }
    #[inline] #[must_use] pub const fn slot_cont_ratio_x1000(&self, slot_idx: usize, side: Side) -> i64 {
        self.plasticity.slot_cont_ratio_x1000(slot_idx, side)
    }
    #[inline] #[must_use] pub const fn directional_cooldown_until_ns_for(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS { 0 } else { self.directional_cooldown_until_ns[slot_idx][Self::dir_idx(side)] }
    }
    #[inline] #[must_use] pub fn global_dir_debt_bps_x100_for(&self, side: Side) -> i64 {
        self.global_dir_debt_bps_x100[Self::dir_idx(side)].load()
    }
    #[inline] #[must_use] pub fn global_dir_cooldown_until_ns_for(&self, side: Side) -> i64 {
        self.global_dir_cooldown_until_ns[Self::dir_idx(side)].load()
    }
    #[inline] #[must_use]
    pub fn qualifies_inside_spread_penny_jump_c1(
        side: Side,
        snap: &SymbolLobSnapshot,
        psi_health: f64,
    ) -> bool {
        let Some((_, _, spread_ticks)) = snap.bbo_and_spread_ticks() else {
            return false;
        };
        if spread_ticks < 2 || !psi_health.is_finite() || psi_health <= crate::alpha_3factor::INV_PHI_SQ {
            return false;
        }
        let dir_s = side.sign_f64();
        let ofi_al = snap.factor_ofi.get() * dir_s;
        let tfi_al = snap.factor_tfi.get() * dir_s;
        let geom_al = snap.factor_geom.get() * dir_s;
        let (m_skew, _) = OrthogonalFactorState::stoikov_microprice_and_skew(side, snap);
        let prop_al = 0.50 * ofi_al + 0.38 * tfi_al + 0.12 * geom_al;
        ofi_al >= crate::alpha_3factor::INV_PHI_CUBE
            && tfi_al >= 0.0
            && (prop_al >= crate::alpha_3factor::INV_PHI_CUBE
                || m_skew >= crate::alpha_3factor::INV_PHI_FOURTH)
    }
    #[inline] pub fn set_slot_momentum_chain_context(&mut self, slot_idx: usize, signed_propulsion: f64, climax_active: bool, ws_rtt_us: u32) {
        if slot_idx < MAX_ACTIVE_PAIRS {
            self.last_obs_propulsion[slot_idx] = if signed_propulsion.is_finite() { signed_propulsion.clamp(-2.0, 2.0) } else { 0.0 };
            self.last_obs_climax_active[slot_idx] = climax_active;
        }
        if ws_rtt_us > 0 {
            self.last_ws_rtt_us = ws_rtt_us;
        }
    }
    #[inline] pub fn observe_markout_60s_continuation(&mut self, slot_idx: usize, side: Side, mfe_sbps: i64, markout_60s_fav_sbps: i64, vol_5m: ScaledBps) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return crate::governor_plasticity::DEFAULT_CONT_RATIO_X1000;
        }
        let fee_rt = self.active_maker_rt_fee_bps();
        if mfe_sbps.max(markout_60s_fav_sbps) < (fee_rt * 11) / 10 {
            return self.plasticity.slot_cont_ratio_x1000(slot_idx, side);
        }
        let eff_vol = ScaledBps(vol_5m.raw().max(self.eq_vol_5m_bps[slot_idx]).max(1_200));
        let t1_floor_sbps = crate::exit_quoter::compute_elastic_offensive_win_floor_bps(fee_rt, eff_vol).raw();
        self.plasticity.observe_continuation_sample(slot_idx, side, mfe_sbps, markout_60s_fav_sbps, t1_floor_sbps)
    }

    /// Counterfactual `+60s` continuation self-learning from clean non-toxic queue aborts (`V5.5.8`).
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn observe_counterfactual_abort_continuation_v558(
        &mut self,
        slot_idx: usize,
        side: Side,
        cf_peak_sbps: i64,
        cf_fav_60s_sbps: i64,
        vol_5m: ScaledBps,
    ) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return crate::governor_plasticity::DEFAULT_CONT_RATIO_X1000;
        }
        let d = Self::dir_idx(side);
        let cur_cr = self.plasticity.cont_ratio_x1000[slot_idx][d];
        let fee_rt = self.active_maker_rt_fee_bps();
        let eff_vol = ScaledBps(vol_5m.raw().max(self.eq_vol_5m_bps[slot_idx]).max(1_200));
        let t1_floor = crate::exit_quoter::compute_elastic_offensive_win_floor_bps(fee_rt, eff_vol)
            .raw()
            .max(100);
        if cf_peak_sbps < t1_floor || cf_fav_60s_sbps < t1_floor {
            return cur_cr;
        }
        let obs_x1000 = (((cf_peak_sbps.max(cf_fav_60s_sbps) as i128) * 1_000) / (t1_floor as i128))
            .clamp(
                crate::governor_plasticity::MIN_CONT_RATIO_X1000 as i128,
                crate::governor_plasticity::MAX_CONT_RATIO_X1000 as i128,
            ) as i64;
        if obs_x1000 > cur_cr {
            let next_cr = ((910 * cur_cr + 90 * obs_x1000) / 1_000).clamp(
                crate::governor_plasticity::MIN_CONT_RATIO_X1000,
                crate::governor_plasticity::MAX_CONT_RATIO_X1000,
            );
            self.plasticity.cont_ratio_x1000[slot_idx][d] = next_cr;
            next_cr
        } else {
            cur_cr
        }
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn portfolio_ewma_net_bps(&self) -> f64 {
        (self.portfolio_ewma_net_bps_x100 as f64) / 100.0
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn recompute_psi_health_in_place(&mut self) -> f64 {
        let p_tkr = (self.plasticity.portfolio_taker_prob_permille().clamp(5, 950) as f64) * 1e-3;
        let fast_ewma_bps = self.portfolio_ewma_net_bps();
        let mut cell_sum_x100 = 0_i64;
        for row in &self.ewma_net_bps_x100 {
            for &v in row {
                cell_sum_x100 = cell_sum_x100.saturating_add(v);
            }
        }
        let cell_mean_bps = (cell_sum_x100 as f64) / ((MAX_ACTIVE_PAIRS * 2 * 100) as f64);
        let live_ratios_armed =
            self.last_daily_loss_used_ratio > 0.0 || self.last_epoch_loss_5u_ratio > 0.0;
        let r_cap = (1.0 - self.last_daily_loss_used_ratio.clamp(0.0, 1.0))
            * (1.0 - self.last_epoch_loss_5u_ratio.clamp(0.0, 1.0));
        let s_tox_ok = 1.0 - crate::wave_elliott::hermite_smoothstep_c1(p_tkr, 0.065, 0.105);
        let w_res = if live_ratios_armed {
            crate::alpha_3factor::INV_PHI
                * s_tox_ok
                * crate::wave_elliott::hermite_smoothstep_c1(
                    r_cap,
                    crate::alpha_3factor::INV_PHI_SQ,
                    crate::alpha_3factor::INV_PHI,
                )
        } else {
            0.0
        };
        let ewma_bps = (1.0 - w_res) * fast_ewma_bps + w_res * fast_ewma_bps.max(cell_mean_bps);
        let fee_rt_bps = (self.active_maker_rt_fee_bps().max(100) as f64) / 100.0;
        let psi = crate::governor_plasticity::compute_psi_health_c1(
            p_tkr,
            ewma_bps,
            fee_rt_bps,
            self.last_daily_loss_used_ratio,
            self.last_epoch_loss_5u_ratio,
        );
        self.cached_psi_health = psi;
        psi
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    pub fn refresh_psi_health(
        &mut self,
        daily_gov: &crate::governor_plasticity::DailySessionGovernor,
        sinking_reserve_bps_x100: i64,
        epoch_loss_1e8: i64,
    ) -> f64 {
        let fee_rt_sbps = self.active_maker_rt_fee_bps();
        self.last_daily_loss_used_ratio =
            daily_gov.daily_loss_used_ratio(sinking_reserve_bps_x100, fee_rt_sbps);
        self.last_epoch_loss_5u_ratio = ((epoch_loss_1e8.max(0) as f64)
            / (crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 as f64))
            .clamp(0.0, 2.0);
        self.recompute_psi_health_in_place()
    }

    #[inline]
    #[must_use]
    pub const fn slot_return_std_bps(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS { return 200; }
        let d = Self::dir_idx(side);
        let var_u = if self.ewma_var_bps2[slot_idx][d] > 1 { self.ewma_var_bps2[slot_idx][d] as u64 } else { 1_u64 };
        var_u.isqrt() as i64
    }

    #[inline]
    #[must_use]
    pub const fn slot_information_ratio_x100(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS { return 0; }
        let d = Self::dir_idx(side);
        let mu = self.ewma_net_bps_x100[slot_idx][d];
        let sigma = self.slot_return_std_bps(slot_idx, side);
        (mu.saturating_mul(100)) / c_max_i64(sigma.saturating_add(self.active_maker_rt_fee_bps()), 1)
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn slot_temporal_cadence_for_side(
        &self,
        slot_idx: usize,
        candidate_side: Option<Side>,
        snap: &SymbolLobSnapshot,
    ) -> TemporalCadenceModulation {
        if snap.ts_local_ns.0 < MIN_VALID_UTC_EPOCH_NS { return TemporalCadenceModulation::neutral(); }
        let fee_rt = self.active_maker_rt_fee_bps();
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS { self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6) } else { fee_rt * 12 };
        let dst_live = evaluate_utc_dst_and_live_funding_cadence(
            snap.ts_local_ns, snap.realized_vol_5m, ScaledBps(eq_v),
            candidate_side, snap.funding_rate_scaled_bps, snap.next_funding_ts_ns,
        );
        let base_utc = evaluate_utc_and_funding_cadence(snap.ts_local_ns, snap.realized_vol_5m, ScaledBps(eq_v));
        let carry_bps = match candidate_side {
            Some(s) if snap.funding_rate_scaled_bps.0 != 0 => -s.sign_i64().saturating_mul(snap.funding_rate_scaled_bps.0),
            _ => 0,
        };
        let scale_e = |e_raw: i64| -> i64 { (e_raw.saturating_mul(fee_rt)) / 400 };
        let dst_scaled = TemporalCadenceModulation {
            intraday_percentile: dst_live.intraday_percentile,
            snr_threshold_delta: dst_live.snr_threshold_delta,
            kin_energy_min_delta_bps: scale_e(dst_live.kin_energy_min_delta_bps),
            kelly_lot_mult_permille: dst_live.kelly_lot_mult_permille,
        };
        if carry_bps > 0 {
            return dst_scaled;
        }
        let rem_8h = snap.ts_local_ns.0.div_euclid(1_000_000_000).rem_euclid(28_800);
        let rem_to_cut_s = if snap.next_funding_ts_ns.0 > snap.ts_local_ns.0 {
            ((snap.next_funding_ts_ns.0 - snap.ts_local_ns.0) / 1_000_000_000).max(1) as f64
        } else {
            (28_800 - rem_8h) as f64
        };
        let near_live_cut = rem_to_cut_s <= 150.0 || rem_8h >= 28_650 || rem_8h <= 45;
        let tau_hold_s =
            (0.381_966_011_250_105 * ((self.intrinsic_diffusion_clock_ns(slot_idx) as f64) * 1e-9)).max(60.0);
        let exp_fund_drag = ((-carry_bps).max(0) as f64) * (-rem_to_cut_s / (4.0 * tau_hold_s)).exp();
        if dst_scaled.intraday_percentile >= 0.48 && !near_live_cut {
            let h_drag = crate::wave_elliott::hermite_smoothstep_c1(
                exp_fund_drag,
                0.05 * (fee_rt as f64),
                0.381_966_011_250_105 * (fee_rt as f64),
            );
            if h_drag <= 1e-6 {
                return dst_scaled;
            }
            let base_e = scale_e(base_utc.kin_energy_min_delta_bps);
            return TemporalCadenceModulation {
                intraday_percentile: dst_scaled.intraday_percentile,
                snr_threshold_delta: dst_scaled.snr_threshold_delta
                    + h_drag * (base_utc.snr_threshold_delta - dst_scaled.snr_threshold_delta).max(0.0),
                kin_energy_min_delta_bps: dst_scaled.kin_energy_min_delta_bps
                    + (((base_e - dst_scaled.kin_energy_min_delta_bps).max(0) as f64 * h_drag).round() as i64),
                kelly_lot_mult_permille: dst_scaled.kelly_lot_mult_permille
                    - (((dst_scaled.kelly_lot_mult_permille - base_utc.kelly_lot_mult_permille).max(0) as f64
                        * h_drag)
                        .round() as i64),
            };
        }
        TemporalCadenceModulation {
            intraday_percentile: dst_scaled.intraday_percentile,
            snr_threshold_delta: dst_scaled.snr_threshold_delta.max(base_utc.snr_threshold_delta),
            kin_energy_min_delta_bps: dst_scaled.kin_energy_min_delta_bps.max(scale_e(base_utc.kin_energy_min_delta_bps)),
            kelly_lot_mult_permille: dst_scaled.kelly_lot_mult_permille.min(base_utc.kelly_lot_mult_permille),
        }
    }

    #[inline]
    #[must_use]
    pub fn slot_temporal_cadence(&self, slot_idx: usize, snap: &SymbolLobSnapshot) -> TemporalCadenceModulation {
        self.slot_temporal_cadence_for_side(slot_idx, None, snap)
    }

    #[inline]
    #[must_use]
    pub fn compute_slot_entry_lots(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
    ) -> LotQty {
        self.compute_slot_entry_lots_v48(slot_idx, side, wallet_usdt_1e8, snap, 1_000)
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
        if raw_ns < (lo as i128) { lo } else if raw_ns > (hi as i128) { hi } else { raw_ns as i64 }
    }

    /// Continuously modulates `Tranche 1` harvest fraction in `[INV_PHI_SQ, INV_PHI]` (`382..618` permille)
    /// inversely proportional to `S_3(cont_ratio; 1.0, PHI)` (V5.5 O3).
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub const fn learned_tranche1_fraction_permille(&self, slot_idx: usize, side: Side, learned_mfe_bps: i64) -> i64 {
        const PHI_T1: f64 = 1.618_033_988_749_895;
        const INV_PHI_T1: f64 = 0.618_033_988_749_895;
        const INV_PHI_SQ_T1: f64 = 0.381_966_011_250_105;
        if slot_idx >= MAX_ACTIVE_PAIRS { return 500; }
        let cr_x1000 = self.plasticity.slot_cont_ratio_x1000(slot_idx, side);
        let cr_plastic = (cr_x1000 as f64) * 1e-3;
        let t1_floor = crate::exit_quoter::compute_elastic_offensive_win_floor_bps(
            self.active_maker_rt_fee_bps(),
            ScaledBps(self.eq_vol_5m_bps[slot_idx]),
        )
        .raw();
        let denom = (if t1_floor > 100 { t1_floor } else { 100 }) as f64;
        let mfe_ratio = (if learned_mfe_bps > 0 { learned_mfe_bps } else { 0 } as f64) / denom;
        let cont_ratio = if cr_x1000 != crate::governor_plasticity::DEFAULT_CONT_RATIO_X1000 {
            cr_plastic
        } else {
            mfe_ratio
        };
        let raw_u = (cont_ratio - 1.0) / (PHI_T1 - 1.0);
        let u = if raw_u < 0.0 { 0.0 } else if raw_u > 1.0 { 1.0 } else { raw_u };
        let s3 = u * u * (3.0 - 2.0 * u);
        let frac = INV_PHI_T1 - (INV_PHI_T1 - INV_PHI_SQ_T1) * s3;
        let pm = (1_000.0 * frac + 0.5) as i64;
        c_clamp_i64(pm, 382, 618)
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
        let fee_rt = self.active_maker_rt_fee_bps();
        let tau_bps = spec.tick_to_price_scaled_bps(avg_price_ticks).unwrap_or(ScaledBps(fee_rt / 8)).raw();
        let sl_dist_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(realized_vol_5m, self.active_fee_schedule()).raw();
        let ir_x100 = self.slot_information_ratio_x100(s_clamped, side).clamp(-60, 120);
        let ret_std = self.slot_return_std_bps(s_clamped, side);
        let mfe_c = learned_mfe_bps.clamp((fee_rt * 7) / 5, fee_rt * 4);
        let gamma_pm = (220 + (ir_x100 * 5) / 6 + ((mfe_c - 2 * fee_rt) * 100) / (10 * fee_rt).max(1)).clamp(180, 340);
        let mae_mag = (4 * tau_bps).max((sl_dist_bps.saturating_mul(gamma_pm)) / 1_000).max(ret_std / 6);
        let tau_mkt_s = (self.intrinsic_diffusion_clock_ns(s_clamped) / 1_000_000_000).max(90);
        let init_mfe = ((fee_rt * 17) / 10).max(1);
        let raw_def_s = ((((tau_mkt_s / 4) * mfe_c) / init_mfe) * (1_000 + (ir_x100.clamp(-40, 80) * 10) / 3)) / 1_000;
        (-mae_mag, raw_def_s.clamp(tau_mkt_s / 6, (tau_mkt_s * 2) / 5))
    }

    #[inline]
    #[must_use]
    const fn elastic_wave_anchor_ttl_ns(&self, slot_idx: usize, d: usize) -> i64 {
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let fee_rt = self.active_maker_rt_fee_bps();
        let edge_pos = if self.ewma_net_bps_x100[slot_idx][d] > 0 { c_min_i64(self.ewma_net_bps_x100[slot_idx][d], fee_rt * 2) } else { 0 };
        (tau_ns.saturating_mul(2 * fee_rt + edge_pos)) / fee_rt
    }

    #[inline] pub fn set_entry_warmup_until_ns(&mut self, until_ns: i64) { self.entry_warmup_until_ns = until_ns; }

    #[inline]
    pub fn seed_prewarm_slot_emas(&mut self, slot_idx: usize, fast_px_1e4: i64, slow_px_1e4: i64, eq_vol_bps: i64, ts_ns: i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS || fast_px_1e4 <= 0 || slow_px_1e4 <= 0 { return; }
        self.ewma_fast_px_1e4[slot_idx] = fast_px_1e4;
        self.ewma_slow_px_1e4[slot_idx] = slow_px_1e4;
        if eq_vol_bps > 0 { self.eq_vol_5m_bps[slot_idx] = eq_vol_bps.clamp(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 2, 24_000); }
        if ts_ns > 0 { self.last_price_sample_ts_ns[slot_idx] = ts_ns; }
    }

    #[inline]
    pub fn dispatch_ema_prewarm_async(&self, now_ns: TimestampNs, vault_tx: &SyncSender<VaultIoCommand>) {
        if self.ewma_fast_px_1e4.iter().all(|&v| v <= 0) { return; }
        let _ = vault_tx.try_send(VaultIoCommand::SaveEmaPrewarm(Box::new(
            crate::vault_io::EmaPrewarmPayload {
                ts_ns: now_ns.0,
                fast_px_1e4: self.ewma_fast_px_1e4,
                slow_px_1e4: self.ewma_slow_px_1e4,
                eq_vol_5m_bps: self.eq_vol_5m_bps,
            },
        )));
    }

    #[must_use]
    pub fn load_ema_prewarm_from_path(&mut self, path: &std::path::Path, now_ns: TimestampNs) -> usize {
        let Ok(raw) = std::fs::read_to_string(path) else { return 0 };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else { return 0 };
        let file_ts = val.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
        if file_ts <= 0 || (now_ns.0 > file_ts && now_ns.0.saturating_sub(file_ts) > MAX_EMA_PREWARM_AGE_NS) { return 0; }
        let Some(f_arr) = val.get("ewma_fast_px_1e4").and_then(serde_json::Value::as_array) else { return 0 };
        let Some(s_arr) = val.get("ewma_slow_px_1e4").and_then(serde_json::Value::as_array) else { return 0 };
        let eq_opt = val.get("eq_vol_5m_bps").and_then(serde_json::Value::as_array);
        let mut loaded = 0_usize;
        for (idx, (fv, sv)) in f_arr.iter().zip(s_arr.iter()).take(MAX_ACTIVE_PAIRS).enumerate() {
            let (f_px, s_px) = (fv.as_i64().unwrap_or(0), sv.as_i64().unwrap_or(0));
            let eq_v = eq_opt.and_then(|a| a.get(idx)).and_then(serde_json::Value::as_i64).unwrap_or(0);
            if f_px > 0 && s_px > 0 {
                self.seed_prewarm_slot_emas(idx, f_px, s_px, eq_v, file_ts);
                loaded += 1;
            }
        }
        loaded
    }

    #[inline] #[must_use]
    pub const fn is_slot_ema_converged(&self, slot_idx: usize) -> bool {
        slot_idx < MAX_ACTIVE_PAIRS
            && self.ewma_fast_px_1e4[slot_idx] > 0
            && self.ewma_slow_px_1e4[slot_idx] > 0
    }

    #[inline] #[must_use]
    pub fn load_ema_prewarm_file(&mut self, now_ns: TimestampNs) -> usize {
        self.load_ema_prewarm_from_path(std::path::Path::new(EMA_PREWARM_PATH), now_ns)
    }

    #[inline]
    pub fn observe_market_price(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let w_live = snap.wave_observables();
        self.last_obs_propulsion[slot_idx] =
            0.50 * snap.factor_ofi.get() + 0.38 * snap.factor_tfi.get() + 0.12 * snap.factor_geom.get();
        self.last_obs_climax_active[slot_idx] =
            w_live.climax_active || matches!(w_live.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion);
        let mid_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        if mid_ticks <= 0 { return; }
        for d in 0..2 {
            self.sync_global_debt_clock_and_decay(slot_idx, d, PriceTicks(mid_ticks), snap.ts_local_ns.0);
        }
        let px_1e4 = mid_ticks.saturating_mul(10_000);
        let slot_tkr_pm = i64::midpoint(
            self.slot_taker_prob_permille(slot_idx, Side::Bid),
            self.slot_taker_prob_permille(slot_idx, Side::Ask),
        );
        let b3x_floor = APPROVED_SPECS[slot_idx]
            .three_x_friction_barrier_bps_dynamic(PriceTicks(mid_ticks), slot_tkr_pm, self.active_fee_schedule())
            .raw();
        let active_regime_floor = b3x_floor.saturating_mul(2);
        let obs_vol = snap.realized_vol_5m.raw();
        let (fast_prev, slow_prev) = (self.ewma_fast_px_1e4[slot_idx], self.ewma_slow_px_1e4[slot_idx]);
        if fast_prev <= 0 || slow_prev <= 0 {
            self.ewma_fast_px_1e4[slot_idx] = px_1e4;
            self.ewma_slow_px_1e4[slot_idx] = px_1e4;
            if obs_vol > 0 { self.eq_vol_5m_bps[slot_idx] = obs_vol.max(active_regime_floor); }
            self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
            return;
        }
        let sample_dt = snap.ts_local_ns.0.saturating_sub(self.last_price_sample_ts_ns[slot_idx]);
        if sample_dt < 1_000_000_000 { return; }
        self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
        let dt_s = ((sample_dt as f64) * 1e-9).clamp(1.0, 30.0);
        let a_fast = (1.0 - (-dt_s / 20.0).exp()).clamp(0.02, 1.0);
        let a_slow = (1.0 - (-dt_s / 300.0).exp()).clamp(0.002, 1.0);
        self.ewma_fast_px_1e4[slot_idx] = (fast_prev as f64 + a_fast * ((px_1e4 - fast_prev) as f64)).round() as i64;
        self.ewma_slow_px_1e4[slot_idx] = (slow_prev as f64 + a_slow * ((px_1e4 - slow_prev) as f64)).round() as i64;
        if obs_vol > 0 {
            let prev_eq = self.eq_vol_5m_bps[slot_idx].max(active_regime_floor) as f64;
            let obs_f = (obs_vol.max(active_regime_floor)) as f64;
            let a_eq = (1.0 - (-dt_s / (if obs_f > prev_eq { 120.0 } else { 900.0 })).exp()).clamp(0.001, 1.0);
            self.eq_vol_5m_bps[slot_idx] = ((prev_eq + a_eq * (obs_f - prev_eq)).round() as i64).clamp(active_regime_floor, 24_000);
        }
        self.step_regime_shift_shadow_rehab(slot_idx, snap);
    }
}

impl OnlineAdaptiveGovernor {
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn compute_elastic_global_debt_cooldown_ns(
        &self,
        slot_idx: usize,
        d: usize,
        entry_px: PriceTicks,
    ) -> i64 {
        const PHI_CD: f64 = 1.618_033_988_749_895;
        const PHI_CUBE_CD: f64 = 4.236_067_977_499_79;
        const PHI_FIFTH_CD: f64 = 11.090_169_943_749_475;
        let eff_slot = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let tau_mkt = (self.intrinsic_diffusion_clock_ns(eff_slot).max(1)) as f64;
        let fee_rt = self.active_maker_rt_fee_bps().max(100);
        let c_rt = if slot_idx < MAX_ACTIVE_PAIRS && entry_px.0 > 0 {
            let side = if d == 0 { Side::Bid } else { Side::Ask };
            let p_pm = self.slot_taker_prob_permille(slot_idx, side);
            APPROVED_SPECS[slot_idx]
                .expected_round_trip_cost_bps_dynamic(entry_px, p_pm, self.active_fee_schedule())
                .raw()
                .max(100) as f64
        } else {
            fee_rt as f64
        };
        let debt_abs = self.global_dir_debt_bps_x100[d].load().unsigned_abs() as f64;
        let s_debt = crate::wave_elliott::hermite_smoothstep_c1(
            debt_abs / c_rt.max(1.0),
            PHI_CUBE_CD,
            PHI_FIFTH_CD,
        );
        (PHI_CD * tau_mkt * (1.0 + s_debt)).round() as i64
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn sync_global_debt_clock_and_decay(
        &self,
        slot_idx: usize,
        d: usize,
        entry_px: PriceTicks,
        now_ts_ns: i64,
    ) {
        const PHI_SQ_DEC: f64 = 2.618_033_988_749_895;
        const INV_PHI_SQ_DEC: f64 = 0.381_966_011_250_105;
        if now_ts_ns <= 0 || d >= 2 {
            return;
        }
        let fee_rt = self.active_maker_rt_fee_bps();
        let contagion_floor = -(fee_rt * 25);
        let mut debt = self.global_dir_debt_bps_x100[d].load();
        let cd = self.global_dir_cooldown_until_ns[d].load();
        let decay_ts = self.global_debt_decay_ts_ns[d].load();

        if debt <= contagion_floor && cd <= 0 && decay_ts <= 0 {
            let dt_cd = self.compute_elastic_global_debt_cooldown_ns(slot_idx, d, entry_px);
            self.global_dir_cooldown_until_ns[d].store(now_ts_ns.saturating_add(dt_cd.max(1)));
            self.global_debt_decay_ts_ns[d].store(0);
            return;
        }

        if (cd > 0 && now_ts_ns >= cd) || (cd <= 0 && decay_ts > 0 && debt < 0) {
            let anchor = if decay_ts > 0 { decay_ts } else { cd };
            if now_ts_ns > anchor && debt < 0 {
                let dt_ns = (now_ts_ns - anchor) as f64;
                let eff_slot = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
                let tau_mkt = (self.intrinsic_diffusion_clock_ns(eff_slot).max(1)) as f64;
                let psi_eff = self.psi_health().max(INV_PHI_SQ_DEC);
                let tau_decay = (PHI_SQ_DEC * tau_mkt) / psi_eff;
                debt = ((debt as f64) * (-dt_ns / tau_decay).exp()).trunc() as i64;
                self.global_dir_debt_bps_x100[d].store(debt);
            }
            if debt >= 0 {
                if cd > 0 && now_ts_ns >= cd {
                    self.global_dir_cooldown_until_ns[d].store(0);
                }
                self.global_debt_decay_ts_ns[d].store(0);
            } else if debt > contagion_floor {
                self.global_dir_cooldown_until_ns[d].store(0);
                self.global_debt_decay_ts_ns[d].store(now_ts_ns);
            } else {
                self.global_debt_decay_ts_ns[d].store(now_ts_ns);
            }
        }
    }

    pub fn step_regime_shift_shadow_rehab(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS || snap.ts_local_ns.0 <= 0 { return; }
        let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
        } else {
            snap.mark_price
        };
        if mid_px.0 <= 0 { return; }
        for d in 0..2 {
            self.sync_global_debt_clock_and_decay(slot_idx, d, mid_px, snap.ts_local_ns.0);
        }
        let spec = APPROVED_SPECS[slot_idx];
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let eq_vol = self.eq_vol_5m_bps[slot_idx];
        let (drift_bps, _) = self.price_drift_and_extension_scaled_bps(slot_idx, snap);
        let fees = self.plasticity.active_fee_schedule();
        for side in [Side::Bid, Side::Ask] {
            if !self.is_in_local_slot_scar_debt(slot_idx, side) { continue; }
            let d = Self::dir_idx(side);
            let p_pm = self.plasticity.slot_taker_prob_permille(slot_idx, side);
            let b3x_bps = spec.three_x_friction_barrier_bps_dynamic(mid_px, p_pm, fees).raw();
            if self.plasticity.evaluate_shadow_rehab_step(
                slot_idx, side, self.sl_streak[slot_idx][d].max(1), tau_ns, eq_vol, b3x_bps, drift_bps, snap,
            ) {
                self.sl_streak[slot_idx][d] = self.sl_streak[slot_idx][d].saturating_sub(1);
                self.cum_net_bps_x100[slot_idx][d] =
                    GovernorPlasticityState::contract_local_debt_golden(self.cum_net_bps_x100[slot_idx][d]);
            }
        }
    }

    #[inline] #[must_use]
    pub const fn price_drift_and_extension_scaled_bps(&self, slot_idx: usize, snap: &SymbolLobSnapshot) -> (i64, i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS { return (0, 0); }
        let (fast, slow) = (self.ewma_fast_px_1e4[slot_idx], self.ewma_slow_px_1e4[slot_idx]);
        if fast <= 0 || slow <= 0 { return (0, 0); }
        let drift_bps = ((fast - slow).saturating_mul(1_000_000)) / slow;
        let cur_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else {
            snap.mark_price.0
        };
        let cur_1e4 = cur_ticks.saturating_mul(10_000);
        (drift_bps, if cur_1e4 > 0 { ((cur_1e4 - slow).saturating_mul(1_000_000)) / slow } else { 0 })
    }

    #[inline] #[must_use]
    pub const fn slot_direction_diagnostics(&self, slot_idx: usize, side: Side) -> (i64, u32, u32) {
        if slot_idx >= MAX_ACTIVE_PAIRS { return (0, 0, 0); }
        let d = Self::dir_idx(side);
        (self.ewma_net_bps_x100[slot_idx][d], self.sl_streak[slot_idx][d], self.win_streak[slot_idx][d])
    }

    #[inline] #[must_use]
    pub const fn is_in_local_slot_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS { return false; }
        let d = Self::dir_idx(side);
        self.cum_net_bps_x100[slot_idx][d] <= -(self.active_maker_rt_fee_bps() * 4) || self.sl_streak[slot_idx][d] > 0
    }

    #[inline] #[must_use]
    pub fn is_in_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS { return false; }
        let d = Self::dir_idx(side);
        self.is_in_local_slot_scar_debt(slot_idx, side)
            || self.global_dir_debt_bps_x100[d].load() < -(self.active_maker_rt_fee_bps() * 15)
    }

    #[inline] #[must_use]
    pub fn audit_historical_entry_veto(&self, slot_idx: usize, side: Side, entry_ticks: i64, check_ts_ns: i64) -> Option<&'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS { return Some("VETO_INVALID_SLOT"); }
        let d = Self::dir_idx(side);
        let fee_rt = self.active_maker_rt_fee_bps();
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        if check_ts_ns > 0 {
            self.sync_global_debt_clock_and_decay(slot_idx, d, PriceTicks(entry_ticks), check_ts_ns);
        }
        if check_ts_ns > 0 && check_ts_ns < self.directional_cooldown_until_ns[slot_idx][d] { return Some("VETO_SLOT_COOLDOWN"); }
        let sym_net = self.cum_net_bps_x100[slot_idx][0].saturating_add(self.cum_net_bps_x100[slot_idx][1]);
        let in_contagion = slot_idx > 0
            && sym_net <= -(fee_rt * 6)
            && self.global_dir_debt_bps_x100[d].load() <= -(fee_rt * 25)
            && self.cum_net_bps_x100[slot_idx][d] <= 0;
        let glob_cd_now = self.global_dir_cooldown_until_ns[d].load();
        if in_contagion && (check_ts_ns <= 0 || check_ts_ns < glob_cd_now) {
            return Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION");
        }
        if check_ts_ns > 0 && check_ts_ns < glob_cd_now { return Some("VETO_GLOBAL_WAVE_OR_SL_BREATHER"); }
        let (anchor_1e4, anchor_ts) = (self.wave_anchor_px_1e4[slot_idx][d], self.wave_anchor_ts_ns[slot_idx][d]);
        let wave_fresh = check_ts_ns <= 0 || anchor_ts <= 0 || check_ts_ns.saturating_sub(anchor_ts) <= self.elastic_wave_anchor_ttl_ns(slot_idx, d);
        let cur_1e4 = entry_ticks.saturating_mul(10_000);
        let wave_active = self.win_streak[slot_idx][d] >= 3 || (self.win_streak[slot_idx][d] >= 1 && self.global_dir_win_streak[d] >= 3);
        if wave_active && wave_fresh && anchor_1e4 > 0 && cur_1e4 > 0 {
            let chased_bps = match side {
                Side::Bid => ((cur_1e4 - anchor_1e4).saturating_mul(1_000_000)) / anchor_1e4,
                Side::Ask => ((anchor_1e4 - cur_1e4).saturating_mul(1_000_000)) / anchor_1e4,
            };
            let stair_min = fee_rt * 6 + c_clamp_i64(self.ewma_net_bps_x100[slot_idx][d], -fee_rt, fee_rt) / 4;
            let stair_max = self.eq_vol_5m_bps[slot_idx].saturating_mul(2) + fee_rt * 8;
            if chased_bps > stair_min && chased_bps < stair_max { return Some("VETO_WAVE_STAIRCASE_25BPS"); }
        }
        if self.abort_streak[slot_idx][d] >= 4 && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && check_ts_ns > 0 && check_ts_ns.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < (tau_ns / 20)
        {
            return Some("VETO_ABORT_STREAK_BREATHER");
        }
        if self.sl_streak[slot_idx][d] >= 1 && self.cum_net_bps_x100[slot_idx][d] <= -(fee_rt * 11) {
            let slot_cd = self.directional_cooldown_until_ns[slot_idx][d];
            let debt_mult = c_clamp_i64(self.cum_net_bps_x100[slot_idx][d].saturating_neg() / fee_rt, 2, 16);
            let deep_q_ns = (tau_ns.saturating_mul(debt_mult).saturating_mul(c_min_u32(self.sl_streak[slot_idx][d], 4) as i64)) / 2;
            if check_ts_ns <= 0 || slot_cd <= 0 || check_ts_ns < slot_cd.saturating_add(deep_q_ns) {
                return Some("VETO_DEEP_SCAR_DEBT_QUARANTINE");
            }
        }
        if slot_idx > 0 {
            let glob_cd = self.global_dir_cooldown_until_ns[d].load();
            let decay_ts = self.global_debt_decay_ts_ns[d].load();
            if self.global_dir_sl_streak[d] >= 1 && self.last_global_win_dir == ((1 - d) as u8) {
                let dom_ns = tau_ns.saturating_mul(c_min_u32(self.global_dir_sl_streak[d], 4) as i64) * 2;
                if glob_cd <= 0 && decay_ts <= 0 {
                    if check_ts_ns > 0 {
                        self.global_dir_cooldown_until_ns[d].store(check_ts_ns.saturating_add(dom_ns));
                    }
                    return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
                }
                if check_ts_ns <= 0 || (glob_cd > 0 && decay_ts <= 0 && check_ts_ns < glob_cd.saturating_add(dom_ns)) {
                    return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
                }
            }
        }
        None
    }

    #[inline]
    pub fn evaluate_kinematic_energy_surface(
        &self, slot_idx: usize, side: Side, drift_bps: i64, ext_bps: i64, signed_geom: f64, snr_abs: f64,
    ) -> Result<(i64, i64), &'static str> {
        let fee_rt = self.active_maker_rt_fee_bps();
        let eq_vol = if slot_idx < MAX_ACTIVE_PAIRS { self.eq_vol_5m_bps[slot_idx].max(fee_rt * 6) } else { fee_rt * 12 };
        let d = Self::dir_idx(side);
        let (learned_edge, ir_x100) = if slot_idx < MAX_ACTIVE_PAIRS {
            (self.ewma_net_bps_x100[slot_idx][d], self.slot_information_ratio_x100(slot_idx, side))
        } else {
            (fee_rt / 2, 33)
        };
        evaluate_kinematic_surface_with_fees(
            side, eq_vol, learned_edge, ir_x100, drift_bps, ext_bps, signed_geom, snr_abs, self.active_fee_schedule(),
        )
    }

    #[inline]
    pub fn observe_closed_trade_at_entry(
        &mut self, slot_idx: usize, side: Side, entry_price: PriceTicks, net_scaled_bps: ScaledBps, is_maker_exit: bool, ts_ns: TimestampNs,
    ) {
        self.observe_closed_trade_core(slot_idx, side, entry_price, net_scaled_bps, None, is_maker_exit, None, ts_ns);
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn observe_closed_trade_core(
        &mut self, slot_idx: usize, side: Side, entry_price: PriceTicks, net_scaled_bps: ScaledBps, mfe_opt: Option<ScaledBps>, is_maker_exit: bool, climax_override: Option<bool>, ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let d = Self::dir_idx(side);
        let fee_rt = self.active_maker_rt_fee_bps();
        let adv_sl = !is_maker_exit && net_scaled_bps.raw() < 0;
        let eff_mfe = mfe_opt.unwrap_or_else(|| {
            if net_scaled_bps.raw() > 0 {
                ScaledBps(net_scaled_bps.raw().saturating_add(fee_rt))
            } else {
                ScaledBps(0)
            }
        });
        self.plasticity.observe_exit_outcome_core(
            slot_idx,
            side,
            adv_sl,
            net_scaled_bps,
            eff_mfe,
            mfe_opt.is_some(),
            ts_ns.0,
            self.eq_vol_5m_bps[slot_idx],
        );
        let prev_mean = self.ewma_net_bps_x100[slot_idx][d].clamp(-100_000, 100_000);
        let is_censored_scratch =
            is_maker_exit && net_scaled_bps.raw() >= 0 && net_scaled_bps.raw() < fee_rt / 4;
        if is_censored_scratch {
            let mfe_net_disc = ((eff_mfe.raw().saturating_sub(fee_rt)).max(0).saturating_mul(382)) / 1_000;
            let eff_scr_net = net_scaled_bps.raw().max(mfe_net_disc).clamp(0, 100_000);
            let diff = eff_scr_net.saturating_sub(prev_mean);
            self.ewma_var_bps2[slot_idx][d] = ((self.ewma_var_bps2[slot_idx][d].clamp(100, 100_000_000) * 953
                + diff.saturating_mul(diff).min(25_000_000) * 47)
                / 1_000)
                .max(2_500);
            self.ewma_net_bps_x100[slot_idx][d] =
                ((prev_mean * 941 + eff_scr_net * 59) / 1_000).clamp(-100_000, 100_000);
            self.portfolio_ewma_net_bps_x100 = ((self.portfolio_ewma_net_bps_x100.clamp(-100_000, 100_000) * 944
                + eff_scr_net * 56)
                / 1_000)
                .clamp(-100_000, 100_000);
        } else {
            let diff = net_scaled_bps.raw().saturating_sub(prev_mean);
            self.ewma_var_bps2[slot_idx][d] = ((self.ewma_var_bps2[slot_idx][d].clamp(100, 100_000_000) * 80
                + diff.saturating_mul(diff).min(25_000_000) * 20)
                / 100)
                .max(2_500);
            self.ewma_net_bps_x100[slot_idx][d] = ((prev_mean * 75
                + net_scaled_bps.raw().clamp(-100_000, 100_000) * 25)
                / 100)
                .clamp(-100_000, 100_000);
            self.portfolio_ewma_net_bps_x100 = ((self.portfolio_ewma_net_bps_x100.clamp(-100_000, 100_000) * 764
                + net_scaled_bps.raw().clamp(-100_000, 100_000) * 236)
                / 1_000)
                .clamp(-100_000, 100_000);
        }
        let _ = self.recompute_psi_health_in_place();
        if ts_ns.0 > 0 {
            self.sync_global_debt_clock_and_decay(slot_idx, d, entry_price, ts_ns.0);
        }
        self.cum_net_bps_x100[slot_idx][d] = self.cum_net_bps_x100[slot_idx][d].saturating_add(net_scaled_bps.raw()).clamp(-(fee_rt * 60), fee_rt * 60);
        let updated_glob_debt = self.global_dir_debt_bps_x100[d]
            .load()
            .saturating_add(net_scaled_bps.raw())
            .clamp(-(fee_rt * 75), fee_rt * 30);
        self.global_dir_debt_bps_x100[d].store(updated_glob_debt);
        let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx);
        let ir_x100 = self.slot_information_ratio_x100(slot_idx, side).clamp(-50, 100);
        if is_maker_exit && net_scaled_bps.raw() < 0 {
            let sl_sbps = PairSpec::elastic_server_sl_distance_bps_with_fees(
                ScaledBps(self.eq_vol_5m_bps[slot_idx].max(1_200)),
                self.active_fee_schedule(),
            );
            let w_bail = crate::governor_plasticity::compute_fractional_maker_bailout_scar_weight_c1(
                net_scaled_bps,
                ScaledBps(fee_rt),
                sl_sbps,
            );
            self.win_streak[slot_idx][d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            self.wave_anchor_ts_ns[slot_idx][d] = 0;
            if w_bail >= crate::alpha_3factor::INV_PHI_SQ {
                self.sl_streak[slot_idx][d] = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            }
            if ts_ns.0 > 0 {
                let bail_cd = ((tau_ns as f64)
                    * (crate::alpha_3factor::INV_PHI_CUBE + crate::alpha_3factor::PHI * w_bail))
                    .round() as i64;
                self.directional_cooldown_until_ns[slot_idx][d] = self.directional_cooldown_until_ns[slot_idx][d]
                    .max(ts_ns.0.saturating_add(bail_cd));
            }
        } else if !is_maker_exit {
            let s = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            self.sl_streak[slot_idx][d] = s;
            self.win_streak[slot_idx][d] = 0;
            self.global_dir_win_streak[d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            self.wave_anchor_ts_ns[slot_idx][d] = 0;
            if ts_ns.0 > 0 { self.directional_cooldown_until_ns[slot_idx][d] = ts_ns.0.saturating_add((tau_ns / 2).saturating_mul(1_i64 << s.min(4))); }
            let loss_sev = ((-net_scaled_bps.raw()).max(0) as f64) / (fee_rt.max(100) as f64);
            let h_glob = crate::wave_elliott::hermite_smoothstep_c1(
                loss_sev,
                2.618_033_988_749_895,
                6.854_101_966_249_685,
            );
            if loss_sev >= 4.236_067_977_499_79 {
                let gs = self.global_dir_sl_streak[d].saturating_add(1).min(4);
                self.global_dir_sl_streak[d] = gs;
                if ts_ns.0 > 0 {
                    let g_cd = (((((tau_ns * 3) / 5).saturating_mul(i64::from(gs))) as f64) * h_glob).round() as i64;
                    let cur_gcd = self.global_dir_cooldown_until_ns[d].load();
                    self.global_dir_cooldown_until_ns[d]
                        .store(cur_gcd.max(ts_ns.0.saturating_add(g_cd)));
                    self.global_debt_decay_ts_ns[d].store(0);
                }
            } else if h_glob > 0.0 && ts_ns.0 > 0 {
                let mild_cd = ((((tau_ns * 3) / 5) as f64) * h_glob).round() as i64;
                let cur_gcd = self.global_dir_cooldown_until_ns[d].load();
                self.global_dir_cooldown_until_ns[d]
                    .store(cur_gcd.max(ts_ns.0.saturating_add(mild_cd)));
                self.global_debt_decay_ts_ns[d].store(0);
            }
        } else if net_scaled_bps.raw() < fee_rt / 4 {
            if ts_ns.0 > 0 {
                let scratch_cd = ts_ns.0.saturating_add((tau_ns * (120 - ir_x100 / 2)) / 360);
                if scratch_cd > self.directional_cooldown_until_ns[slot_idx][d] { self.directional_cooldown_until_ns[slot_idx][d] = scratch_cd; }
            }
        } else {
            self.clear_abort_streak(slot_idx, side);
            let prev_ts = self.wave_anchor_ts_ns[slot_idx][d];
            let expired = ts_ns.0 > 0 && prev_ts > 0 && ts_ns.0.saturating_sub(prev_ts) > self.elastic_wave_anchor_ttl_ns(slot_idx, d);
            if entry_price.0 > 0 && (self.wave_anchor_px_1e4[slot_idx][d] <= 0 || expired) {
                self.wave_anchor_px_1e4[slot_idx][d] = entry_price.0.saturating_mul(10_000);
            }
            if ts_ns.0 > 0 { self.wave_anchor_ts_ns[slot_idx][d] = ts_ns.0; }
            let w = if expired { 1_u32 } else { self.win_streak[slot_idx][d].saturating_add(1) };
            self.win_streak[slot_idx][d] = w;
            self.win_streak[slot_idx][1 - d] = 0;
            self.wave_anchor_px_1e4[slot_idx][1 - d] = 0;
            self.wave_anchor_ts_ns[slot_idx][1 - d] = 0;
            let gw = self.global_dir_win_streak[d].saturating_add(1);
            self.global_dir_win_streak[d] = if gw >= 6 { 0 } else { gw };
            self.global_dir_win_streak[1 - d] = 0;
            self.last_global_win_dir = d as u8;
            if ts_ns.0 > 0 {
                let base_win_cd_ns = ((tau_ns * (100 - ir_x100 / 3)) / 1_500) * (w.min(2) as i64);
                let aligned_prop = self.last_obs_propulsion[slot_idx] * side.sign_f64();
                let climax_now = climax_override.unwrap_or(self.last_obs_climax_active[slot_idx]);
                let eff_rtt_us = if self.last_ws_rtt_us > 0 {
                    self.last_ws_rtt_us
                } else {
                    crate::execution_fsm::read_live_ws_pulse().0
                };
                let eff_win_cd_ns =
                    crate::governor_plasticity::compute_momentum_chain_win_cooldown_ns_c1(
                        base_win_cd_ns,
                        is_maker_exit,
                        net_scaled_bps,
                        fee_rt,
                        climax_now,
                        self.cached_psi_health,
                        aligned_prop,
                        eff_rtt_us,
                    );
                let win_cd = ts_ns.0.saturating_add(eff_win_cd_ns);
                if win_cd > self.directional_cooldown_until_ns[slot_idx][d] { self.directional_cooldown_until_ns[slot_idx][d] = win_cd; }
            }
            if self.sl_streak[slot_idx][d] > 0 && w >= 3 && self.cum_net_bps_x100[slot_idx][d] >= -(fee_rt * 8) {
                self.sl_streak[slot_idx][d] -= 1;
                self.win_streak[slot_idx][d] = 0;
            }
            if self.global_dir_sl_streak[d] > 0 && updated_glob_debt >= -(fee_rt * 15) {
                self.global_dir_sl_streak[d] -= 1;
            }
            if updated_glob_debt >= 0 {
                self.global_debt_decay_ts_ns[d].store(0);
            }
        }
        if net_scaled_bps.raw() < 0 && ts_ns.0 > 0 && updated_glob_debt <= -(fee_rt * 25) {
            let cur_gcd = self.global_dir_cooldown_until_ns[d].load();
            if cur_gcd <= 0 || ts_ns.0 >= cur_gcd {
                let dt_cd = self.compute_elastic_global_debt_cooldown_ns(slot_idx, d, entry_price);
                self.global_dir_cooldown_until_ns[d].store(ts_ns.0.saturating_add(dt_cd.max(1)));
                self.global_debt_decay_ts_ns[d].store(0);
            }
        }
    }

    #[inline]
    pub fn clear_abort_streak(&mut self, slot_idx: usize, side: Side) {
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let d = Self::dir_idx(side);
        self.abort_streak[slot_idx][d] = 0;
        self.abort_anchor_px_1e4[slot_idx][d] = i64::MIN;
        self.abort_last_ts_ns[slot_idx][d] = 0;
    }

    #[inline]
    pub fn observe_entry_abort(&mut self, slot_idx: usize, side: Side, aborted_price: PriceTicks, ts_ns: TimestampNs) {
        self.observe_entry_abort_with_context(slot_idx, side, aborted_price, ts_ns, false, true);
    }

    /// Context-aware queue abort observer (V5.4.2 M2): favorable BBO miss without toxic Hawkes sweep
    /// updates `BetaBinomialFillRate` (`beta += 1000`) and exempts punitive anti-knife cooldown,
    /// keeping `V08 POST_ABORT_KNIFE_TRAP` 100% active on adverse/toxic aborts.
    #[inline]
    pub fn observe_entry_abort_with_context(
        &mut self,
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        ts_ns: TimestampNs,
        favorable_bbo_miss: bool,
        toxic_hawkes_sweep: bool,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        if favorable_bbo_miss && !toxic_hawkes_sweep {
            self.plasticity.observe_favorable_entry_miss(slot_idx);
            return;
        }
        if favorable_bbo_miss {
            self.plasticity.observe_favorable_entry_miss(slot_idx);
        }
        let d = Self::dir_idx(side);
        let prev_ts = self.abort_last_ts_ns[slot_idx][d];
        if prev_ts > 0 && ts_ns.0 > prev_ts && ts_ns.0.saturating_sub(prev_ts) >= (self.intrinsic_diffusion_clock_ns(slot_idx) / 5) {
            self.abort_streak[slot_idx][d] = 0;
        }
        self.abort_streak[slot_idx][d] = self.abort_streak[slot_idx][d].saturating_add(1).min(8);
        self.abort_anchor_px_1e4[slot_idx][d] = aborted_price.0.saturating_mul(10_000);
        self.abort_last_ts_ns[slot_idx][d] = ts_ns.0;
    }

    pub fn audit_scar64_entry_and_sweep_shield(&self, slot_idx: usize, side: Side, snap: &SymbolLobSnapshot, btc_signed_snr: f64) -> Result<(), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS { return Ok(()); }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Ok(()); };
        let d = Self::dir_idx(side);
        let d_sign = side.sign_i64();
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if self.abort_streak[slot_idx][d] >= 1 && self.abort_anchor_px_1e4[slot_idx][d] > 0 && snap.ts_local_ns.0 > 0 {
            let sweep_ttl_ns = ((25_000_000_000_i64 * 6_000) / snap.realized_vol_5m.raw().clamp(1_200, 15_000)).clamp(18_000_000_000, 45_000_000_000);
            let move_1e4 = (entry_px.0.saturating_mul(10_000) - self.abort_anchor_px_1e4[slot_idx][d]).saturating_mul(d_sign);
            if snap.ts_local_ns.0.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < sweep_ttl_ns && move_1e4 < 0 {
                return Err("VETO_POST_ABORT_ADVERSE_SWEEP");
            }
        }
        if slot_idx > 0 {
            let glob_debt = (-self.global_dir_debt_bps_x100[d].load()).max(0) as f64;
            let opp_macro = (-snap.wave_observables().macro_phase.get() * side.sign_f64()).max(0.0);
            if glob_debt >= 8_000.0 || self.global_dir_sl_streak[d] >= 1 || opp_macro > 0.05 {
                let min_btc_snr = (-0.04 + 0.04 * (glob_debt / 10_000.0).tanh() + 0.85 * (opp_macro - 0.05).max(0.0)).clamp(-0.04, 0.28);
                if btc_signed_snr < min_btc_snr { return Err("VETO_SCAR64_BTC_DEBT_HEADWIND"); }
            }
            let (fast_1e4, slow_1e4) = (self.ewma_fast_px_1e4[slot_idx], self.ewma_slow_px_1e4[slot_idx]);
            let signed_ofi = snap.factor_ofi.get() * side.sign_f64();
            let sym_sl_tot = self.sl_streak[slot_idx][0].saturating_add(self.sl_streak[slot_idx][1]);
            if fast_1e4 > 0 && slow_1e4 > 0 {
                let px_1e4 = i64::midpoint(best_bid.price.0, best_ask.price.0).saturating_mul(10_000);
                let s_drift = (((fast_1e4 - slow_1e4).saturating_mul(1_000_000)) / slow_1e4).saturating_mul(d_sign);
                let lead_impulse = (((px_1e4 - slow_1e4).saturating_mul(1_000_000)) / slow_1e4).saturating_mul(d_sign).saturating_sub(s_drift);
                let strong_healthy_ofi = signed_ofi >= 0.50 && s_drift >= 0 && snap.factor_tfi.get() * side.sign_f64() >= 0.0;
                if sym_sl_tot >= 1 && (signed_ofi < 0.34 || (lead_impulse < 0 && !strong_healthy_ofi)) { return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE"); }
                let low_vol_decel = snap.realized_vol_5m.raw() < 2_500 || (snap.realized_vol_5m.raw() < 3_000 && signed_ofi < 0.35);
                if s_drift > 0 && lead_impulse < -15 && (low_vol_decel || btc_signed_snr < 0.08) {
                    return Err("VETO_FOLLOWER_DECEL_UNDER_COMPRESSION");
                }
            } else if sym_sl_tot >= 1 && signed_ofi < 0.34 {
                return Err("VETO_SYMBOL_CROSS_SCAR_WEAK_IMPULSE");
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn to_scar_snapshot_json(&self, ts_ns: TimestampNs) -> serde_json::Value {
        let glob_debt = [
            self.global_dir_debt_bps_x100[0].load(),
            self.global_dir_debt_bps_x100[1].load(),
        ];
        let glob_cd = [
            self.global_dir_cooldown_until_ns[0].load(),
            self.global_dir_cooldown_until_ns[1].load(),
        ];
        let mut val = serde_json::json!({
            "event": "GOV_SCAR_SNAPSHOT", "ts_ns": ts_ns.0,
            "ewma_net_bps_x100": self.ewma_net_bps_x100, "ewma_var_bps2": self.ewma_var_bps2,
            "eq_vol_5m_bps": self.eq_vol_5m_bps, "cum_net_bps_x100": self.cum_net_bps_x100,
            "sl_streak": self.sl_streak, "win_streak": self.win_streak,
            "directional_cooldown_until_ns": self.directional_cooldown_until_ns,
            "wave_anchor_px_1e4": self.wave_anchor_px_1e4, "wave_anchor_ts_ns": self.wave_anchor_ts_ns,
            "global_dir_debt_bps_x100": glob_debt,
            "global_dir_sl_streak": self.global_dir_sl_streak, "global_dir_win_streak": self.global_dir_win_streak,
            "global_dir_cooldown_until_ns": glob_cd, "last_global_win_dir": self.last_global_win_dir,
            "cross_chop_cooldown_until_ns": self.cross_chop_cooldown_until_ns,
            "abc_chop_last_sl_ts_ns": self.abc_chop_last_sl_ts_ns,
            "abc_chop_sl_score_pm": self.abc_chop_sl_score_pm,
            "portfolio_ewma_net_bps_x100": self.portfolio_ewma_net_bps_x100,
        });
        if let Some(map) = val.as_object_mut() { self.plasticity.write_to_scar_json_map(map); }
        val
    }

    pub fn apply_scar_snapshot_json(&mut self, val: &serde_json::Value) -> Option<TimestampNs> {
        if val.get("event").and_then(serde_json::Value::as_str) != Some("GOV_SCAR_SNAPSHOT") { return None; }
        let snap_ts = val.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
        extract_matrix_i64_12x2(val, "ewma_net_bps_x100", &mut self.ewma_net_bps_x100);
        self.ewma_var_bps2 = [[INIT_RETURN_VAR_BPS2; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "ewma_var_bps2", &mut self.ewma_var_bps2);
        let min_fl = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 2;
        if let Some(eq_arr) = val.get("eq_vol_5m_bps").and_then(serde_json::Value::as_array) {
            for (s, cell) in eq_arr.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = cell.as_i64() { if n > 0 { self.eq_vol_5m_bps[s] = n.clamp(min_fl, 24_000); } }
            }
        }
        extract_matrix_i64_12x2(val, "cum_net_bps_x100", &mut self.cum_net_bps_x100);
        extract_matrix_u32_12x2(val, "sl_streak", &mut self.sl_streak);
        extract_matrix_u32_12x2(val, "win_streak", &mut self.win_streak);
        for s in 0..MAX_ACTIVE_PAIRS {
            for d in 0..2 {
                self.ewma_net_bps_x100[s][d] = self.ewma_net_bps_x100[s][d].clamp(-100_000, 100_000);
                self.ewma_var_bps2[s][d] = self.ewma_var_bps2[s][d].clamp(2_500, 100_000_000);
                self.cum_net_bps_x100[s][d] = self.cum_net_bps_x100[s][d].clamp(-100_000, 100_000);
                self.sl_streak[s][d] = self.sl_streak[s][d].min(4);
                self.win_streak[s][d] = self.win_streak[s][d].min(16);
            }
        }
        extract_matrix_i64_12x2(val, "directional_cooldown_until_ns", &mut self.directional_cooldown_until_ns);
        extract_matrix_i64_12x2(val, "wave_anchor_px_1e4", &mut self.wave_anchor_px_1e4);
        self.wave_anchor_ts_ns = [[0; 2]; MAX_ACTIVE_PAIRS];
        extract_matrix_i64_12x2(val, "wave_anchor_ts_ns", &mut self.wave_anchor_ts_ns);
        for s in 0..MAX_ACTIVE_PAIRS {
            for d in 0..2 {
                if self.wave_anchor_px_1e4[s][d] > 0 && self.wave_anchor_ts_ns[s][d] <= 0 {
                    let fb = self.directional_cooldown_until_ns[s][d];
                    self.wave_anchor_ts_ns[s][d] = if fb > 0 { fb } else { snap_ts };
                }
            }
        }
        if let Some(a) = val.get("global_dir_debt_bps_x100").and_then(serde_json::Value::as_array) {
            for (d, c) in a.iter().take(2).enumerate() {
                if let Some(n) = c.as_i64() {
                    self.global_dir_debt_bps_x100[d].store(n.clamp(-200_000, 200_000));
                }
            }
        }
        if let Some(a) = val.get("global_dir_sl_streak").and_then(serde_json::Value::as_array) {
            for (d, c) in a.iter().take(2).enumerate() { if let Some(n) = c.as_u64() { self.global_dir_sl_streak[d] = n.min(4) as u32; } }
        }
        if let Some(a) = val.get("global_dir_win_streak").and_then(serde_json::Value::as_array) {
            for (d, c) in a.iter().take(2).enumerate() { if let Some(n) = c.as_u64() { self.global_dir_win_streak[d] = n.min(6) as u32; } }
        }
        self.global_dir_cooldown_until_ns[0].store(0);
        self.global_dir_cooldown_until_ns[1].store(0);
        self.global_debt_decay_ts_ns[0].store(0);
        self.global_debt_decay_ts_ns[1].store(0);
        if let Some(a) = val.get("global_dir_cooldown_until_ns").and_then(serde_json::Value::as_array) {
            for (d, c) in a.iter().take(2).enumerate() {
                if let Some(n) = c.as_i64() {
                    self.global_dir_cooldown_until_ns[d].store(n.max(0));
                }
            }
        }
        if let Some(ld) = val.get("last_global_win_dir").and_then(serde_json::Value::as_u64) { self.last_global_win_dir = ld.min(255) as u8; }
        self.cross_chop_cooldown_until_ns = [0; MAX_ACTIVE_PAIRS];
        if let Some(cc) = val.get("cross_chop_cooldown_until_ns").and_then(serde_json::Value::as_array) {
            for (s, c) in cc.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
                if let Some(n) = c.as_i64() { self.cross_chop_cooldown_until_ns[s] = n.max(0); }
            }
        }
        self.abc_chop_last_sl_ts_ns = val.get("abc_chop_last_sl_ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0).max(0);
        self.abc_chop_sl_score_pm = val.get("abc_chop_sl_score_pm").and_then(serde_json::Value::as_i64).unwrap_or(0).clamp(0, 1_000);
        self.portfolio_ewma_net_bps_x100 = val
            .get("portfolio_ewma_net_bps_x100")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw())
            .clamp(-100_000, 100_000);
        self.plasticity.apply_from_scar_json(val);
        let _ = self.recompute_psi_health_in_place();
        Some(TimestampNs(snap_ts))
    }

    /// V4.7 Unified Governor Gate coupled to anti-spoof `GEOM` SNR, signed Epps beta & dynamic VIP `FeeSchedule`.
    #[must_use]
    pub fn authorizes_adaptive_entry_v47(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> bool {
        self.authorizes_adaptive_entry_v47_with_ignition(
            slot_idx,
            side,
            wallet_usdt_1e8,
            candidate_snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            0.0,
        )
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn authorizes_adaptive_entry_v47_with_ignition(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        lead_lag_ignition_bonus: f64,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS { return false; }
        if self.entry_warmup_until_ns > 0 && candidate_snap.ts_local_ns.0 > 0 && candidate_snap.ts_local_ns.0 < self.entry_warmup_until_ns { return false; }
        let entry_px = if candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0 {
            side.select_entry_bbo_price(candidate_snap.bids[0].price, candidate_snap.asks[0].price)
        } else { candidate_snap.mark_price };
        if entry_px.0 <= 0 || self.audit_historical_entry_veto(slot_idx, side, entry_px.0, candidate_snap.ts_local_ns.0).is_some() || !self.passes_cross_side_abc_chop_shield(slot_idx, side, candidate_snap) { return false; }
        let d = Self::dir_idx(side);
        let spec = APPROVED_SPECS[slot_idx];
        let fees = self.active_fee_schedule();
        let (own_q, opp_q, own_deep) = match side {
            Side::Bid => (candidate_snap.bids[0].qty, candidate_snap.asks[0].qty, candidate_snap.deep_bid_lots_l1_l5),
            Side::Ask => (candidate_snap.asks[0].qty, candidate_snap.bids[0].qty, candidate_snap.deep_ask_lots_l1_l5),
        };
        let is_penny_jump =
            Self::qualifies_inside_spread_penny_jump_c1(side, candidate_snap, self.psi_health());
        if !spec.passes_l1_queue_depth_and_wall_guard_penny_jump(
            entry_px, candidate_snap.realized_vol_5m, self.ewma_net_bps_x100[slot_idx][d],
            own_q, opp_q, own_deep, candidate_snap.factor_ofi.get().abs(), candidate_snap.factor_tfi.get().abs(),
            self.slot_taker_prob_permille(slot_idx, side), fees, is_penny_jump,
        ) { return false; }
        let (self_drift_bps, self_ext_bps) = self.price_drift_and_extension_scaled_bps(slot_idx, candidate_snap);
        let dir_s = side.sign_f64();
        let signed_geom = candidate_snap.factor_geom.get() * dir_s;
        if !spoof_density.is_finite() || !epps_signed_beta.is_finite() { return false; }
        let bonus = if lead_lag_ignition_bonus.is_finite() {
            lead_lag_ignition_bonus.clamp(0.0, crate::alpha_3factor::INV_PHI_CUBE)
        } else {
            0.0
        };
        let beta_c = if slot_idx == 0 { 0.0 } else { epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP) };
        let btc_iceberg_al = if slot_idx > 0 && candidate_snap.symbol_id != btc_leader_snap.symbol_id {
            (if beta_c < -1e-6 { -1.0 } else { 1.0 })
                * OrthogonalFactorState::snr_composite_score(btc_leader_snap)
                * dir_s
        } else {
            0.0
        };
        let s_iceberg = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
            candidate_snap.factor_ofi.get() * dir_s,
            candidate_snap.factor_tfi.get() * dir_s,
            btc_iceberg_al,
        );
        let base_cand_comp =
            OrthogonalFactorState::adaptive_snr_from_snapshot_with_fees(candidate_snap, spoof_density, fees)
                .get()
                .abs()
                * (1.0 - crate::alpha_3factor::INV_PHI * s_iceberg);
        let cand_comp = (base_cand_comp + bonus).min(1.0);
        let in_local_scar = self.is_in_local_slot_scar_debt(slot_idx, side);
        let in_scar = self.is_in_scar_debt(slot_idx, side);
        if !spec.passes_oversized_lot_conviction_guard_with_vol_and_fees(
            wallet_usdt_1e8, entry_px, candidate_snap.realized_vol_5m, cand_comp, in_local_scar, fees,
        ) { return false; }
        let Ok((raw_e_kin, base_e_min)) = self.evaluate_kinematic_energy_surface(slot_idx, side, self_drift_bps, self_ext_bps, signed_geom, cand_comp) else { return false; };
        let cadence = self.slot_temporal_cadence_for_side(slot_idx, Some(side), candidate_snap);
        let fee_rt = self.active_maker_rt_fee_bps();
        let is_multilot = self
            .compute_slot_entry_lots_v48(slot_idx, side, wallet_usdt_1e8, candidate_snap, 1_000)
            .0
            >= 2;
        let qstruct_kin_bonus = if is_multilot && s_iceberg <= 0.0 {
            let vac_l2_l3 = crate::exec_daemon::compute_vacuum_l2_l3_c1(side, candidate_snap);
            let q_ml = crate::types::compute_multilot_q_struct_c1(
                LotQty(2),
                0.0,
                candidate_snap.factor_ofi.get() * dir_s,
                candidate_snap.factor_tfi.get() * dir_s,
                vac_l2_l3,
            );
            let s_qml = crate::wave_elliott::hermite_smoothstep_c1(
                q_ml,
                crate::alpha_3factor::INV_PHI_SQ,
                crate::alpha_3factor::INV_PHI,
            );
            ((self.psi_health() * s_qml * (fee_rt as f64) * crate::alpha_3factor::INV_PHI_CUBE)
                .round() as i64)
                .max(0)
        } else {
            0
        };
        let kin_bonus = ((bonus * (fee_rt as f64) * crate::alpha_3factor::PHI).round() as i64)
            .max(0)
            .saturating_add(qstruct_kin_bonus);
        let e_kin = raw_e_kin.saturating_add(kin_bonus);
        let scar_rt = fee_rt.max(400);
        if e_kin < (base_e_min + cadence.kin_energy_min_delta_bps).clamp(fee_rt / 5, (scar_rt * 4) / 5) { return false; }
        let sl_s = self.sl_streak[slot_idx][d] as f64;
        let win_s = (self.win_streak[slot_idx][d].min(4)) as f64;
        let debt_bps = self.cum_net_bps_x100[slot_idx][d] as f64;
        let debt_penalty = if debt_bps < 0.0 { ((-debt_bps) / 16_000.0).min(0.28) } else { 0.0 };
        let global_debt_bps = self.global_dir_debt_bps_x100[d].load() as f64;
        let global_penalty = if global_debt_bps < -2_000.0 { ((-global_debt_bps - 2_000.0) / 35_000.0).min(0.14) } else { 0.0 };
        let staircase_penalty = if win_s >= 2.0 { 0.03 * (win_s - 1.0) } else { 0.0 };
        let ir_adj = (self.slot_information_ratio_x100(slot_idx, side) as f64 / 2_000.0).clamp(-0.04, 0.05);
        let preprint_ok = !in_local_scar && crate::types::compute_preprint_coherence_c1(candidate_snap.factor_ofi.get() * dir_s, signed_geom, candidate_snap.factor_tfi.get() * dir_s, spoof_density) > 0.0;
        let req_tfi_abs = if in_local_scar { 0.45 } else if preprint_ok { 0.0 } else { 0.38 };
        if in_scar && candidate_snap.factor_tfi.get().abs() < req_tfi_abs { return false; }
        if self.sl_streak[slot_idx][d] >= 1 && self.ewma_net_bps_x100[slot_idx][d] <= -800 && cand_comp < 0.72 { return false; }
        let beta_sign = if beta_c >= 0.0 { 1.0 } else { -1.0 };
        let btc_signed_snr = if slot_idx == 0 {
            OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s
        } else {
            beta_c * (OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s)
        };
        if slot_idx > 0 && beta_c.abs() >= 0.05 {
            let btc_ofi = btc_leader_snap.factor_ofi.get() * dir_s * beta_sign;
            let btc_tfi = btc_leader_snap.factor_tfi.get() * dir_s * beta_sign;
            if cand_comp >= 0.50 && !in_local_scar {
                if btc_signed_snr < -0.08 || (btc_ofi < -0.18 && btc_tfi < -0.18) { return false; }
            } else if btc_ofi < -0.05 || btc_tfi < -0.05 || btc_signed_snr < (0.08 + 0.08 * sl_s + 0.45 * (debt_penalty + global_penalty)) {
                return false;
            }
        }
        let Ok(wave_mod) = crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(
            candidate_snap.wave_observables(), side, btc_signed_snr,
        ) else { return false; };
        if self.audit_scar64_entry_and_sweep_shield(slot_idx, side, candidate_snap, btc_signed_snr).is_err() { return false; }
        let taker_pm = self.slot_taker_prob_permille(slot_idx, side);
        if opposing_ob_for_side(side, candidate_snap).vetoes_into_opposing_wall_dynamic(
            side, spec, entry_px, candidate_snap.realized_vol_5m, candidate_snap.factor_tfi.get() * dir_s, taker_pm, fees,
        ) { return false; }
        let ob_res = aligned_ob_for_side(side, candidate_snap)
            .aligned_confluence_resonance_dynamic(side, spec, entry_px, candidate_snap.realized_vol_5m, taker_pm, fees);
        let gp_res = candidate_snap.wave_observables().golden_pocket_resonance(side);
        let adaptive_threshold = (0.42 - (0.012 * ob_res + 0.014 * ob_res * gp_res)
            + 0.08 * sl_s + debt_penalty + global_penalty + staircase_penalty
            + wave_mod.snr_threshold_delta + cadence.snr_threshold_delta - ir_adj).clamp(0.36, 0.84);
        cand_comp >= adaptive_threshold
    }

    #[inline] #[must_use]
    pub fn authorizes_adaptive_entry_with_wallet(
        &self, slot_idx: usize, side: Side, wallet_usdt_1e8: i64, candidate_snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        self.authorizes_adaptive_entry_v47(
            slot_idx, side, wallet_usdt_1e8, candidate_snap, btc_leader_snap,
            0.0, if slot_idx == 0 { 0.0 } else { 1.0 },
        )
    }

    #[inline] #[must_use]
    pub fn max_total_entry_chase_ns(&self, slot_idx: usize, realized_vol_5m: ScaledBps) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = if slot_idx < MAX_ACTIVE_PAIRS { self.eq_vol_5m_bps[slot_idx].max(min_v * 2) } else { min_v * 4 };
        let base_flow_ns = VOL_5M_HORIZON_NS / 60;
        let ir_best = c_max_i64(self.slot_information_ratio_x100(slot_idx, Side::Bid), self.slot_information_ratio_x100(slot_idx, Side::Ask)).clamp(-40, 80);
        let raw_ns = (((base_flow_ns as i128 * eq_v as i128) / (realized_vol_5m.raw().max(min_v) as i128)) * ((1_000 + ir_best * 2) as i128)) / 1_000;
        (raw_ns as i64).clamp((base_flow_ns * 16) / 25, (base_flow_ns * 28) / 25)
    }

    /// V4.7 Preflight confirmation coupled to anti-spoof `GEOM` SNR & signed Epps beta.
    pub fn confirm_preflight_kinematics_v47(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> Result<(i64, i64), &'static str> {
        self.confirm_preflight_kinematics_v47_with_ignition(
            slot_idx,
            side,
            wallet_usdt_1e8,
            snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            0.0,
        )
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn confirm_preflight_kinematics_v47_with_ignition(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        lead_lag_ignition_bonus: f64,
    ) -> Result<(i64, i64), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS { return Err("VETO_INVALID_SLOT"); }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Err("VETO_PREFLIGHT_INVALID_BBO"); };
        let (ofi, tfi) = (snap.factor_ofi.get(), snap.factor_tfi.get());
        let fees = self.active_fee_schedule();
        let eta_lat = crate::alpha_3factor::compute_tick_lattice_granularity_eta(snap, fees);
        if crate::alpha_3factor::is_opposing_or_silent_spoof_veto_c1(
            ofi,
            tfi,
            eta_lat,
            crate::alpha_3factor::PHI,
        ) {
            return Err("VETO_PREFLIGHT_SPOOFING");
        }
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if let Some(veto) = self.audit_historical_entry_veto(slot_idx, side, entry_px.0, snap.ts_local_ns.0) { return Err(veto); }
        if !self.passes_cross_side_abc_chop_shield(slot_idx, side, snap) { return Err("VETO_CROSS_SIDE_ABC_CHOP_COOLDOWN"); }
        let dir_s = side.sign_f64();
        if (ofi * dir_s) < 0.08 { return Err("VETO_PREFLIGHT_OFI_DECAY"); }
        let bonus = if lead_lag_ignition_bonus.is_finite() {
            lead_lag_ignition_bonus.clamp(0.0, crate::alpha_3factor::INV_PHI_CUBE)
        } else {
            0.0
        };
        let beta_c = if slot_idx == 0 { 0.0 } else { epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP) };
        let btc_iceberg_al = if slot_idx > 0 && snap.symbol_id != btc_leader_snap.symbol_id {
            (if beta_c < -1e-6 { -1.0 } else { 1.0 })
                * OrthogonalFactorState::snr_composite_score(btc_leader_snap)
                * dir_s
        } else {
            0.0
        };
        let s_iceberg = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
            ofi * dir_s,
            tfi * dir_s,
            btc_iceberg_al,
        );
        let raw_signed_snr =
            OrthogonalFactorState::adaptive_snr_from_snapshot_with_fees(snap, spoof_density, fees)
                .get()
                * dir_s;
        let signed_snr = (raw_signed_snr * (1.0 - crate::alpha_3factor::INV_PHI * s_iceberg) + bonus).min(1.0);
        if signed_snr < 0.30 { return Err("VETO_PREFLIGHT_SNR_COLLAPSE"); }
        let spec = APPROVED_SPECS[slot_idx];
        if opposing_ob_for_side(side, snap).vetoes_into_opposing_wall_dynamic(
            side, spec, entry_px, snap.realized_vol_5m, tfi * dir_s, self.slot_taker_prob_permille(slot_idx, side), fees,
        ) { return Err("VETO_PREFLIGHT_OPPOSING_OB_WALL"); }
        if !spec.passes_oversized_lot_conviction_guard_with_vol_and_fees(
            wallet_usdt_1e8, entry_px, snap.realized_vol_5m, signed_snr, self.is_in_local_slot_scar_debt(slot_idx, side), fees,
        ) { return Err("VETO_PREFLIGHT_OVERSIZED_LOT"); }
        let signed_geom = snap.factor_geom.get() * dir_s;
        let (own_q, opp_q) = match side { Side::Bid => (best_bid.qty.0, best_ask.qty.0), Side::Ask => (best_ask.qty.0, best_bid.qty.0) };
        let min_pre_geom = (0.06 - (signed_snr - 0.42) * 0.45).clamp(if signed_snr >= 0.68 && own_q >= opp_q { 0.00 } else { 0.02 }, 0.06);
        if signed_geom < min_pre_geom { return Err("VETO_PREFLIGHT_GEOM_REVERSAL"); }
        let btc_snr = if slot_idx == 0 {
            OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s
        } else {
            beta_c * (OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s)
        };
        if slot_idx > 0 && beta_c.abs() >= 0.05 && btc_snr < -0.08 && signed_snr < 0.62 {
            return Err("VETO_PREFLIGHT_BTC_HEADWIND");
        }
        self.audit_scar64_entry_and_sweep_shield(slot_idx, side, snap, btc_snr)?;
        let _ = crate::wave_elliott::CausalElliottWaveState::evaluate_wave_entry_modulation(snap.wave_observables(), side, btc_snr)?;
        let (drift_bps, ext_bps) = self.price_drift_and_extension_scaled_bps(slot_idx, snap);
        let fee_rt = self.active_maker_rt_fee_bps();
        let is_multilot = self
            .compute_slot_entry_lots_v48(slot_idx, side, wallet_usdt_1e8, snap, 1_000)
            .0
            >= 2;
        let qstruct_kin_bonus = if is_multilot && s_iceberg <= 0.0 {
            let vac_l2_l3 = crate::exec_daemon::compute_vacuum_l2_l3_c1(side, snap);
            let q_ml = crate::types::compute_multilot_q_struct_c1(
                LotQty(2),
                0.0,
                ofi * dir_s,
                tfi * dir_s,
                vac_l2_l3,
            );
            let s_qml = crate::wave_elliott::hermite_smoothstep_c1(
                q_ml,
                crate::alpha_3factor::INV_PHI_SQ,
                crate::alpha_3factor::INV_PHI,
            );
            ((self.psi_health() * s_qml * (fee_rt as f64) * crate::alpha_3factor::INV_PHI_CUBE)
                .round() as i64)
                .max(0)
        } else {
            0
        };
        let kin_bonus = ((bonus * (fee_rt as f64) * crate::alpha_3factor::PHI).round() as i64)
            .max(0)
            .saturating_add(qstruct_kin_bonus);
        if let Ok((ek, emin)) = self.evaluate_kinematic_energy_surface(slot_idx, side, drift_bps, ext_bps, signed_geom, signed_snr) {
            return Ok((ek.saturating_add(kin_bonus), emin));
        }
        let d = Self::dir_idx(side);
        evaluate_preflight_hysteresis_with_fees(
            side, self.eq_vol_5m_bps[slot_idx], self.ewma_net_bps_x100[slot_idx][d],
            self.slot_information_ratio_x100(slot_idx, side), drift_bps, ext_bps, signed_geom, min_pre_geom, signed_snr, fees,
        )
        .map(|(ek, emin)| (ek.saturating_add(kin_bonus), emin))
    }

    #[inline]
    pub fn confirm_preflight_kinematics_non_choking(
        &self, slot_idx: usize, side: Side, wallet_usdt_1e8: i64, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot,
    ) -> Result<(i64, i64), &'static str> {
        self.confirm_preflight_kinematics_v47(
            slot_idx, side, wallet_usdt_1e8, snap, btc_leader_snap,
            0.0, if slot_idx == 0 { 0.0 } else { 1.0 },
        )
    }

    #[inline] #[must_use]
    pub fn parse_historical_trade_scar_tuple(val: &serde_json::Value, min_exclusive_ts_ns: i64) -> Option<(ScarEventKey, ScarEventPayload)> {
        parse_scar_tuple_core(val, min_exclusive_ts_ns)
    }

    pub fn rehydrate_from_scar_and_loss_files(
        &mut self, scar_path: &std::path::Path, red_loss_path: &std::path::Path, session_vault_path: Option<&std::path::Path>,
    ) -> (i64, usize) {
        use std::io::BufRead;
        let mut epoch_reset_ts_ns = 0_i64;
        if let Some(vp) = session_vault_path {
            if let Ok(vf) = std::fs::File::open(vp) {
                for ln in std::io::BufReader::new(vf).lines().map_while(Result::ok) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&ln) {
                        if v.get("event").and_then(serde_json::Value::as_str) == Some("EPOCH_RESET") {
                            epoch_reset_ts_ns = epoch_reset_ts_ns.max(v.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0));
                        }
                    }
                }
            }
        }
        let mut snap_ts_ns = epoch_reset_ts_ns;
        let mut plasticity_snap_ts_ns = 0_i64;
        if let Ok(s_file) = std::fs::File::open(scar_path) {
            let mut latest_snap: Option<serde_json::Value> = None;
            for line in std::io::BufReader::new(s_file).lines().map_while(Result::ok) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    if val.get("event").and_then(serde_json::Value::as_str) == Some("GOV_SCAR_SNAPSHOT") { latest_snap = Some(val); }
                }
            }
            if let Some(ref sv) = latest_snap {
                let sv_ts = sv.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
                if sv_ts >= epoch_reset_ts_ns {
                    if let Some(restored) = self.apply_scar_snapshot_json(sv) {
                        snap_ts_ns = snap_ts_ns.max(restored.0);
                        plasticity_snap_ts_ns = restored.0;
                    }
                } else {
                    self.plasticity.apply_from_scar_json(sv);
                    plasticity_snap_ts_ns = sv_ts;
                }
            }
        }
        let mut all_dedup: std::collections::BTreeMap<ScarEventKey, ScarEventPayload> = std::collections::BTreeMap::new();
        let mut paths = [scar_path, red_loss_path, scar_path];
        let cnt = if let Some(vp) = session_vault_path { paths[2] = vp; 3_usize } else { 2_usize };
        for p in &paths[..cnt] {
            let Ok(file) = std::fs::File::open(p) else { continue };
            for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    if let Some((k, pl)) = Self::parse_historical_trade_scar_tuple(&val, 0) {
                        all_dedup.entry(k).or_insert(pl);
                    }
                }
            }
        }
        // Preserve multi-epoch Bayesian Taker priors across EPOCH_RESET (never suffer prior amnesia):
        if epoch_reset_ts_ns > 0 {
            let fee_rt = self.active_maker_rt_fee_bps();
            for (&(ts_ns, s_idx, d_u8), &(_, net_sc, mkr)) in &all_dedup {
                if ts_ns > plasticity_snap_ts_ns && ts_ns <= snap_ts_ns && s_idx < MAX_ACTIVE_PAIRS {
                    let side = if d_u8 == 1 { Side::Ask } else { Side::Bid };
                    let adv = !mkr && net_sc.raw() < 0;
                    let mfe_est = if net_sc.raw() > 0 { ScaledBps(net_sc.raw().saturating_add(fee_rt)) } else { ScaledBps(0) };
                    self.plasticity.observe_exit_outcome_with_net_and_mfe(
                        s_idx, side, adv, net_sc, mfe_est, ts_ns, self.eq_vol_5m_bps[s_idx],
                    );
                }
            }
        }
        let mut max_ts = snap_ts_ns;
        let mut ingested = 0_usize;
        for (&(ts_ns, s_idx, d_u8), &(en_t, net_sc, mkr)) in &all_dedup {
            if ts_ns > snap_ts_ns {
                ingested += 1;
                self.observe_closed_trade_at_entry(s_idx, if d_u8 == 1 { Side::Ask } else { Side::Bid }, en_t, net_sc, mkr, TimestampNs(ts_ns));
                if ts_ns > max_ts { max_ts = ts_ns; }
            }
        }
        (max_ts, ingested)
    }

    #[inline] #[must_use]
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
    fn test_v47_governor_directional_ob_anti_spoof_and_inverse_epps_beta() {
        assert_eq!(OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(), (UNIFIED_LOSS_LAB_COUNT, 2_160));
        let mut gov = OnlineAdaptiveGovernor::new();
        gov.set_active_fee_schedule(FeeSchedule::from_live_rates_clamped(ScaledBps(100), ScaledBps(350)));
        let t0 = 1_762_992_000_000_000_000_i64 + 14 * 3_600_000_000_000 + 1_800_000_000_000;
        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = TimestampNs(t0);
        btc.realized_vol_5m = ScaledBps(5_200);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(450) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(350) };
        btc.refresh_deep_l1_l5_totals();
        btc.factor_ofi = BoundedScore::new_clamped(0.65);
        btc.factor_tfi = BoundedScore::new_clamped(0.65);
        btc.factor_geom = BoundedScore::new_clamped(0.35);

        let mut avax = btc;
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.bids[0] = BookLevel { price: PriceTicks(8_170), qty: LotQty(500) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_171), qty: LotQty(380) };
        avax.refresh_deep_l1_l5_totals();
        avax.set_order_block_observables_directional(
            OrderBlockObservables {
                bull_ob_score: BoundedScore::new_clamped(0.46),
                bear_ob_score: BoundedScore::new_clamped(0.60),
                dist_bull_ob_bps: ScaledBps(1_250),
                dist_bear_ob_bps: ScaledBps(1_250),
                bull_wall_ticks: PriceTicks(8_068),
                bear_wall_ticks: PriceTicks(8_272),
                learned_efficacy_pm: 618,
            },
            382,
            854,
        );
        assert!(!gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, 6_628_080_000, &avax, &btc));

        // Inverse Epps beta (-0.85): when BTC dumps (-0.65), inverse-correlated AVAX Bid hedge is AUTHORIZED!
        let mut avax_hedge = avax;
        avax_hedge.set_order_block_observables_directional(OrderBlockObservables::default(), 618, 618);
        btc.factor_ofi = BoundedScore::new_clamped(-0.65);
        btc.factor_tfi = BoundedScore::new_clamped(-0.65);
        btc.factor_geom = BoundedScore::new_clamped(-0.35);
        assert!(!gov.authorizes_adaptive_entry_v47(3, Side::Bid, 6_628_080_000, &avax_hedge, &btc, 0.0, 0.85));
        assert!(gov.authorizes_adaptive_entry_v47(3, Side::Bid, 6_628_080_000, &avax_hedge, &btc, 0.0, -0.85));
        assert!(gov.confirm_preflight_kinematics_v47(3, Side::Bid, 6_628_080_000, &avax_hedge, &btc, 0.0, -0.85).is_ok());
    }
}

impl OnlineAdaptiveGovernor {
    /// O(1) portfolio-wide mean equilibrium 5m volatility in ScaledBps for `DailySessionGovernor`.
    #[inline]
    #[must_use]
    pub fn portfolio_mean_eq_vol_bps(&self) -> i64 {
        let mut sum = 0_i64;
        for &v in &self.eq_vol_5m_bps {
            sum = sum.saturating_add(v.max(1_200));
        }
        sum / (MAX_ACTIVE_PAIRS as i64)
    }

    /// O(1) portfolio-wide mean directional Information Ratio (`x100`) across all 24 `(slot, side)` cells.
    #[inline]
    #[must_use]
    pub fn portfolio_mean_ir_x100(&self) -> i64 {
        let mut sum = 0_i64;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            sum = sum.saturating_add(self.slot_information_ratio_x100(s_idx, Side::Bid));
            sum = sum.saturating_add(self.slot_information_ratio_x100(s_idx, Side::Ask));
        }
        sum / ((MAX_ACTIVE_PAIRS * 2) as i64)
    }

    /// V4.8 Kelly lot sizer coupled to the C1 daily boundary damper `m_day(t) in [618..=1_000] permille`.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn compute_slot_entry_lots_v48(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        snap: &SymbolLobSnapshot,
        daily_damper_permille: i64,
    ) -> LotQty {
        let s_clamped = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_clamped];
        let entry_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price)
        } else {
            snap.mark_price
        };
        let cadence = self.slot_temporal_cadence_for_side(s_clamped, Some(side), snap);
        let storm_onset = (ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4).max(4_800) as f64;
        let eq_v = self.eq_vol_5m_bps[s_clamped].max(1_200);
        let cur_v = snap.realized_vol_5m.raw().max(1_200);
        let h_storm = crate::wave_elliott::hermite_smoothstep_c1((eq_v as f64) / storm_onset, 1.0, 1.618_033_988_749_895);
        let eff_kelly_vol = ScaledBps(if cur_v < eq_v && h_storm > 0.0 {
            cur_v + (((eq_v - cur_v) as f64 * h_storm).round() as i64)
        } else {
            cur_v
        });
        let coil_damper_pm = if cur_v < eq_v && h_storm > 0.0 {
            let rho_lull = 1.0 - (cur_v as f64) / (eq_v as f64);
            let h_coil = h_storm * crate::wave_elliott::hermite_smoothstep_c1(rho_lull, 0.381_966_011_250_105, 0.763_932_022_500_210);
            (1_000.0 - 382.0 * h_coil).round() as i64
        } else {
            1_000
        };
        let damper_floor = if daily_damper_permille < 618 { 382 } else { 618 };
        let cadence_floor = if daily_damper_permille < 618 { 382 } else { 550 };
        let damper_c = ((daily_damper_permille.clamp(damper_floor, 1_000) * coil_damper_pm) / 1_000).clamp(damper_floor, 1_000);
        let eff_cadence_pm = ((cadence.kelly_lot_mult_permille * damper_c) / 1_000).clamp(cadence_floor, 1_200);
        let (edge_bps, _, _) = self.slot_direction_diagnostics(s_clamped, side);
        spec.compute_risk_parity_kelly_lots_with_fees(
            wallet_usdt_1e8,
            entry_px,
            eff_kelly_vol,
            edge_bps,
            self.slot_information_ratio_x100(s_clamped, side),
            eff_cadence_pm,
            self.is_in_scar_debt(s_clamped, side),
            self.active_fee_schedule(),
        )
    }

    /// V4.8 Unified Governor Gate: enforces endogenous quantum Kelly log-growth (`Delta ln W > 0`) + V4.7 shields.
    #[inline]
    #[must_use]
    pub fn authorizes_adaptive_entry_v48(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        learned_mfe_bps: ScaledBps,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let entry_px = if candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0 {
            side.select_entry_bbo_price(candidate_snap.bids[0].price, candidate_snap.asks[0].price)
        } else {
            candidate_snap.mark_price
        };
        if entry_px.0 <= 0 {
            return false;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let taker_pm = self.slot_taker_prob_permille(slot_idx, side);
        if !spec.passes_quantum_ev_growth_gate(
            wallet_usdt_1e8, entry_px, candidate_snap.realized_vol_5m,
            learned_mfe_bps, taker_pm, self.active_fee_schedule(),
        ) {
            return false;
        }
        self.authorizes_adaptive_entry_v47(
            slot_idx, side, wallet_usdt_1e8, candidate_snap, btc_leader_snap, spoof_density, epps_signed_beta,
        )
    }

    /// V4.8 Preflight confirmation: re-verifies quantum EV log-growth before releasing Maker order to wire.
    #[inline]
    pub fn confirm_preflight_kinematics_v48(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> Result<(i64, i64), &'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Err("VETO_INVALID_SLOT");
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err("VETO_PREFLIGHT_INVALID_BBO");
        };
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let spec = APPROVED_SPECS[slot_idx];
        if !spec.passes_quantum_ev_growth_gate(
            wallet_usdt_1e8, entry_px, snap.realized_vol_5m,
            learned_mfe_bps, self.slot_taker_prob_permille(slot_idx, side), self.active_fee_schedule(),
        ) {
            return Err("VETO_PREFLIGHT_QUANTUM_EV_GROWTH");
        }
        self.confirm_preflight_kinematics_v47(
            slot_idx, side, wallet_usdt_1e8, snap, btc_leader_snap, spoof_density, epps_signed_beta,
        )
    }
}

#[cfg(test)]
mod v48_governor_quantum_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_v48_governor_blocks_trade13_oversized_btc_and_damps_kelly_near_daily_boundary() {
        let gov = OnlineAdaptiveGovernor::new();
        let t0 = 1_762_992_000_000_000_000_i64 + 14 * 3_600_000_000_000;
        let w_65 = 6_513_770_000_i64;

        let mut btc_t13 = SymbolLobSnapshot::new_empty();
        btc_t13.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_t13.ts_local_ns = TimestampNs(t0);
        btc_t13.realized_vol_5m = ScaledBps(3_069);
        btc_t13.bids[0] = BookLevel { price: PriceTicks(646_312), qty: LotQty(895) };
        btc_t13.asks[0] = BookLevel { price: PriceTicks(646_313), qty: LotQty(1_200) };
        btc_t13.refresh_deep_l1_l5_totals();
        btc_t13.factor_ofi = BoundedScore::new_clamped(-0.73);
        btc_t13.factor_tfi = BoundedScore::new_clamped(-0.88);
        btc_t13.factor_geom = BoundedScore::new_clamped(-1.00);
        assert!(!gov.authorizes_adaptive_entry_v48(0, Side::Ask, w_65, ScaledBps(568), &btc_t13, &btc_t13, 0.0, 0.0));
        assert_eq!(
            gov.confirm_preflight_kinematics_v48(0, Side::Ask, w_65, ScaledBps(568), &btc_t13, &btc_t13, 0.0, 0.0),
            Err("VETO_PREFLIGHT_QUANTUM_EV_GROWTH")
        );

        let mut avax = btc_t13;
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.realized_vol_5m = ScaledBps(3_600);
        avax.bids[0] = BookLevel { price: PriceTicks(7_263), qty: LotQty(5_506) };
        avax.asks[0] = BookLevel { price: PriceTicks(7_264), qty: LotQty(3_200) };
        avax.refresh_deep_l1_l5_totals();
        avax.factor_ofi = BoundedScore::new_clamped(0.81);
        avax.factor_tfi = BoundedScore::new_clamped(0.98);
        avax.factor_geom = BoundedScore::new_clamped(0.45);
        let mut btc_bull = btc_t13;
        btc_bull.factor_ofi = BoundedScore::new_clamped(0.65);
        btc_bull.factor_tfi = BoundedScore::new_clamped(0.70);
        btc_bull.factor_geom = BoundedScore::new_clamped(0.40);
        assert!(gov.authorizes_adaptive_entry_v48(3, Side::Bid, w_65, ScaledBps(628), &avax, &btc_bull, 0.0, 1.0));

        let lots_full = gov.compute_slot_entry_lots_v48(3, Side::Bid, 50_000_000_000, &avax, 1_000);
        let lots_damped = gov.compute_slot_entry_lots_v48(3, Side::Bid, 50_000_000_000, &avax, 618);
        assert!(lots_damped.0 < lots_full.0 && lots_damped.0 >= 2 && gov.portfolio_mean_eq_vol_bps() >= 1_200);
    }
}

/// Orthogonal MFE plasticity predicate: prevents zero-MFE Stop-Loss sweeps from shrinking future profit targets.
#[inline]
#[must_use]
pub const fn should_update_mfe_plasticity(
    is_maker_exit: bool,
    observed_mfe_sbps: i64,
    fee_rt_mkr_sbps: i64,
) -> bool {
    let floor_sbps = if fee_rt_mkr_sbps > 100 {
        fee_rt_mkr_sbps
    } else {
        100
    };
    is_maker_exit || observed_mfe_sbps > floor_sbps
}

#[inline]
#[must_use]
fn clamp_ob_permille_u16(raw_pm: i64) -> u16 {
    let bounded = if raw_pm <= 0 {
        618
    } else if raw_pm > 1_000 {
        1_000
    } else {
        raw_pm
    };
    u16::try_from(bounded).unwrap_or(618)
}

impl OnlineAdaptiveGovernor {
    /// Unified V4.9 entry gate returning allocated `LotQty` or exact `LastVetoCode` (`EV > 0` enforced).
    ///
    /// # Errors
    /// Returns the causal `LastVetoCode` when quantum EV, `ImpulseWave3`, `GEOM` or single-lot asymmetry fails.
    #[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
    pub fn authorizes_adaptive_entry_v49(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        daily_damper_permille: i64,
        learned_mfe_bps: ScaledBps,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> Result<LotQty, crate::lob_shm::LastVetoCode> {
        let is_follower = slot_idx > 0 && candidate_snap.symbol_id != btc_leader_snap.symbol_id;
        let implied_corr = if is_follower {
            let own_v = candidate_snap.realized_vol_5m.raw().max(100) as f64;
            let btc_v = btc_leader_snap.realized_vol_5m.raw().max(100) as f64;
            (epps_signed_beta * (btc_v / own_v)).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        self.authorizes_adaptive_entry_v54(
            slot_idx,
            side,
            wallet_usdt_1e8,
            daily_damper_permille,
            learned_mfe_bps,
            candidate_snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            implied_corr,
        )
    }

    /// Unified V5.4 entry gate coupling indivisible `1-lot` EV to `OFI + BTC` propulsion and `Epps + BTC` `CorrectiveAbc` sync.
    ///
    /// # Errors
    /// Returns the causal `LastVetoCode` when quantum EV, `ImpulseWave3`, `GEOM` or Pillar-2 EV fails.
    #[allow(clippy::too_many_arguments)]
    pub fn authorizes_adaptive_entry_v54(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        daily_damper_permille: i64,
        learned_mfe_bps: ScaledBps,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
    ) -> Result<LotQty, crate::lob_shm::LastVetoCode> {
        self.authorizes_adaptive_entry_v542(
            slot_idx,
            side,
            wallet_usdt_1e8,
            daily_damper_permille,
            learned_mfe_bps,
            candidate_snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            None,
        )
    }

    /// Unified V5.4.2 entry gate coupling `psi_health`, `CorrectiveAbc` `chi_threat`, `BTC` lead-lag memory & Pillar-2 EV.
    ///
    /// # Errors
    /// Returns the causal `LastVetoCode` when quantum EV, `ImpulseWave3`, `GEOM` or Pillar-2 EV fails.
    #[allow(clippy::too_many_arguments)]
    pub fn authorizes_adaptive_entry_v542(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        daily_damper_permille: i64,
        learned_mfe_bps: ScaledBps,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
    ) -> Result<LotQty, crate::lob_shm::LastVetoCode> {
        self.authorizes_adaptive_entry_v55(
            slot_idx,
            side,
            wallet_usdt_1e8,
            daily_damper_permille,
            learned_mfe_bps,
            candidate_snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            0.0,
        )
    }

    /// Unified V5.5 entry gate coupling unified-horizon `BTC` lead-lag ignition bonus (`O2`),
    /// `psi_health`, `CorrectiveAbc` `chi_threat`, and Pillar-2 EV.
    ///
    /// # Errors
    /// Returns the causal `LastVetoCode` when quantum EV, `ImpulseWave3`, `GEOM` or Pillar-2 EV fails.
    #[allow(clippy::too_many_arguments)]
    pub fn authorizes_adaptive_entry_v55(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        daily_damper_permille: i64,
        learned_mfe_bps: ScaledBps,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        lead_lag_ignition_bonus: f64,
    ) -> Result<LotQty, crate::lob_shm::LastVetoCode> {
        use crate::lob_shm::LastVetoCode;

        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Err(LastVetoCode::WarmupOrLock);
        }
        let entry_px = if candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0 {
            side.select_entry_bbo_price(candidate_snap.bids[0].price, candidate_snap.asks[0].price)
        } else {
            candidate_snap.mark_price
        };
        if entry_px.0 <= 0 {
            return Err(LastVetoCode::WarmupOrLock);
        }
        let spec = APPROVED_SPECS[slot_idx];
        let fees = self.active_fee_schedule();
        let taker_pm = self.slot_taker_prob_permille(slot_idx, side);
        if !spec.passes_quantum_ev_growth_gate(
            wallet_usdt_1e8,
            entry_px,
            candidate_snap.realized_vol_5m,
            learned_mfe_bps,
            taker_pm,
            fees,
        ) {
            return Err(LastVetoCode::QueueDepthOrOversizedLot);
        }
        if !self.passes_elastic_funding_cliff_guard(slot_idx, side, learned_mfe_bps, candidate_snap) {
            return Err(LastVetoCode::KinematicSurfaceOrCadence);
        }
        let is_follower = slot_idx > 0 && candidate_snap.symbol_id != btc_leader_snap.symbol_id;
        let btc_w3_aligned = crate::alpha_3factor::compute_btc_leader_aligned_score(
            side,
            candidate_snap,
            btc_leader_snap,
            if is_follower { epps_corr } else { 0.0 },
            btc_lead_aligned_override,
        );
        if crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
            side,
            candidate_snap,
            btc_w3_aligned,
            self.psi_health(),
            self.eq_vol_5m_bps[slot_idx],
            taker_pm,
            fees,
        ) {
            return Err(LastVetoCode::MacroElliottContradiction);
        }
        if !crate::alpha_3factor::passes_elastic_anti_spoof_ofi_guard(
            side,
            candidate_snap,
            spoof_density,
            taker_pm,
            self.eq_vol_5m_bps[slot_idx],
            fees,
        ) {
            return Err(LastVetoCode::SpoofingVeto);
        }
        crate::alpha_3factor::passes_elastic_geom_and_macro_impulse_guard_with_health(
            candidate_snap,
            btc_leader_snap,
            side,
            spoof_density,
            epps_corr,
            btc_lead_aligned_override,
            self.psi_health(),
        )?;
        let lots = self.compute_slot_entry_lots_v48(
            slot_idx,
            side,
            wallet_usdt_1e8,
            candidate_snap,
            daily_damper_permille,
        );
        let d_entry = Self::dir_idx(side);
        let tau_mkt_s = crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(candidate_snap.realized_vol_5m);
        let kappa_evt = if self.last_ws_rtt_us > 0 { 1.0 } else { crate::alpha_3factor::INV_PHI };
        let target_excursion_sbps = match OrthogonalFactorState::evaluate_entry_signal_v554(
            spec,
            candidate_snap,
            btc_leader_snap,
            learned_mfe_bps,
            ScaledBps(self.ewma_net_bps_x100[slot_idx][d_entry]),
            taker_pm,
            self.slot_information_ratio_x100(slot_idx, side),
            tau_mkt_s,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            self.psi_health(),
            lead_lag_ignition_bonus,
            kappa_evt,
        ) {
            crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
                side: sig_side,
                target_excursion_scaled_bps,
            } if sig_side == side => target_excursion_scaled_bps,
            _ => ScaledBps(0),
        };
        if self
            .evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                slot_idx,
                side,
                lots,
                entry_px,
                learned_mfe_bps,
                target_excursion_sbps,
                candidate_snap,
                btc_leader_snap,
                epps_signed_beta,
                spoof_density,
            )
            .is_none()
        {
            return Err(LastVetoCode::QueueDepthOrOversizedLot);
        }
        if !self.authorizes_adaptive_entry_v47_with_ignition(
            slot_idx,
            side,
            wallet_usdt_1e8,
            candidate_snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            lead_lag_ignition_bonus,
        ) {
            return Err(LastVetoCode::KinematicSurfaceOrCadence);
        }
        Ok(lots)
    }

    /// Unified V4.9 preflight confirmation re-verifying book support, macro wave & single-lot EV before wire send.
    ///
    /// # Errors
    /// Returns `LastVetoCode` if BBO, `ImpulseWave3`, `GEOM`, single-lot EV or preflight kinematics decay.
    #[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
    pub fn confirm_preflight_kinematics_v49(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        allocated_lots: LotQty,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
    ) -> Result<(i64, i64), crate::lob_shm::LastVetoCode> {
        let is_follower = slot_idx > 0 && snap.symbol_id != btc_leader_snap.symbol_id;
        let implied_corr = if is_follower {
            let own_v = snap.realized_vol_5m.raw().max(100) as f64;
            let btc_v = btc_leader_snap.realized_vol_5m.raw().max(100) as f64;
            (epps_signed_beta * (btc_v / own_v)).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        self.confirm_preflight_kinematics_v54(
            slot_idx,
            side,
            wallet_usdt_1e8,
            allocated_lots,
            learned_mfe_bps,
            snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            implied_corr,
        )
    }

    /// Unified V5.4 preflight confirmation coupling `OFI + BTC` 1-lot EV and `Epps + BTC` `CorrectiveAbc` sync.
    ///
    /// # Errors
    /// Returns `LastVetoCode` if BBO, `ImpulseWave3`, `GEOM`, Pillar-2 EV or preflight kinematics decay.
    #[allow(clippy::too_many_arguments)]
    pub fn confirm_preflight_kinematics_v54(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        allocated_lots: LotQty,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
    ) -> Result<(i64, i64), crate::lob_shm::LastVetoCode> {
        self.confirm_preflight_kinematics_v542(
            slot_idx,
            side,
            wallet_usdt_1e8,
            allocated_lots,
            learned_mfe_bps,
            snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            None,
        )
    }

    /// Unified V5.4.2 preflight confirmation coupling `psi_health`, `BTC` lead-lag memory & Pillar-2 EV.
    ///
    /// # Errors
    /// Returns `LastVetoCode` if BBO, `ImpulseWave3`, `GEOM`, Pillar-2 EV or preflight kinematics decay.
    #[allow(clippy::too_many_arguments)]
    pub fn confirm_preflight_kinematics_v542(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        allocated_lots: LotQty,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
    ) -> Result<(i64, i64), crate::lob_shm::LastVetoCode> {
        self.confirm_preflight_kinematics_v55(
            slot_idx,
            side,
            wallet_usdt_1e8,
            allocated_lots,
            learned_mfe_bps,
            snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            0.0,
        )
    }

    /// Unified V5.5 preflight confirmation coupling unified-horizon `BTC` lead-lag ignition bonus (`O2`),
    /// `psi_health`, `BTC` lead-lag memory & Pillar-2 EV.
    ///
    /// # Errors
    /// Returns `LastVetoCode` if BBO, `ImpulseWave3`, `GEOM`, Pillar-2 EV or preflight kinematics decay.
    #[allow(clippy::too_many_arguments)]
    pub fn confirm_preflight_kinematics_v55(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        allocated_lots: LotQty,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        spoof_density: f64,
        epps_signed_beta: f64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        lead_lag_ignition_bonus: f64,
    ) -> Result<(i64, i64), crate::lob_shm::LastVetoCode> {
        use crate::lob_shm::LastVetoCode;

        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Err(LastVetoCode::WarmupOrLock);
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err(LastVetoCode::WarmupOrLock);
        };
        if !self.passes_elastic_funding_cliff_guard(slot_idx, side, learned_mfe_bps, snap) {
            return Err(LastVetoCode::PreflightDriftOrDecay);
        }
        let taker_pm = self.slot_taker_prob_permille(slot_idx, side);
        let fees = self.active_fee_schedule();
        let is_follower = slot_idx > 0 && snap.symbol_id != btc_leader_snap.symbol_id;
        let btc_w3_aligned = crate::alpha_3factor::compute_btc_leader_aligned_score(
            side,
            snap,
            btc_leader_snap,
            if is_follower { epps_corr } else { 0.0 },
            btc_lead_aligned_override,
        );
        if crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
            side,
            snap,
            btc_w3_aligned,
            self.psi_health(),
            self.eq_vol_5m_bps[slot_idx],
            taker_pm,
            fees,
        ) {
            return Err(LastVetoCode::MacroElliottContradiction);
        }
        if !crate::alpha_3factor::passes_elastic_anti_spoof_ofi_guard(
            side,
            snap,
            spoof_density,
            taker_pm,
            self.eq_vol_5m_bps[slot_idx],
            fees,
        ) {
            return Err(LastVetoCode::SpoofingVeto);
        }
        crate::alpha_3factor::passes_elastic_geom_and_macro_impulse_guard_with_health(
            snap,
            btc_leader_snap,
            side,
            spoof_density,
            epps_corr,
            btc_lead_aligned_override,
            self.psi_health(),
        )?;
        let entry_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let spec = APPROVED_SPECS[slot_idx];
        if !spec.passes_quantum_ev_growth_gate(
            wallet_usdt_1e8,
            entry_px,
            snap.realized_vol_5m,
            learned_mfe_bps,
            taker_pm,
            fees,
        ) {
            return Err(LastVetoCode::QueueDepthOrOversizedLot);
        }
        if !self.passes_elastic_preflight_l1_support(slot_idx, side, entry_px, snap) {
            return Err(LastVetoCode::QueueDepthOrOversizedLot);
        }
        let d_pf = Self::dir_idx(side);
        let tau_mkt_pf_s = crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(snap.realized_vol_5m);
        let kappa_pf_evt = if self.last_ws_rtt_us > 0 { 1.0 } else { crate::alpha_3factor::INV_PHI };
        let target_excursion_sbps = match OrthogonalFactorState::evaluate_entry_signal_v554(
            spec,
            snap,
            btc_leader_snap,
            learned_mfe_bps,
            ScaledBps(self.ewma_net_bps_x100[slot_idx][d_pf]),
            taker_pm,
            self.slot_information_ratio_x100(slot_idx, side),
            tau_mkt_pf_s,
            fees,
            spoof_density,
            epps_signed_beta,
            epps_corr,
            btc_lead_aligned_override,
            self.psi_health(),
            lead_lag_ignition_bonus,
            kappa_pf_evt,
        ) {
            crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
                side: sig_side,
                target_excursion_scaled_bps,
            } if sig_side == side => target_excursion_scaled_bps,
            _ => ScaledBps(0),
        };
        if self
            .evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                slot_idx,
                side,
                allocated_lots,
                entry_px,
                learned_mfe_bps,
                target_excursion_sbps,
                snap,
                btc_leader_snap,
                epps_signed_beta,
                spoof_density,
            )
            .is_none()
        {
            return Err(LastVetoCode::QueueDepthOrOversizedLot);
        }
        self.confirm_preflight_kinematics_v47_with_ignition(
            slot_idx,
            side,
            wallet_usdt_1e8,
            snap,
            btc_leader_snap,
            spoof_density,
            epps_signed_beta,
            lead_lag_ignition_bonus,
        )
        .map_err(|_| LastVetoCode::PreflightDriftOrDecay)
    }
}

#[cfg(test)]
mod v49_governor_asymmetry_tests {
    use super::*;
    use crate::lob_shm::{BookLevel, LastVetoCode};
    use crate::types::BoundedScore;
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_v49_mfe_orthogonality_and_unified_governor_blocks_tao17_eth18() {
        assert!(!should_update_mfe_plasticity(false, 0, 400));
        assert!(!should_update_mfe_plasticity(false, 5, 400));
        assert!(should_update_mfe_plasticity(true, 906, 400));
        assert!(should_update_mfe_plasticity(false, 850, 400));

        let gov = OnlineAdaptiveGovernor::new();
        let t0 = 1_762_992_000_000_000_000_i64 + 14 * 3_600_000_000_000;
        let w_65 = 6_502_850_000_i64;

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.ts_local_ns = TimestampNs(t0);
        btc.realized_vol_5m = ScaledBps(3_000);
        btc.bids[0] = BookLevel { price: PriceTicks(646_300), qty: LotQty(1_500) };
        btc.asks[0] = BookLevel { price: PriceTicks(646_301), qty: LotQty(1_500) };
        btc.refresh_deep_l1_l5_totals();
        btc.factor_ofi = BoundedScore::new_clamped(-0.65);
        btc.factor_tfi = BoundedScore::new_clamped(-0.70);
        btc.factor_geom = BoundedScore::new_clamped(-0.60);

        // TAO #17 Ask into bullish ImpulseWave3 (+0.90) -> MacroElliottContradiction.
        let mut tao17 = btc;
        tao17.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao17.realized_vol_5m = ScaledBps(2_600);
        tao17.bids[0] = BookLevel { price: PriceTicks(19_335), qty: LotQty(900) };
        tao17.asks[0] = BookLevel { price: PriceTicks(19_336), qty: LotQty(900) };
        tao17.refresh_deep_l1_l5_totals();
        tao17.factor_ofi = BoundedScore::new_clamped(-0.68);
        tao17.factor_tfi = BoundedScore::new_clamped(-0.99);
        tao17.factor_geom = BoundedScore::new_clamped(-0.45);
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
            gov.authorizes_adaptive_entry_v49(4, Side::Ask, w_65, 1_000, ScaledBps(594), &tao17, &btc, 0.0, 1.0),
            Err(LastVetoCode::MacroElliottContradiction)
        );

        // ETH #18 Bid (1 lot, weak geom +0.09) -> QueueDepthOrOversizedLot / SpoofingVeto.
        let mut eth18 = btc;
        eth18.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth18.realized_vol_5m = ScaledBps(2_800);
        eth18.bids[0] = BookLevel { price: PriceTicks(179_267), qty: LotQty(4_000) };
        eth18.asks[0] = BookLevel { price: PriceTicks(179_268), qty: LotQty(4_000) };
        eth18.refresh_deep_l1_l5_totals();
        eth18.factor_ofi = BoundedScore::new_clamped(0.57);
        eth18.factor_tfi = BoundedScore::new_clamped(0.93);
        eth18.factor_geom = BoundedScore::new_clamped(0.09);
        assert!(gov
            .authorizes_adaptive_entry_v49(1, Side::Bid, w_65, 1_000, ScaledBps(647), &eth18, &eth18, 0.0, 1.0)
            .is_err());
    }
}

impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    pub const fn slot_eq_vol_5m_bps(&self, slot_idx: usize) -> i64 {
        if slot_idx < MAX_ACTIVE_PAIRS {
            self.eq_vol_5m_bps[slot_idx]
        } else {
            ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4
        }
    }

    /// Evaluates Pillar-2 ex-ante Expected Value (`ScaledBps`, `1 bp = 100`) at `entry_px` and `allocated_lots`.
    /// Returns `Some(ev_sbps)` strictly when `ev_sbps > 0` and both Pillar-2 asymmetry gates pass.
    #[inline]
    #[must_use]
    pub fn evaluate_slot_pilar2_ex_ante_ev_sbps(
        &self,
        slot_idx: usize,
        side: Side,
        allocated_lots: LotQty,
        entry_px: PriceTicks,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
    ) -> Option<i64> {
        if slot_idx >= MAX_ACTIVE_PAIRS || entry_px.0 <= 0 {
            return None;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let signed_geom = side.sign_f64() * snap.factor_geom.get();
        let bull_u16 = clamp_ob_permille_u16(snap.ob_bull_efficacy_pm);
        let bear_u16 = clamp_ob_permille_u16(snap.ob_bear_efficacy_pm);
        spec.evaluate_pilar2_asymmetry_ev_sbps(
            allocated_lots,
            entry_px,
            snap.realized_vol_5m,
            learned_mfe_bps.raw(),
            self.slot_taker_prob_permille(slot_idx, side),
            signed_geom,
            bull_u16,
            bear_u16,
            side,
            self.active_fee_schedule(),
        )
    }

    /// Evaluates V5.4.1/V5.5.3 Pillar-2 ex-ante Expected Value coupling multi-lot `M_eff(V, q_struct)` to `cont_ratio_x1000`,
    /// endogenous `target_excursion_scaled_bps` & aligned `fib_proj_bps`, and `1-lot` excursion to `OFI + BTC` propulsion `S_prop`.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
    pub fn evaluate_slot_pilar2_ex_ante_ev_with_leader_sbps(
        &self,
        slot_idx: usize,
        side: Side,
        allocated_lots: LotQty,
        entry_px: PriceTicks,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
    ) -> Option<i64> {
        if slot_idx >= MAX_ACTIVE_PAIRS || entry_px.0 <= 0 {
            return None;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let d = Self::dir_idx(side);
        let auto_target = match OrthogonalFactorState::evaluate_entry_signal_v542(
            spec,
            snap,
            btc_leader_snap,
            learned_mfe_bps,
            ScaledBps(self.ewma_net_bps_x100[slot_idx][d]),
            self.slot_taker_prob_permille(slot_idx, side),
            self.active_fee_schedule(),
            0.0,
            epps_signed_beta,
            if slot_idx > 0 && snap.symbol_id != btc_leader_snap.symbol_id {
                let own_v = snap.realized_vol_5m.raw().max(100) as f64;
                let btc_v = btc_leader_snap.realized_vol_5m.raw().max(100) as f64;
                (epps_signed_beta * (btc_v / own_v)).clamp(-1.0, 1.0)
            } else {
                0.0
            },
            None,
            self.psi_health(),
        ) {
            crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
                side: sig_side,
                target_excursion_scaled_bps,
            } if sig_side == side => target_excursion_scaled_bps,
            _ => ScaledBps(0),
        };
        self.evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
            slot_idx,
            side,
            allocated_lots,
            entry_px,
            learned_mfe_bps,
            auto_target,
            snap,
            btc_leader_snap,
            epps_signed_beta,
        )
    }

    /// Evaluates V5.5.3 `O(V)` homogeneous Pillar-2 ex-ante Expected Value (`ScaledBps`) explicitly connecting
    /// `cont_ratio_x1000`, `target_excursion_scaled_bps`, and aligned `fib_proj_bps` for `allocated_lots >= 2`
    /// while preserving `1-lot` (`BTC/ETH`) `compute_single_lot_flow_btc_propulsion_c1` selectivity 100% intact.
    #[inline]
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::suboptimal_flops,
        clippy::too_many_arguments
    )]
    pub fn evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
        &self,
        slot_idx: usize,
        side: Side,
        allocated_lots: LotQty,
        entry_px: PriceTicks,
        learned_mfe_bps: ScaledBps,
        target_excursion_scaled_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
    ) -> Option<i64> {
        self.evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
            slot_idx,
            side,
            allocated_lots,
            entry_px,
            learned_mfe_bps,
            target_excursion_scaled_bps,
            snap,
            btc_leader_snap,
            epps_signed_beta,
            1.0,
        )
    }

    /// `V5.5.5` slot Pillar-2 EV: `V5.5.3` + B-02 pre-print `OFI+GEOM` `q_struct` (live `spoof_density`) and
    /// B-04b Inside-Spread Penny-Jump passive credit (only when `qualifies_inside_spread_penny_jump_c1`, i.e. `SPR >= 2`).
    /// `spoof_density = 1.0` (neutral, used by `_v553`) reproduces `V5.5.3` exactly.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::suboptimal_flops,
        clippy::too_many_arguments
    )]
    pub fn evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
        &self,
        slot_idx: usize,
        side: Side,
        allocated_lots: LotQty,
        entry_px: PriceTicks,
        learned_mfe_bps: ScaledBps,
        target_excursion_scaled_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
        spoof_density: f64,
    ) -> Option<i64> {
        if slot_idx >= MAX_ACTIVE_PAIRS || entry_px.0 <= 0 {
            return None;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let dir_s = side.sign_f64();
        let signed_geom = dir_s * snap.factor_geom.get();
        let ofi_aligned = dir_s * snap.factor_ofi.get();
        let tfi_aligned = dir_s * snap.factor_tfi.get();
        let is_follower = slot_idx > 0 && snap.symbol_id != btc_leader_snap.symbol_id;
        let beta_dir = if epps_signed_beta < -1e-6 { -1.0 } else { 1.0 };
        let btc_aligned = if is_follower {
            beta_dir * crate::alpha_3factor::OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s
        } else {
            crate::alpha_3factor::OrthogonalFactorState::snr_composite_score(snap) * dir_s
        };
        let btc_iceberg_al = if is_follower { btc_aligned } else { 0.0 };
        let s_iceberg = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
            ofi_aligned,
            tfi_aligned,
            btc_iceberg_al,
        );
        let eff_learned_mfe = ((learned_mfe_bps.raw() as f64)
            * (1.0 - crate::alpha_3factor::INV_PHI * s_iceberg))
            .round() as i64;
        let bull_u16 = clamp_ob_permille_u16(snap.ob_bull_efficacy_pm);
        let bear_u16 = clamp_ob_permille_u16(snap.ob_bear_efficacy_pm);
        let vacuum_l2_l3 = crate::exec_daemon::compute_vacuum_l2_l3_c1(side, snap);
        let cont_ratio_x1000 = self.slot_cont_ratio_x1000(slot_idx, side);
        let aligned_fib_proj_bps = ScaledBps(
            snap.wave_observables()
                .fib_proj_bps
                .raw()
                .saturating_mul(side.sign_i64())
                .max(0),
        );
        let penny_spread_ticks = if Self::qualifies_inside_spread_penny_jump_c1(side, snap, self.psi_health()) {
            snap.bbo_and_spread_ticks().map_or(1, |(_, _, spr)| spr)
        } else {
            1
        };
        let base_ev = spec.evaluate_pilar2_asymmetry_ev_v555_sbps(
            allocated_lots,
            entry_px,
            snap.realized_vol_5m,
            eff_learned_mfe,
            self.slot_taker_prob_permille(slot_idx, side),
            signed_geom,
            bull_u16,
            bear_u16,
            side,
            self.active_fee_schedule(),
            ofi_aligned,
            tfi_aligned,
            btc_aligned,
            vacuum_l2_l3,
            cont_ratio_x1000,
            target_excursion_scaled_bps,
            aligned_fib_proj_bps,
            spoof_density,
            penny_spread_ticks,
            self.slot_fill_rate_eta(slot_idx),
        )?;
        let ev_damped = ((base_ev as f64) * (1.0 - crate::alpha_3factor::PHI * s_iceberg)).round() as i64;
        (ev_damped > 0).then_some(ev_damped)
    }

    /// Continuous C1 pre-funding cliff survival gate coupled to self-learned `tau_mkt`, `MFE`, `IR` & `p_Taker`.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn passes_elastic_funding_cliff_guard(
        &self,
        slot_idx: usize,
        side: Side,
        learned_mfe_bps: ScaledBps,
        snap: &SymbolLobSnapshot,
    ) -> bool {
        const PHI_F: f64 = 1.618_033_988_749_895;
        const INV_PHI_F: f64 = 0.618_033_988_749_895;
        const INV_PHI2_F: f64 = 0.381_966_011_250_105;

        if slot_idx >= MAX_ACTIVE_PAIRS || snap.ts_local_ns.0 < MIN_VALID_UTC_EPOCH_NS {
            return true;
        }
        let adverse_fund_sbps = side.sign_i64().saturating_mul(snap.funding_rate_scaled_bps.0);
        if adverse_fund_sbps <= 0 {
            return true;
        }
        let rem_s = if snap.next_funding_ts_ns.0 > snap.ts_local_ns.0 {
            ((snap.next_funding_ts_ns.0 - snap.ts_local_ns.0) / 1_000_000_000).max(1) as f64
        } else {
            (28_800 - snap.ts_local_ns.0.div_euclid(1_000_000_000).rem_euclid(28_800)) as f64
        };
        let tau_mkt_s = ((self.intrinsic_diffusion_clock_ns(slot_idx) as f64) * 1e-9).max(60.0);
        let tau_hold_s = (INV_PHI2_F * tau_mkt_s).max(25.0);
        let expected_fund_drag = (adverse_fund_sbps as f64) * (-rem_s / tau_hold_s).exp();
        let p_tkr = (self.slot_taker_prob_permille(slot_idx, side).clamp(5, 950) as f64) * 1e-3;
        let fee_rt = self.active_fee_schedule().round_trip_maker_exit_bps().raw() as f64;
        let d = Self::dir_idx(side);
        let ir_bonus = (self.ewma_net_bps_x100[slot_idx][d].max(0) as f64) * INV_PHI2_F;
        let net_edge = ((learned_mfe_bps.raw().max(0) as f64 - fee_rt).max(0.0) + ir_bonus)
            * (1.0 - p_tkr)
            / PHI_F.powf(p_tkr * 4.0);
        expected_fund_drag <= INV_PHI_F * net_edge
    }
}

impl OnlineAdaptiveGovernor {
    /// Elastic C1 preflight & re-peg L1 support floor scaled by self-learned `p_Taker` & `sqrt(sigma_5m / B_3x)`.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn passes_elastic_preflight_l1_support(
        &self,
        slot_idx: usize,
        side: Side,
        entry_px: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> bool {
        const PHI_L1: f64 = 1.618_033_988_749_895;
        if slot_idx >= MAX_ACTIVE_PAIRS || entry_px.0 <= 0 {
            return false;
        }
        let spec = APPROVED_SPECS[slot_idx];
        let own_q = match side {
            Side::Bid => snap.bids[0].qty,
            Side::Ask => snap.asks[0].qty,
        };
        if own_q.0 <= 0 {
            return false;
        }
        let fees = self.active_fee_schedule();
        let p_pm = self.slot_taker_prob_permille(slot_idx, side);
        let b3x = spec
            .three_x_friction_barrier_bps_dynamic(entry_px, p_pm, fees)
            .raw()
            .max(150) as f64;
        let vol_ratio = ((snap.realized_vol_5m.raw().max(150) as f64) / b3x)
            .sqrt()
            .clamp(1.0, PHI_L1 * PHI_L1);
        let p_tkr = (p_pm.clamp(5, 950) as f64) * 1e-3;
        let min_l1_1e8 = (crate::types::BYBIT_MIN_ORDER_NOTIONAL_1E8 as f64)
            * PHI_L1
            * (1.0 + PHI_L1 * p_tkr)
            * vol_ratio;
        (spec.lot_notional_1e8(own_q, entry_px) as f64) >= min_l1_1e8
    }
}

impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn passes_cross_side_abc_chop_shield(&self, slot_idx: usize, side: Side, snap: &SymbolLobSnapshot) -> bool {
        const PHI_C: f64 = 1.618_033_988_749_895;
        const INV_PHI_C: f64 = 0.618_033_988_749_895;
        const INV_PHI2_C: f64 = 0.381_966_011_250_105;
        if slot_idx >= MAX_ACTIVE_PAIRS || snap.ts_local_ns.0 <= 0 { return true; }
        let w = snap.wave_observables();
        let aligned_macro = w.macro_phase.get() * side.sign_f64();
        if matches!(w.regime, crate::wave_elliott::WaveRegime::ImpulseWave3) && aligned_macro >= INV_PHI2_C {
            return true;
        }
        let is_chop_reg = matches!(
            w.regime,
            crate::wave_elliott::WaveRegime::CorrectiveAbc
                | crate::wave_elliott::WaveRegime::CorrectiveWave4
                | crate::wave_elliott::WaveRegime::ClimaxExhaustion
        );
        if is_chop_reg && snap.ts_local_ns.0 < self.cross_chop_cooldown_until_ns[slot_idx] {
            return false;
        }
        if matches!(w.regime, crate::wave_elliott::WaveRegime::CorrectiveAbc)
            && self.abc_chop_last_sl_ts_ns > 0
            && snap.ts_local_ns.0 >= self.abc_chop_last_sl_ts_ns
        {
            let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx) as f64;
            let chop_lvl = (self.abc_chop_sl_score_pm.clamp(0, 1_000) as f64) * 1e-3;
            let port_win_ns = (PHI_C * tau_ns * (1.0 + PHI_C * chop_lvl)).round() as i64;
            if snap.ts_local_ns.0.saturating_sub(self.abc_chop_last_sl_ts_ns) < port_win_ns {
                let (al_pm, op_pm) = match side {
                    Side::Bid => (snap.ob_bull_efficacy_pm, snap.ob_bear_efficacy_pm),
                    Side::Ask => (snap.ob_bear_efficacy_pm, snap.ob_bull_efficacy_pm),
                };
                let rho_ob = if al_pm > 0 || op_pm > 0 {
                    (al_pm.max(382) as f64) / ((al_pm.max(382) + op_pm.max(382)) as f64)
                } else {
                    0.50
                };
                if rho_ob < INV_PHI_C { return false; }
            }
        }
        true
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
    pub fn observe_closed_trade_with_regime_v51(
        &mut self,
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        net_scaled_bps: ScaledBps,
        mfe_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        wave: crate::wave_elliott::WaveObservables,
        event_dilation: f64,
        ts_ns: TimestampNs,
    ) {
        const PHI_C: f64 = 1.618_033_988_749_895;
        const INV_PHI_C: f64 = 0.618_033_988_749_895;
        if slot_idx >= MAX_ACTIVE_PAIRS { return; }
        let climax_flag =
            wave.climax_active || matches!(wave.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion);
        self.observe_closed_trade_core(
            slot_idx,
            side,
            entry_price,
            net_scaled_bps,
            Some(mfe_scaled_bps),
            is_maker_exit,
            Some(climax_flag),
            ts_ns,
        );
        let fee_rt = self.active_maker_rt_fee_bps();
        let adv_sl = !is_maker_exit && net_scaled_bps.raw() < 0;
        let is_abc = matches!(
            wave.regime,
            crate::wave_elliott::WaveRegime::CorrectiveAbc | crate::wave_elliott::WaveRegime::CorrectiveWave4
        );
        if adv_sl && is_abc {
            let fee_f = (fee_rt.max(100)) as f64;
            let s_zero = crate::wave_elliott::hermite_smoothstep_c1(fee_f - (mfe_scaled_bps.raw().max(0) as f64), 0.0, fee_f);
            if s_zero > 1e-6 {
                self.plasticity.observe_sympathetic_chop_coupling(slot_idx, side, mfe_scaled_bps);
                let kappa = event_dilation.clamp(INV_PHI_C, PHI_C);
                let tau_ns = self.intrinsic_diffusion_clock_ns(slot_idx) as f64;
                let p_opp = (self.slot_taker_prob_permille(slot_idx, side.opposite()).clamp(5, 950) as f64) * 1e-3;
                let dt_opp = (((INV_PHI_C * tau_ns) / kappa) * (1.0 + PHI_C * p_opp)).round() as i64;
                if ts_ns.0 > 0 {
                    self.cross_chop_cooldown_until_ns[slot_idx] = self.cross_chop_cooldown_until_ns[slot_idx].max(ts_ns.0.saturating_add(dt_opp));
                    self.abc_chop_last_sl_ts_ns = self.abc_chop_last_sl_ts_ns.max(ts_ns.0);
                }
                let shock_pm = (INV_PHI_C * 1_000.0 * s_zero).round() as i64;
                self.abc_chop_sl_score_pm = ((self.abc_chop_sl_score_pm * 618) / 1_000 + shock_pm).clamp(0, 1_000);
            }
        } else if is_maker_exit && net_scaled_bps.raw() >= fee_rt / 4 {
            self.abc_chop_sl_score_pm = (self.abc_chop_sl_score_pm * 618) / 1_000;
        }
    }
}

#[cfg(test)]
mod v51_f6_plasticity_chop_tests {
    use super::*;
    use crate::fee_cadence_beta::BetaBinomialTakerPrior;
    use crate::kinematic_surface::UNIFIED_LOSS_LAB_COUNT;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_v51_f6_surprise_weighted_taker_prior_and_cross_side_abc_cooldown() {
        assert_eq!(OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold(), (UNIFIED_LOSS_LAB_COUNT, 2_160));
        let mut pr = BetaBinomialTakerPrior::new_seeded(5);
        let pm1 = observe_outcome_surprise_weighted(&mut pr, true, ScaledBps(0), ScaledBps(400));
        let pm2 = observe_outcome_surprise_weighted(&mut pr, true, ScaledBps(0), ScaledBps(400));
        assert!((110..120).contains(&pm1) && pm2 >= 180);

        let mut gov = OnlineAdaptiveGovernor::new();
        let t0 = 1_795_000_000_000_000_000_i64;
        let w_abc = WaveObservables {
            regime: WaveRegime::CorrectiveAbc, climax_active: false, hurst: WaveObservables::encode_hurst(0.46),
            macro_phase: BoundedScore::new_clamped(0.25), oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
        };
        gov.observe_closed_trade_with_regime_v51(3, Side::Ask, PriceTicks(7_156), ScaledBps(-2_008), ScaledBps(139), false, w_abc, 1.0, TimestampNs(t0));
        assert!(gov.slot_taker_prob_permille(3, Side::Ask) >= 100 && gov.slot_taker_prob_permille(3, Side::Bid) >= 65);
        gov.observe_closed_trade_with_regime_v51(1, Side::Ask, PriceTicks(179_961), ScaledBps(-1_956), ScaledBps(0), false, w_abc, 1.0, TimestampNs(t0 + 60_000_000_000));
        gov.observe_closed_trade_with_regime_v51(1, Side::Bid, PriceTicks(179_542), ScaledBps(-2_431), ScaledBps(0), false, w_abc, 1.0, TimestampNs(t0 + 120_000_000_000));
        assert!(gov.slot_taker_prob_permille(1, Side::Bid) >= 135);

        let mut tao = SymbolLobSnapshot::new_empty();
        tao.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao.ts_local_ns = TimestampNs(t0 + 540_000_000_000);
        tao.realized_vol_5m = ScaledBps(3_200);
        tao.bids[0] = BookLevel { price: PriceTicks(19_691), qty: LotQty(450) };
        tao.asks[0] = BookLevel { price: PriceTicks(19_692), qty: LotQty(320) };
        tao.refresh_deep_l1_l5_totals();
        tao.set_wave_observables(w_abc);
        assert!(!gov.passes_cross_side_abc_chop_shield(4, Side::Bid, &tao));
        let mut w_brk = w_abc;
        w_brk.regime = WaveRegime::ImpulseWave3;
        w_brk.macro_phase = BoundedScore::new_clamped(0.75);
        tao.set_wave_observables(w_brk);
        assert!(gov.passes_cross_side_abc_chop_shield(4, Side::Bid, &tao));
    }
}

#[inline]
#[must_use]
fn hermite_s3_unit(x: f64, edge0: f64, edge1: f64) -> f64 {
    crate::wave_elliott::hermite_smoothstep_c1(x, edge0, edge1)
}

#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_elastic_l1_patience_ns_with_queue_v531(
    tau_mkt_s: i64,
    event_dilation: f64,
    hawkes_lambda_ratio: f64,
    signed_flow_slow: crate::types::BoundedScore,
    init_q: LotQty,
    rem_q: LotQty,
) -> i64 {
    const PHI_P: f64 = 1.618_033_988_749_895;
    const PHI_CUBE_P: f64 = 4.236_067_977_499_79;
    const INV_PHI_P: f64 = 0.618_033_988_749_895;
    const INV_PHI2_P: f64 = 0.381_966_011_250_105;
    const INV_PHI3_P: f64 = 0.236_067_977_499_79;
    const INV_PHI4_P: f64 = 0.145_898_033_750_315;
    const INV_PHI6_P: f64 = 0.055_728_090_000_84;

    let tau_s = (tau_mkt_s.clamp(60, 900)) as f64;
    let raw_kappa = if event_dilation.is_finite() {
        event_dilation.clamp(INV_PHI_P, PHI_P)
    } else {
        PHI_P
    };
    let lam_sqrt = if hawkes_lambda_ratio.is_finite() {
        hawkes_lambda_ratio.clamp(INV_PHI2_P, PHI_CUBE_P * PHI_P).sqrt()
    } else {
        (PHI_CUBE_P * PHI_P).sqrt()
    };
    let fl = signed_flow_slow.get();
    let kappa = if fl >= -INV_PHI4_P {
        raw_kappa.min(1.0)
    } else {
        raw_kappa
    };
    let s_pos = hermite_s3_unit(fl, 0.0, INV_PHI_P);
    let s_neg = hermite_s3_unit(-fl, 0.0, INV_PHI_P);
    let flow_mult = (1.0 + INV_PHI_P * s_pos - INV_PHI2_P * s_neg).max(INV_PHI3_P);
    let q_prog = if init_q.0 > 0 {
        (1.0 - (rem_q.0.max(0) as f64) / (init_q.0 as f64)).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let s_flow_ok = hermite_s3_unit(fl, -INV_PHI4_P, INV_PHI2_P);
    let fifo_prio_mult = 1.0 + PHI_P * q_prog * s_flow_ok;
    let patience_s = (INV_PHI6_P * tau_s * flow_mult * fifo_prio_mult) / (kappa * lam_sqrt);
    (patience_s * 1_000_000_000.0).round() as i64
}

#[inline]
#[must_use]
pub fn compute_elastic_l1_patience_ns_v51(
    tau_mkt_s: i64,
    event_dilation: f64,
    hawkes_lambda_ratio: f64,
    signed_flow_slow: crate::types::BoundedScore,
) -> i64 {
    compute_elastic_l1_patience_ns_with_queue_v531(
        tau_mkt_s,
        event_dilation,
        hawkes_lambda_ratio,
        signed_flow_slow,
        LotQty(0),
        LotQty(0),
    )
}

impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn elastic_resting_l1_patience_ns_with_queue_v531(
        &self,
        slot_idx: usize,
        side: Side,
        realized_vol_5m: ScaledBps,
        event_dilation: f64,
        hawkes_lambda_ratio: f64,
        signed_flow_slow: crate::types::BoundedScore,
        init_q: LotQty,
        rem_q: LotQty,
    ) -> i64 {
        const PHI_P: f64 = 1.618_033_988_749_895;
        const INV_PHI_P: f64 = 0.618_033_988_749_895;
        const INV_PHI2_P: f64 = 0.381_966_011_250_105;
        const INV_PHI3_P: f64 = 0.236_067_977_499_79;
        let s_c = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let tau_mkt_s = (self.intrinsic_diffusion_clock_ns(s_c) / 1_000_000_000).max(60);
        let base_ns = compute_elastic_l1_patience_ns_with_queue_v531(
            tau_mkt_s,
            event_dilation,
            hawkes_lambda_ratio,
            signed_flow_slow,
            init_q,
            rem_q,
        ) as f64;
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = self.eq_vol_5m_bps[s_c].max(min_v * 2) as f64;
        let cur_v = (realized_vol_5m.raw().max(min_v)) as f64;
        let vol_scale = (eq_v / cur_v).sqrt().clamp(INV_PHI_P, PHI_P);
        let p_tkr = (self.slot_taker_prob_permille(s_c, side).clamp(5, 950) as f64) * 1e-3;
        let ir_norm = (self.slot_information_ratio_x100(s_c, side).clamp(-50, 100) as f64) * 1e-2;
        let learn_mult = ((1.0 - INV_PHI2_P * p_tkr) * (1.0 + INV_PHI3_P * ir_norm)).clamp(INV_PHI2_P, PHI_P);
        (base_ns * vol_scale * learn_mult).round() as i64
    }

    #[inline]
    #[must_use]
    pub fn elastic_resting_l1_patience_ns_v51(
        &self,
        slot_idx: usize,
        side: Side,
        realized_vol_5m: ScaledBps,
        event_dilation: f64,
        hawkes_lambda_ratio: f64,
        signed_flow_slow: crate::types::BoundedScore,
    ) -> i64 {
        self.elastic_resting_l1_patience_ns_with_queue_v531(
            slot_idx,
            side,
            realized_vol_5m,
            event_dilation,
            hawkes_lambda_ratio,
            signed_flow_slow,
            LotQty(0),
            LotQty(0),
        )
    }
}

#[cfg(test)]
mod v51_f4_elastic_patience_tests {
    use super::*;
    use crate::types::BoundedScore;

    #[test]
    fn test_v51_f4_elastic_l1_patience_expands_on_aligned_flow_and_contracts_on_hawkes_burst() {
        let gov = OnlineAdaptiveGovernor::new();
        let pat_aligned = gov.elastic_resting_l1_patience_ns_v51(
            4, Side::Bid, ScaledBps(2_800), 1.0, 1.0, BoundedScore::new_clamped(0.55),
        );
        let pat_toxic = gov.elastic_resting_l1_patience_ns_v51(
            4, Side::Bid, ScaledBps(6_500), 1.5, 4.0, BoundedScore::new_clamped(-0.48),
        );
        assert!(pat_aligned >= 10_000_000_000 && pat_toxic <= 5_000_000_000 && pat_aligned > pat_toxic * 2);
    }
}
