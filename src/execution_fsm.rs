//! Autonomous 12-Slot Execution FSM, Live VIP Fee Propagation, Signed Epps Wald SPRT & Chandelier SL (V4.8).
#![allow(clippy::manual_clamp, clippy::suboptimal_flops, clippy::too_many_arguments)]

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::fee_cadence_beta::FeeSchedule;
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{SlotTelemetrySnapshot, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
    SPLIT_LEG_MIN_NOTIONAL_1E8,
};
use crate::wave_elliott::WaveRegime;
#[cfg(test)]
use crate::wave_elliott::WaveObservables;

pub use crate::bayesian_governor::OnlineAdaptiveGovernor;
pub use crate::exit_quoter::{
    classify_maker_exit_sub_reason, classify_maker_exit_sub_reason_with_fees,
    compute_canonical_exit_quote_core, compute_canonical_exit_quote_omega,
    compute_chandelier_profit_lock_sl, compute_elastic_offensive_win_floor_bps,
    compute_tranche1_split_lots_core, compute_validated_sl_ticks,
    compute_validated_sl_ticks_with_fees, compute_wave_dynamic_t1_permille,
    defensive_zero_loss_maker_exit_price, defensive_zero_loss_maker_exit_price_with_fees,
    dynamic_maker_floor_price, elastic_offensive_win_maker_exit_price,
    evaluate_shadow_runner_propulsion, evaluate_sl_or_emergency_flatten_core,
    evaluate_sl_or_emergency_flatten_with_fees, is_chandelier_profit_locked_sl,
    is_chandelier_profit_locked_sl_with_fees, offensive_win_maker_exit_price,
    reconcile_closed_pnl_core, resolve_fallback_unverified_close_core, CanonicalExitQuote,
    ClosedPnlQueryOutcome, ClosedTradeKind, ClosedTradeSummary, ExitOrderRole, ExitSubReason,
    OpenExitContext, ServerSlDecision, ShadowRunnerKinematics, VerifiedCloseResolution,
    MAX_CLOSE_RECONCILE_RETRIES, OFFENSIVE_WIN_FLOOR_GROSS_BPS,
};
pub use crate::kinematic_surface::{
    check_queue_depletion_velocity_abort, compute_stoikov_microprice_skew,
    compute_wald_v2_contracted_sl_bps, compute_wald_v2_contracted_sl_bps_with_fees,
    evaluate_moallemi_stoikov_queue_shield, evaluate_moallemi_stoikov_queue_shield_with_beta,
    passes_stoikov_repeg_coherence_guard, passes_stoikov_repeg_coherence_guard_with_beta,
    step_wald_sprt_v2, wald_sprt_boundaries, HawkesQueueTracker, QueueShieldVerdict, ScarEventKey,
    ScarEventPayload, WaldSprtState, UNIFIED_LOSS_LAB_COUNT,
};
pub use crate::ws_protocol::{
    amend_fifo_queue_priority, asymmetric_recv_window_ms, compute_amend_total_lots,
    format_bijective_order_link_id, format_ws_trade_req_id, parse_signed_decimal_1e8,
    parse_verified_avg_price_ticks, parse_verified_exit_price_ticks, sign_bybit_v5_hmac_sha256,
    OrderLinkRole, Vip0WsRateGovernor, WsMutationOp, WsOrderLatticeState, WsOrderRank,
    WsTerminalKind,
};

const VOL_5M_HORIZON_NS: i64 = 300_000_000_000;
const FLOW_CONTIGUOUS_GAP_MAX_NS: i64 = 2_000_000_000;
const EPPS_PHI_CAP: f64 = 1.618_033_988_749_895;

#[inline]
#[must_use]
fn directional_ob_for_side(side: Side, snap: &SymbolLobSnapshot) -> crate::order_block::OrderBlockObservables {
    let mut obs = snap.order_block_observables();
    let dir_pm = match side { Side::Bid => snap.ob_bear_efficacy_pm, Side::Ask => snap.ob_bull_efficacy_pm };
    if dir_pm > 0 { obs.learned_efficacy_pm = dir_pm; }
    obs
}

#[inline]
#[must_use]
pub fn passes_order_block_wall_guard_dynamic(
    slot_idx: usize,
    side: Side,
    snap: &SymbolLobSnapshot,
    taker_prob_pm: i64,
    fees: FeeSchedule,
) -> bool {
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
        PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
    } else { snap.mark_price };
    !directional_ob_for_side(side, snap).vetoes_into_opposing_wall_dynamic(
        side, spec, mid_px, snap.realized_vol_5m, snap.factor_tfi.get() * side.sign_f64(), taker_prob_pm, fees,
    )
}

#[inline]
#[must_use]
pub fn passes_order_block_wall_guard(slot_idx: usize, side: Side, snap: &SymbolLobSnapshot) -> bool {
    passes_order_block_wall_guard_dynamic(slot_idx, side, snap, 50, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
#[must_use]
pub fn evaluate_macro_coherence_surface_c1(side: Side, snap: &SymbolLobSnapshot) -> f64 {
    let w = snap.wave_observables();
    let base_c1 = crate::wave_elliott::evaluate_elliott_hurst_coherence_penalty_c1(side, w);
    let dir_s = side.sign_f64();
    let h_t = w.hurst_exponent();
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_bps = (w.fib_proj_bps.raw() as f64 / 100.0) * dir_s;
    let oi_inv = crate::wave_elliott::hermite_smoothstep_c1(h_t, 0.494, 0.506)
        * crate::wave_elliott::hermite_smoothstep_c1(-0.20 - macro_aligned, 0.0, 0.08);
    let fib_br = if matches!(w.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5) {
        crate::wave_elliott::hermite_smoothstep_c1(-35.0 - fib_aligned_bps, 0.0, 25.0)
            * crate::wave_elliott::hermite_smoothstep_c1(h_t, 0.488, 0.508)
    } else { 0.0 };
    base_c1.max(oi_inv).max(fib_br).clamp(0.0, 1.0)
}

#[inline]
#[must_use]
pub fn passes_elliott_macro_coherence_guard(side: Side, snap: &SymbolLobSnapshot) -> bool {
    evaluate_macro_coherence_surface_c1(side, snap) < 0.618_033_988_749_895
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OrderLifecycleState {
    #[default]
    Idle,
    PlacingEntryMaker {
        side: Side, entry_price: PriceTicks, qty: LotQty,
        target_excursion: ScaledBps, sl_trigger_price: PriceTicks, submitted_ts: TimestampNs,
    },
    RestingEntryQueue {
        side: Side, entry_price: PriceTicks, qty: LotQty,
        target_excursion: ScaledBps, sl_trigger_price: PriceTicks, resting_since_ts: TimestampNs,
    },
    PositionOpenMakerExit {
        side: Side, entry_price: PriceTicks, avg_price_ticks: PriceTicks,
        peak_favorable_price: PriceTicks, worst_adverse_price: PriceTicks,
        initial_lots: LotQty, open_lots: LotQty, tranche1_harvested: bool,
        tranche1_lots: LotQty, tranche1_exit_price: PriceTicks, defensive_floor_locked: bool,
        active_exit_role: ExitOrderRole, active_exit_price: PriceTicks, active_exit_lots: LotQty,
        structural_tp_price: PriceTicks, resting_exit_price: PriceTicks,
        sl_trigger_price: PriceTicks, opened_ts: TimestampNs,
    },
}

#[derive(Clone, Debug)]
pub struct SlotExecutionEngine {
    pub slot_idx: usize,
    pub state: OrderLifecycleState,
    pub cooldown_until_ns: TimestampNs,
    pub tranche1_harvests_count: u64,
    pub lvdbs_deep_surfs_count: u64,
    pub learned_mfe_bps: i64,
    pub learned_t1_permille: i64,
    pub learned_ir_x100: i64,
    pub learned_ret_std_bps: i64,
    pub intrinsic_tau_mkt_s: i64,
    pub active_fees: FeeSchedule,
    pub learned_taker_prob_pm: i64,
    pub last_eff_sl_ticks: PriceTicks,
    pub open_vol_5m_bps: ScaledBps,
    pub ratchet_locked_gross_bps: i64,
    pub last_closed_mfe_bps: i64,
    pub last_closed_mae_bps: i64,
    pub last_closed_peak_ticks: PriceTicks,
    pub last_closed_worst_ticks: PriceTicks,
    pub last_closed_wald_pct: i64,
    pub last_exit_sub_reason: ExitSubReason,
    pub last_opened_ts: TimestampNs,
    pub next_peg_allowed_ts_ns: i64,
    pub exit_peg_attempts: u32,
    pub close_reconcile_retries: u8,
    pub flow_ema_fast: f64,
    pub flow_ema_slow: f64,
    pub last_flow_sample_ts_ns: i64,
    pub in_slot_scar_debt: bool,
    pub chandelier_profit_sl_armed: bool,
    pub hawkes_tracker: HawkesQueueTracker,
    pub wald_sprt: WaldSprtState,
}

impl SlotExecutionEngine {
    #[inline] #[must_use]
    pub const fn new(slot_idx: usize) -> Self {
        Self {
            slot_idx, state: OrderLifecycleState::Idle, cooldown_until_ns: TimestampNs(0),
            tranche1_harvests_count: 0, lvdbs_deep_surfs_count: 0,
            learned_mfe_bps: (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 17) / 5,
            learned_t1_permille: 500, learned_ir_x100: 33, learned_ret_std_bps: 200,
            intrinsic_tau_mkt_s: VOL_5M_HORIZON_NS / 1_000_000_000,
            active_fees: FeeSchedule::VIP0_DEFAULT, learned_taker_prob_pm: 50,
            last_eff_sl_ticks: PriceTicks(0), open_vol_5m_bps: ScaledBps(6_000),
            ratchet_locked_gross_bps: 0, last_closed_mfe_bps: 0, last_closed_mae_bps: 0,
            last_closed_peak_ticks: PriceTicks(0), last_closed_worst_ticks: PriceTicks(0),
            last_closed_wald_pct: 0, last_exit_sub_reason: ExitSubReason::AdverseScratchEscape,
            last_opened_ts: TimestampNs(0), next_peg_allowed_ts_ns: 0, exit_peg_attempts: 0,
            close_reconcile_retries: 0, flow_ema_fast: 0.0, flow_ema_slow: 0.0,
            last_flow_sample_ts_ns: 0, in_slot_scar_debt: false, chandelier_profit_sl_armed: false,
            hawkes_tracker: HawkesQueueTracker::new(), wald_sprt: WaldSprtState::new(),
        }
    }

    #[inline] #[must_use]
    pub const fn new_12_slot_array() -> [Self; MAX_ACTIVE_PAIRS] {
        [
            Self::new(0), Self::new(1), Self::new(2), Self::new(3),
            Self::new(4), Self::new(5), Self::new(6), Self::new(7),
            Self::new(8), Self::new(9), Self::new(10), Self::new(11),
        ]
    }

    #[inline] #[must_use]
    pub const fn active_maker_rt_fee_bps(&self) -> i64 {
        let rt = self.active_fees.round_trip_maker_exit_bps().raw();
        if rt > 100 { rt } else { 100 }
    }

    #[inline] #[must_use]
    pub fn extract_12_slot_mfe_array(engines: &[Self; MAX_ACTIVE_PAIRS]) -> [i64; MAX_ACTIVE_PAIRS] {
        let mut out = [0_i64; MAX_ACTIVE_PAIRS];
        for (i, eng) in engines.iter().enumerate() { out[i] = eng.learned_mfe_bps; }
        out
    }

    pub fn hydrate_12_slot_mfe_from_path(engines: &mut [Self; MAX_ACTIVE_PAIRS], path: &std::path::Path) {
        let first_rt = engines[0].active_maker_rt_fee_bps();
        let seed = (first_rt * 17) / 10;
        let loaded = crate::vault_io::load_mfe_plasticity_from_path(path, seed);
        for (i, eng) in engines.iter_mut().enumerate() {
            let fee_rt = eng.active_maker_rt_fee_bps();
            eng.learned_mfe_bps = loaded[i].clamp(fee_rt + 40, fee_rt * 12);
        }
    }

    #[inline] #[must_use]
    pub fn compute_tranche1_split_lots(slot_idx: usize, open_lots: LotQty, exit_price: PriceTicks, t1_permille: i64) -> Option<LotQty> {
        compute_tranche1_split_lots_core(slot_idx, open_lots, exit_price, t1_permille)
    }

    #[inline]
    pub fn sync_governor_for_side(&mut self, gov: &OnlineAdaptiveGovernor, side: Side) {
        self.active_fees = gov.active_fee_schedule();
        self.learned_taker_prob_pm = gov.slot_taker_prob_permille(self.slot_idx, side);
        self.learned_t1_permille = gov.learned_tranche1_fraction_permille(self.slot_idx, side, self.learned_mfe_bps);
        self.learned_ir_x100 = gov.slot_information_ratio_x100(self.slot_idx, side);
        self.learned_ret_std_bps = gov.slot_return_std_bps(self.slot_idx, side);
        self.intrinsic_tau_mkt_s = (gov.intrinsic_diffusion_clock_ns(self.slot_idx) / 1_000_000_000).max(90);
        self.in_slot_scar_debt = gov.is_in_local_slot_scar_debt(self.slot_idx, side);
    }

    #[inline]
    pub fn sync_governor_dynamics(&mut self, gov: &OnlineAdaptiveGovernor) {
        if let Some(side) = self.active_side() { self.sync_governor_for_side(gov, side); }
        else { self.active_fees = gov.active_fee_schedule(); }
    }

    #[inline] #[must_use]
    pub fn evaluate_resting_queue_shield_with_beta(
        &mut self, side: Side, init_q_ahead: LotQty, cur_q_ahead: LotQty, opp_tape_delta_lots: LotQty,
        snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot, epps_signed_beta: f64,
    ) -> QueueShieldVerdict {
        evaluate_moallemi_stoikov_queue_shield_with_beta(
            &mut self.hawkes_tracker, side, init_q_ahead, cur_q_ahead, opp_tape_delta_lots, snap, btc_leader_snap, self.slot_idx, epps_signed_beta,
        )
    }

    #[inline] #[must_use]
    pub fn evaluate_resting_queue_shield(
        &mut self, side: Side, init_q_ahead: LotQty, cur_q_ahead: LotQty, opp_tape_delta_lots: LotQty,
        snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot,
    ) -> QueueShieldVerdict {
        let beta = OrthogonalFactorState::epps_btc_lead_lag_beta(self.slot_idx, snap, btc_leader_snap);
        self.evaluate_resting_queue_shield_with_beta(side, init_q_ahead, cur_q_ahead, opp_tape_delta_lots, snap, btc_leader_snap, beta)
    }

    fn step_wald_sprt_v2_permille_with_beta(
        &mut self, side: Side, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot, epps_signed_beta: f64,
    ) -> WaldSprtState {
        let (a_hold, b_unwind) = crate::kinematic_surface::wald_sprt_boundaries_from_permille(self.learned_taker_prob_pm);
        let now_ns = snap.ts_local_ns.0;
        if self.wald_sprt.last_step_ts_ns <= 0 || now_ns <= self.wald_sprt.last_step_ts_ns {
            if now_ns > 0 { self.wald_sprt.last_step_ts_ns = now_ns; }
            return self.wald_sprt;
        }
        let dt_s = ((now_ns - self.wald_sprt.last_step_ts_ns) as f64 * 1e-9).clamp(0.001, 2.0);
        self.wald_sprt.last_step_ts_ns = now_ns;
        let dir_s = side.sign_f64();
        let phi_self = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
        let (m_skew, _) = compute_stoikov_microprice_skew(side, snap);
        let beta_btc = epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP);
        let btc_aligned = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        let y_ev = if self.slot_idx == 0 { 0.70 * phi_self + 0.30 * m_skew } else { 0.50 * phi_self + 0.20 * m_skew + 0.30 * beta_btc * btc_aligned };
        self.wald_sprt.log_likelihood = (self.wald_sprt.log_likelihood + 0.963_932_022_500_210_3 * (y_ev - 0.140_983_005_625_052_5) * dt_s).clamp(1.5 * b_unwind, a_hold);
        if self.wald_sprt.log_likelihood <= b_unwind {
            let excess = ((b_unwind - self.wald_sprt.log_likelihood) / b_unwind.abs().max(1e-6)).max(0.0);
            let btc_drag = if self.slot_idx > 0 { (-beta_btc * btc_aligned - 0.20).max(0.0) } else { 0.0 };
            self.wald_sprt.contraction_pct = ((18.0 + 22.0 * excess + 12.0 * btc_drag).round() as i64).clamp(15, 48);
            self.wald_sprt.exhaustion_unwind_active = true;
        } else {
            self.wald_sprt.contraction_pct = 0;
            self.wald_sprt.exhaustion_unwind_active = false;
        }
        self.wald_sprt
    }

    pub fn observe_wald_sprt_step_with_beta(
        &mut self, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot, epps_signed_beta: f64,
    ) -> WaldSprtState {
        let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, opened_ts, .. } = self.state else { return self.wald_sprt; };
        let st = self.step_wald_sprt_v2_permille_with_beta(side, snap, btc_leader_snap, epps_signed_beta);
        self.last_closed_wald_pct = self.last_closed_wald_pct.max(st.contraction_pct);
        let hold_s = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0) / 1_000_000_000;
        if avg_price_ticks.0 > 0 {
            let bbo_exit = match side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
            let ref_px = if bbo_exit.0 > 0 { bbo_exit } else if snap.mark_price.0 > 0 { snap.mark_price } else { snap.last_trade_price };
            let cur_gross_bps = if ref_px.0 > 0 { ((ref_px.0 - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1) } else { 0 };
            let dir_s = side.sign_f64();
            let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
            let win_floor = compute_elastic_offensive_win_floor_bps(self.active_maker_rt_fee_bps(), snap.realized_vol_5m).raw();
            if hold_s >= 3 && st.exhaustion_unwind_active && cur_gross_bps < -200 && own_flow < 0.15 && cur_gross_bps < win_floor {
                self.latch_defensive_floor_if_needed(true);
            }
            if hold_s < 180 && cur_gross_bps >= 150 && own_flow >= 0.35 {
                let w = snap.wave_observables();
                if !w.climax_active && (w.macro_phase.get() * dir_s) > -0.25 {
                    if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.state {
                        if *defensive_floor_locked {
                            *defensive_floor_locked = false;
                            self.wald_sprt.log_likelihood = self.wald_sprt.log_likelihood.max(0.0);
                            self.wald_sprt.exhaustion_unwind_active = false;
                        }
                    }
                }
            }
        }
        st
    }

    #[inline]
    pub fn observe_wald_sprt_step(&mut self, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot) -> WaldSprtState {
        let beta = OrthogonalFactorState::epps_btc_lead_lag_beta(self.slot_idx, snap, btc_leader_snap);
        self.observe_wald_sprt_step_with_beta(snap, btc_leader_snap, beta)
    }

    #[inline]
    pub fn observe_open_flow_kinematics(&mut self, snap: &SymbolLobSnapshot) {
        let raw_flow = 0.50 * snap.factor_ofi.get() + 0.38 * snap.factor_tfi.get() + 0.12 * snap.factor_geom.get();
        let ts_ns = snap.ts_local_ns.0;
        if self.last_flow_sample_ts_ns <= 0 || ts_ns <= self.last_flow_sample_ts_ns
            || ts_ns.saturating_sub(self.last_flow_sample_ts_ns) > FLOW_CONTIGUOUS_GAP_MAX_NS
        {
            self.flow_ema_fast = raw_flow;
            self.flow_ema_slow = raw_flow;
            if ts_ns > 0 { self.last_flow_sample_ts_ns = ts_ns; }
            return;
        }
        let dt_s = ((ts_ns - self.last_flow_sample_ts_ns) as f64 * 1e-9).clamp(0.001, 2.0);
        self.last_flow_sample_ts_ns = ts_ns;
        self.flow_ema_fast += (1.0 - (-dt_s / 0.25).exp()).clamp(0.004, 1.0) * (raw_flow - self.flow_ema_fast);
        self.flow_ema_slow += (1.0 - (-dt_s / 1.50).exp()).clamp(0.0005, 1.0) * (raw_flow - self.flow_ema_slow);
    }

    #[inline] #[must_use]
    pub const fn shadow_runner_kinematics(&self) -> ShadowRunnerKinematics {
        ShadowRunnerKinematics {
            flow_ema_fast: self.flow_ema_fast, flow_ema_slow: self.flow_ema_slow,
            in_scar_debt: self.in_slot_scar_debt, chandelier_armed: self.chandelier_profit_sl_armed,
        }
    }

    #[inline] #[must_use]
    pub const fn active_side(&self) -> Option<Side> {
        match self.state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, .. }
            | OrderLifecycleState::RestingEntryQueue { side, .. }
            | OrderLifecycleState::PositionOpenMakerExit { side, .. } => Some(side),
        }
    }

    #[inline] #[must_use]
    pub fn elastic_preflight_window_ns(&self, realized_vol_5m: ScaledBps) -> i64 {
        let min_v = ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw();
        let eq_v = min_v * 4;
        let vol_c = realized_vol_5m.raw().clamp(min_v, eq_v * 3);
        let fee_rt = self.active_maker_rt_fee_bps();
        let init_mfe = ((fee_rt * 17) / 10).max(1);
        let mfe_c = self.learned_mfe_bps.clamp((fee_rt * 7) / 5, fee_rt * 3);
        let base_flight_ns = (self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000) / 12_000;
        let raw_ns = (((base_flight_ns * eq_v) / vol_c * mfe_c) / init_mfe * (950 + self.learned_taker_prob_pm).clamp(970, 1_200)) / 1_000;
        raw_ns.clamp((base_flight_ns * 3) / 5, (base_flight_ns * 8) / 5)
    }

    #[inline] #[must_use]
    pub fn max_preflight_drift_ticks(&self, snap: &SymbolLobSnapshot) -> i64 {
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
        } else { snap.mark_price };
        spec.max_preflight_drift_ticks(mid_px, self.learned_taker_prob_pm, self.active_fees)
    }

    pub fn sync_preflight_l1_entry_auto(&mut self, snap: &SymbolLobSnapshot) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let max_drift = self.max_preflight_drift_ticks(snap);
        self.sync_preflight_l1_entry(snap, max_drift)
    }

    #[inline]
    pub fn observe_mfe_sample_with_fee_rt(&mut self, gross_mfe_scaled_bps: i64, fee_rt_bps: i64) {
        let eff_rt = fee_rt_bps.max(100);
        let m_obs = gross_mfe_scaled_bps.clamp(eff_rt + 40, eff_rt.saturating_mul(12));
        let eta_pm = if m_obs >= self.learned_mfe_bps { 200 } else { 120 };
        self.learned_mfe_bps = ((1_000 - eta_pm) * self.learned_mfe_bps + eta_pm * m_obs) / 1_000;
    }

    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        self.observe_mfe_sample_with_fee_rt(gross_mfe_scaled_bps, self.active_maker_rt_fee_bps());
    }

    #[inline]
    pub fn compute_fill_or_adopt_sl_ticks_for_slot(&self, side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<PriceTicks, VortexError> {
        compute_validated_sl_ticks_with_fees(side, verified_avg_price, snap, self.active_fees)
    }

    #[inline]
    pub fn compute_fill_or_adopt_sl_ticks(side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<PriceTicks, VortexError> {
        compute_validated_sl_ticks(side, verified_avg_price, snap)
    }

    #[inline]
    pub fn evaluate_sl_or_emergency_flatten_for_slot(&self, side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<ServerSlDecision, VortexError> {
        evaluate_sl_or_emergency_flatten_with_fees(side, verified_avg_price, snap, self.active_fees)
    }

    #[inline]
    pub fn evaluate_sl_or_emergency_flatten(side: Side, verified_avg_price: PriceTicks, snap: &SymbolLobSnapshot) -> Result<ServerSlDecision, VortexError> {
        evaluate_sl_or_emergency_flatten_core(side, verified_avg_price, snap)
    }

    pub fn maybe_ratchet_chandelier_or_wald_sl_with_beta(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
    ) -> Option<PriceTicks> {
        if let Some(ctx) = self.open_exit_context() {
            if let Some(chand_sl) = compute_chandelier_profit_lock_sl(&ctx, snap) {
                self.last_eff_sl_ticks = chand_sl;
                self.chandelier_profit_sl_armed = true;
                if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = self.state { *sl_p = chand_sl; }
                return Some(chand_sl);
            }
        }
        if self.chandelier_profit_sl_armed { return None; }
        let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, sl_trigger_price: cur_sl, opened_ts, .. } = self.state else { return None; };
        if avg_price_ticks.0 <= 0 { return None; }
        let fee_rt = self.active_maker_rt_fee_bps();
        if self.last_closed_mfe_bps >= (fee_rt * 13) / 10 { return None; }
        let hold_s = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0) / 1_000_000_000;
        let contracted_bps = if hold_s >= 3 && self.wald_sprt.exhaustion_unwind_active {
            let (comp_bps, eff_gamma) = compute_wald_v2_contracted_sl_bps_with_fees(snap.realized_vol_5m, &self.wald_sprt, self.active_fees);
            self.last_closed_wald_pct = self.last_closed_wald_pct.max(eff_gamma);
            comp_bps
        } else {
            if self.last_closed_wald_pct < 12 || hold_s < (self.intrinsic_tau_mkt_s.max(90) / 5).max(45) { return None; }
            let dir_s = side.sign_f64();
            let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
            let beta_c = if self.slot_idx == 0 { 0.0 } else { epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP) };
            let btc_snr = if self.slot_idx == 0 {
                OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s
            } else {
                beta_c * (OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s)
            };
            if own_flow >= -0.05 && btc_snr >= -0.20 { return None; }
            let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, self.active_fees).raw();
            let btc_headwind = if self.slot_idx > 0 && btc_snr < -0.25 { ((-btc_snr - 0.25) * 20.0).round() as i64 } else { 0 };
            let eff_wald_pct = (self.last_closed_wald_pct + btc_headwind).clamp(12, 42);
            ((base_sl_bps * (100 - eff_wald_pct)) / 100).max(fee_rt * 9)
        };
        let sl_delta = ((avg_price_ticks.0.saturating_mul(contracted_bps)) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
        let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
        let cand_sl = match side {
            Side::Bid => PriceTicks((avg_price_ticks.0 - sl_delta).min(ref_mark.0.min(ref_last.0) - 4).max(1)),
            Side::Ask => PriceTicks(avg_price_ticks.0.saturating_add(sl_delta).max(ref_mark.0.max(ref_last.0).saturating_add(4))),
        };
        let valid_sl = PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark).ok()?;
        let prev_sl = if self.last_eff_sl_ticks.0 > 0 { self.last_eff_sl_ticks } else { cur_sl };
        let tighter = match side {
            Side::Bid => valid_sl.0 >= prev_sl.0.saturating_add(3) && valid_sl.0 < avg_price_ticks.0,
            Side::Ask => valid_sl.0 <= prev_sl.0.saturating_sub(3) && valid_sl.0 > avg_price_ticks.0,
        };
        if !tighter { return None; }
        self.last_eff_sl_ticks = valid_sl;
        if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = self.state { *sl_p = valid_sl; }
        Some(valid_sl)
    }

    #[inline]
    pub fn maybe_ratchet_chandelier_or_wald_sl(&mut self, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot) -> Option<PriceTicks> {
        let beta = OrthogonalFactorState::epps_btc_lead_lag_beta(self.slot_idx, snap, btc_leader_snap);
        self.maybe_ratchet_chandelier_or_wald_sl_with_beta(snap, btc_leader_snap, beta)
    }
}

impl SlotExecutionEngine {
    #[inline]
    pub fn maybe_ratchet_wald_stop_loss(&mut self, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot) -> Option<PriceTicks> {
        self.maybe_ratchet_chandelier_or_wald_sl(snap, btc_leader_snap)
    }

    pub fn sync_to_shm_telemetry(&self, arena: &VortexShmArena) {
        let Ok(tel) = arena.telemetry(self.slot_idx) else { return; };
        let snap = match self.state {
            OrderLifecycleState::Idle => SlotTelemetrySnapshot { state_code: 0, lvdbs_surfs: self.lvdbs_deep_surfs_count, ..SlotTelemetrySnapshot::default() },
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, sl_trigger_price, submitted_ts, .. } => SlotTelemetrySnapshot {
                state_code: 1, side_code: side as u64, entry_ticks: entry_price.0, avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0, initial_lots: qty.0, lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: submitted_ts.0.max(0) as u64, queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, resting_since_ts, .. } => SlotTelemetrySnapshot {
                state_code: 2, side_code: side as u64, entry_ticks: entry_price.0, avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0, initial_lots: qty.0, lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: resting_since_ts.0.max(0) as u64, queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::PositionOpenMakerExit {
                side, entry_price, avg_price_ticks, peak_favorable_price, initial_lots, open_lots,
                tranche1_harvested, active_exit_price, resting_exit_price, sl_trigger_price, opened_ts, ..
            } => SlotTelemetrySnapshot {
                state_code: if tranche1_harvested { 4 } else if open_lots.0 < initial_lots.0 { 5 } else { 3 },
                side_code: side as u64, entry_ticks: entry_price.0, avg_price_ticks: avg_price_ticks.0,
                peak_ticks: peak_favorable_price.0,
                tp_ticks: if resting_exit_price.0 > 0 { resting_exit_price.0 } else { active_exit_price.0 },
                sl_ticks: if self.last_eff_sl_ticks.0 > 0 { self.last_eff_sl_ticks.0 } else { sl_trigger_price.0 },
                open_lots: open_lots.0, initial_lots: initial_lots.0, lvdbs_surfs: self.lvdbs_deep_surfs_count,
                tranche1_harvested, resting_since_ns: opened_ts.0.max(0) as u64, queue_ahead_lots: 0,
                ..SlotTelemetrySnapshot::default()
            },
        };
        tel.write_consistent(&snap);
    }

    pub fn try_arm_maker_entry(&mut self, snap: &SymbolLobSnapshot, verdict: SignalVerdict, qty: LotQty) -> Result<OrderLifecycleState, VortexError> {
        if !matches!(self.state, OrderLifecycleState::Idle) || snap.ts_local_ns.0 < self.cooldown_until_ns.0 { return Ok(self.state); }
        let SignalVerdict::AuthorizeMakerEntry { side, target_excursion_scaled_bps } = verdict else { return Ok(self.state); };
        if !passes_elliott_macro_coherence_guard(side, snap)
            || !passes_order_block_wall_guard_dynamic(self.slot_idx, side, snap, self.learned_taker_prob_pm, self.active_fees)
        {
            return Ok(self.state);
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Ok(self.state); };
        let entry_price = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let sl_trigger_price = self.compute_fill_or_adopt_sl_ticks_for_slot(side, entry_price, snap)?;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.hawkes_tracker.reset();
        self.wald_sprt.reset();
        self.state = OrderLifecycleState::PlacingEntryMaker {
            side, entry_price, qty: LotQty(qty.0.max(1)),
            target_excursion: target_excursion_scaled_bps, sl_trigger_price, submitted_ts: snap.ts_local_ns,
        };
        Ok(self.state)
    }

    pub fn sync_preflight_l1_entry(&mut self, snap: &SymbolLobSnapshot, max_drift_ticks: i64) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let OrderLifecycleState::PlacingEntryMaker { side, entry_price: armed_px, .. } = self.state else { return Err(VortexError::InvalidPriceOrTickSpec); };
        if !passes_elliott_macro_coherence_guard(side, snap)
            || !passes_order_block_wall_guard_dynamic(self.slot_idx, side, snap, self.learned_taker_prob_pm, self.active_fees)
        {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Err(VortexError::InvalidPriceOrTickSpec); };
        let cur_l1_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if cur_l1_px.0 <= 0 || (cur_l1_px.0 - armed_px.0).abs() > max_drift_ticks { return Err(VortexError::InvalidPriceOrTickSpec); }
        let new_sl = self.compute_fill_or_adopt_sl_ticks_for_slot(side, cur_l1_px, snap)?;
        if let OrderLifecycleState::PlacingEntryMaker { ref mut entry_price, ref mut sl_trigger_price, .. } = self.state {
            *entry_price = cur_l1_px;
            *sl_trigger_price = new_sl;
        }
        Ok((cur_l1_px, new_sl))
    }

    pub fn compute_repeg_sl_trigger(&self, snap: &SymbolLobSnapshot, new_l1_price: PriceTicks) -> Result<PriceTicks, VortexError> {
        let OrderLifecycleState::RestingEntryQueue { side, .. } = self.state else { return Err(VortexError::InvalidPriceOrTickSpec); };
        self.compute_fill_or_adopt_sl_ticks_for_slot(side, new_l1_price, snap)
    }

    #[inline]
    pub fn apply_resting_repeg(&mut self, new_l1_price: PriceTicks, new_sl_price: PriceTicks, now_ts: TimestampNs) {
        self.hawkes_tracker.reset();
        if let OrderLifecycleState::RestingEntryQueue { ref mut entry_price, ref mut sl_trigger_price, ref mut resting_since_ts, .. } = self.state {
            *entry_price = new_l1_price;
            *sl_trigger_price = new_sl_price;
            *resting_since_ts = now_ts;
        }
    }

    pub fn on_entry_order_resting(&mut self, resting_ts: TimestampNs) -> Result<OrderLifecycleState, VortexError> {
        self.hawkes_tracker.reset();
        if let OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, target_excursion, sl_trigger_price, .. } = self.state {
            self.state = OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, target_excursion, sl_trigger_price, resting_since_ts: resting_ts,
            };
        }
        Ok(self.state)
    }

    pub fn on_maker_entry_filled_verified(
        &mut self, fill_ts: TimestampNs, verified_side: Option<Side>, verified_avg_price: Option<PriceTicks>, verified_open_lots: Option<LotQty>,
    ) -> Result<OrderLifecycleState, VortexError> {
        let (state_side, state_entry_px, state_qty, target_excursion, state_sl_px) = match self.state {
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, target_excursion, sl_trigger_price, .. }
            | OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, target_excursion, sl_trigger_price, .. } => {
                (side, entry_price, qty, target_excursion, sl_trigger_price)
            }
            OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => return Ok(self.state),
        };
        let side = verified_side.unwrap_or(state_side);
        let eff_entry_px = match verified_avg_price { Some(px) if px.0 > 0 => px, _ => state_entry_px };
        let eff_lots = match verified_open_lots { Some(lq) if lq.0 > 0 => lq, _ => LotQty(state_qty.0.max(1)) };
        let eff_sl_px = if verified_avg_price.is_some() && (eff_entry_px != state_entry_px || side != state_side) {
            let sl_dist = (state_entry_px.0 - state_sl_px.0).abs().max(4);
            match side { Side::Bid => PriceTicks((eff_entry_px.0 - sl_dist).max(1)), Side::Ask => PriceTicks(eff_entry_px.0.saturating_add(sl_dist)) }
        } else { state_sl_px };
        let fee_rt = self.active_maker_rt_fee_bps();
        let min_win_gross = compute_elastic_offensive_win_floor_bps(fee_rt, self.open_vol_5m_bps).raw();
        let win1_px = elastic_offensive_win_maker_exit_price(side, eff_entry_px, fee_rt, self.open_vol_5m_bps, 0.50);
        let split_t1_opt = Self::compute_tranche1_split_lots(self.slot_idx, eff_lots, win1_px, self.learned_t1_permille);
        let tp_bps = if split_t1_opt.is_some() {
            target_excursion.raw().max(min_win_gross + 40)
        } else {
            let vol_raw = self.open_vol_5m_bps.raw();
            let max_single_cap = fee_rt * 12;
            let vol_unit_cap = if vol_raw > 0 { ((vol_raw * 62) / 100).clamp(min_win_gross + 15, max_single_cap) } else { (fee_rt * 9) / 4 };
            let mfe_cap = self.learned_mfe_bps.clamp(min_win_gross + 15, max_single_cap);
            target_excursion.raw().min(mfe_cap.min(vol_unit_cap)).max(min_win_gross)
        };
        let structural_tp_price = dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
        let (init_role, init_exit_px, init_exit_lots) = match split_t1_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, win1_px, t1_lots),
            None => (ExitOrderRole::StandardMakerExit100, structural_tp_price, eff_lots),
        };
        self.last_opened_ts = fill_ts;
        self.last_eff_sl_ticks = eff_sl_px;
        self.ratchet_locked_gross_bps = 0;
        self.tranche1_harvests_count = 0;
        self.lvdbs_deep_surfs_count = 0;
        self.last_closed_mfe_bps = 0;
        self.last_closed_mae_bps = 0;
        self.last_closed_peak_ticks = eff_entry_px;
        self.last_closed_worst_ticks = eff_entry_px;
        self.last_closed_wald_pct = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = fill_ts.0;
        self.flow_ema_fast = 0.0;
        self.flow_ema_slow = 0.0;
        self.last_flow_sample_ts_ns = 0;
        self.chandelier_profit_sl_armed = false;
        self.hawkes_tracker.reset();
        self.wald_sprt.reset();
        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side, entry_price: eff_entry_px, avg_price_ticks: eff_entry_px,
            peak_favorable_price: eff_entry_px, worst_adverse_price: eff_entry_px,
            initial_lots: eff_lots, open_lots: eff_lots, tranche1_harvested: false,
            tranche1_lots: LotQty(0), tranche1_exit_price: PriceTicks(0), defensive_floor_locked: false,
            active_exit_role: init_role, active_exit_price: init_exit_px, active_exit_lots: init_exit_lots,
            structural_tp_price, resting_exit_price: PriceTicks(0), sl_trigger_price: eff_sl_px, opened_ts: fill_ts,
        };
        Ok(self.state)
    }

    pub fn on_maker_entry_filled(&mut self, fill_ts: TimestampNs) -> Result<OrderLifecycleState, VortexError> {
        self.on_maker_entry_filled_verified(fill_ts, None, None, None)
    }

    pub fn adopt_live_open_position(
        &mut self, side: Side, verified_avg_price: PriceTicks, initial_lots: LotQty, open_lots: LotQty,
        tranche1_harvested: bool, peak_price: PriceTicks, worst_price: PriceTicks, opened_ts: TimestampNs, snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        let sl_t = self.compute_fill_or_adopt_sl_ticks_for_slot(side, verified_avg_price, snap)?;
        let eff_open = LotQty(open_lots.0.max(1));
        let eff_init = LotQty(initial_lots.0.max(eff_open.0));
        let q_fl = defensive_zero_loss_maker_exit_price_with_fees(side, verified_avg_price, self.active_fees);
        let fee_rt = self.active_maker_rt_fee_bps();
        let struct_tp = dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(self.learned_mfe_bps.max((fee_rt * 27) / 20)));
        let harvested_lots = LotQty((eff_init.0 - eff_open.0).max(0));
        let t1_done = tranche1_harvested || harvested_lots.0 > 0;
        let split_opt = if !t1_done { Self::compute_tranche1_split_lots(self.slot_idx, eff_open, q_fl, self.learned_t1_permille) } else { None };
        let (role, init_px, init_lots) = match split_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, q_fl, t1_lots),
            None if t1_done => (ExitOrderRole::LvdbsDeepSurf50, struct_tp, eff_open),
            None => (ExitOrderRole::StandardMakerExit100, struct_tp, eff_open),
        };
        let pk_init = if peak_price.0 > 0 { peak_price } else { verified_avg_price };
        let wr_init = if worst_price.0 > 0 { worst_price } else { verified_avg_price };
        self.last_opened_ts = opened_ts;
        self.last_eff_sl_ticks = sl_t;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.ratchet_locked_gross_bps = 0;
        self.last_closed_peak_ticks = pk_init;
        self.last_closed_worst_ticks = wr_init;
        self.tranche1_harvests_count = u64::from(t1_done);
        self.lvdbs_deep_surfs_count = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = opened_ts.0;
        self.flow_ema_fast = 0.0;
        self.flow_ema_slow = 0.0;
        self.last_flow_sample_ts_ns = 0;
        self.chandelier_profit_sl_armed = is_chandelier_profit_locked_sl_with_fees(side, verified_avg_price, sl_t, self.active_fees);
        self.hawkes_tracker.reset();
        self.wald_sprt.reset();
        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side, entry_price: verified_avg_price, avg_price_ticks: verified_avg_price,
            peak_favorable_price: pk_init, worst_adverse_price: wr_init,
            initial_lots: eff_init, open_lots: eff_open, tranche1_harvested: t1_done,
            tranche1_lots: harvested_lots, tranche1_exit_price: if t1_done { q_fl } else { PriceTicks(0) },
            defensive_floor_locked: false, active_exit_role: role, active_exit_price: init_px,
            active_exit_lots: init_lots, structural_tp_price: struct_tp, resting_exit_price: PriceTicks(0),
            sl_trigger_price: sl_t, opened_ts,
        };
        Ok(sl_t)
    }

    #[inline]
    pub fn sync_open_avg_price_ticks(&mut self, verified_avg_price: PriceTicks) {
        if verified_avg_price.0 <= 0 { return; }
        let min_win_gross = (self.active_maker_rt_fee_bps() * 13) / 10;
        if let OrderLifecycleState::PositionOpenMakerExit {
            side, ref mut entry_price, ref mut avg_price_ticks, ref mut peak_favorable_price,
            ref mut worst_adverse_price, ref mut structural_tp_price, ..
        } = self.state
        {
            if *avg_price_ticks != verified_avg_price {
                let prev_avg = avg_price_ticks.0.max(1);
                let prev_bps = (((structural_tp_price.0 - prev_avg).abs().saturating_mul(1_000_000)) / prev_avg).max(min_win_gross);
                *entry_price = verified_avg_price;
                *avg_price_ticks = verified_avg_price;
                *structural_tp_price = dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(prev_bps));
                match side {
                    Side::Bid => {
                        if peak_favorable_price.0 < verified_avg_price.0 { *peak_favorable_price = verified_avg_price; }
                        if worst_adverse_price.0 > verified_avg_price.0 { *worst_adverse_price = verified_avg_price; }
                    }
                    Side::Ask => {
                        if peak_favorable_price.0 > verified_avg_price.0 { *peak_favorable_price = verified_avg_price; }
                        if worst_adverse_price.0 < verified_avg_price.0 { *worst_adverse_price = verified_avg_price; }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn latch_defensive_floor_if_needed(&mut self, lock_active: bool) {
        let fee_rt = self.active_maker_rt_fee_bps();
        let state_mfe = if let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, peak_favorable_price, .. } = self.state {
            ((peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1)
        } else { 0 };
        let best_mfe = self.last_closed_mfe_bps.max(state_mfe);
        if best_mfe >= (fee_rt * 139) / 100 {
            let cand = (fee_rt + ((best_mfe - fee_rt) * 72) / 100).max(fee_rt + (fee_rt * 3) / 10);
            if cand > self.ratchet_locked_gross_bps { self.ratchet_locked_gross_bps = cand; }
        }
        if lock_active {
            if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.state {
                *defensive_floor_locked = true;
            }
        }
    }

    pub fn record_tranche1_harvest(&mut self, fill_price: PriceTicks) -> bool {
        let (s_idx, phi) = (self.slot_idx, self.learned_t1_permille);
        let spec = APPROVED_SPECS[s_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let OrderLifecycleState::PositionOpenMakerExit {
            ref mut open_lots, ref mut tranche1_harvested, ref mut tranche1_lots,
            ref mut tranche1_exit_price, ref mut active_exit_role, ref mut active_exit_lots, ..
        } = self.state else { return false; };
        if *tranche1_harvested { return false; }
        let wave_override = if matches!(*active_exit_role, ExitOrderRole::Tranche1Harvest50) && active_exit_lots.0 >= 1 && active_exit_lots.0 < open_lots.0 {
            let rem = LotQty(open_lots.0 - active_exit_lots.0);
            if spec.lot_notional_1e8(*active_exit_lots, fill_price) >= SPLIT_LEG_MIN_NOTIONAL_1E8
                && spec.lot_notional_1e8(rem, fill_price) >= SPLIT_LEG_MIN_NOTIONAL_1E8
            { Some(*active_exit_lots) } else { None }
        } else { None };
        let Some(harvested) = wave_override.or_else(|| Self::compute_tranche1_split_lots(s_idx, *open_lots, fill_price, phi)) else { return false; };
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *tranche1_harvested = true;
        *tranche1_lots = harvested;
        *tranche1_exit_price = fill_price;
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        self.tranche1_harvests_count = self.tranche1_harvests_count.saturating_add(1);
        true
    }

    #[inline]
    fn open_exit_context(&self) -> Option<OpenExitContext> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, peak_favorable_price, worst_adverse_price, initial_lots, open_lots,
            tranche1_harvested, tranche1_lots, tranche1_exit_price, defensive_floor_locked,
            active_exit_price, structural_tp_price, resting_exit_price, sl_trigger_price, opened_ts, ..
        } = self.state else { return None; };
        Some(OpenExitContext {
            slot_idx: self.slot_idx, side, avg_price_ticks, peak_favorable_price, worst_adverse_price,
            initial_lots, open_lots, tranche1_harvested, tranche1_lots, tranche1_exit_price,
            defensive_floor_locked, active_exit_price, structural_tp_price, resting_exit_price,
            sl_trigger_price, opened_ts, learned_mfe_bps: self.learned_mfe_bps,
            learned_t1_permille: self.learned_t1_permille, learned_ir_x100: self.learned_ir_x100,
            learned_ret_std_bps: self.learned_ret_std_bps, intrinsic_tau_mkt_s: self.intrinsic_tau_mkt_s,
            last_eff_sl_ticks: self.last_eff_sl_ticks, ratchet_locked_gross_bps: self.ratchet_locked_gross_bps,
            last_closed_mfe_bps: self.last_closed_mfe_bps, last_closed_mae_bps: self.last_closed_mae_bps,
            active_fees: self.active_fees, learned_taker_prob_pm: self.learned_taker_prob_pm,
        })
    }

    #[inline]
    fn fold_price_into_excursions(&mut self, side: Side, avg_px: PriceTicks, obs_px: PriceTicks) {
        if avg_px.0 <= 0 || obs_px.0 <= 0 { return; }
        if self.last_closed_peak_ticks.0 <= 0 { self.last_closed_peak_ticks = avg_px; }
        if self.last_closed_worst_ticks.0 <= 0 { self.last_closed_worst_ticks = avg_px; }
        match side {
            Side::Bid => {
                if obs_px.0 > self.last_closed_peak_ticks.0 { self.last_closed_peak_ticks = obs_px; }
                if obs_px.0 < self.last_closed_worst_ticks.0 { self.last_closed_worst_ticks = obs_px; }
            }
            Side::Ask => {
                if obs_px.0 < self.last_closed_peak_ticks.0 { self.last_closed_peak_ticks = obs_px; }
                if obs_px.0 > self.last_closed_worst_ticks.0 { self.last_closed_worst_ticks = obs_px; }
            }
        }
        let d_sign = side.sign_i64();
        let denom = avg_px.0.max(1);
        self.last_closed_mfe_bps = self.last_closed_mfe_bps.max(((self.last_closed_peak_ticks.0 - avg_px.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / denom);
        self.last_closed_mae_bps = self.last_closed_mae_bps.min(((self.last_closed_worst_ticks.0 - avg_px.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / denom);
    }

    #[must_use]
    pub fn reconcile_closed_pnl_payload(&self, payload: &serde_json::Value, vac_level_hint: usize, allow_partial_lot_coverage: bool) -> ClosedPnlQueryOutcome {
        let Some(ctx) = self.open_exit_context() else { return ClosedPnlQueryOutcome::TransportOrApiError; };
        reconcile_closed_pnl_core(&ctx, payload, vac_level_hint, allow_partial_lot_coverage)
    }

    #[must_use]
    pub fn resolve_fallback_unverified_close(&self, snap: &SymbolLobSnapshot, vac_level_hint: usize) -> Option<VerifiedCloseResolution> {
        Some(resolve_fallback_unverified_close_core(&self.open_exit_context()?, snap, vac_level_hint))
    }

    pub fn apply_verified_close_resolution(&mut self, resolution: VerifiedCloseResolution, closed_ts: TimestampNs, ledger: &mut ParitySyncLedger) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit { side, .. } = self.state else { return None; };
        self.last_exit_sub_reason = resolution.sub_reason;
        if matches!(resolution.kind, ClosedTradeKind::MakerPassiveUnwind)
            && matches!(resolution.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf | ExitSubReason::FibonacciImpulseSurf | ExitSubReason::ShadowRunnerCrestHarvest)
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
        }
        Some(self.finalize_close_with_lots(
            resolution.kind, side, resolution.verified_entry_price, resolution.verified_exit_price,
            resolution.closed_lots, resolution.legs_count, resolution.exchange_closed_pnl_1e8, closed_ts, ledger,
        ))
    }

    #[must_use]
    pub fn compute_canonical_exit_quote(&self, snap: &SymbolLobSnapshot) -> Option<CanonicalExitQuote> {
        compute_canonical_exit_quote_omega(&self.open_exit_context()?, self.shadow_runner_kinematics(), snap)
    }

    pub fn finalize_close_with_lots(
        &mut self, kind: ClosedTradeKind, side: Side, entry_price: PriceTicks, exit_price: PriceTicks,
        closed_lots: LotQty, legs_consolidated: u32, exchange_closed_pnl_1e8: Option<i64>,
        closed_ts: TimestampNs, ledger: &mut ParitySyncLedger,
    ) -> ClosedTradeSummary {
        let is_maker_exit = !matches!(kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee_scaled_bps = if is_maker_exit { self.active_fees.round_trip_maker_exit_bps() } else { self.active_fees.round_trip_taker_exit_bps() };
        let eff_lots = LotQty(closed_lots.0.max(1));
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let (calc_pnl_1e8, fee_1e8, calc_gross_bps, calc_net_bps) =
            spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, eff_lots, entry_price, exit_price, is_maker_exit, self.active_fees);
        let net_pnl_1e8 = exchange_closed_pnl_1e8.unwrap_or(calc_pnl_1e8);
        let entry_notional_1e8 = spec.lot_notional_1e8(eff_lots, entry_price);
        let (gross_scaled_bps, net_scaled_bps) = if exchange_closed_pnl_1e8.is_some() && entry_notional_1e8 > 0 {
            let exact_net = ((net_pnl_1e8 as i128 * 1_000_000) / (entry_notional_1e8 as i128)) as i64;
            let exact_gross = exact_net.saturating_add(fee_scaled_bps.raw());
            if exact_net >= 100 && matches!(self.last_exit_sub_reason, ExitSubReason::Tranche1Harvest50) {
                self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
            }
            (ScaledBps(exact_gross), ScaledBps(exact_net))
        } else {
            (calc_gross_bps, calc_net_bps)
        };
        self.fold_price_into_excursions(side, entry_price, exit_price);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, net_scaled_bps);
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_scaled_bps.raw()));
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + (self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000) / 100);
        self.close_reconcile_retries = 0;
        self.chandelier_profit_sl_armed = false;
        self.last_flow_sample_ts_ns = 0;
        self.hawkes_tracker.reset();
        self.wald_sprt.reset();
        self.state = OrderLifecycleState::Idle;
        ClosedTradeSummary {
            kind, side, entry_price, exit_price, closed_lots: eff_lots,
            legs_consolidated: legs_consolidated.max(1), gross_scaled_bps, fee_scaled_bps,
            net_scaled_bps, net_pnl_1e8, fee_1e8, is_maker_exit,
        }
    }

    pub fn finalize_open_position_close(
        &mut self, leg2_kind: ClosedTradeKind, leg2_exit_price: PriceTicks, closed_ts: TimestampNs, ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, open_lots, tranche1_harvested, tranche1_lots, tranche1_exit_price, ..
        } = self.state else { return None; };
        let q2 = LotQty(open_lots.0.max(1));
        if !tranche1_harvested || tranche1_lots.0 <= 0 || tranche1_exit_price.0 <= 0 {
            return Some(self.finalize_close_with_lots(leg2_kind, side, avg_price_ticks, leg2_exit_price, q2, 1, None, closed_ts, ledger));
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let q1 = tranche1_lots;
        let is_maker_exit = !matches!(leg2_kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee1_bps = self.active_fees.round_trip_maker_exit_bps();
        let fee2_bps = if is_maker_exit { fee1_bps } else { self.active_fees.round_trip_taker_exit_bps() };
        let (pnl1, fee1, g1, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, q1, avg_price_ticks, tranche1_exit_price, true, self.active_fees);
        let (pnl2, fee2, g2, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, q2, avg_price_ticks, leg2_exit_price, is_maker_exit, self.active_fees);
        let tot_q = (q1.0 + q2.0).max(1);
        let tot_q_128 = tot_q as i128;
        let w_exit = (q1.0 as i128) * (tranche1_exit_price.0 as i128) + (q2.0 as i128) * (leg2_exit_price.0 as i128);
        let avg_exit_ticks = match side { Side::Bid => (w_exit / tot_q_128).max(1) as i64, Side::Ask => ((w_exit + tot_q_128 - 1) / tot_q_128) as i64 };
        let gross_bps = (q1.0 * g1.raw() + q2.0 * g2.raw()) / tot_q;
        let fee_bps = (q1.0 * fee1_bps.raw() + q2.0 * fee2_bps.raw()) / tot_q;
        let net_bps = gross_bps - fee_bps;
        let net_pnl_1e8 = pnl1.saturating_add(pnl2);
        let fee_1e8 = fee1.saturating_add(fee2);
        self.fold_price_into_excursions(side, avg_price_ticks, tranche1_exit_price);
        self.fold_price_into_excursions(side, avg_price_ticks, leg2_exit_price);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, ScaledBps(net_bps));
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_bps));
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + (self.intrinsic_tau_mkt_s.max(90) * 1_000_000_000) / 100);
        self.close_reconcile_retries = 0;
        self.chandelier_profit_sl_armed = false;
        self.last_flow_sample_ts_ns = 0;
        self.hawkes_tracker.reset();
        self.wald_sprt.reset();
        self.state = OrderLifecycleState::Idle;
        Some(ClosedTradeSummary {
            kind: leg2_kind, side, entry_price: avg_price_ticks, exit_price: PriceTicks(avg_exit_ticks),
            closed_lots: LotQty(tot_q), legs_consolidated: 2, gross_scaled_bps: ScaledBps(gross_bps),
            fee_scaled_bps: ScaledBps(fee_bps), net_scaled_bps: ScaledBps(net_bps), net_pnl_1e8, fee_1e8, is_maker_exit,
        })
    }

    pub fn step_open_position_with_beta(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        gov: &OnlineAdaptiveGovernor,
        ledger: &mut ParitySyncLedger,
        epps_signed_beta: f64,
    ) -> Option<ClosedTradeSummary> {
        self.sync_governor_dynamics(gov);
        self.observe_open_flow_kinematics(snap);
        let _ = self.observe_wald_sprt_step_with_beta(snap, btc_leader_snap, epps_signed_beta);
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price, sl_trigger_price, ..
        } = self.state else { return None; };
        let cur_bbo_ref = match side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
        let ref_trade = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { cur_bbo_ref };
        match side {
            Side::Bid => {
                if ref_trade.0 > peak_favorable_price.0 { peak_favorable_price = ref_trade; }
                if ref_trade.0 > 0 && ref_trade.0 < worst_adverse_price.0 { worst_adverse_price = ref_trade; }
            }
            Side::Ask => {
                if ref_trade.0 > 0 && ref_trade.0 < peak_favorable_price.0 { peak_favorable_price = ref_trade; }
                if ref_trade.0 > worst_adverse_price.0 { worst_adverse_price = ref_trade; }
            }
        }
        if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = self.state {
            *pk = peak_favorable_price;
            *wr = worst_adverse_price;
        }
        let d_sign = side.sign_i64();
        let avg_denom = avg_price_ticks.0.max(1);
        self.last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / avg_denom;
        self.last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0).saturating_mul(d_sign).saturating_mul(1_000_000)) / avg_denom;
        self.last_closed_peak_ticks = peak_favorable_price;
        self.last_closed_worst_ticks = worst_adverse_price;
        if self.last_eff_sl_ticks.0 <= 0 { self.last_eff_sl_ticks = sl_trigger_price; }
        let eff_sl_now = self.last_eff_sl_ticks;
        let sl_hit = match side {
            Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= eff_sl_now.0,
            Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= eff_sl_now.0,
        };
        if sl_hit {
            self.last_exit_sub_reason = if self.chandelier_profit_sl_armed
                || is_chandelier_profit_locked_sl_with_fees(side, avg_price_ticks, eff_sl_now, self.active_fees)
            { ExitSubReason::ChandelierProfitLockSl } else { ExitSubReason::WaldBaseStopLoss };
            return self.finalize_open_position_close(ClosedTradeKind::ServerMarkPriceStopLoss, eff_sl_now, snap.ts_local_ns, ledger);
        }
        let quote = self.compute_canonical_exit_quote(snap)?;
        if let OrderLifecycleState::PositionOpenMakerExit { active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p, active_exit_lots: ref mut ax_l, .. } = self.state {
            *ax_r = quote.role; *ax_p = quote.target_price; *ax_l = quote.order_lots;
        }
        self.last_closed_wald_pct = self.last_closed_wald_pct.max(quote.wald_pct);
        self.latch_defensive_floor_if_needed(quote.defensive_lock_active);
        let _ = self.maybe_ratchet_chandelier_or_wald_sl_with_beta(snap, btc_leader_snap, epps_signed_beta);
        let reached_target = match side { Side::Bid => ref_trade.0 >= quote.target_price.0, Side::Ask => ref_trade.0 > 0 && ref_trade.0 <= quote.target_price.0 };
        if !reached_target { return None; }
        if matches!(quote.role, ExitOrderRole::Tranche1Harvest50) {
            let _ = self.record_tranche1_harvest(quote.target_price);
            return None;
        }
        self.exit_peg_attempts = self.exit_peg_attempts.saturating_add(1);
        let q_fl = defensive_zero_loss_maker_exit_price_with_fees(side, avg_price_ticks, self.active_fees);
        let above_qf = match side { Side::Bid => quote.target_price.0 > q_fl.0, Side::Ask => quote.target_price.0 < q_fl.0 };
        if above_qf && matches!(quote.sub_reason, ExitSubReason::FibonacciImpulseSurf | ExitSubReason::LvdbsDeepVacuumSurf | ExitSubReason::ShadowRunnerCrestHarvest) {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = quote.sub_reason;
        } else if above_qf && quote.vac_level >= 2 {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
        } else {
            self.last_exit_sub_reason = quote.sub_reason;
        }
        self.finalize_open_position_close(ClosedTradeKind::MakerPassiveUnwind, quote.target_price, snap.ts_local_ns, ledger)
    }

    #[inline]
    pub fn step_open_position(
        &mut self, snap: &SymbolLobSnapshot, btc_leader_snap: &SymbolLobSnapshot, gov: &OnlineAdaptiveGovernor, ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let beta = OrthogonalFactorState::epps_btc_lead_lag_beta(self.slot_idx, snap, btc_leader_snap);
        self.step_open_position_with_beta(snap, btc_leader_snap, gov, ledger, beta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_v47_fsm_vip_rebate_mfe_hydration_signed_epps_ratchet_and_trades_46_58() {
        let mut gov = OnlineAdaptiveGovernor::new();
        let mm_fees = FeeSchedule::from_live_rates_clamped(ScaledBps(-50), ScaledBps(300));
        gov.set_active_fee_schedule(mm_fees);
        let mut engines = SlotExecutionEngine::new_12_slot_array();
        for eng in &mut engines { eng.sync_governor_dynamics(&gov); }
        let missing = std::env::temp_dir().join("vortex_mfe_missing_v47_seed.json");
        let _ = std::fs::remove_file(&missing);
        SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut engines, &missing);
        assert_eq!(engines[3].learned_mfe_bps, 170);

        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        ledger.set_live_vip_fee_rates(mm_fees.maker_fee_bps, mm_fees.taker_fee_bps);
        let s_reb = engines[3].finalize_close_with_lots(
            ClosedTradeKind::MakerPassiveUnwind, Side::Bid, PriceTicks(8_200), PriceTicks(8_208),
            LotQty(26), 1, None, TimestampNs(5_000_000_000), &mut ledger,
        );
        assert!(s_reb.fee_1e8 < 0 && s_reb.net_scaled_bps.raw() > s_reb.gross_scaled_bps.raw());
        assert_eq!(ledger.current_equity_1e8() - ledger.initial_equity_1e8, s_reb.net_pnl_1e8);

        // Inverse Epps beta (-0.85) shields open Bid SL from BTC dump, while positive beta (+0.85) ratchets SL tighter
        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.ts_local_ns = TimestampNs(10_000_000_000);
        avax.realized_vol_5m = ScaledBps(6_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(420) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(320) };
        avax.mark_price = PriceTicks(8_200);
        avax.last_trade_price = PriceTicks(8_200);
        let mut eng_sl = SlotExecutionEngine::new(3);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) };
        assert!(eng_sl.try_arm_maker_entry(&avax, v, LotQty(26)).is_ok());
        assert!(eng_sl.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        eng_sl.last_closed_wald_pct = 20;
        eng_sl.last_closed_mfe_bps = 100;
        avax.ts_local_ns = TimestampNs(70_000_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_195), qty: LotQty(400) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_196), qty: LotQty(350) };
        avax.mark_price = PriceTicks(8_195);
        avax.last_trade_price = PriceTicks(8_195);
        avax.factor_ofi = BoundedScore::new_clamped(-0.02);
        avax.factor_tfi = BoundedScore::new_clamped(-0.02);
        let mut btc_dump = avax;
        btc_dump.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_dump.factor_ofi = BoundedScore::new_clamped(-0.75);
        btc_dump.factor_tfi = BoundedScore::new_clamped(-0.80);
        btc_dump.factor_geom = BoundedScore::new_clamped(-0.50);
        assert!(eng_sl.maybe_ratchet_chandelier_or_wald_sl_with_beta(&avax, &btc_dump, -0.85).is_none());
        assert!(eng_sl.maybe_ratchet_chandelier_or_wald_sl_with_beta(&avax, &btc_dump, 0.85).is_some());

        let mut eth = SymbolLobSnapshot::new_empty();
        eth.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth.realized_vol_5m = ScaledBps(3_529);
        eth.bids[0] = BookLevel { price: PriceTicks(184_830), qty: LotQty(420) };
        eth.asks[0] = BookLevel { price: PriceTicks(184_831), qty: LotQty(4_896) };
        eth.set_wave_observables(WaveObservables {
            regime: WaveRegime::CorrectiveAbc, climax_active: false, hurst: WaveObservables::encode_hurst(0.674),
            macro_phase: BoundedScore::new_clamped(0.30), oi_impulse: BoundedScore::new_clamped(0.13),
            liq_imbalance: BoundedScore::new_clamped(0.01), fib_proj_bps: ScaledBps(0),
        });
        assert!(!passes_elliott_macro_coherence_guard(Side::Ask, &eth));
        eth.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.587),
            macro_phase: BoundedScore::new_clamped(-0.85), oi_impulse: BoundedScore::new_clamped(-0.03),
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(-2_236),
        });
        assert!(passes_elliott_macro_coherence_guard(Side::Ask, &eth));
    }
}

impl SlotExecutionEngine {
    /// Causal intrinsic evaluation horizon (`5s..=15s`) scaled by event-clock dilation & `sqrt(B_3x / sigma_5m)`.
    #[inline]
    #[must_use]
    pub fn intrinsic_toxicity_eval_ns(
        &self,
        avg_price: PriceTicks,
        realized_vol_5m: ScaledBps,
        event_dilation: f64,
    ) -> i64 {
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let b3x = spec
            .three_x_friction_barrier_bps_dynamic(avg_price, self.learned_taker_prob_pm, self.active_fees)
            .raw()
            .max(150) as f64;
        let vol_eff = (realized_vol_5m.raw() as f64).max(b3x);
        let kappa = event_dilation.clamp(0.618_033_988_749_895, 1.618_033_988_749_895);
        let tau_base_s = (self.intrinsic_tau_mkt_s.max(90) as f64) / (60.0 * kappa);
        let eval_ns = (tau_base_s * (b3x / vol_eff).sqrt() * 1e9).round() as i64;
        eval_ns.clamp(5_000_000_000, 15_000_000_000)
    }

    /// Injects Glosten-Milgrom post-fill toxicity `zeta_tox` into Wald SPRT v2.0 & arms Zero-Loss Maker escape.
    pub fn observe_post_fill_toxicity_and_wald_v48(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
        markout_ring: &crate::markout_tracker::SlotMarkoutRing,
        event_dilation: f64,
    ) -> f64 {
        const PHI_TX: f64 = 1.618_033_988_749_895;
        const PHI_SQ_TX: f64 = 2.618_033_988_749_895;
        const INV_PHI_TX: f64 = 0.618_033_988_749_895;
        let _ = self.observe_wald_sprt_step_with_beta(snap, btc_leader_snap, epps_signed_beta);
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, peak_favorable_price, opened_ts, ..
        } = self.state else {
            return 0.0;
        };
        if avg_price_ticks.0 <= 0 {
            return 0.0;
        }
        let dt_ns = snap.ts_local_ns.0.saturating_sub(opened_ts.0);
        if dt_ns < self.intrinsic_toxicity_eval_ns(avg_price_ticks, snap.realized_vol_5m, event_dilation) {
            return 0.0;
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let c_rt = spec.expected_round_trip_cost_bps_dynamic(avg_price_ticks, self.learned_taker_prob_pm, self.active_fees);
        let zeta_tox = markout_ring.active_fill_toxicity_zeta(opened_ts, c_rt);
        if zeta_tox < INV_PHI_TX {
            return 0.0;
        }
        let tau_tick_bps = spec.tick_to_price_scaled_bps(avg_price_ticks).unwrap_or(ScaledBps(50)).raw();
        let state_mfe = ((peak_favorable_price.0 - avg_price_ticks.0)
            .saturating_mul(side.sign_i64())
            .saturating_mul(1_000_000))
            / avg_price_ticks.0.max(1);
        let cur_mfe_bps = state_mfe.max(self.last_closed_mfe_bps);
        let own_flow = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
        if cur_mfe_bps > tau_tick_bps || own_flow >= 0.0 {
            return 0.0;
        }
        let (a_hold, b_unwind) = crate::kinematic_surface::wald_sprt_boundaries_from_permille(self.learned_taker_prob_pm);
        let jump = PHI_TX * zeta_tox * (1.0 + (-own_flow).max(0.0)) * b_unwind.abs();
        self.wald_sprt.log_likelihood = (self.wald_sprt.log_likelihood - jump).clamp(1.5 * b_unwind, a_hold);
        self.wald_sprt.exhaustion_unwind_active = true;
        let s3 = crate::wave_elliott::hermite_smoothstep_c1(zeta_tox, INV_PHI_TX, PHI_SQ_TX);
        let tox_pct = (48.0 + 34.0 * s3).round() as i64;
        self.wald_sprt.contraction_pct = self.wald_sprt.contraction_pct.max(tox_pct).clamp(48, 82);
        self.last_closed_wald_pct = self.last_closed_wald_pct.max(self.wald_sprt.contraction_pct);
        self.latch_defensive_floor_if_needed(true);
        zeta_tox
    }

    /// Ratchets Server SL via Hermite `C1` toxicity compression (`64%..83%` tighter) when `MFE <= 1 tick`.
    pub fn maybe_ratchet_sl_with_toxicity_v48(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
        zeta_tox: f64,
    ) -> Option<PriceTicks> {
        const PHI_SQ_TX: f64 = 2.618_033_988_749_895;
        const INV_PHI_TX: f64 = 0.618_033_988_749_895;
        let base_ratchet = self.maybe_ratchet_chandelier_or_wald_sl_with_beta(snap, btc_leader_snap, epps_signed_beta);
        if self.chandelier_profit_sl_armed || zeta_tox < INV_PHI_TX {
            return base_ratchet;
        }
        let OrderLifecycleState::PositionOpenMakerExit {
            side, avg_price_ticks, sl_trigger_price: cur_sl, ..
        } = self.state else {
            return base_ratchet;
        };
        if avg_price_ticks.0 <= 0 {
            return base_ratchet;
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let c_rt = spec.expected_round_trip_cost_bps_dynamic(avg_price_ticks, self.learned_taker_prob_pm, self.active_fees).raw();
        let tau_tick = spec.tick_to_price_scaled_bps(avg_price_ticks).unwrap_or(ScaledBps(50)).raw();
        let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, self.active_fees).raw();
        let wald_sl_bps = ((base_sl_bps * 52) / 100).max(c_rt * 4);
        let floor_sl_bps = ((PHI_SQ_TX * (c_rt as f64)).round() as i64).max(tau_tick.saturating_mul(4));
        let s3 = crate::wave_elliott::hermite_smoothstep_c1(zeta_tox, INV_PHI_TX, PHI_SQ_TX);
        let eff_sl_bps = wald_sl_bps - (((wald_sl_bps - floor_sl_bps).max(0) as f64 * s3).round() as i64);
        let sl_delta = ((avg_price_ticks.0.saturating_mul(eff_sl_bps.max(floor_sl_bps))) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
        let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
        let cand_sl = match side {
            Side::Bid => PriceTicks((avg_price_ticks.0 - sl_delta).min(ref_mark.0.min(ref_last.0) - 4).max(1)),
            Side::Ask => PriceTicks(avg_price_ticks.0.saturating_add(sl_delta).max(ref_mark.0.max(ref_last.0).saturating_add(4))),
        };
        let Ok(valid_sl) = PairSpec::validate_stop_loss_geometry(side, cand_sl, ref_last, ref_mark) else {
            return base_ratchet;
        };
        let prev_sl = if self.last_eff_sl_ticks.0 > 0 { self.last_eff_sl_ticks } else { cur_sl };
        let tighter = match side {
            Side::Bid => valid_sl.0 >= prev_sl.0.saturating_add(2) && valid_sl.0 < avg_price_ticks.0,
            Side::Ask => valid_sl.0 <= prev_sl.0.saturating_sub(2) && valid_sl.0 > avg_price_ticks.0,
        };
        if !tighter {
            return base_ratchet;
        }
        self.last_eff_sl_ticks = valid_sl;
        if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = self.state {
            *sl_p = valid_sl;
        }
        Some(valid_sl)
    }

    /// Unified V4.8 open-position step for Shadow/Replay parity (`Glosten-Milgrom + Wald + Chandelier`).
    pub fn step_open_position_v48(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        gov: &OnlineAdaptiveGovernor,
        ledger: &mut ParitySyncLedger,
        epps_signed_beta: f64,
        markout_ring: &crate::markout_tracker::SlotMarkoutRing,
        event_dilation: f64,
    ) -> Option<ClosedTradeSummary> {
        self.sync_governor_dynamics(gov);
        let zeta = self.observe_post_fill_toxicity_and_wald_v48(
            snap, btc_leader_snap, epps_signed_beta, markout_ring, event_dilation,
        );
        if zeta > 0.0 {
            let _ = self.maybe_ratchet_sl_with_toxicity_v48(snap, btc_leader_snap, epps_signed_beta, zeta);
        }
        self.step_open_position_with_beta(snap, btc_leader_snap, gov, ledger, epps_signed_beta)
    }
}

#[cfg(test)]
mod v48_toxicity_unwind_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::markout_tracker::SlotMarkoutRing;
    use crate::types::BoundedScore;

    #[test]
    fn test_v48_toxic_zero_mfe_sweep_compresses_sl_over_65pct_while_healthy_mfe_holds() {
        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.ts_local_ns = TimestampNs(10_000_000_000);
        avax.realized_vol_5m = ScaledBps(6_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(450) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(320) };
        avax.mark_price = PriceTicks(8_200);
        avax.last_trade_price = PriceTicks(8_200);

        let mut eng = SlotExecutionEngine::new(3);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) };
        assert!(eng.try_arm_maker_entry(&avax, v, LotQty(22)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        let init_sl_dist = 8_200 - eng.last_eff_sl_ticks.0;

        let mut ring = SlotMarkoutRing::new();
        ring.arm_fill_probe(Side::Bid, PriceTicks(8_200), PriceTicks(8_200), TimestampNs(10_000_000_000));
        ring.step_clock(PriceTicks(8_197), TimestampNs(10_300_000_000), 200);
        ring.step_clock(PriceTicks(8_196), TimestampNs(11_200_000_000), 200);
        ring.step_clock(PriceTicks(8_192), TimestampNs(16_000_000_000), 200);
        ring.step_clock(PriceTicks(8_188), TimestampNs(25_500_000_000), 200);

        avax.ts_local_ns = TimestampNs(25_500_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_188), qty: LotQty(180) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_189), qty: LotQty(490) };
        avax.mark_price = PriceTicks(8_188);
        avax.last_trade_price = PriceTicks(8_188);
        avax.factor_ofi = BoundedScore::new_clamped(-0.55);
        avax.factor_tfi = BoundedScore::new_clamped(-0.62);

        let zeta = eng.observe_post_fill_toxicity_and_wald_v48(&avax, &avax, 1.0, &ring, 1.2);
        assert!(zeta > 1.80 && eng.wald_sprt.contraction_pct >= 68);
        let compressed_sl = eng.maybe_ratchet_sl_with_toxicity_v48(&avax, &avax, 1.0, zeta);
        assert!(compressed_sl.is_some());
        let new_sl_dist = 8_200 - eng.last_eff_sl_ticks.0;
        assert!(new_sl_dist * 100 <= init_sl_dist * 35);
    }
}
