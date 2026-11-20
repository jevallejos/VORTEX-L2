//! Autonomous 12-Slot Execution FSM, Live VIP Fee Propagation, Signed Epps Wald SPRT & Chandelier SL (V4.8).
#![allow(clippy::manual_clamp, clippy::suboptimal_flops, clippy::too_many_arguments)]

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::fee_cadence_beta::FeeSchedule;
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{SlotTelemetrySnapshot, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::types::{
    BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError,
    APPROVED_SPECS, SPLIT_LEG_MIN_NOTIONAL_1E8,
};
use crate::wave_elliott::{WaveObservables, WaveRegime};

pub use crate::bayesian_governor::OnlineAdaptiveGovernor;
pub use crate::exit_quoter::{
    classify_maker_exit_sub_reason, classify_maker_exit_sub_reason_with_fees,
    compute_canonical_exit_quote_core, compute_canonical_exit_quote_omega,
    compute_chandelier_profit_lock_sl, compute_elastic_offensive_win_floor_bps, compute_reversible_defensive_state_c1,
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
pub fn evaluate_macro_coherence_surface_with_sync_c1(
    side: Side,
    snap: &SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
) -> f64 {
    evaluate_macro_coherence_surface_with_health_c1(side, snap, epps_corr, btc_aligned, 1.0)
}

#[inline]
#[must_use]
pub fn evaluate_macro_coherence_surface_with_health_c1(
    side: Side,
    snap: &SymbolLobSnapshot,
    epps_corr: f64,
    btc_aligned: f64,
    psi_health: f64,
) -> f64 {
    if crate::alpha_3factor::is_opposing_impulse_wave3_hazard(side, snap) {
        return 1.0;
    }
    let w = snap.wave_observables();
    let base_c1 = crate::wave_elliott::evaluate_elliott_hurst_coherence_penalty_c1(side, w);
    let dir_s = side.sign_f64();
    let h_t = w.hurst_exponent();
    let macro_aligned = w.macro_phase.get() * dir_s;
    let fib_aligned_sbps = (w.fib_proj_bps.raw() as f64) * dir_s;
    let (b3x_sbps, eq_vol_sbps) =
        crate::alpha_3factor::endogenous_b3x_and_eq_vol_sbps(snap, 0, 50, FeeSchedule::VIP0_DEFAULT);
    let fib_cut_sbps = (crate::alpha_3factor::PHI * (b3x_sbps as f64))
        .max(crate::alpha_3factor::INV_PHI * (eq_vol_sbps as f64));
    let oi_inv = crate::wave_elliott::hermite_smoothstep_c1(h_t, 0.494, 0.506)
        * crate::wave_elliott::hermite_smoothstep_c1(-0.20 - macro_aligned, 0.0, 0.08);
    let fib_br = if matches!(w.regime, WaveRegime::ImpulseWave3 | WaveRegime::ImpulseWave5) {
        crate::wave_elliott::hermite_smoothstep_c1(
            -fib_cut_sbps - fib_aligned_sbps,
            0.0,
            (crate::alpha_3factor::INV_PHI * fib_cut_sbps).max(100.0),
        )
    } else {
        0.0
    };
    let abc_pen = crate::alpha_3factor::evaluate_corrective_abc_fractal_penalty_with_health_c1(
        side,
        snap,
        epps_corr,
        btc_aligned,
        psi_health,
    );
    let s_abc_sync = crate::alpha_3factor::compute_corrective_abc_snapshot_sync_with_health_c1(
        side,
        snap,
        epps_corr,
        btc_aligned,
        psi_health,
    );
    let slow_zigzag_pen = base_c1.max(oi_inv) * (1.0 - s_abc_sync);
    slow_zigzag_pen.max(fib_br).max(abc_pen).clamp(0.0, 1.0)
}

#[inline]
#[must_use]
pub fn evaluate_macro_coherence_surface_c1(side: Side, snap: &SymbolLobSnapshot) -> f64 {
    evaluate_macro_coherence_surface_with_health_c1(side, snap, 0.0, 0.0, 0.0)
}

#[inline]
#[must_use]
pub fn passes_elliott_macro_coherence_guard_with_sync(
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    epps_corr: f64,
) -> bool {
    passes_elliott_macro_coherence_guard_with_health(side, snap, btc_leader_snap, epps_corr, None, 1.0)
}

#[inline]
#[must_use]
pub fn passes_elliott_macro_coherence_guard_with_health(
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    epps_corr: f64,
    btc_lead_aligned_override: Option<f64>,
    psi_health: f64,
) -> bool {
    let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
    let eff_corr = if is_follower { epps_corr } else { 0.0 };
    let corr_sign = if eff_corr < 0.0 { -1.0 } else { 1.0 };
    let inst_btc_aligned = if is_follower {
        corr_sign * OrthogonalFactorState::snr_composite_score(btc_leader_snap) * side.sign_f64()
    } else {
        0.0
    };
    let btc_aligned = if is_follower {
        btc_lead_aligned_override.map_or(inst_btc_aligned, |m| inst_btc_aligned.max(m))
    } else {
        0.0
    };
    evaluate_macro_coherence_surface_with_health_c1(side, snap, eff_corr, btc_aligned, psi_health)
        < 0.618_033_988_749_895
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
    pub tranche2_harvests_count: u64,
    pub lvdbs_deep_surfs_count: u64,
    pub tranche2_harvested: bool,
    pub tranche2_lots: LotQty,
    pub tranche2_exit_price: PriceTicks,
    pub partial_harvest_legs: u32,
    pub realized_partial_net_1e8: i64,
    pub realized_legs_fee_1e8: i64,
    pub last_closed_legs_fee_1e8: i64,
    pub learned_psi_health: f64,
    pub learned_cont_ratio_x1000: i64,
    pub last_peak_favorable_ts_ns: i64,
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
    pub last_obs_funding_cut_id: i64,
    pub pre_cut_funding_rate_sbps: i64,
    pub accrued_adverse_funding_sbps: i64,
    pub original_armed_entry_price: PriceTicks,
    pub hawkes_tracker: HawkesQueueTracker,
    pub wald_sprt: WaldSprtState,
}

impl SlotExecutionEngine {
    #[inline] #[must_use]
    pub const fn new(slot_idx: usize) -> Self {
        Self {
            slot_idx, state: OrderLifecycleState::Idle, cooldown_until_ns: TimestampNs(0),
            tranche1_harvests_count: 0, tranche2_harvests_count: 0, lvdbs_deep_surfs_count: 0,
            tranche2_harvested: false, tranche2_lots: LotQty(0), tranche2_exit_price: PriceTicks(0),
            partial_harvest_legs: 0, realized_partial_net_1e8: 0, realized_legs_fee_1e8: 0,
            last_closed_legs_fee_1e8: 0, learned_psi_health: 1.0, learned_cont_ratio_x1000: 1_000,
            last_peak_favorable_ts_ns: 0,
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
            last_obs_funding_cut_id: 0, pre_cut_funding_rate_sbps: 0, accrued_adverse_funding_sbps: 0,
            original_armed_entry_price: PriceTicks(0),
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
        self.learned_psi_health = gov.psi_health();
        self.learned_cont_ratio_x1000 = gov.slot_cont_ratio_x1000(self.slot_idx, side);
    }

    #[inline]
    pub fn sync_governor_dynamics(&mut self, gov: &OnlineAdaptiveGovernor) {
        self.learned_psi_health = gov.psi_health();
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
        let beta_btc = if epps_signed_beta.is_finite() {
            epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP)
        } else {
            0.0
        };
        let btc_aligned = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
        let y_ev = if self.slot_idx == 0 { 0.70 * phi_self + 0.30 * m_skew } else { 0.50 * phi_self + 0.20 * m_skew + 0.30 * beta_btc * btc_aligned };
        let prev_ll = if self.wald_sprt.log_likelihood.is_finite() { self.wald_sprt.log_likelihood } else { 0.0 };
        self.wald_sprt.log_likelihood = (prev_ll + 0.963_932_022_500_210_3 * (y_ev - 0.140_983_005_625_052_5) * dt_s).clamp(1.5 * b_unwind, a_hold);
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
        self.observe_open_flow_kinematics(snap);
        let st = self.step_wald_sprt_v2_permille_with_beta(side, snap, btc_leader_snap, epps_signed_beta);
        self.last_closed_wald_pct = self.last_closed_wald_pct.max(st.contraction_pct);
        if avg_price_ticks.0 > 0 {
            let bbo_exit = match side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
            let ref_px = if bbo_exit.0 > 0 { bbo_exit } else if snap.mark_price.0 > 0 { snap.mark_price } else { snap.last_trade_price };
            let cur_gross_bps = if ref_px.0 > 0 { ((ref_px.0 - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1) } else { 0 };
            let dir_s = side.sign_f64();
            let own_flow = self.signed_intrinsic_flow_slow(side, snap).get();
            let has_slow = self.last_flow_sample_ts_ns > 0;
            let signed_slow = BoundedScore::new_clamped(if has_slow { self.flow_ema_slow * dir_s } else { own_flow });
            let win_floor = compute_elastic_offensive_win_floor_bps(self.active_maker_rt_fee_bps(), snap.realized_vol_5m).raw();
            let hold_ns = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0);
            let (req_dip_sbps, req_hold_ns) = self.elastic_defensive_latch_bounds(side, snap, btc_leader_snap, epps_signed_beta);
            let p_tkr = (self.learned_taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
            let exh_ceil = 0.090_169_943_749_47 * (1.0 - p_tkr);
            let flow_exhausted = signed_slow.get() < -exh_ceil || (signed_slow.get() < exh_ceil && cur_gross_bps <= (req_dip_sbps * 3) / 2);
            if hold_ns >= req_hold_ns
                && st.exhaustion_unwind_active
                && cur_gross_bps < req_dip_sbps
                && flow_exhausted
                && cur_gross_bps < win_floor
            {
                self.latch_defensive_floor_if_needed(true);
            }
            let w = snap.wave_observables();
            let prev_locked = matches!(self.state, OrderLifecycleState::PositionOpenMakerExit { defensive_floor_locked: true, .. });
            let (still_locked, wave3_esc) = compute_reversible_defensive_state_c1(
                ScaledBps(cur_gross_bps), ScaledBps(win_floor), signed_slow,
                self.learned_taker_prob_pm, self.learned_ir_x100, w, side, prev_locked,
            );
            if wave3_esc {
                self.latch_defensive_floor_if_needed(true);
            } else if prev_locked && !still_locked && !w.climax_active {
                if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.state {
                    *defensive_floor_locked = false;
                }
                let (ws_rtt_us, ws_quota_rem) = read_live_ws_pulse();
                let prio_cd = compute_adaptive_repeg_cooldown_ns(u64::from(ws_rtt_us), u32::from(ws_quota_rem));
                self.next_peg_allowed_ts_ns = self.next_peg_allowed_ts_ns.min(snap.ts_local_ns.0.saturating_add(prio_cd));
                self.wald_sprt.log_likelihood = self.wald_sprt.log_likelihood.max(0.0);
                self.wald_sprt.exhaustion_unwind_active = false;
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
        self.observe_open_funding_cut_step(snap);
        let raw_flow = BoundedScore::new_clamped(0.50 * snap.factor_ofi.get() + 0.38 * snap.factor_tfi.get() + 0.12 * snap.factor_geom.get());
        let ts_ns = snap.ts_local_ns.0;
        if self.last_flow_sample_ts_ns > 0 && ts_ns == self.last_flow_sample_ts_ns {
            return;
        }
        if self.last_flow_sample_ts_ns <= 0 || ts_ns < self.last_flow_sample_ts_ns
            || ts_ns.saturating_sub(self.last_flow_sample_ts_ns) > FLOW_CONTIGUOUS_GAP_MAX_NS
        {
            self.flow_ema_fast = raw_flow.get();
            self.flow_ema_slow = raw_flow.get();
            if ts_ns > 0 { self.last_flow_sample_ts_ns = ts_ns; }
            return;
        }
        let dt_ns = ts_ns.saturating_sub(self.last_flow_sample_ts_ns);
        self.last_flow_sample_ts_ns = ts_ns;
        let (ws_rtt_us, _) = read_live_ws_pulse();
        let ir_dil = (1.0 + 0.381_966_011_250_105 * ((self.learned_ir_x100.clamp(-50, 100) as f64) * 1e-2)).clamp(0.618_033_988_749_895, 1.618_033_988_749_895);
        let slow_next = step_intrinsic_slow_flow_ema(
            BoundedScore::new_clamped(self.flow_ema_slow), raw_flow, dt_ns,
            self.intrinsic_tau_mkt_s, ir_dil, u64::from(ws_rtt_us),
        );
        let fast_next = step_intrinsic_slow_flow_ema(
            BoundedScore::new_clamped(self.flow_ema_fast), raw_flow, dt_ns.saturating_mul(6),
            self.intrinsic_tau_mkt_s, 1.618_033_988_749_895, u64::from(ws_rtt_us),
        );
        self.flow_ema_slow = slow_next.get();
        self.flow_ema_fast = fast_next.get();
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

    pub fn sync_preflight_l1_entry_auto_with_sync(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_corr: f64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let max_drift = self.max_preflight_drift_ticks(snap);
        self.sync_preflight_l1_entry_with_sync(snap, btc_leader_snap, max_drift, epps_corr)
    }

    pub fn sync_preflight_l1_entry_auto_v542(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let max_drift = self.max_preflight_drift_ticks(snap);
        self.sync_preflight_l1_entry_v542(
            snap,
            btc_leader_snap,
            max_drift,
            epps_corr,
            btc_lead_aligned_override,
            psi_health,
        )
    }

    #[inline]
    pub fn observe_mfe_sample_with_fee_rt(&mut self, gross_mfe_scaled_bps: i64, fee_rt_bps: i64) {
        let eff_rt = fee_rt_bps.max(100);
        if gross_mfe_scaled_bps > self.learned_mfe_bps {
            let m_up = gross_mfe_scaled_bps.min(9_600);
            self.learned_mfe_bps = (618 * self.learned_mfe_bps + 382 * m_up) / 1_000;
        } else {
            let m_dn = gross_mfe_scaled_bps.clamp(eff_rt + 40, 9_600);
            self.learned_mfe_bps = (880 * self.learned_mfe_bps + 120 * m_dn) / 1_000;
        }
    }

    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        self.observe_mfe_sample_with_fee_rt(gross_mfe_scaled_bps, self.active_maker_rt_fee_bps());
    }

    /// Couples superior uncensored favorable excursions observed post-fill in `SlotMarkoutRing` directly upward.
    #[inline]
    pub fn observe_markout_ring_favorable_peak(&mut self, markout_peak_sbps: i64) {
        if markout_peak_sbps > self.learned_mfe_bps {
            self.observe_mfe_sample_with_fee_rt(markout_peak_sbps, self.active_maker_rt_fee_bps());
        }
    }

    /// Right-censoring-aware MFE plasticity helper for vault rehydration & closed-trade accounting (V5.4.1).
    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn observe_mfe_sample_uncensored(
        &mut self,
        gross_close_sbps: i64,
        closed_mfe_sbps: i64,
        is_maker_exit: bool,
        is_scratch: bool,
        fee_rt_bps: i64,
    ) {
        const PHI_MFE: f64 = 1.618_033_988_749_895;
        const INV_PHI_MFE: f64 = 0.618_033_988_749_895;
        let eff_rt = fee_rt_bps.max(100);
        let m_obs = closed_mfe_sbps.max(gross_close_sbps);
        if (is_scratch && m_obs < self.learned_mfe_bps)
            || !crate::bayesian_governor::should_update_mfe_plasticity(is_maker_exit, m_obs, eff_rt)
        {
            return;
        }
        let offensive_floor_sbps = (PHI_MFE * (eff_rt as f64)).round() as i64;
        let right_censored_maker_win = is_maker_exit
            && !is_scratch
            && gross_close_sbps >= offensive_floor_sbps
            && m_obs > 0
            && (gross_close_sbps as f64) >= INV_PHI_MFE * (m_obs as f64);
        if right_censored_maker_win && m_obs < self.learned_mfe_bps {
            return;
        }
        self.observe_mfe_sample_with_fee_rt(m_obs, eff_rt);
    }

    /// Buckley-James / Kaplan-Meier right-censored MFE plasticity for passive Limit TP fills (V5.4.1).
    /// Requires `gross_close_sbps >= round(PHI * active_fee_rt_mkr_sbps)` for right-censoring immunity,
    /// allowing sub-offensive scratches to smoothly relax `learned_mfe_bps` via `S_3` when volatility cools.
    #[inline]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn observe_censored_close_mfe_plasticity(
        &mut self,
        side: Side,
        gross_close_sbps: i64,
        is_maker_exit: bool,
    ) {
        const PHI_MFE: f64 = 1.618_033_988_749_895;
        const INV_PHI_MFE: f64 = 0.618_033_988_749_895;
        let eff_rt = self.active_maker_rt_fee_bps();
        let m_obs = self.last_closed_mfe_bps.max(gross_close_sbps);
        if !crate::bayesian_governor::should_update_mfe_plasticity(is_maker_exit, m_obs, eff_rt) {
            return;
        }
        let offensive_floor_sbps = (PHI_MFE * (eff_rt as f64)).round() as i64;
        let m_km = if is_maker_exit
            && m_obs >= (eff_rt * 13) / 10
            && !matches!(self.last_exit_sub_reason, ExitSubReason::AdverseScratchEscape)
        {
            let mu_exit = self.flow_ema_fast * side.sign_f64();
            let s_flow = crate::wave_elliott::hermite_smoothstep_c1(
                mu_exit,
                0.145_898_033_750_315,
                INV_PHI_MFE,
            );
            let sigma_1m = (self.open_vol_5m_bps.raw().max(1_200) as f64) / 2.236_067_977_499_79;
            let headroom = (INV_PHI_MFE * sigma_1m - (m_obs as f64)).max(0.0);
            let overshoot = (0.381_966_011_250_105 * s_flow * headroom).round() as i64;
            m_obs.saturating_add(overshoot)
        } else {
            m_obs
        };
        if m_km > self.learned_mfe_bps {
            self.observe_mfe_sample_with_fee_rt(m_km, eff_rt);
            return;
        }
        let right_censored_maker_win = is_maker_exit
            && !matches!(self.last_exit_sub_reason, ExitSubReason::AdverseScratchEscape)
            && gross_close_sbps >= offensive_floor_sbps
            && m_obs > 0
            && (gross_close_sbps as f64) >= INV_PHI_MFE * (m_obs as f64);
        if right_censored_maker_win && m_km < self.learned_mfe_bps {
            return;
        }
        let is_sub_offensive_scratch = matches!(self.last_exit_sub_reason, ExitSubReason::AdverseScratchEscape)
            || gross_close_sbps < offensive_floor_sbps;
        if is_sub_offensive_scratch && m_km < self.learned_mfe_bps {
            let eq_vol_f = (ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw().saturating_mul(2)) as f64;
            let open_v_f = (if self.open_vol_5m_bps.raw() > 0 {
                self.open_vol_5m_bps.raw()
            } else {
                ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw()
            }) as f64;
            let s_cool = crate::wave_elliott::hermite_smoothstep_c1(
                PHI_MFE * eq_vol_f - open_v_f,
                0.0,
                eq_vol_f,
            );
            if s_cool <= 0.0 {
                return;
            }
            let floor_dn = m_km.max(eff_rt + 40);
            let eff_sample = ((self.learned_mfe_bps as f64)
                - s_cool * ((self.learned_mfe_bps - floor_dn).max(0) as f64))
                .round() as i64;
            self.observe_mfe_sample_with_fee_rt(eff_sample, eff_rt);
            return;
        }
        self.observe_mfe_sample_with_fee_rt(m_km, eff_rt);
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
        let chi_aging = self.current_anti_hostage_aging_c1(snap.ts_local_ns);
        let fee_rt = self.active_maker_rt_fee_bps();
        let ref_m_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { avg_price_ticks.0 };
        let cur_g_sbps = ((ref_m_px - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1);
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let tau_t_sbps = spec.tick_to_price_scaled_bps(avg_price_ticks).unwrap_or(ScaledBps(50)).raw();
        let retained_mfe = compute_c1_retained_mfe_bps(self.last_closed_mfe_bps, cur_g_sbps, fee_rt, self.learned_taker_prob_pm, tau_t_sbps, self.chandelier_profit_sl_armed);
        if retained_mfe >= (fee_rt * 13) / 10 && chi_aging <= 0.0 { return None; }
        let hold_s = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0) / 1_000_000_000;
        let sl_ref_vol = if self.open_vol_5m_bps.0 > 0 && self.open_vol_5m_bps.0 < snap.realized_vol_5m.0 { self.open_vol_5m_bps } else { snap.realized_vol_5m };
        let wald_contracted_opt = if retained_mfe >= (fee_rt * 13) / 10 {
            None
        } else if hold_s >= 3 && self.wald_sprt.exhaustion_unwind_active {
            let (comp_bps, eff_gamma) = compute_wald_v2_contracted_sl_bps_with_fees(sl_ref_vol, &self.wald_sprt, self.active_fees);
            self.last_closed_wald_pct = self.last_closed_wald_pct.max(eff_gamma);
            Some(comp_bps)
        } else if self.last_closed_wald_pct >= 12 && hold_s >= (self.intrinsic_tau_mkt_s.max(90) / 5).max(45) {
            let dir_s = side.sign_f64();
            let own_flow = self.signed_intrinsic_flow_slow(side, snap).get();
            let beta_c = if self.slot_idx == 0 { 0.0 } else { epps_signed_beta.clamp(-EPPS_PHI_CAP, EPPS_PHI_CAP) };
            let btc_snr = if self.slot_idx == 0 {
                OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s
            } else {
                beta_c * (OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s)
            };
            if own_flow < -0.05 || btc_snr < -0.20 {
                let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(sl_ref_vol, self.active_fees).raw();
                let btc_headwind = if self.slot_idx > 0 && btc_snr < -0.25 { ((-btc_snr - 0.25) * 20.0).round() as i64 } else { 0 };
                let eff_wald_pct = (self.last_closed_wald_pct + btc_headwind).clamp(12, 42);
                Some(((base_sl_bps * (100 - eff_wald_pct)) / 100).max(fee_rt * 9))
            } else {
                None
            }
        } else {
            None
        };
        let aging_contracted_opt = if chi_aging > 0.0 {
            let c_rt = spec.expected_round_trip_cost_bps_dynamic(avg_price_ticks, self.learned_taker_prob_pm, self.active_fees).raw();
            let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(sl_ref_vol, self.active_fees).raw();
            let min_diff_floor_bps = ((2.618_033_988_749_895 * (c_rt.max(150) as f64)).round() as i64)
                .max(tau_t_sbps.max(10).saturating_mul(4));
            let aged_bps = base_sl_bps
                - (((base_sl_bps - min_diff_floor_bps).max(0) as f64 * chi_aging).round() as i64);
            Some(aged_bps.max(min_diff_floor_bps))
        } else {
            None
        };
        let contracted_bps = match (wald_contracted_opt, aging_contracted_opt) {
            (Some(w), Some(a)) => w.min(a),
            (Some(w), None) => w,
            (None, Some(a)) => a,
            (None, None) => return None,
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
        let min_step = if chi_aging > 0.0 { 1_i64 } else { 3_i64 };
        let tighter = match side {
            Side::Bid => valid_sl.0 >= prev_sl.0.saturating_add(min_step) && valid_sl.0 < avg_price_ticks.0,
            Side::Ask => valid_sl.0 <= prev_sl.0.saturating_sub(min_step) && valid_sl.0 > avg_price_ticks.0,
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
        let prev_vc = tel.read_consistent_or_default(2).last_veto_code;
        let snap = match self.state {
            OrderLifecycleState::Idle => SlotTelemetrySnapshot {
                state_code: 0,
                last_veto_code: prev_vc,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                ..SlotTelemetrySnapshot::default()
            },
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
        self.try_arm_maker_entry_with_sync(snap, snap, verdict, qty, 0.0)
    }

    pub fn try_arm_maker_entry_with_sync(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        verdict: SignalVerdict,
        qty: LotQty,
        epps_corr: f64,
    ) -> Result<OrderLifecycleState, VortexError> {
        self.try_arm_maker_entry_v542(snap, btc_leader_snap, verdict, qty, epps_corr, None, 0.0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_arm_maker_entry_v542(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        verdict: SignalVerdict,
        qty: LotQty,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
    ) -> Result<OrderLifecycleState, VortexError> {
        if !matches!(self.state, OrderLifecycleState::Idle) || snap.ts_local_ns.0 < self.cooldown_until_ns.0 { return Ok(self.state); }
        let SignalVerdict::AuthorizeMakerEntry { side, target_excursion_scaled_bps } = verdict else { return Ok(self.state); };
        let eff_psi = if btc_lead_aligned_override.is_some() || psi_health > 0.0 { psi_health } else { 1.0 };
        if crate::alpha_3factor::is_opposing_impulse_wave3_hazard_dynamic(
            side,
            snap,
            snap.realized_vol_5m.raw(),
            self.learned_taker_prob_pm,
            self.active_fees,
        )
            || !passes_elliott_macro_coherence_guard_with_health(
                side,
                snap,
                btc_leader_snap,
                epps_corr,
                btc_lead_aligned_override,
                eff_psi,
            )
            || !passes_order_block_wall_guard_dynamic(self.slot_idx, side, snap, self.learned_taker_prob_pm, self.active_fees)
        {
            return Ok(self.state);
        }
        let has_live_factors = snap.sequence_id > 0
            || snap.factor_ofi.get().abs() > 0.0
            || snap.factor_tfi.get().abs() > 0.0
            || snap.factor_geom.get().abs() > 0.0;
        if has_live_factors
            && !crate::alpha_3factor::passes_elastic_anti_spoof_ofi_guard(
                side,
                snap,
                self.hawkes_tracker.spoof_density_ewma,
                self.learned_taker_prob_pm,
                snap.realized_vol_5m.raw(),
                self.active_fees,
            )
        {
            return Ok(self.state);
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Ok(self.state); };
        let base_l1_price = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
        let corr_sign = if is_follower && epps_corr < 0.0 { -1.0 } else { 1.0 };
        let btc_al = if is_follower {
            let inst = corr_sign * OrthogonalFactorState::snr_composite_score(btc_leader_snap) * side.sign_f64();
            btc_lead_aligned_override.map_or(inst, |m| inst.max(m))
        } else {
            0.0
        };
        let prop = compute_aligned_entry_propulsion_c1(side, snap, btc_al);
        let cand_px = select_elastic_entry_maker_price_v542(side, snap, psi_health, prop);
        let entry_price = if cand_px != base_l1_price
            && passes_cumulative_repeg_chase_cap_v542(
                side,
                base_l1_price,
                cand_px,
                target_excursion_scaled_bps,
                ScaledBps(self.active_maker_rt_fee_bps()),
            )
        {
            cand_px
        } else {
            base_l1_price
        };
        let sl_trigger_price = self.compute_fill_or_adopt_sl_ticks_for_slot(side, entry_price, snap)?;
        self.original_armed_entry_price = base_l1_price;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.hawkes_tracker.reset();
        self.last_obs_funding_cut_id = 0; self.pre_cut_funding_rate_sbps = 0; self.accrued_adverse_funding_sbps = 0; self.wald_sprt.reset();
        self.state = OrderLifecycleState::PlacingEntryMaker {
            side, entry_price, qty: LotQty(qty.0.max(1)),
            target_excursion: target_excursion_scaled_bps, sl_trigger_price, submitted_ts: snap.ts_local_ns,
        };
        Ok(self.state)
    }

    pub fn sync_preflight_l1_entry(&mut self, snap: &SymbolLobSnapshot, max_drift_ticks: i64) -> Result<(PriceTicks, PriceTicks), VortexError> {
        self.sync_preflight_l1_entry_with_sync(snap, snap, max_drift_ticks, 0.0)
    }

    pub fn sync_preflight_l1_entry_with_sync(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        max_drift_ticks: i64,
        epps_corr: f64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        self.sync_preflight_l1_entry_v542(snap, btc_leader_snap, max_drift_ticks, epps_corr, None, 0.0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_preflight_l1_entry_v542(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        max_drift_ticks: i64,
        epps_corr: f64,
        btc_lead_aligned_override: Option<f64>,
        psi_health: f64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let OrderLifecycleState::PlacingEntryMaker { side, entry_price: armed_px, target_excursion, .. } = self.state else { return Err(VortexError::InvalidPriceOrTickSpec); };
        let eff_psi = if btc_lead_aligned_override.is_some() || psi_health > 0.0 { psi_health } else { 1.0 };
        if crate::alpha_3factor::is_opposing_impulse_wave3_hazard_dynamic(
            side,
            snap,
            self.open_vol_5m_bps.raw(),
            self.learned_taker_prob_pm,
            self.active_fees,
        )
            || !passes_elliott_macro_coherence_guard_with_health(
                side,
                snap,
                btc_leader_snap,
                epps_corr,
                btc_lead_aligned_override,
                eff_psi,
            )
            || !passes_order_block_wall_guard_dynamic(self.slot_idx, side, snap, self.learned_taker_prob_pm, self.active_fees)
        {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let has_live_factors = snap.sequence_id > 0
            || snap.factor_ofi.get().abs() > 0.0
            || snap.factor_tfi.get().abs() > 0.0
            || snap.factor_geom.get().abs() > 0.0;
        if has_live_factors
            && !crate::alpha_3factor::passes_elastic_anti_spoof_ofi_guard(
                side,
                snap,
                self.hawkes_tracker.spoof_density_ewma,
                self.learned_taker_prob_pm,
                self.open_vol_5m_bps.raw(),
                self.active_fees,
            )
        {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else { return Err(VortexError::InvalidPriceOrTickSpec); };
        let cur_l1_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let anchor_px = if self.original_armed_entry_price.0 > 0 { self.original_armed_entry_price } else { armed_px };
        if cur_l1_px.0 <= 0 || (cur_l1_px.0 - anchor_px.0).abs() > max_drift_ticks { return Err(VortexError::InvalidPriceOrTickSpec); }
        let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
        let corr_sign = if is_follower && epps_corr < 0.0 { -1.0 } else { 1.0 };
        let btc_al = if is_follower {
            let inst = corr_sign * OrthogonalFactorState::snr_composite_score(btc_leader_snap) * side.sign_f64();
            btc_lead_aligned_override.map_or(inst, |m| inst.max(m))
        } else {
            0.0
        };
        let prop = compute_aligned_entry_propulsion_c1(side, snap, btc_al);
        let cand_px = select_elastic_entry_maker_price_v542(side, snap, psi_health, prop);
        let synced_px = if cand_px != cur_l1_px
            && passes_cumulative_repeg_chase_cap_v542(
                side,
                anchor_px,
                cand_px,
                target_excursion,
                ScaledBps(self.active_maker_rt_fee_bps()),
            )
        {
            cand_px
        } else {
            cur_l1_px
        };
        let new_sl = self.compute_fill_or_adopt_sl_ticks_for_slot(side, synced_px, snap)?;
        if let OrderLifecycleState::PlacingEntryMaker { ref mut entry_price, ref mut sl_trigger_price, .. } = self.state {
            *entry_price = synced_px;
            *sl_trigger_price = new_sl;
        }
        Ok((synced_px, new_sl))
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
            let eq_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw().saturating_mul(2));
            let reg_mfe = if vol_raw > 0 {
                crate::types::modulate_learned_mfe_by_diffusive_regime_sbps(
                    self.learned_mfe_bps,
                    self.open_vol_5m_bps,
                    eq_vol,
                )
            } else {
                self.learned_mfe_bps
            };
            let phi_mfe = ((1.618_033_988_749_895 * (reg_mfe as f64)).round() as i64).max(reg_mfe);
            let mfe_cap = phi_mfe.clamp(min_win_gross + 15, max_single_cap);
            target_excursion.raw().min(mfe_cap.min(vol_unit_cap)).max(min_win_gross)
        };
        let structural_tp_price = dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
        let (init_role, init_exit_px, init_exit_lots) = match split_t1_opt {
            Some(t1_lots) => (ExitOrderRole::Tranche1Harvest50, win1_px, t1_lots),
            None => (ExitOrderRole::StandardMakerExit100, structural_tp_price, eff_lots),
        };
        self.last_opened_ts = fill_ts;
        self.last_peak_favorable_ts_ns = fill_ts.0;
        self.last_eff_sl_ticks = eff_sl_px;
        self.ratchet_locked_gross_bps = 0;
        self.tranche1_harvests_count = 0;
        self.lvdbs_deep_surfs_count = 0;
        self.tranche2_harvested = false;
        self.tranche2_lots = LotQty(0);
        self.tranche2_exit_price = PriceTicks(0);
        self.partial_harvest_legs = 0;
        self.realized_partial_net_1e8 = 0;
        self.realized_legs_fee_1e8 = 0;
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
        self.last_obs_funding_cut_id = 0; self.pre_cut_funding_rate_sbps = 0; self.accrued_adverse_funding_sbps = 0; self.wald_sprt.reset();
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
        let adopt_mfe = if snap.realized_vol_5m.raw() > 0 {
            crate::types::modulate_learned_mfe_by_diffusive_regime_sbps(
                self.learned_mfe_bps,
                snap.realized_vol_5m,
                ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw().saturating_mul(2)),
            )
        } else {
            self.learned_mfe_bps
        };
        let struct_tp = dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(adopt_mfe.max((fee_rt * 27) / 20)));
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
        let avg_d = verified_avg_price.0.max(1);
        self.last_opened_ts = opened_ts;
        self.last_peak_favorable_ts_ns = opened_ts.0;
        self.last_eff_sl_ticks = sl_t;
        self.open_vol_5m_bps = snap.realized_vol_5m;
        self.ratchet_locked_gross_bps = 0;
        self.last_closed_peak_ticks = pk_init;
        self.last_closed_worst_ticks = wr_init;
        self.last_closed_mfe_bps = ((pk_init.0 - verified_avg_price.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_d;
        self.last_closed_mae_bps = ((wr_init.0 - verified_avg_price.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_d;
        self.last_closed_wald_pct = 0;
        self.tranche1_harvests_count = u64::from(t1_done);
        self.lvdbs_deep_surfs_count = 0;
        self.tranche2_harvested = false;
        self.tranche2_lots = LotQty(0);
        self.tranche2_exit_price = PriceTicks(0);
        self.partial_harvest_legs = u32::from(t1_done);
        self.realized_partial_net_1e8 = 0;
        self.realized_legs_fee_1e8 = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = opened_ts.0;
        self.flow_ema_fast = 0.0;
        self.flow_ema_slow = 0.0;
        self.last_flow_sample_ts_ns = 0;
        self.chandelier_profit_sl_armed = is_chandelier_profit_locked_sl_with_fees(side, verified_avg_price, sl_t, self.active_fees);
        self.hawkes_tracker.reset();
        self.last_obs_funding_cut_id = 0; self.pre_cut_funding_rate_sbps = 0; self.accrued_adverse_funding_sbps = 0; self.wald_sprt.reset();
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
            side, avg_price_ticks, ref mut peak_favorable_price,
            ref mut open_lots, ref mut tranche1_harvested, ref mut tranche1_lots,
            ref mut tranche1_exit_price, ref mut active_exit_role, ref mut active_exit_lots, ..
        } = self.state else { return false; };
        if *tranche1_harvested { return false; }
        let wave_override = if matches!(*active_exit_role, ExitOrderRole::Tranche1Harvest50) && active_exit_lots.0 >= 1 && active_exit_lots.0 < open_lots.0 {
            let rem = LotQty(open_lots.0 - active_exit_lots.0);
            if rem.0 >= 1 && spec.lot_notional_1e8(*open_lots, fill_price) >= SPLIT_LEG_MIN_NOTIONAL_1E8
            { Some(*active_exit_lots) } else { None }
        } else { None };
        let Some(harvested) = wave_override.or_else(|| Self::compute_tranche1_split_lots(s_idx, *open_lots, fill_price, phi)) else { return false; };
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *tranche1_harvested = true;
        *tranche1_lots = harvested;
        *tranche1_exit_price = fill_price;
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        if (side == Side::Bid && fill_price.0 > peak_favorable_price.0)
            || (side == Side::Ask && fill_price.0 > 0 && fill_price.0 < peak_favorable_price.0)
        {
            *peak_favorable_price = fill_price;
        }
        let (pnl1, fee1, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
            side,
            harvested,
            avg_price_ticks,
            fill_price,
            true,
            self.active_fees,
        );
        self.realized_partial_net_1e8 = self.realized_partial_net_1e8.saturating_add(pnl1);
        self.realized_legs_fee_1e8 = self.realized_legs_fee_1e8.saturating_add(fee1);
        self.partial_harvest_legs = self.partial_harvest_legs.saturating_add(1);
        self.tranche1_harvests_count = self.tranche1_harvests_count.saturating_add(1);
        self.fold_price_into_excursions(side, avg_price_ticks, fill_price);
        true
    }

    /// Records intermediate `reduceOnly` `Tranche 2` (`~ PHI * T1_gross`, `round(INV_PHI * rem_lots).clamp(1, rem_lots - 1)`)
    /// leaving `T3` as *Uncapped Shadow-Runner*, elevating guaranteed floor, and accumulating exact
    /// `realized_partial_net_1e8`, `realized_legs_fee_1e8`, and `lvdbs_deep_surfs_count` (V5.5 O3/O4).
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn record_tranche2_harvest(&mut self, fill_price: PriceTicks) -> bool {
        let s_idx = self.slot_idx;
        let spec = APPROVED_SPECS[s_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            ref mut peak_favorable_price,
            ref mut open_lots,
            tranche1_harvested,
            tranche1_exit_price,
            ref mut active_exit_role,
            ref mut active_exit_lots,
            ..
        } = self.state
        else {
            return false;
        };
        if !tranche1_harvested || self.tranche2_harvested || open_lots.0 < 2 || fill_price.0 <= 0 {
            return false;
        }
        let harvested = if matches!(*active_exit_role, ExitOrderRole::Tranche2HarvestPhi)
            && active_exit_lots.0 >= 1
            && active_exit_lots.0 < open_lots.0
        {
            *active_exit_lots
        } else {
            LotQty(
                ((crate::alpha_3factor::INV_PHI * (open_lots.0 as f64)).round() as i64)
                    .clamp(1, open_lots.0 - 1),
            )
        };
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        if (side == Side::Bid && fill_price.0 > peak_favorable_price.0)
            || (side == Side::Ask && fill_price.0 > 0 && fill_price.0 < peak_favorable_price.0)
        {
            *peak_favorable_price = fill_price;
        }
        let t1_px = tranche1_exit_price;
        let (pnl2, fee2, g2, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
            side,
            harvested,
            avg_price_ticks,
            fill_price,
            true,
            self.active_fees,
        );
        self.tranche2_harvested = true;
        self.tranche2_lots = harvested;
        self.tranche2_exit_price = fill_price;
        self.realized_partial_net_1e8 = self.realized_partial_net_1e8.saturating_add(pnl2);
        self.realized_legs_fee_1e8 = self.realized_legs_fee_1e8.saturating_add(fee2);
        self.partial_harvest_legs = self.partial_harvest_legs.saturating_add(1);
        self.tranche2_harvests_count = self.tranche2_harvests_count.saturating_add(1);
        self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
        let avg_d = avg_price_ticks.0.max(1);
        let t1_gross = if t1_px.0 > 0 {
            ((t1_px.0 - avg_price_ticks.0)
                .saturating_mul(side.sign_i64())
                .saturating_mul(1_000_000))
                / avg_d
        } else {
            self.active_maker_rt_fee_bps()
        };
        let post_t2_floor = t1_gross
            .max(((g2.raw() as f64) * crate::alpha_3factor::INV_PHI).round() as i64)
            .max(self.active_maker_rt_fee_bps());
        self.ratchet_locked_gross_bps = self.ratchet_locked_gross_bps.max(post_t2_floor);
        self.fold_price_into_excursions(side, avg_price_ticks, fill_price);
        true
    }

    /// Synchronizes a live partial `reduceOnly` execution (`T1` or `T2`) observed via Bybit WS `position`/`order` stream.
    pub fn record_live_partial_harvest_synced(&mut self, delta_harvested: LotQty, fill_price_hint: PriceTicks) {
        if delta_harvested.0 <= 0 {
            return;
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let Some((side, avg_px, t1_already, t1_px)) = (match self.state {
            OrderLifecycleState::PositionOpenMakerExit {
                side,
                avg_price_ticks,
                tranche1_harvested,
                tranche1_exit_price,
                ..
            } => Some((side, avg_price_ticks, tranche1_harvested, tranche1_exit_price)),
            _ => None,
        }) else {
            return;
        };
        let eff_px = if fill_price_hint.0 > 0 { fill_price_hint } else { avg_px };
        let (pnl_leg, fee_leg, g_leg, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
            side,
            delta_harvested,
            avg_px,
            eff_px,
            true,
            self.active_fees,
        );
        self.realized_partial_net_1e8 = self.realized_partial_net_1e8.saturating_add(pnl_leg);
        self.realized_legs_fee_1e8 = self.realized_legs_fee_1e8.saturating_add(fee_leg);
        self.partial_harvest_legs = self.partial_harvest_legs.saturating_add(1);
        if !t1_already || self.tranche1_harvests_count == 0 {
            self.tranche1_harvests_count = self.tranche1_harvests_count.max(1);
        } else if !self.tranche2_harvested {
            self.tranche2_harvested = true;
            self.tranche2_lots = delta_harvested;
            self.tranche2_exit_price = eff_px;
            self.tranche2_harvests_count = self.tranche2_harvests_count.saturating_add(1);
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            let t1_g = if t1_px.0 > 0 {
                ((t1_px.0 - avg_px.0)
                    .saturating_mul(side.sign_i64())
                    .saturating_mul(1_000_000))
                    / avg_px.0.max(1)
            } else {
                self.active_maker_rt_fee_bps()
            };
            let post_t2 = t1_g
                .max(((g_leg.raw() as f64) * crate::alpha_3factor::INV_PHI).round() as i64)
                .max(self.active_maker_rt_fee_bps());
            self.ratchet_locked_gross_bps = self.ratchet_locked_gross_bps.max(post_t2);
        }
        self.fold_price_into_excursions(side, avg_px, eff_px);
    }

    #[inline]
    #[must_use]
    pub const fn cascade_and_aging_context_v55(&self) -> crate::exit_quoter::CascadeAndAgingContextV55 {
        crate::exit_quoter::CascadeAndAgingContextV55 {
            tranche1_harvests_count: self.tranche1_harvests_count,
            tranche2_harvested: self.tranche2_harvested,
            harvested_legs: self.partial_harvest_legs,
            psi_health: self.learned_psi_health,
            cont_ratio_x1000: self.learned_cont_ratio_x1000,
            last_peak_favorable_ts_ns: self.last_peak_favorable_ts_ns,
        }
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn current_anti_hostage_aging_c1(&self, now_ts: TimestampNs) -> f64 {
        let OrderLifecycleState::PositionOpenMakerExit { opened_ts, .. } = self.state else {
            return 0.0;
        };
        let hold_s = (now_ts.0.saturating_sub(opened_ts.0).max(0) as f64) * 1e-9;
        let peak_ts = if self.last_peak_favorable_ts_ns > 0 {
            self.last_peak_favorable_ts_ns
        } else {
            opened_ts.0
        };
        let stag_s = (now_ts.0.saturating_sub(peak_ts).max(0) as f64) * 1e-9;
        let tau_h = crate::exit_quoter::compute_anti_hostage_horizon_s(
            self.intrinsic_tau_mkt_s,
            self.partial_harvest_legs,
        );
        crate::exit_quoter::compute_anti_hostage_aging_c1(hold_s, stag_s, tau_h)
    }

    #[inline]
    pub fn update_peak_favorable_timestamp_if_improved(
        &mut self,
        prev_peak: PriceTicks,
        new_peak: PriceTicks,
        side: Side,
        now_ts: TimestampNs,
    ) {
        if now_ts.0 <= 0 {
            return;
        }
        let improved = match side {
            Side::Bid => new_peak.0 > prev_peak.0,
            Side::Ask => new_peak.0 > 0 && new_peak.0 < prev_peak.0,
        };
        if improved {
            self.last_peak_favorable_ts_ns = now_ts.0;
        }
    }

    /// Computes live net residual risk `residual_risk_1e8 = (rem_lots_loss_to_eff_sl_1e8 - realized_partial_net_1e8.max(0)).max(0)`
    /// and its `C1` Hermite risk/stagger scale `S_3(residual_risk_ratio; 0.0, INV_PHI_SQ)` (V5.5 O4 House-Money).
    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn house_money_residual_risk_1e8_and_s3_scale(&self) -> (i64, f64) {
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        match self.state {
            OrderLifecycleState::Idle => (0, 0.0),
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, sl_trigger_price, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, .. } => {
                let (pnl_sl, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
                    side,
                    qty,
                    entry_price,
                    sl_trigger_price,
                    false,
                    self.active_fees,
                );
                ((-pnl_sl).max(1), 1.0)
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side,
                avg_price_ticks,
                initial_lots,
                open_lots,
                sl_trigger_price,
                ..
            } => {
                let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
                    self.last_eff_sl_ticks
                } else {
                    sl_trigger_price
                };
                let (init_pnl_sl, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
                    side,
                    LotQty(initial_lots.0.max(open_lots.0).max(1)),
                    avg_price_ticks,
                    sl_trigger_price,
                    false,
                    self.active_fees,
                );
                let init_loss_1e8 = (-init_pnl_sl).max(1);
                let (rem_pnl_sl, _, _, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(
                    side,
                    LotQty(open_lots.0.max(1)),
                    avg_price_ticks,
                    eff_sl,
                    false,
                    self.active_fees,
                );
                let rem_lots_loss_to_eff_sl_1e8 = (-rem_pnl_sl).max(0);
                let residual_risk_1e8 =
                    (rem_lots_loss_to_eff_sl_1e8 - self.realized_partial_net_1e8.max(0)).max(0);
                if residual_risk_1e8 <= 0 {
                    return (0, 0.0);
                }
                let ratio = (residual_risk_1e8 as f64) / (init_loss_1e8 as f64);
                let s3 = crate::wave_elliott::hermite_smoothstep_c1(
                    ratio,
                    0.0,
                    crate::alpha_3factor::INV_PHI_SQ,
                );
                (residual_risk_1e8, s3.clamp(0.0, 1.0))
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn is_house_money_free_ride(&self) -> bool {
        matches!(self.state, OrderLifecycleState::PositionOpenMakerExit { .. })
            && self.house_money_residual_risk_1e8_and_s3_scale().0 == 0
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
        let OrderLifecycleState::PositionOpenMakerExit { side, tranche1_harvested, tranche1_lots, tranche1_exit_price, .. } = self.state else { return None; };
        self.last_exit_sub_reason = resolution.sub_reason;
        if matches!(resolution.kind, ClosedTradeKind::MakerPassiveUnwind)
            && matches!(resolution.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf | ExitSubReason::FibonacciImpulseSurf | ExitSubReason::ShadowRunnerCrestHarvest)
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
        }
        if resolution.exchange_closed_pnl_1e8.is_none() && tranche1_harvested && tranche1_lots.0 > 0 && tranche1_exit_price.0 > 0 {
            return self.finalize_open_position_close(resolution.kind, resolution.verified_exit_price, closed_ts, ledger);
        }
        Some(self.finalize_close_with_lots(
            resolution.kind, side, resolution.verified_entry_price, resolution.verified_exit_price,
            resolution.closed_lots, resolution.legs_count, resolution.exchange_closed_pnl_1e8, closed_ts, ledger,
        ))
    }

    #[inline]
    fn compute_post_close_cooldown_until_ns(&self, closed_ts: TimestampNs, is_maker_exit: bool, net_scaled_bps: ScaledBps) -> TimestampNs {
        let tau_ns = self.intrinsic_tau_mkt_s.max(90).saturating_mul(1_000_000_000);
        if is_maker_exit && net_scaled_bps.raw() < 0 {
            let fee_rt = self.active_maker_rt_fee_bps();
            let sl_sbps = PairSpec::elastic_server_sl_distance_bps_with_fees(self.open_vol_5m_bps, self.active_fees);
            let w_bail = crate::governor_plasticity::compute_fractional_maker_bailout_scar_weight_c1(
                net_scaled_bps,
                ScaledBps(fee_rt),
                sl_sbps,
            );
            let cd_ns = ((tau_ns as f64) * (crate::alpha_3factor::INV_PHI_CUBE + crate::alpha_3factor::INV_PHI * w_bail)).round() as i64;
            TimestampNs(closed_ts.0.saturating_add(cd_ns.max(tau_ns / 100)))
        } else {
            TimestampNs(closed_ts.0.saturating_add(tau_ns / 100))
        }
    }

    #[inline]
    pub fn apply_momentum_chain_post_close_cooldown(
        &mut self,
        closed_ts: TimestampNs,
        is_maker_exit: bool,
        net_scaled_bps: ScaledBps,
        climax_active: bool,
        psi_health: f64,
        aligned_propulsion: f64,
        ws_rtt_us: u32,
    ) {
        if !is_maker_exit || net_scaled_bps.raw() <= 0 || closed_ts.0 <= 0 {
            return;
        }
        let base_cd_ns = self.cooldown_until_ns.0.saturating_sub(closed_ts.0).max(0);
        let eff_cd_ns = crate::governor_plasticity::compute_momentum_chain_win_cooldown_ns_c1(
            base_cd_ns,
            is_maker_exit,
            net_scaled_bps,
            self.active_maker_rt_fee_bps(),
            climax_active,
            psi_health,
            aligned_propulsion,
            ws_rtt_us,
        );
        self.cooldown_until_ns = TimestampNs(closed_ts.0.saturating_add(eff_cd_ns));
    }

    #[must_use]
    pub fn compute_canonical_exit_quote(&self, snap: &SymbolLobSnapshot) -> Option<CanonicalExitQuote> {
        let ctx = self.open_exit_context()?;
        let adv_f_sbps = self.accrued_open_adverse_funding_sbps(snap);
        let (ws_rtt_us, _) = read_live_ws_pulse();
        let mut quote = crate::exit_quoter::compute_canonical_exit_quote_omega_v55(
            &ctx,
            self.shadow_runner_kinematics(),
            self.cascade_and_aging_context_v55(),
            snap,
            self.open_vol_5m_bps,
            Some(self.wald_sprt.log_likelihood),
            adv_f_sbps,
            ws_rtt_us,
        )?;
        if adv_f_sbps > 0 {
            let z_px = defensive_zero_loss_maker_exit_price_with_fees(ctx.side, ctx.avg_price_ticks, self.active_fees);
            let is_maker_bailout = match ctx.side {
                Side::Bid => quote.target_price.0 < z_px.0,
                Side::Ask => quote.target_price.0 > z_px.0,
            };
            if !is_maker_bailout {
                let f_fl = defensive_zero_loss_maker_exit_price_with_funding(ctx.side, ctx.avg_price_ticks, self.active_fees, adv_f_sbps);
                quote.target_price = match ctx.side {
                    Side::Bid => PriceTicks(quote.target_price.0.max(f_fl.0)),
                    Side::Ask => PriceTicks(quote.target_price.0.min(f_fl.0).max(1)),
                };
            }
        }
        Some(quote)
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
        self.last_closed_legs_fee_1e8 = self.realized_legs_fee_1e8.max(fee_1e8);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, net_scaled_bps);
        self.observe_censored_close_mfe_plasticity(side, gross_scaled_bps.raw(), is_maker_exit);
        self.cooldown_until_ns = self.compute_post_close_cooldown_until_ns(closed_ts, is_maker_exit, net_scaled_bps);
        self.close_reconcile_retries = 0;
        self.chandelier_profit_sl_armed = false;
        self.tranche2_harvested = false;
        self.tranche2_lots = LotQty(0);
        self.tranche2_exit_price = PriceTicks(0);
        self.partial_harvest_legs = 0;
        self.realized_partial_net_1e8 = 0;
        self.realized_legs_fee_1e8 = 0;
        self.last_peak_favorable_ts_ns = 0;
        self.last_flow_sample_ts_ns = 0;
        self.hawkes_tracker.reset();
        self.last_obs_funding_cut_id = 0; self.pre_cut_funding_rate_sbps = 0; self.accrued_adverse_funding_sbps = 0; self.wald_sprt.reset();
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
        let q_rem = LotQty(open_lots.0.max(1));
        if !tranche1_harvested || tranche1_lots.0 <= 0 || tranche1_exit_price.0 <= 0 {
            return Some(self.finalize_close_with_lots(leg2_kind, side, avg_price_ticks, leg2_exit_price, q_rem, 1, None, closed_ts, ledger));
        }
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let q1 = tranche1_lots;
        let is_maker_exit = !matches!(leg2_kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee_mkr_bps = self.active_fees.round_trip_maker_exit_bps();
        let fee_rem_bps = if is_maker_exit { fee_mkr_bps } else { self.active_fees.round_trip_taker_exit_bps() };
        let (pnl1, fee1, g1, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, q1, avg_price_ticks, tranche1_exit_price, true, self.active_fees);
        let has_t2 = self.tranche2_harvested && self.tranche2_lots.0 > 0 && self.tranche2_exit_price.0 > 0;
        let (q2_mid, t2_px, pnl2_mid, fee2_mid, g2_mid, legs_total) = if has_t2 {
            let q_m = self.tranche2_lots;
            let px_m = self.tranche2_exit_price;
            let (p_m, f_m, g_m, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, q_m, avg_price_ticks, px_m, true, self.active_fees);
            (q_m, px_m, p_m, f_m, g_m.raw(), 3_u32)
        } else {
            (LotQty(0), PriceTicks(0), 0_i64, 0_i64, 0_i64, 2_u32)
        };
        let (pnl_rem, fee_rem, g_rem, _) = spec.compute_leg_pnl_and_fee_1e8_with_schedule(side, q_rem, avg_price_ticks, leg2_exit_price, is_maker_exit, self.active_fees);
        let tot_q = (q1.0 + q2_mid.0 + q_rem.0).max(1);
        let tot_q_128 = tot_q as i128;
        let w_exit = (q1.0 as i128) * (tranche1_exit_price.0 as i128)
            + (q2_mid.0 as i128) * (t2_px.0 as i128)
            + (q_rem.0 as i128) * (leg2_exit_price.0 as i128);
        let avg_exit_ticks = match side { Side::Bid => (w_exit / tot_q_128).max(1) as i64, Side::Ask => ((w_exit + tot_q_128 - 1) / tot_q_128) as i64 };
        let gross_bps = (q1.0 * g1.raw() + q2_mid.0 * g2_mid + q_rem.0 * g_rem.raw()) / tot_q;
        let fee_bps = (q1.0 * fee_mkr_bps.raw() + q2_mid.0 * fee_mkr_bps.raw() + q_rem.0 * fee_rem_bps.raw()) / tot_q;
        let net_bps = gross_bps - fee_bps;
        let net_pnl_1e8 = pnl1.saturating_add(pnl2_mid).saturating_add(pnl_rem);
        let fee_1e8 = fee1.saturating_add(fee2_mid).saturating_add(fee_rem);
        self.last_closed_legs_fee_1e8 = fee_1e8;
        self.fold_price_into_excursions(side, avg_price_ticks, tranche1_exit_price);
        if has_t2 {
            self.fold_price_into_excursions(side, avg_price_ticks, t2_px);
        }
        self.fold_price_into_excursions(side, avg_price_ticks, leg2_exit_price);
        ledger.record_closed_trade_with_sinking_split(net_pnl_1e8, fee_1e8, ScaledBps(net_bps));
        self.observe_censored_close_mfe_plasticity(side, gross_bps, is_maker_exit);
        self.cooldown_until_ns = self.compute_post_close_cooldown_until_ns(closed_ts, is_maker_exit, ScaledBps(net_bps));
        self.close_reconcile_retries = 0;
        self.chandelier_profit_sl_armed = false;
        self.tranche2_harvested = false;
        self.tranche2_lots = LotQty(0);
        self.tranche2_exit_price = PriceTicks(0);
        self.partial_harvest_legs = 0;
        self.realized_partial_net_1e8 = 0;
        self.realized_legs_fee_1e8 = 0;
        self.last_peak_favorable_ts_ns = 0;
        self.last_flow_sample_ts_ns = 0;
        self.hawkes_tracker.reset();
        self.last_obs_funding_cut_id = 0; self.pre_cut_funding_rate_sbps = 0; self.accrued_adverse_funding_sbps = 0; self.wald_sprt.reset();
        self.state = OrderLifecycleState::Idle;
        Some(ClosedTradeSummary {
            kind: leg2_kind, side, entry_price: avg_price_ticks, exit_price: PriceTicks(avg_exit_ticks),
            closed_lots: LotQty(tot_q), legs_consolidated: legs_total, gross_scaled_bps: ScaledBps(gross_bps),
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
        let prev_peak = peak_favorable_price;
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
        self.update_peak_favorable_timestamp_if_improved(prev_peak, peak_favorable_price, side, snap.ts_local_ns);
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
        if quote.defensive_lock_active {
            self.latch_defensive_floor_if_needed(true);
        } else if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.state {
            *defensive_floor_locked = false;
        }
        let _ = self.maybe_ratchet_chandelier_or_wald_sl_with_beta(snap, btc_leader_snap, epps_signed_beta);
        let reached_target = match side { Side::Bid => ref_trade.0 >= quote.target_price.0, Side::Ask => ref_trade.0 > 0 && ref_trade.0 <= quote.target_price.0 };
        if !reached_target { return None; }
        if matches!(quote.role, ExitOrderRole::Tranche1Harvest50) {
            let _ = self.record_tranche1_harvest(quote.target_price);
            return None;
        }
        if matches!(quote.role, ExitOrderRole::Tranche2HarvestPhi) {
            let _ = self.record_tranche2_harvest(quote.target_price);
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
        let own_flow = self.signed_intrinsic_flow_slow(side, snap).get();
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
        if self.chandelier_profit_sl_armed || !zeta_tox.is_finite() || zeta_tox < INV_PHI_TX {
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
        let sl_ref_vol = if self.open_vol_5m_bps.0 > 0 && self.open_vol_5m_bps.0 < snap.realized_vol_5m.0 { self.open_vol_5m_bps } else { snap.realized_vol_5m };
        let base_sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(sl_ref_vol, self.active_fees).raw();
        let wald_sl_bps = ((base_sl_bps * 52) / 100).max(c_rt * 4);
        let ref_m_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { avg_price_ticks.0 };
        let cur_g_sbps = ((ref_m_px - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1);
        let fee_mkr_rt = self.active_maker_rt_fee_bps();
        let slow_fl = self.signed_intrinsic_flow_slow(side, snap).get();
        let eff_mfe = if slow_fl <= -INV_PHI_TX {
            compute_c1_retained_mfe_bps(self.last_closed_mfe_bps, cur_g_sbps, fee_mkr_rt, self.learned_taker_prob_pm, tau_tick, self.chandelier_profit_sl_armed)
        } else {
            self.last_closed_mfe_bps
        };
        let floor_sl_bps = compute_diffusive_sl_floor_bps_v51(
            sl_ref_vol, ScaledBps(c_rt), self.learned_taker_prob_pm, zeta_tox,
            ScaledBps(eff_mfe), ScaledBps(fee_mkr_rt), ScaledBps(tau_tick), snap.wave_observables(), side,
        ).raw();
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

static LAST_WS_RTT_US: core::sync::atomic::AtomicU32 = core::sync::atomic::AtomicU32::new(0);
static LAST_WS_QUOTA_REM: core::sync::atomic::AtomicU16 = core::sync::atomic::AtomicU16::new(100);

#[inline]
pub fn record_live_ws_pulse(rtt_us: u32, quota_rem: u16) {
    if rtt_us > 0 {
        LAST_WS_RTT_US.store(rtt_us, core::sync::atomic::Ordering::Relaxed);
    }
    LAST_WS_QUOTA_REM.store(quota_rem, core::sync::atomic::Ordering::Relaxed);
}

#[inline]
#[must_use]
pub fn read_live_ws_pulse() -> (u32, u16) {
    (
        LAST_WS_RTT_US.load(core::sync::atomic::Ordering::Relaxed),
        LAST_WS_QUOTA_REM.load(core::sync::atomic::Ordering::Relaxed),
    )
}

/// Stack-only 1Hz intra-trade trajectory ring (`0` heap allocs inside the `200.0 Hz` loop).
#[derive(Copy, Clone, Debug)]
pub struct SlotIntraTradeRing {
    samples: [crate::vault_io::IntraTradeSample; crate::vault_io::MAX_INTRA_TRADE_SAMPLES],
    sample_count: u16,
    last_rel_sec: i64,
    opened_ts_ns: i64,
}

impl Default for SlotIntraTradeRing {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SlotIntraTradeRing {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            samples: [crate::vault_io::IntraTradeSample {
                rel_sec: 0,
                mid_delta_sbps: 0,
                tp_delta_sbps: 0,
                sl_delta_sbps: 0,
                ofi_pm: 0,
                tfi_pm: 0,
                wald_shrink_pm: 0,
                zeta_tox_pm: 0,
            }; crate::vault_io::MAX_INTRA_TRADE_SAMPLES],
            sample_count: 0,
            last_rel_sec: -1,
            opened_ts_ns: 0,
        }
    }

    #[inline]
    pub fn reset_on_open(&mut self, opened_ts: TimestampNs) {
        self.sample_count = 0;
        self.last_rel_sec = -1;
        self.opened_ts_ns = opened_ts.0;
    }

    #[inline]
    pub fn observe_200hz_tick(
        &mut self,
        eng: &SlotExecutionEngine,
        snap: &SymbolLobSnapshot,
        zeta_tox: f64,
    ) {
        let Some(smp) = eng.sample_intra_trade_1hz(snap, zeta_tox) else {
            self.last_rel_sec = -1;
            return;
        };
        let OrderLifecycleState::PositionOpenMakerExit { opened_ts, .. } = eng.state else {
            return;
        };
        if self.opened_ts_ns != opened_ts.0 {
            self.reset_on_open(opened_ts);
        }
        let sec_i64 = i64::from(smp.rel_sec);
        if sec_i64 <= self.last_rel_sec {
            return;
        }
        self.last_rel_sec = sec_i64;
        let idx = usize::from(self.sample_count);
        if idx < crate::vault_io::MAX_INTRA_TRADE_SAMPLES {
            self.samples[idx] = smp;
            self.sample_count = self.sample_count.saturating_add(1);
        }
    }

    #[inline]
    #[must_use]
    pub const fn len(&self) -> u16 {
        self.sample_count
    }

    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.sample_count == 0
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn build_position_path_summary(
        &self,
        trade_id: u64,
        slot_idx: usize,
        summary: &ClosedTradeSummary,
        opened_ts_ns: TimestampNs,
        closed_ts_ns: TimestampNs,
        markout_5h: crate::markout_tracker::MarkoutCurve5hBps,
    ) -> crate::vault_io::PositionPathSummary {
        let (ws_rtt_us, ws_quota_rem) = read_live_ws_pulse();
        crate::vault_io::PositionPathSummary {
            trade_id,
            slot_idx: (slot_idx.min(MAX_ACTIVE_PAIRS - 1)) as u8,
            side: summary.side,
            opened_ts_ns,
            closed_ts_ns,
            entry_ticks: summary.entry_price,
            exit_ticks: summary.exit_price,
            net_scaled_bps: summary.net_scaled_bps,
            markout_5h,
            sample_count: self.sample_count,
            ws_rtt_us,
            ws_quota_rem,
            samples: self.samples,
        }
    }
}

impl SlotExecutionEngine {
    /// Pure 1Hz intra-trade state projection in `ScaledBps` & permille (`0` allocations).
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    pub fn sample_intra_trade_1hz(
        &self,
        snap: &SymbolLobSnapshot,
        zeta_tox: f64,
    ) -> Option<crate::vault_io::IntraTradeSample> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            active_exit_price,
            resting_exit_price,
            sl_trigger_price,
            opened_ts,
            ..
        } = self.state
        else {
            return None;
        };
        if avg_price_ticks.0 <= 0 {
            return None;
        }
        let dt_s = (snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0) / 1_000_000_000)
            .clamp(0, i64::from(u16::MAX)) as u16;
        let mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
        } else if snap.mark_price.0 > 0 {
            snap.mark_price.0
        } else {
            avg_price_ticks.0
        };
        let sgn = side.sign_i64();
        let denom = avg_price_ticks.0.max(1);
        let to_i16_sbps = |px: i64| -> i16 {
            let v = ((px - avg_price_ticks.0).saturating_mul(sgn).saturating_mul(1_000_000)) / denom;
            v.clamp(i64::from(i16::MIN), i64::from(i16::MAX)) as i16
        };
        let tp_px = if resting_exit_price.0 > 0 {
            resting_exit_price.0
        } else if active_exit_price.0 > 0 {
            active_exit_price.0
        } else if self.wald_sprt.exhaustion_unwind_active {
            crate::exit_quoter::defensive_zero_loss_maker_exit_price_with_fees(
                side,
                avg_price_ticks,
                self.active_fees,
            )
            .0
        } else {
            let step = ((denom.saturating_mul(self.learned_mfe_bps.max(450))) / 1_000_000).max(1);
            avg_price_ticks.0.saturating_add(sgn.saturating_mul(step))
        };
        let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
            self.last_eff_sl_ticks.0
        } else {
            sl_trigger_price.0
        };
        let ofi_pm = (snap.factor_ofi.get() * 1_000.0).round().clamp(-1_000.0, 1_000.0) as i16;
        let tfi_pm = (snap.factor_tfi.get() * 1_000.0).round().clamp(-1_000.0, 1_000.0) as i16;
        let wald_pm = (self.wald_sprt.contraction_pct.clamp(0, 100) * 10) as u16;
        let z_pm = (zeta_tox.clamp(0.0, 6.0) * 1_000.0).round() as u16;
        Some(crate::vault_io::IntraTradeSample {
            rel_sec: dt_s,
            mid_delta_sbps: to_i16_sbps(mid_px),
            tp_delta_sbps: to_i16_sbps(tp_px),
            sl_delta_sbps: to_i16_sbps(eff_sl),
            ofi_pm,
            tfi_pm,
            wald_shrink_pm: wald_pm,
            zeta_tox_pm: z_pm,
        })
    }
}

#[cfg(test)]
mod v49_intra_trade_path_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::markout_tracker::MarkoutCurve5hBps;
    use crate::types::BoundedScore;

    #[test]
    fn test_v49_intra_trade_1hz_ring_zero_alloc_capture_and_ws_pulse() {
        record_live_ws_pulse(1_420, 96);
        assert_eq!(read_live_ws_pulse(), (1_420, 96));
        let mut s = SymbolLobSnapshot::new_empty();
        s.symbol_id = APPROVED_SPECS[3].symbol_id;
        s.ts_local_ns = TimestampNs(5_000_000_000);
        s.realized_vol_5m = ScaledBps(5_200);
        s.bids[0] = BookLevel { price: PriceTicks(7_200), qty: LotQty(400) };
        s.asks[0] = BookLevel { price: PriceTicks(7_201), qty: LotQty(400) };
        s.mark_price = PriceTicks(7_200);
        s.last_trade_price = PriceTicks(7_200);
        s.factor_ofi = BoundedScore::new_clamped(0.60);
        s.factor_tfi = BoundedScore::new_clamped(0.85);

        let mut eng = SlotExecutionEngine::new(3);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(650) };
        assert!(eng.try_arm_maker_entry(&s, v, LotQty(18)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(5_000_000_000)).is_ok());

        let mut ring = SlotIntraTradeRing::new();
        for tick in 0..450_i64 {
            s.ts_local_ns = TimestampNs(5_000_000_000 + tick * 5_000_000);
            s.bids[0].price = PriceTicks(7_200 + (tick / 200));
            s.asks[0].price = PriceTicks(7_201 + (tick / 200));
            ring.observe_200hz_tick(&eng, &s, 0.35);
        }
        assert_eq!(ring.len(), 3);
        let dummy_sum = ClosedTradeSummary {
            side: Side::Bid,
            closed_lots: LotQty(18),
            entry_price: PriceTicks(7_200),
            exit_price: PriceTicks(7_205),
            gross_scaled_bps: ScaledBps(694),
            fee_scaled_bps: ScaledBps(400),
            net_scaled_bps: ScaledBps(294),
            net_pnl_1e8: 380_000,
            fee_1e8: 518_400,
            legs_consolidated: 1,
            is_maker_exit: true,
            kind: ClosedTradeKind::MakerPassiveUnwind,
        };
        let path_sum = ring.build_position_path_summary(
            16, 3, &dummy_sum, TimestampNs(5_000_000_000), TimestampNs(7_250_000_000), MarkoutCurve5hBps::default(),
        );
        assert_eq!(path_sum.sample_count, 3);
        assert_eq!(path_sum.ws_rtt_us, 1_420);
        assert_eq!(path_sum.samples[2].rel_sec, 2);
    }
}

/// Dimensionless C1 stagnation & 5-horizon (`+60s`) post-fill compression factor in `[0.0, 1.0]`.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::suboptimal_flops
)]
pub fn compute_stagnation_sl_compression_factor(
    hold_ns: i64,
    tau_mkt_ns: i64,
    event_clock_mod: f64,
    mfe_above_floor_sbps: i64,
    signed_tfi: f64,
    zeta_tox_5h: f64,
) -> f64 {
    const PHI_SQ: f64 = 2.618_033_988_749_895;
    const INV_PHI: f64 = 0.618_033_988_749_895;
    const INV_PHI3: f64 = 0.236_067_977_499_790;

    if mfe_above_floor_sbps > 0 || hold_ns <= 0 {
        return 0.0;
    }
    let kappa = event_clock_mod.clamp(INV_PHI, 1.618_033_988_749_895);
    let tau_base_ns = (tau_mkt_ns.clamp(90_000_000_000, 180_000_000_000)) as f64;
    let tau_char_ns = ((INV_PHI3 * tau_base_ns) / kappa).max(15_000_000_000.0);
    const PHI: f64 = 1.618_033_988_749_895;
    const INV_PHI2: f64 = 0.381_966_011_250_105;
    const INV_PHI5: f64 = 0.090_169_943_749_47;
    let _ = INV_PHI3;
    let tau_dim = (hold_ns as f64) / tau_char_ns;
    let h_time = crate::wave_elliott::hermite_smoothstep_c1(tau_dim, INV_PHI, PHI_SQ);
    let eff_adv = (-signed_tfi.clamp(-1.0, 1.0)).max(0.0) + INV_PHI2 * zeta_tox_5h.clamp(0.0, PHI);
    let h_flow = crate::wave_elliott::hermite_smoothstep_c1(eff_adv, INV_PHI5, INV_PHI);
    let s_stag = (h_time * h_flow).clamp(0.0, 1.0);
    let h_tox = if signed_tfi < 0.0 {
        crate::wave_elliott::hermite_smoothstep_c1(zeta_tox_5h, INV_PHI, PHI_SQ)
    } else {
        0.0
    };
    s_stag.max(h_tox)
}

impl SlotExecutionEngine {
    /// Restores `learned_mfe_bps` if a `ServerMarkPriceStopLoss` closed with zero price discovery (Law II & V).
    #[inline]
    pub fn restore_mfe_if_zero_discovery_sl(
        &mut self,
        prev_learned_mfe_bps: i64,
        summary: &ClosedTradeSummary,
    ) {
        let fee_rt_mkr = self.active_fees.round_trip_maker_exit_bps().raw();
        if !crate::bayesian_governor::should_update_mfe_plasticity(
            summary.is_maker_exit,
            self.last_closed_mfe_bps,
            fee_rt_mkr,
        ) && self.learned_mfe_bps < prev_learned_mfe_bps
        {
            self.learned_mfe_bps = prev_learned_mfe_bps;
        }
    }

    /// V4.9 unified post-fill toxicity (`5h +60s`) + dimensionless Zero-MFE stagnation evaluator.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::manual_clamp,
        clippy::suboptimal_flops
    )]
    pub fn observe_post_fill_toxicity_and_wald_v49(
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
        const INV_PHI2_TX: f64 = 0.381_966_011_250_105;

        let zeta_v48 = self.observe_post_fill_toxicity_and_wald_v48(
            snap,
            btc_leader_snap,
            epps_signed_beta,
            markout_ring,
            event_dilation,
        );
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            peak_favorable_price,
            opened_ts,
            ..
        } = self.state
        else {
            return zeta_v48;
        };
        if avg_price_ticks.0 <= 0 {
            return zeta_v48;
        }
        let dt_ns = snap.ts_local_ns.0.saturating_sub(opened_ts.0);
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let c_rt = spec.expected_round_trip_cost_bps_dynamic(
            avg_price_ticks,
            self.learned_taker_prob_pm,
            self.active_fees,
        );
        let tau_tick_bps = spec
            .tick_to_price_scaled_bps(avg_price_ticks)
            .unwrap_or(ScaledBps(50))
            .raw();
        let fee_mkr_floor = self.active_maker_rt_fee_bps().max(tau_tick_bps);
        let state_mfe = ((peak_favorable_price.0 - avg_price_ticks.0)
            .saturating_mul(side.sign_i64())
            .saturating_mul(1_000_000))
            / avg_price_ticks.0.max(1);
        let cur_mfe_bps = state_mfe.max(self.last_closed_mfe_bps);
        let ref_now_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { avg_price_ticks.0 };
        let cur_gross_sbps = ((ref_now_px - avg_price_ticks.0).saturating_mul(side.sign_i64()).saturating_mul(1_000_000)) / avg_price_ticks.0.max(1);
        let retained_mfe_bps = compute_c1_retained_mfe_bps(cur_mfe_bps, cur_gross_sbps, fee_mkr_floor, self.learned_taker_prob_pm, tau_tick_bps, self.chandelier_profit_sl_armed);
        let mfe_above_floor = retained_mfe_bps.saturating_sub(fee_mkr_floor);
        if mfe_above_floor > 0 {
            return zeta_v48;
        }
        let z_active = markout_ring.active_fill_toxicity_zeta(opened_ts, c_rt);
        let z_5h = if dt_ns >= 60_000_000_000 {
            crate::markout_tracker::glosten_milgrom_toxicity_zeta_5h(markout_ring.last_fill_curve_5h(), c_rt).max(z_active)
        } else {
            z_active
        };
        let raw_own_flow = self.signed_intrinsic_flow_slow(side, snap).get();
        let btc_flow = if self.slot_idx == 0 {
            0.0
        } else {
            epps_signed_beta.clamp(-PHI_TX, PHI_TX)
                * ((0.55 * btc_leader_snap.factor_ofi.get() + 0.45 * btc_leader_snap.factor_tfi.get())
                    * side.sign_f64())
        };
        let own_flow = raw_own_flow
            .min(INV_PHI_TX * raw_own_flow + INV_PHI2_TX * btc_flow)
            .clamp(-1.0, 1.0);
        let tau_mkt_ns = self.intrinsic_tau_mkt_s.max(90).saturating_mul(1_000_000_000);
        let eval_ice_ns = self.intrinsic_toxicity_eval_ns(avg_price_ticks, snap.realized_vol_5m, event_dilation);
        let ice_comp = if !self.chandelier_profit_sl_armed && cur_gross_sbps < 0 && dt_ns >= eval_ice_ns {
            let open_v = (self.open_vol_5m_bps.raw().max(1_200)) as f64;
            let c_rt_f = (c_rt.raw().max(250)) as f64;
            let d0 = (PHI_SQ_TX * c_rt_f).max(INV_PHI2_TX * open_v);
            let d1 = (PHI_TX * PHI_SQ_TX * c_rt_f).max(INV_PHI_TX * open_v).max(d0 + 250.0);
            let h_px = crate::wave_elliott::hermite_smoothstep_c1((-cur_gross_sbps) as f64, d0, d1);
            let h_tm = crate::wave_elliott::hermite_smoothstep_c1((dt_ns as f64) / (eval_ice_ns.max(1) as f64), PHI_TX, PHI_TX * PHI_SQ_TX);
            (h_px * h_tm).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let lead_contagion_comp = if self.slot_idx > 0
            && !self.chandelier_profit_sl_armed
            && cur_gross_sbps < 0
            && dt_ns >= eval_ice_ns
        {
            let vol_own_f = (snap.realized_vol_5m.raw().max(1_200)) as f64;
            let vol_btc_f = (btc_leader_snap.realized_vol_5m.raw().max(1_200)) as f64;
            let vol_surge = (vol_btc_f / vol_own_f).sqrt().clamp(INV_PHI_TX, PHI_TX);
            let p_tkr_f = (self.learned_taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
            let s_lead = (-btc_flow) * vol_surge * (1.0 + PHI_TX * p_tkr_f);
            let h_lead = crate::wave_elliott::hermite_smoothstep_c1(s_lead, INV_PHI_TX, PHI_TX);
            let h_lag = 1.0
                - crate::wave_elliott::hermite_smoothstep_c1(
                    raw_own_flow,
                    -INV_PHI2_TX,
                    INV_PHI2_TX,
                );
            let h_under = crate::wave_elliott::hermite_smoothstep_c1(
                (-cur_gross_sbps as f64) / (fee_mkr_floor.max(100) as f64),
                INV_PHI2_TX,
                1.0,
            );
            (h_lead * h_lag * h_under).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let comp = ice_comp.max(lead_contagion_comp).max(compute_stagnation_sl_compression_factor(
            dt_ns,
            tau_mkt_ns,
            event_dilation,
            mfe_above_floor,
            own_flow,
            z_5h.max({
                let eval_ns = self.intrinsic_toxicity_eval_ns(avg_price_ticks, snap.realized_vol_5m, event_dilation);
                if dt_ns >= eval_ns && cur_gross_sbps < 0 && own_flow < -INV_PHI_TX {
                    let h_fl = crate::wave_elliott::hermite_smoothstep_c1(-own_flow, INV_PHI_TX, 1.0 - INV_PHI2_TX * INV_PHI2_TX);
                    let h_dd = crate::wave_elliott::hermite_smoothstep_c1((-cur_gross_sbps as f64) / (fee_mkr_floor.max(100) as f64), INV_PHI2_TX, PHI_TX);
                    PHI_SQ_TX * h_fl * h_dd
                } else {
                    0.0
                }
            }),
        ));
        if comp < INV_PHI2_TX {
            return zeta_v48;
        }
        let zeta_eff = zeta_v48.max(INV_PHI_TX + (PHI_SQ_TX - INV_PHI_TX) * comp);
        let (a_hold, b_unwind) =
            crate::kinematic_surface::wald_sprt_boundaries_from_permille(self.learned_taker_prob_pm);
        let jump = PHI_TX * comp * (1.0 + (-own_flow).max(0.0)) * b_unwind.abs();
        self.wald_sprt.log_likelihood =
            (self.wald_sprt.log_likelihood - jump).clamp(1.5 * b_unwind, a_hold);
        self.wald_sprt.exhaustion_unwind_active = true;
        let stag_pct = (48.0 + 34.0 * comp).round() as i64;
        self.wald_sprt.contraction_pct = self.wald_sprt.contraction_pct.max(stag_pct).clamp(48, 82);
        self.last_closed_wald_pct = self.last_closed_wald_pct.max(self.wald_sprt.contraction_pct);
        self.latch_defensive_floor_if_needed(true);
        zeta_eff
    }

    /// Unified V4.9 open-position step (`5h Toxicity + Zero-MFE Stagnation + Orthogonal MFE Guard`).
    #[allow(clippy::too_many_arguments)]
    pub fn step_open_position_v49(
        &mut self,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        gov: &OnlineAdaptiveGovernor,
        ledger: &mut ParitySyncLedger,
        epps_signed_beta: f64,
        markout_ring: &crate::markout_tracker::SlotMarkoutRing,
        event_dilation: f64,
    ) -> Option<ClosedTradeSummary> {
        let prev_mfe = self.learned_mfe_bps;
        self.sync_governor_dynamics(gov);
        let zeta_eff = self.observe_post_fill_toxicity_and_wald_v49(
            snap,
            btc_leader_snap,
            epps_signed_beta,
            markout_ring,
            event_dilation,
        );
        if zeta_eff > 0.0 {
            let _ = self.maybe_ratchet_sl_with_toxicity_v48(
                snap,
                btc_leader_snap,
                epps_signed_beta,
                zeta_eff,
            );
        }
        let mk_peak = markout_ring.peak_favorable_fill_markout_sbps(self.last_opened_ts);
        if mk_peak > self.last_closed_mfe_bps {
            self.last_closed_mfe_bps = mk_peak;
        }
        let closed = self.step_open_position_with_beta(snap, btc_leader_snap, gov, ledger, epps_signed_beta);
        if let Some(ref sum) = closed {
            self.restore_mfe_if_zero_discovery_sl(prev_mfe, sum);
            self.observe_markout_ring_favorable_peak(mk_peak);
        }
        closed
    }
}

#[cfg(test)]
mod v49_stagnation_unwind_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::markout_tracker::SlotMarkoutRing;
    use crate::types::BoundedScore;

    #[test]
    fn test_v49_slow_bleed_tao17_stagnation_compresses_sl_over_65pct_and_preserves_mfe() {
        let mut tao = SymbolLobSnapshot::new_empty();
        tao.symbol_id = APPROVED_SPECS[4].symbol_id;
        tao.ts_local_ns = TimestampNs(10_000_000_000);
        tao.realized_vol_5m = ScaledBps(2_619);
        tao.bids[0] = BookLevel { price: PriceTicks(19_335), qty: LotQty(300) };
        tao.asks[0] = BookLevel { price: PriceTicks(19_336), qty: LotQty(300) };
        tao.mark_price = PriceTicks(19_336);
        tao.last_trade_price = PriceTicks(19_336);

        let mut eng = SlotExecutionEngine::new(4);
        eng.intrinsic_tau_mkt_s = 180;
        eng.learned_mfe_bps = 680;
        let v = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(680),
        };
        assert!(eng.try_arm_maker_entry(&tao, v, LotQty(6)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        let init_sl_dist = eng.last_eff_sl_ticks.0 - 19_336;

        // Exact TAO #17 mild 15s markouts ([-0.51, -0.51, -0.51, -2.06] bps) where V4.8 stayed blind (< 0.618).
        let mut ring = SlotMarkoutRing::new();
        ring.arm_fill_probe(Side::Ask, PriceTicks(19_336), PriceTicks(19_336), TimestampNs(10_000_000_000));
        ring.step_clock(PriceTicks(19_337), TimestampNs(10_300_000_000), 200);
        ring.step_clock(PriceTicks(19_337), TimestampNs(11_200_000_000), 200);
        ring.step_clock(PriceTicks(19_337), TimestampNs(16_000_000_000), 200);
        ring.step_clock(PriceTicks(19_340), TimestampNs(25_500_000_000), 200);
        assert!(eng.observe_post_fill_toxicity_and_wald_v48(&tao, &tao, 1.0, &ring, 1.0).abs() <= f64::EPSILON);

        // At t = +105s (well before 327s), MFE == 0 & flow flipped bullish (+0.19 / +0.73) -> V4.9 compresses >65%.
        tao.ts_local_ns = TimestampNs(115_000_000_000);
        tao.bids[0] = BookLevel { price: PriceTicks(19_345), qty: LotQty(400) };
        tao.asks[0] = BookLevel { price: PriceTicks(19_346), qty: LotQty(150) };
        tao.mark_price = PriceTicks(19_345);
        tao.last_trade_price = PriceTicks(19_345);
        tao.factor_ofi = BoundedScore::new_clamped(0.19);
        tao.factor_tfi = BoundedScore::new_clamped(0.73);

        let z_v49 = eng.observe_post_fill_toxicity_and_wald_v49(&tao, &tao, 1.0, &ring, 1.0);
        assert!(z_v49 > 1.80 && eng.wald_sprt.exhaustion_unwind_active);
        assert!(eng.maybe_ratchet_sl_with_toxicity_v48(&tao, &tao, 1.0, z_v49).is_some());
        let new_sl_dist = eng.last_eff_sl_ticks.0 - 19_336;
        assert!(new_sl_dist * 100 <= init_sl_dist * 35);

        // Simulate SL hit at compressed trigger: learned_mfe_bps MUST remain 680 (zero shrinkage).
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_502_850_000);
        tao.ts_local_ns = TimestampNs(120_000_000_000);
        tao.mark_price = PriceTicks(eng.last_eff_sl_ticks.0 + 1);
        tao.last_trade_price = tao.mark_price;
        let closed = eng.step_open_position_v49(&tao, &tao, &gov, &mut ledger, 1.0, &ring, 1.0);
        assert!(closed.is_some());
        assert_eq!(eng.learned_mfe_bps, 680);
    }
}

/// Continuous C1 retained MFE shield: preserves 100% of MFE while `cur_gross >= 0`, decaying smoothly underwater.
#[inline]
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::suboptimal_flops
)]
pub fn compute_c1_retained_mfe_bps(
    observed_mfe_sbps: i64,
    cur_gross_sbps: i64,
    fee_rt_mkr_sbps: i64,
    taker_prob_pm: i64,
    tau_tick_sbps: i64,
    chandelier_armed: bool,
) -> i64 {
    const PHI_R: f64 = 1.618_033_988_749_895;
    const INV_PHI_R: f64 = 0.618_033_988_749_895;
    const INV_PHI2_R: f64 = 0.381_966_011_250_105;

    if observed_mfe_sbps <= 0 || cur_gross_sbps >= 0 || chandelier_armed {
        return observed_mfe_sbps;
    }
    let p_tkr = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let tol_sbps = (INV_PHI_R * (fee_rt_mkr_sbps.max(100) as f64) * (1.0 - PHI_R * p_tkr).max(0.25))
        .max(tau_tick_sbps.max(10) as f64);
    let u_under = ((-cur_gross_sbps) as f64) / tol_sbps;
    let h_give = crate::wave_elliott::hermite_smoothstep_c1(u_under, INV_PHI2_R, 1.0);
    ((observed_mfe_sbps as f64) * (1.0 - h_give)).round() as i64
}

/// Funding-adjusted Zero-Loss Maker floor covering round-trip Maker fees plus settled adverse funding.
#[inline]
#[must_use]
pub fn defensive_zero_loss_maker_exit_price_with_funding(
    side: Side,
    avg_price_ticks: PriceTicks,
    fees: FeeSchedule,
    adverse_funding_sbps: i64,
) -> PriceTicks {
    let base = defensive_zero_loss_maker_exit_price_with_fees(side, avg_price_ticks, fees);
    if adverse_funding_sbps <= 0 || avg_price_ticks.0 <= 0 {
        return base;
    }
    let extra = (avg_price_ticks.0.saturating_mul(adverse_funding_sbps) + 999_999) / 1_000_000;
    match side {
        Side::Bid => PriceTicks(base.0.saturating_add(extra)),
        Side::Ask => PriceTicks((base.0 - extra).max(1)),
    }
}

impl SlotExecutionEngine {
    /// Endogenously computes settled adverse 8h funding (`ScaledBps`) accrued since `opened_ts`.
    #[inline]
    #[must_use]
    pub const fn accrued_open_adverse_funding_sbps(&self, snap: &SymbolLobSnapshot) -> i64 {
        const MIN_UTC_NS: i64 = 1_577_836_800_000_000_000;
        const CUT_8H_NS: i64 = 28_800_000_000_000;
        let OrderLifecycleState::PositionOpenMakerExit { side, opened_ts, .. } = self.state else {
            return 0;
        };
        if opened_ts.0 < MIN_UTC_NS || snap.ts_local_ns.0 <= opened_ts.0 {
            return 0;
        }
        let cuts = snap.ts_local_ns.0.div_euclid(CUT_8H_NS) - opened_ts.0.div_euclid(CUT_8H_NS);
        if cuts <= 0 {
            return 0;
        }
        let adv_rate = side.sign_i64().saturating_mul(snap.funding_rate_scaled_bps.0);
        let fallback = if adv_rate <= 0 { 0 } else { adv_rate.saturating_mul(if cuts > 3 { 3 } else { cuts }) };
        if self.accrued_adverse_funding_sbps > fallback { self.accrued_adverse_funding_sbps } else { fallback }
    }
}

impl SlotExecutionEngine {
    /// Captures pre-boundary funding rate at 200Hz so post-settlement Bybit rate resets never erase paid funding.
    #[inline]
    pub fn observe_open_funding_cut_step(&mut self, snap: &SymbolLobSnapshot) {
        const MIN_UTC_NS: i64 = 1_577_836_800_000_000_000;
        const CUT_8H_NS: i64 = 28_800_000_000_000;
        let OrderLifecycleState::PositionOpenMakerExit { side, opened_ts, .. } = self.state else {
            return;
        };
        if opened_ts.0 < MIN_UTC_NS || snap.ts_local_ns.0 < opened_ts.0 {
            return;
        }
        let cur_cut = snap.ts_local_ns.0.div_euclid(CUT_8H_NS);
        if self.last_obs_funding_cut_id <= 0 {
            self.last_obs_funding_cut_id = opened_ts.0.div_euclid(CUT_8H_NS);
            self.pre_cut_funding_rate_sbps = snap.funding_rate_scaled_bps.0;
        }
        if cur_cut > self.last_obs_funding_cut_id {
            let eff_rate = if self.pre_cut_funding_rate_sbps != 0 {
                self.pre_cut_funding_rate_sbps
            } else {
                snap.funding_rate_scaled_bps.0
            };
            let adv = side.sign_i64().saturating_mul(eff_rate);
            if adv > 0 {
                let spans = (cur_cut - self.last_obs_funding_cut_id).clamp(1, 3);
                self.accrued_adverse_funding_sbps =
                    self.accrued_adverse_funding_sbps.saturating_add(adv.saturating_mul(spans));
            }
            self.last_obs_funding_cut_id = cur_cut;
        }
        self.pre_cut_funding_rate_sbps = snap.funding_rate_scaled_bps.0;
    }
}

impl SlotExecutionEngine {
    /// Continuous C1 defensive Zero-Loss latch bounds replacing legacy V4.0 `-200 sbps @ 3s` static scalar.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn elastic_defensive_latch_bounds(
        &self,
        side: Side,
        snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
        epps_signed_beta: f64,
    ) -> (i64, i64) {
        const PHI_L: f64 = 1.618_033_988_749_895;
        const INV_PHI_L: f64 = 0.618_033_988_749_895;
        const INV_PHI2_L: f64 = 0.381_966_011_250_105;

        let fee_rt = (self.active_maker_rt_fee_bps().max(100)) as f64;
        let p_tkr = (self.learned_taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
        let ir_mult = (1.0 + (self.learned_ir_x100.clamp(-60, 120) as f64) / 400.0).clamp(0.85, 1.30);
        let vol_f = (snap.realized_vol_5m.raw().max(1_200)) as f64;
        let calm_dip = (PHI_L * fee_rt)
            .max(INV_PHI2_L * vol_f * (1.0 - PHI_L * p_tkr).max(0.35))
            * ir_mult;
        let tox_dip = (INV_PHI_L * fee_rt * (1.0 - p_tkr)).max(200.0);
        let dir_s = side.sign_f64();
        let own_fl = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
        let btc_snr = if self.slot_idx == 0 {
            0.0
        } else {
            epps_signed_beta.clamp(-PHI_L, PHI_L)
                * OrthogonalFactorState::snr_composite_score(btc_leader_snap)
                * dir_s
        };
        let adv_press = (-own_fl).max(0.0) + INV_PHI2_L * (-btc_snr - 0.15).max(0.0);
        let h_tox = crate::wave_elliott::hermite_smoothstep_c1(adv_press, INV_PHI2_L, 0.854_101_966_249_685);
        let eff_dip = calm_dip - h_tox * (calm_dip - tox_dip).max(0.0);
        let base_hold_ns = ((INV_PHI_L * (self.intrinsic_tau_mkt_s.max(90) as f64) / 30.0) * 1e9)
            .clamp(2_800_000_000.0, 7_500_000_000.0);
        let eff_hold_ns = (base_hold_ns * (1.0 - 0.45 * h_tox)).round() as i64;
        (-(eff_dip.round() as i64), eff_hold_ns)
    }
}

#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn step_intrinsic_slow_flow_ema(
    prev_ema: BoundedScore,
    inst_flow: BoundedScore,
    dt_ns: i64,
    tau_mkt_s: i64,
    event_dilation: f64,
    ws_rtt_us: u64,
) -> BoundedScore {
    const PHI_F: f64 = 1.618_033_988_749_895;
    const INV_PHI_F: f64 = 0.618_033_988_749_895;
    const INV_PHI3_F: f64 = 0.236_067_977_499_79;
    if dt_ns <= 0 { return prev_ema; }
    const INV_PHI6_F: f64 = 0.055_728_090_000_84;
    let kappa = event_dilation.clamp(INV_PHI_F, PHI_F);
    let tau_s = (tau_mkt_s.clamp(90, 360) as f64) * INV_PHI6_F;
    let rtt_s = ((ws_rtt_us.clamp(1_000, 500_000) as f64) * 1e-6) * PHI_F;
    let tau_flow_s = ((INV_PHI3_F * tau_s) / kappa + rtt_s).clamp(INV_PHI3_F, PHI_F * PHI_F + PHI_F);
    let dt_s = ((dt_ns as f64) * 1e-9).clamp(0.000_5, 2.0);
    let alpha = (1.0 - (-dt_s / tau_flow_s).exp()).clamp(0.000_5, 1.0);
    BoundedScore::new_clamped(prev_ema.get() + alpha * (inst_flow.get() - prev_ema.get()))
}

#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn compute_adaptive_repeg_cooldown_ns(ws_rtt_us: u64, ws_quota_rem: u32) -> i64 {
    const PHI_SQ_R: f64 = 2.618_033_988_749_895;
    const INV_PHI_R: f64 = 0.618_033_988_749_895;
    let eff_rtt_ns = (if ws_rtt_us > 0 { ws_rtt_us } else { 45_000 }).clamp(5_000, 250_000) as f64 * 1_000.0;
    let q_norm = (f64::from(ws_quota_rem.min(100)) / 100.0).clamp(0.05, 1.0);
    let denom = (INV_PHI_R + INV_PHI_R * q_norm).max(INV_PHI_R);
    ((PHI_SQ_R * eff_rtt_ns) / denom).round() as i64
}

/// Aligned orderflow propulsion `P_align` in `[-1.0, 1.0]` penalized by passive Iceberg absorption (V5.4.2 M2).
#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops)]
pub fn compute_aligned_entry_propulsion_c1(
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_aligned: f64,
) -> f64 {
    let dir_s = side.sign_f64();
    let ofi_al = snap.factor_ofi.get() * dir_s;
    let tfi_al = snap.factor_tfi.get() * dir_s;
    if !ofi_al.is_finite() || !tfi_al.is_finite() {
        return 0.0;
    }
    let s_ice = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
        ofi_al,
        tfi_al,
        btc_aligned,
    );
    let raw = crate::alpha_3factor::INV_PHI * ofi_al + crate::alpha_3factor::INV_PHI_SQ * tfi_al;
    if ofi_al <= crate::alpha_3factor::INV_PHI_SQ || tfi_al <= crate::alpha_3factor::INV_PHI_SQ {
        raw.min(crate::alpha_3factor::INV_PHI) * (1.0 - s_ice)
    } else {
        (raw * (1.0 - s_ice)).clamp(-1.0, 1.0)
    }
}

/// Inside-Spread `PostOnly` Penny-Jump entry selector (V5.4.2 M2):
/// Quotes `1 tick` inside the spread (`bids[0] + 1 < asks[0]` on `Bid`, `asks[0] - 1 > bids[0]` on `Ask`)
/// ONLY when `spread_ticks >= 2`, `psi_health > INV_PHI_SQ`, and `aligned_propulsion > INV_PHI` (`0` PostOnly rejects).
#[inline]
#[must_use]
pub fn select_elastic_entry_maker_price_v542(
    side: Side,
    snap: &SymbolLobSnapshot,
    psi_health: f64,
    aligned_propulsion: f64,
) -> PriceTicks {
    let best_bid = snap.bids[0].price;
    let best_ask = snap.asks[0].price;
    if best_bid.0 <= 0 || best_ask.0 <= best_bid.0 {
        return side.select_entry_bbo_price(best_bid, best_ask);
    }
    let spread_ticks = best_ask.0 - best_bid.0;
    if spread_ticks >= 2
        && psi_health > crate::alpha_3factor::INV_PHI_SQ
        && aligned_propulsion > crate::alpha_3factor::INV_PHI
    {
        match side {
            Side::Bid => {
                let cand = PriceTicks(best_bid.0.saturating_add(1));
                if cand.0 > best_bid.0 && cand.0 < best_ask.0 {
                    return cand;
                }
            }
            Side::Ask => {
                let cand = PriceTicks(best_ask.0.saturating_sub(1));
                if cand.0 < best_ask.0 && cand.0 > best_bid.0 {
                    return cand;
                }
            }
        }
    }
    side.select_entry_bbo_price(best_bid, best_ask)
}

/// Agile `Re-Peg` cooldown `rtt_eff * (PHI^2 + PHI^3 * S_3(eta_fill; INV_PHI_CUBE, INV_PHI))` (V5.4.2 M2).
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_agile_repeg_cooldown_ns_v542(ws_rtt_us: u64, eta_fill: f64) -> i64 {
    let rtt_eff_ns = (if ws_rtt_us > 0 { ws_rtt_us.clamp(10_000, 350_000) } else { 50_000 } as f64)
        * 1_000.0;
    let s_eta = crate::wave_elliott::hermite_smoothstep_c1(
        eta_fill,
        crate::alpha_3factor::INV_PHI_CUBE,
        crate::alpha_3factor::INV_PHI,
    );
    (rtt_eff_ns * (crate::alpha_3factor::PHI_SQ + crate::alpha_3factor::PHI_CUBE * s_eta)).round()
        as i64
}

/// Cumulative chase cap from original armed entry price: strictly `< INV_PHI_SQ * (target_gross_bps - fee_rt_mkr_bps)` (V5.4.2 M2).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn passes_cumulative_repeg_chase_cap_v542(
    side: Side,
    original_armed_px: PriceTicks,
    candidate_repeg_px: PriceTicks,
    target_gross_bps: ScaledBps,
    fee_rt_mkr_bps: ScaledBps,
) -> bool {
    let anchor = original_armed_px.0;
    if anchor <= 0 || candidate_repeg_px.0 <= 0 {
        return false;
    }
    let fav_ticks = match side {
        Side::Bid => (candidate_repeg_px.0 - anchor).max(0),
        Side::Ask => (anchor - candidate_repeg_px.0).max(0),
    };
    if fav_ticks == 0 {
        return true;
    }
    let net_headroom_sbps = (target_gross_bps.raw() - fee_rt_mkr_bps.raw()) as f64;
    if net_headroom_sbps <= 0.0 {
        return false;
    }
    let chase_sbps = ((fav_ticks as f64) * 1_000_000.0) / (anchor as f64);
    chase_sbps < crate::alpha_3factor::INV_PHI_SQ * net_headroom_sbps
}

#[cfg(test)]
mod v51_f2_fsm_unlock_tests {
    use super::*;
    use crate::lob_shm::BookLevel;

    #[test]
    fn test_v51_f2_fsm_intrinsic_slow_ema_reversible_unlock_and_adaptive_repeg() {
        record_live_ws_pulse(75_000, 100);
        let cd_full = compute_adaptive_repeg_cooldown_ns(75_000, 100);
        let cd_low_q = compute_adaptive_repeg_cooldown_ns(75_000, 15);
        assert!((150_000_000..=250_000_000).contains(&cd_full) && cd_low_q > cd_full);
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_482_040_000);
        let mut eth = SymbolLobSnapshot::new_empty();
        eth.symbol_id = APPROVED_SPECS[1].symbol_id;
        eth.ts_local_ns = TimestampNs(5_000_000_000);
        eth.realized_vol_5m = ScaledBps(2_280);
        eth.bids[0] = BookLevel { price: PriceTicks(179_542), qty: LotQty(500) };
        eth.asks[0] = BookLevel { price: PriceTicks(179_543), qty: LotQty(400) };
        eth.mark_price = PriceTicks(179_542);
        eth.last_trade_price = PriceTicks(179_542);
        let mut eng = SlotExecutionEngine::new(1);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(950) };
        assert!(eng.try_arm_maker_entry(&eth, v, LotQty(1)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(5_000_000_000)).is_ok());
        eng.latch_defensive_floor_if_needed(true);
        eth.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.68),
            macro_phase: BoundedScore::new_clamped(0.90), oi_impulse: BoundedScore::new_clamped(0.30),
            liq_imbalance: BoundedScore::new_clamped(0.35), fib_proj_bps: ScaledBps(4_559),
        });
        eth.factor_ofi = BoundedScore::new_clamped(0.64);
        eth.factor_tfi = BoundedScore::new_clamped(0.75);
        eth.factor_geom = BoundedScore::new_clamped(0.88);
        for k in 1..=40_i64 {
            eth.ts_local_ns = TimestampNs(200_000_000_000 + k * 50_000_000);
            eth.bids[0].price = PriceTicks(179_618);
            eth.asks[0].price = PriceTicks(179_619);
            eth.mark_price = PriceTicks(179_618);
            eth.last_trade_price = PriceTicks(179_618);
            assert!(eng.step_open_position(&eth, &eth, &gov, &mut ledger).is_none());
        }
        assert!(matches!(eng.state, OrderLifecycleState::PositionOpenMakerExit { defensive_floor_locked: false, active_exit_price, .. } if active_exit_price.0 > 180_000));
    }
}

impl SlotExecutionEngine {
    #[inline]
    #[must_use]
    pub fn signed_intrinsic_flow_slow(&self, side: Side, snap: &SymbolLobSnapshot) -> BoundedScore {
        let dir_s = side.sign_f64();
        if self.last_flow_sample_ts_ns > 0 {
            BoundedScore::new_clamped(self.flow_ema_slow * dir_s)
        } else {
            BoundedScore::new_clamped((0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s)
        }
    }
}

#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn compute_diffusive_sl_floor_bps_v51(
    open_vol_5m: ScaledBps,
    c_rt_bps: ScaledBps,
    taker_prob_pm: i64,
    zeta_5h: f64,
    mfe_bps: ScaledBps,
    fee_rt_mkr: ScaledBps,
    tau_tick: ScaledBps,
    wave: WaveObservables,
    side: Side,
) -> ScaledBps {
    const PHI_D: f64 = 1.618_033_988_749_895;
    const PHI_SQ_D: f64 = 2.618_033_988_749_895;
    const PHI_CUBE_D: f64 = 4.236_067_977_499_79;
    const INV_PHI_D: f64 = 0.618_033_988_749_895;
    const INV_PHI2_D: f64 = 0.381_966_011_250_105;
    const SQRT_5_D: f64 = PHI_D + INV_PHI_D;

    let c_rt_f = (c_rt_bps.raw().max(150)) as f64;
    let tick_r = tau_tick.raw().max(10);
    let tox_floor = ((PHI_SQ_D * c_rt_f).round() as i64).max(tick_r.saturating_mul(4));
    let p_tkr = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let vol_1m_f = ((open_vol_5m.raw().max(1_200)) as f64) / SQRT_5_D;
    let diff_vol_bps = (PHI_D * vol_1m_f * (1.0 - INV_PHI2_D * p_tkr)).round() as i64;
    let diff_floor = ((PHI_CUBE_D * c_rt_f).round() as i64).max(diff_vol_bps).max(tox_floor);
    let mfe_ceil = (fee_rt_mkr.raw() as f64 * INV_PHI_D).max(tick_r as f64);
    let h_zero_disc = crate::wave_elliott::hermite_smoothstep_c1(
        mfe_ceil - (mfe_bps.raw() as f64),
        0.0,
        (tick_r.max(25)) as f64,
    );
    let s_mk = crate::wave_elliott::hermite_smoothstep_c1(zeta_5h, INV_PHI_D, PHI_SQ_D) * h_zero_disc;
    let aligned_macro = wave.macro_phase.get() * side.sign_f64();
    let s_w3 = if matches!(wave.regime, WaveRegime::ImpulseWave3) {
        crate::wave_elliott::hermite_smoothstep_c1(-aligned_macro, INV_PHI_D, 0.854_101_966_249_685)
    } else {
        0.0
    };
    let chi_tox = s_mk.max(s_w3).clamp(0.0, 1.0);
    let span = (diff_floor - tox_floor).max(0) as f64;
    ScaledBps((diff_floor - (chi_tox * span).round() as i64).max(tox_floor))
}

#[cfg(test)]
mod v51_f3_diffusive_sl_floor_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::markout_tracker::SlotMarkoutRing;

    #[test]
    fn test_v51_f3_5ms_pulse_immunity_avax3_survival_and_confirmed_toxic_floor() {
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_482_040_000);
        let ring = SlotMarkoutRing::new();
        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.ts_local_ns = TimestampNs(10_000_000_000);
        avax.realized_vol_5m = ScaledBps(2_983);
        avax.bids[0] = BookLevel { price: PriceTicks(7_155), qty: LotQty(420) };
        avax.asks[0] = BookLevel { price: PriceTicks(7_156), qty: LotQty(420) };
        avax.mark_price = PriceTicks(7_156);
        avax.last_trade_price = PriceTicks(7_156);
        let mut eng = SlotExecutionEngine::new(3);
        let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, target_excursion_scaled_bps: ScaledBps(780) };
        assert!(eng.try_arm_maker_entry(&avax, v, LotQty(18)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        for tick in 1..=12_400_i64 {
            avax.ts_local_ns = TimestampNs(10_000_000_000 + tick * 5_000_000);
            let px = if tick == 400 { 7_155 } else if tick >= 12_000 { 7_165 } else { 7_160 };
            avax.bids[0].price = PriceTicks(px - 1);
            avax.asks[0].price = PriceTicks(px);
            avax.mark_price = PriceTicks(px);
            avax.last_trade_price = PriceTicks(px);
            let spike = (12_200..=12_210).contains(&tick);
            avax.factor_ofi = BoundedScore::new_clamped(if spike { 0.88 } else { 0.10 });
            avax.factor_tfi = BoundedScore::new_clamped(if spike { 0.92 } else { 0.12 });
            assert!(eng.step_open_position_v49(&avax, &avax, &gov, &mut ledger, 1.0, &ring, 1.0).is_none());
        }
        assert!(eng.last_eff_sl_ticks.0 > 7_168);
        let w_opp = WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.71),
            macro_phase: BoundedScore::new_clamped(0.92), oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(5_200),
        };
        let fl_diff = compute_diffusive_sl_floor_bps_v51(ScaledBps(2_983), ScaledBps(418), 50, 0.0, ScaledBps(200), ScaledBps(400), ScaledBps(140), WaveObservables::default(), Side::Ask);
        let fl_tox = compute_diffusive_sl_floor_bps_v51(ScaledBps(2_983), ScaledBps(418), 50, 2.40, ScaledBps(0), ScaledBps(400), ScaledBps(140), w_opp, Side::Ask);
        assert!(fl_diff.raw() >= 1_800 && fl_tox.raw() <= 1_200);
    }
}
