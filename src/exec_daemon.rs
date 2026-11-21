//! 200Hz Zero-Jitter 12-Slot Execution Daemon (V5.5: Multi-Lot Priority, 3-Tranche Cascade, Free-Ride, Chain & Anti-Hostage).
#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use core::sync::atomic::Ordering;
use std::path::Path;
use std::process::ExitCode;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::alpha_3factor::{EppsBtcCorrelationTracker, OrthogonalFactorState, SignalVerdict};
use crate::bybit_client::InstrumentSpecVerification;
use crate::bybit_gateway::{run_daemon_startup_handshake, BybitAsyncGateway, GatewayCommand, GatewayEvent};
pub use crate::exec_gateway_handlers::{
    compute_open_positions_mtm_upl_1e8, compute_validation_epoch_loss_1e8,
    maybe_latch_validation_epoch_5usdt_shield,
};
use crate::exec_gateway_handlers::{
    bootstrap_live_adopt_if_enabled, handle_gateway_event, run_oos_520_certification_suite, ExecRuntimeState,
};
use crate::execution_fsm::{
    compute_agile_repeg_cooldown_ns_v542, compute_aligned_entry_propulsion_c1,
    passes_cumulative_repeg_chase_cap_v542, passes_elliott_macro_coherence_guard_with_health,
    passes_order_block_wall_guard_dynamic, passes_stoikov_repeg_coherence_guard_with_beta,
    select_elastic_entry_maker_price_v542, CanonicalExitQuote, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
};
use crate::exit_quoter::compute_validated_sl_ticks_with_fees;
use crate::fee_cadence_beta::{FeeSchedule, MonetaryPortfolioBetaGuard};
use crate::forensic_parquet::ParitySyncLedger;
use crate::governor_plasticity::DailySessionGovernor;
use crate::lob_shm::{LastVetoCode, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::replay_walkforward::settle_crossed_funding_boundary;
use crate::telemetry_hud::{
    dispatch_closed_trade_logs_with_locks, dispatch_open_positions_state, format_telegram_close_message,
    rehydrate_cold_mfe_seeds_from_scar_vault, rehydrate_daily_governor_from_vault,
    rehydrate_shadow_ledger, render_live_hud, render_trades_and_executive_dashboard,
    spawn_bounded_telegram_notifier, SlotForensicEntrySnapshot, DEFAULT_WALLET_EQUITY_1E8,
    SCAR_MEMORY_VAULT_PATH,
};
use crate::types::{
    now_epoch_ns, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, ANSI_GRAY,
    ANSI_GREEN, ANSI_RESET, APPROVED_SPECS,
};
use crate::vault_io::{
    spawn_vault_io_worker, AtomicLockWatch, FundingSettlementPersistTask, VaultIoCommand,
    MFE_PLASTICITY_VAULT_PATH,
};

const STAGGER_BAR_TO_FLOW_PM: i64 = 200;
const HOURLY_HANDSHAKE_REFRESH_NS: i64 = 3_600_000_000_000;
pub const SHM_HEARTBEAT_DROP_NS: u64 = 5_000_000_000;
const LIVE_BOOT_WS_CONVERGE_TIMEOUT_NS: i64 = 75_000_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ElasticQueueTimersNs {
    pub macro_abort_ns: i64,
    pub headwind_abort_ns: i64,
    pub repeg_wait_ns: i64,
    pub repeg_cooldown_ns: i64,
}

#[inline]
#[must_use]
pub const fn elastic_resting_queue_timers_ns(max_chase_ns: i64) -> ElasticQueueTimersNs {
    let tc = if max_chase_ns < 2_500_000_000 { 2_500_000_000 } else if max_chase_ns > 8_000_000_000 { 8_000_000_000 } else { max_chase_ns };
    let (raw_macro, raw_hw, raw_repeg, raw_cool) = (tc / 10, (tc * 440) / 1_000, tc / 2, (tc * 300) / 1_000);
    ElasticQueueTimersNs {
        macro_abort_ns: if raw_macro < 250_000_000 { 250_000_000 } else if raw_macro > 900_000_000 { 900_000_000 } else { raw_macro },
        headwind_abort_ns: if raw_hw < 1_200_000_000 { 1_200_000_000 } else if raw_hw > 3_500_000_000 { 3_500_000_000 } else { raw_hw },
        repeg_wait_ns: if raw_repeg < 1_400_000_000 { 1_400_000_000 } else if raw_repeg > 3_800_000_000 { 3_800_000_000 } else { raw_repeg },
        repeg_cooldown_ns: if raw_cool < 800_000_000 { 800_000_000 } else if raw_cool > 2_400_000_000 { 2_400_000_000 } else { raw_cool },
    }
}

#[inline]
#[must_use]
pub const fn is_safe_halt_standby(halt_or_sleep: bool, open_count: usize, pending_entry_count: usize, any_inflight: bool) -> bool {
    halt_or_sleep && open_count == 0 && pending_entry_count == 0 && !any_inflight
}

#[inline]
#[must_use]
pub const fn compute_exit_sync_schedule_with_fees(
    quote: &CanonicalExitQuote, prev_resting_exit_px: PriceTicks, avg_price_ticks: PriceTicks, fees: FeeSchedule,
) -> (bool, i64) {
    if quote.urgent_close_repeg && prev_resting_exit_px.0 > 0 && prev_resting_exit_px.0 != quote.target_price.0 {
        return (true, 250_000_000);
    }
    if !quote.urgent_close_repeg && matches!(quote.sub_reason, ExitSubReason::FibonacciImpulseSurf) && prev_resting_exit_px.0 > 0 {
        let diff = (quote.target_price.0 - prev_resting_exit_px.0).abs();
        let denom = if avg_price_ticks.0 > 1 { avg_price_ticks.0 } else { 1 };
        let drift_bps = (diff.saturating_mul(1_000_000)) / denom;
        let m_eff = if fees.maker_fee_bps.0 > 100 { fees.maker_fee_bps.0 } else { 100 };
        if drift_bps < (m_eff.saturating_mul(3)) / 5 { return (false, 1_800_000_000); }
    }
    (false, 1_350_000_000)
}

#[inline]
#[must_use]
pub const fn compute_exit_sync_schedule(
    quote: &CanonicalExitQuote,
    prev_resting_exit_px: PriceTicks,
    avg_price_ticks: PriceTicks,
) -> (bool, i64) {
    compute_exit_sync_schedule_with_fees(
        quote,
        prev_resting_exit_px,
        avg_price_ticks,
        FeeSchedule::VIP0_DEFAULT,
    )
}

/// Elastic sub-second *Crest Snap-In* & Runner amendment scheduler coupled to `ws_rtt_us` and `ws_quota_rem` (V5.5 O6).
/// - In `urgent_close_repeg` or `ShadowRunnerCrestHarvest`: schedules atomic amendment with delay
///   `(PHI * rtt_ns) / S_3(ws_quota_rem as f64; 15.0, 75.0).max(INV_PHI_SQ)` (`~113 ms` at `70 ms` RTT & `quota = 100`).
/// - In normal Runner float: scales smoothly between `PHI^3 * rtt_ns` and `PHI^5 * rtt_ns` by drift in bps and `ws_quota_rem`.
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_exit_sync_schedule_elastic(
    quote: &CanonicalExitQuote,
    prev_resting_exit_px: PriceTicks,
    avg_price_ticks: PriceTicks,
    ws_rtt_us: u32,
    ws_quota_rem: u16,
) -> (bool, i64) {
    const PHI_E: f64 = crate::alpha_3factor::PHI;
    const PHI_CUBE_E: f64 = crate::alpha_3factor::PHI_CUBE;
    const PHI_FIFTH_E: f64 = crate::alpha_3factor::PHI_FOURTH * crate::alpha_3factor::PHI;
    const INV_PHI_E: f64 = crate::alpha_3factor::INV_PHI;
    const INV_PHI_SQ_E: f64 = crate::alpha_3factor::INV_PHI_SQ;

    if ws_rtt_us == 0 {
        return compute_exit_sync_schedule(quote, prev_resting_exit_px, avg_price_ticks);
    }
    let rtt_ns = (f64::from(ws_rtt_us.clamp(5_000, 350_000))) * 1_000.0;
    let s_quota = crate::wave_elliott::hermite_smoothstep_c1(f64::from(ws_quota_rem), 15.0, 75.0)
        .max(INV_PHI_SQ_E);
    let is_crest_or_urgent = quote.urgent_close_repeg
        || matches!(quote.sub_reason, ExitSubReason::ShadowRunnerCrestHarvest);
    if is_crest_or_urgent {
        let snap_delay_ns = ((PHI_E * rtt_ns) / s_quota).round() as i64;
        let preempt = prev_resting_exit_px.0 > 0 && prev_resting_exit_px.0 != quote.target_price.0;
        return (preempt, snap_delay_ns);
    }
    let drift_sbps = if prev_resting_exit_px.0 > 0 {
        let diff = (quote.target_price.0 - prev_resting_exit_px.0).abs();
        ((diff as f64) * 1_000_000.0) / (avg_price_ticks.0.max(1) as f64)
    } else {
        0.0
    };
    let fee_ref_sbps = (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw().saturating_mul(2)) as f64;
    let s_drift = crate::wave_elliott::hermite_smoothstep_c1(
        drift_sbps,
        INV_PHI_SQ_E * fee_ref_sbps,
        PHI_E * fee_ref_sbps,
    );
    let urgency_blend = (INV_PHI_E * s_drift + INV_PHI_SQ_E * s_quota).clamp(0.0, 1.0);
    let mult = PHI_FIFTH_E - (PHI_FIFTH_E - PHI_CUBE_E) * urgency_blend;
    let runner_delay_ns = (mult * rtt_ns).round() as i64;
    (false, runner_delay_ns)
}

/// Normalized `C1` opposing `L2 + L3` book vacuum in `[0.0, 1.0]` for multi-lot priority routing (V5.5 O1).
#[inline]
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn compute_vacuum_l2_l3_c1(side: Side, snap: &SymbolLobSnapshot) -> f64 {
    let (own_l1_lots, opp_lvls) = match side {
        Side::Bid => (snap.bids[0].qty.0, &snap.asks),
        Side::Ask => (snap.asks[0].qty.0, &snap.bids),
    };
    if own_l1_lots <= 0 || (opp_lvls[1].price.0 <= 0 && opp_lvls[2].price.0 <= 0) {
        return 0.0;
    }
    let two_own = 2.0 * (own_l1_lots as f64);
    let opp_l2_l3 = (opp_lvls[1].qty.0.max(0) + opp_lvls[2].qty.0.max(0)) as f64;
    let ratio = two_own / (two_own + opp_l2_l3).max(1e-9);
    crate::wave_elliott::hermite_smoothstep_c1(
        ratio,
        0.50,
        crate::alpha_3factor::ONE_MINUS_INV_PHI_FOURTH,
    )
}

/// Multi-lot offensive admission utility `U_i` (`x1000` fixed-point, V5.5 O1):
/// `U_i = ev_net_sbps * (1.0 + INV_PHI * psi_health * S_3(lots as f64; 1.0, 4.0)) * (1.0 + INV_PHI_SQ * vacuum_l2_l3) * (INV_PHI + INV_PHI_SQ * eta_fill)`.
#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops)]
pub fn compute_offensive_entry_utility_score_x1000_v55(
    ev_net_sbps: i64,
    lots: LotQty,
    psi_health: f64,
    vacuum_l2_l3: f64,
    eta_fill: f64,
) -> i64 {
    if ev_net_sbps <= 0 || lots.0 <= 0 {
        return 0;
    }
    let psi = if psi_health.is_finite() { psi_health.clamp(0.0, 1.0) } else { 0.0 };
    let vac = if vacuum_l2_l3.is_finite() { vacuum_l2_l3.clamp(0.0, 1.0) } else { 0.0 };
    let eta = if eta_fill.is_finite() { eta_fill.clamp(0.0, 1.0) } else { 0.5 };
    let s_lots = crate::wave_elliott::hermite_smoothstep_c1(lots.0 as f64, 1.0, 4.0);
    let m_lots = 1.0 + crate::alpha_3factor::INV_PHI * psi * s_lots;
    let m_vac = 1.0 + crate::alpha_3factor::INV_PHI_SQ * vac;
    let m_eta = crate::alpha_3factor::INV_PHI + crate::alpha_3factor::INV_PHI_SQ * eta;
    ((ev_net_sbps as f64) * m_lots * m_vac * m_eta * 1_000.0).round() as i64
}

/// Zero-allocation in-place descending sorter over a stack-resident `[(usize, i64); MAX_ACTIVE_PAIRS]` candidate buffer (V5.5 O1).
#[inline]
pub fn sort_offensive_candidates_zero_alloc(
    candidates: &mut [(usize, i64); MAX_ACTIVE_PAIRS],
    count: usize,
) {
    let n = count.min(MAX_ACTIVE_PAIRS);
    for i in 1..n {
        let key = candidates[i];
        let mut j = i;
        while j > 0 && candidates[j - 1].1 < key.1 {
            candidates[j] = candidates[j - 1];
            j -= 1;
        }
        candidates[j] = key;
    }
}

#[derive(Copy, Clone)]
struct StagedOffensiveCandidate {
    side: Side,
    verdict: SignalVerdict,
    lots: LotQty,
    risk_ppm: i64,
    beta_x1000: i64,
    epps_corr: f64,
    btc_lead_opt: Option<f64>,
}

#[inline]
#[must_use]
pub const fn clamp_resting_queue_ahead_physical(cur_q: LotQty, live_l1_q: LotQty, opp_tape_lots: LotQty, prev_l1_q: LotQty) -> LotQty {
    let hit = if opp_tape_lots.0 > 0 { opp_tape_lots.0 } else { 0 };
    let rem = cur_q.0.saturating_sub(hit);
    let mut next_q = if rem > 0 { rem } else { 0 };
    let l1_cap = if live_l1_q.0 > 0 { live_l1_q.0 } else { 0 };
    if prev_l1_q.0 > 0 && l1_cap < prev_l1_q.0 && next_q > 1 {
        let cancel_drop = prev_l1_q.0.saturating_sub(l1_cap).saturating_sub(hit);
        if cancel_drop > 0 {
            let prop_c = ((cancel_drop as i128) * (next_q as i128) / (prev_l1_q.0 as i128)) as i64;
            let after_c = next_q.saturating_sub(prop_c);
            next_q = if after_c > 1 { after_c } else { 1 };
        }
    }
    if next_q > l1_cap { next_q = l1_cap; }
    LotQty(next_q)
}

#[inline]
#[must_use]
pub const fn step_l1_queue_ahead_lots(
    side: Side, entry_price: PriceTicks, at_l1: bool, live_l1_q: LotQty,
    prev_l1_q_obs: LotQty, prev_q_ahead: LotQty, tape_delta_lots: i64, last_trade_price: PriceTicks,
) -> (LotQty, LotQty, LotQty) {
    let hits_px = match side {
        Side::Bid => last_trade_price.0 > 0 && last_trade_price.0 <= entry_price.0,
        Side::Ask => last_trade_price.0 >= entry_price.0,
    };
    let opp_hit = if tape_delta_lots > 0 && hits_px { LotQty(tape_delta_lots) } else { LotQty(0) };
    if at_l1 {
        let l1_cap = LotQty(if live_l1_q.0 > 0 { live_l1_q.0 } else { 0 });
        (clamp_resting_queue_ahead_physical(prev_q_ahead, l1_cap, opp_hit, prev_l1_q_obs), opp_hit, l1_cap)
    } else {
        let rem = prev_q_ahead.0.saturating_sub(opp_hit.0);
        (LotQty(if rem > 0 { rem } else { 0 }), opp_hit, LotQty(0))
    }
}

#[inline]
#[must_use]
pub const fn slot_position_risk_ppm_with_fees(
    spec: PairSpec, wallet_1e8: i64, px: PriceTicks, lots: LotQty, vol_5m: ScaledBps, fees: FeeSchedule,
) -> i64 {
    let px_eff = PriceTicks(if px.0 > 0 { px.0 } else { 1 });
    let qty_eff = LotQty(if lots.0 > 0 { lots.0 } else { 1 });
    let n_pos = { let n = spec.lot_notional_1e8(qty_eff, px_eff); if n > 1 { n } else { 1 } };
    let w_pos = if wallet_1e8 > 100_000_000 { wallet_1e8 } else { 100_000_000 };
    let sl_bps = PairSpec::elastic_server_sl_distance_bps_with_fees(vol_5m, fees).raw();
    let tau_bps = match spec.tick_to_price_scaled_bps(px_eff) { Ok(b) if b.0 > 0 => b.0, _ => 100 };
    let fee_tkr = { let tr = fees.round_trip_taker_exit_bps().raw(); if tr > 100 { tr } else { 100 } };
    (((n_pos as i128) * ((sl_bps.saturating_add(tau_bps).saturating_add(fee_tkr)) as i128)) / (w_pos as i128)) as i64
}

#[must_use]
pub fn build_monetary_beta_guard_with_epps(
    wallet_1e8: i64, engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    epps_trackers: &[EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS], fees: FeeSchedule,
) -> MonetaryPortfolioBetaGuard {
    let mut guard = MonetaryPortfolioBetaGuard::new();
    let btc_vol = ScaledBps(cached_snaps[0].realized_vol_5m.0.max(2_500));
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, .. } => {
                let snap = &cached_snaps[s_idx];
                let risk_ppm = slot_position_risk_ppm_with_fees(APPROVED_SPECS[s_idx], wallet_1e8, entry_price, qty, snap.realized_vol_5m, fees);
                let beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                guard.register_slot_open(s_idx, side, risk_ppm, beta_x1000);
            }
            OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, open_lots, .. } => {
                let snap = &cached_snaps[s_idx];
                let risk_ppm = slot_position_risk_ppm_with_fees(APPROVED_SPECS[s_idx], wallet_1e8, avg_price_ticks, open_lots, snap.realized_vol_5m, fees);
                let beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                let (_, residual_s3_scale) = engines[s_idx].house_money_residual_risk_1e8_and_s3_scale();
                guard.register_slot_open_house_money(s_idx, side, risk_ppm, beta_x1000, residual_s3_scale);
            }
        }
    }
    guard
}

#[inline]
pub fn publish_slot_veto_code(arena: &VortexShmArena, slot_idx: usize, veto: LastVetoCode) {
    if let Ok(tel) = arena.telemetry(slot_idx) {
        let mut cur = tel.read_consistent_or_default(2);
        let code = veto as u8;
        if cur.last_veto_code != code { cur.last_veto_code = code; tel.write_consistent(&cur); }
    }
}

#[must_use]
pub fn select_safe_live_sl_ticks_with_fees(
    side: Side, avg_price_ticks: PriceTicks, verified_exchange_sl: PriceTicks,
    candidate_eff_sl: PriceTicks, snap: &SymbolLobSnapshot, fees: FeeSchedule,
) -> Option<PriceTicks> {
    if avg_price_ticks.0 <= 0 { return None; }
    let base_sl = compute_validated_sl_ticks_with_fees(side, avg_price_ticks, snap, fees).unwrap_or(candidate_eff_sl);
    let mut target = match side {
        Side::Bid => {
            let t = if candidate_eff_sl.0 > base_sl.0 { candidate_eff_sl } else { base_sl };
            if verified_exchange_sl.0 > t.0 { verified_exchange_sl } else { t }
        }
        Side::Ask => {
            let t = if candidate_eff_sl.0 > 0 && candidate_eff_sl.0 < base_sl.0 { candidate_eff_sl } else { base_sl };
            if verified_exchange_sl.0 > 0 && verified_exchange_sl.0 < t.0 { verified_exchange_sl } else { t }
        }
    };
    if target.0 <= 0 { return None; }
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    match side {
        Side::Bid => {
            target = PriceTicks(target.0.min(ref_mark.0.min(ref_last.0) - 3).max(1));
            if verified_exchange_sl.0 > 0 && target.0 <= verified_exchange_sl.0 { return None; }
        }
        Side::Ask => {
            target = PriceTicks(target.0.max(ref_mark.0.max(ref_last.0).saturating_add(3)));
            if verified_exchange_sl.0 > 0 && target.0 >= verified_exchange_sl.0 { return None; }
        }
    }
    PairSpec::validate_stop_loss_geometry(side, target, ref_last, ref_mark).ok()
}

#[inline]
#[must_use]
pub fn select_safe_live_sl_ticks(
    side: Side, avg_price_ticks: PriceTicks, verified_exchange_sl: PriceTicks,
    candidate_eff_sl: PriceTicks, snap: &SymbolLobSnapshot,
) -> Option<PriceTicks> {
    select_safe_live_sl_ticks_with_fees(side, avg_price_ticks, verified_exchange_sl, candidate_eff_sl, snap, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
#[must_use]
pub fn elastic_same_side_stagger_bounds(tau_mkt_s: i64, vol_5m: ScaledBps, b3x_bps: ScaledBps, fee_rt_bps: i64) -> (i64, i64) {
    let tau_s = if tau_mkt_s > 0 { tau_mkt_s } else { 300 };
    let stagger_s = ((tau_s.saturating_mul(STAGGER_BAR_TO_FLOW_PM)) / 1_000).clamp(15, 180);
    let eff_rt = fee_rt_bps.max(100);
    let pad = ((b3x_bps.raw().clamp(300, 1_500) / 30).clamp(10, 50)).max((vol_5m.raw().clamp(0, 4_000) * 12) / 1_000);
    (stagger_s.saturating_mul(1_000_000_000), eff_rt.saturating_mul(2).saturating_add(pad))
}

#[inline]
#[must_use]
pub const fn is_at_or_ahead_of_l1(side: Side, entry_price: PriceTicks, snap: &SymbolLobSnapshot) -> bool {
    match side {
        Side::Bid => snap.bids[0].price.0 > 0 && entry_price.0 >= snap.bids[0].price.0,
        Side::Ask => snap.asks[0].price.0 > 0 && entry_price.0 <= snap.asks[0].price.0,
    }
}

#[must_use]
pub fn can_stage_same_direction_entry(
    candidate_slot: usize, side: Side, now_ns: TimestampNs,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS], cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> bool {
    let neutral_epps = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS];
    can_stage_same_direction_entry_v542(
        candidate_slot,
        side,
        now_ns,
        engines,
        cached_snaps,
        &neutral_epps,
        0.0,
        50,
    )
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn can_stage_same_direction_entry_v542(
    candidate_slot: usize,
    side: Side,
    now_ns: TimestampNs,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    epps_trackers: &[EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS],
    psi_health: f64,
    cand_taker_prob_pm: i64,
) -> bool {
    let cand_corr = if candidate_slot == 0 {
        1.0
    } else {
        epps_trackers[candidate_slot.min(MAX_ACTIVE_PAIRS - 1)].correlation(false)
    };
    let mut same_side_resting = 0_usize;
    for (s_idx, eng) in engines.iter().enumerate() {
        if s_idx == candidate_slot { continue; }
        match eng.state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side: s, .. } => {
                if s == side { return false; }
            }
            OrderLifecycleState::RestingEntryQueue { side: s, entry_price, .. } => {
                if s == side {
                    same_side_resting += 1;
                    if same_side_resting >= 2 || psi_health <= crate::alpha_3factor::INV_PHI_SQ {
                        return false;
                    }
                    let first_fav_left_behind =
                        crate::kinematic_surface::is_bbo_favorably_past_resting_entry(
                            s,
                            entry_price,
                            &cached_snaps[s_idx],
                        );
                    let s1_corr = if s_idx == 0 {
                        1.0
                    } else {
                        epps_trackers[s_idx].correlation(false)
                    };
                    let max_tkr_prob =
                        (eng.learned_taker_prob_pm.max(cand_taker_prob_pm).max(0) as f64) * 1e-3;
                    let low_epps_and_healthy_tox = (s1_corr * cand_corr).abs()
                        < crate::alpha_3factor::INV_PHI
                        && max_tkr_prob < crate::alpha_3factor::INV_PHI_FOURTH;
                    if !first_fav_left_behind && !low_epps_and_healthy_tox {
                        return false;
                    }
                }
            }
            OrderLifecycleState::PositionOpenMakerExit { side: s, avg_price_ticks, tranche1_harvested, opened_ts, .. } => {
                if s == side {
                    let (_, residual_s3_scale) = eng.house_money_residual_risk_1e8_and_s3_scale();
                    if residual_s3_scale <= 1e-9 {
                        continue;
                    }
                    let snap = &cached_snaps[s_idx];
                    let b3x = APPROVED_SPECS[s_idx].three_x_friction_barrier_bps_dynamic(avg_price_ticks.max(PriceTicks(1)), eng.learned_taker_prob_pm, eng.active_fees);
                    let (stagger_ns, req_cushion_bps) = elastic_same_side_stagger_bounds(eng.intrinsic_tau_mkt_s, snap.realized_vol_5m, b3x, eng.active_fees.round_trip_maker_exit_bps().raw());
                    let eff_stagger_ns = ((stagger_ns as f64) * residual_s3_scale).round() as i64;
                    let eff_req_cushion_bps = ((req_cushion_bps as f64) * residual_s3_scale).round() as i64;
                    if now_ns.0.saturating_sub(opened_ts.0) < eff_stagger_ns { return false; }
                    let ref_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { match s { Side::Bid => snap.bids[0].price.0, Side::Ask => snap.asks[0].price.0 } };
                    if ref_px <= 0 { return false; }
                    let cur_gross_bps = ((ref_px - avg_price_ticks.0) * s.sign_i64()).saturating_mul(1_000_000) / avg_price_ticks.0.max(1);
                    let tranche1_secured = (tranche1_harvested || eng.tranche1_harvests_count >= 1) && cur_gross_bps >= 0;
                    if cur_gross_bps < eff_req_cushion_bps && !eng.chandelier_profit_sl_armed && !tranche1_secured { return false; }
                }
            }
        }
    }
    true
}

/// Hard cap on total concurrently armed/open slots (`V5.5.5` B-06 3rd-slot unlock keeps `total < 6`).
pub const MAX_TOTAL_ARMED_SLOTS_V555: usize = 6;

/// `V5.5.5` B-06 *Buried-Queue 3rd-Slot Unlock* for the `2-Slot Epps Queue Stagger` (pure, shared by live
/// daemon, walk-forward replay and Digital Twin). Arms a 3rd same-side slot only when exactly 2 same-side
/// `RestingEntryQueue` slots are buried (`v_prio < INV_PHI_CUBE` after `elapsed >= PHI^2 * ws_rtt`), no other
/// same-side order is pending/open, the candidate enters via Inside-Spread Penny-Jump (`SPR >= 2`, `q_ahead = 0`)
/// or an ultra-thin L1 queue (`q_l1 <= INV_PHI * lots`), total slots stay `< 6` and the
/// `MonetaryPortfolioBetaGuard` (`<= 14,400 ppm` total / `<= 10,800 ppm` signed beta) admits the risk.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn allows_buried_queue_third_slot_unlock_v555(
    candidate_slot: usize,
    side: Side,
    now_ns: TimestampNs,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    queue_init_cur: &[(LotQty, LotQty); MAX_ACTIVE_PAIRS],
    cand_snap: &SymbolLobSnapshot,
    cand_lots: LotQty,
    psi_health: f64,
    ws_rtt_us: u32,
    beta_guard: &MonetaryPortfolioBetaGuard,
    risk_ppm: i64,
    beta_x1000: i64,
) -> bool {
    if candidate_slot >= MAX_ACTIVE_PAIRS
        || ws_rtt_us == 0
        || !psi_health.is_finite()
        || psi_health <= crate::alpha_3factor::INV_PHI_SQ
    {
        return false;
    }
    let bury_ns = crate::alpha_3factor::PHI_SQ * (f64::from(ws_rtt_us) * 1_000.0);
    let mut active_total = 0_usize;
    let mut buried_same_side = 0_usize;
    for (s_idx, (eng, &(init_q, cur_q))) in engines.iter().zip(queue_init_cur.iter()).enumerate() {
        if s_idx == candidate_slot {
            if !matches!(eng.state, OrderLifecycleState::Idle) {
                return false;
            }
            continue;
        }
        match eng.state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::RestingEntryQueue { side: s, resting_since_ts, .. } => {
                active_total += 1;
                if s == side {
                    let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0).max(0) as f64;
                    let v_prio = compute_moallemi_yuan_queue_priority_c1(init_q, cur_q);
                    if elapsed_ns < bury_ns || v_prio >= crate::alpha_3factor::INV_PHI_CUBE {
                        return false;
                    }
                    buried_same_side += 1;
                }
            }
            OrderLifecycleState::PlacingEntryMaker { side: s, .. }
            | OrderLifecycleState::PositionOpenMakerExit { side: s, .. } => {
                active_total += 1;
                if s == side {
                    return false;
                }
            }
        }
    }
    if buried_same_side != 2 || active_total.saturating_add(1) >= MAX_TOTAL_ARMED_SLOTS_V555 {
        return false;
    }
    let penny_jump = OnlineAdaptiveGovernor::qualifies_inside_spread_penny_jump_c1(side, cand_snap, psi_health);
    let l1_q = match side {
        Side::Bid => cand_snap.bids[0].qty.0,
        Side::Ask => cand_snap.asks[0].qty.0,
    }
    .max(0) as f64;
    let ultra_thin = l1_q <= (crate::alpha_3factor::INV_PHI * (cand_lots.0.max(1) as f64)).max(1.0);
    (penny_jump || ultra_thin) && beta_guard.can_open_with_risk_ppm(candidate_slot, side, risk_ppm, beta_x1000)
}

pub fn settle_live_8h_funding_cuts_v48(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS], cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    last_funding_cut: &mut [i64; MAX_ACTIVE_PAIRS], ledger: &mut ParitySyncLedger,
    vault_tx: Option<&SyncSender<VaultIoCommand>>,
    mut daily_ctx: Option<(&mut DailySessionGovernor, TimestampNs, i64, &OnlineAdaptiveGovernor)>,
) -> u32 {
    let mut settled_count = 0_u32;
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        if let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, open_lots, opened_ts, .. } = engines[s_idx].state {
            let spec = APPROVED_SPECS[s_idx];
            let snap = &cached_snaps[s_idx];
            if let Some(f_1e8) = settle_crossed_funding_boundary(spec, side, open_lots, avg_price_ticks, opened_ts.0, snap, &mut last_funding_cut[s_idx], ledger) {
                settled_count += 1;
                let mark_px = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
                let pos_notional = spec.lot_notional_1e8(open_lots, mark_px);
                if let Some(vtx) = vault_tx {
                    let _ = vtx.try_send(VaultIoCommand::RecordFundingSettlement(FundingSettlementPersistTask {
                        ts_ns: snap.ts_local_ns.0, slot_idx: s_idx, side, funding_cut_id: last_funding_cut[s_idx],
                        funding_rate_scaled_bps: snap.funding_rate_scaled_bps.0, position_notional_1e8: pos_notional,
                        funding_net_1e8: f_1e8, cum_funding_1e8: ledger.cumulative_funding_1e8, cum_net_pnl_1e8: ledger.cumulative_net_pnl_1e8,
                    }));
                }
                if let Some((ref mut dg, now_ns, live_eq, gov)) = daily_ctx {
                    let _ = dg.observe_realized_cashflow(
                        now_ns, f_1e8, false, false, false, live_eq.saturating_add(f_1e8),
                        ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
                        gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
                    );
                    if let Some(vtx) = vault_tx {
                        let _ = vtx.try_send(VaultIoCommand::SaveDailyGovernor { ts_ns: now_ns.0, state: **dg });
                    }
                }
            }
        } else {
            last_funding_cut[s_idx] = 0;
        }
    }
    settled_count
}

#[inline]
pub fn settle_live_8h_funding_cuts(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS], cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    last_funding_cut: &mut [i64; MAX_ACTIVE_PAIRS], ledger: &mut ParitySyncLedger,
) -> u32 {
    settle_live_8h_funding_cuts_v48(engines, cached_snaps, last_funding_cut, ledger, None, None)
}

/// Live startup convergence gate (V5.3.2): requires warmup floor (`now_ns >= entry_warmup_until_ns`),
/// live L2 book on BTC leader (`slot 0`) + EMA convergence + at least `MAX_ACTIVE_PAIRS / 2` active L2 slots,
/// and authenticated WS pulse (`ws_rtt_us > 0`, `ws_quota_rem >= 62`) or convergence timeout.
/// Does NOT require `factor_tfi != 0.0` across all 12 slots (quiet altcoins legitimately decay to `0.00`).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn passes_live_boot_shm_and_ws_convergence_gate(
    now_ns: TimestampNs,
    entry_warmup_until_ns: i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    gov: &OnlineAdaptiveGovernor,
    ws_rtt_us: u32,
    ws_quota_rem: u16,
) -> bool {
    if entry_warmup_until_ns > 0 && now_ns.0 < entry_warmup_until_ns {
        return false;
    }
    let min_ws_quota_pct = (crate::alpha_3factor::INV_PHI * 100.0).round() as u16;
    let ws_pulse_ok = ws_rtt_us > 0 && ws_quota_rem >= min_ws_quota_pct;
    let converge_timeout_ok = entry_warmup_until_ns > 0
        && now_ns.0 >= entry_warmup_until_ns.saturating_add(LIVE_BOOT_WS_CONVERGE_TIMEOUT_NS);
    if !ws_pulse_ok && !converge_timeout_ok {
        return false;
    }
    let btc = &cached_snaps[0];
    if btc.sequence_id == 0
        || btc.bids[0].price.0 <= 0
        || btc.asks[0].price.0 <= btc.bids[0].price.0
        || !gov.is_slot_ema_converged(0)
    {
        return false;
    }
    let mut live_l2_slots = 0_usize;
    for s in cached_snaps {
        if s.sequence_id > 0 && s.bids[0].price.0 > 0 && s.asks[0].price.0 > s.bids[0].price.0 {
            live_l2_slots += 1;
        }
    }
    live_l2_slots >= (MAX_ACTIVE_PAIRS / 2)
}

/// Irreversible boot convergence latch (V5.3.2): once `passes_live_boot_shm_and_ws_convergence_gate`
/// succeeds for the first time, `*boot_converged` latches to `true` for the lifetime of the process.
/// Post-boot, only a real SHM producer drop (`hb_age > 5s`) or an uninitialized slot
/// (`sequence_id == 0` or `bids[0].price <= 0`) can block entry with `WarmupOrLock`.
#[must_use]
pub fn evaluate_live_boot_convergence_with_latch(
    boot_converged: &mut bool,
    slot_idx: usize,
    now_ns: TimestampNs,
    entry_warmup_until_ns: i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    gov: &OnlineAdaptiveGovernor,
    ws_rtt_us: u32,
    ws_quota_rem: u16,
    shm_hb_age_ns: Option<u64>,
) -> bool {
    if slot_idx >= MAX_ACTIVE_PAIRS {
        return false;
    }
    if shm_hb_age_ns.is_some_and(|age_ns| age_ns > SHM_HEARTBEAT_DROP_NS) {
        return false;
    }
    if !*boot_converged {
        if passes_live_boot_shm_and_ws_convergence_gate(
            now_ns,
            entry_warmup_until_ns,
            cached_snaps,
            gov,
            ws_rtt_us,
            ws_quota_rem,
        ) {
            *boot_converged = true;
        } else {
            return false;
        }
    }
    let cand = &cached_snaps[slot_idx];
    cand.sequence_id > 0 && cand.bids[0].price.0 > 0
}

#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops)]
pub fn should_abort_queue_on_adverse_flow_c1(
    inst_signed_flow: f64,
    slow_signed_flow: f64,
    taker_prob_pm: i64,
    ir_x100: i64,
    spoof_density: f64,
) -> bool {
    let p_t = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let ir_pos = (ir_x100.clamp(0, 100) as f64) * 1e-2;
    let sp = if spoof_density.is_finite() {
        spoof_density.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let theta_imp = -(crate::alpha_3factor::INV_PHI
        * (1.0 + crate::alpha_3factor::INV_PHI_CUBE * ir_pos)
        / (1.0 + crate::alpha_3factor::PHI * p_t + crate::alpha_3factor::INV_PHI_SQ * sp));
    let theta_ema = -(crate::alpha_3factor::INV_PHI_CUBE
        * (1.0 + crate::alpha_3factor::INV_PHI_CUBE * ir_pos)
        / (1.0 + crate::alpha_3factor::PHI * p_t));
    inst_signed_flow <= theta_imp && slow_signed_flow <= theta_ema
}

pub fn step_12_slot_daemon_tick(
    now_ns: TimestampNs, gateway: Option<&BybitAsyncGateway>, instruments: InstrumentSpecVerification,
    lock_watch: Option<&AtomicLockWatch>, rt: &mut ExecRuntimeState, last_rest_dispatch_ns: &mut i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS], entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS], gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger, arena: &VortexShmArena, tg_tx: &SyncSender<String>, vault_tx: &SyncSender<VaultIoCommand>,
) -> (usize, usize) {
    let (mut open_count, mut pending_entry_count) = (0_usize, 0_usize);
    if let Some(lw) = lock_watch {
        if lw.is_halt_locked() {
            rt.halt_locked = true;
        }
    }
    let btc_snap = cached_snaps[0];
    let (tick_ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
    if btc_snap.sequence_id > 0 || btc_snap.bids[0].price.0 > 0 {
        let _ = rt.btc_lead_memory.observe_btc_snapshot_with_rtt(&btc_snap, tick_ws_rtt_us);
    }
    for k_idx in 1..MAX_ACTIVE_PAIRS {
        if cached_snaps[k_idx].sequence_id > 0 || cached_snaps[k_idx].bids[0].price.0 > 0 {
            let _ = rt.btc_lead_memory.observe_slot_snapshot(k_idx, &cached_snaps[k_idx], tick_ws_rtt_us);
        }
    }
    let mut mfe_learned_updated = false;
    for k_idx in 0..MAX_ACTIVE_PAIRS {
        if let Some(mk_pk) = rt.markout_rings[k_idx].take_uncensored_fill_peak_sbps() {
            let prev_mfe = engines[k_idx].learned_mfe_bps;
            engines[k_idx].observe_markout_ring_favorable_peak(mk_pk);
            if engines[k_idx].learned_mfe_bps > prev_mfe {
                mfe_learned_updated = true;
            }
        }
        while let Some((mk_side, pk_60s, mk_fav_60s)) = rt.markout_rings[k_idx].take_resolved_60s_fill_with_peak() {
            let _ = gov.observe_markout_60s_continuation(
                k_idx,
                mk_side,
                pk_60s,
                mk_fav_60s,
                cached_snaps[k_idx].realized_vol_5m,
            );
            rt.scar_snapshot_dirty = true;
        }
        while let Some((cf_side, cf_peak, cf_fav_60s)) =
            rt.markout_rings[k_idx].take_counterfactual_abort_learning()
        {
            let prev_mfe = engines[k_idx].learned_mfe_bps;
            engines[k_idx].observe_counterfactual_abort_mfe(cf_peak);
            let _ = gov.observe_counterfactual_abort_continuation_v558(
                k_idx,
                cf_side,
                cf_peak,
                cf_fav_60s,
                cached_snaps[k_idx].realized_vol_5m,
            );
            rt.scar_snapshot_dirty = true;
            if engines[k_idx].learned_mfe_bps > prev_mfe {
                mfe_learned_updated = true;
            }
        }
    }
    if mfe_learned_updated {
        let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
            ts_ns: now_ns.0,
            learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(engines),
        });
    }
    let fees = gov.active_fee_schedule();
    let live_eq = rt.wallet_1e8.max(ledger.current_equity_1e8());
    let p_day = gov.slot_temporal_cadence(0, &btc_snap).intraday_percentile;
    let prev_phase = rt.daily_gov.phase;
    let cur_phase = rt.daily_gov.evaluate_intraday_gate(
        now_ns, live_eq, ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
        gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(), p_day,
    );
    if cur_phase != prev_phase {
        let _ = vault_tx.try_send(VaultIoCommand::SaveDailyGovernor { ts_ns: now_ns.0, state: rt.daily_gov });
        if !rt.halt_locked
            && (rt.daily_gov.is_hibernating() || prev_phase.label().starts_with("SLEEP"))
        {
            let _ = tg_tx.try_send(format!(
                "\u{1f6e1}\u{fe0f} [VORTEX-OMEGA V5.5 DAILY SHIELD] Transicion UTC: {} -> {} | Hoy: {:+.4} USDT ({:+.2}%)",
                prev_phase.label(), cur_phase.label(),
                (rt.daily_gov.day_realized_pnl_1e8 as f64) / 100_000_000.0,
                (rt.daily_gov.day_pnl_ppm() as f64) / 10_000.0
            ));
        }
    }
    let mut epoch_loss_1e8 = maybe_latch_validation_epoch_5usdt_shield(
        now_ns,
        gateway.map(|_| rt.wallet_1e8),
        lock_watch,
        rt,
        cached_snaps,
        engines,
        ledger,
        tg_tx,
        vault_tx,
    );
    let mut psi_health = gov.refresh_psi_health(
        &rt.daily_gov,
        ledger.sinking_fund_reserve_bps_x100,
        epoch_loss_1e8,
    );
    let mut block_new_entries = rt.halt_locked || rt.drain_active || rt.daily_gov.is_hibernating();
    let mut halt_newly_latched_mid_tick = false;

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        if rt.slot_rpc_inflight[s_idx]
            && rt.slot_rpc_sent_ns[s_idx] > 0
            && now_ns.0.saturating_sub(rt.slot_rpc_sent_ns[s_idx]) >= 15_000_000_000
        {
            rt.slot_rpc_inflight[s_idx] = false;
            rt.abort_cmd_inflight[s_idx] = false;
            engines[s_idx].next_peg_allowed_ts_ns = 0;
        }
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
        engines[s_idx].hawkes_tracker.set_spoof_density(spoof_d);
        let evt_dil = rt.spoof_clocks[s_idx].event_clock_dilation();
        let epps_corr = if s_idx == 0 { 0.0 } else { rt.epps_trackers[s_idx].correlation(false) };
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, target_excursion, submitted_ts, .. } => {
                if rt.slot_rpc_inflight[s_idx] { pending_entry_count += 1; continue; }
                let live_unconverged = gateway.is_some() && {
                    let (ws_rtt_us, ws_quota_rem) = crate::execution_fsm::read_live_ws_pulse();
                    !evaluate_live_boot_convergence_with_latch(
                        &mut rt.boot_converged,
                        s_idx,
                        now_ns,
                        rt.entry_warmup_until_ns,
                        cached_snaps,
                        gov,
                        ws_rtt_us,
                        ws_quota_rem,
                        arena.heartbeat_age_ns(now_ns),
                    )
                };
                if block_new_entries || !instruments.is_slot_tradable(s_idx) || live_unconverged {
                    rt.queue_flow_ema[s_idx] = 0.0;
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    continue;
                }
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if now_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m) { continue; }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
                let btc_lead_opt = (s_idx > 0).then(|| {
                    rt.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                });
                let slot_psi = gov.slot_psi_health(s_idx, side);
                let ll_bonus = rt.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                    s_idx, side, &snap, &btc_snap, tick_ws_rtt_us, epps_beta, epps_corr, slot_psi, fees,
                );
                let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_v5510(
                    s_idx, side, rt.wallet_1e8, qty, mfe_s, Some(target_excursion), &snap, &btc_snap, spoof_d, epps_beta, epps_corr, btc_lead_opt, ll_bonus,
                ) else {
                    rt.queue_flow_ema[s_idx] = 0.0;
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let Ok((mut synced_entry_px, mut synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto_v542(
                    &snap, &btc_snap, epps_corr, btc_lead_opt, slot_psi,
                ) else {
                    rt.queue_flow_ema[s_idx] = 0.0;
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let cur_l1_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                let pre_ev_opt = gov
                    .evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                        s_idx, side, qty, synced_entry_px, mfe_s, target_excursion, &snap, &btc_snap, epps_beta, spoof_d,
                    )
                    .or_else(|| {
                        if synced_entry_px != cur_l1_px {
                            if let Ok((l1_px, l1_sl)) = engines[s_idx].sync_preflight_l1_entry_auto_v542(
                                &snap, &btc_snap, epps_corr, btc_lead_opt, 0.0,
                            ) {
                                synced_entry_px = l1_px;
                                synced_sl_px = l1_sl;
                                return gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                                    s_idx, side, qty, synced_entry_px, mfe_s, target_excursion, &snap, &btc_snap, epps_beta, spoof_d,
                                );
                            }
                        }
                        None
                    });
                let Some(pre_ev_sbps) = pre_ev_opt else {
                    rt.queue_flow_ema[s_idx] = 0.0;
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
                    continue;
                };
                let inside_spread = match side {
                    Side::Bid => snap.bids[0].price.0 > 0 && synced_entry_px.0 > snap.bids[0].price.0,
                    Side::Ask => snap.asks[0].price.0 > 0 && synced_entry_px.0 < snap.asks[0].price.0,
                };
                let (q_l1_init, q_l1_cur, opp_cum) = match side {
                    Side::Bid => (
                        if inside_spread { LotQty(0) } else { snap.bids[0].qty },
                        if inside_spread { LotQty(1) } else { snap.bids[0].qty },
                        snap.cum_sell_trade_lots,
                    ),
                    Side::Ask => (
                        if inside_spread { LotQty(0) } else { snap.asks[0].qty },
                        if inside_spread { LotQty(1) } else { snap.asks[0].qty },
                        snap.cum_buy_trade_lots,
                    ),
                };
                let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                    armed_ts_ns: now_ns.0, init_q_lots: q_l1_init.0,
                    ofi_in: snap.factor_ofi.get(), tfi_in: snap.factor_tfi.get(), geom_in: snap.factor_geom.get(),
                    snr_in: OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get(),
                    vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                    spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                    btc_comp_in: OrthogonalFactorState::snr_composite_score(&btc_snap),
                    gov_ewma_bps: (ewma_b as f64) / 100.0, win_streak: win_st, sl_streak: sl_st,
                    drift_bps: (dr_b as f64) / 100.0, ext_bps: (ex_b as f64) / 100.0,
                    kin_accel_net_bps: (((ex_b - dr_b) * side.sign_i64()) as f64) / 100.0,
                    kin_energy_in_bps: (e_kin as f64) / 100.0, req_kin_energy_in_bps: (e_min as f64) / 100.0,
                    ev_ex_ante_bps: (pre_ev_sbps as f64) / 100.0,
                };
                rt.init_q_ahead[s_idx] = q_l1_init;
                rt.cur_q_ahead[s_idx] = q_l1_cur;
                rt.last_l1_q_obs[s_idx] = q_l1_cur;
                rt.last_opp_trades[s_idx] = opp_cum;
                rt.geom_adv_since_ns[s_idx] = 0;
                rt.next_poll_fill_ts_ns[s_idx] = now_ns.0 + 1_200_000_000;
                rt.queue_flow_ema[s_idx] = 0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get();
                if let Some(gw) = gateway {
                    if gw.try_send(GatewayCommand::PlaceEntry { slot_idx: s_idx, side, entry_price: synced_entry_px, qty, sl_price: synced_sl_px }) {
                        rt.slot_rpc_inflight[s_idx] = true;
                        rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                    }
                } else {
                    let _ = engines[s_idx].on_entry_order_resting(now_ns);
                    arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                    engines[s_idx].sync_to_shm_telemetry(arena);
                }
            }
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, target_excursion, sl_trigger_price, resting_since_ts } => {
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                let (exact_at_l1, live_l1_q, cur_opp_cum) = match side {
                    Side::Bid => (snap.bids[0].price == entry_price, snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].price == entry_price, snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let at_l1 = is_at_or_ahead_of_l1(side, entry_price, &snap);
                let fav_bbo_miss = crate::kinematic_surface::is_bbo_favorably_past_resting_entry(side, entry_price, &snap);
                let prev_l1_obs = rt.last_l1_q_obs[s_idx];
                let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
                rt.last_opp_trades[s_idx] = cur_opp_cum;
                let (next_q, opp_hit, next_obs) = step_l1_queue_ahead_lots(side, entry_price, exact_at_l1, live_l1_q, prev_l1_obs, rt.cur_q_ahead[s_idx], tape_delta, snap.last_trade_price);
                rt.cur_q_ahead[s_idx] = next_q;
                rt.last_l1_q_obs[s_idx] = next_obs;
                if let Ok(tel) = arena.telemetry(s_idx) { tel.store_queue_ahead_lots(next_q); }
                let (ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
                let raw_q_flow = 0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get();
                if rt.queue_flow_ema[s_idx].abs() <= 1e-12 {
                    rt.queue_flow_ema[s_idx] = raw_q_flow;
                } else {
                    rt.queue_flow_ema[s_idx] = crate::execution_fsm::step_intrinsic_slow_flow_ema(
                        crate::types::BoundedScore::new_clamped(rt.queue_flow_ema[s_idx]),
                        crate::types::BoundedScore::new_clamped(raw_q_flow),
                        5_000_000,
                        engines[s_idx].intrinsic_tau_mkt_s,
                        evt_dil,
                        u64::from(ws_rtt_us),
                    )
                    .get();
                }
                let flow_imp = raw_q_flow * side.sign_f64();
                let signed_slow = crate::types::BoundedScore::new_clamped(rt.queue_flow_ema[s_idx] * side.sign_f64());
                let mut q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(side, rt.init_q_ahead[s_idx], next_q, opp_hit, &snap, &btc_snap, epps_beta);
                let wall_collapsed = exact_at_l1 && (crate::alpha_3factor::should_abort_l1_cancellation_collapse_v51(spec, side, prev_l1_obs, live_l1_q, opp_hit, entry_price, signed_slow, engines[s_idx].active_fees) || (opp_hit.0 == 0 && crate::alpha_3factor::should_abort_collapsed_resting_l1_wall_dynamic(spec, side, entry_price, &snap, rt.init_q_ahead[s_idx], engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees)));
                if wall_collapsed { q_shield.abort_now = true; }
                if rt.slot_rpc_inflight[s_idx] && !block_new_entries { continue; }
                let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                let total_chase_ns = now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns);
                let raw_base_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
                let hwk_prec_mult = rt.markout_rings[s_idx].hawkes_abort_precision_patience_mult_c1();
                let base_chase_ns = ((raw_base_chase_ns as f64) * hwk_prec_mult).round() as i64;
                let dt_s = ((elapsed_ns.max(5_000_000) as f64) * 1e-9).max(0.05);
                let hwk_lam = compute_resting_l1_hawkes_lambda_v531(
                    evt_dil,
                    opp_hit,
                    rt.init_q_ahead[s_idx],
                    dt_s,
                    q_shield.hawkes_lambda_ratio,
                    signed_slow,
                );
                let (max_chase_ns, chase_expired) = evaluate_resting_queue_chase_budget_v532(
                    gov,
                    s_idx,
                    side,
                    snap.realized_vol_5m,
                    evt_dil,
                    hwk_lam,
                    signed_slow,
                    rt.init_q_ahead[s_idx],
                    next_q,
                    base_chase_ns,
                    at_l1,
                    elapsed_ns,
                    total_chase_ns,
                );
                let q_timers = elastic_resting_queue_timers_ns(base_chase_ns);
                let fast_sweep = exact_at_l1 && evaluate_toxic_sweep_velocity_abort_v51(spec, side, entry_price, rt.init_q_ahead[s_idx], next_q, elapsed_ns, &snap, evt_dil, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees, ws_rtt_us);
                if fast_sweep { q_shield.abort_now = true; }
                let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
                let btc_lead_opt = (s_idx > 0).then(|| {
                    rt.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                });
                let v_prio = compute_moallemi_yuan_queue_priority_c1(rt.init_q_ahead[s_idx], next_q);
                let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                let headwind_erosion = is_resting_headwind_erosion_v554(
                    s_idx,
                    side,
                    elapsed_ns,
                    q_timers.headwind_abort_ns,
                    rt.init_q_ahead[s_idx],
                    next_q,
                    fast_sweep,
                    btc_aligned,
                    flow_imp,
                    &snap,
                    signed_slow,
                );
                let btc_w3 = crate::alpha_3factor::compute_btc_leader_aligned_score(
                    side,
                    &snap,
                    &btc_snap,
                    if s_idx > 0 { epps_corr } else { 0.0 },
                    btc_lead_opt,
                );
                let slot_psi = gov.slot_psi_health(s_idx, side);
                let opp_wave3_now = crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
                    side,
                    &snap,
                    btc_w3,
                    slot_psi,
                    gov.slot_eq_vol_5m_bps(s_idx),
                    engines[s_idx].learned_taker_prob_pm,
                    engines[s_idx].active_fees,
                );
                let macro_bad = opp_wave3_now
                    || (elapsed_ns >= q_timers.macro_abort_ns
                        && !passes_resting_elliott_macro_hysteresis_c1(
                            side,
                            &snap,
                            &btc_snap,
                            epps_corr,
                            btc_lead_opt,
                            slot_psi,
                            v_prio,
                        ));
                let ob_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                let geom_flip = evaluate_elastic_spoof_geom_abort_with_entry_v54(
                    side,
                    entry_price,
                    &snap,
                    entry_forensics[s_idx].geom_in,
                    elapsed_ns,
                    signed_slow,
                    spoof_d,
                    evt_dil,
                    engines[s_idx].learned_taker_prob_pm,
                    ws_rtt_us,
                    now_ns.0,
                    &mut rt.geom_adv_since_ns[s_idx],
                    rt.init_q_ahead[s_idx],
                    next_q,
                );
                let adverse_flow = should_abort_queue_on_adverse_flow_c1(
                    flow_imp,
                    signed_slow.get(),
                    engines[s_idx].learned_taker_prob_pm,
                    engines[s_idx].learned_ir_x100,
                    spoof_d,
                );
                let fav_miss_ticks = favorable_bbo_miss_ticks(side, entry_price, &snap);
                let fav_miss_1_2 = fav_miss_ticks >= 1 && fav_miss_ticks <= 2;
                let hard_abort = block_new_entries
                    || !instruments.is_slot_tradable(s_idx)
                    || (elapsed_ns >= 25_000_000 && exact_at_l1 && q_shield.abort_now)
                    || fast_sweep
                    || headwind_erosion
                    || macro_bad
                    || ob_bad
                    || geom_flip
                    || adverse_flow;
                if !hard_abort {
                    let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                    let prop_now = compute_aligned_entry_propulsion_c1(side, &snap, btc_lead_opt.unwrap_or(0.0));
                    let eta_fill = gov.slot_fill_rate_eta(s_idx);
                    let agile_repeg_mode = fav_miss_1_2
                        || (slot_psi > crate::alpha_3factor::INV_PHI_SQ
                            && fav_bbo_miss
                            && prop_now > crate::alpha_3factor::INV_PHI_CUBE);
                    let agile_cd_ns = compute_agile_repeg_cooldown_ns_v542(u64::from(ws_rtt_us), eta_fill);
                    let eff_repeg_wait_ns = if agile_repeg_mode { agile_cd_ns } else { q_timers.repeg_wait_ns };
                    let eff_repeg_cd_ns = if agile_repeg_mode { agile_cd_ns } else { q_timers.repeg_cooldown_ns };
                    let repeg_window_open = !at_l1
                        && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                        && !opp_wave3_now
                        && if fav_miss_1_2 {
                            (elapsed_ns >= eff_repeg_wait_ns || chase_expired)
                                && total_chase_ns < max_chase_ns.saturating_add(base_chase_ns / 2)
                        } else {
                            !chase_expired
                                && elapsed_ns >= eff_repeg_wait_ns
                                && total_chase_ns + eff_repeg_wait_ns < max_chase_ns
                        };
                    if repeg_window_open {
                        let ll_bonus = rt.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                            s_idx, side, &snap, &btc_snap, tick_ws_rtt_us, epps_beta, epps_corr, slot_psi, fees,
                        );
                        if let Some((chosen_repeg, repeg_ev_sbps)) = evaluate_resting_repeg_candidate_v554(
                            gov,
                            s_idx,
                            side,
                            rt.wallet_1e8,
                            qty,
                            entry_price,
                            engines[s_idx].original_armed_entry_price,
                            mfe_s,
                            target_excursion,
                            engines[s_idx].learned_taker_prob_pm,
                            engines[s_idx].active_fees,
                            &snap,
                            &btc_snap,
                            spoof_d,
                            epps_beta,
                            epps_corr,
                            btc_lead_opt,
                            ll_bonus,
                            slot_psi,
                            prop_now,
                            v_prio,
                        ) {
                            if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&snap, chosen_repeg) {
                                entry_forensics[s_idx].ofi_in = snap.factor_ofi.get();
                                entry_forensics[s_idx].tfi_in = snap.factor_tfi.get();
                                entry_forensics[s_idx].geom_in = snap.factor_geom.get();
                                entry_forensics[s_idx].snr_in = OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get();
                                entry_forensics[s_idx].vol5m_in = (snap.realized_vol_5m.raw() as f64) / 100.0;
                                entry_forensics[s_idx].ev_ex_ante_bps = (repeg_ev_sbps as f64) / 100.0;
                                engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + eff_repeg_cd_ns;
                                if let Some(gw) = gateway {
                                    if gw.try_send(GatewayCommand::RepegEntry { slot_idx: s_idx, side, new_price: chosen_repeg, qty, new_sl_price: repeg_sl }) {
                                        rt.geom_adv_since_ns[s_idx] = 0;
                                        rt.queue_flow_ema[s_idx] = raw_q_flow;
                                        rt.slot_rpc_inflight[s_idx] = true;
                                        rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                                        continue;
                                    }
                                } else {
                                    let inside_repeg = match side {
                                        Side::Bid => snap.bids[0].price.0 > 0 && chosen_repeg.0 > snap.bids[0].price.0,
                                        Side::Ask => snap.asks[0].price.0 > 0 && chosen_repeg.0 < snap.asks[0].price.0,
                                    };
                                    let q_rep = if inside_repeg { LotQty(1) } else { live_l1_q.max(LotQty(1)) };
                                    rt.init_q_ahead[s_idx] = if inside_repeg { LotQty(0) } else { q_rep };
                                    rt.cur_q_ahead[s_idx] = q_rep;
                                    rt.last_l1_q_obs[s_idx] = q_rep;
                                    rt.geom_adv_since_ns[s_idx] = 0;
                                    rt.queue_flow_ema[s_idx] = raw_q_flow;
                                    engines[s_idx].apply_resting_repeg(chosen_repeg, repeg_sl, now_ns);
                                    engines[s_idx].sync_to_shm_telemetry(arena);
                                    continue;
                                }
                            }
                        }
                    }
                }
                if hard_abort || chase_expired {
                    let abort_veto = if block_new_entries { LastVetoCode::WarmupOrLock } else if macro_bad { LastVetoCode::MacroElliottContradiction } else if ob_bad { LastVetoCode::OpposingOrderBlockWall } else if geom_flip { LastVetoCode::SpoofingVeto } else if headwind_erosion { LastVetoCode::BtcLeadLagHeadwind } else { LastVetoCode::HawkesStoikovQueueAbort };
                    let toxic_hawkes_sweep = (exact_at_l1 && q_shield.abort_now) || fast_sweep || geom_flip || adverse_flow || opp_wave3_now || headwind_erosion;
                    rt.last_abort_veto[s_idx] = abort_veto;
                    rt.abort_was_favorable_miss[s_idx] = fav_bbo_miss && !toxic_hawkes_sweep && !block_new_entries;
                    publish_slot_veto_code(arena, s_idx, abort_veto);
                    if let Some(gw) = gateway {
                        if !rt.abort_cmd_inflight[s_idx] && gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                            rt.abort_cmd_inflight[s_idx] = true;
                        }
                    } else {
                        pending_entry_count = pending_entry_count.saturating_sub(1);
                        rt.queue_flow_ema[s_idx] = 0.0;
                        gov.observe_entry_abort_with_context(
                            s_idx,
                            side,
                            entry_price,
                            now_ns,
                            fav_bbo_miss && !block_new_entries,
                            toxic_hawkes_sweep || block_new_entries,
                        );
                        rt.markout_rings[s_idx].arm_abort_probe_v49(side, entry_price, abort_veto, TimestampNs(entry_forensics[s_idx].armed_ts_ns.max(1)), now_ns, rt.init_q_ahead[s_idx], rt.cur_q_ahead[s_idx]);
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                    continue;
                }
                if let Some(gw) = gateway {
                    if now_ns.0 >= rt.next_poll_fill_ts_ns[s_idx] && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                        *last_rest_dispatch_ns = now_ns.0;
                        rt.next_poll_fill_ts_ns[s_idx] = now_ns.0 + 1_200_000_000;
                        if gw.try_send(GatewayCommand::PollEntryFill { slot_idx: s_idx, sl_hint: sl_trigger_price }) { rt.slot_rpc_inflight[s_idx] = true; rt.slot_rpc_sent_ns[s_idx] = now_ns.0; }
                    }
                } else {
                    let bbo_crossed = match side { Side::Bid => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 <= entry_price.0, Side::Ask => snap.bids[0].price.0 >= entry_price.0 };
                    if next_q.0 <= 0 || bbo_crossed {
                        pending_entry_count = pending_entry_count.saturating_sub(1);
                        open_count += 1;
                        rt.queue_flow_ema[s_idx] = 0.0;
                        gov.clear_abort_streak(s_idx, side);
                        gov.observe_passive_entry_fill(s_idx);
                        engines[s_idx].sync_governor_for_side(gov, side);
                        if let Some((eff_s, eff_px, eff_q)) = crate::exec_gateway_handlers::promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None) {
                            let mid_now = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
                            let vol_fill = if engines[s_idx].open_vol_5m_bps.0 > 0 {
                                engines[s_idx].open_vol_5m_bps
                            } else {
                                snap.realized_vol_5m
                            };
                            let c_rt_mkr = ScaledBps(engines[s_idx].active_maker_rt_fee_bps());
                            rt.markout_rings[s_idx].arm_fill_probe_with_vol(eff_s, eff_px, mid_now, now_ns, vol_fill, c_rt_mkr);
                            crate::telemetry_hud::record_and_notify_entry_fill(s_idx, spec, engines, entry_forensics, arena, tg_tx, vault_tx, now_ns, eff_s, eff_px, eff_q, "SHADOW MAKER FILL");
                        }
                    }
                }
            }
            OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price, open_lots, tranche1_harvested, resting_exit_price, sl_trigger_price, opened_ts, .. } => {
                open_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if let Some(gw) = gateway {
                    engines[s_idx].observe_open_flow_kinematics(&snap);
                    let cur_bbo = match side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
                    let ref_px = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { cur_bbo };
                    let prev_peak = peak_favorable_price;
                    match side {
                        Side::Bid => { if ref_px.0 > peak_favorable_price.0 { peak_favorable_price = ref_px; }
                        if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                        Side::Ask => { if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 { peak_favorable_price = ref_px; }
                        if ref_px.0 > worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                    }
                    engines[s_idx].update_peak_favorable_timestamp_if_improved(prev_peak, peak_favorable_price, side, snap.ts_local_ns);
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state { *pk = peak_favorable_price; *wr = worst_adverse_price; }
                    let (d_sign, denom) = (side.sign_i64(), avg_price_ticks.0.max(1));
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v49(&snap, &btc_snap, epps_beta, &rt.markout_rings[s_idx], evt_dil); rt.last_zeta_tox[s_idx] = zeta_tox;
                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let (unverified_sl_breach, eff_sl_now) = should_trigger_live_emergency_flatten(rt, s_idx, side, engines[s_idx].last_eff_sl_ticks, &snap);
                    if unverified_sl_breach && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
                        if gw.try_send(GatewayCommand::EmergencyFlattenSlot { slot_idx: s_idx, open_side: side, open_lots, backup_sl: eff_sl_now }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                            rt.sl_retry_after_ns[s_idx] = now_ns.0 + 250_000_000;
                        }
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    if let Some(new_sl) = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&snap, &btc_snap, epps_beta, zeta_tox) {
                        if new_sl != rt.verified_exchange_sl[s_idx] { rt.sl_needs_sync[s_idx] = true; }
                    }
                    if rt.sl_needs_sync[s_idx] && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
                        if let Some(safe_sl) = select_safe_live_sl_ticks_with_fees(side, avg_price_ticks, rt.verified_exchange_sl[s_idx], engines[s_idx].last_eff_sl_ticks, &snap, engines[s_idx].active_fees) {
                            let keep_tighter = match side { Side::Bid => safe_sl.0 > engines[s_idx].last_eff_sl_ticks.0, Side::Ask => engines[s_idx].last_eff_sl_ticks.0 == 0 || safe_sl.0 < engines[s_idx].last_eff_sl_ticks.0 };
                            if keep_tighter { engines[s_idx].last_eff_sl_ticks = safe_sl; }
                            if gw.try_send(GatewayCommand::EnforceStopLoss { slot_idx: s_idx, sl_price: safe_sl }) { rt.slot_rpc_inflight[s_idx] = true; rt.slot_rpc_sent_ns[s_idx] = now_ns.0; rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000; }
                        } else { rt.sl_needs_sync[s_idx] = false; }
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap) {
                        let (prev_def_locked, prev_ax_lots) = match engines[s_idx].state {
                            OrderLifecycleState::PositionOpenMakerExit {
                                defensive_floor_locked,
                                active_exit_lots,
                                ..
                            } => (defensive_floor_locked, active_exit_lots),
                            _ => (false, LotQty(0)),
                        };
                        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                        if quote.defensive_lock_active {
                            engines[s_idx].latch_defensive_floor_if_needed(true);
                        } else if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = engines[s_idx].state {
                            *defensive_floor_locked = false;
                        }
                        if let OrderLifecycleState::PositionOpenMakerExit {
                            active_exit_role: ref mut ax_r,
                            active_exit_price: ref mut ax_p,
                            active_exit_lots: ref mut ax_l,
                            ..
                        } = engines[s_idx].state
                        {
                            *ax_r = quote.role;
                            *ax_p = quote.target_price;
                            *ax_l = quote.order_lots;
                        }
                        engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                        let (ws_rtt_us, ws_quota_rem) = crate::execution_fsm::read_live_ws_pulse();
                        let (base_preempt, next_sync_delay_ns) = compute_exit_sync_schedule_elastic(&quote, resting_exit_price, avg_price_ticks, ws_rtt_us, ws_quota_rem);
                        let unlocked_outward = prev_def_locked
                            && !quote.defensive_lock_active
                            && resting_exit_price.0 > 0
                            && resting_exit_price != quote.target_price;
                        let lots_split_changed = resting_exit_price.0 > 0
                            && prev_ax_lots.0 > 0
                            && prev_ax_lots != quote.order_lots;
                        let preempt_peg = base_preempt || unlocked_outward || lots_split_changed;
                        if unlocked_outward || lots_split_changed {
                            engines[s_idx].next_peg_allowed_ts_ns = 0;
                        }
                        if !rt.slot_rpc_inflight[s_idx] && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns) && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                            *last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
                            let live_mid_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                                PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
                            } else if snap.mark_price.0 > 0 {
                                snap.mark_price
                            } else {
                                avg_price_ticks
                            };
                            if gw.try_send(GatewayCommand::SyncOpenSlot { slot_idx: s_idx, side, live_mid_px, target_px: quote.target_price, target_lots: quote.order_lots, defensive_lock: quote.urgent_close_repeg, cur_gross_bps: quote.cur_gross_bps, vac_level: quote.vac_level, opened_ts_ns: opened_ts.0, tranche1_harvested, fees: engines[s_idx].active_fees }) {
                                rt.slot_rpc_inflight[s_idx] = true;
                                rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                            }
                        }
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }
                if let Some(summary) = engines[s_idx].step_open_position_v49(&snap, &btc_snap, gov, ledger, epps_beta, &rt.markout_rings[s_idx], evt_dil) {
                    open_count = open_count.saturating_sub(1);
                    let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                    let w_close = snap.wave_observables();
                    let signed_prop = 0.50 * snap.factor_ofi.get() + 0.38 * snap.factor_tfi.get() + 0.12 * snap.factor_geom.get();
                    gov.set_slot_momentum_chain_context(s_idx, signed_prop, w_close.climax_active, tick_ws_rtt_us);
                    gov.observe_closed_trade_with_regime_v51(s_idx, summary.side, summary.entry_price, summary.net_scaled_bps, ScaledBps(engines[s_idx].last_closed_mfe_bps), non_punitive, w_close, evt_dil, now_ns);
                    engines[s_idx].apply_momentum_chain_post_close_cooldown(
                        now_ns,
                        non_punitive,
                        summary.net_scaled_bps,
                        w_close.climax_active || matches!(w_close.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion),
                        gov.psi_health(),
                        signed_prop * summary.side.sign_f64(),
                        tick_ws_rtt_us,
                    );
                    let _ = rt.daily_gov.observe_realized_cashflow(
                        now_ns, summary.net_pnl_1e8, true, summary.net_pnl_1e8 > 0, summary.net_pnl_1e8 < 0,
                        rt.wallet_1e8.max(ledger.current_equity_1e8()), ledger.sinking_fund_reserve_bps_x100,
                        gov.active_maker_rt_fee_bps(), gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
                    );
                    let _ = vault_tx.try_send(VaultIoCommand::SaveDailyGovernor { ts_ns: now_ns.0, state: rt.daily_gov });
                    arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
                    let mk_curve = rt.markout_rings[s_idx].last_fill_curve();
                    let lvr_5s = rt.markout_rings[s_idx].ewma_lvr_5s_bps();
                    dispatch_closed_trade_logs_with_locks(s_idx, &summary, &engines[s_idx], &entry_forensics[s_idx], &snap, ledger, gov, &mk_curve, lvr_5s, vault_tx, lock_watch, &mut rt.halt_locked, now_ns);
                    let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity { ts_ns: now_ns.0, learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(engines) });
                    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
                    let _ = tg_tx.try_send(format_telegram_close_message(spec, &summary, engines[s_idx].last_exit_sub_reason, ledger.closed_trades_count, ledger));
                    let prev_halt = rt.halt_locked;
                    epoch_loss_1e8 = maybe_latch_validation_epoch_5usdt_shield(
                        now_ns,
                        gateway.map(|_| rt.wallet_1e8),
                        lock_watch,
                        rt,
                        cached_snaps,
                        engines,
                        ledger,
                        tg_tx,
                        vault_tx,
                    );
                    psi_health = gov.refresh_psi_health(
                        &rt.daily_gov,
                        ledger.sinking_fund_reserve_bps_x100,
                        epoch_loss_1e8,
                    );
                    if rt.halt_locked && (!prev_halt || !block_new_entries) {
                        block_new_entries = true;
                        halt_newly_latched_mid_tick = true;
                    }
                }
                engines[s_idx].sync_to_shm_telemetry(arena);
            }
        }
    }

    if halt_newly_latched_mid_tick {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            match engines[s_idx].state {
                OrderLifecycleState::PlacingEntryMaker { .. } => {
                    pending_entry_count = pending_entry_count.saturating_sub(1);
                    rt.queue_flow_ema[s_idx] = 0.0;
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                }
                OrderLifecycleState::RestingEntryQueue { side, entry_price, sl_trigger_price, .. } => {
                    rt.last_abort_veto[s_idx] = LastVetoCode::WarmupOrLock;
                    rt.abort_was_favorable_miss[s_idx] = false;
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    if let Some(gw) = gateway {
                        if !rt.abort_cmd_inflight[s_idx] && gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.slot_rpc_sent_ns[s_idx] = now_ns.0;
                            rt.abort_cmd_inflight[s_idx] = true;
                        }
                    } else {
                        pending_entry_count = pending_entry_count.saturating_sub(1);
                        rt.queue_flow_ema[s_idx] = 0.0;
                        gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                        rt.markout_rings[s_idx].arm_abort_probe_v49(
                            side,
                            entry_price,
                            LastVetoCode::WarmupOrLock,
                            TimestampNs(entry_forensics[s_idx].armed_ts_ns.max(1)),
                            now_ns,
                            rt.init_q_ahead[s_idx],
                            rt.cur_q_ahead[s_idx],
                        );
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                }
                OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => {}
            }
        }
    }

    let mut beta_guard = build_monetary_beta_guard_with_epps(rt.wallet_1e8, engines, cached_snaps, &rt.epps_trackers, fees);
    if !block_new_entries && (open_count + pending_entry_count) < 6 {
        let btc_vol = ScaledBps(btc_snap.realized_vol_5m.0.max(2_500));
        let raw_daily_damper_pm = rt.daily_gov.kelly_boundary_damper_permille(
            ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
            gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
        );
        let epoch_5u_damper = crate::governor_plasticity::compute_epoch_5u_loss_lot_damper_c1(epoch_loss_1e8);
        let daily_damper_pm = ((raw_daily_damper_pm as f64) * epoch_5u_damper).round() as i64;
        let mut ranked_candidates = [(0_usize, 0_i64); MAX_ACTIVE_PAIRS];
        let mut staged_payloads: [Option<StagedOffensiveCandidate>; MAX_ACTIVE_PAIRS] = [None; MAX_ACTIVE_PAIRS];
        let mut cand_count = 0_usize;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) || rt.slot_rpc_inflight[s_idx] { continue; }
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 { continue; }
            if gateway.is_some() {
                let (ws_rtt_us, ws_quota_rem) = crate::execution_fsm::read_live_ws_pulse();
                if !evaluate_live_boot_convergence_with_latch(
                    &mut rt.boot_converged,
                    s_idx,
                    now_ns,
                    rt.entry_warmup_until_ns,
                    cached_snaps,
                    gov,
                    ws_rtt_us,
                    ws_quota_rem,
                    arena.heartbeat_age_ns(now_ns),
                ) {
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    continue;
                }
            }
            if !instruments.is_slot_tradable(s_idx) { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); continue; }
            if now_ns.0 < engines[s_idx].cooldown_until_ns.0 {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::ScarCooldownOrContagion);
                continue;
            }
            let spec = APPROVED_SPECS[s_idx];
            let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
            engines[s_idx].hawkes_tracker.set_spoof_density(spoof_d);
            let epps_corr = if s_idx == 0 { 0.0 } else { rt.epps_trackers[s_idx].correlation(false) };
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
            let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
            let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
            let probe_btc_lead = (s_idx > 0).then(|| {
                rt.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, probe_side)
            });
            let probe_slot_psi = gov.slot_psi_health(s_idx, probe_side);
            let probe_ll_bonus = rt.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                s_idx, probe_side, &snap, &btc_snap, tick_ws_rtt_us, epps_beta, epps_corr, probe_slot_psi, fees,
            );
            let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
            let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
            let probe_ir_x100 = gov.slot_information_ratio_x100(s_idx, probe_side);
            let probe_tau_mkt_s =
                crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(snap.realized_vol_5m);
            let probe_kappa_evt = rt.spoof_clocks[s_idx].event_clock_dilation();
            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
            let verdict = OrthogonalFactorState::evaluate_entry_signal_v554(
                spec,
                &snap,
                &btc_snap,
                mfe_s,
                ScaledBps(probe_edge),
                taker_pm,
                probe_ir_x100,
                probe_tau_mkt_s,
                fees,
                spoof_d,
                epps_beta,
                epps_corr,
                probe_btc_lead,
                probe_slot_psi,
                probe_ll_bonus,
                probe_kappa_evt,
            );
            match verdict {
                SignalVerdict::SpoofingVeto => publish_slot_veto_code(arena, s_idx, LastVetoCode::SpoofingVeto),
                SignalVerdict::NeutralNoAction => {
                    if rt.last_abort_veto[s_idx] == LastVetoCode::None {
                        let probe_btc_w3 = crate::alpha_3factor::compute_btc_leader_aligned_score(
                            probe_side,
                            &snap,
                            &btc_snap,
                            if s_idx > 0 { epps_corr } else { 0.0 },
                            probe_btc_lead,
                        );
                        let veto_c = if crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
                            probe_side,
                            &snap,
                            probe_btc_w3,
                            psi_health,
                            gov.slot_eq_vol_5m_bps(s_idx),
                            taker_pm,
                            fees,
                        ) || !passes_elliott_macro_coherence_guard_with_health(probe_side, &snap, &btc_snap, epps_corr, probe_btc_lead, psi_health)
                        {
                            LastVetoCode::MacroElliottContradiction
                        } else {
                            LastVetoCode::Friction3xOrEligibility
                        };
                        publish_slot_veto_code(arena, s_idx, veto_c);
                    }
                }
                SignalVerdict::AuthorizeMakerEntry { side, target_excursion_scaled_bps } => {
                    let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                    let slot_psi = gov.slot_psi_health(s_idx, side);
                    let btc_lead_opt = (s_idx > 0).then(|| {
                        rt.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                    });
                    let ll_bonus = rt.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                        s_idx, side, &snap, &btc_snap, tick_ws_rtt_us, epps_beta, epps_corr, slot_psi, fees,
                    );
                    let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    let is_foll = s_idx > 0 && snap.symbol_id != btc_snap.symbol_id;
                    let btc_ice_al = if is_foll {
                        (if epps_beta < -1e-6 { -1.0 } else { 1.0 })
                            * OrthogonalFactorState::snr_composite_score(&btc_snap)
                            * side.sign_f64()
                    } else {
                        0.0
                    };
                    let s_ice = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
                        snap.factor_ofi.get() * side.sign_f64(),
                        snap.factor_tfi.get() * side.sign_f64(),
                        btc_ice_al,
                    );
                    let eff_q_mfe = ScaledBps(
                        ((mfe_s.raw() as f64) * (1.0 - crate::alpha_3factor::INV_PHI * s_ice)).round() as i64,
                    );
                    if s_ice >= crate::alpha_3factor::INV_PHI
                        || !spec.passes_quantum_ev_growth_gate(rt.wallet_1e8, cand_px, snap.realized_vol_5m, eff_q_mfe, s_taker_pm, fees)
                    {
                        let veto_ice = if s_ice >= crate::alpha_3factor::INV_PHI {
                            LastVetoCode::SpoofingVeto
                        } else {
                            LastVetoCode::QueueDepthOrOversizedLot
                        };
                        publish_slot_veto_code(arena, s_idx, veto_ice);
                        continue;
                    }
                    if !passes_elliott_macro_coherence_guard_with_health(side, &snap, &btc_snap, epps_corr, btc_lead_opt, slot_psi) { publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction); continue; }
                    if !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, s_taker_pm, fees) { publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall); continue; }
                    let lots = gov.compute_slot_entry_lots_v48(s_idx, side, rt.wallet_1e8, &snap, daily_damper_pm);
                    let risk_ppm = slot_position_risk_ppm_with_fees(spec, rt.wallet_1e8, cand_px, lots, snap.realized_vol_5m, fees);
                    let beta_x1000 = rt.epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                    let queue_init_cur: [(LotQty, LotQty); MAX_ACTIVE_PAIRS] = core::array::from_fn(|k| (rt.init_q_ahead[k], rt.cur_q_ahead[k]));
                    if !can_stage_same_direction_entry_v542(s_idx, side, now_ns, engines, cached_snaps, &rt.epps_trackers, slot_psi, s_taker_pm)
                        && !allows_buried_queue_third_slot_unlock_v555(s_idx, side, now_ns, engines, &queue_init_cur, &snap, lots, slot_psi, tick_ws_rtt_us, &beta_guard, risk_ppm, beta_x1000)
                    { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    if !beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000) || !passes_elastic_daily_tail_risk_budget(s_idx, side, risk_ppm, rt.wallet_1e8, &rt.daily_gov, ledger.sinking_fund_reserve_bps_x100, engines, cached_snaps, gov) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    if let Err(v49_veto) = gov.authorizes_adaptive_entry_v5510(s_idx, side, rt.wallet_1e8, daily_damper_pm, mfe_s, Some(target_excursion_scaled_bps), &snap, &btc_snap, spoof_d, epps_beta, epps_corr, btc_lead_opt, ll_bonus) {
                        let veto_c = if gov.audit_historical_entry_veto(s_idx, side, cand_px.0, snap.ts_local_ns.0).is_some() { LastVetoCode::ScarCooldownOrContagion } else { v49_veto };
                        publish_slot_veto_code(arena, s_idx, veto_c);
                        continue;
                    }
                    let Some(ev_net_sbps) = gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                        s_idx, side, lots, cand_px, mfe_s, target_excursion_scaled_bps, &snap, &btc_snap, epps_beta, spoof_d,
                    ) else {
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
                        continue;
                    };
                    let vac_l2_l3 = compute_vacuum_l2_l3_c1(side, &snap);
                    let eta_fill = gov.slot_fill_rate_eta(s_idx);
                    let u_score = compute_offensive_entry_utility_score_x1000_v55(
                        ev_net_sbps,
                        lots,
                        slot_psi,
                        vac_l2_l3,
                        eta_fill,
                    );
                    ranked_candidates[cand_count] = (s_idx, u_score);
                    staged_payloads[s_idx] = Some(StagedOffensiveCandidate {
                        side,
                        verdict,
                        lots,
                        risk_ppm,
                        beta_x1000,
                        epps_corr,
                        btc_lead_opt,
                    });
                    cand_count += 1;
                }
            }
        }
        sort_offensive_candidates_zero_alloc(&mut ranked_candidates, cand_count);
        for &(s_idx, _) in &ranked_candidates[..cand_count] {
            if (open_count + pending_entry_count) >= 6 { break; }
            let Some(stg) = staged_payloads[s_idx] else { continue };
            let snap = cached_snaps[s_idx];
            engines[s_idx].sync_governor_for_side(gov, stg.side);
            if engines[s_idx].try_arm_maker_entry_v542(&snap, &btc_snap, stg.verdict, stg.lots, stg.epps_corr, stg.btc_lead_opt, gov.slot_psi_health(s_idx, stg.side)).is_ok() && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. }) {
                rt.queue_flow_ema[s_idx] = 0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get();
                rt.last_abort_veto[s_idx] = LastVetoCode::None;
                publish_slot_veto_code(arena, s_idx, LastVetoCode::None);
                beta_guard.register_slot_open(s_idx, stg.side, stg.risk_ppm, stg.beta_x1000);
                engines[s_idx].sync_to_shm_telemetry(arena);
                pending_entry_count += 1;
                break;
            }
        }
    } else if block_new_entries {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle) { publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock); }
        }
    }
    rt.step_v49_deep_telemetry_200hz(now_ns, cached_snaps, engines, gov, ledger, Some(vault_tx));
    (open_count, pending_entry_count)
}
pub fn run_autonomous_12_slot_daemon(arena: &VortexShmArena, live_capital: bool) -> ExitCode {
    let tg_tx = spawn_bounded_telegram_notifier();
    let lock_watch = Arc::new(AtomicLockWatch::default());
    let vault_tx = spawn_vault_io_worker(Arc::clone(&lock_watch));
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut rt = ExecRuntimeState::new();
    let mut cached_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut entry_forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut last_seq = [0_u64; MAX_ACTIVE_PAIRS];
    let mut last_funding_cut = crate::telemetry_hud::rehydrate_last_funding_cuts_from_vault();

    let boot_ns = now_epoch_ns();
    rt.last_plasticity_audit_ns = boot_ns.0;
    let startup_hs = run_daemon_startup_handshake(live_capital, boot_ns);
    let mut instruments = startup_hs.instruments;
    gov.set_active_fee_schedule(startup_hs.fee_schedule);
    ledger.set_live_vip_fee_rates(startup_hs.fee_schedule.maker_fee_bps, startup_hs.fee_schedule.taker_fee_bps);
    for eng in &mut engines {
        eng.sync_governor_dynamics(&gov);
    }

    arena.exec_armed_count.store(0, Ordering::Release); arena.exec_abort_count.store(0, Ordering::Release); arena.exec_fill_count.store(0, Ordering::Release);
    let rehyd_halt = rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);
    if rehyd_halt
        || lock_watch.is_halt_locked()
        || Path::new(crate::telemetry_hud::HALT_LOCK_PATH).exists()
    {
        rt.halt_locked = true;
        rt.total_loss_shield_latched = true;
        lock_watch.latch_halt_in_ram();
    }
    SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut engines, Path::new(MFE_PLASTICITY_VAULT_PATH));
    rehydrate_cold_mfe_seeds_from_scar_vault(&mut engines, Path::new(SCAR_MEMORY_VAULT_PATH));
    let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
        ts_ns: boot_ns.0,
        learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
    });
    let epps_slots = crate::vault_io::rehydrate_epps_trackers_from_audit_path(
        &mut rt.epps_trackers,
        Path::new(crate::vault_io::PLASTICITY_AUDIT_VAULT_PATH),
        boot_ns,
    );
    let _ = crate::markout_tracker::SlotMarkoutRing::rehydrate_aborts_from_jsonl_path(
        &mut rt.markout_rings,
        Path::new(crate::vault_io::QUEUE_ABORTS_VAULT_PATH),
    );
    let prewarmed_slots = gov.load_ema_prewarm_file(boot_ns);
    if live_capital {
        let warmup_ns = if prewarmed_slots >= MAX_ACTIVE_PAIRS { 15_000_000_000_i64 } else { 75_000_000_000_i64 };
        let warmup_until_ns = boot_ns.0.saturating_add(warmup_ns);
        gov.set_entry_warmup_until_ns(warmup_until_ns);
        rt.entry_warmup_until_ns = warmup_until_ns;
        println!(
            "{ANSI_GREEN}\u{1f7e2} [EMA+EPPS PREWARM V5.5] Hydrated {prewarmed_slots}/12 EMAs, {epps_slots}/12 Epps | Live L2+WS converge gate armed ({}s floor) | Fees: {:+.2}/{:.2}bp{ANSI_RESET}",
            warmup_ns / 1_000_000_000,
            (startup_hs.fee_schedule.maker_fee_bps.raw() as f64) / 100.0,
            (startup_hs.fee_schedule.taker_fee_bps.raw() as f64) / 100.0
        );
    }
    bootstrap_live_adopt_if_enabled(live_capital, arena, &gov, &mut rt, &mut entry_forensics, &mut engines);
    if live_capital && rt.wallet_1e8 > 0 {
        ledger.initial_equity_1e8 = rt.wallet_1e8.saturating_sub(ledger.cumulative_net_pnl_1e8);
    }
    let boot_eq = rt.wallet_1e8.max(ledger.current_equity_1e8());
    rt.daily_gov = rehydrate_daily_governor_from_vault(now_epoch_ns(), boot_eq, &ledger, &gov);
    let _ = vault_tx.try_send(VaultIoCommand::SaveDailyGovernor { ts_ns: boot_ns.0, state: rt.daily_gov });

    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);
    let mut last_wallet_sync_ns = boot_ns.0;
    let mut last_handshake_refresh_ns = boot_ns.0;
    let mut last_rest_dispatch_ns = 0_i64;
    println!(
        "{ANSI_GREEN}\u{1f7e2} [VORTEX-OMEGA V5.5 200.0 Hz ARMED] Multi-Lot Router + Lead-Lag + 3-Tranche Cascade + Free-Ride + Anti-Hostage.{ANSI_RESET}"
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();
        rt.halt_locked = rt.halt_locked || lock_watch.is_halt_locked();
        rt.drain_active = lock_watch.is_drain_active();

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot_ref) = arena.slot(s_idx) {
                if let Ok(snap) = slot_ref.read_snapshot(4) {
                    cached_snaps[s_idx] = snap;
                    if snap.sequence_id != last_seq[s_idx] {
                        last_seq[s_idx] = snap.sequence_id;
                        let _ = rt.spoof_clocks[s_idx]
                            .step_event_clock_and_spoof_with_snap(&snap, gov.active_fee_schedule());
                        let mid_now = PriceTicks(i64::midpoint(
                            snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1),
                        ));
                        let mid_btc = PriceTicks(i64::midpoint(
                            cached_snaps[0].bids[0].price.0.max(1), cached_snaps[0].asks[0].price.0.max(1),
                        ));
                        rt.epps_trackers[s_idx].observe_5s_pair(snap.ts_local_ns, s_idx == 0, mid_now, mid_btc);
                        rt.markout_rings[s_idx].step_clock(
                            mid_now, snap.ts_local_ns, gov.active_fee_schedule().maker_fee_bps.raw(),
                        );
                        if let Some(mk_pk) = rt.markout_rings[s_idx].take_uncensored_fill_peak_sbps() {
                            let prev_mfe = engines[s_idx].learned_mfe_bps;
                            engines[s_idx].observe_markout_ring_favorable_peak(mk_pk);
                            if engines[s_idx].learned_mfe_bps > prev_mfe {
                                let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                                    ts_ns: now_ns.0,
                                    learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
                                });
                            }
                        }
                        while let Some((mk_side, pk_60s, mk_fav_60s)) = rt.markout_rings[s_idx].take_resolved_60s_fill_with_peak() {
                            let _ = gov.observe_markout_60s_continuation(
                                s_idx,
                                mk_side,
                                pk_60s,
                                mk_fav_60s,
                                snap.realized_vol_5m,
                            );
                            rt.scar_snapshot_dirty = true;
                        }
                        while let Some((cf_side, cf_peak, cf_fav_60s)) =
                            rt.markout_rings[s_idx].take_counterfactual_abort_learning()
                        {
                            let prev_mfe = engines[s_idx].learned_mfe_bps;
                            engines[s_idx].observe_counterfactual_abort_mfe(cf_peak);
                            let _ = gov.observe_counterfactual_abort_continuation_v558(
                                s_idx,
                                cf_side,
                                cf_peak,
                                cf_fav_60s,
                                snap.realized_vol_5m,
                            );
                            rt.scar_snapshot_dirty = true;
                            if engines[s_idx].learned_mfe_bps > prev_mfe {
                                let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                                    ts_ns: now_ns.0,
                                    learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
                                });
                            }
                        }
                        gov.observe_market_price(s_idx, &snap);
                    }
                }
            }
        }

        if let Some(ref gw) = gateway {
            while let Some(evt) = gw.try_recv() {
                if let GatewayEvent::HandshakeRefreshed { handshake: hs } = &evt {
                    if hs.synced_from_exchange { instruments = hs.instruments; }
                }
                let (prev_halt, prev_closed) = (rt.halt_locked, ledger.closed_trades_count);
                handle_gateway_event(
                    evt, now_ns, Some(gw), &mut rt, &cached_snaps, &entry_forensics,
                    &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
                );
                if rt.halt_locked && !prev_halt { lock_watch.latch_halt_in_ram(); }
                if ledger.closed_trades_count > prev_closed {
                    let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                        ts_ns: now_ns.0, learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
                    });
                }
            }
        }

        let eq_fnd = rt.wallet_1e8.max(ledger.current_equity_1e8());
        let _ = settle_live_8h_funding_cuts_v48(
            &engines, &cached_snaps, &mut last_funding_cut, &mut ledger,
            Some(&vault_tx), Some((&mut rt.daily_gov, now_ns, eq_fnd, &gov)),
        );

        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            gov.dispatch_ema_prewarm_async(now_ns, &vault_tx);
            let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                ts_ns: now_ns.0, learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
            });
            if rt.scar_snapshot_dirty && now_ns.0.saturating_sub(rt.last_scar_persist_ns) >= 60_000_000_000 {
                rt.scar_snapshot_dirty = false;
                rt.last_scar_persist_ns = now_ns.0;
                let _ = vault_tx.try_send(VaultIoCommand::SaveGovernorScar {
                    ts_ns: now_ns.0,
                    gov: Box::new(gov.clone()),
                });
            }
            if let Some(ref gw) = gateway { let _ = gw.try_send(GatewayCommand::SyncWallet); }
        }
        if now_ns.0.saturating_sub(last_handshake_refresh_ns) >= HOURLY_HANDSHAKE_REFRESH_NS {
            last_handshake_refresh_ns = now_ns.0;
            if let Some(ref gw) = gateway { let _ = gw.try_send(GatewayCommand::RefreshHandshake { now_ns }); }
        }

        let (open_count, pending_entry_count) = step_12_slot_daemon_tick(
            now_ns, gateway.as_ref(), instruments, Some(&lock_watch), &mut rt,
            &mut last_rest_dispatch_ns, &cached_snaps, &mut entry_forensics,
            &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );
        let any_inflight = rt.slot_rpc_inflight.iter().any(|&b| b);
        let (flush_drain_once, drain_standby) = step_drain_standby_latch(rt.drain_active, open_count, pending_entry_count, any_inflight, &mut rt.drain_flushed_once);
        if flush_drain_once {
            let _ = tg_tx.try_send("\u{1f7e2} [VORTEX-OMEGA V5.5 DRAIN STANDBY] 0 open positions verified (Wallet == Equity). Standing by.".to_string());
            let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
            if vault_tx.send(VaultIoCommand::FlushBarrier(ack_tx)).is_ok() {
                let _ = ack_rx.recv_timeout(Duration::from_secs(2));
            }
            println!("{ANSI_GREEN}\u{1f7e2} [DRAIN_STANDBY_ACTIVE] 0 open positions verified. Vault flushed once (0 systemd restarts).{ANSI_RESET}");
        }
        if drain_standby || is_safe_halt_standby(rt.halt_locked || rt.daily_gov.is_hibernating(), open_count, pending_entry_count, any_inflight) {
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub fn run_exec_cli_entrypoint() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let shm_path = MappedShmRegion::default_shm_path();
    if args.iter().any(|a| a == "--live") {
        match MappedShmRegion::open_existing_read_only(&shm_path) {
            Ok(reg) => render_live_hud(reg.arena()),
            Err(_) => println!("{ANSI_GRAY}\u{26aa} [VORTEX-OMEGA V5.5 LIVE HUD] SHM inactive ({}) \u{2014} daemons stopped.{ANSI_RESET}", shm_path.display()),
        }
        return ExitCode::SUCCESS;
    }
    if args.iter().any(|a| a == "--trades") {
        render_trades_and_executive_dashboard();
        return ExitCode::SUCCESS;
    }
    let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
    let sep = "=".repeat(80);
    println!("{sep}\n[VORTEX-EXEC OMEGA V5.5] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER\n{sep}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> OOS Closed Trades Verified : {oos_trades} (Gate >= 500){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Maker Exit Ratio           : {maker_ratio:.2}% (Gate >= 80.00%){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Net Profit Factor (PostFee): {pf:.2} (Gate >= 1.35){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Cumulative Net PnL (OOS)   : + {cum_usdt:.4} USDT | PARITY_SYNC Drift: {drift} units{ANSI_RESET}\n{sep}");
    if oos_trades < 500 || maker_ratio < 80.0 || pf < 1.35 || drift != 0 { return ExitCode::FAILURE; }
    if args.iter().any(|a| a == "--daemon") {
        let Ok(reg) = MappedShmRegion::open_or_create(&shm_path, false) else { return ExitCode::FAILURE; };
        return run_autonomous_12_slot_daemon(reg.arena(), args.iter().any(|a| a == "--live-capital"));
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_fsm::{ClosedTradeKind, ClosedTradeSummary, ExitOrderRole};
    use crate::governor_plasticity::DailyShieldPhase;
    use crate::lob_shm::BookLevel;
    use crate::markout_tracker::MarkoutCurveBps;
    use crate::types::BoundedScore;

    #[test]
    fn test_v48_daemon_p0b_custody_8h_funding_cellular_sl_and_utc_wakeup() {
        assert!(!is_safe_halt_standby(true, 1, 0, false) && is_safe_halt_standby(true, 0, 0, false));
        let mut s_ref = SymbolLobSnapshot::new_empty();
        s_ref.symbol_id = APPROVED_SPECS[3].symbol_id;
        s_ref.realized_vol_5m = ScaledBps(5_000);
        s_ref.mark_price = PriceTicks(8_237);
        s_ref.last_trade_price = PriceTicks(8_237);
        s_ref.bids[0] = BookLevel { price: PriceTicks(8_236), qty: LotQty(350) };
        s_ref.asks[0] = BookLevel { price: PriceTicks(8_237), qty: LotQty(200) };
        assert_eq!(select_safe_live_sl_ticks(Side::Bid, PriceTicks(8_200), PriceTicks(8_120), PriceTicks(8_226), &s_ref), Some(PriceTicks(8_226)));
        let q_pre = CanonicalExitQuote {
            role: ExitOrderRole::StandardMakerExit100, target_price: PriceTicks(762_500), order_lots: LotQty(1),
            sub_reason: ExitSubReason::ShadowRunnerCrestHarvest, vac_level: 1, defensive_lock_active: false,
            urgent_close_repeg: true, cur_gross_bps: 4_611, wald_pct: 0,
        };
        assert_eq!(compute_exit_sync_schedule_with_fees(&q_pre, PriceTicks(766_400), PriceTicks(759_000), FeeSchedule::VIP0_DEFAULT), (true, 250_000_000));

        let shm_path = MappedShmRegion::ephemeral_self_test_path("v48_daemon_gate");
        if let Ok(r) = MappedShmRegion::open_or_create(&shm_path, true) {
            let (tg_tx, _) = mpsc::sync_channel::<String>(16);
            let watch = Arc::new(AtomicLockWatch::new(false, false));
            let (vault_tx, vault_rx) = mpsc::sync_channel::<VaultIoCommand>(16);
            let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
            let mut rt = ExecRuntimeState::new();
            rt.wallet_1e8 = 6_513_770_000;
            let day1_ns = TimestampNs(1_794_787_200_000_000_000 + 14 * 3_600_000_000_000);
            let _ = rt.daily_gov.sync_utc_day(day1_ns, rt.wallet_1e8);
            let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
            snaps[0].symbol_id = APPROVED_SPECS[0].symbol_id;
            snaps[0].is_eligible = true;
            snaps[0].sequence_id = 10;
            snaps[0].ts_local_ns = day1_ns;
            snaps[0].realized_vol_5m = ScaledBps(5_500);
            snaps[0].bids[0] = BookLevel { price: PriceTicks(646_312), qty: LotQty(2_500) };
            snaps[0].asks[0] = BookLevel { price: PriceTicks(646_313), qty: LotQty(1_200) };
            snaps[0].refresh_deep_l1_l5_totals();
            snaps[0].mark_price = PriceTicks(646_312);
            snaps[0].last_trade_price = PriceTicks(646_312);
            snaps[0].factor_ofi = BoundedScore::new_clamped(0.85);
            snaps[0].factor_tfi = BoundedScore::new_clamped(0.92);
            snaps[0].factor_geom = BoundedScore::new_clamped(0.75);
            snaps[3] = snaps[0];
            snaps[3].symbol_id = APPROVED_SPECS[3].symbol_id;
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(450) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(320) };
            snaps[3].refresh_deep_l1_l5_totals();
            snaps[3].mark_price = PriceTicks(8_200);
            snaps[3].last_trade_price = PriceTicks(8_200);

            let mut entry_f = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
            let mut engines = SlotExecutionEngine::new_12_slot_array();
            engines[0].learned_mfe_bps = 680;
            engines[3].learned_mfe_bps = 680;
            let mut gov = OnlineAdaptiveGovernor::new();
            let mut ledger = ParitySyncLedger::new(rt.wallet_1e8);
            let mut last_rest = 0_i64;

            let cell_sl = ClosedTradeSummary {
                kind: ClosedTradeKind::ServerMarkPriceStopLoss, side: Side::Ask,
                entry_price: PriceTicks(8_200), exit_price: PriceTicks(8_235), closed_lots: LotQty(22),
                legs_consolidated: 1, gross_scaled_bps: ScaledBps(-426), fee_scaled_bps: ScaledBps(750),
                net_scaled_bps: ScaledBps(-1_176), net_pnl_1e8: -21_200_000, fee_1e8: 13_500_000, is_maker_exit: false,
            };
            dispatch_closed_trade_logs_with_locks(
                3, &cell_sl, &engines[3], &entry_f[3], &snaps[3], &ledger, &gov,
                &MarkoutCurveBps::default(), 0, &vault_tx, Some(&watch), &mut rt.halt_locked, day1_ns,
            );
            assert!(!rt.halt_locked && !watch.is_halt_locked());

            rt.daily_gov.phase = DailyShieldPhase::HibernatingProfitLocked;
            assert_eq!(step_12_slot_daemon_tick(day1_ns, None, inst, Some(&watch), &mut rt, &mut last_rest, &snaps, &mut entry_f, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx), (0, 0));

            let day2_ns = TimestampNs(day1_ns.0 + 86_400_000_000_000);
            snaps[0].ts_local_ns = day2_ns;
            snaps[3].ts_local_ns = day2_ns;
            assert_eq!(step_12_slot_daemon_tick(day2_ns, None, inst, Some(&watch), &mut rt, &mut last_rest, &snaps, &mut entry_f, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx), (0, 1));
            assert_eq!(rt.daily_gov.phase, DailyShieldPhase::ActiveHunting);
            let tel0 = r.arena().telemetry(0).map(|t| t.read_consistent_or_default(2)).unwrap_or_default();
            assert_eq!(LastVetoCode::from_code(tel0.last_veto_code), LastVetoCode::QueueDepthOrOversizedLot);
            assert!(matches!(engines[3].state, OrderLifecycleState::PlacingEntryMaker { side: Side::Bid, .. }));

            let _ = engines[3].on_maker_entry_filled(TimestampNs(1_794_844_800_000_000_000 - 5_000_000_000));
            snaps[3].ts_local_ns = TimestampNs(1_794_844_800_000_000_000 + 2_000_000_000);
            snaps[3].funding_rate_scaled_bps = ScaledBps(-150);
            let mut f_cuts = [0_i64; MAX_ACTIVE_PAIRS];
            assert_eq!(settle_live_8h_funding_cuts(&engines, &snaps, &mut f_cuts, &mut ledger), 1);
            assert!(ledger.cumulative_funding_1e8 > 0 && vault_rx.try_recv().is_ok());
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}

#[cfg(test)]
mod v49_daemon_telemetry_tests {
    use super::*;
    use crate::execution_fsm::record_live_ws_pulse;
    use crate::lob_shm::BookLevel;

    #[test]
    fn test_v49_daemon_1hz_position_path_and_ws_pulse_end_to_end() {
        record_live_ws_pulse(980, 99);
        let mut rt = ExecRuntimeState::new();
        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let gov = OnlineAdaptiveGovernor::new();
        let ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let (vtx, vrx) = mpsc::sync_channel::<VaultIoCommand>(32);

        snaps[1].symbol_id = APPROVED_SPECS[1].symbol_id;
        snaps[1].ts_local_ns = TimestampNs(1_794_880_271_000_000_000);
        snaps[1].realized_vol_5m = ScaledBps(3_000);
        snaps[1].bids[0] = BookLevel { price: PriceTicks(182_033), qty: LotQty(120) };
        snaps[1].asks[0] = BookLevel { price: PriceTicks(182_034), qty: LotQty(120) };
        snaps[1].mark_price = PriceTicks(182_033);
        snaps[1].last_trade_price = PriceTicks(182_033);

        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(670),
        };
        assert!(engines[1].try_arm_maker_entry(&snaps[1], verdict, LotQty(1)).is_ok());
        assert!(engines[1].on_maker_entry_filled(TimestampNs(1_794_880_271_000_000_000)).is_ok());

        for sec in 0..4_i64 {
            let t_now = TimestampNs(1_794_880_271_000_000_000 + sec * 1_000_000_000);
            snaps[1].ts_local_ns = t_now;
            snaps[1].last_trade_price = PriceTicks(182_033 + sec * 25);
            rt.step_v49_deep_telemetry_200hz(t_now, &snaps, &engines, &gov, &ledger, Some(&vtx));
        }
        assert_eq!(rt.intra_rings[1].len(), 4);

        rt.emit_live_position_path_from_delta_v49(
            15, 1, Side::Bid, PriceTicks(182_033), LotQty(1), 391_644,
            TimestampNs(1_794_880_271_000_000_000), TimestampNs(1_794_880_355_000_000_000),
            &snaps[1], &vtx,
        );
        assert!(rt.intra_rings[1].is_empty());

        let mut verified_path = false;
        while let Ok(cmd) = vrx.try_recv() {
            if let VaultIoCommand::RecordPositionPath(p) = cmd {
                verified_path = p.trade_id == 15 && p.sample_count == 4 && p.ws_rtt_us == 980;
            }
        }
        assert!(verified_path);
    }
}

#[cfg(test)]
mod v49_daemon_ev_guard_tests {
    use super::*;
    use crate::bybit_client::InstrumentSpecVerification;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[test]
    fn test_v49_daemon_vetoes_tao17_and_eth18_at_200hz() {
        let shm_p = MappedShmRegion::ephemeral_self_test_path("v49_ev_daemon");
        let Ok(reg) = MappedShmRegion::open_or_create(&shm_p, true) else { return };
        let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(8);
        let (v_tx, _v_rx) = mpsc::sync_channel::<VaultIoCommand>(8);
        let mut rt = ExecRuntimeState::new();
        rt.wallet_1e8 = 6_502_850_000;
        let mut rest_ns = 0_i64;
        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        let mut foren = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut engs = SlotExecutionEngine::new_12_slot_array();
        let mut gov = OnlineAdaptiveGovernor::new();
        let mut led = ParitySyncLedger::new(6_502_850_000);
        let t0 = TimestampNs(1_794_888_600_000_000_000);

        snaps[0].symbol_id = APPROVED_SPECS[0].symbol_id;
        snaps[0].sequence_id = 10;
        snaps[0].ts_local_ns = t0;
        snaps[0].realized_vol_5m = ScaledBps(3_000);
        snaps[0].bids[0] = BookLevel { price: PriceTicks(646_300), qty: LotQty(1_200) };
        snaps[0].asks[0] = BookLevel { price: PriceTicks(646_301), qty: LotQty(1_200) };
        snaps[0].factor_ofi = BoundedScore::new_clamped(-0.65);
        snaps[0].factor_tfi = BoundedScore::new_clamped(-0.75);
        snaps[0].factor_geom = BoundedScore::new_clamped(-0.60);

        snaps[4] = snaps[0];
        snaps[4].symbol_id = APPROVED_SPECS[4].symbol_id;
        snaps[4].bids[0] = BookLevel { price: PriceTicks(19_335), qty: LotQty(900) };
        snaps[4].asks[0] = BookLevel { price: PriceTicks(19_336), qty: LotQty(900) };
        snaps[4].refresh_deep_l1_l5_totals();
        snaps[4].set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.515),
            macro_phase: BoundedScore::new_clamped(0.90),
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(5_514),
        });

        let _ = step_12_slot_daemon_tick(
            t0, None, InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS), None,
            &mut rt, &mut rest_ns, &snaps, &mut foren, &mut engs, &mut gov, &mut led,
            reg.arena(), &tg_tx, &v_tx,
        );
        assert!(matches!(engs[4].state, OrderLifecycleState::Idle));
        if let Ok(t4) = reg.arena().telemetry(4) {
            assert_eq!(LastVetoCode::from_code(t4.read_consistent_or_default(2).last_veto_code), LastVetoCode::MacroElliottContradiction);
        }
        let _ = std::fs::remove_file(&shm_p);
    }
}

/// P0-A predicate: dispatches `EmergencyFlattenSlot` on `BreachEmergencyFlatten` even when `verified_exchange_sl > 0`.
#[inline]
#[must_use]
pub fn should_trigger_live_emergency_flatten(
    rt: &ExecRuntimeState,
    slot_idx: usize,
    side: Side,
    fallback_eff_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> (bool, PriceTicks) {
    if let Some(backup_sl) = rt.emergency_flatten_sl[slot_idx] {
        return (true, backup_sl);
    }
    let v_sl = rt.verified_exchange_sl[slot_idx];
    if v_sl.0 <= 0 {
        let unv = match side {
            Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= fallback_eff_sl.0,
            Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= fallback_eff_sl.0,
        };
        return (unv, fallback_eff_sl);
    }
    if fallback_eff_sl.0 > 0 {
        let cushion_t = ((fallback_eff_sl.0.saturating_mul(153)) / 1_000_000).max(2);
        let last_px = if snap.last_trade_price.0 > 0 { snap.last_trade_price.0 } else { snap.mark_price.0 };
        let gap_through = match side {
            Side::Bid => fallback_eff_sl.0 >= v_sl.0.saturating_add(cushion_t * 2) && snap.mark_price.0 > 0 && snap.mark_price.0 <= fallback_eff_sl.0.saturating_sub(cushion_t) && last_px <= fallback_eff_sl.0.saturating_sub(cushion_t),
            Side::Ask => v_sl.0 >= fallback_eff_sl.0.saturating_add(cushion_t * 2) && snap.mark_price.0 >= fallback_eff_sl.0.saturating_add(cushion_t) && last_px >= fallback_eff_sl.0.saturating_add(cushion_t),
        };
        if gap_through { return (true, fallback_eff_sl); }
    }
    (false, fallback_eff_sl)
}

/// Elastic C1 daily tail-risk envelope: bounds multi-slot exposure by `DailySessionGovernor`, `p_Taker` & `Chandelier`.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn passes_elastic_daily_tail_risk_budget(
    cand_slot: usize,
    cand_side: Side,
    cand_risk_ppm: i64,
    wallet_1e8: i64,
    daily_gov: &DailySessionGovernor,
    sinking_reserve_bps_x100: i64,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    gov: &OnlineAdaptiveGovernor,
) -> bool {
    const PHI_D: f64 = 1.618_033_988_749_895;
    const INV_PHI_D: f64 = 0.618_033_988_749_895;
    const INV_PHI2_D: f64 = 0.381_966_011_250_105;

    let fees = gov.active_fee_schedule();
    let stop_abs_ppm = daily_gov
        .elastic_loss_stop_ppm(sinking_reserve_bps_x100, gov.active_maker_rt_fee_bps())
        .abs()
        .max(3_883) as f64;
    let used_loss_ppm = (-daily_gov.day_pnl_ppm()).max(0) as f64;
    let rem_budget_ppm = (stop_abs_ppm - used_loss_ppm).max(0.0);

    let eff_tail = |s_i: usize, sd: Side, r_ppm: i64| -> f64 {
        let p_tkr = (gov.slot_taker_prob_permille(s_i, sd).clamp(5, 950) as f64) * 1e-3;
        let h_t = crate::wave_elliott::hermite_smoothstep_c1(p_tkr, 0.04, 0.25);
        (r_ppm.max(0) as f64) * (INV_PHI_D + INV_PHI2_D * h_t)
    };

    let mut open_tail_ppm = 0.0_f64;
    let mut unhedged_count = 0_usize;
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        if s_idx == cand_slot {
            continue;
        }
        let active_opt = match engines[s_idx].state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, .. } => {
                Some((side, entry_price, qty, false, 1.0_f64))
            }
            OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, open_lots, .. } => {
                let (_, res_s3) = engines[s_idx].house_money_residual_risk_1e8_and_s3_scale();
                Some((side, avg_price_ticks, open_lots, engines[s_idx].chandelier_profit_sl_armed, res_s3))
            }
        };
        if let Some((sd, px, lots, chand_locked, res_s3)) = active_opt {
            if !chand_locked && res_s3 > 1e-9 {
                let r_ppm = slot_position_risk_ppm_with_fees(
                    APPROVED_SPECS[s_idx],
                    wallet_1e8,
                    px,
                    lots,
                    cached_snaps[s_idx].realized_vol_5m,
                    fees,
                );
                let eff_r_ppm = ((r_ppm as f64) * res_s3).round() as i64;
                open_tail_ppm += eff_tail(s_idx, sd, eff_r_ppm);
                unhedged_count += 1;
            }
        }
    }
    let cand_tail_ppm = eff_tail(cand_slot, cand_side, cand_risk_ppm);
    let cluster_env = PHI_D / (1.0 + INV_PHI_D * (unhedged_count as f64));
    (open_tail_ppm + cand_tail_ppm) <= cluster_env * rem_budget_ppm
}

#[inline]
pub const fn step_drain_standby_latch(
    drain_active: bool,
    open_count: usize,
    pending_entry_count: usize,
    any_inflight: bool,
    drain_flushed_once: &mut bool,
) -> (bool, bool) {
    if !drain_active {
        *drain_flushed_once = false;
        return (false, false);
    }
    if open_count == 0 && pending_entry_count == 0 && !any_inflight {
        if !*drain_flushed_once {
            *drain_flushed_once = true;
            return (true, true);
        }
        return (false, true);
    }
    (false, false)
}

#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops)]
pub fn compute_resting_l1_hawkes_lambda_v531(
    event_dilation: f64,
    opp_hit: LotQty,
    init_q: LotQty,
    dt_s: f64,
    tracker_lambda_ratio: f64,
    signed_flow_slow: crate::types::BoundedScore,
) -> f64 {
    const INV_PHI4_H: f64 = 0.145_898_033_750_315;
    let inst_burst = (1.0
        + 1.618_033_988_749_895
            * ((opp_hit.0.max(0) as f64) / ((init_q.0.max(1) as f64) * dt_s.max(0.05))))
    .clamp(0.382, 6.854);
    let trk = if tracker_lambda_ratio.is_finite() {
        tracker_lambda_ratio.clamp(1.0, 6.854)
    } else {
        1.0
    };
    let burst = inst_burst.max(trk);
    let dil_mult = if signed_flow_slow.get() >= -INV_PHI4_H && burst <= 1.05 {
        event_dilation.min(1.0)
    } else {
        event_dilation
    };
    dil_mult * burst
}

#[inline]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn compute_resting_l1_max_chase_ns_v531(
    gov: &OnlineAdaptiveGovernor,
    slot_idx: usize,
    side: Side,
    realized_vol_5m: ScaledBps,
    event_dilation: f64,
    hawkes_lambda_ratio: f64,
    signed_flow_slow: crate::types::BoundedScore,
    init_q: LotQty,
    rem_q: LotQty,
    base_chase_ns: i64,
) -> i64 {
    let pat_l1 = gov.elastic_resting_l1_patience_ns_with_queue_v531(
        slot_idx,
        side,
        realized_vol_5m,
        event_dilation,
        hawkes_lambda_ratio,
        signed_flow_slow,
        init_q,
        rem_q,
    );
    if signed_flow_slow.get() >= -crate::alpha_3factor::INV_PHI_FOURTH
        && hawkes_lambda_ratio <= crate::alpha_3factor::PHI
    {
        pat_l1.max(base_chase_ns)
    } else {
        pat_l1
    }
}

/// Elastic resting-entry chase budget & expiration predicate (V5.3.2):
/// Eliminates the rigid `(!at_l1 && elapsed_ns >= (base_chase_ns * 3) / 4)` (`4.200s + ~156ms RTT = 4,356ms`)
/// cutoff when slow flow is non-adverse (`signed_flow_slow >= -INV_PHI_FOURTH`) and no toxic Hawkes burst is
/// active (`hawkes_lambda_ratio <= PHI`), preserving full elastic L1 patience (~10..28s scaling with `tau_mkt_s`
/// and FIFO progress) while retaining fast aborts under adverse flow or Hawkes sweeps.
#[inline]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_resting_queue_chase_budget_v532(
    gov: &OnlineAdaptiveGovernor,
    slot_idx: usize,
    side: Side,
    realized_vol_5m: ScaledBps,
    event_dilation: f64,
    hawkes_lambda_ratio: f64,
    signed_flow_slow: crate::types::BoundedScore,
    init_q: LotQty,
    rem_q: LotQty,
    base_chase_ns: i64,
    at_l1: bool,
    elapsed_ns: i64,
    total_chase_ns: i64,
) -> (i64, bool) {
    let healthy_flow = signed_flow_slow.get() >= -crate::alpha_3factor::INV_PHI_FOURTH;
    let no_toxic_burst = hawkes_lambda_ratio <= crate::alpha_3factor::PHI;
    let l1_patience_ns = compute_resting_l1_max_chase_ns_v531(
        gov,
        slot_idx,
        side,
        realized_vol_5m,
        event_dilation,
        hawkes_lambda_ratio,
        signed_flow_slow,
        init_q,
        rem_q,
        base_chase_ns,
    );
    let keep_elastic_patience = at_l1 || (healthy_flow && no_toxic_burst);
    let max_chase_ns = if keep_elastic_patience {
        l1_patience_ns
    } else {
        base_chase_ns.min(l1_patience_ns)
    };
    let off_l1_expired = !keep_elastic_patience && elapsed_ns >= (base_chase_ns * 3) / 4;
    let chase_expired = off_l1_expired
        || elapsed_ns >= max_chase_ns
        || total_chase_ns >= max_chase_ns.saturating_add(base_chase_ns / 2);
    (max_chase_ns, chase_expired)
}

/// Moallemi-Yuan FIFO queue priority `v_prio = 1.0 - (rem_q / (init_q + 1))` in `[0.0, 1.0]` (`V5.5.4`).
#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops)]
pub fn compute_moallemi_yuan_queue_priority_c1(init_q: LotQty, rem_q: LotQty) -> f64 {
    let init_denom = (init_q.0.max(0) as f64) + 1.0;
    let rem_f = rem_q.0.max(0) as f64;
    (1.0 - (rem_f / init_denom)).clamp(0.0, 1.0)
}

/// `C1` priority-weighted deadband `INV_PHI_FOURTH * (1.0 + INV_PHI * v_prio)` on `MacroElliottContradiction` (`V5.5.4`).
#[inline]
#[must_use]
#[allow(clippy::suboptimal_flops)]
pub fn compute_resting_macro_deadband_c1(v_prio: f64) -> f64 {
    let vp = if v_prio.is_finite() { v_prio.clamp(0.0, 1.0) } else { 0.0 };
    crate::alpha_3factor::INV_PHI_FOURTH * (1.0 + crate::alpha_3factor::INV_PHI * vp)
}

/// Resting-queue Elliott macro coherence guard with `C1` Moallemi-Yuan priority deadband (`V5.5.4`).
#[inline]
#[must_use]
pub fn passes_resting_elliott_macro_hysteresis_c1(
    side: Side,
    snap: &SymbolLobSnapshot,
    btc_leader_snap: &SymbolLobSnapshot,
    epps_corr: f64,
    btc_lead_aligned_override: Option<f64>,
    psi_health: f64,
    v_prio: f64,
) -> bool {
    let is_follower = snap.symbol_id != btc_leader_snap.symbol_id;
    let eff_corr = if is_follower { epps_corr } else { 0.0 };
    let btc_aligned = if is_follower {
        crate::alpha_3factor::compute_btc_leader_aligned_score(
            side,
            snap,
            btc_leader_snap,
            eff_corr,
            btc_lead_aligned_override,
        )
    } else {
        0.0
    };
    let surf = crate::execution_fsm::evaluate_macro_coherence_surface_with_health_c1(
        side,
        snap,
        eff_corr,
        btc_aligned,
        psi_health,
    );
    surf < crate::alpha_3factor::INV_PHI + compute_resting_macro_deadband_c1(v_prio)
}

/// Resting-queue `BtcLeadLagHeadwind` guard requiring local weakness (`ofi_al < INV_PHI_CUBE` or `signed_slow < 0.0`)
/// whenever `rem_q` progresses organically without `fast_sweep` (`V5.5.4`).
#[inline]
#[must_use]
pub fn is_resting_headwind_erosion_v554(
    slot_idx: usize,
    side: Side,
    elapsed_ns: i64,
    headwind_abort_ns: i64,
    init_q_ahead: LotQty,
    cur_q_ahead: LotQty,
    fast_sweep: bool,
    btc_aligned: f64,
    flow_imp: f64,
    snap: &SymbolLobSnapshot,
    signed_slow: crate::types::BoundedScore,
) -> bool {
    if !crate::kinematic_surface::is_headwind_queue_erosion_c1(
        slot_idx,
        elapsed_ns,
        headwind_abort_ns,
        init_q_ahead,
        cur_q_ahead,
        btc_aligned,
        flow_imp,
    ) {
        return false;
    }
    let progressed_without_sweep =
        init_q_ahead.0 > 0 && cur_q_ahead.0 < init_q_ahead.0 && !fast_sweep;
    if progressed_without_sweep {
        let ofi_al = snap.factor_ofi.get() * side.sign_f64();
        let local_weakness =
            ofi_al < crate::alpha_3factor::INV_PHI_CUBE || signed_slow.get() < 0.0;
        if !local_weakness {
            return false;
        }
    }
    true
}

/// Favorable BBO escape distance in ticks past a passive `RestingEntryQueue` order (`V5.5.4`).
#[inline]
#[must_use]
pub const fn favorable_bbo_miss_ticks(
    side: Side,
    entry_price: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> i64 {
    if entry_price.0 <= 0 {
        return 0;
    }
    match side {
        Side::Bid => {
            if snap.bids[0].price.0 > entry_price.0 {
                snap.bids[0].price.0 - entry_price.0
            } else {
                0
            }
        }
        Side::Ask => {
            if snap.asks[0].price.0 > 0 && snap.asks[0].price.0 < entry_price.0 {
                entry_price.0 - snap.asks[0].price.0
            } else {
                0
            }
        }
    }
}

/// Evaluates `RestingEntryQueue` `Re-Peg` candidate (`V5.5.4`), unlocking `1..=2` tick `fav_bbo_miss` re-quoting
/// whenever the new maker price preserves `Pilar 2 EV > 0` after deducting cumulative chase excursion.
#[must_use]
pub fn evaluate_resting_repeg_candidate_v554(
    gov: &OnlineAdaptiveGovernor,
    s_idx: usize,
    side: Side,
    wallet_1e8: i64,
    qty: LotQty,
    entry_price: PriceTicks,
    original_armed_px: PriceTicks,
    learned_mfe_bps: ScaledBps,
    target_excursion: ScaledBps,
    taker_prob_pm: i64,
    fees: FeeSchedule,
    snap: &SymbolLobSnapshot,
    btc_snap: &SymbolLobSnapshot,
    spoof_d: f64,
    epps_beta: f64,
    epps_corr: f64,
    btc_lead_opt: Option<f64>,
    ll_bonus: f64,
    psi_health: f64,
    prop_now: f64,
    v_prio: f64,
) -> Option<(PriceTicks, i64)> {
    if s_idx >= MAX_ACTIVE_PAIRS || snap.bbo_and_spread_ticks().is_none() {
        return None;
    }
    let spec = APPROVED_SPECS[s_idx];
    if !crate::alpha_3factor::passes_elastic_anti_spoof_ofi_guard(
        side,
        snap,
        spoof_d,
        taker_prob_pm,
        gov.slot_eq_vol_5m_bps(s_idx),
        fees,
    ) || !passes_stoikov_repeg_coherence_guard_with_beta(
        s_idx, side, snap, btc_snap, epps_beta,
    ) || !passes_resting_elliott_macro_hysteresis_c1(
        side, snap, btc_snap, epps_corr, btc_lead_opt, psi_health, v_prio,
    ) || !passes_order_block_wall_guard_dynamic(s_idx, side, snap, taker_prob_pm, fees)
        || !gov.passes_elastic_funding_cliff_guard(s_idx, side, learned_mfe_bps, snap)
    {
        return None;
    }
    let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
    let cand_repeg = select_elastic_entry_maker_price_v542(side, snap, psi_health, prop_now);
    let anchor_px = if original_armed_px.0 > 0 {
        original_armed_px
    } else {
        entry_price
    };
    let fav_miss_ticks = favorable_bbo_miss_ticks(side, entry_price, snap);
    let fav_miss_1_2 = fav_miss_ticks >= 1 && fav_miss_ticks <= 2;
    let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, taker_prob_pm, fees).max(2);
    let fee_mkr_sbps = ScaledBps(fees.round_trip_maker_exit_bps().raw());
    let bbo_shift = match side {
        Side::Bid => new_l1.0 - entry_price.0,
        Side::Ask => entry_price.0 - new_l1.0,
    };
    if bbo_shift < 0 || bbo_shift > max_chase_ticks {
        return None;
    }
    let eval_px = |px: PriceTicks| -> Option<(PriceTicks, i64)> {
        let post_only_safe = match side {
            Side::Bid => px.0 > entry_price.0 && px.0 < snap.asks[0].price.0,
            Side::Ask => px.0 > 0 && px.0 < entry_price.0 && px.0 > snap.bids[0].price.0,
        };
        if !post_only_safe {
            return None;
        }
        let cum_chase_ticks = ((px.0 - anchor_px.0) * side.sign_i64()).max(0);
        let chase_sbps = (cum_chase_ticks.saturating_mul(1_000_000)) / anchor_px.0.max(1);
        if fav_miss_1_2 && cum_chase_ticks <= 2 {
            let rem_mfe = ScaledBps((learned_mfe_bps.raw() - chase_sbps).max(0));
            let rem_exc = ScaledBps((target_excursion.raw() - chase_sbps).max(0));
            if rem_exc.raw() <= fee_mkr_sbps.raw()
                || !spec.passes_quantum_ev_growth_gate(
                    wallet_1e8,
                    px,
                    snap.realized_vol_5m,
                    rem_mfe,
                    taker_prob_pm,
                    fees,
                )
            {
                return None;
            }
            let ev_sbps = gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
                s_idx, side, qty, px, rem_mfe, rem_exc, snap, btc_snap, epps_beta, spoof_d,
            )?;
            return Some((px, ev_sbps));
        }
        if !passes_cumulative_repeg_chase_cap_v542(
            side,
            anchor_px,
            px,
            target_excursion,
            fee_mkr_sbps,
        ) || gov
            .confirm_preflight_kinematics_v55(
                s_idx,
                side,
                wallet_1e8,
                qty,
                learned_mfe_bps,
                snap,
                btc_snap,
                spoof_d,
                epps_beta,
                epps_corr,
                btc_lead_opt,
                ll_bonus,
            )
            .is_err()
        {
            return None;
        }
        let ev_sbps = gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v555_sbps(
            s_idx,
            side,
            qty,
            px,
            learned_mfe_bps,
            target_excursion,
            snap,
            btc_snap,
            epps_beta,
            spoof_d,
        )?;
        Some((px, ev_sbps))
    };
    if cand_repeg != new_l1 {
        if let Some(res) = eval_px(cand_repeg) {
            return Some(res);
        }
    }
    eval_px(new_l1)
}

#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn evaluate_elastic_spoof_geom_abort_with_entry_v54(
    side: Side,
    entry_price: PriceTicks,
    snap: &SymbolLobSnapshot,
    armed_geom_raw: f64,
    elapsed_ns: i64,
    signed_flow_slow: crate::types::BoundedScore,
    spoof_density: f64,
    event_dilation: f64,
    taker_prob_pm: i64,
    ws_rtt_us: u32,
    now_ns: i64,
    geom_adv_since_ns: &mut i64,
    init_q_lots: LotQty,
    rem_q_lots: LotQty,
) -> bool {
    if crate::kinematic_surface::is_bbo_favorably_past_resting_entry(side, entry_price, snap) {
        *geom_adv_since_ns = 0;
        return false;
    }
    evaluate_elastic_spoof_geom_abort_with_queue_v531(
        side,
        snap,
        armed_geom_raw,
        elapsed_ns,
        signed_flow_slow,
        spoof_density,
        event_dilation,
        taker_prob_pm,
        ws_rtt_us,
        now_ns,
        geom_adv_since_ns,
        init_q_lots,
        rem_q_lots,
    )
}

#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn evaluate_elastic_spoof_geom_abort_with_queue_v531(
    side: Side,
    snap: &SymbolLobSnapshot,
    armed_geom_raw: f64,
    elapsed_ns: i64,
    signed_flow_slow: crate::types::BoundedScore,
    spoof_density: f64,
    event_dilation: f64,
    taker_prob_pm: i64,
    ws_rtt_us: u32,
    now_ns: i64,
    geom_adv_since_ns: &mut i64,
    init_q_lots: LotQty,
    rem_q_lots: LotQty,
) -> bool {
    let dir_s = side.sign_f64();
    let signed_ofi = snap.factor_ofi.get() * dir_s;
    if !crate::kinematic_surface::allows_front_of_queue_spoof_geom_abort_v54(
        init_q_lots,
        rem_q_lots,
        elapsed_ns,
        signed_ofi,
        signed_flow_slow.get(),
        ws_rtt_us,
        event_dilation,
    ) {
        *geom_adv_since_ns = 0;
        return false;
    }
    evaluate_elastic_spoof_geom_abort_v51(
        side,
        snap,
        armed_geom_raw,
        elapsed_ns,
        signed_flow_slow,
        spoof_density,
        event_dilation,
        taker_prob_pm,
        ws_rtt_us,
        now_ns,
        geom_adv_since_ns,
    )
}

#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn evaluate_elastic_spoof_geom_abort_v51(
    side: Side,
    snap: &SymbolLobSnapshot,
    armed_geom_raw: f64,
    elapsed_ns: i64,
    signed_flow_slow: crate::types::BoundedScore,
    spoof_density: f64,
    event_dilation: f64,
    taker_prob_pm: i64,
    ws_rtt_us: u32,
    now_ns: i64,
    geom_adv_since_ns: &mut i64,
) -> bool {
    const PHI_S: f64 = 1.618_033_988_749_895;
    const PHI_SQ_S: f64 = 2.618_033_988_749_895;
    const INV_PHI_S: f64 = 0.618_033_988_749_895;
    const INV_PHI2_S: f64 = 0.381_966_011_250_105;
    const INV_PHI3_S: f64 = 0.236_067_977_499_79;
    const INV_PHI4_S: f64 = 0.145_898_033_750_315;
    let dir_s = side.sign_f64();
    let signed_geom = snap.factor_geom.get() * dir_s;
    if signed_geom >= 0.0 || elapsed_ns <= 0 {
        *geom_adv_since_ns = 0;
        return false;
    }
    let p_t = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let sp = if spoof_density.is_finite() { spoof_density.clamp(0.0, 1.0) } else { 1.0 };
    let kappa = if event_dilation.is_finite() { event_dilation.clamp(INV_PHI_S, PHI_S) } else { PHI_S };
    let clean_armed_g = if armed_geom_raw.is_finite() { armed_geom_raw } else { 0.0 };
    let arm_rtt_ns = (if ws_rtt_us > 0 { u64::from(ws_rtt_us) } else { 25_000 }).clamp(15_000, 100_000) as f64 * 1_000.0;
    let tau_arm_ns = ((PHI_S * arm_rtt_ns * (1.0 - INV_PHI2_S * p_t)) / kappa).round() as i64;
    let g_in = (clean_armed_g * dir_s).max(0.0);
    let s_rug = crate::wave_elliott::hermite_smoothstep_c1(g_in - signed_geom, INV_PHI2_S, 1.0 - INV_PHI4_S);
    let psi_geom = (-signed_geom) * (1.0 + PHI_S * s_rug);
    let s_fl = crate::wave_elliott::hermite_smoothstep_c1(signed_flow_slow.get(), 0.0, INV_PHI_S);
    let theta_hard = INV_PHI3_S * (1.0 + INV_PHI2_S * s_fl) / ((1.0 + PHI_S * p_t) * (1.0 + INV_PHI_S * sp));
    if elapsed_ns >= tau_arm_ns && psi_geom >= theta_hard {
        return true;
    }
    let g_soft = -INV_PHI4_S * (1.0 - INV_PHI2_S * p_t) / (1.0 + INV_PHI_S * sp);
    let flow_weak = signed_flow_slow.get() < INV_PHI3_S * (1.0 - sp);
    if signed_geom < g_soft && flow_weak {
        if *geom_adv_since_ns <= 0 { *geom_adv_since_ns = now_ns; return false; }
        let hold_rtt_ns = (if ws_rtt_us > 0 { u64::from(ws_rtt_us) } else { 75_000 }).clamp(20_000, 250_000) as f64 * 1_000.0;
        let req_ns = ((PHI_SQ_S * hold_rtt_ns * (1.0 + INV_PHI_S * sp)) / kappa).round() as i64;
        now_ns.saturating_sub(*geom_adv_since_ns) >= req_ns
    } else {
        *geom_adv_since_ns = 0;
        false
    }
}

#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::suboptimal_flops, clippy::too_many_arguments)]
pub fn evaluate_toxic_sweep_velocity_abort_v51(
    spec: PairSpec,
    side: Side,
    entry_price: PriceTicks,
    init_q_ahead: LotQty,
    cur_q_ahead: LotQty,
    elapsed_ns: i64,
    snap: &SymbolLobSnapshot,
    event_dilation: f64,
    taker_prob_pm: i64,
    fees: FeeSchedule,
    ws_rtt_us: u32,
) -> bool {
    const PHI_W: f64 = 1.618_033_988_749_895;
    const PHI_CUBE_W: f64 = 4.236_067_977_499_79;
    const INV_PHI_W: f64 = 0.618_033_988_749_895;
    const INV_PHI2_W: f64 = 0.381_966_011_250_105;
    const INV_PHI3_W: f64 = 0.236_067_977_499_79;
    if elapsed_ns <= 0 || init_q_ahead.0 <= 0 || cur_q_ahead.0 >= init_q_ahead.0 { return false; }
    let min_l1 = spec.min_resting_l1_lots_dynamic(entry_price, snap.realized_vol_5m, taker_prob_pm, fees).0.max(1);
    if (init_q_ahead.0 as f64) < PHI_W * (min_l1 as f64) { return false; }
    let p_t = (taker_prob_pm.clamp(5, 950) as f64) * 1e-3;
    let rtt_ns = (if ws_rtt_us > 0 { u64::from(ws_rtt_us) } else { 75_000 }).clamp(20_000, 200_000) as f64 * 1_000.0;
    let kappa = if event_dilation.is_finite() { event_dilation.clamp(INV_PHI_W, PHI_W) } else { PHI_W };
    let tau_sweep_ns = ((PHI_CUBE_W * rtt_ns * (1.0 + INV_PHI_W * (1.0 - p_t))) / kappa).round() as i64;
    if elapsed_ns > tau_sweep_ns { return false; }
    let rem_ratio = (cur_q_ahead.0.max(0) as f64) / (init_q_ahead.0 as f64);
    let dir_s = side.sign_f64();
    let adv_tape = (snap.factor_tfi.get() * dir_s) < -INV_PHI3_W || (snap.factor_ofi.get() * dir_s) < -INV_PHI3_W;
    rem_ratio <= INV_PHI2_W && adv_tape
}

#[cfg(test)]
mod v51_f1_f4b_daemon_tests {
    use super::*;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_v51_f1_drain_latch_once_and_f4b_spoof_hysteresis_and_sweep_abort() {
        let mut flushed = false;
        assert_eq!(step_drain_standby_latch(true, 1, 0, false, &mut flushed), (false, false));
        assert_eq!(step_drain_standby_latch(true, 0, 0, false, &mut flushed), (true, true));
        assert_eq!(step_drain_standby_latch(true, 0, 0, false, &mut flushed), (false, true));
        assert_eq!(step_drain_standby_latch(false, 0, 0, false, &mut flushed), (false, false));

        let mut s = SymbolLobSnapshot::new_empty();
        s.symbol_id = APPROVED_SPECS[1].symbol_id;
        s.realized_vol_5m = ScaledBps(2_200);
        s.bids[0] = BookLevel { price: PriceTicks(179_960), qty: LotQty(400) };
        s.asks[0] = BookLevel { price: PriceTicks(179_961), qty: LotQty(200) };
        s.factor_geom = BoundedScore::new_clamped(-0.18);
        let mut adv_since = 0_i64;
        assert!(!evaluate_elastic_spoof_geom_abort_v51(Side::Bid, &s, 0.20, 200_000_000, BoundedScore::new_clamped(0.55), 0.10, 1.0, 50, 75_000, 1_000_000_000, &mut adv_since));
        assert!(!evaluate_elastic_spoof_geom_abort_v51(Side::Bid, &s, 0.20, 200_000_000, BoundedScore::new_clamped(-0.15), 0.10, 1.0, 50, 75_000, 1_000_000_000, &mut adv_since));
        assert!(!evaluate_elastic_spoof_geom_abort_v51(Side::Bid, &s, 0.20, 350_000_000, BoundedScore::new_clamped(-0.15), 0.10, 1.0, 50, 75_000, 1_150_000_000, &mut adv_since));
        assert!(evaluate_elastic_spoof_geom_abort_v51(Side::Bid, &s, 0.20, 550_000_000, BoundedScore::new_clamped(-0.15), 0.10, 1.0, 50, 75_000, 1_350_000_000, &mut adv_since));

        s.factor_ofi = BoundedScore::new_clamped(0.52);
        s.factor_tfi = BoundedScore::new_clamped(0.84);
        assert!(evaluate_toxic_sweep_velocity_abort_v51(APPROVED_SPECS[1], Side::Ask, PriceTicks(179_961), LotQty(11_211), LotQty(1_200), 210_000_000, &s, 1.0, 50, FeeSchedule::VIP0_DEFAULT, 75_000));
    }

    #[test]
    fn test_v52_h3_idle_ws_orphan_adopt_and_h6a_adverse_flow_c1() {
        assert!(!should_abort_queue_on_adverse_flow_c1(-0.72, 0.15, 50, 33, 0.0));
        assert!(should_abort_queue_on_adverse_flow_c1(-0.72, -0.35, 50, 33, 0.0));
        assert!(!should_abort_queue_on_adverse_flow_c1(-0.42, -0.18, 50, 80, 0.0));
        assert!(should_abort_queue_on_adverse_flow_c1(-0.42, -0.18, 450, -20, 0.75));

        let shm_p = MappedShmRegion::ephemeral_self_test_path("v52_h3_orphan_adopt");
        if let Ok(reg) = MappedShmRegion::open_or_create(&shm_p, true) {
            let mut engines = SlotExecutionEngine::new_12_slot_array();
            let mut gov = OnlineAdaptiveGovernor::new();
            let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
            let mut rt = ExecRuntimeState::new();
            let forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
            let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
            snaps[3].symbol_id = APPROVED_SPECS[3].symbol_id;
            snaps[3].ts_local_ns = TimestampNs(2_000_000_000);
            snaps[3].realized_vol_5m = ScaledBps(5_000);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(400) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(250) };
            snaps[3].mark_price = PriceTicks(8_201);
            snaps[3].last_trade_price = PriceTicks(8_201);

            rt.slot_rpc_inflight[3] = true;
            rt.abort_cmd_inflight[3] = true;
            rt.queue_flow_ema[3] = -0.35;
            let (tg_tx, tg_rx) = mpsc::sync_channel::<String>(8);
            let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultIoCommand>(8);

            handle_gateway_event(
                GatewayEvent::WsPositionUpdate {
                    slot_idx: 3,
                    is_open: true,
                    real_side: Side::Ask,
                    verified_avg: Some(PriceTicks(8_202)),
                    real_lots: LotQty(26),
                },
                TimestampNs(2_000_000_000),
                None,
                &mut rt,
                &snaps,
                &forensics,
                &mut engines,
                &mut gov,
                &mut ledger,
                reg.arena(),
                &tg_tx,
                &vault_tx,
            );

            assert!(!rt.slot_rpc_inflight[3] && !rt.abort_cmd_inflight[3]);
            assert!(rt.queue_flow_ema[3].abs() <= f64::EPSILON && rt.sl_needs_sync[3]);
            assert!(matches!(
                engines[3].state,
                OrderLifecycleState::PositionOpenMakerExit {
                    side: Side::Ask,
                    avg_price_ticks: PriceTicks(8_202),
                    open_lots: LotQty(26),
                    ..
                }
            ));
            assert!(engines[3].last_eff_sl_ticks.0 > 8_202);
            let tg_msg = tg_rx.try_recv().unwrap_or_default();
            assert!(tg_msg.contains("LIVE WS ORPHAN ADOPT"));

            if let OrderLifecycleState::PositionOpenMakerExit { resting_exit_price: ref mut rx_p, .. } = engines[3].state {
                *rx_p = PriceTicks(8_194);
            }
            let smp_opt = engines[3].sample_intra_trade_1hz(&snaps[3], 0.0);
            assert!(smp_opt.is_some());
            let Some(smp) = smp_opt else { return };
            let expected_tp_sbps = (((8_202_i64 - 8_194) * 1_000_000) / 8_202) as i16;
            assert_eq!(smp.tp_delta_sbps, expected_tp_sbps);
            drop(reg);
        }
        let _ = std::fs::remove_file(&shm_p);
    }
}
