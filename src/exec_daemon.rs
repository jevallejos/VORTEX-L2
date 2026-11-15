//! 200Hz Autonomous 12-Slot Execution Daemon (V4.6: Epps 5s, Live Markout Ring, Zero-Jitter Async & Elastic Queue).
#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
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
use crate::bybit_gateway::{run_daemon_startup_handshake, BybitAsyncGateway, GatewayCommand};
use crate::exec_gateway_handlers::{
    bootstrap_live_adopt_if_enabled, handle_gateway_event, promote_verified_fill_state,
    run_oos_520_certification_suite, ExecRuntimeState,
};
use crate::execution_fsm::{
    compute_validated_sl_ticks_with_fees, passes_elliott_macro_coherence_guard,
    passes_order_block_wall_guard_dynamic, passes_stoikov_repeg_coherence_guard_with_beta,
    CanonicalExitQuote, ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState,
    ServerSlDecision, SlotExecutionEngine,
};
use crate::fee_cadence_beta::{FeeSchedule, MonetaryPortfolioBetaGuard};
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{
    LastVetoCode, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::telemetry_hud::{
    dispatch_closed_trade_logs_v46, dispatch_open_positions_state, format_telegram_close_message,
    record_and_notify_entry_fill, rehydrate_shadow_ledger, render_live_hud,
    render_trades_and_executive_dashboard, spawn_bounded_telegram_notifier,
    spawn_bounded_vault_persister, SlotForensicEntrySnapshot, VaultPersistTask,
    DEFAULT_WALLET_EQUITY_1E8, DRAIN_LOCK_PATH, HALT_LOCK_PATH,
};
use crate::types::{
    now_epoch_ns, LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, ANSI_GRAY,
    ANSI_GREEN, ANSI_RESET, APPROVED_SPECS,
};
use crate::vault_io::{
    load_mfe_plasticity_from_path, spawn_vault_io_worker, AtomicLockWatch, VaultIoCommand,
    MFE_PLASTICITY_VAULT_PATH,
};

const STAGGER_BAR_TO_FLOW_PM: i64 = 200;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ElasticQueueTimersNs {
    pub macro_abort_ns: i64,
    pub headwind_abort_ns: i64,
    pub repeg_wait_ns: i64,
    pub repeg_cooldown_ns: i64,
}

/// Scales resting queue timers proportionally to `max_chase_ns` (`5.0s` canonical -> `0.5s, 2.2s, 2.5s, 1.5s`).
#[inline]
#[must_use]
pub const fn elastic_resting_queue_timers_ns(max_chase_ns: i64) -> ElasticQueueTimersNs {
    let c = if max_chase_ns < 2_500_000_000 {
        2_500_000_000
    } else if max_chase_ns > 8_000_000_000 {
        8_000_000_000
    } else {
        max_chase_ns
    };
    let macro_raw = c / 10;
    let hw_raw = (c / 1_000) * 440;
    let rep_raw = c / 2;
    let cool_raw = (c / 10) * 3;
    ElasticQueueTimersNs {
        macro_abort_ns: if macro_raw < 250_000_000 { 250_000_000 } else if macro_raw > 900_000_000 { 900_000_000 } else { macro_raw },
        headwind_abort_ns: if hw_raw < 1_200_000_000 { 1_200_000_000 } else if hw_raw > 3_500_000_000 { 3_500_000_000 } else { hw_raw },
        repeg_wait_ns: if rep_raw < 1_400_000_000 { 1_400_000_000 } else if rep_raw > 3_800_000_000 { 3_800_000_000 } else { rep_raw },
        repeg_cooldown_ns: if cool_raw < 800_000_000 { 800_000_000 } else if cool_raw > 2_400_000_000 { 2_400_000_000 } else { cool_raw },
    }
}

#[inline]
#[must_use]
pub const fn is_safe_halt_standby(halt_locked: bool, open_count: usize, pending_entry_count: usize, any_inflight: bool) -> bool {
    halt_locked && open_count == 0 && pending_entry_count == 0 && !any_inflight
}

#[inline]
#[must_use]
pub const fn compute_exit_sync_schedule_with_fees(
    quote: &CanonicalExitQuote,
    prev_resting_exit_px: PriceTicks,
    avg_price_ticks: PriceTicks,
    fees: FeeSchedule,
) -> (bool, i64) {
    if quote.urgent_close_repeg && prev_resting_exit_px.0 > 0 && prev_resting_exit_px.0 != quote.target_price.0 {
        return (true, 250_000_000);
    }
    if !quote.urgent_close_repeg && matches!(quote.sub_reason, ExitSubReason::FibonacciImpulseSurf) && prev_resting_exit_px.0 > 0 {
        let diff = (quote.target_price.0 - prev_resting_exit_px.0).abs();
        let denom = if avg_price_ticks.0 > 1 { avg_price_ticks.0 } else { 1 };
        let drift_bps = (diff.saturating_mul(1_000_000)) / denom;
        let m_eff = if fees.maker_fee_bps.0 > 100 { fees.maker_fee_bps.0 } else { 100 };
        if drift_bps < (m_eff.saturating_mul(3)) / 5 {
            return (false, 1_800_000_000);
        }
    }
    (false, 1_350_000_000)
}

#[inline]
#[must_use]
pub const fn compute_exit_sync_schedule(quote: &CanonicalExitQuote, prev_resting_exit_px: PriceTicks, avg_price_ticks: PriceTicks) -> (bool, i64) {
    compute_exit_sync_schedule_with_fees(quote, prev_resting_exit_px, avg_price_ticks, FeeSchedule::VIP0_DEFAULT)
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
    side: Side,
    entry_price: PriceTicks,
    at_l1: bool,
    live_l1_q: LotQty,
    prev_l1_q_obs: LotQty,
    prev_q_ahead: LotQty,
    tape_delta_lots: i64,
    last_trade_price: PriceTicks,
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
    spec: PairSpec,
    wallet_1e8: i64,
    px: PriceTicks,
    lots: LotQty,
    vol_5m: ScaledBps,
    fees: FeeSchedule,
) -> i64 {
    let notional_1e8 = spec.lot_notional_1e8(LotQty(if lots.0 > 0 { lots.0 } else { 1 }), PriceTicks(if px.0 > 0 { px.0 } else { 1 }));
    let eff_notional = if notional_1e8 > 1 { notional_1e8 } else { 1 };
    let raw_sl = spec.all_in_sl_loss_ppm_with_fees(px, vol_5m, fees);
    let sl_ppm = (if raw_sl > 1 { raw_sl } else { 1 }) as i128;
    let eq = (if wallet_1e8 > 1 { wallet_1e8 } else { 1 }) as i128;
    let ceil_ppm = (((eff_notional as i128).saturating_mul(sl_ppm) + eq - 1) / eq) as i64;
    if ceil_ppm < 1 { 1 } else if ceil_ppm > 14_400 { 14_400 } else { ceil_ppm }
}

#[inline]
#[must_use]
pub const fn slot_position_risk_ppm(spec: PairSpec, wallet_1e8: i64, px: PriceTicks, lots: LotQty, vol_5m: ScaledBps) -> i64 {
    slot_position_risk_ppm_with_fees(spec, wallet_1e8, px, lots, vol_5m, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
#[must_use]
pub const fn slot_btc_beta_x1000(slot_idx: usize, slot_vol_5m: ScaledBps, btc_vol_5m: i64) -> i64 {
    if slot_idx == 0 { return 1_000; }
    let denom = if btc_vol_5m > 2_500 { btc_vol_5m } else { 2_500 };
    let v_num = if slot_vol_5m.0 > 1_000 { slot_vol_5m.0 } else { 1_000 };
    let raw = v_num.saturating_mul(850) / denom;
    if raw < 500 { 500 } else if raw > 1_450 { 1_450 } else { raw }
}

/// V4.6 Island 1B: Builds dollar-weighted portfolio beta guard using live signed `EppsBtcCorrelationTracker` & `FeeSchedule`.
#[must_use]
pub fn build_monetary_beta_guard_with_epps(
    wallet_1e8: i64,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    epps_trackers: &[EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS],
    fees: FeeSchedule,
) -> MonetaryPortfolioBetaGuard {
    let mut guard = MonetaryPortfolioBetaGuard::new();
    let btc_vol = ScaledBps(cached_snaps[0].realized_vol_5m.0.max(2_500));
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let active_tuple = match engines[s_idx].state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, .. } => Some((side, entry_price, qty)),
            OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, open_lots, .. } => Some((side, avg_price_ticks, open_lots)),
        };
        if let Some((side, px, lots)) = active_tuple {
            let snap = &cached_snaps[s_idx];
            let risk_ppm = slot_position_risk_ppm_with_fees(APPROVED_SPECS[s_idx], wallet_1e8, px, lots, snap.realized_vol_5m, fees);
            let beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
            guard.register_slot_open(s_idx, side, risk_ppm, beta_x1000);
        }
    }
    guard
}

#[must_use]
pub fn build_monetary_beta_guard(
    wallet_1e8: i64,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> MonetaryPortfolioBetaGuard {
    let trackers = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS];
    build_monetary_beta_guard_with_epps(wallet_1e8, engines, cached_snaps, &trackers, FeeSchedule::VIP0_DEFAULT)
}

#[inline]
pub fn publish_slot_veto_code(arena: &VortexShmArena, slot_idx: usize, veto: LastVetoCode) {
    if let Ok(tel) = arena.telemetry(slot_idx) {
        let mut cur = tel.read_consistent_or_default(2);
        let code = veto as u8;
        if cur.last_veto_code != code {
            cur.last_veto_code = code;
            tel.write_consistent(&cur);
        }
    }
}

#[must_use]
pub fn select_safe_live_sl_ticks_with_fees(
    side: Side,
    avg_price_ticks: PriceTicks,
    verified_exchange_sl: PriceTicks,
    candidate_eff_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
    fees: FeeSchedule,
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

#[must_use]
pub fn select_safe_live_sl_ticks(
    side: Side,
    avg_price_ticks: PriceTicks,
    verified_exchange_sl: PriceTicks,
    candidate_eff_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> Option<PriceTicks> {
    select_safe_live_sl_ticks_with_fees(
        side, avg_price_ticks, verified_exchange_sl, candidate_eff_sl, snap, FeeSchedule::VIP0_DEFAULT,
    )
}

#[inline]
#[must_use]
pub fn elastic_same_side_stagger_bounds(tau_mkt_s: i64, vol_5m: ScaledBps, b3x_bps: ScaledBps, fee_rt_bps: i64) -> (i64, i64) {
    let tau_s = if tau_mkt_s > 0 { tau_mkt_s } else { 300 };
    let stagger_s = ((tau_s.saturating_mul(STAGGER_BAR_TO_FLOW_PM)) / 1_000).clamp(15, 180);
    let eff_rt = fee_rt_bps.max(100);
    let pad = (b3x_bps.raw().clamp(300, 1_500) / 30).clamp(25, 50).max((vol_5m.raw().clamp(0, 10_000) / 100).clamp(25, 100));
    (stagger_s.saturating_mul(1_000_000_000), eff_rt.saturating_add(pad))
}

#[must_use]
pub fn can_stage_same_direction_entry(
    candidate_slot: usize,
    side: Side,
    now_ns: TimestampNs,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> bool {
    for (s_idx, eng) in engines.iter().enumerate() {
        if s_idx == candidate_slot { continue; }
        match eng.state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side: s, .. }
            | OrderLifecycleState::RestingEntryQueue { side: s, .. } => {
                if s == side { return false; }
            }
            OrderLifecycleState::PositionOpenMakerExit { side: s, avg_price_ticks, opened_ts, .. } => {
                if s == side {
                    let snap = &cached_snaps[s_idx];
                    let b3x = APPROVED_SPECS[s_idx].three_x_friction_barrier_bps_dynamic(
                        avg_price_ticks.max(PriceTicks(1)), eng.learned_taker_prob_pm, eng.active_fees,
                    );
                    let rt_maker = eng.active_fees.round_trip_maker_exit_bps().raw();
                    let (stagger_ns, req_cushion_bps) =
                        elastic_same_side_stagger_bounds(eng.intrinsic_tau_mkt_s, snap.realized_vol_5m, b3x, rt_maker);
                    if now_ns.0.saturating_sub(opened_ts.0) < stagger_ns { return false; }
                    let ref_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else {
                        match s { Side::Bid => snap.bids[0].price.0, Side::Ask => snap.asks[0].price.0 }
                    };
                    let cur_gross_bps = ((ref_px - avg_price_ticks.0) * s.sign_i64()).saturating_mul(1_000_000) / avg_price_ticks.0.max(1);
                    if cur_gross_bps < req_cushion_bps && !eng.chandelier_profit_sl_armed { return false; }
                }
            }
        }
    }
    true
}

fn step_placing_entry_slot(
    s_idx: usize,
    side: Side,
    qty: LotQty,
    submitted_ts: TimestampNs,
    now_ns: TimestampNs,
    block_new_entries: bool,
    gateway: Option<&BybitAsyncGateway>,
    rt: &mut ExecRuntimeState,
    snap: &SymbolLobSnapshot,
    btc_snap: &SymbolLobSnapshot,
    entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
) -> usize {
    if rt.slot_rpc_inflight[s_idx] { return 1; }
    if block_new_entries {
        engines[s_idx].state = OrderLifecycleState::Idle;
        engines[s_idx].sync_to_shm_telemetry(arena);
        publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
        return 0;
    }
    engines[s_idx].sync_governor_dynamics(gov);
    if now_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m) {
        return 1;
    }
    let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_non_choking(s_idx, side, rt.wallet_1e8, snap, btc_snap) else {
        engines[s_idx].state = OrderLifecycleState::Idle;
        engines[s_idx].sync_to_shm_telemetry(arena);
        publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
        return 1;
    };
    let Ok((synced_entry_px, synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto(snap) else {
        let veto = if !passes_elliott_macro_coherence_guard(side, snap) {
            LastVetoCode::MacroElliottContradiction
        } else if !passes_order_block_wall_guard_dynamic(s_idx, side, snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees) {
            LastVetoCode::OpposingOrderBlockWall
        } else {
            LastVetoCode::PreflightDriftOrDecay
        };
        engines[s_idx].state = OrderLifecycleState::Idle;
        engines[s_idx].sync_to_shm_telemetry(arena);
        publish_slot_veto_code(arena, s_idx, veto);
        return 1;
    };
    let (q_l1, opp_cum) = match side {
        Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
        Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
    };
    let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, snap);
    let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
    let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
    entry_forensics[s_idx] = SlotForensicEntrySnapshot {
        armed_ts_ns: now_ns.0, init_q_lots: q_l1.0,
        ofi_in: snap.factor_ofi.get(), tfi_in: snap.factor_tfi.get(), geom_in: snap.factor_geom.get(),
        snr_in: OrthogonalFactorState::adaptive_snr_from_snapshot(snap, spoof_d).get(),
        vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
        spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
        btc_comp_in: OrthogonalFactorState::snr_composite_score(btc_snap),
        gov_ewma_bps: (ewma_b as f64) / 100.0, win_streak: win_st, sl_streak: sl_st,
        drift_bps: (dr_b as f64) / 100.0, ext_bps: (ex_b as f64) / 100.0,
        kin_accel_net_bps: (((ex_b - dr_b) * side.sign_i64()) as f64) / 100.0,
        kin_energy_in_bps: (e_kin as f64) / 100.0, req_kin_energy_in_bps: (e_min as f64) / 100.0,
    };
    rt.init_q_ahead[s_idx] = q_l1;
    rt.cur_q_ahead[s_idx] = q_l1;
    rt.last_l1_q_obs[s_idx] = q_l1;
    rt.last_opp_trades[s_idx] = opp_cum;
    if let Some(gw) = gateway {
        if gw.try_send(GatewayCommand::PlaceEntry { slot_idx: s_idx, side, entry_price: synced_entry_px, qty, sl_price: synced_sl_px }) {
            rt.slot_rpc_inflight[s_idx] = true;
        }
    } else {
        let _ = engines[s_idx].on_entry_order_resting(now_ns);
        arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
        engines[s_idx].sync_to_shm_telemetry(arena);
    }
    1
}

fn step_resting_queue_slot(
    s_idx: usize,
    side: Side,
    entry_price: PriceTicks,
    qty: LotQty,
    sl_trigger_price: PriceTicks,
    resting_since_ts: TimestampNs,
    now_ns: TimestampNs,
    block_new_entries: bool,
    gateway: Option<&BybitAsyncGateway>,
    rt: &mut ExecRuntimeState,
    last_rest_dispatch_ns: &mut i64,
    snap: &SymbolLobSnapshot,
    btc_snap: &SymbolLobSnapshot,
    entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
) {
    let spec = APPROVED_SPECS[s_idx];
    engines[s_idx].sync_governor_dynamics(gov);
    let (at_l1, live_l1_q, cur_opp_cum) = match side {
        Side::Bid => (snap.bids[0].price == entry_price, snap.bids[0].qty, snap.cum_sell_trade_lots),
        Side::Ask => (snap.asks[0].price == entry_price, snap.asks[0].qty, snap.cum_buy_trade_lots),
    };
    let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
    rt.last_opp_trades[s_idx] = cur_opp_cum;
    let (next_q, opp_hit, next_l1_obs) = step_l1_queue_ahead_lots(
        side, entry_price, at_l1, live_l1_q, rt.last_l1_q_obs[s_idx], rt.cur_q_ahead[s_idx], tape_delta, snap.last_trade_price,
    );
    rt.cur_q_ahead[s_idx] = next_q;
    rt.last_l1_q_obs[s_idx] = next_l1_obs;
    if let Ok(tel) = arena.telemetry(s_idx) { tel.store_queue_ahead_lots(rt.cur_q_ahead[s_idx]); }
    let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, snap, btc_snap, &rt.epps_trackers[s_idx]);
    let q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(
        side, rt.init_q_ahead[s_idx], rt.cur_q_ahead[s_idx], opp_hit, snap, btc_snap, epps_beta,
    );
    if rt.slot_rpc_inflight[s_idx] { return; }
    let resting_elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
    let total_chase_ns = if entry_forensics[s_idx].armed_ts_ns > 0 {
        now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns)
    } else { resting_elapsed_ns };
    let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
    let q_timers = elastic_resting_queue_timers_ns(max_chase_ns);
    let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
    let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(btc_snap) * side.sign_f64());
    let headwind_erosion = s_idx > 0
        && resting_elapsed_ns >= q_timers.headwind_abort_ns
        && rt.init_q_ahead[s_idx].0 > 0
        && rt.cur_q_ahead[s_idx].0 * 100 <= rt.init_q_ahead[s_idx].0 * 65
        && (btc_aligned < -0.02 || flow_imp < 0.38);
    let elliott_broken = resting_elapsed_ns >= q_timers.macro_abort_ns && !passes_elliott_macro_coherence_guard(side, snap);
    let ob_wall_broken = resting_elapsed_ns >= q_timers.macro_abort_ns
        && !passes_order_block_wall_guard_dynamic(s_idx, side, snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
    let dq_abort = (resting_elapsed_ns >= 25_000_000 && at_l1 && q_shield.abort_now)
        || headwind_erosion || elliott_broken || ob_wall_broken || flow_imp <= -0.42;
    if block_new_entries || resting_elapsed_ns >= (max_chase_ns * 3) / 4 || total_chase_ns >= max_chase_ns || dq_abort {
        let abort_veto = if block_new_entries { LastVetoCode::WarmupOrLock }
            else if ob_wall_broken { LastVetoCode::OpposingOrderBlockWall }
            else if elliott_broken { LastVetoCode::MacroElliottContradiction }
            else if headwind_erosion { LastVetoCode::BtcLeadLagHeadwind }
            else { LastVetoCode::HawkesStoikovQueueAbort };
        if let Some(gw) = gateway {
            if gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                rt.slot_rpc_inflight[s_idx] = true;
                rt.abort_cmd_inflight[s_idx] = true;
                rt.last_abort_veto[s_idx] = abort_veto;
            }
        } else {
            gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
            rt.markout_rings[s_idx].arm_abort_probe(side, entry_price, abort_veto, now_ns);
            engines[s_idx].state = OrderLifecycleState::Idle;
            arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
            engines[s_idx].sync_to_shm_telemetry(arena);
        }
        publish_slot_veto_code(arena, s_idx, abort_veto);
        return;
    }
    if !at_l1
        && resting_elapsed_ns >= q_timers.repeg_wait_ns
        && total_chase_ns + q_timers.repeg_wait_ns < max_chase_ns
        && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
        && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, snap, btc_snap, epps_beta)
        && passes_elliott_macro_coherence_guard(side, snap)
        && passes_order_block_wall_guard_dynamic(s_idx, side, snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees)
    {
        let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
        let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
        let moved_away = match side {
            Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks,
            Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks,
        };
        if moved_away && gov.confirm_preflight_kinematics_non_choking(s_idx, side, rt.wallet_1e8, snap, btc_snap).is_ok() {
            if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(snap, new_l1) {
                engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + q_timers.repeg_cooldown_ns;
                if let Some(gw) = gateway {
                    if gw.try_send(GatewayCommand::RepegEntry { slot_idx: s_idx, side, new_price: new_l1, qty, new_sl_price: repeg_sl }) {
                        rt.slot_rpc_inflight[s_idx] = true;
                        return;
                    }
                } else {
                    engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                    rt.init_q_ahead[s_idx] = live_l1_q;
                    rt.cur_q_ahead[s_idx] = live_l1_q;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    return;
                }
            }
        }
    }
    if let Some(gw) = gateway {
        if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
            *last_rest_dispatch_ns = now_ns.0;
            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
            if gw.try_send(GatewayCommand::PollEntryFill { slot_idx: s_idx, sl_hint: sl_trigger_price }) {
                rt.slot_rpc_inflight[s_idx] = true;
            }
        }
    } else {
        let bbo_crossed = match side {
            Side::Bid => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 <= entry_price.0,
            Side::Ask => snap.bids[0].price.0 >= entry_price.0,
        };
        if rt.cur_q_ahead[s_idx].0 <= 0 || bbo_crossed {
            gov.clear_abort_streak(s_idx, side);
            engines[s_idx].sync_governor_for_side(gov, side);
            if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None) {
                let mid_t0 = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                    PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
                } else { eff_px };
                rt.markout_rings[s_idx].arm_fill_probe(eff_s, eff_px, mid_t0, now_ns);
                record_and_notify_entry_fill(s_idx, spec, engines, entry_forensics, arena, tg_tx, vault_tx, now_ns, eff_s, eff_px, eff_q, "SHADOW MAKER FILL");
            }
        }
    }
}

fn step_open_position_slot(
    s_idx: usize,
    side: Side,
    avg_price_ticks: PriceTicks,
    mut peak_favorable_price: PriceTicks,
    mut worst_adverse_price: PriceTicks,
    open_lots: LotQty,
    tranche1_harvested: bool,
    resting_exit_price: PriceTicks,
    sl_trigger_price: PriceTicks,
    opened_ts: TimestampNs,
    now_ns: TimestampNs,
    gateway: Option<&BybitAsyncGateway>,
    rt: &mut ExecRuntimeState,
    last_rest_dispatch_ns: &mut i64,
    snap: &SymbolLobSnapshot,
    btc_snap: &SymbolLobSnapshot,
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
) -> usize {
    let spec = APPROVED_SPECS[s_idx];
    engines[s_idx].sync_governor_dynamics(gov);
    let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, snap, btc_snap, &rt.epps_trackers[s_idx]);
    if let Some(gw) = gateway {
        engines[s_idx].observe_open_flow_kinematics(snap);
        let _ = engines[s_idx].observe_wald_sprt_step_with_beta(snap, btc_snap, epps_beta);
        let cur_bbo = match side { Side::Bid => snap.asks[0].price, Side::Ask => snap.bids[0].price };
        let ref_px = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { cur_bbo };
        match side {
            Side::Bid => {
                if ref_px.0 > peak_favorable_price.0 { peak_favorable_price = ref_px; }
                if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 { worst_adverse_price = ref_px; }
            }
            Side::Ask => {
                if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 { peak_favorable_price = ref_px; }
                if ref_px.0 > worst_adverse_price.0 { worst_adverse_price = ref_px; }
            }
        }
        if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state {
            *pk = peak_favorable_price;
            *wr = worst_adverse_price;
        }
        let avg_denom = avg_price_ticks.0.max(1);
        engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_denom;
        engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_denom;
        engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
        engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
        if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
        if rt.emergency_flatten_sl[s_idx].is_none() {
            if let Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, .. }) =
                engines[s_idx].evaluate_sl_or_emergency_flatten_for_slot(side, avg_price_ticks, snap)
            {
                rt.emergency_flatten_sl[s_idx] = Some(backup_sl);
            }
        }
        if let Some(backup_sl) = rt.emergency_flatten_sl[s_idx] {
            if !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx]
                && gw.try_send(GatewayCommand::EmergencyFlattenSlot { slot_idx: s_idx, open_side: side, open_lots, backup_sl })
            {
                rt.slot_rpc_inflight[s_idx] = true;
                rt.sl_retry_after_ns[s_idx] = now_ns.0 + 500_000_000;
            }
            engines[s_idx].sync_to_shm_telemetry(arena);
            return 1;
        }
        let Some(quote) = engines[s_idx].compute_canonical_exit_quote(snap) else {
            engines[s_idx].sync_to_shm_telemetry(arena);
            return 1;
        };
        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
        engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
        if engines[s_idx].maybe_ratchet_chandelier_or_wald_sl(snap, btc_snap).is_some() {
            rt.sl_needs_sync[s_idx] = true;
        }
        if let OrderLifecycleState::PositionOpenMakerExit { active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p, active_exit_lots: ref mut ax_l, .. } = engines[s_idx].state {
            *ax_r = quote.role; *ax_p = quote.target_price; *ax_l = quote.order_lots;
        }
        if rt.sl_needs_sync[s_idx] && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
            if let Some(safe_sl) = select_safe_live_sl_ticks_with_fees(
                side, avg_price_ticks, rt.verified_exchange_sl[s_idx], engines[s_idx].last_eff_sl_ticks, snap, engines[s_idx].active_fees,
            ) {
                engines[s_idx].last_eff_sl_ticks = safe_sl;
                if gw.try_send(GatewayCommand::EnforceStopLoss { slot_idx: s_idx, sl_price: safe_sl }) {
                    rt.slot_rpc_inflight[s_idx] = true;
                    rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000;
                }
            } else {
                rt.sl_needs_sync[s_idx] = false;
            }
        }
        let (preempt_peg, next_sync_delay_ns) =
            compute_exit_sync_schedule_with_fees(&quote, resting_exit_price, avg_price_ticks, engines[s_idx].active_fees);
        if !rt.slot_rpc_inflight[s_idx]
            && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns)
            && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000
        {
            *last_rest_dispatch_ns = now_ns.0;
            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
            if gw.try_send(GatewayCommand::SyncOpenSlot {
                slot_idx: s_idx, side, target_px: quote.target_price, target_lots: quote.order_lots,
                defensive_lock: quote.urgent_close_repeg, cur_gross_bps: quote.cur_gross_bps,
                vac_level: quote.vac_level, opened_ts_ns: opened_ts.0, tranche1_harvested, fees: engines[s_idx].active_fees,
            }) {
                rt.slot_rpc_inflight[s_idx] = true;
            }
        }
        engines[s_idx].sync_to_shm_telemetry(arena);
        return 1;
    }
    let mut still_open = 1_usize;
    if let Some(summary) = engines[s_idx].step_open_position(snap, btc_snap, gov, ledger) {
        still_open = 0;
        gov.observe_closed_trade_at_entry(
            s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
            summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100, now_ns,
        );
        arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
        let mk_curve = rt.markout_rings[s_idx].last_fill_curve();
        let lvr_5s = rt.markout_rings[s_idx].ewma_lvr_5s_bps();
        dispatch_closed_trade_logs_v46(
            s_idx, &summary, &engines[s_idx], &entry_forensics[s_idx], snap, ledger, gov,
            &mk_curve, lvr_5s, vault_tx, &mut rt.halt_locked, now_ns,
        );
        dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
        let _ = tg_tx.try_send(format_telegram_close_message(
            spec, &summary, engines[s_idx].last_exit_sub_reason, ledger.closed_trades_count, ledger,
        ));
    }
    engines[s_idx].sync_to_shm_telemetry(arena);
    still_open
}

pub fn step_12_slot_daemon_tick(
    now_ns: TimestampNs,
    gateway: Option<&BybitAsyncGateway>,
    rt: &mut ExecRuntimeState,
    last_rest_dispatch_ns: &mut i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
) -> (usize, usize) {
    let block_new_entries = rt.drain_active || rt.halt_locked;
    let btc_snap = cached_snaps[0];
    let (mut open_count, mut pending_entry_count, mut transitioned_mask) = (0_usize, 0_usize, 0_u16);

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) {
            transitioned_mask |= 1_u16 << s_idx;
        }
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                pending_entry_count += step_placing_entry_slot(
                    s_idx, side, qty, submitted_ts, now_ns, block_new_entries, gateway, rt, &snap,
                    &btc_snap, entry_forensics, engines, gov, arena,
                );
            }
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, resting_since_ts, .. } => {
                pending_entry_count += 1;
                step_resting_queue_slot(
                    s_idx, side, entry_price, qty, sl_trigger_price, resting_since_ts, now_ns,
                    block_new_entries, gateway, rt, last_rest_dispatch_ns, &snap, &btc_snap,
                    entry_forensics, engines, gov, arena, tg_tx, vault_tx,
                );
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side, avg_price_ticks, peak_favorable_price, worst_adverse_price, open_lots,
                tranche1_harvested, resting_exit_price, sl_trigger_price, opened_ts, ..
            } => {
                open_count += step_open_position_slot(
                    s_idx, side, avg_price_ticks, peak_favorable_price, worst_adverse_price,
                    open_lots, tranche1_harvested, resting_exit_price, sl_trigger_price, opened_ts,
                    now_ns, gateway, rt, last_rest_dispatch_ns, &snap, &btc_snap, entry_forensics,
                    engines, gov, ledger, arena, tg_tx, vault_tx,
                );
            }
        }
    }

    if block_new_entries {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                && cached_snaps[s_idx].sequence_id > 0 && (transitioned_mask & (1_u16 << s_idx)) == 0
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
            }
        }
        return (open_count, pending_entry_count);
    }

    let fees = gov.active_fee_schedule();
    let mut beta_guard = build_monetary_beta_guard_with_epps(rt.wallet_1e8, engines, cached_snaps, &rt.epps_trackers, fees);
    let (_, _, active_slots) = beta_guard.open_counts();
    if active_slots >= 6 {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                && cached_snaps[s_idx].sequence_id > 0 && (transitioned_mask & (1_u16 << s_idx)) == 0
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger);
            }
        }
        return (open_count, pending_entry_count);
    }

    let btc_vol = ScaledBps(btc_snap.realized_vol_5m.0.max(2_500));
    let mut armed_this_tick = 0_usize;
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let (_, _, cur_tot) = beta_guard.open_counts();
        if cur_tot >= 6 || armed_this_tick >= 1 { break; }
        if !matches!(engines[s_idx].state, OrderLifecycleState::Idle)
            || rt.slot_rpc_inflight[s_idx] || (transitioned_mask & (1_u16 << s_idx)) != 0
        {
            continue;
        }
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
        let comp_raw = OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get();
        let probe_side = if comp_raw >= 0.0 { Side::Bid } else { Side::Ask };
        let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
        let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
        let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(
            spec, &snap, &btc_snap, ScaledBps(engines[s_idx].learned_mfe_bps),
            ScaledBps(probe_edge), taker_pm, fees, spoof_d, epps_beta,
        );
        if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
            let side_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
            if !passes_elliott_macro_coherence_guard(side, &snap) {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction);
                continue;
            }
            if !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, side_taker_pm, fees) {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall);
                continue;
            }
            let cand_lots = gov.compute_slot_entry_lots(s_idx, side, rt.wallet_1e8, &snap);
            let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
            let cand_risk_ppm = slot_position_risk_ppm_with_fees(spec, rt.wallet_1e8, cand_px, cand_lots, snap.realized_vol_5m, fees);
            let cand_beta = rt.epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
            if !beta_guard.can_open_with_risk_ppm(s_idx, side, cand_risk_ppm, cand_beta)
                || !can_stage_same_direction_entry(s_idx, side, now_ns, engines, cached_snaps)
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger);
                continue;
            }
            if !gov.authorizes_adaptive_entry_with_wallet(s_idx, side, rt.wallet_1e8, &snap, &btc_snap) {
                let (_, sl_st, _) = gov.slot_direction_diagnostics(s_idx, side);
                let btc_signed = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                let g_veto = if gov.is_in_scar_debt(s_idx, side) || sl_st > 0 || now_ns.0 < engines[s_idx].cooldown_until_ns.0 {
                    LastVetoCode::ScarCooldownOrContagion
                } else if s_idx > 0 && btc_signed < -0.15 {
                    LastVetoCode::BtcLeadLagHeadwind
                } else {
                    LastVetoCode::KinematicSurfaceOrCadence
                };
                publish_slot_veto_code(arena, s_idx, g_veto);
                continue;
            }
            engines[s_idx].sync_governor_for_side(gov, side);
            if engines[s_idx].try_arm_maker_entry(&snap, verdict, cand_lots).is_ok()
                && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
            {
                beta_guard.register_slot_open(s_idx, side, cand_risk_ppm, cand_beta);
                engines[s_idx].sync_to_shm_telemetry(arena);
                pending_entry_count += 1;
                armed_this_tick += 1;
            } else {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
            }
        } else {
            let mid_px = PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0));
            let btc_signed = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * probe_side.sign_f64());
            let s_veto = if !snap.is_eligible || !spec.validate_pair_eligibility_dynamic(mid_px, snap.realized_vol_5m, taker_pm, fees) {
                LastVetoCode::Friction3xOrEligibility
            } else if snap.wave_climax_active || snap.factor_geom.get().abs() >= 0.94 || matches!(verdict, SignalVerdict::SpoofingVeto) {
                LastVetoCode::SpoofingVeto
            } else if !passes_elliott_macro_coherence_guard(probe_side, &snap) {
                LastVetoCode::MacroElliottContradiction
            } else if !passes_order_block_wall_guard_dynamic(s_idx, probe_side, &snap, taker_pm, fees) {
                LastVetoCode::OpposingOrderBlockWall
            } else if s_idx > 0 && btc_signed < -0.18 {
                LastVetoCode::BtcLeadLagHeadwind
            } else {
                LastVetoCode::KinematicSurfaceOrCadence
            };
            publish_slot_veto_code(arena, s_idx, s_veto);
        }
    }
    (open_count, pending_entry_count)
}

pub fn run_autonomous_12_slot_daemon(arena: &VortexShmArena, live_capital: bool) -> ExitCode {
    let tg_tx = spawn_bounded_telegram_notifier();
    let vault_tx = spawn_bounded_vault_persister();
    let lock_watch = Arc::new(AtomicLockWatch::new(Path::new(HALT_LOCK_PATH).exists(), Path::new(DRAIN_LOCK_PATH).exists()));
    let vault_io_tx = spawn_vault_io_worker(Arc::clone(&lock_watch));
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut rt = ExecRuntimeState::new();
    rt.halt_locked = lock_watch.is_halt_locked();
    rt.drain_active = lock_watch.is_drain_active();
    let mut cached_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut entry_forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut last_seq = [0_u64; MAX_ACTIVE_PAIRS];
    let (mut last_wallet_sync_ns, mut last_rest_dispatch_ns) = (0_i64, 0_i64);

    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);
    let saved_mfe = load_mfe_plasticity_from_path(Path::new(MFE_PLASTICITY_VAULT_PATH), engines[0].learned_mfe_bps);
    for s_idx in 0..MAX_ACTIVE_PAIRS { engines[s_idx].learned_mfe_bps = saved_mfe[s_idx]; }
    let boot_ns = now_epoch_ns();
    let handshake = run_daemon_startup_handshake(live_capital, boot_ns);
    gov.set_active_fee_schedule(handshake.fee_schedule);
    ledger.set_live_vip_fee_rates(handshake.fee_schedule.maker_fee_bps, handshake.fee_schedule.taker_fee_bps);
    for eng in &mut engines { eng.sync_governor_dynamics(&gov); }

    let prewarmed_slots = gov.load_ema_prewarm_file(boot_ns);
    if live_capital {
        let warmup_ns = if prewarmed_slots >= MAX_ACTIVE_PAIRS { 15_000_000_000_i64 } else { 75_000_000_000_i64 };
        gov.set_entry_warmup_until_ns(boot_ns.0.saturating_add(warmup_ns));
        println!(
            "{}\u{1f7e2} [EMA PREWARM V4.6] Hydrated {}/12 slots | Fees M/T: {}/{} | L2 stabilize: {}s{}",
            ANSI_GREEN, prewarmed_slots, handshake.fee_schedule.maker_fee_bps.raw(),
            handshake.fee_schedule.taker_fee_bps.raw(), warmup_ns / 1_000_000_000, ANSI_RESET
        );
    }
    bootstrap_live_adopt_if_enabled(live_capital, arena, &gov, &mut rt, &mut entry_forensics, &mut engines);
    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);
    println!(
        "{}\u{1f7e2} [VORTEX-OMEGA V4.6 200.0 Hz ARMED] Epps 5s + Live Markout Ring + VIP Fees + Zero-Jitter SHM v15.{}",
        ANSI_GREEN, ANSI_RESET
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();
        rt.halt_locked = rt.halt_locked || lock_watch.is_halt_locked();
        rt.drain_active = lock_watch.is_drain_active();
        let mkr_fee_bps = handshake.fee_schedule.maker_fee_bps.raw();

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot_ref) = arena.slot(s_idx) {
                if let Ok(mut snap) = slot_ref.read_snapshot(4) {
                    if !handshake.instruments.is_slot_tradable(s_idx) { snap.is_eligible = false; }
                    cached_snaps[s_idx] = snap;
                    if snap.sequence_id != last_seq[s_idx] {
                        last_seq[s_idx] = snap.sequence_id;
                        let _ = rt.spoof_clocks[s_idx].step_event_clock_and_spoof(snap.ts_local_ns, snap.factor_ofi, snap.factor_tfi);
                        let mid_own = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
                        let btc_s = cached_snaps[0];
                        let mid_btc = PriceTicks(i64::midpoint(btc_s.bids[0].price.0.max(1), btc_s.asks[0].price.0.max(1)));
                        rt.epps_trackers[s_idx].observe_5s_pair(snap.ts_local_ns, s_idx == 0, mid_own, mid_btc);
                        rt.markout_rings[s_idx].step_clock(mid_own, snap.ts_local_ns, mkr_fee_bps);
                        gov.observe_market_price(s_idx, &snap);
                        gov.step_regime_shift_shadow_rehab(s_idx, &snap);
                    }
                }
            }
        }
        if let Some(ref gw) = gateway {
            while let Some(evt) = gw.try_recv() {
                handle_gateway_event(
                    evt, now_ns, Some(gw), &mut rt, &cached_snaps, &entry_forensics,
                    &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
                );
            }
        }
        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            gov.dispatch_ema_prewarm_async(now_ns, &vault_io_tx);
            let mfe_arr = SlotExecutionEngine::extract_12_slot_mfe_array(&engines);
            let _ = vault_io_tx.try_send(VaultIoCommand::SaveMfePlasticity { ts_ns: now_ns.0, learned_mfe_bps: mfe_arr });
            if let Some(ref gw) = gateway { let _ = gw.try_send(GatewayCommand::SyncWallet); }
        }
        let prev_halt = rt.halt_locked;
        let (open_count, pending_entry_count) = step_12_slot_daemon_tick(
            now_ns, gateway.as_ref(), &mut rt, &mut last_rest_dispatch_ns, &cached_snaps,
            &mut entry_forensics, &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );
        if !prev_halt && rt.halt_locked { lock_watch.latch_halt_in_ram(); }
        let any_inflight = rt.slot_rpc_inflight.iter().any(|&b| b);
        if rt.drain_active && open_count == 0 && pending_entry_count == 0 && !any_inflight {
            let _ = tg_tx.try_send("\u{1f7e2} [VORTEX-OMEGA V4.6 DRAIN COMPLETE] 0 open positions verified (Wallet == Equity).".to_string());
            let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
            if vault_tx.send(VaultPersistTask::FlushSync(ack_tx)).is_ok() {
                let _ = ack_rx.recv_timeout(Duration::from_secs(2));
            }
            let (io_ack_tx, io_ack_rx) = mpsc::sync_channel::<()>(1);
            if vault_io_tx.send(VaultIoCommand::FlushBarrier(io_ack_tx)).is_ok() {
                let _ = io_ack_rx.recv_timeout(Duration::from_secs(2));
            }
            thread::sleep(Duration::from_millis(120));
            println!("{ANSI_GREEN}\u{1f7e2} [DRAIN_COMPLETE_STOPPED] 0 open positions verified. Vault flushed.{ANSI_RESET}");
            return ExitCode::SUCCESS;
        }
        if is_safe_halt_standby(rt.halt_locked, open_count, pending_entry_count, any_inflight) {
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
            Err(_) => println!("{ANSI_GRAY}\u{26aa} [VORTEX-OMEGA V4.6 LIVE HUD] SHM inactive ({}) \u{2014} daemons stopped.{ANSI_RESET}", shm_path.display()),
        }
        return ExitCode::SUCCESS;
    }
    if args.iter().any(|a| a == "--trades") {
        render_trades_and_executive_dashboard();
        return ExitCode::SUCCESS;
    }
    let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
    let sep = "=".repeat(80);
    println!("{sep}");
    println!("[VORTEX-EXEC V4.6] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
    println!("{sep}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> OOS Closed Trades Verified : {oos_trades} (Gate >= 500){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Maker Exit Ratio           : {maker_ratio:.2}% (Gate >= 80.00%){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Net Profit Factor (PostFee): {pf:.2} (Gate >= 1.35){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Cumulative Net PnL (OOS)   : + {cum_usdt:.4} USDT | PARITY_SYNC Drift: {drift} units{ANSI_RESET}");
    println!("{sep}");
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
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_v46_daemon_epps_monetary_beta_elastic_queue_timers_and_abort_markout_probe() {
        assert!(!is_safe_halt_standby(true, 1, 0, false) && is_safe_halt_standby(true, 0, 0, false));
        let t_canon = elastic_resting_queue_timers_ns(5_000_000_000);
        assert_eq!((t_canon.macro_abort_ns, t_canon.headwind_abort_ns, t_canon.repeg_wait_ns, t_canon.repeg_cooldown_ns),
            (500_000_000, 2_200_000_000, 2_500_000_000, 1_500_000_000));
        let t_fast = elastic_resting_queue_timers_ns(3_200_000_000);
        assert!(t_fast.repeg_wait_ns < t_canon.repeg_wait_ns && t_fast.macro_abort_ns < t_canon.macro_abort_ns);

        let shm_path = MappedShmRegion::ephemeral_self_test_path("v46_daemon_epps_mk");
        if let Ok(r) = MappedShmRegion::open_or_create(&shm_path, true) {
            let mut engines = SlotExecutionEngine::new_12_slot_array();
            let mut gov = OnlineAdaptiveGovernor::new();
            let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
            let mut rt = ExecRuntimeState::new();
            let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
            snaps[0].symbol_id = APPROVED_SPECS[0].symbol_id;
            snaps[0].sequence_id = 1;
            snaps[0].realized_vol_5m = ScaledBps(6_000);
            snaps[0].bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(400) };
            snaps[0].asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
            snaps[3].symbol_id = APPROVED_SPECS[3].symbol_id;
            snaps[3].sequence_id = 10;
            snaps[3].ts_local_ns = TimestampNs(1_000_000_000);
            snaps[3].realized_vol_5m = ScaledBps(7_500);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(500) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(380) };
            snaps[3].mark_price = PriceTicks(8_200);
            snaps[3].last_trade_price = PriceTicks(8_200);
            snaps[3].factor_ofi = BoundedScore::new_clamped(0.45);
            snaps[3].factor_tfi = BoundedScore::new_clamped(0.48);
            let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(680) };
            assert!(engines[3].try_arm_maker_entry(&snaps[3], v, LotQty(26)).is_ok());
            assert!(engines[3].on_entry_order_resting(TimestampNs(1_000_000_000)).is_ok());
            rt.init_q_ahead[3] = LotQty(500);
            rt.cur_q_ahead[3] = LotQty(500);
            rt.last_l1_q_obs[3] = LotQty(500);
            let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
            let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultPersistTask>(16);
            let mut forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
            forensics[3].armed_ts_ns = 1_000_000_000;
            let mut last_rest = 0_i64;
            snaps[3].ts_local_ns = TimestampNs(1_050_000_000);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(60) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(450) };
            snaps[3].deep_bid_lots_l1_l5 = LotQty(200);
            snaps[3].deep_ask_lots_l1_l5 = LotQty(1_400);
            snaps[3].cum_sell_trade_lots = LotQty(440);
            let _ = step_12_slot_daemon_tick(
                TimestampNs(1_050_000_000), None, &mut rt, &mut last_rest, &snaps,
                &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert_eq!(engines[3].state, OrderLifecycleState::Idle);
            rt.markout_rings[3].step_clock(PriceTicks(8_170), TimestampNs(17_000_000_000), 200);
            assert_eq!(rt.markout_rings[3].true_positive_aborts(), (1, 1));
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
