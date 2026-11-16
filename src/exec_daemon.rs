//! 200Hz Zero-Jitter 12-Slot Execution Daemon (V4.8: Elastic Daily Shield, Quantum EV, Toxic Unwind & 8h Funding).
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
use crate::exec_gateway_handlers::{
    bootstrap_live_adopt_if_enabled, handle_gateway_event, run_oos_520_certification_suite, ExecRuntimeState,
};
use crate::execution_fsm::{
    passes_elliott_macro_coherence_guard, passes_order_block_wall_guard_dynamic,
    passes_stoikov_repeg_coherence_guard_with_beta, CanonicalExitQuote, ExitSubReason,
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
    rehydrate_daily_governor_from_vault, rehydrate_shadow_ledger, render_live_hud,
    render_trades_and_executive_dashboard, spawn_bounded_telegram_notifier, SlotForensicEntrySnapshot,
    DEFAULT_WALLET_EQUITY_1E8,
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

#[must_use]
pub fn can_stage_same_direction_entry(
    candidate_slot: usize, side: Side, now_ns: TimestampNs,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS], cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> bool {
    for (s_idx, eng) in engines.iter().enumerate() {
        if s_idx == candidate_slot { continue; }
        match eng.state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side: s, .. } | OrderLifecycleState::RestingEntryQueue { side: s, .. } => {
                if s == side { return false; }
            }
            OrderLifecycleState::PositionOpenMakerExit { side: s, avg_price_ticks, opened_ts, .. } => {
                if s == side {
                    let snap = &cached_snaps[s_idx];
                    let b3x = APPROVED_SPECS[s_idx].three_x_friction_barrier_bps_dynamic(avg_price_ticks.max(PriceTicks(1)), eng.learned_taker_prob_pm, eng.active_fees);
                    let (stagger_ns, req_cushion_bps) = elastic_same_side_stagger_bounds(eng.intrinsic_tau_mkt_s, snap.realized_vol_5m, b3x, eng.active_fees.round_trip_maker_exit_bps().raw());
                    if now_ns.0.saturating_sub(opened_ts.0) < stagger_ns { return false; }
                    let ref_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { match s { Side::Bid => snap.bids[0].price.0, Side::Ask => snap.asks[0].price.0 } };
                    let cur_gross_bps = ((ref_px - avg_price_ticks.0) * s.sign_i64()).saturating_mul(1_000_000) / avg_price_ticks.0.max(1);
                    if cur_gross_bps < req_cushion_bps && !eng.chandelier_profit_sl_armed { return false; }
                }
            }
        }
    }
    true
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

pub fn step_12_slot_daemon_tick(
    now_ns: TimestampNs, gateway: Option<&BybitAsyncGateway>, instruments: InstrumentSpecVerification,
    lock_watch: Option<&AtomicLockWatch>, rt: &mut ExecRuntimeState, last_rest_dispatch_ns: &mut i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS], entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS], gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger, arena: &VortexShmArena, tg_tx: &SyncSender<String>, vault_tx: &SyncSender<VaultIoCommand>,
) -> (usize, usize) {
    let (mut open_count, mut pending_entry_count) = (0_usize, 0_usize);
    let btc_snap = cached_snaps[0];
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
        if rt.daily_gov.is_hibernating() || prev_phase.label().starts_with("SLEEP") {
            let _ = tg_tx.try_send(format!(
                "\u{1f6e1}\u{fe0f} [VORTEX-OMEGA V4.8 DAILY SHIELD] Transicion UTC: {} -> {} | Hoy: {:+.4} USDT ({:+.2}%)",
                prev_phase.label(), cur_phase.label(),
                (rt.daily_gov.day_realized_pnl_1e8 as f64) / 100_000_000.0,
                (rt.daily_gov.day_pnl_ppm() as f64) / 10_000.0
            ));
        }
    }
    let block_new_entries = rt.halt_locked || rt.drain_active || rt.daily_gov.is_hibernating();

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
        let evt_dil = rt.spoof_clocks[s_idx].event_clock_dilation();
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                if rt.slot_rpc_inflight[s_idx] { pending_entry_count += 1; continue; }
                if block_new_entries || !instruments.is_slot_tradable(s_idx) {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    continue;
                }
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if now_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m) { continue; }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let Ok((synced_entry_px, synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto(&snap) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let (q_l1, opp_cum) = match side {
                    Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                    armed_ts_ns: now_ns.0, init_q_lots: q_l1.0,
                    ofi_in: snap.factor_ofi.get(), tfi_in: snap.factor_tfi.get(), geom_in: snap.factor_geom.get(),
                    snr_in: OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get(),
                    vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                    spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                    btc_comp_in: OrthogonalFactorState::snr_composite_score(&btc_snap),
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
            }
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, resting_since_ts, .. } => {
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                let (at_l1, live_l1_q, cur_opp_cum) = match side {
                    Side::Bid => (snap.bids[0].price == entry_price, snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].price == entry_price, snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
                rt.last_opp_trades[s_idx] = cur_opp_cum;
                let (next_q, opp_hit, next_obs) = step_l1_queue_ahead_lots(side, entry_price, at_l1, live_l1_q, rt.last_l1_q_obs[s_idx], rt.cur_q_ahead[s_idx], tape_delta, snap.last_trade_price);
                rt.cur_q_ahead[s_idx] = next_q;
                rt.last_l1_q_obs[s_idx] = next_obs;
                if let Ok(tel) = arena.telemetry(s_idx) { tel.store_queue_ahead_lots(next_q); }
                let q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(side, rt.init_q_ahead[s_idx], next_q, opp_hit, &snap, &btc_snap, epps_beta);
                if rt.slot_rpc_inflight[s_idx] { continue; }
                let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                let total_chase_ns = now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns);
                let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
                let q_timers = elastic_resting_queue_timers_ns(max_chase_ns);
                let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
                let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                let q_eroded = rt.init_q_ahead[s_idx].0 > 0 && next_q.0 * 100 <= rt.init_q_ahead[s_idx].0 * 65;
                let headwind_erosion = s_idx > 0 && elapsed_ns >= q_timers.headwind_abort_ns && q_eroded && (btc_aligned < -0.02 || flow_imp < 0.38);
                let macro_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_elliott_macro_coherence_guard(side, &snap);
                let ob_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                if block_new_entries || !instruments.is_slot_tradable(s_idx) || elapsed_ns >= (max_chase_ns * 3) / 4 || total_chase_ns >= max_chase_ns || (elapsed_ns >= 25_000_000 && q_shield.abort_now) || headwind_erosion || macro_bad || ob_bad || flow_imp <= -0.42 {
                    let abort_veto = if block_new_entries { LastVetoCode::WarmupOrLock } else if macro_bad { LastVetoCode::MacroElliottContradiction } else if ob_bad { LastVetoCode::OpposingOrderBlockWall } else if headwind_erosion { LastVetoCode::BtcLeadLagHeadwind } else { LastVetoCode::HawkesStoikovQueueAbort };
                    rt.last_abort_veto[s_idx] = abort_veto;
                    publish_slot_veto_code(arena, s_idx, abort_veto);
                    if let Some(gw) = gateway {
                        if !rt.abort_cmd_inflight[s_idx] && gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.abort_cmd_inflight[s_idx] = true;
                        }
                    } else {
                        gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                        rt.markout_rings[s_idx].arm_abort_probe(side, entry_price, abort_veto, now_ns);
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                    continue;
                }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                if !at_l1 && elapsed_ns >= q_timers.repeg_wait_ns && total_chase_ns + q_timers.repeg_wait_ns < max_chase_ns && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, &snap, &btc_snap, epps_beta) && passes_elliott_macro_coherence_guard(side, &snap) && passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees) {
                    let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                    let moved_away = match side { Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks, Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks };
                    if moved_away && gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                        if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&snap, new_l1) {
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + q_timers.repeg_cooldown_ns;
                            if let Some(gw) = gateway {
                                if gw.try_send(GatewayCommand::RepegEntry { slot_idx: s_idx, side, new_price: new_l1, qty, new_sl_price: repeg_sl }) { rt.slot_rpc_inflight[s_idx] = true; continue; }
                            } else {
                                engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                continue;
                            }
                        }
                    }
                }
                if let Some(gw) = gateway {
                    if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                        *last_rest_dispatch_ns = now_ns.0;
                        engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
                        if gw.try_send(GatewayCommand::PollEntryFill { slot_idx: s_idx, sl_hint: sl_trigger_price }) { rt.slot_rpc_inflight[s_idx] = true; }
                    }
                } else {
                    let bbo_crossed = match side { Side::Bid => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 <= entry_price.0, Side::Ask => snap.bids[0].price.0 >= entry_price.0 };
                    if next_q.0 <= 0 || bbo_crossed {
                        gov.clear_abort_streak(s_idx, side);
                        engines[s_idx].sync_governor_for_side(gov, side);
                        if let Some((eff_s, eff_px, eff_q)) = crate::exec_gateway_handlers::promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None) {
                            let mid_now = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
                            rt.markout_rings[s_idx].arm_fill_probe(eff_s, eff_px, mid_now, now_ns);
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
                    match side {
                        Side::Bid => { if ref_px.0 > peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                        Side::Ask => { if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                    }
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state { *pk = peak_favorable_price; *wr = worst_adverse_price; }
                    let (d_sign, denom) = (side.sign_i64(), avg_price_ticks.0.max(1));
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v48(&snap, &btc_snap, epps_beta, &rt.markout_rings[s_idx], evt_dil);
                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let eff_sl_now = engines[s_idx].last_eff_sl_ticks;
                    let unverified_sl_breach = rt.verified_exchange_sl[s_idx].0 <= 0 && match side { Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= eff_sl_now.0, Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= eff_sl_now.0 };
                    if unverified_sl_breach && !rt.slot_rpc_inflight[s_idx] {
                        if gw.try_send(GatewayCommand::EmergencyFlattenSlot { slot_idx: s_idx, open_side: side, open_lots, backup_sl: eff_sl_now }) { rt.slot_rpc_inflight[s_idx] = true; }
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    if let Some(new_sl) = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&snap, &btc_snap, epps_beta, zeta_tox) {
                        if new_sl != rt.verified_exchange_sl[s_idx] { rt.sl_needs_sync[s_idx] = true; }
                    }
                    if rt.sl_needs_sync[s_idx] && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
                        if let Some(safe_sl) = select_safe_live_sl_ticks_with_fees(side, avg_price_ticks, rt.verified_exchange_sl[s_idx], engines[s_idx].last_eff_sl_ticks, &snap, engines[s_idx].active_fees) {
                            engines[s_idx].last_eff_sl_ticks = safe_sl;
                            if gw.try_send(GatewayCommand::EnforceStopLoss { slot_idx: s_idx, sl_price: safe_sl }) { rt.slot_rpc_inflight[s_idx] = true; rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000; }
                        } else { rt.sl_needs_sync[s_idx] = false; }
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap) {
                        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                        engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                        engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                        let (preempt_peg, next_sync_delay_ns) = compute_exit_sync_schedule_with_fees(&quote, resting_exit_price, avg_price_ticks, engines[s_idx].active_fees);
                        if !rt.slot_rpc_inflight[s_idx] && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns) && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                            *last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
                            if gw.try_send(GatewayCommand::SyncOpenSlot { slot_idx: s_idx, side, target_px: quote.target_price, target_lots: quote.order_lots, defensive_lock: quote.urgent_close_repeg, cur_gross_bps: quote.cur_gross_bps, vac_level: quote.vac_level, opened_ts_ns: opened_ts.0, tranche1_harvested, fees: engines[s_idx].active_fees }) {
                                rt.slot_rpc_inflight[s_idx] = true;
                            }
                        }
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }
                if let Some(summary) = engines[s_idx].step_open_position_v48(&snap, &btc_snap, gov, ledger, epps_beta, &rt.markout_rings[s_idx], evt_dil) {
                    open_count = open_count.saturating_sub(1);
                    let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                    gov.observe_closed_trade_at_entry(s_idx, summary.side, summary.entry_price, summary.net_scaled_bps, non_punitive, now_ns);
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
                }
                engines[s_idx].sync_to_shm_telemetry(arena);
            }
        }
    }

    let mut beta_guard = build_monetary_beta_guard_with_epps(rt.wallet_1e8, engines, cached_snaps, &rt.epps_trackers, fees);
    if !block_new_entries && (open_count + pending_entry_count) < 6 {
        let mut armed_this_tick = 0_usize;
        let btc_vol = ScaledBps(btc_snap.realized_vol_5m.0.max(2_500));
        let daily_damper_pm = rt.daily_gov.kelly_boundary_damper_permille(
            ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
            gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
        );
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if (open_count + pending_entry_count) >= 6 || armed_this_tick >= 1 { break; }
            if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) || rt.slot_rpc_inflight[s_idx] { continue; }
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 { continue; }
            if !instruments.is_slot_tradable(s_idx) { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); continue; }
            let spec = APPROVED_SPECS[s_idx];
            let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
            let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
            let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
            let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
            let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(spec, &snap, &btc_snap, mfe_s, ScaledBps(probe_edge), taker_pm, fees, spoof_d, epps_beta);
            match verdict {
                SignalVerdict::SpoofingVeto => publish_slot_veto_code(arena, s_idx, LastVetoCode::SpoofingVeto),
                SignalVerdict::NeutralNoAction => {
                    if rt.last_abort_veto[s_idx] == LastVetoCode::None { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); }
                }
                SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                    let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                    let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    if !spec.passes_quantum_ev_growth_gate(rt.wallet_1e8, cand_px, snap.realized_vol_5m, mfe_s, s_taker_pm, fees) {
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
                        continue;
                    }
                    if !passes_elliott_macro_coherence_guard(side, &snap) { publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction); continue; }
                    if !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, s_taker_pm, fees) { publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall); continue; }
                    if !can_stage_same_direction_entry(s_idx, side, now_ns, engines, cached_snaps) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    let lots = gov.compute_slot_entry_lots_v48(s_idx, side, rt.wallet_1e8, &snap, daily_damper_pm);
                    let risk_ppm = slot_position_risk_ppm_with_fees(spec, rt.wallet_1e8, cand_px, lots, snap.realized_vol_5m, fees);
                    let beta_x1000 = rt.epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                    if !beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    if !gov.authorizes_adaptive_entry_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) {
                        let veto_c = if gov.audit_historical_entry_veto(s_idx, side, cand_px.0, snap.ts_local_ns.0).is_some() { LastVetoCode::ScarCooldownOrContagion } else { LastVetoCode::KinematicSurfaceOrCadence };
                        publish_slot_veto_code(arena, s_idx, veto_c);
                        continue;
                    }
                    engines[s_idx].sync_governor_for_side(gov, side);
                    if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_ok() && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. }) {
                        rt.last_abort_veto[s_idx] = LastVetoCode::None;
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::None);
                        beta_guard.register_slot_open(s_idx, side, risk_ppm, beta_x1000);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        pending_entry_count += 1;
                        armed_this_tick += 1;
                    }
                }
            }
        }
    } else if block_new_entries {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle) { publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock); }
        }
    }
    (open_count, pending_entry_count)
}

pub fn step_12_slot_daemon_tick(
    now_ns: TimestampNs, gateway: Option<&BybitAsyncGateway>, instruments: InstrumentSpecVerification,
    lock_watch: Option<&AtomicLockWatch>, rt: &mut ExecRuntimeState, last_rest_dispatch_ns: &mut i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS], entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS], gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger, arena: &VortexShmArena, tg_tx: &SyncSender<String>, vault_tx: &SyncSender<VaultIoCommand>,
) -> (usize, usize) {
    let (mut open_count, mut pending_entry_count) = (0_usize, 0_usize);
    let btc_snap = cached_snaps[0];
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
        if rt.daily_gov.is_hibernating() || prev_phase.label().starts_with("SLEEP") {
            let _ = tg_tx.try_send(format!(
                "\u{1f6e1}\u{fe0f} [VORTEX-OMEGA V4.8 DAILY SHIELD] Transicion UTC: {} -> {} | Hoy: {:+.4} USDT ({:+.2}%)",
                prev_phase.label(), cur_phase.label(),
                (rt.daily_gov.day_realized_pnl_1e8 as f64) / 100_000_000.0,
                (rt.daily_gov.day_pnl_ppm() as f64) / 10_000.0
            ));
        }
    }
    let block_new_entries = rt.halt_locked || rt.drain_active || rt.daily_gov.is_hibernating();

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
        let evt_dil = rt.spoof_clocks[s_idx].event_clock_dilation();
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                if rt.slot_rpc_inflight[s_idx] { pending_entry_count += 1; continue; }
                if block_new_entries || !instruments.is_slot_tradable(s_idx) {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    continue;
                }
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if now_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m) { continue; }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let Ok((synced_entry_px, synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto(&snap) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let (q_l1, opp_cum) = match side {
                    Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                    armed_ts_ns: now_ns.0, init_q_lots: q_l1.0,
                    ofi_in: snap.factor_ofi.get(), tfi_in: snap.factor_tfi.get(), geom_in: snap.factor_geom.get(),
                    snr_in: OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get(),
                    vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                    spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                    btc_comp_in: OrthogonalFactorState::snr_composite_score(&btc_snap),
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
            }
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, resting_since_ts, .. } => {
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                let (at_l1, live_l1_q, cur_opp_cum) = match side {
                    Side::Bid => (snap.bids[0].price == entry_price, snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].price == entry_price, snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
                rt.last_opp_trades[s_idx] = cur_opp_cum;
                let (next_q, opp_hit, next_obs) = step_l1_queue_ahead_lots(side, entry_price, at_l1, live_l1_q, rt.last_l1_q_obs[s_idx], rt.cur_q_ahead[s_idx], tape_delta, snap.last_trade_price);
                rt.cur_q_ahead[s_idx] = next_q;
                rt.last_l1_q_obs[s_idx] = next_obs;
                if let Ok(tel) = arena.telemetry(s_idx) { tel.store_queue_ahead_lots(next_q); }
                let q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(side, rt.init_q_ahead[s_idx], next_q, opp_hit, &snap, &btc_snap, epps_beta);
                if rt.slot_rpc_inflight[s_idx] { continue; }
                let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                let total_chase_ns = now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns);
                let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
                let q_timers = elastic_resting_queue_timers_ns(max_chase_ns);
                let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
                let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                let q_eroded = rt.init_q_ahead[s_idx].0 > 0 && next_q.0 * 100 <= rt.init_q_ahead[s_idx].0 * 65;
                let headwind_erosion = s_idx > 0 && elapsed_ns >= q_timers.headwind_abort_ns && q_eroded && (btc_aligned < -0.02 || flow_imp < 0.38);
                let macro_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_elliott_macro_coherence_guard(side, &snap);
                let ob_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                if block_new_entries || !instruments.is_slot_tradable(s_idx) || elapsed_ns >= (max_chase_ns * 3) / 4 || total_chase_ns >= max_chase_ns || (elapsed_ns >= 25_000_000 && q_shield.abort_now) || headwind_erosion || macro_bad || ob_bad || flow_imp <= -0.42 {
                    let abort_veto = if block_new_entries { LastVetoCode::WarmupOrLock } else if macro_bad { LastVetoCode::MacroElliottContradiction } else if ob_bad { LastVetoCode::OpposingOrderBlockWall } else if headwind_erosion { LastVetoCode::BtcLeadLagHeadwind } else { LastVetoCode::HawkesStoikovQueueAbort };
                    rt.last_abort_veto[s_idx] = abort_veto;
                    publish_slot_veto_code(arena, s_idx, abort_veto);
                    if let Some(gw) = gateway {
                        if !rt.abort_cmd_inflight[s_idx] && gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.abort_cmd_inflight[s_idx] = true;
                        }
                    } else {
                        gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                        rt.markout_rings[s_idx].arm_abort_probe(side, entry_price, abort_veto, now_ns);
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                    continue;
                }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                if !at_l1 && elapsed_ns >= q_timers.repeg_wait_ns && total_chase_ns + q_timers.repeg_wait_ns < max_chase_ns && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, &snap, &btc_snap, epps_beta) && passes_elliott_macro_coherence_guard(side, &snap) && passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees) {
                    let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                    let moved_away = match side { Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks, Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks };
                    if moved_away && gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                        if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&snap, new_l1) {
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + q_timers.repeg_cooldown_ns;
                            if let Some(gw) = gateway {
                                if gw.try_send(GatewayCommand::RepegEntry { slot_idx: s_idx, side, new_price: new_l1, qty, new_sl_price: repeg_sl }) { rt.slot_rpc_inflight[s_idx] = true; continue; }
                            } else {
                                engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                continue;
                            }
                        }
                    }
                }
                if let Some(gw) = gateway {
                    if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                        *last_rest_dispatch_ns = now_ns.0;
                        engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
                        if gw.try_send(GatewayCommand::PollEntryFill { slot_idx: s_idx, sl_hint: sl_trigger_price }) { rt.slot_rpc_inflight[s_idx] = true; }
                    }
                } else {
                    let bbo_crossed = match side { Side::Bid => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 <= entry_price.0, Side::Ask => snap.bids[0].price.0 >= entry_price.0 };
                    if next_q.0 <= 0 || bbo_crossed {
                        gov.clear_abort_streak(s_idx, side);
                        engines[s_idx].sync_governor_for_side(gov, side);
                        if let Some((eff_s, eff_px, eff_q)) = crate::exec_gateway_handlers::promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None) {
                            let mid_now = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
                            rt.markout_rings[s_idx].arm_fill_probe(eff_s, eff_px, mid_now, now_ns);
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
                    match side {
                        Side::Bid => { if ref_px.0 > peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                        Side::Ask => { if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                    }
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state { *pk = peak_favorable_price; *wr = worst_adverse_price; }
                    let (d_sign, denom) = (side.sign_i64(), avg_price_ticks.0.max(1));
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v48(&snap, &btc_snap, epps_beta, &rt.markout_rings[s_idx], evt_dil);
                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let eff_sl_now = engines[s_idx].last_eff_sl_ticks;
                    let unverified_sl_breach = rt.verified_exchange_sl[s_idx].0 <= 0 && match side { Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= eff_sl_now.0, Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= eff_sl_now.0 };
                    if unverified_sl_breach && !rt.slot_rpc_inflight[s_idx] {
                        if gw.try_send(GatewayCommand::EmergencyFlattenSlot { slot_idx: s_idx, open_side: side, open_lots, backup_sl: eff_sl_now }) { rt.slot_rpc_inflight[s_idx] = true; }
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    if let Some(new_sl) = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&snap, &btc_snap, epps_beta, zeta_tox) {
                        if new_sl != rt.verified_exchange_sl[s_idx] { rt.sl_needs_sync[s_idx] = true; }
                    }
                    if rt.sl_needs_sync[s_idx] && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
                        if let Some(safe_sl) = select_safe_live_sl_ticks_with_fees(side, avg_price_ticks, rt.verified_exchange_sl[s_idx], engines[s_idx].last_eff_sl_ticks, &snap, engines[s_idx].active_fees) {
                            engines[s_idx].last_eff_sl_ticks = safe_sl;
                            if gw.try_send(GatewayCommand::EnforceStopLoss { slot_idx: s_idx, sl_price: safe_sl }) { rt.slot_rpc_inflight[s_idx] = true; rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000; }
                        } else { rt.sl_needs_sync[s_idx] = false; }
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap) {
                        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                        engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                        engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                        let (preempt_peg, next_sync_delay_ns) = compute_exit_sync_schedule_with_fees(&quote, resting_exit_price, avg_price_ticks, engines[s_idx].active_fees);
                        if !rt.slot_rpc_inflight[s_idx] && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns) && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                            *last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
                            if gw.try_send(GatewayCommand::SyncOpenSlot { slot_idx: s_idx, side, target_px: quote.target_price, target_lots: quote.order_lots, defensive_lock: quote.urgent_close_repeg, cur_gross_bps: quote.cur_gross_bps, vac_level: quote.vac_level, opened_ts_ns: opened_ts.0, tranche1_harvested, fees: engines[s_idx].active_fees }) {
                                rt.slot_rpc_inflight[s_idx] = true;
                            }
                        }
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }
                if let Some(summary) = engines[s_idx].step_open_position_v48(&snap, &btc_snap, gov, ledger, epps_beta, &rt.markout_rings[s_idx], evt_dil) {
                    open_count = open_count.saturating_sub(1);
                    let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                    gov.observe_closed_trade_at_entry(s_idx, summary.side, summary.entry_price, summary.net_scaled_bps, non_punitive, now_ns);
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
                }
                engines[s_idx].sync_to_shm_telemetry(arena);
            }
        }
    }

    let mut beta_guard = build_monetary_beta_guard_with_epps(rt.wallet_1e8, engines, cached_snaps, &rt.epps_trackers, fees);
    if !block_new_entries && (open_count + pending_entry_count) < 6 {
        let mut armed_this_tick = 0_usize;
        let btc_vol = ScaledBps(btc_snap.realized_vol_5m.0.max(2_500));
        let daily_damper_pm = rt.daily_gov.kelly_boundary_damper_permille(
            ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
            gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
        );
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if (open_count + pending_entry_count) >= 6 || armed_this_tick >= 1 { break; }
            if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) || rt.slot_rpc_inflight[s_idx] { continue; }
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 { continue; }
            if !instruments.is_slot_tradable(s_idx) { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); continue; }
            let spec = APPROVED_SPECS[s_idx];
            let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
            let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
            let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
            let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
            let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(spec, &snap, &btc_snap, mfe_s, ScaledBps(probe_edge), taker_pm, fees, spoof_d, epps_beta);
            match verdict {
                SignalVerdict::SpoofingVeto => publish_slot_veto_code(arena, s_idx, LastVetoCode::SpoofingVeto),
                SignalVerdict::NeutralNoAction => {
                    if rt.last_abort_veto[s_idx] == LastVetoCode::None { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); }
                }
                SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                    let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                    let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    if !spec.passes_quantum_ev_growth_gate(rt.wallet_1e8, cand_px, snap.realized_vol_5m, mfe_s, s_taker_pm, fees) {
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
                        continue;
                    }
                    if !passes_elliott_macro_coherence_guard(side, &snap) { publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction); continue; }
                    if !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, s_taker_pm, fees) { publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall); continue; }
                    if !can_stage_same_direction_entry(s_idx, side, now_ns, engines, cached_snaps) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    let lots = gov.compute_slot_entry_lots_v48(s_idx, side, rt.wallet_1e8, &snap, daily_damper_pm);
                    let risk_ppm = slot_position_risk_ppm_with_fees(spec, rt.wallet_1e8, cand_px, lots, snap.realized_vol_5m, fees);
                    let beta_x1000 = rt.epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                    if !beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    if !gov.authorizes_adaptive_entry_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) {
                        let veto_c = if gov.audit_historical_entry_veto(s_idx, side, cand_px.0, snap.ts_local_ns.0).is_some() { LastVetoCode::ScarCooldownOrContagion } else { LastVetoCode::KinematicSurfaceOrCadence };
                        publish_slot_veto_code(arena, s_idx, veto_c);
                        continue;
                    }
                    engines[s_idx].sync_governor_for_side(gov, side);
                    if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_ok() && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. }) {
                        rt.last_abort_veto[s_idx] = LastVetoCode::None;
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::None);
                        beta_guard.register_slot_open(s_idx, side, risk_ppm, beta_x1000);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        pending_entry_count += 1;
                        armed_this_tick += 1;
                    }
                }
            }
        }
    } else if block_new_entries {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle) { publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock); }
        }
    }
    (open_count, pending_entry_count)
}

pub fn step_12_slot_daemon_tick(
    now_ns: TimestampNs, gateway: Option<&BybitAsyncGateway>, instruments: InstrumentSpecVerification,
    lock_watch: Option<&AtomicLockWatch>, rt: &mut ExecRuntimeState, last_rest_dispatch_ns: &mut i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS], entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS], gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger, arena: &VortexShmArena, tg_tx: &SyncSender<String>, vault_tx: &SyncSender<VaultIoCommand>,
) -> (usize, usize) {
    let (mut open_count, mut pending_entry_count) = (0_usize, 0_usize);
    let btc_snap = cached_snaps[0];
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
        if rt.daily_gov.is_hibernating() || prev_phase.label().starts_with("SLEEP") {
            let _ = tg_tx.try_send(format!(
                "\u{1f6e1}\u{fe0f} [VORTEX-OMEGA V4.8 DAILY SHIELD] Transicion UTC: {} -> {} | Hoy: {:+.4} USDT ({:+.2}%)",
                prev_phase.label(), cur_phase.label(),
                (rt.daily_gov.day_realized_pnl_1e8 as f64) / 100_000_000.0,
                (rt.daily_gov.day_pnl_ppm() as f64) / 10_000.0
            ));
        }
    }
    let block_new_entries = rt.halt_locked || rt.drain_active || rt.daily_gov.is_hibernating();

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 { continue; }
        let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
        let evt_dil = rt.spoof_clocks[s_idx].event_clock_dilation();
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                if rt.slot_rpc_inflight[s_idx] { pending_entry_count += 1; continue; }
                if block_new_entries || !instruments.is_slot_tradable(s_idx) {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
                    continue;
                }
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if now_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m) { continue; }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let Ok((synced_entry_px, synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto(&snap) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
                    continue;
                };
                let (q_l1, opp_cum) = match side {
                    Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                    armed_ts_ns: now_ns.0, init_q_lots: q_l1.0,
                    ofi_in: snap.factor_ofi.get(), tfi_in: snap.factor_tfi.get(), geom_in: snap.factor_geom.get(),
                    snr_in: OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get(),
                    vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                    spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                    btc_comp_in: OrthogonalFactorState::snr_composite_score(&btc_snap),
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
            }
            OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, sl_trigger_price, resting_since_ts, .. } => {
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                let (at_l1, live_l1_q, cur_opp_cum) = match side {
                    Side::Bid => (snap.bids[0].price == entry_price, snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].price == entry_price, snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
                rt.last_opp_trades[s_idx] = cur_opp_cum;
                let (next_q, opp_hit, next_obs) = step_l1_queue_ahead_lots(side, entry_price, at_l1, live_l1_q, rt.last_l1_q_obs[s_idx], rt.cur_q_ahead[s_idx], tape_delta, snap.last_trade_price);
                rt.cur_q_ahead[s_idx] = next_q;
                rt.last_l1_q_obs[s_idx] = next_obs;
                if let Ok(tel) = arena.telemetry(s_idx) { tel.store_queue_ahead_lots(next_q); }
                let q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(side, rt.init_q_ahead[s_idx], next_q, opp_hit, &snap, &btc_snap, epps_beta);
                if rt.slot_rpc_inflight[s_idx] { continue; }
                let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                let total_chase_ns = now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns);
                let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
                let q_timers = elastic_resting_queue_timers_ns(max_chase_ns);
                let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
                let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                let q_eroded = rt.init_q_ahead[s_idx].0 > 0 && next_q.0 * 100 <= rt.init_q_ahead[s_idx].0 * 65;
                let headwind_erosion = s_idx > 0 && elapsed_ns >= q_timers.headwind_abort_ns && q_eroded && (btc_aligned < -0.02 || flow_imp < 0.38);
                let macro_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_elliott_macro_coherence_guard(side, &snap);
                let ob_bad = elapsed_ns >= q_timers.macro_abort_ns && !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                if block_new_entries || !instruments.is_slot_tradable(s_idx) || elapsed_ns >= (max_chase_ns * 3) / 4 || total_chase_ns >= max_chase_ns || (elapsed_ns >= 25_000_000 && q_shield.abort_now) || headwind_erosion || macro_bad || ob_bad || flow_imp <= -0.42 {
                    let abort_veto = if block_new_entries { LastVetoCode::WarmupOrLock } else if macro_bad { LastVetoCode::MacroElliottContradiction } else if ob_bad { LastVetoCode::OpposingOrderBlockWall } else if headwind_erosion { LastVetoCode::BtcLeadLagHeadwind } else { LastVetoCode::HawkesStoikovQueueAbort };
                    rt.last_abort_veto[s_idx] = abort_veto;
                    publish_slot_veto_code(arena, s_idx, abort_veto);
                    if let Some(gw) = gateway {
                        if !rt.abort_cmd_inflight[s_idx] && gw.try_send(GatewayCommand::AbortEntry { slot_idx: s_idx, side, aborted_price: entry_price, sl_hint: sl_trigger_price }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                            rt.abort_cmd_inflight[s_idx] = true;
                        }
                    } else {
                        gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                        rt.markout_rings[s_idx].arm_abort_probe(side, entry_price, abort_veto, now_ns);
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                    }
                    continue;
                }
                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                if !at_l1 && elapsed_ns >= q_timers.repeg_wait_ns && total_chase_ns + q_timers.repeg_wait_ns < max_chase_ns && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, &snap, &btc_snap, epps_beta) && passes_elliott_macro_coherence_guard(side, &snap) && passes_order_block_wall_guard_dynamic(s_idx, side, &snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees) {
                    let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                    let moved_away = match side { Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks, Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks };
                    if moved_away && gov.confirm_preflight_kinematics_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                        if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&snap, new_l1) {
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + q_timers.repeg_cooldown_ns;
                            if let Some(gw) = gateway {
                                if gw.try_send(GatewayCommand::RepegEntry { slot_idx: s_idx, side, new_price: new_l1, qty, new_sl_price: repeg_sl }) { rt.slot_rpc_inflight[s_idx] = true; continue; }
                            } else {
                                engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                continue;
                            }
                        }
                    }
                }
                if let Some(gw) = gateway {
                    if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                        *last_rest_dispatch_ns = now_ns.0;
                        engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
                        if gw.try_send(GatewayCommand::PollEntryFill { slot_idx: s_idx, sl_hint: sl_trigger_price }) { rt.slot_rpc_inflight[s_idx] = true; }
                    }
                } else {
                    let bbo_crossed = match side { Side::Bid => snap.asks[0].price.0 > 0 && snap.asks[0].price.0 <= entry_price.0, Side::Ask => snap.bids[0].price.0 >= entry_price.0 };
                    if next_q.0 <= 0 || bbo_crossed {
                        gov.clear_abort_streak(s_idx, side);
                        engines[s_idx].sync_governor_for_side(gov, side);
                        if let Some((eff_s, eff_px, eff_q)) = crate::exec_gateway_handlers::promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None) {
                            let mid_now = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
                            rt.markout_rings[s_idx].arm_fill_probe(eff_s, eff_px, mid_now, now_ns);
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
                    match side {
                        Side::Bid => { if ref_px.0 > peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                        Side::Ask => { if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 { peak_favorable_price = ref_px; } if ref_px.0 > worst_adverse_price.0 { worst_adverse_price = ref_px; } }
                    }
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state { *pk = peak_favorable_price; *wr = worst_adverse_price; }
                    let (d_sign, denom) = (side.sign_i64(), avg_price_ticks.0.max(1));
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v48(&snap, &btc_snap, epps_beta, &rt.markout_rings[s_idx], evt_dil);
                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let eff_sl_now = engines[s_idx].last_eff_sl_ticks;
                    let unverified_sl_breach = rt.verified_exchange_sl[s_idx].0 <= 0 && match side { Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= eff_sl_now.0, Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= eff_sl_now.0 };
                    if unverified_sl_breach && !rt.slot_rpc_inflight[s_idx] {
                        if gw.try_send(GatewayCommand::EmergencyFlattenSlot { slot_idx: s_idx, open_side: side, open_lots, backup_sl: eff_sl_now }) { rt.slot_rpc_inflight[s_idx] = true; }
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    if let Some(new_sl) = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&snap, &btc_snap, epps_beta, zeta_tox) {
                        if new_sl != rt.verified_exchange_sl[s_idx] { rt.sl_needs_sync[s_idx] = true; }
                    }
                    if rt.sl_needs_sync[s_idx] && !rt.slot_rpc_inflight[s_idx] && now_ns.0 >= rt.sl_retry_after_ns[s_idx] {
                        if let Some(safe_sl) = select_safe_live_sl_ticks_with_fees(side, avg_price_ticks, rt.verified_exchange_sl[s_idx], engines[s_idx].last_eff_sl_ticks, &snap, engines[s_idx].active_fees) {
                            engines[s_idx].last_eff_sl_ticks = safe_sl;
                            if gw.try_send(GatewayCommand::EnforceStopLoss { slot_idx: s_idx, sl_price: safe_sl }) { rt.slot_rpc_inflight[s_idx] = true; rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000; }
                        } else { rt.sl_needs_sync[s_idx] = false; }
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap) {
                        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                        engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                        engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                        let (preempt_peg, next_sync_delay_ns) = compute_exit_sync_schedule_with_fees(&quote, resting_exit_price, avg_price_ticks, engines[s_idx].active_fees);
                        if !rt.slot_rpc_inflight[s_idx] && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns) && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000 {
                            *last_rest_dispatch_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
                            if gw.try_send(GatewayCommand::SyncOpenSlot { slot_idx: s_idx, side, target_px: quote.target_price, target_lots: quote.order_lots, defensive_lock: quote.urgent_close_repeg, cur_gross_bps: quote.cur_gross_bps, vac_level: quote.vac_level, opened_ts_ns: opened_ts.0, tranche1_harvested, fees: engines[s_idx].active_fees }) {
                                rt.slot_rpc_inflight[s_idx] = true;
                            }
                        }
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }
                if let Some(summary) = engines[s_idx].step_open_position_v48(&snap, &btc_snap, gov, ledger, epps_beta, &rt.markout_rings[s_idx], evt_dil) {
                    open_count = open_count.saturating_sub(1);
                    let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                    gov.observe_closed_trade_at_entry(s_idx, summary.side, summary.entry_price, summary.net_scaled_bps, non_punitive, now_ns);
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
                }
                engines[s_idx].sync_to_shm_telemetry(arena);
            }
        }
    }

    let mut beta_guard = build_monetary_beta_guard_with_epps(rt.wallet_1e8, engines, cached_snaps, &rt.epps_trackers, fees);
    if !block_new_entries && (open_count + pending_entry_count) < 6 {
        let mut armed_this_tick = 0_usize;
        let btc_vol = ScaledBps(btc_snap.realized_vol_5m.0.max(2_500));
        let daily_damper_pm = rt.daily_gov.kelly_boundary_damper_permille(
            ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
            gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
        );
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if (open_count + pending_entry_count) >= 6 || armed_this_tick >= 1 { break; }
            if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) || rt.slot_rpc_inflight[s_idx] { continue; }
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 { continue; }
            if !instruments.is_slot_tradable(s_idx) { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); continue; }
            let spec = APPROVED_SPECS[s_idx];
            let spoof_d = rt.spoof_clocks[s_idx].spoofing_density();
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &rt.epps_trackers[s_idx]);
            let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
            let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
            let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
            let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(spec, &snap, &btc_snap, mfe_s, ScaledBps(probe_edge), taker_pm, fees, spoof_d, epps_beta);
            match verdict {
                SignalVerdict::SpoofingVeto => publish_slot_veto_code(arena, s_idx, LastVetoCode::SpoofingVeto),
                SignalVerdict::NeutralNoAction => {
                    if rt.last_abort_veto[s_idx] == LastVetoCode::None { publish_slot_veto_code(arena, s_idx, LastVetoCode::Friction3xOrEligibility); }
                }
                SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                    let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                    let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    if !spec.passes_quantum_ev_growth_gate(rt.wallet_1e8, cand_px, snap.realized_vol_5m, mfe_s, s_taker_pm, fees) {
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::QueueDepthOrOversizedLot);
                        continue;
                    }
                    if !passes_elliott_macro_coherence_guard(side, &snap) { publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction); continue; }
                    if !passes_order_block_wall_guard_dynamic(s_idx, side, &snap, s_taker_pm, fees) { publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall); continue; }
                    if !can_stage_same_direction_entry(s_idx, side, now_ns, engines, cached_snaps) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    let lots = gov.compute_slot_entry_lots_v48(s_idx, side, rt.wallet_1e8, &snap, daily_damper_pm);
                    let risk_ppm = slot_position_risk_ppm_with_fees(spec, rt.wallet_1e8, cand_px, lots, snap.realized_vol_5m, fees);
                    let beta_x1000 = rt.epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_vol);
                    if !beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000) { publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger); continue; }
                    if !gov.authorizes_adaptive_entry_v48(s_idx, side, rt.wallet_1e8, mfe_s, &snap, &btc_snap, spoof_d, epps_beta) {
                        let veto_c = if gov.audit_historical_entry_veto(s_idx, side, cand_px.0, snap.ts_local_ns.0).is_some() { LastVetoCode::ScarCooldownOrContagion } else { LastVetoCode::KinematicSurfaceOrCadence };
                        publish_slot_veto_code(arena, s_idx, veto_c);
                        continue;
                    }
                    engines[s_idx].sync_governor_for_side(gov, side);
                    if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_ok() && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. }) {
                        rt.last_abort_veto[s_idx] = LastVetoCode::None;
                        publish_slot_veto_code(arena, s_idx, LastVetoCode::None);
                        beta_guard.register_slot_open(s_idx, side, risk_ppm, beta_x1000);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        pending_entry_count += 1;
                        armed_this_tick += 1;
                    }
                }
            }
        }
    } else if block_new_entries {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle) { publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock); }
        }
    }
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
    let mut last_funding_cut = [0_i64; MAX_ACTIVE_PAIRS];

    let boot_ns = now_epoch_ns();
    let startup_hs = run_daemon_startup_handshake(live_capital, boot_ns);
    let mut instruments = startup_hs.instruments;
    gov.set_active_fee_schedule(startup_hs.fee_schedule);
    ledger.set_live_vip_fee_rates(startup_hs.fee_schedule.maker_fee_bps, startup_hs.fee_schedule.taker_fee_bps);
    for eng in &mut engines {
        eng.sync_governor_dynamics(&gov);
    }
    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena); rt.daily_gov = rehydrate_daily_governor_from_vault(now_epoch_ns(), rt.wallet_1e8.max(ledger.current_equity_1e8()), &ledger, &gov);
    SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut engines, Path::new(MFE_PLASTICITY_VAULT_PATH));

    let prewarmed_slots = gov.load_ema_prewarm_file(boot_ns);
    if live_capital {
        let warmup_ns = if prewarmed_slots >= MAX_ACTIVE_PAIRS { 15_000_000_000_i64 } else { 75_000_000_000_i64 };
        gov.set_entry_warmup_until_ns(boot_ns.0.saturating_add(warmup_ns));
        println!(
            "{ANSI_GREEN}\u{1f7e2} [EMA PREWARM V4.7] Hydrated {prewarmed_slots}/12 slots | Live L2 stabilize: {}s | Fees: {:+.2}/{:.2}bp{ANSI_RESET}",
            warmup_ns / 1_000_000_000,
            (startup_hs.fee_schedule.maker_fee_bps.raw() as f64) / 100.0,
            (startup_hs.fee_schedule.taker_fee_bps.raw() as f64) / 100.0
        );
    }
    bootstrap_live_adopt_if_enabled(live_capital, arena, &gov, &mut rt, &mut entry_forensics, &mut engines);
    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);
    let mut last_wallet_sync_ns = boot_ns.0;
    let mut last_handshake_refresh_ns = boot_ns.0;
    let mut last_rest_dispatch_ns = 0_i64;
    println!(
        "{ANSI_GREEN}\u{1f7e2} [VORTEX-OMEGA V4.8 200.0 Hz ARMED] Unified Zero-Jitter Vault + Anti-Spoof Epps + 8h Funding + Hourly VIP.{ANSI_RESET}"
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
                        let _ = rt.spoof_clocks[s_idx].step_event_clock_and_spoof(
                            snap.ts_local_ns, snap.factor_ofi, snap.factor_tfi,
                        );
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
                        gov.observe_market_price(s_idx, &snap);
                    }
                }
            }
        }

        if let Some(ref gw) = gateway {
            while let Some(evt) = gw.try_recv() {
                if let GatewayEvent::HandshakeRefreshed { handshake: hs } = &evt {
                    if hs.synced_from_exchange {
                        instruments = hs.instruments;
                    }
                }
                let prev_halt = rt.halt_locked;
                let prev_closed = ledger.closed_trades_count;
                handle_gateway_event(
                    evt, now_ns, Some(gw), &mut rt, &cached_snaps, &entry_forensics,
                    &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
                );
                if rt.halt_locked && !prev_halt {
                    lock_watch.latch_halt_in_ram();
                }
                if ledger.closed_trades_count > prev_closed {
                    let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                        ts_ns: now_ns.0,
                        learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
                    });
                }
            }
        }

        let _ = settle_live_8h_funding_cuts_v48(&engines, &cached_snaps, &mut last_funding_cut, &mut ledger, Some(&vault_tx), Some((&mut rt.daily_gov, now_ns, rt.wallet_1e8.max(ledger.current_equity_1e8()), &gov)));

        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            gov.dispatch_ema_prewarm_async(now_ns, &vault_tx);
            let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                ts_ns: now_ns.0,
                learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(&engines),
            });
            if let Some(ref gw) = gateway {
                let _ = gw.try_send(GatewayCommand::SyncWallet);
            }
        }

        if now_ns.0.saturating_sub(last_handshake_refresh_ns) >= HOURLY_HANDSHAKE_REFRESH_NS {
            last_handshake_refresh_ns = now_ns.0;
            if let Some(ref gw) = gateway {
                let _ = gw.try_send(GatewayCommand::RefreshHandshake { now_ns });
            }
        }

        let (open_count, pending_entry_count) = step_12_slot_daemon_tick(
            now_ns, gateway.as_ref(), instruments, Some(&lock_watch), &mut rt,
            &mut last_rest_dispatch_ns, &cached_snaps, &mut entry_forensics,
            &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );

        let any_inflight = rt.slot_rpc_inflight.iter().any(|&b| b);
        if rt.drain_active && open_count == 0 && pending_entry_count == 0 && !any_inflight {
            let _ = tg_tx.try_send(
                "\u{1f7e2} [VORTEX-OMEGA V4.8 DRAIN COMPLETE] 0 open positions verified (Wallet == Equity).".to_string(),
            );
            let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
            if vault_tx.send(VaultIoCommand::FlushBarrier(ack_tx)).is_ok() {
                let _ = ack_rx.recv_timeout(Duration::from_secs(2));
            }
            thread::sleep(Duration::from_millis(120));
            println!("{ANSI_GREEN}\u{1f7e2} [DRAIN_COMPLETE_STOPPED] 0 open positions verified. Vault flushed.{ANSI_RESET}");
            return ExitCode::SUCCESS;
        }
        if is_safe_halt_standby(rt.halt_locked || rt.daily_gov.is_hibernating(), open_count, pending_entry_count, any_inflight) {
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub fn run_exec_cli_entrypoint() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_live_hud = args.iter().any(|a| a == "--live");
    let is_trades = args.iter().any(|a| a == "--trades");
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let is_live_cap = args.iter().any(|a| a == "--live-capital");
    let shm_path = MappedShmRegion::default_shm_path();

    if is_live_hud {
        match MappedShmRegion::open_existing_read_only(&shm_path) {
            Ok(reg) => render_live_hud(reg.arena()),
            Err(_) => println!(
                "{ANSI_GRAY}\u{26aa} [VORTEX-OMEGA V4.8 LIVE HUD] SHM inactive ({}) \u{2014} daemons stopped.{ANSI_RESET}",
                shm_path.display()
            ),
        }
        return ExitCode::SUCCESS;
    }
    if is_trades {
        render_trades_and_executive_dashboard();
        return ExitCode::SUCCESS;
    }
    let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
    let sep = "=".repeat(80);
    println!("{sep}");
    println!("[VORTEX-EXEC OMEGA V4.7] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
    println!("{sep}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> OOS Closed Trades Verified : {oos_trades} (Gate >= 500){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Maker Exit Ratio           : {maker_ratio:.2}% (Gate >= 80.00%){ANSI_RESET}");
    println!("  {ANSI_GREEN}\u{1f7e2} -> Net Profit Factor (PostFee): {pf:.2} (Gate >= 1.35){ANSI_RESET}");
    println!(
        "  {ANSI_GREEN}\u{1f7e2} -> Cumulative Net PnL (OOS)   : + {cum_usdt:.4} USDT | PARITY_SYNC Drift: {drift} units{ANSI_RESET}"
    );
    println!("{sep}");
    if oos_trades < 500 || maker_ratio < 80.0 || pf < 1.35 || drift != 0 {
        return ExitCode::FAILURE;
    }
    if is_daemon {
        let Ok(reg) = MappedShmRegion::open_or_create(&shm_path, false) else {
            return ExitCode::FAILURE;
        };
        return run_autonomous_12_slot_daemon(reg.arena(), is_live_cap);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_fsm::ExitOrderRole;
    use crate::lob_shm::BookLevel;
    use crate::types::BoundedScore;

    #[test]
    fn test_v47_p0b_halt_custody_unified_vault_io_8h_funding_and_anti_spoof_arming() {
        assert!(!is_safe_halt_standby(true, 1, 0, false));
        assert!(is_safe_halt_standby(true, 0, 0, false));

        let shm_path = MappedShmRegion::ephemeral_self_test_path("p0b_v47_unified_daemon");
        if let Ok(r) = MappedShmRegion::open_or_create(&shm_path, true) {
            let (tg_tx, _) = mpsc::sync_channel::<String>(8);
            let watch = Arc::new(AtomicLockWatch::default());
            let (vault_tx, _) = mpsc::sync_channel::<VaultIoCommand>(16);
            let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);
            let mut rt = ExecRuntimeState::new();
            let mut last_rest = 0_i64;
            let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
            for (idx, s) in snaps.iter_mut().enumerate() {
                s.symbol_id = APPROVED_SPECS[idx].symbol_id;
                s.is_eligible = true;
                s.sequence_id = 1;
                s.ts_local_ns = TimestampNs(200_000_000_000);
                s.realized_vol_5m = ScaledBps(6_500);
                s.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(450) };
                s.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(320) };
                s.refresh_deep_l1_l5_totals();
                s.mark_price = PriceTicks(8_200);
                s.last_trade_price = PriceTicks(8_200);
                s.factor_ofi = BoundedScore::new_clamped(0.68);
                s.factor_tfi = BoundedScore::new_clamped(0.72);
                s.factor_geom = BoundedScore::new_clamped(0.40);
            }
            for idx in [1_usize, 2] { snaps[idx].sequence_id = 0; }
            let mut forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
            let mut engines = SlotExecutionEngine::new_12_slot_array();
            let mut gov = OnlineAdaptiveGovernor::new();
            let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
            let (_, pend) = step_12_slot_daemon_tick(
                TimestampNs(200_000_000_000), None, inst, Some(&watch), &mut rt, &mut last_rest,
                &snaps, &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert!(pend >= 1);

            // Verify live 8h funding settlement helper on open slot
            let cut_ns = 1_794_672_000_000_000_000_i64;
            let armed_s = engines.iter().position(|e| !matches!(e.state, OrderLifecycleState::Idle)).unwrap_or(0); let _ = engines[armed_s].on_maker_entry_filled(TimestampNs(cut_ns - 5_000_000_000));
            snaps[armed_s].ts_local_ns = TimestampNs(cut_ns + 2_000_000_000);
            snaps[armed_s].funding_rate_scaled_bps = ScaledBps(-150);
            let mut f_cuts = [0_i64; MAX_ACTIVE_PAIRS];
            assert_eq!(settle_live_8h_funding_cuts(&engines, &snaps, &mut f_cuts, &mut ledger), 1);
            assert!(ledger.cumulative_funding_1e8 > 0);
        }
        let _ = std::fs::remove_file(&shm_path);
    }

    #[test]
    fn test_live_200hz_shadow_runner_preemption_and_safe_chandelier_sl_dispatch() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(5_000);
        snap.mark_price = PriceTicks(8_237);
        snap.last_trade_price = PriceTicks(8_237);
        snap.bids[0] = BookLevel { price: PriceTicks(8_236), qty: LotQty(350) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_237), qty: LotQty(200) };
        assert_eq!(
            select_safe_live_sl_ticks(Side::Bid, PriceTicks(8_200), PriceTicks(8_120), PriceTicks(8_226), &snap),
            Some(PriceTicks(8_226))
        );
        let snap_quote = CanonicalExitQuote {
            role: ExitOrderRole::StandardMakerExit100,
            target_price: PriceTicks(762_500),
            order_lots: LotQty(1),
            sub_reason: ExitSubReason::ShadowRunnerCrestHarvest,
            vac_level: 1,
            defensive_lock_active: false,
            urgent_close_repeg: true,
            cur_gross_bps: 4_611,
            wald_pct: 0,
        };
        let (preempt, cd_fast) = compute_exit_sync_schedule_with_fees(
            &snap_quote, PriceTicks(766_400), PriceTicks(759_000), FeeSchedule::VIP0_DEFAULT,
        );
        assert!(preempt && cd_fast == 250_000_000);
    }
}
#[cfg(test)]
mod v48_daemon_daily_shield_tests {
    use super::*;
    use crate::governor_plasticity::DailyShieldPhase;
    use crate::lob_shm::BookLevel;

    #[test]
    fn test_v48_daemon_p0b_hibernation_custody_quantum_veto_and_utc_wakeup() {
        assert!(!is_safe_halt_standby(true, 1, 0, false));
        assert!(is_safe_halt_standby(true, 0, 0, false));

        let shm_path = MappedShmRegion::ephemeral_self_test_path("v48_daily_shield_daemon");
        let reg = MappedShmRegion::open_or_create(&shm_path, true).ok();
        assert!(reg.is_some());
        let Some(reg) = reg else { return };
        let arena = reg.arena();

        let locks = Arc::new(AtomicLockWatch::new(false, false));
        let vault_tx = spawn_vault_io_worker(Arc::clone(&locks));
        let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
        let mut rt = ExecRuntimeState::new();
        rt.wallet_1e8 = 6_513_770_000;
        let day1_ns = TimestampNs(1_794_787_200_000_000_000 + 14 * 3_600_000_000_000);
        let _ = rt.daily_gov.sync_utc_day(day1_ns, rt.wallet_1e8);
        rt.daily_gov.phase = DailyShieldPhase::HibernatingProfitLocked;

        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        snaps[0].symbol_id = APPROVED_SPECS[0].symbol_id;
        snaps[0].sequence_id = 10;
        snaps[0].ts_local_ns = day1_ns;
        snaps[0].realized_vol_5m = ScaledBps(5_500);
        snaps[0].bids[0] = BookLevel { price: PriceTicks(646_312), qty: LotQty(2_500) };
        snaps[0].asks[0] = BookLevel { price: PriceTicks(646_313), qty: LotQty(1_200) };
        snaps[0].refresh_deep_l1_l5_totals();
        snaps[0].factor_ofi = crate::types::BoundedScore::new_clamped(0.85);
        snaps[0].factor_tfi = crate::types::BoundedScore::new_clamped(0.92);
        snaps[0].factor_geom = crate::types::BoundedScore::new_clamped(0.75);

        let mut entry_f = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut engines: [SlotExecutionEngine; MAX_ACTIVE_PAIRS] = core::array::from_fn(SlotExecutionEngine::new);
        engines[0].learned_mfe_bps = 1_450;
        let mut gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(rt.wallet_1e8);
        let mut last_rest = 0_i64;
        let inst = InstrumentSpecVerification::baseline_from_specs(&APPROVED_SPECS);

        let (op1, pe1) = step_12_slot_daemon_tick(
            day1_ns, None, inst, Some(&locks), &mut rt, &mut last_rest,
            &snaps, &mut entry_f, &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );
        assert_eq!((op1, pe1), (0, 0));

        let day2_ns = TimestampNs(day1_ns.0 + 86_400_000_000_000);
        snaps[0].ts_local_ns = day2_ns;
        let _ = step_12_slot_daemon_tick(
            day2_ns, None, inst, Some(&locks), &mut rt, &mut last_rest,
            &snaps, &mut entry_f, &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );
        assert_eq!(rt.daily_gov.phase, DailyShieldPhase::ActiveHunting);
        let tel0 = arena.telemetry(0).map(|t| t.read_consistent_or_default(2)).unwrap_or_default();
        assert_eq!(LastVetoCode::from_code(tel0.last_veto_code), LastVetoCode::QueueDepthOrOversizedLot);
        let _ = std::fs::remove_file(&shm_path);
    }
}
