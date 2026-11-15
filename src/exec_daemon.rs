//! 200Hz Autonomous 12-Slot Execution Daemon (V4.5: Queue Bound, Monetary Beta & SHM Veto Telemetry).

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

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::bybit_gateway::{run_daemon_startup_handshake, BybitAsyncGateway, GatewayCommand};
use crate::exec_gateway_handlers::{
    bootstrap_live_adopt_if_enabled, handle_gateway_event, promote_verified_fill_state,
    run_oos_520_certification_suite, ExecRuntimeState,
};
use crate::execution_fsm::{
    passes_elliott_macro_coherence_guard, passes_order_block_wall_guard,
    passes_stoikov_repeg_coherence_guard, CanonicalExitQuote, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, ServerSlDecision, SlotExecutionEngine,
};
use crate::fee_cadence_beta::{FeeSchedule, MonetaryPortfolioBetaGuard};
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{
    LastVetoCode, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::telemetry_hud::{
    dispatch_closed_trade_logs, dispatch_open_positions_state, format_telegram_close_message,
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

#[inline]
#[must_use]
pub const fn is_safe_halt_standby(
    halt_locked: bool,
    open_count: usize,
    pending_entry_count: usize,
    any_inflight: bool,
) -> bool {
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
    if quote.urgent_close_repeg
        && prev_resting_exit_px.0 > 0
        && prev_resting_exit_px.0 != quote.target_price.0
    {
        return (true, 250_000_000);
    }
    if !quote.urgent_close_repeg
        && matches!(quote.sub_reason, ExitSubReason::FibonacciImpulseSurf)
        && prev_resting_exit_px.0 > 0
    {
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

#[inline]
#[must_use]
pub const fn clamp_resting_queue_ahead_physical(
    cur_q: LotQty,
    live_l1_q: LotQty,
    opp_tape_lots: LotQty,
    prev_l1_q: LotQty,
) -> LotQty {
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
    if next_q > l1_cap {
        next_q = l1_cap;
    }
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
        let next_q = clamp_resting_queue_ahead_physical(prev_q_ahead, l1_cap, opp_hit, prev_l1_q_obs);
        (next_q, opp_hit, l1_cap)
    } else {
        let rem = prev_q_ahead.0.saturating_sub(opp_hit.0);
        (LotQty(if rem > 0 { rem } else { 0 }), opp_hit, LotQty(0))
    }
}

#[inline]
#[must_use]
pub const fn slot_position_risk_ppm(
    spec: PairSpec,
    wallet_1e8: i64,
    px: PriceTicks,
    lots: LotQty,
    vol_5m: ScaledBps,
) -> i64 {
    let px_eff = if px.0 > 0 { px.0 } else { 1 } as i128;
    let tick_eff = if spec.tick_size_1e8 > 1 { spec.tick_size_1e8 } else { 1 } as i128;
    let qty_eff = if lots.0 > 0 { lots.0 } else { 1 } as i128;
    let step_eff = if spec.lot_step_1e8 > 1 { spec.lot_step_1e8 } else { 1 } as i128;
    let raw_notional =
        (px_eff.saturating_mul(tick_eff).saturating_mul(qty_eff.saturating_mul(step_eff)))
            / 100_000_000_i128;
    let notional_1e8 = if raw_notional < 1 {
        1_i64
    } else if raw_notional > (i64::MAX as i128) {
        i64::MAX
    } else {
        raw_notional as i64
    };
    spec.compute_position_sl_risk_ppm(wallet_1e8, notional_1e8, px, vol_5m)
}

#[inline]
#[must_use]
pub const fn slot_btc_beta_x1000(slot_idx: usize, slot_vol_5m: ScaledBps, btc_vol_5m: i64) -> i64 {
    if slot_idx == 0 {
        return 1_000;
    }
    let denom = if btc_vol_5m > 2_500 { btc_vol_5m } else { 2_500 };
    let v_num = if slot_vol_5m.0 > 1_000 { slot_vol_5m.0 } else { 1_000 };
    let raw = v_num.saturating_mul(850) / denom;
    if raw < 500 { 500 } else if raw > 1_450 { 1_450 } else { raw }
}

#[must_use]
pub fn build_monetary_beta_guard(
    wallet_1e8: i64,
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> MonetaryPortfolioBetaGuard {
    let mut guard = MonetaryPortfolioBetaGuard::new();
    let btc_vol = cached_snaps[0].realized_vol_5m.0.max(2_500);
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let active_tuple = match engines[s_idx].state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, entry_price, qty, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, .. } => {
                Some((side, entry_price, qty))
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side, avg_price_ticks, open_lots, ..
            } => Some((side, avg_price_ticks, open_lots)),
        };
        if let Some((side, px, lots)) = active_tuple {
            let snap = &cached_snaps[s_idx];
            let risk_ppm =
                slot_position_risk_ppm(APPROVED_SPECS[s_idx], wallet_1e8, px, lots, snap.realized_vol_5m);
            let beta_x1000 = slot_btc_beta_x1000(s_idx, snap.realized_vol_5m, btc_vol);
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
        if cur.last_veto_code != code {
            cur.last_veto_code = code;
            tel.write_consistent(&cur);
        }
    }
}

#[must_use]
pub fn select_safe_live_sl_ticks(
    side: Side,
    avg_price_ticks: PriceTicks,
    verified_exchange_sl: PriceTicks,
    candidate_eff_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
) -> Option<PriceTicks> {
    if avg_price_ticks.0 <= 0 {
        return None;
    }
    let base_sl = SlotExecutionEngine::compute_fill_or_adopt_sl_ticks(side, avg_price_ticks, snap)
        .unwrap_or(candidate_eff_sl);
    let mut target = match side {
        Side::Bid => {
            let t = if candidate_eff_sl.0 > base_sl.0 { candidate_eff_sl } else { base_sl };
            if verified_exchange_sl.0 > t.0 { verified_exchange_sl } else { t }
        }
        Side::Ask => {
            let t = if candidate_eff_sl.0 > 0 && candidate_eff_sl.0 < base_sl.0 {
                candidate_eff_sl
            } else {
                base_sl
            };
            if verified_exchange_sl.0 > 0 && verified_exchange_sl.0 < t.0 {
                verified_exchange_sl
            } else {
                t
            }
        }
    };
    if target.0 <= 0 {
        return None;
    }
    let ref_mark = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
    let ref_last = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mark };
    match side {
        Side::Bid => {
            target = PriceTicks(target.0.min(ref_mark.0.min(ref_last.0) - 3).max(1));
            if verified_exchange_sl.0 > 0 && target.0 <= verified_exchange_sl.0 {
                return None;
            }
        }
        Side::Ask => {
            target = PriceTicks(target.0.max(ref_mark.0.max(ref_last.0).saturating_add(3)));
            if verified_exchange_sl.0 > 0 && target.0 >= verified_exchange_sl.0 {
                return None;
            }
        }
    }
    PairSpec::validate_stop_loss_geometry(side, target, ref_last, ref_mark).ok()
}

#[inline]
#[must_use]
pub fn elastic_same_side_stagger_bounds(
    tau_mkt_s: i64,
    vol_5m: ScaledBps,
    b3x_bps: ScaledBps,
    fee_rt_bps: i64,
) -> (i64, i64) {
    let tau_s = if tau_mkt_s > 0 { tau_mkt_s } else { 300 };
    let stagger_s = ((tau_s.saturating_mul(STAGGER_BAR_TO_FLOW_PM)) / 1_000).clamp(15, 180);
    let eff_rt = fee_rt_bps.max(100);
    let b3x_pad = (b3x_bps.raw().clamp(300, 1_500) / 30).clamp(25, 50);
    let vol_pad = (vol_5m.raw().clamp(0, 10_000) / 100).clamp(25, 100);
    let pad = if b3x_pad > vol_pad { b3x_pad } else { vol_pad };
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
        if s_idx == candidate_slot {
            continue;
        }
        match eng.state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side: s, .. }
            | OrderLifecycleState::RestingEntryQueue { side: s, .. } => {
                if s == side {
                    return false;
                }
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side: s, avg_price_ticks, opened_ts, ..
            } => {
                if s == side {
                    let snap = &cached_snaps[s_idx];
                    let b3x = APPROVED_SPECS[s_idx].three_x_friction_barrier_bps_dynamic(
                        avg_price_ticks.max(PriceTicks(1)),
                        eng.learned_taker_prob_pm,
                        eng.active_fees,
                    );
                    let rt_maker = eng.active_fees.round_trip_maker_exit_bps().raw();
                    let (stagger_ns, req_cushion_bps) = elastic_same_side_stagger_bounds(
                        eng.intrinsic_tau_mkt_s,
                        snap.realized_vol_5m,
                        b3x,
                        rt_maker,
                    );
                    if now_ns.0.saturating_sub(opened_ts.0) < stagger_ns {
                        return false;
                    }
                    let ref_px = if snap.mark_price.0 > 0 {
                        snap.mark_price.0
                    } else {
                        match s {
                            Side::Bid => snap.bids[0].price.0,
                            Side::Ask => snap.asks[0].price.0,
                        }
                    };
                    let cur_gross_bps = ((ref_px - avg_price_ticks.0) * s.sign_i64())
                        .saturating_mul(1_000_000)
                        / avg_price_ticks.0.max(1);
                    if cur_gross_bps < req_cushion_bps && !eng.chandelier_profit_sl_armed {
                        return false;
                    }
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
    if rt.slot_rpc_inflight[s_idx] {
        return 1;
    }
    if block_new_entries {
        engines[s_idx].state = OrderLifecycleState::Idle;
        engines[s_idx].sync_to_shm_telemetry(arena);
        publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
        return 0;
    }
    engines[s_idx].sync_governor_dynamics(gov);
    if now_ns.0.saturating_sub(submitted_ts.0)
        < engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m)
    {
        return 1;
    }
    let Ok((e_kin, e_min)) =
        gov.confirm_preflight_kinematics_non_choking(s_idx, side, rt.wallet_1e8, snap, btc_snap)
    else {
        engines[s_idx].state = OrderLifecycleState::Idle;
        engines[s_idx].sync_to_shm_telemetry(arena);
        publish_slot_veto_code(arena, s_idx, LastVetoCode::PreflightDriftOrDecay);
        return 1;
    };
    let Ok((synced_entry_px, synced_sl_px)) = engines[s_idx].sync_preflight_l1_entry_auto(snap)
    else {
        let veto = if !passes_elliott_macro_coherence_guard(side, snap) {
            LastVetoCode::MacroElliottContradiction
        } else if !passes_order_block_wall_guard(s_idx, side, snap) {
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
    entry_forensics[s_idx] = SlotForensicEntrySnapshot {
        armed_ts_ns: now_ns.0,
        init_q_lots: q_l1.0,
        ofi_in: snap.factor_ofi.get(),
        tfi_in: snap.factor_tfi.get(),
        geom_in: snap.factor_geom.get(),
        snr_in: OrthogonalFactorState::snr_composite_score(snap),
        vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
        spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
        btc_comp_in: OrthogonalFactorState::snr_composite_score(btc_snap),
        gov_ewma_bps: (ewma_b as f64) / 100.0,
        win_streak: win_st,
        sl_streak: sl_st,
        drift_bps: (dr_b as f64) / 100.0,
        ext_bps: (ex_b as f64) / 100.0,
        kin_accel_net_bps: (((ex_b - dr_b) * side.sign_i64()) as f64) / 100.0,
        kin_energy_in_bps: (e_kin as f64) / 100.0,
        req_kin_energy_in_bps: (e_min as f64) / 100.0,
    };
    rt.init_q_ahead[s_idx] = q_l1;
    rt.cur_q_ahead[s_idx] = q_l1;
    rt.last_l1_q_obs[s_idx] = q_l1;
    rt.last_opp_trades[s_idx] = opp_cum;
    if let Some(gw) = gateway {
        if gw.try_send(GatewayCommand::PlaceEntry {
            slot_idx: s_idx,
            side,
            entry_price: synced_entry_px,
            qty,
            sl_price: synced_sl_px,
        }) {
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
        side,
        entry_price,
        at_l1,
        live_l1_q,
        rt.last_l1_q_obs[s_idx],
        rt.cur_q_ahead[s_idx],
        tape_delta,
        snap.last_trade_price,
    );
    rt.cur_q_ahead[s_idx] = next_q;
    rt.last_l1_q_obs[s_idx] = next_l1_obs;
    if let Ok(tel) = arena.telemetry(s_idx) {
        tel.store_queue_ahead_lots(rt.cur_q_ahead[s_idx]);
    }
    let q_shield = engines[s_idx].evaluate_resting_queue_shield(
        side,
        rt.init_q_ahead[s_idx],
        rt.cur_q_ahead[s_idx],
        opp_hit,
        snap,
        btc_snap,
    );
    if rt.slot_rpc_inflight[s_idx] {
        return;
    }
    let resting_elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
    let total_chase_ns = if entry_forensics[s_idx].armed_ts_ns > 0 {
        now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns)
    } else {
        resting_elapsed_ns
    };
    let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
    let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
    let btc_q_snr = OrthogonalFactorState::snr_composite_score(btc_snap) * side.sign_f64();
    let headwind_erosion = s_idx > 0
        && resting_elapsed_ns >= 2_200_000_000
        && rt.init_q_ahead[s_idx].0 > 0
        && rt.cur_q_ahead[s_idx].0 * 100 <= rt.init_q_ahead[s_idx].0 * 65
        && (btc_q_snr < -0.02 || flow_imp < 0.38);
    let elliott_broken =
        resting_elapsed_ns >= 500_000_000 && !passes_elliott_macro_coherence_guard(side, snap);
    let ob_wall_broken =
        resting_elapsed_ns >= 500_000_000 && !passes_order_block_wall_guard(s_idx, side, snap);
    let dq_abort = (resting_elapsed_ns >= 25_000_000 && at_l1 && q_shield.abort_now)
        || headwind_erosion
        || elliott_broken
        || ob_wall_broken
        || flow_imp <= -0.42;

    if block_new_entries
        || resting_elapsed_ns >= (max_chase_ns * 3) / 4
        || total_chase_ns >= max_chase_ns
        || dq_abort
    {
        let abort_veto = if block_new_entries {
            LastVetoCode::WarmupOrLock
        } else if ob_wall_broken {
            LastVetoCode::OpposingOrderBlockWall
        } else if elliott_broken {
            LastVetoCode::MacroElliottContradiction
        } else if headwind_erosion {
            LastVetoCode::BtcLeadLagHeadwind
        } else {
            LastVetoCode::HawkesStoikovQueueAbort
        };
        if let Some(gw) = gateway {
            if gw.try_send(GatewayCommand::AbortEntry {
                slot_idx: s_idx,
                side,
                aborted_price: entry_price,
                sl_hint: sl_trigger_price,
            }) {
                rt.slot_rpc_inflight[s_idx] = true;
                rt.abort_cmd_inflight[s_idx] = true;
            }
        } else {
            gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
            engines[s_idx].state = OrderLifecycleState::Idle;
            arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
            engines[s_idx].sync_to_shm_telemetry(arena);
        }
        publish_slot_veto_code(arena, s_idx, abort_veto);
        return;
    }

    if !at_l1
        && resting_elapsed_ns >= 2_500_000_000
        && total_chase_ns + 2_500_000_000 < max_chase_ns
        && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
        && passes_stoikov_repeg_coherence_guard(s_idx, side, snap, btc_snap)
        && passes_elliott_macro_coherence_guard(side, snap)
        && passes_order_block_wall_guard(s_idx, side, snap)
    {
        let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
        let max_chase_ticks = spec.max_repeg_chase_ticks(
            entry_price,
            engines[s_idx].learned_taker_prob_pm,
            engines[s_idx].active_fees,
        );
        let moved_away = match side {
            Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks,
            Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks,
        };
        if moved_away
            && gov
                .confirm_preflight_kinematics_non_choking(s_idx, side, rt.wallet_1e8, snap, btc_snap)
                .is_ok()
        {
            if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(snap, new_l1) {
                engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_500_000_000;
                if let Some(gw) = gateway {
                    if gw.try_send(GatewayCommand::RepegEntry {
                        slot_idx: s_idx,
                        side,
                        new_price: new_l1,
                        qty,
                        new_sl_price: repeg_sl,
                    }) {
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
        if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
            && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000
        {
            *last_rest_dispatch_ns = now_ns.0;
            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
            if gw.try_send(GatewayCommand::PollEntryFill {
                slot_idx: s_idx,
                sl_hint: sl_trigger_price,
            }) {
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
            if let Some((eff_s, eff_px, eff_q)) =
                promote_verified_fill_state(&mut engines[s_idx], now_ns, None, None, None)
            {
                record_and_notify_entry_fill(
                    s_idx, spec, engines, entry_forensics, arena, tg_tx, vault_tx, now_ns, eff_s,
                    eff_px, eff_q, "SHADOW MAKER FILL",
                );
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
    if let Some(gw) = gateway {
        engines[s_idx].observe_open_flow_kinematics(snap);
        let _ = engines[s_idx].observe_wald_sprt_step(snap, btc_snap);
        let cur_bbo = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let ref_px = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { cur_bbo };
        match side {
            Side::Bid => {
                if ref_px.0 > peak_favorable_price.0 {
                    peak_favorable_price = ref_px;
                }
                if ref_px.0 > 0 && ref_px.0 < worst_adverse_price.0 {
                    worst_adverse_price = ref_px;
                }
            }
            Side::Ask => {
                if ref_px.0 > 0 && ref_px.0 < peak_favorable_price.0 {
                    peak_favorable_price = ref_px;
                }
                if ref_px.0 > worst_adverse_price.0 {
                    worst_adverse_price = ref_px;
                }
            }
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            peak_favorable_price: ref mut pk,
            worst_adverse_price: ref mut wr,
            ..
        } = engines[s_idx].state
        {
            *pk = peak_favorable_price;
            *wr = worst_adverse_price;
        }
        let avg_denom = avg_price_ticks.0.max(1);
        engines[s_idx].last_closed_mfe_bps =
            ((peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64())
                .saturating_mul(1_000_000)
                / avg_denom;
        engines[s_idx].last_closed_mae_bps =
            ((worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64())
                .saturating_mul(1_000_000)
                / avg_denom;
        engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
        engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
        if engines[s_idx].last_eff_sl_ticks.0 <= 0 {
            engines[s_idx].last_eff_sl_ticks = sl_trigger_price;
        }
        if rt.emergency_flatten_sl[s_idx].is_none() {
            if let Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, .. }) =
                SlotExecutionEngine::evaluate_sl_or_emergency_flatten(side, avg_price_ticks, snap)
            {
                rt.emergency_flatten_sl[s_idx] = Some(backup_sl);
            }
        }
        if let Some(backup_sl) = rt.emergency_flatten_sl[s_idx] {
            if !rt.slot_rpc_inflight[s_idx]
                && now_ns.0 >= rt.sl_retry_after_ns[s_idx]
                && gw.try_send(GatewayCommand::EmergencyFlattenSlot {
                    slot_idx: s_idx,
                    open_side: side,
                    open_lots,
                    backup_sl,
                })
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
        if rt.sl_needs_sync[s_idx]
            && !rt.slot_rpc_inflight[s_idx]
            && now_ns.0 >= rt.sl_retry_after_ns[s_idx]
        {
            if let Some(safe_sl) = select_safe_live_sl_ticks(
                side,
                avg_price_ticks,
                rt.verified_exchange_sl[s_idx],
                engines[s_idx].last_eff_sl_ticks,
                snap,
            ) {
                engines[s_idx].last_eff_sl_ticks = safe_sl;
                if gw.try_send(GatewayCommand::EnforceStopLoss {
                    slot_idx: s_idx,
                    sl_price: safe_sl,
                }) {
                    rt.slot_rpc_inflight[s_idx] = true;
                    rt.sl_retry_after_ns[s_idx] = now_ns.0 + 1_200_000_000;
                }
            } else {
                rt.sl_needs_sync[s_idx] = false;
            }
        }
        let (preempt_peg, next_sync_delay_ns) = compute_exit_sync_schedule_with_fees(
            &quote,
            resting_exit_price,
            avg_price_ticks,
            engines[s_idx].active_fees,
        );
        if !rt.slot_rpc_inflight[s_idx]
            && (preempt_peg || now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns)
            && now_ns.0.saturating_sub(*last_rest_dispatch_ns) >= 55_000_000
        {
            *last_rest_dispatch_ns = now_ns.0;
            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + next_sync_delay_ns;
            if gw.try_send(GatewayCommand::SyncOpenSlot {
                slot_idx: s_idx,
                side,
                target_px: quote.target_price,
                target_lots: quote.order_lots,
                defensive_lock: quote.urgent_close_repeg,
                cur_gross_bps: quote.cur_gross_bps,
                vac_level: quote.vac_level,
                opened_ts_ns: opened_ts.0,
                tranche1_harvested,
                fees: engines[s_idx].active_fees,
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
        let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
        gov.observe_closed_trade_at_entry(
            s_idx,
            summary.side,
            summary.entry_price,
            summary.net_scaled_bps,
            non_punitive,
            now_ns,
        );
        arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
        dispatch_closed_trade_logs(
            s_idx,
            &summary,
            &engines[s_idx],
            &entry_forensics[s_idx],
            snap,
            ledger,
            gov,
            vault_tx,
            &mut rt.halt_locked,
            now_ns,
        );
        dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
        let _ = tg_tx.try_send(format_telegram_close_message(
            spec,
            &summary,
            engines[s_idx].last_exit_sub_reason,
            ledger.closed_trades_count,
            ledger,
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
    let mut open_count = 0_usize;
    let mut pending_entry_count = 0_usize;
    let mut transitioned_mask = 0_u16;

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 {
            continue;
        }
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
            OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, sl_trigger_price, resting_since_ts, ..
            } => {
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
                && cached_snaps[s_idx].sequence_id > 0
                && (transitioned_mask & (1_u16 << s_idx)) == 0
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::WarmupOrLock);
            }
        }
        return (open_count, pending_entry_count);
    }

    let mut beta_guard = build_monetary_beta_guard(rt.wallet_1e8, engines, cached_snaps);
    let (_, _, active_slots) = beta_guard.open_counts();
    if active_slots >= 6 {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                && cached_snaps[s_idx].sequence_id > 0
                && (transitioned_mask & (1_u16 << s_idx)) == 0
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger);
            }
        }
        return (open_count, pending_entry_count);
    }

    let fees = gov.active_fee_schedule();
    let btc_vol = btc_snap.realized_vol_5m.0.max(2_500);
    let mut armed_this_tick = 0_usize;
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let (_, _, cur_tot) = beta_guard.open_counts();
        if cur_tot >= 6 || armed_this_tick >= 1 {
            break;
        }
        if !matches!(engines[s_idx].state, OrderLifecycleState::Idle)
            || rt.slot_rpc_inflight[s_idx]
            || (transitioned_mask & (1_u16 << s_idx)) != 0
        {
            continue;
        }
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 {
            continue;
        }
        let comp_raw = OrthogonalFactorState::snr_composite_score(&snap);
        let probe_side = if comp_raw >= 0.0 { Side::Bid } else { Side::Ask };
        let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
        let verdict = OrthogonalFactorState::evaluate_entry_signal_elastic(
            spec,
            &snap,
            &btc_snap,
            ScaledBps(engines[s_idx].learned_mfe_bps),
            ScaledBps(probe_edge),
        );
        if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
            if !passes_elliott_macro_coherence_guard(side, &snap) {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::MacroElliottContradiction);
                continue;
            }
            if !passes_order_block_wall_guard(s_idx, side, &snap) {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::OpposingOrderBlockWall);
                continue;
            }
            let cand_lots = gov.compute_slot_entry_lots(s_idx, side, rt.wallet_1e8, &snap);
            let cand_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
            let cand_risk_ppm =
                slot_position_risk_ppm(spec, rt.wallet_1e8, cand_px, cand_lots, snap.realized_vol_5m);
            let cand_beta = slot_btc_beta_x1000(s_idx, snap.realized_vol_5m, btc_vol);
            if !beta_guard.can_open_with_risk_ppm(s_idx, side, cand_risk_ppm, cand_beta)
                || !can_stage_same_direction_entry(s_idx, side, now_ns, engines, cached_snaps)
            {
                publish_slot_veto_code(arena, s_idx, LastVetoCode::BetaOrSameSideStagger);
                continue;
            }
            if !gov.authorizes_adaptive_entry_with_wallet(s_idx, side, rt.wallet_1e8, &snap, &btc_snap)
            {
                let (_, sl_st, _) = gov.slot_direction_diagnostics(s_idx, side);
                let btc_signed =
                    OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64();
                let g_veto = if gov.is_in_scar_debt(s_idx, side)
                    || sl_st > 0
                    || now_ns.0 < engines[s_idx].cooldown_until_ns.0
                {
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
            let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
            let btc_signed =
                OrthogonalFactorState::snr_composite_score(&btc_snap) * probe_side.sign_f64();
            let s_veto = if !snap.is_eligible
                || !spec.validate_pair_eligibility_dynamic(
                    mid_px,
                    snap.realized_vol_5m,
                    taker_pm,
                    fees,
                )
            {
                LastVetoCode::Friction3xOrEligibility
            } else if snap.wave_climax_active || snap.factor_geom.get().abs() >= 0.94 {
                LastVetoCode::SpoofingVeto
            } else if !passes_elliott_macro_coherence_guard(probe_side, &snap) {
                LastVetoCode::MacroElliottContradiction
            } else if !passes_order_block_wall_guard(s_idx, probe_side, &snap) {
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
    let lock_watch = Arc::new(AtomicLockWatch::new(
        Path::new(HALT_LOCK_PATH).exists(),
        Path::new(DRAIN_LOCK_PATH).exists(),
    ));
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
    let mut last_wallet_sync_ns = 0_i64;
    let mut last_rest_dispatch_ns = 0_i64;

    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);
    let saved_mfe = load_mfe_plasticity_from_path(
        Path::new(MFE_PLASTICITY_VAULT_PATH),
        engines[0].learned_mfe_bps,
    );
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        engines[s_idx].learned_mfe_bps = saved_mfe[s_idx];
    }

    let boot_ns = now_epoch_ns();
    let handshake = run_daemon_startup_handshake(live_capital, boot_ns);
    gov.set_active_fee_schedule(handshake.fee_schedule);
    for eng in &mut engines {
        eng.sync_governor_dynamics(&gov);
    }

    let prewarmed_slots = gov.load_ema_prewarm_file(boot_ns);
    if live_capital {
        let warmup_ns = if prewarmed_slots >= MAX_ACTIVE_PAIRS {
            15_000_000_000_i64
        } else {
            75_000_000_000_i64
        };
        gov.set_entry_warmup_until_ns(boot_ns.0.saturating_add(warmup_ns));
        println!(
            "{}\u{1f7e2} [EMA PREWARM V4.5] Hydrated {}/12 slots | Fees M/T: {}/{} | L2 stabilize: {}s{}",
            ANSI_GREEN,
            prewarmed_slots,
            handshake.fee_schedule.maker_fee_bps.raw(),
            handshake.fee_schedule.taker_fee_bps.raw(),
            warmup_ns / 1_000_000_000,
            ANSI_RESET
        );
    }
    bootstrap_live_adopt_if_enabled(
        live_capital, arena, &gov, &mut rt, &mut entry_forensics, &mut engines,
    );
    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);
    println!(
        "{}\u{1f7e2} [VORTEX-OMEGA V4.5 200.0 Hz ARMED] Queue Bound + Monetary Beta + Plasticity + SHM v15.{}",
        ANSI_GREEN, ANSI_RESET
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();
        rt.halt_locked = rt.halt_locked || lock_watch.is_halt_locked();
        rt.drain_active = lock_watch.is_drain_active();

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot_ref) = arena.slot(s_idx) {
                if let Ok(mut snap) = slot_ref.read_snapshot(4) {
                    if !handshake.instruments.is_slot_tradable(s_idx) {
                        snap.is_eligible = false;
                    }
                    cached_snaps[s_idx] = snap;
                    if snap.sequence_id != last_seq[s_idx] {
                        last_seq[s_idx] = snap.sequence_id;
                        gov.observe_market_price(s_idx, &snap);
                        gov.step_regime_shift_shadow_rehab(s_idx, &snap);
                    }
                }
            }
        }
        if let Some(ref gw) = gateway {
            while let Some(evt) = gw.try_recv() {
                handle_gateway_event(
                    evt, now_ns, Some(gw), &mut rt, &cached_snaps, &entry_forensics, &mut engines,
                    &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
                );
            }
        }
        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            gov.save_ema_prewarm_file(now_ns);
            let mut mfe_arr = [0_i64; MAX_ACTIVE_PAIRS];
            for i in 0..MAX_ACTIVE_PAIRS {
                mfe_arr[i] = engines[i].learned_mfe_bps;
            }
            let _ = vault_io_tx.try_send(VaultIoCommand::SaveMfePlasticity {
                ts_ns: now_ns.0,
                learned_mfe_bps: mfe_arr,
            });
            if let Some(ref gw) = gateway {
                let _ = gw.try_send(GatewayCommand::SyncWallet);
            }
        }

        let prev_halt = rt.halt_locked;
        let (open_count, pending_entry_count) = step_12_slot_daemon_tick(
            now_ns, gateway.as_ref(), &mut rt, &mut last_rest_dispatch_ns, &cached_snaps,
            &mut entry_forensics, &mut engines, &mut gov, &mut ledger, arena, &tg_tx, &vault_tx,
        );
        if !prev_halt && rt.halt_locked {
            lock_watch.latch_halt_in_ram();
        }

        let any_inflight = rt.slot_rpc_inflight.iter().any(|&b| b);
        if rt.drain_active && open_count == 0 && pending_entry_count == 0 && !any_inflight {
            let _ = tg_tx.try_send(
                "\u{1f7e2} [VORTEX-L3 DRAIN COMPLETE] 0 open positions verified (Wallet == Equity)."
                    .to_string(),
            );
            let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(1);
            if vault_tx.send(VaultPersistTask::FlushSync(ack_tx)).is_ok() {
                let _ = ack_rx.recv_timeout(Duration::from_secs(2));
            }
            thread::sleep(Duration::from_millis(120));
            println!(
                "{}\u{1f7e2} [DRAIN_COMPLETE_STOPPED] 0 open positions verified. Vault flushed.{}",
                ANSI_GREEN, ANSI_RESET
            );
            return ExitCode::SUCCESS;
        }

        if is_safe_halt_standby(rt.halt_locked, open_count, pending_entry_count, any_inflight) {
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
                "{}\u{26aa} [VORTEX-L3 LIVE HUD] SHM inactive ({}) \u{2014} daemons stopped.{}",
                ANSI_GRAY,
                shm_path.display(),
                ANSI_RESET
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
    println!("{}", sep);
    println!("[VORTEX-EXEC V4.5] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
    println!("{}", sep);
    println!("  {}\u{1f7e2} -> OOS Closed Trades Verified : {} (Gate >= 500){}", ANSI_GREEN, oos_trades, ANSI_RESET);
    println!("  {}\u{1f7e2} -> Maker Exit Ratio           : {:.2}% (Gate >= 80.00%){}", ANSI_GREEN, maker_ratio, ANSI_RESET);
    println!("  {}\u{1f7e2} -> Net Profit Factor (PostFee): {:.2} (Gate >= 1.35){}", ANSI_GREEN, pf, ANSI_RESET);
    println!(
        "  {}\u{1f7e2} -> Cumulative Net PnL (OOS)   : + {:.4} USDT | PARITY_SYNC Drift: {} units{}",
        ANSI_GREEN, cum_usdt, drift, ANSI_RESET
    );
    println!("{}", sep);
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
    fn test_p0b_halt_custody_and_live_daemon_ssot_risk_parity_arming() {
        assert!(!is_safe_halt_standby(true, 1, 0, false));
        assert!(is_safe_halt_standby(true, 0, 0, false));

        let shm_path = MappedShmRegion::ephemeral_self_test_path("p0b_ssot_53b");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());

        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let mut gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut rt = ExecRuntimeState::new();
        rt.halt_locked = true;
        rt.drain_active = false;

        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        for s in 0..MAX_ACTIVE_PAIRS {
            snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
            snaps[s].is_eligible = true;
            snaps[s].sequence_id = 10;
            snaps[s].ts_local_ns = TimestampNs(5_000_000_000);
            snaps[s].realized_vol_5m = ScaledBps(7_000);
            snaps[s].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(450) };
            snaps[s].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(350) };
            snaps[s].refresh_deep_l1_l5_totals();
            snaps[s].mark_price = PriceTicks(8_200);
            snaps[s].last_trade_price = PriceTicks(8_200);
            snaps[s].factor_ofi = BoundedScore::new_clamped(0.85);
            snaps[s].factor_tfi = BoundedScore::new_clamped(0.88);
            snaps[s].factor_geom = BoundedScore::new_clamped(0.55);
        }
        snaps[0].bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(450) };
        snaps[0].asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(350) };
        snaps[0].refresh_deep_l1_l5_totals();
        snaps[0].mark_price = PriceTicks(759_000);
        snaps[0].last_trade_price = PriceTicks(759_000);

        let v_bid = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(engines[2].try_arm_maker_entry(&snaps[2], v_bid, LotQty(4)).is_ok());
        assert!(engines[2].on_entry_order_resting(TimestampNs(4_000_000_000)).is_ok());
        assert!(engines[3].try_arm_maker_entry(&snaps[3], v_bid, LotQty(1)).is_ok());
        assert!(engines[3].on_maker_entry_filled(TimestampNs(4_500_000_000)).is_ok());

        let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
        let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultPersistTask>(16);
        let mut forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut last_rest = 0_i64;

        if let Ok(ref r) = reg {
            let (open_1, _) = step_12_slot_daemon_tick(
                TimestampNs(6_000_000_000), None, &mut rt, &mut last_rest, &snaps,
                &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert_eq!(engines[2].state, OrderLifecycleState::Idle);
            assert_eq!(open_1, 1);

            snaps[3].ts_local_ns = TimestampNs(90_000_000_000);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_209), qty: LotQty(400) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_210), qty: LotQty(300) };
            snaps[3].mark_price = PriceTicks(8_210);
            snaps[3].last_trade_price = PriceTicks(8_210);
            let (open_2, pend_2) = step_12_slot_daemon_tick(
                TimestampNs(90_000_000_000), None, &mut rt, &mut last_rest, &snaps,
                &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert_eq!(open_2, 0);
            assert_eq!(pend_2, 0);

            rt.halt_locked = false;
            snaps[1].sequence_id = 0;
            snaps[2].sequence_id = 0;
            snaps[0].factor_ofi = BoundedScore::new_clamped(0.60);
            snaps[0].factor_tfi = BoundedScore::new_clamped(0.62);
            snaps[0].factor_geom = BoundedScore::new_clamped(0.30);
            snaps[3].ts_local_ns = TimestampNs(200_000_000_000);
            snaps[3].realized_vol_5m = ScaledBps(15_000);
            let expected_hi_vol_lots =
                gov.compute_slot_entry_lots(3, Side::Bid, rt.wallet_1e8, &snaps[3]);
            let (_, pend_3) = step_12_slot_daemon_tick(
                TimestampNs(200_000_000_000), None, &mut rt, &mut last_rest, &snaps,
                &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert!(pend_3 >= 1);
            if let OrderLifecycleState::PlacingEntryMaker { qty, .. } = engines[3].state {
                assert_eq!(qty, expected_hi_vol_lots);
            } else {
                assert_eq!(pend_3, 999);
            }
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
        snap.mark_price = PriceTicks(8_227);
        snap.last_trade_price = PriceTicks(8_227);
        assert_eq!(
            select_safe_live_sl_ticks(Side::Bid, PriceTicks(8_200), PriceTicks(8_120), PriceTicks(8_226), &snap),
            Some(PriceTicks(8_224))
        );
        assert!(select_safe_live_sl_ticks(
            Side::Bid, PriceTicks(8_200), PriceTicks(8_226), PriceTicks(8_226), &snap
        ).is_none());

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
        assert_eq!(
            compute_exit_sync_schedule(&snap_quote, PriceTicks(766_400), PriceTicks(759_000)),
            (true, 250_000_000)
        );
        let float_quote = CanonicalExitQuote {
            role: ExitOrderRole::StandardMakerExit100,
            target_price: PriceTicks(766_440),
            order_lots: LotQty(1),
            sub_reason: ExitSubReason::FibonacciImpulseSurf,
            vac_level: 3,
            defensive_lock_active: false,
            urgent_close_repeg: false,
            cur_gross_bps: 4_200,
            wald_pct: 0,
        };
        assert_eq!(
            compute_exit_sync_schedule(&float_quote, PriceTicks(766_400), PriceTicks(759_000)),
            (false, 1_800_000_000)
        );
    }

    #[test]
    fn test_live_200hz_hawkes_anti_pickoff_and_stoikov_repeg_veto() {
        let shm_path = MappedShmRegion::ephemeral_self_test_path("hawkes_daemon_53b");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());

        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let mut gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut rt = ExecRuntimeState::new();
        rt.halt_locked = false;
        rt.drain_active = false;

        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        snaps[0].symbol_id = APPROVED_SPECS[0].symbol_id;
        snaps[0].sequence_id = 1;
        snaps[0].realized_vol_5m = ScaledBps(6_000);
        snaps[0].bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(400) };
        snaps[0].asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(300) };
        snaps[0].factor_ofi = BoundedScore::new_clamped(0.50);
        snaps[0].factor_tfi = BoundedScore::new_clamped(0.55);

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

        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(680),
        };
        assert!(engines[3].try_arm_maker_entry(&snaps[3], verdict, LotQty(26)).is_ok());
        assert!(engines[3].on_entry_order_resting(TimestampNs(1_000_000_000)).is_ok());
        rt.init_q_ahead[3] = LotQty(500);
        rt.cur_q_ahead[3] = LotQty(500);
        rt.last_l1_q_obs[3] = LotQty(500);
        rt.last_opp_trades[3] = LotQty(0);

        let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
        let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultPersistTask>(16);
        let mut forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        forensics[3].armed_ts_ns = 1_000_000_000;
        let mut last_rest = 0_i64;

        if let Ok(ref r) = reg {
            snaps[3].ts_local_ns = TimestampNs(1_050_000_000);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(60) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(450) };
            snaps[3].deep_bid_lots_l1_l5 = LotQty(200);
            snaps[3].deep_ask_lots_l1_l5 = LotQty(1_400);
            snaps[3].cum_sell_trade_lots = LotQty(440);
            snaps[3].last_trade_price = PriceTicks(8_200);
            snaps[3].factor_ofi = BoundedScore::new_clamped(0.42);
            snaps[3].factor_tfi = BoundedScore::new_clamped(0.40);

            let _ = step_12_slot_daemon_tick(
                TimestampNs(1_050_000_000), None, &mut rt, &mut last_rest, &snaps,
                &mut forensics, &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            assert_eq!(engines[3].state, OrderLifecycleState::Idle);
            assert_eq!(r.arena().exec_abort_count.load(Ordering::Acquire), 1);
            if let Ok(tel3) = r.arena().telemetry(3) {
                assert_eq!(
                    LastVetoCode::from_code(tel3.read_consistent_or_default(4).last_veto_code),
                    LastVetoCode::HawkesStoikovQueueAbort
                );
            }
        }
        let _ = std::fs::remove_file(&shm_path);
    }

    #[test]
    fn test_v45_queue_upper_bound_identity_elastic_stagger_monetary_beta_and_shm_veto_codes() {
        let (q_after_pull, hit0, obs_after) = step_l1_queue_ahead_lots(
            Side::Bid, PriceTicks(8_200), true, LotQty(18), LotQty(500), LotQty(350), 0, PriceTicks(8_201),
        );
        assert_eq!(hit0, LotQty(0));
        assert_eq!(obs_after, LotQty(18));
        assert!(q_after_pull.0 >= 1 && q_after_pull.0 <= 18);

        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        snaps[2].symbol_id = APPROVED_SPECS[2].symbol_id;
        snaps[2].mark_price = PriceTicks(8_260);
        snaps[2].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(400) };
        snaps[2].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(350) };
        let v_open = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(680),
        };
        assert!(engines[2].try_arm_maker_entry(&snaps[2], v_open, LotQty(10)).is_ok());
        assert!(engines[2].on_maker_entry_filled(TimestampNs(10_000_000_000)).is_ok());
        engines[2].intrinsic_tau_mkt_s = 45;
        assert!(can_stage_same_direction_entry(3, Side::Bid, TimestampNs(35_000_000_000), &engines, &snaps));
        engines[2].intrinsic_tau_mkt_s = 400;
        assert!(!can_stage_same_direction_entry(3, Side::Bid, TimestampNs(35_000_000_000), &engines, &snaps));

        let shm_path = MappedShmRegion::ephemeral_self_test_path("v45_veto_beta");
        if let Ok(r) = MappedShmRegion::open_or_create(&shm_path, true) {
            publish_slot_veto_code(r.arena(), 5, LastVetoCode::BetaOrSameSideStagger);
            if let Ok(t5) = r.arena().telemetry(5) {
                assert_eq!(
                    LastVetoCode::from_code(t5.read_consistent_or_default(4).last_veto_code),
                    LastVetoCode::BetaOrSameSideStagger
                );
            }
            let bg = build_monetary_beta_guard(DEFAULT_WALLET_EQUITY_1E8, &engines, &snaps);
            assert_eq!(bg.open_counts(), (1, 0, 1));
            assert!(bg.total_open_risk_ppm() >= 1);
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
