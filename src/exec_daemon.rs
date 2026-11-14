//! 200Hz Autonomous 12-Slot Execution Daemon with Hawkes Queue Shield, Wald v2 & Chandelier.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::path::Path;
use std::process::ExitCode;
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::Duration;

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::bybit_gateway::{BybitAsyncGateway, GatewayCommand};
use crate::exec_gateway_handlers::{
    bootstrap_live_adopt_if_enabled, handle_gateway_event, promote_verified_fill_state,
    run_oos_520_certification_suite, ExecRuntimeState,
};
use crate::execution_fsm::{
    passes_stoikov_repeg_coherence_guard, CanonicalExitQuote, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, ServerSlDecision, SlotExecutionEngine,
};
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::telemetry_hud::{
    dispatch_closed_trade_logs, dispatch_open_positions_state, format_telegram_close_message,
    record_and_notify_entry_fill, rehydrate_shadow_ledger, render_live_hud,
    render_trades_and_executive_dashboard, spawn_bounded_telegram_notifier,
    spawn_bounded_vault_persister, SlotForensicEntrySnapshot, VaultPersistTask,
    DEFAULT_WALLET_EQUITY_1E8, DRAIN_LOCK_PATH, HALT_LOCK_PATH,
};
use crate::types::{
    now_epoch_ns, LotQty, PairSpec, PortfolioBetaGuard, PriceTicks, ScaledBps, Side, TimestampNs,
    ANSI_GRAY, ANSI_GREEN, ANSI_RESET, APPROVED_SPECS,
};

/// P0-B Law III & V Predicate: Low-power standby allowed ONLY at zero open/queued/inflight exposure.
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

/// Computes `(preempt_now, next_cooldown_ns)` for 200Hz exit sync with Snap-In preemption & VIP-0 float hysteresis.
#[inline]
#[must_use]
pub const fn compute_exit_sync_schedule(
    quote: &CanonicalExitQuote,
    prev_resting_exit_px: PriceTicks,
    avg_price_ticks: PriceTicks,
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
        if drift_bps < 120 {
            return (false, 1_800_000_000);
        }
    }
    (false, 1_350_000_000)
}

/// Validates live anti-10001 geometry & strict monotonicity against `verified_exchange_sl` before SL RPC.
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
            let strict_lo = ref_mark.0.min(ref_last.0);
            target = PriceTicks(target.0.min(strict_lo - 3).max(1));
            if verified_exchange_sl.0 > 0 && target.0 <= verified_exchange_sl.0 {
                return None;
            }
        }
        Side::Ask => {
            let strict_hi = ref_mark.0.max(ref_last.0);
            target = PriceTicks(target.0.max(strict_hi.saturating_add(3)));
            if verified_exchange_sl.0 > 0 && target.0 >= verified_exchange_sl.0 {
                return None;
            }
        }
    }
    PairSpec::validate_stop_loss_geometry(side, target, ref_last, ref_mark).ok()
}

/// Executes one deterministic 200Hz custody & entry tick across all 12 slots.
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

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = cached_snaps[s_idx];
        if snap.sequence_id == 0 {
            continue;
        }
        match engines[s_idx].state {
            OrderLifecycleState::Idle => {}
            OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                if rt.slot_rpc_inflight[s_idx] {
                    pending_entry_count += 1;
                    continue;
                }
                if block_new_entries {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }
                pending_entry_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                let wait_ns = engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m);
                if now_ns.0.saturating_sub(submitted_ts.0) < wait_ns {
                    continue;
                }
                let Ok((e_kin, e_min)) = gov.confirm_preflight_kinematics_non_choking(
                    s_idx, side, rt.wallet_1e8, &snap, &btc_snap,
                ) else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                };
                let Ok((synced_entry_px, synced_sl_px)) =
                    engines[s_idx].sync_preflight_l1_entry(&snap, 8)
                else {
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                };
                let (q_l1, opp_cum) = match side {
                    Side::Bid => (snap.bids[0].qty, snap.cum_sell_trade_lots),
                    Side::Ask => (snap.asks[0].qty, snap.cum_buy_trade_lots),
                };
                let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                    armed_ts_ns: now_ns.0,
                    init_q_lots: q_l1.0,
                    ofi_in: snap.factor_ofi.get(),
                    tfi_in: snap.factor_tfi.get(),
                    geom_in: snap.factor_geom.get(),
                    snr_in: OrthogonalFactorState::snr_composite_score(&snap),
                    vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                    spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                    btc_comp_in: OrthogonalFactorState::snr_composite_score(&btc_snap),
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
            }
            OrderLifecycleState::RestingEntryQueue {
                side, entry_price, qty, sl_trigger_price, resting_since_ts, ..
            } => {
                pending_entry_count += 1;
                let (at_l1, live_l1_q, cur_opp_cum) = match side {
                    Side::Bid => (
                        snap.bids[0].price == entry_price,
                        snap.bids[0].qty,
                        snap.cum_sell_trade_lots,
                    ),
                    Side::Ask => (
                        snap.asks[0].price == entry_price,
                        snap.asks[0].qty,
                        snap.cum_buy_trade_lots,
                    ),
                };
                let tape_delta = (cur_opp_cum.0 - rt.last_opp_trades[s_idx].0).max(0);
                rt.last_opp_trades[s_idx] = cur_opp_cum;
                let hits_px = match side {
                    Side::Bid => snap.last_trade_price.0 > 0 && snap.last_trade_price.0 <= entry_price.0,
                    Side::Ask => snap.last_trade_price.0 >= entry_price.0,
                };
                let opp_tape_hit_lots = if tape_delta > 0 && hits_px {
                    rt.cur_q_ahead[s_idx] = LotQty((rt.cur_q_ahead[s_idx].0 - tape_delta).max(0));
                    LotQty(tape_delta)
                } else {
                    LotQty(0)
                };
                if at_l1 {
                    if rt.last_l1_q_obs[s_idx].0 > 0 && live_l1_q.0 < rt.last_l1_q_obs[s_idx].0 {
                        let drop_25 = (rt.last_l1_q_obs[s_idx].0 - live_l1_q.0) / 4;
                        if rt.cur_q_ahead[s_idx].0 > 1 && drop_25 > 0 {
                            rt.cur_q_ahead[s_idx] = LotQty((rt.cur_q_ahead[s_idx].0 - drop_25).max(1));
                        }
                    }
                    rt.last_l1_q_obs[s_idx] = live_l1_q;
                } else {
                    rt.last_l1_q_obs[s_idx] = LotQty(0);
                }
                if let Ok(tel) = arena.telemetry(s_idx) {
                    tel.store_queue_ahead_lots(rt.cur_q_ahead[s_idx]);
                }
                let q_shield = engines[s_idx].evaluate_resting_queue_shield(
                    side,
                    rt.init_q_ahead[s_idx],
                    rt.cur_q_ahead[s_idx],
                    opp_tape_hit_lots,
                    &snap,
                    &btc_snap,
                );
                if rt.slot_rpc_inflight[s_idx] {
                    continue;
                }

                let resting_elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                let total_chase_ns = if entry_forensics[s_idx].armed_ts_ns > 0 {
                    now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns)
                } else {
                    resting_elapsed_ns
                };
                let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, snap.realized_vol_5m);
                let flow_imp =
                    (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * side.sign_f64();
                let btc_q_snr =
                    OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64();
                let headwind_erosion = s_idx > 0
                    && resting_elapsed_ns >= 2_200_000_000
                    && rt.init_q_ahead[s_idx].0 > 0
                    && rt.cur_q_ahead[s_idx].0 * 100 <= rt.init_q_ahead[s_idx].0 * 65
                    && (btc_q_snr < -0.02 || flow_imp < 0.38);
                let dq_abort = (resting_elapsed_ns >= 25_000_000 && at_l1 && q_shield.abort_now)
                    || headwind_erosion
                    || flow_imp <= -0.42;

                if block_new_entries
                    || resting_elapsed_ns >= (max_chase_ns * 3) / 4
                    || total_chase_ns >= max_chase_ns
                    || dq_abort
                {
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
                    continue;
                }

                if !at_l1
                    && resting_elapsed_ns >= 2_500_000_000
                    && total_chase_ns + 2_500_000_000 < max_chase_ns
                    && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                    && passes_stoikov_repeg_coherence_guard(s_idx, side, &snap, &btc_snap)
                {
                    let new_l1 = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
                    let moved_away = match side {
                        Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= 15,
                        Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= 15,
                    };
                    if moved_away
                        && gov
                            .confirm_preflight_kinematics_non_choking(
                                s_idx, side, rt.wallet_1e8, &snap, &btc_snap,
                            )
                            .is_ok()
                    {
                        if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&snap, new_l1) {
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
                                    continue;
                                }
                            } else {
                                engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                rt.init_q_ahead[s_idx] = live_l1_q;
                                rt.cur_q_ahead[s_idx] = live_l1_q;
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                continue;
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
                                s_idx, spec, engines, entry_forensics, arena, tg_tx, vault_tx,
                                now_ns, eff_s, eff_px, eff_q, "SHADOW MAKER FILL",
                            );
                        }
                    }
                }
            }
            OrderLifecycleState::PositionOpenMakerExit {
                side,
                avg_price_ticks,
                mut peak_favorable_price,
                mut worst_adverse_price,
                open_lots,
                tranche1_harvested,
                resting_exit_price,
                sl_trigger_price,
                opened_ts,
                ..
            } => {
                open_count += 1;
                engines[s_idx].sync_governor_dynamics(gov);
                if let Some(gw) = gateway {
                    engines[s_idx].observe_open_flow_kinematics(&snap);
                    let _ = engines[s_idx].observe_wald_sprt_step(&snap, &btc_snap);
                    let cur_bbo = match side {
                        Side::Bid => snap.asks[0].price,
                        Side::Ask => snap.bids[0].price,
                    };
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
                    if let OrderLifecycleState::PositionOpenMakerExit {
                        peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, ..
                    } = engines[s_idx].state
                    {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let avg_denom = avg_price_ticks.0.max(1);
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0)
                        * side.sign_i64())
                        .saturating_mul(1_000_000)
                        / avg_denom;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0)
                        * side.sign_i64())
                        .saturating_mul(1_000_000)
                        / avg_denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 {
                        engines[s_idx].last_eff_sl_ticks = sl_trigger_price;
                    }

                    if rt.emergency_flatten_sl[s_idx].is_none() {
                        if let Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, .. }) =
                            SlotExecutionEngine::evaluate_sl_or_emergency_flatten(
                                side, avg_price_ticks, &snap,
                            )
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
                        continue;
                    }

                    let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&snap) else {
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    };
                    engines[s_idx].last_closed_wald_pct =
                        engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                    engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                    if engines[s_idx]
                        .maybe_ratchet_chandelier_or_wald_sl(&snap, &btc_snap)
                        .is_some()
                    {
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
                            &snap,
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

                    let (preempt_peg, next_sync_delay_ns) =
                        compute_exit_sync_schedule(&quote, resting_exit_price, avg_price_ticks);
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
                        }) {
                            rt.slot_rpc_inflight[s_idx] = true;
                        }
                    }
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    continue;
                }

                if let Some(summary) =
                    engines[s_idx].step_open_position(&snap, &btc_snap, gov, ledger)
                {
                    open_count = open_count.saturating_sub(1);
                    let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                    gov.observe_closed_trade_at_entry(
                        s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                        non_punitive, now_ns,
                    );
                    arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
                    dispatch_closed_trade_logs(
                        s_idx, &summary, &engines[s_idx], &entry_forensics[s_idx], &snap, ledger,
                        gov, vault_tx, &mut rt.halt_locked, now_ns,
                    );
                    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
                    let _ = tg_tx.try_send(format_telegram_close_message(
                        spec, &summary, engines[s_idx].last_exit_sub_reason,
                        ledger.closed_trades_count, ledger,
                    ));
                }
                engines[s_idx].sync_to_shm_telemetry(arena);
            }
        }
    }

    let mut beta_guard = PortfolioBetaGuard::new();
    for eng in engines.iter() {
        if let Some(act_side) = eng.active_side() {
            beta_guard.observe_active_side(act_side);
        }
    }
    if !block_new_entries && beta_guard.total_active() < 6 {
        let mut armed_this_tick = 0_usize;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if beta_guard.total_active() >= 6 || armed_this_tick >= 2 {
                break;
            }
            if !matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                || rt.slot_rpc_inflight[s_idx]
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
                if !beta_guard.can_admit_side(side) {
                    continue;
                }
                if gov.authorizes_adaptive_entry_with_wallet(
                    s_idx, side, rt.wallet_1e8, &snap, &btc_snap,
                ) {
                    let lots = gov.compute_slot_entry_lots(s_idx, side, rt.wallet_1e8, &snap);
                    engines[s_idx].sync_governor_for_side(gov, side);
                    if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_ok()
                        && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
                    {
                        let _ = beta_guard.try_admit(side);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        pending_entry_count += 1;
                        armed_this_tick += 1;
                    }
                }
            }
        }
    }
    (open_count, pending_entry_count)
}

pub fn run_autonomous_12_slot_daemon(arena: &VortexShmArena, live_capital: bool) -> ExitCode {
    let tg_tx = spawn_bounded_telegram_notifier();
    let vault_tx = spawn_bounded_vault_persister();
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut rt = ExecRuntimeState::new();
    let mut cached_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut entry_forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut last_seq = [0_u64; MAX_ACTIVE_PAIRS];
    let mut last_wallet_sync_ns = 0_i64;
    let mut last_rest_dispatch_ns = 0_i64;
    let mut lock_poll_ticks = 0_u32;

    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);
    bootstrap_live_adopt_if_enabled(
        live_capital, arena, &gov, &mut rt, &mut entry_forensics, &mut engines,
    );
    let gateway = BybitAsyncGateway::spawn_if_enabled(live_capital);
    println!(
        "{}\u{1f7e2} [VORTEX-OMEGA 200.0 Hz ARMED] Hawkes Shield + Wald v2 + Shadow-Runner + Chandelier SL.{}",
        ANSI_GREEN,
        ANSI_RESET
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();
        lock_poll_ticks += 1;
        if lock_poll_ticks >= 50 {
            lock_poll_ticks = 0;
            rt.halt_locked = Path::new(HALT_LOCK_PATH).exists();
            rt.drain_active = Path::new(DRAIN_LOCK_PATH).exists();
        }
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot_ref) = arena.slot(s_idx) {
                if let Ok(snap) = slot_ref.read_snapshot(4) {
                    cached_snaps[s_idx] = snap;
                    if snap.sequence_id != last_seq[s_idx] {
                        last_seq[s_idx] = snap.sequence_id;
                        gov.observe_market_price(s_idx, &snap);
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
            if let Some(ref gw) = gateway {
                let _ = gw.try_send(GatewayCommand::SyncWallet);
            }
        }

        let (open_count, pending_entry_count) = step_12_slot_daemon_tick(
            now_ns,
            gateway.as_ref(),
            &mut rt,
            &mut last_rest_dispatch_ns,
            &cached_snaps,
            &mut entry_forensics,
            &mut engines,
            &mut gov,
            &mut ledger,
            arena,
            &tg_tx,
            &vault_tx,
        );

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
    println!("[VORTEX-EXEC V3.0] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
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

        let v_s2 = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(engines[2].try_arm_maker_entry(&snaps[2], v_s2, LotQty(4)).is_ok());
        assert!(engines[2].on_entry_order_resting(TimestampNs(4_000_000_000)).is_ok());

        let v_s3 = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(engines[3].try_arm_maker_entry(&snaps[3], v_s3, LotQty(1)).is_ok());
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
            for s in [1_usize, 2] {
                snaps[s].sequence_id = 0;
            }
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
            if let OrderLifecycleState::PositionOpenMakerExit { .. }
            | OrderLifecycleState::Idle
            | OrderLifecycleState::RestingEntryQueue { .. } = engines[3].state
            {
                assert_eq!(pend_3, 999);
            } else if let OrderLifecycleState::PlacingEntryMaker { qty, .. } = engines[3].state {
                assert_eq!(qty, expected_hi_vol_lots);
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

        let ok_sl = select_safe_live_sl_ticks(
            Side::Bid, PriceTicks(8_200), PriceTicks(8_120), PriceTicks(8_226), &snap,
        );
        assert_eq!(ok_sl, Some(PriceTicks(8_226)));

        snap.mark_price = PriceTicks(8_227);
        snap.last_trade_price = PriceTicks(8_227);
        let clamped_sl = select_safe_live_sl_ticks(
            Side::Bid, PriceTicks(8_200), PriceTicks(8_120), PriceTicks(8_226), &snap,
        );
        assert_eq!(clamped_sl, Some(PriceTicks(8_224)));

        let blocked_reg = select_safe_live_sl_ticks(
            Side::Bid, PriceTicks(8_200), PriceTicks(8_226), PriceTicks(8_226), &snap,
        );
        assert!(blocked_reg.is_none());

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
        let (preempt, cd_fast) =
            compute_exit_sync_schedule(&snap_quote, PriceTicks(766_400), PriceTicks(759_000));
        assert!(preempt);
        assert_eq!(cd_fast, 250_000_000);

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
        let (preempt_fl, cd_hyst) =
            compute_exit_sync_schedule(&float_quote, PriceTicks(766_400), PriceTicks(759_000));
        assert!(!preempt_fl);
        assert_eq!(cd_hyst, 1_800_000_000);
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
            // Inject aggressive sell sweep hitting 8_200 (440 of 500 lots eaten in 50ms!)
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
            // Must abort BEFORE filling the toxic sweep!
            assert_eq!(engines[3].state, OrderLifecycleState::Idle);
            assert_eq!(r.arena().exec_abort_count.load(Ordering::Acquire), 1);
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
