//! Live Gateway Event Handlers, Chandelier ACK/Rollback, Wave-3 Sync & OOS 520 Gate.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::path::Path;
use std::sync::mpsc::SyncSender;

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::bybit_client::BybitV5LiveClient;
use crate::bybit_gateway::{
    verify_ws_private_parser_self_check, BybitAsyncGateway, GatewayCommand, GatewayEvent,
};
use crate::execution_fsm::{
    defensive_zero_loss_maker_exit_price, is_chandelier_profit_locked_sl,
    offensive_win_maker_exit_price, parse_verified_avg_price_ticks, ClosedPnlQueryOutcome,
    ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState, ServerSlDecision,
    SlotExecutionEngine, VerifiedCloseResolution, MAX_CLOSE_RECONCILE_RETRIES,
};
use crate::forensic_parquet::ParitySyncLedger;
use crate::http_ws_transport::OnlineIndexerLagEstimator;
use crate::lob_shm::{BookLevel, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::telemetry_hud::{
    dispatch_closed_trade_logs, dispatch_open_positions_state, format_telegram_close_message,
    load_saved_slot_state, record_and_notify_entry_fill, restore_entry_forensics_from_json,
    SlotForensicEntrySnapshot, VaultPersistTask, DEFAULT_WALLET_EQUITY_1E8, DRAIN_LOCK_PATH,
    HALT_LOCK_PATH,
};
use crate::types::{
    classify_trade_badge, now_epoch_ns, BoundedScore, LotQty, PortfolioBetaGuard, PriceTicks,
    ScaledBps, Side, TimestampNs, ANSI_GREEN, ANSI_RED, ANSI_RESET, ANSI_YELLOW, APPROVED_SPECS,
};

#[derive(Debug)]
pub struct ExecRuntimeState {
    pub idx_lag: OnlineIndexerLagEstimator,
    pub wallet_1e8: i64,
    pub slot_rpc_inflight: [bool; MAX_ACTIVE_PAIRS],
    pub abort_cmd_inflight: [bool; MAX_ACTIVE_PAIRS],
    pub sl_needs_sync: [bool; MAX_ACTIVE_PAIRS],
    pub verified_exchange_sl: [PriceTicks; MAX_ACTIVE_PAIRS],
    pub emergency_flatten_sl: [Option<PriceTicks>; MAX_ACTIVE_PAIRS],
    pub sl_retry_after_ns: [i64; MAX_ACTIVE_PAIRS],
    pub init_q_ahead: [LotQty; MAX_ACTIVE_PAIRS],
    pub cur_q_ahead: [LotQty; MAX_ACTIVE_PAIRS],
    pub last_l1_q_obs: [LotQty; MAX_ACTIVE_PAIRS],
    pub last_opp_trades: [LotQty; MAX_ACTIVE_PAIRS],
    pub halt_locked: bool,
    pub drain_active: bool,
}

impl Default for ExecRuntimeState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl ExecRuntimeState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            idx_lag: OnlineIndexerLagEstimator::new(),
            wallet_1e8: DEFAULT_WALLET_EQUITY_1E8,
            slot_rpc_inflight: [false; MAX_ACTIVE_PAIRS],
            abort_cmd_inflight: [false; MAX_ACTIVE_PAIRS],
            sl_needs_sync: [false; MAX_ACTIVE_PAIRS],
            verified_exchange_sl: [PriceTicks(0); MAX_ACTIVE_PAIRS],
            emergency_flatten_sl: [None; MAX_ACTIVE_PAIRS],
            sl_retry_after_ns: [0; MAX_ACTIVE_PAIRS],
            init_q_ahead: [LotQty(0); MAX_ACTIVE_PAIRS],
            cur_q_ahead: [LotQty(0); MAX_ACTIVE_PAIRS],
            last_l1_q_obs: [LotQty(0); MAX_ACTIVE_PAIRS],
            last_opp_trades: [LotQty(0); MAX_ACTIVE_PAIRS],
            halt_locked: Path::new(HALT_LOCK_PATH).exists(),
            drain_active: Path::new(DRAIN_LOCK_PATH).exists(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LiveCloseStepAction {
    AwaitNextPoll { next_poll_delay_ns: i64 },
    FinalizeVerified(VerifiedCloseResolution),
}

#[must_use]
pub fn promote_verified_fill_state(
    eng: &mut SlotExecutionEngine,
    fill_ts: TimestampNs,
    verified_side: Option<Side>,
    verified_avg: Option<PriceTicks>,
    verified_lots: Option<LotQty>,
) -> Option<(Side, PriceTicks, LotQty)> {
    let _ = eng.on_maker_entry_filled_verified(fill_ts, verified_side, verified_avg, verified_lots);
    if let OrderLifecycleState::PositionOpenMakerExit {
        side,
        avg_price_ticks,
        open_lots,
        ..
    } = eng.state
    {
        Some((side, avg_price_ticks, open_lots))
    } else {
        None
    }
}

/// P0-A Law V: Evaluates post-fill SL and arms EmergencyFlattenSlot if price breached 1.5x bound.
pub fn apply_post_fill_sl_or_breach(
    slot_idx: usize,
    eff_side: Side,
    eff_px: PriceTicks,
    sl_verified_by_gateway: bool,
    prev_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
    eng: &mut SlotExecutionEngine,
    rt: &mut ExecRuntimeState,
) {
    match SlotExecutionEngine::evaluate_sl_or_emergency_flatten(eff_side, eff_px, snap) {
        Ok(ServerSlDecision::ArmServerSl(valid_sl)) => {
            eng.last_eff_sl_ticks = valid_sl;
            eng.chandelier_profit_sl_armed =
                is_chandelier_profit_locked_sl(eff_side, eff_px, valid_sl);
            if let OrderLifecycleState::PositionOpenMakerExit {
                sl_trigger_price: ref mut sl_p,
                ..
            } = eng.state
            {
                *sl_p = valid_sl;
            }
            rt.emergency_flatten_sl[slot_idx] = None;
            if sl_verified_by_gateway && valid_sl == prev_sl {
                rt.verified_exchange_sl[slot_idx] = valid_sl;
                rt.sl_needs_sync[slot_idx] = false;
            } else {
                if sl_verified_by_gateway && prev_sl.0 > 0 {
                    rt.verified_exchange_sl[slot_idx] = prev_sl;
                }
                rt.sl_needs_sync[slot_idx] = true;
            }
        }
        Ok(ServerSlDecision::BreachEmergencyFlatten {
            backup_sl,
            observed_loss_bps,
            max_allowed_bps,
        }) => {
            eng.last_eff_sl_ticks = backup_sl;
            eng.chandelier_profit_sl_armed = false;
            if let OrderLifecycleState::PositionOpenMakerExit {
                sl_trigger_price: ref mut sl_p,
                ..
            } = eng.state
            {
                *sl_p = backup_sl;
            }
            rt.sl_needs_sync[slot_idx] = false;
            rt.emergency_flatten_sl[slot_idx] = Some(backup_sl);
            println!(
                "{}\u{1f534} [P0-A BREACH FLATTEN ARMED] Slot {:2} {} {:?} loss={} > max={} ScaledBps (BackupSL={}t){}",
                ANSI_RED,
                slot_idx,
                APPROVED_SPECS[slot_idx].symbol_id.name(),
                eff_side,
                observed_loss_bps.raw(),
                max_allowed_bps.raw(),
                backup_sl.0,
                ANSI_RESET
            );
        }
        Err(_) => {
            if !sl_verified_by_gateway {
                rt.sl_needs_sync[slot_idx] = true;
            }
        }
    }
}

#[must_use]
pub fn resolve_live_closed_position_step(
    eng: &mut SlotExecutionEngine,
    snap: &SymbolLobSnapshot,
    closed_pnl_payload: Option<&serde_json::Value>,
    vac_level_hint: usize,
    idx_lag: &mut OnlineIndexerLagEstimator,
    observed_at_ns: i64,
) -> Option<LiveCloseStepAction> {
    if !matches!(eng.state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
        return None;
    }
    let attempt = eng.close_reconcile_retries.saturating_add(1);
    let allow_partial = attempt >= MAX_CLOSE_RECONCILE_RETRIES;
    let outcome = match closed_pnl_payload {
        Some(payload) => {
            idx_lag.observe_closed_pnl_payload(payload, observed_at_ns);
            eng.reconcile_closed_pnl_payload(payload, vac_level_hint, allow_partial)
        }
        None => ClosedPnlQueryOutcome::TransportOrApiError,
    };
    match outcome {
        ClosedPnlQueryOutcome::Verified(res) => {
            eng.close_reconcile_retries = 0;
            Some(LiveCloseStepAction::FinalizeVerified(res))
        }
        ClosedPnlQueryOutcome::PendingIndexerRetry | ClosedPnlQueryOutcome::TransportOrApiError => {
            if attempt < MAX_CLOSE_RECONCILE_RETRIES {
                idx_lag.observe_empty_retry();
                eng.close_reconcile_retries = attempt;
                Some(LiveCloseStepAction::AwaitNextPoll {
                    next_poll_delay_ns: idx_lag.adaptive_poll_delay_ns(attempt),
                })
            } else {
                eng.close_reconcile_retries = 0;
                eng.resolve_fallback_unverified_close(snap, vac_level_hint)
                    .map(LiveCloseStepAction::FinalizeVerified)
            }
        }
    }
}

pub fn finalize_and_emit_live_close(
    s_idx: usize,
    resolution: VerifiedCloseResolution,
    now_ns: TimestampNs,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
    halt_locked: &mut bool,
) {
    let spec = APPROVED_SPECS[s_idx];
    let sym_name = spec.symbol_id.name();
    let hold_s = (now_ns.0.saturating_sub(engines[s_idx].last_opened_ts.0)).max(0) / 1_000_000_000;
    let Some(summary) = engines[s_idx].apply_verified_close_resolution(resolution, now_ns, ledger)
    else {
        return;
    };
    let non_punitive_exit = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
    gov.observe_closed_trade_at_entry(
        s_idx,
        summary.side,
        summary.entry_price,
        summary.net_scaled_bps,
        non_punitive_exit,
        now_ns,
    );
    arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
    dispatch_closed_trade_logs(
        s_idx,
        &summary,
        &engines[s_idx],
        &entry_forensics[s_idx],
        &cached_snaps[s_idx],
        ledger,
        gov,
        vault_tx,
        halt_locked,
        now_ns,
    );
    engines[s_idx].sync_to_shm_telemetry(arena);
    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
    let net_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let (en_w, en_f) = spec.format_price_usdt_parts(summary.entry_price);
    let (ex_w, ex_f) = spec.format_price_usdt_parts(summary.exit_price);
    let pnl_usdt = (summary.net_pnl_1e8 as f64) / 100_000_000.0;
    let lots_s = spec.format_lot_qty_str(summary.closed_lots);
    let is_part =
        matches!(engines[s_idx].last_exit_sub_reason, ExitSubReason::Tranche1Harvest50)
            && net_f < 1.00;
    let (col, icon, lbl) = classify_trade_badge(net_f, is_part);
    println!(
        "{}{} [TRADE CLOSE \u{2014} {}] {} {:?} qty={} ({}L) | {}.{:04} -> {}.{:04} | Net: {:+.2} bps ({:+.4} USDT) | Hold: {}s{}",
        col, icon, lbl.trim(), sym_name, summary.side, lots_s, summary.legs_consolidated,
        en_w, en_f, ex_w, ex_f, net_f, pnl_usdt, hold_s, ANSI_RESET
    );
    let _ = tg_tx.try_send(format_telegram_close_message(
        spec,
        &summary,
        engines[s_idx].last_exit_sub_reason,
        ledger.closed_trades_count,
        ledger,
    ));
}

pub fn handle_gateway_event(
    evt: GatewayEvent,
    now_ns: TimestampNs,
    gateway: Option<&BybitAsyncGateway>,
    rt: &mut ExecRuntimeState,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
) {
    match evt {
        GatewayEvent::WalletUpdated { wallet_1e8: w } => {
            if w > 0 {
                rt.wallet_1e8 = w;
            }
        }
        GatewayEvent::EntryPlaced { slot_idx, accepted, entry_price, sl_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PlacingEntryMaker { side, qty, .. } = engines[slot_idx].state
            else {
                return;
            };
            if accepted {
                rt.verified_exchange_sl[slot_idx] = sl_price;
                let _ = engines[slot_idx].on_entry_order_resting(now_ns);
                arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
                let spec = APPROVED_SPECS[slot_idx];
                let snap = cached_snaps[slot_idx];
                let (ew_p, ef_p) = spec.format_price_usdt_parts(entry_price);
                let (sw_p, sf_p) = spec.format_price_usdt_parts(sl_price);
                println!(
                    "{}\u{1f7e1} [BYBIT V5 LIVE ORDER SENT] {} {:?} qty={} @ {}.{:04} USDT (SL: {}.{:04} | Q_L1: {} lots | Seq: {} | OFI: {:+.2} | TFI: {:+.2} | GEOM: {:+.2}){}",
                    ANSI_YELLOW, spec.symbol_id.name(), side, spec.format_lot_qty_str(qty),
                    ew_p, ef_p, sw_p, sf_p, rt.init_q_ahead[slot_idx].0, snap.sequence_id,
                    snap.factor_ofi.get(), snap.factor_tfi.get(), snap.factor_geom.get(), ANSI_RESET
                );
            } else {
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryRepegged { slot_idx, accepted, new_price, new_sl_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::RestingEntryQueue { side, .. } = engines[slot_idx].state else {
                return;
            };
            if accepted {
                rt.verified_exchange_sl[slot_idx] = new_sl_price;
                engines[slot_idx].apply_resting_repeg(new_price, new_sl_price, now_ns);
                let q_new = match side {
                    Side::Bid => cached_snaps[slot_idx].bids[0].qty,
                    Side::Ask => cached_snaps[slot_idx].asks[0].qty,
                };
                rt.init_q_ahead[slot_idx] = q_new;
                rt.cur_q_ahead[slot_idx] = q_new;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else {
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryAborted { slot_idx, side, aborted_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.abort_cmd_inflight[slot_idx] = false;
            rt.verified_exchange_sl[slot_idx] = PriceTicks(0);
            if matches!(
                engines[slot_idx].state,
                OrderLifecycleState::PlacingEntryMaker { .. }
                    | OrderLifecycleState::RestingEntryQueue { .. }
            ) {
                gov.observe_entry_abort(slot_idx, side, aborted_price, now_ns);
                engines[slot_idx].state = OrderLifecycleState::Idle;
                arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryOrderVanished { slot_idx }
        | GatewayEvent::WsEntryOrderTerminated { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.verified_exchange_sl[slot_idx] = PriceTicks(0);
            if let OrderLifecycleState::PlacingEntryMaker { side, entry_price, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, .. } =
                engines[slot_idx].state
            {
                if rt.abort_cmd_inflight[slot_idx] {
                    rt.abort_cmd_inflight[slot_idx] = false;
                    gov.observe_entry_abort(slot_idx, side, entry_price, now_ns);
                    arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                }
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryStillResting { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::EntryFilledLive {
            slot_idx, real_side, verified_avg, real_lots, sl_verified, tag,
        } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.abort_cmd_inflight[slot_idx] = false;
            if !matches!(
                engines[slot_idx].state,
                OrderLifecycleState::PlacingEntryMaker { .. }
                    | OrderLifecycleState::RestingEntryQueue { .. }
            ) {
                return;
            }
            gov.clear_abort_streak(slot_idx, real_side);
            engines[slot_idx].sync_governor_for_side(gov, real_side);
            let prev_sl = engines[slot_idx].last_eff_sl_ticks;
            if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(
                &mut engines[slot_idx], now_ns, Some(real_side), verified_avg, Some(real_lots),
            ) {
                apply_post_fill_sl_or_breach(
                    slot_idx, eff_s, eff_px, sl_verified, prev_sl, &cached_snaps[slot_idx],
                    &mut engines[slot_idx], rt,
                );
                record_and_notify_entry_fill(
                    slot_idx, APPROVED_SPECS[slot_idx], engines, entry_forensics, arena, tg_tx,
                    vault_tx, now_ns, eff_s, eff_px, eff_q, tag,
                );
            }
        }
        GatewayEvent::StopLossSynced { slot_idx, sl_price, verified } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            if verified {
                rt.sl_needs_sync[slot_idx] = false;
                rt.emergency_flatten_sl[slot_idx] = None;
                rt.verified_exchange_sl[slot_idx] = sl_price;
                engines[slot_idx].last_eff_sl_ticks = sl_price;
                if let OrderLifecycleState::PositionOpenMakerExit {
                    side,
                    avg_price_ticks,
                    sl_trigger_price: ref mut sl_p,
                    ..
                } = engines[slot_idx].state
                {
                    *sl_p = sl_price;
                    engines[slot_idx].chandelier_profit_sl_armed =
                        is_chandelier_profit_locked_sl(side, avg_price_ticks, sl_price);
                }
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else if rt.emergency_flatten_sl[slot_idx].is_none() {
                let fallback_sl = rt.verified_exchange_sl[slot_idx];
                if fallback_sl.0 > 0 {
                    engines[slot_idx].last_eff_sl_ticks = fallback_sl;
                    if let OrderLifecycleState::PositionOpenMakerExit {
                        side,
                        avg_price_ticks,
                        sl_trigger_price: ref mut sl_p,
                        ..
                    } = engines[slot_idx].state
                    {
                        *sl_p = fallback_sl;
                        engines[slot_idx].chandelier_profit_sl_armed =
                            is_chandelier_profit_locked_sl(side, avg_price_ticks, fallback_sl);
                    }
                }
                rt.sl_needs_sync[slot_idx] = true;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::SlotSyncSkipped { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::SlotStillOpenSynced {
            slot_idx, real_side, verified_avg, real_lots, active_resting_tp,
        } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PositionOpenMakerExit {
                side, avg_price_ticks, initial_lots, tranche1_harvested,
                resting_exit_price: prev_rest_tp, opened_ts, ..
            } = engines[slot_idx].state
            else {
                return;
            };
            if real_side == side {
                if let Some(v_avg) = verified_avg {
                    engines[slot_idx].sync_open_avg_price_ticks(v_avg);
                }
            }
            let hold_ms = (now_ns.0.saturating_sub(opened_ts.0)).max(0) / 1_000_000;
            let eff_init = initial_lots.max(real_lots);
            let ref_exit_px = if prev_rest_tp.0 > 0 {
                prev_rest_tp
            } else {
                defensive_zero_loss_maker_exit_price(side, avg_price_ticks)
            };
            let mut t1_done = tranche1_harvested;
            if let Some(min_w3_t1) =
                SlotExecutionEngine::compute_tranche1_split_lots(slot_idx, eff_init, ref_exit_px, 350)
            {
                let max_rem_after_t1 = (eff_init.0 - min_w3_t1.0).max(1);
                if !t1_done
                    && prev_rest_tp.0 > 0
                    && real_lots.0 > 0
                    && real_lots.0 <= max_rem_after_t1
                    && hold_ms >= 50
                {
                    t1_done = true;
                    engines[slot_idx].tranche1_harvests_count =
                        engines[slot_idx].tranche1_harvests_count.saturating_add(1);
                }
            }
            if active_resting_tp.0 > 0 && active_resting_tp != prev_rest_tp {
                engines[slot_idx].exit_peg_attempts =
                    engines[slot_idx].exit_peg_attempts.saturating_add(1);
            }
            if let OrderLifecycleState::PositionOpenMakerExit {
                initial_lots: ref mut in_l,
                open_lots: ref mut op_l,
                tranche1_harvested: ref mut t1_h,
                tranche1_lots: ref mut t1_l,
                tranche1_exit_price: ref mut t1_px,
                resting_exit_price: ref mut rx_p,
                ..
            } = engines[slot_idx].state
            {
                *in_l = eff_init;
                *op_l = real_lots;
                if t1_done && !*t1_h {
                    *t1_h = true;
                    *t1_l = LotQty((eff_init.0 - real_lots.0).max(1));
                    if prev_rest_tp.0 > 0 {
                        *t1_px = prev_rest_tp;
                    }
                }
                *rx_p = active_resting_tp;
            }
            dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
            engines[slot_idx].sync_to_shm_telemetry(arena);
        }
        GatewayEvent::SlotClosedDetected { slot_idx, vac_level, closed_pnl_payload } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.sl_needs_sync[slot_idx] = false;
            rt.emergency_flatten_sl[slot_idx] = None;
            let Some(step_action) = resolve_live_closed_position_step(
                &mut engines[slot_idx],
                &cached_snaps[slot_idx],
                closed_pnl_payload.as_ref(),
                vac_level,
                &mut rt.idx_lag,
                now_ns.0,
            ) else {
                engines[slot_idx].sync_to_shm_telemetry(arena);
                return;
            };
            match step_action {
                LiveCloseStepAction::AwaitNextPoll { next_poll_delay_ns } => {
                    engines[slot_idx].next_peg_allowed_ts_ns = now_ns.0 + next_poll_delay_ns;
                    engines[slot_idx].sync_to_shm_telemetry(arena);
                }
                LiveCloseStepAction::FinalizeVerified(res) => {
                    rt.verified_exchange_sl[slot_idx] = PriceTicks(0);
                    finalize_and_emit_live_close(
                        slot_idx, res, now_ns, cached_snaps, entry_forensics, engines, gov, ledger,
                        arena, tg_tx, vault_tx, &mut rt.halt_locked,
                    );
                }
            }
        }
        GatewayEvent::WsPositionUpdate {
            slot_idx, is_open, real_side, verified_avg, real_lots,
        } => {
            if is_open {
                let armed_qty_opt = match engines[slot_idx].state {
                    OrderLifecycleState::PlacingEntryMaker { qty, .. }
                    | OrderLifecycleState::RestingEntryQueue { qty, .. } => Some(qty),
                    OrderLifecycleState::Idle
                    | OrderLifecycleState::PositionOpenMakerExit { .. } => None,
                };
                if let Some(armed_qty) = armed_qty_opt {
                    rt.slot_rpc_inflight[slot_idx] = false;
                    rt.abort_cmd_inflight[slot_idx] = false;
                    if real_lots.0 < armed_qty.0 {
                        if let Some(gw) = gateway {
                            let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx });
                        }
                    }
                    gov.clear_abort_streak(slot_idx, real_side);
                    engines[slot_idx].sync_governor_for_side(gov, real_side);
                    let prev_sl = engines[slot_idx].last_eff_sl_ticks;
                    if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(
                        &mut engines[slot_idx], now_ns, Some(real_side), verified_avg, Some(real_lots),
                    ) {
                        apply_post_fill_sl_or_breach(
                            slot_idx, eff_s, eff_px, false, prev_sl, &cached_snaps[slot_idx],
                            &mut engines[slot_idx], rt,
                        );
                        record_and_notify_entry_fill(
                            slot_idx, APPROVED_SPECS[slot_idx], engines, entry_forensics, arena,
                            tg_tx, vault_tx, now_ns, eff_s, eff_px, eff_q, "MAKER FILL WS",
                        );
                    }
                } else if let OrderLifecycleState::PositionOpenMakerExit { side, open_lots, .. } =
                    engines[slot_idx].state
                {
                    if real_side == side {
                        if let Some(v_avg) = verified_avg {
                            engines[slot_idx].sync_open_avg_price_ticks(v_avg);
                        }
                    }
                    if real_lots.0 > 0 && real_lots.0 < open_lots.0 {
                        engines[slot_idx].next_peg_allowed_ts_ns = 0;
                    }
                }
            } else if matches!(engines[slot_idx].state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
                engines[slot_idx].next_peg_allowed_ts_ns = 0;
            }
        }
    }
}

pub fn bootstrap_live_adopt_if_enabled(
    live_capital: bool,
    arena: &VortexShmArena,
    gov: &OnlineAdaptiveGovernor,
    rt: &mut ExecRuntimeState,
    entry_forensics: &mut [SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
) {
    let Some(mut boot_client) = BybitV5LiveClient::from_env_if_enabled(live_capital) else {
        return;
    };
    if let Some((w_f, _, _, _)) = boot_client.fetch_wallet_and_equity_usdt() {
        if w_f > 0.0 {
            rt.wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
        }
    }
    for (s_idx, spec) in APPROVED_SPECS.iter().enumerate() {
        let sym_name = spec.symbol_id.name();
        let Some((true, sz_s, side_p, avg_s)) = boot_client.query_open_position_verified(sym_name)
        else {
            continue;
        };
        let _ = boot_client.cancel_non_reduce_only_orders(*spec);
        let Ok(bybit_avg_t) = parse_verified_avg_price_ticks(*spec, side_p, &avg_s) else {
            continue;
        };
        let lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
        let now_t = now_epoch_ns();
        let saved = load_saved_slot_state(s_idx);
        let init_l = saved.as_ref().and_then(|v| v.get("initial_lots")?.as_i64()).map_or(lots, LotQty);
        let t1_done = saved.as_ref().and_then(|v| v.get("tranche1_harvested")?.as_bool()).unwrap_or(false);
        let def_locked = saved.as_ref().and_then(|v| v.get("defensive_floor_locked")?.as_bool()).unwrap_or(false);
        let t1_ex_saved = saved.as_ref().and_then(|v| v.get("tranche1_exit_ticks")?.as_i64()).unwrap_or(0);
        let opened_ns = saved.as_ref().and_then(|v| v.get("opened_ts_ns")?.as_i64()).map_or(now_t, TimestampNs);
        let peak_t = saved.as_ref().and_then(|v| v.get("peak_ticks")?.as_i64()).filter(|&p| p > 0).map_or(bybit_avg_t, PriceTicks);
        let worst_t = saved.as_ref().and_then(|v| v.get("worst_ticks")?.as_i64()).filter(|&w| w > 0).map_or(bybit_avg_t, PriceTicks);
        let snap = arena
            .slot(s_idx)
            .and_then(|s| s.read_snapshot(4))
            .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
        engines[s_idx].sync_governor_for_side(gov, side_p);
        if let Ok(sl_t) = engines[s_idx].adopt_live_open_position(
            side_p, bybit_avg_t, init_l, lots, t1_done, peak_t, worst_t, opened_ns, &snap,
        ) {
            engines[s_idx].latch_defensive_floor_if_needed(def_locked);
            if t1_ex_saved > 0 {
                if let OrderLifecycleState::PositionOpenMakerExit {
                    tranche1_exit_price: ref mut t1_p, ..
                } = engines[s_idx].state
                {
                    *t1_p = PriceTicks(t1_ex_saved);
                }
            }
            let sl_ok = boot_client.set_position_trading_stop(*spec, sl_t);
            apply_post_fill_sl_or_breach(
                s_idx, side_p, bybit_avg_t, sl_ok, sl_t, &snap, &mut engines[s_idx], rt,
            );
            entry_forensics[s_idx] = match saved {
                Some(ref sv) => restore_entry_forensics_from_json(sv, opened_ns.0, init_l.0),
                None => SlotForensicEntrySnapshot {
                    armed_ts_ns: opened_ns.0,
                    init_q_lots: init_l.0,
                    ..SlotForensicEntrySnapshot::default()
                },
            };
            engines[s_idx].sync_to_shm_telemetry(arena);
            println!(
                "{}\u{1f7e2} [VORTEX-L3 LIVE ADOPT] Slot {:2} {} {:?} qty={} @ {}t (SL={}t ok={} T1={}){}",
                ANSI_GREEN, s_idx, sym_name, side_p, sz_s, bybit_avg_t.0, sl_t.0, sl_ok, t1_done, ANSI_RESET
            );
        }
    }
    dispatch_open_positions_state(engines, entry_forensics, None);
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Oos520MultiRegimeReport {
    pub closed_trades: u64,
    pub maker_exit_ratio_pct: f64,
    pub profit_factor: f64,
    pub cum_net_pnl_usdt: f64,
    pub parity_sync_drift_1e8: i64,
    pub utc_funding_vetoes: u64,
    pub hawkes_pre_fill_aborts: u64,
    pub wave3_fibonacci_closes: u64,
    pub climax_exhaustion_harvests: u64,
    pub shadow_crest_harvests: u64,
    pub chandelier_profit_sl_wins: u64,
    pub wald_v2_zero_loss_escapes: u64,
    pub wald_v2_compressed_sl_closes: u64,
}

#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
pub fn run_oos_520_multi_regime_suite() -> Oos520MultiRegimeReport {
    if !verify_ws_private_parser_self_check() {
        return Oos520MultiRegimeReport {
            parity_sync_drift_1e8: 1,
            ..Oos520MultiRegimeReport::default()
        };
    }
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let sample_px: [i64; MAX_ACTIVE_PAIRS] = [
        759_000, 223_700, 8_930, 8_170, 22_400, 8_510,
        3_130, 16_140, 1_650, 10_720, 3_690, 5_176,
    ];
    let mut rep = Oos520MultiRegimeReport::default();

    // Deterministic pre-audit of 8h Pre-Funding (-40s) and 21:00 UTC Trough marginal signal vetoes
    let pre_funding_ts = TimestampNs((59_000_i64 * 28_800 - 40) * 1_000_000_000);
    let utc_trough_ts = TimestampNs((20_000_i64 * 86_400 + 21 * 3_600) * 1_000_000_000);
    for probe_ts in [pre_funding_ts, utc_trough_ts] {
        let mut weak_snap = SymbolLobSnapshot::new_empty();
        weak_snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        weak_snap.is_eligible = true;
        weak_snap.ts_local_ns = probe_ts;
        weak_snap.realized_vol_5m = ScaledBps(6_500);
        weak_snap.bids[0] = BookLevel { price: PriceTicks(8_170), qty: LotQty(320) };
        weak_snap.asks[0] = BookLevel { price: PriceTicks(8_171), qty: LotQty(300) };
        weak_snap.refresh_deep_l1_l5_totals();
        weak_snap.mark_price = PriceTicks(8_170);
        weak_snap.last_trade_price = PriceTicks(8_170);
        weak_snap.factor_ofi = BoundedScore::new_clamped(0.47);
        weak_snap.factor_tfi = BoundedScore::new_clamped(0.48);
        weak_snap.factor_geom = BoundedScore::new_clamped(0.36);
        if !gov.authorizes_adaptive_entry_with_wallet(
            3,
            Side::Bid,
            DEFAULT_WALLET_EQUITY_1E8,
            &weak_snap,
            &weak_snap,
        ) {
            rep.utc_funding_vetoes += 1;
        }
    }

    let mut ts = 1_700_000_000_000_000_000_i64;
    let mut maker_total = 0_u64;
    let (mut profit_1e8, mut loss_1e8, mut sum_trade_pnl_1e8) = (0_i64, 0_i64, 0_i64);
    let mut step = 0_usize;

    while rep.closed_trades < 520 && step < 8_000 {
        let s_idx = step % MAX_ACTIVE_PAIRS;
        step += 1;
        // 2,725s step sweeps across all 24 UTC hours and 8h funding boundaries
        ts += 2_725_000_000_000;
        let spec = APPROVED_SPECS[s_idx];
        let base_px = sample_px[s_idx];

        let mut btc_snap = SymbolLobSnapshot::new_empty();
        btc_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_snap.is_eligible = true;
        btc_snap.ts_local_ns = TimestampNs(ts);
        btc_snap.realized_vol_5m = ScaledBps(6_500);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(450) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(350) };
        btc_snap.refresh_deep_l1_l5_totals();
        btc_snap.mark_price = PriceTicks(759_000);
        btc_snap.last_trade_price = PriceTicks(759_000);
        btc_snap.factor_ofi = BoundedScore::new_clamped(0.86);
        btc_snap.factor_tfi = BoundedScore::new_clamped(0.88);
        btc_snap.factor_geom = BoundedScore::new_clamped(0.58);

        let mut snap = btc_snap;
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_200);
        snap.bids[0] = BookLevel { price: PriceTicks(base_px), qty: LotQty(450) };
        snap.asks[0] = BookLevel { price: PriceTicks(base_px + 1), qty: LotQty(350) };
        snap.refresh_deep_l1_l5_totals();
        snap.mark_price = PriceTicks(base_px);
        snap.last_trade_price = PriceTicks(base_px);

        let fee_rt = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
        let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, Side::Bid);
        let verdict = OrthogonalFactorState::evaluate_entry_signal_elastic(
            spec,
            &snap,
            &btc_snap,
            ScaledBps(engines[s_idx].learned_mfe_bps.min(fee_rt * 2)),
            ScaledBps(probe_edge.clamp(-fee_rt, fee_rt / 2)),
        );
        let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict else { continue };

        let mut beta_guard = PortfolioBetaGuard::new();
        for eng in &engines {
            if let Some(act_s) = eng.active_side() {
                beta_guard.observe_active_side(act_s);
            }
        }
        let eq_1e8 = ledger.current_equity_1e8();
        if !beta_guard.can_admit_side(side) {
            continue;
        }
        if !gov.authorizes_adaptive_entry_with_wallet(s_idx, side, eq_1e8, &snap, &btc_snap) {
            rep.utc_funding_vetoes += 1;
            continue;
        }
        let en_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
        let lots = gov.compute_slot_entry_lots(s_idx, side, eq_1e8, &snap);
        engines[s_idx].sync_governor_for_side(&gov, side);
        if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_err() {
            continue;
        }
        let _ = engines[s_idx].on_entry_order_resting(TimestampNs(ts + 15_000_000));

        // Inject hidden toxic L1 sweep every 23rd step -> Hawkes + Stoikov aborts BEFORE fill
        if step % 23 == 7 {
            let mut cur_q = 500_i64;
            let mut aborted_q = false;
            for b_k in 1..=5_i64 {
                cur_q -= 75;
                snap.ts_local_ns = TimestampNs(ts + 15_000_000 + b_k * 20_000_000);
                snap.bids[0] = BookLevel { price: en_px, qty: LotQty(cur_q) };
                snap.asks[0] = BookLevel { price: PriceTicks(en_px.0 + 1), qty: LotQty(410) };
                snap.deep_bid_lots_l1_l5 = LotQty(320);
                snap.deep_ask_lots_l1_l5 = LotQty(1_320);
                snap.factor_ofi = BoundedScore::new_clamped(0.14);
                snap.factor_tfi = BoundedScore::new_clamped(0.10);
                let qv = engines[s_idx].evaluate_resting_queue_shield(
                    side, LotQty(500), LotQty(cur_q), LotQty(75), &snap, &btc_snap,
                );
                if qv.abort_now {
                    gov.observe_entry_abort(s_idx, side, en_px, snap.ts_local_ns);
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    rep.hawkes_pre_fill_aborts += 1;
                    aborted_q = true;
                    break;
                }
            }
            if aborted_q {
                continue;
            }
        }

        let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 25_000_000));
        let regime_bucket = rep.closed_trades % 20;
        let mut closed_summary = None;

        match regime_bucket {
            0..=10 => {
                // Regime A (55%): ImpulseWave3 ~38% T1 + Fibonacci 1.618x Surf (+82 bps)
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ImpulseWave3,
                    climax_active: false,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.66),
                    macro_phase: BoundedScore::new_clamped(0.85),
                    oi_impulse: BoundedScore::new_clamped(0.45),
                    liq_imbalance: BoundedScore::new_clamped(0.60),
                    fib_proj_bps: ScaledBps(7_200),
                });
                let q_fl = offensive_win_maker_exit_price(side, en_px, 0.50);
                snap.ts_local_ns = TimestampNs(ts + 8_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(q_fl.0 - 1), qty: LotQty(440) };
                snap.asks[0] = BookLevel { price: q_fl, qty: LotQty(180) };
                snap.mark_price = q_fl;
                snap.last_trade_price = q_fl;
                closed_summary =
                    engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                if closed_summary.is_none() {
                    let fib_d = ((base_px * 8_200 + 999_999) / 1_000_000).max(14);
                    let px2 = PriceTicks(base_px + fib_d);
                    let clear_pad = ((base_px * 120) / 1_000_000).max(3);
                    snap.ts_local_ns = TimestampNs(ts + 20_000_000_000);
                    snap.bids[0] = BookLevel { price: PriceTicks(px2.0 - 1), qty: LotQty(410) };
                    snap.asks[0] = BookLevel { price: px2, qty: LotQty(150) };
                    snap.mark_price = PriceTicks(px2.0 + clear_pad);
                    snap.last_trade_price = PriceTicks(px2.0 + clear_pad);
                    snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                        regime: crate::wave_elliott::WaveRegime::ImpulseWave3,
                        climax_active: false,
                        hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.66),
                        macro_phase: BoundedScore::new_clamped(0.88),
                        oi_impulse: BoundedScore::new_clamped(0.40),
                        liq_imbalance: BoundedScore::new_clamped(0.55),
                        fib_proj_bps: ScaledBps(10),
                    });
                    closed_summary =
                        engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                }
            }
            11..=14 => {
                // Regime B (20%): Blow-off ClimaxExhaustion passive 100% Maker harvest
                let clim_d = ((base_px * 2_600 + 999_999) / 1_000_000).max(5);
                let px_c = PriceTicks(base_px + clim_d);
                snap.ts_local_ns = TimestampNs(ts + 12_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(px_c.0 - 1), qty: LotQty(360) };
                snap.asks[0] = BookLevel { price: px_c, qty: LotQty(170) };
                snap.mark_price = px_c;
                snap.last_trade_price = px_c;
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ClimaxExhaustion,
                    climax_active: true,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.68),
                    macro_phase: BoundedScore::new_clamped(0.92),
                    oi_impulse: BoundedScore::new_clamped(-0.30),
                    liq_imbalance: BoundedScore::new_clamped(0.82),
                    fib_proj_bps: ScaledBps(0),
                });
                closed_summary =
                    engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
            15 => {
                // Regime C (5%): Shadow-Runner 50ms surge + Crest Deceleration Snap-In
                let step_k = ((base_px * 520) / 1_000_000).max(1);
                let mut sub_t = ts + 25_000_000;
                for k in 1..=8_i64 {
                    sub_t += 50_000_000;
                    let px_k = PriceTicks(base_px + k * step_k);
                    snap.ts_local_ns = TimestampNs(sub_t);
                    snap.bids[0] = BookLevel { price: PriceTicks(px_k.0 - 1), qty: LotQty(400) };
                    snap.asks[0] = BookLevel { price: px_k, qty: LotQty(190) };
                    snap.mark_price = px_k;
                    snap.last_trade_price = px_k;
                    snap.factor_ofi = BoundedScore::new_clamped(0.62 + 0.03 * (k as f64));
                    snap.factor_tfi = BoundedScore::new_clamped(0.66 + 0.03 * (k as f64));
                    snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                        regime: crate::wave_elliott::WaveRegime::NeutralWarmup,
                        climax_active: false,
                        hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.60),
                        macro_phase: BoundedScore::ZERO,
                        oi_impulse: BoundedScore::ZERO,
                        liq_imbalance: BoundedScore::ZERO,
                        fib_proj_bps: ScaledBps(0),
                    });
                    let _ = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                }
                let crest_px = PriceTicks(base_px + 9 * step_k);
                for _ in 1..=4_i64 {
                    sub_t += 50_000_000;
                    snap.ts_local_ns = TimestampNs(sub_t);
                    snap.bids[0] = BookLevel { price: PriceTicks(crest_px.0 - 1), qty: LotQty(240) };
                    snap.asks[0] = BookLevel { price: crest_px, qty: LotQty(220) };
                    snap.mark_price = crest_px;
                    snap.last_trade_price = PriceTicks(crest_px.0 + 1);
                    snap.factor_ofi = BoundedScore::new_clamped(0.04);
                    snap.factor_tfi = BoundedScore::new_clamped(0.03);
                    snap.factor_geom = BoundedScore::ZERO;
                    if let Some(cs) =
                        engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger)
                    {
                        closed_summary = Some(cs);
                        break;
                    }
                }
            }
            17 => {
                // Regime D (5%): Wave-5 +58 bps rally -> Chandelier Profit-Lock Server SL WIN
                snap.realized_vol_5m = ScaledBps(5_000);
                let rally_t = ((base_px * 5_800 + 999_999) / 1_000_000).max(10);
                let px_r = PriceTicks(base_px + rally_t);
                snap.ts_local_ns = TimestampNs(ts + 9_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(px_r.0 - 1), qty: LotQty(370) };
                snap.asks[0] = BookLevel { price: px_r, qty: LotQty(180) };
                snap.mark_price = px_r;
                snap.last_trade_price = px_r;
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ImpulseWave5,
                    climax_active: false,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.60),
                    macro_phase: BoundedScore::new_clamped(0.70),
                    oi_impulse: BoundedScore::new_clamped(0.20),
                    liq_imbalance: BoundedScore::new_clamped(0.20),
                    fib_proj_bps: ScaledBps(2_600),
                });
                let _ = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                let chand_sl = engines[s_idx].last_eff_sl_ticks;
                snap.ts_local_ns = TimestampNs(ts + 12_000_000_000);
                snap.mark_price = PriceTicks(chand_sl.0 - 1);
                snap.last_trade_price = PriceTicks(chand_sl.0 - 1);
                closed_summary =
                    engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
            _ => {
                // Regimes E & F (15%): BTC Lead-Lag Dump -> Wald SPRT v2.0 (16=Bounce Escape, 18..19=Compressed SL)
                btc_snap.factor_ofi = BoundedScore::new_clamped(-0.78);
                btc_snap.factor_tfi = BoundedScore::new_clamped(-0.82);
                btc_snap.factor_geom = BoundedScore::new_clamped(-0.55);
                let dip_px = (base_px - ((base_px * 900) / 1_000_000).max(2)).max(2);
                snap.bids[0] = BookLevel { price: PriceTicks(dip_px), qty: LotQty(60) };
                snap.asks[0] = BookLevel { price: PriceTicks(dip_px + 1), qty: LotQty(420) };
                snap.deep_bid_lots_l1_l5 = LotQty(190);
                snap.deep_ask_lots_l1_l5 = LotQty(1_310);
                snap.mark_price = PriceTicks(dip_px);
                snap.last_trade_price = PriceTicks(dip_px);
                snap.factor_ofi = BoundedScore::new_clamped(-0.62);
                snap.factor_tfi = BoundedScore::new_clamped(-0.68);
                let mut w_ts = ts + 25_000_000;
                for _ in 0..44 {
                    w_ts += 100_000_000;
                    snap.ts_local_ns = TimestampNs(w_ts);
                    let _ = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                }
                snap.ts_local_ns = TimestampNs(w_ts + 300_000_000);
                if regime_bucket == 16 {
                    let z_fl = defensive_zero_loss_maker_exit_price(side, en_px);
                    snap.bids[0] = BookLevel { price: PriceTicks(z_fl.0 - 1), qty: LotQty(260) };
                    snap.asks[0] = BookLevel { price: z_fl, qty: LotQty(170) };
                    snap.mark_price = z_fl;
                    snap.last_trade_price = z_fl;
                } else {
                    let comp_sl = engines[s_idx].last_eff_sl_ticks.max(PriceTicks(1));
                    snap.mark_price = comp_sl;
                    snap.last_trade_price = comp_sl;
                }
                closed_summary =
                    engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
        }

        if let Some(summary) = closed_summary {
            rep.closed_trades += 1;
            if summary.is_maker_exit {
                maker_total += 1;
            }
            match engines[s_idx].last_exit_sub_reason {
                ExitSubReason::FibonacciImpulseSurf | ExitSubReason::LvdbsDeepVacuumSurf => {
                    rep.wave3_fibonacci_closes += 1;
                }
                ExitSubReason::ClimaxExhaustionHarvest => {
                    rep.climax_exhaustion_harvests += 1;
                }
                ExitSubReason::ShadowRunnerCrestHarvest => {
                    rep.shadow_crest_harvests += 1;
                }
                ExitSubReason::ChandelierProfitLockSl => {
                    rep.chandelier_profit_sl_wins += 1;
                }
                ExitSubReason::AdverseScratchEscape => {
                    rep.wald_v2_zero_loss_escapes += 1;
                }
                ExitSubReason::WaldBaseStopLoss => {
                    rep.wald_v2_compressed_sl_closes += 1;
                }
                _ => {}
            }
            if summary.net_pnl_1e8 >= 0 {
                profit_1e8 += summary.net_pnl_1e8;
            } else {
                loss_1e8 += summary.net_pnl_1e8.abs();
            }
            sum_trade_pnl_1e8 += summary.net_pnl_1e8;
            let non_punitive = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
            gov.observe_closed_trade_at_entry(
                s_idx,
                summary.side,
                summary.entry_price,
                summary.net_scaled_bps,
                non_punitive,
                snap.ts_local_ns,
            );
        } else {
            engines[s_idx].state = OrderLifecycleState::Idle;
        }
    }

    rep.maker_exit_ratio_pct = (maker_total as f64 * 100.0) / (rep.closed_trades.max(1) as f64);
    rep.profit_factor = (profit_1e8 as f64) / (loss_1e8.max(1) as f64);
    rep.cum_net_pnl_usdt = (ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    rep.parity_sync_drift_1e8 = (ledger.cumulative_net_pnl_1e8 - sum_trade_pnl_1e8).abs();
    rep
}

#[must_use]
pub fn run_oos_520_certification_suite() -> (u64, f64, f64, f64, i64) {
    let r = run_oos_520_multi_regime_suite();
    (
        r.closed_trades,
        r.maker_exit_ratio_pct,
        r.profit_factor,
        r.cum_net_pnl_usdt,
        r.parity_sync_drift_1e8,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_fsm::ClosedTradeKind;
    use crate::lob_shm::MappedShmRegion;
    use std::sync::mpsc;

    #[test]
    fn test_oos_520_verified_reconcile_and_p0a_post_fill_breach_arming() {
        let rep = run_oos_520_multi_regime_suite();
        assert_eq!(rep.closed_trades, 520);
        assert!(rep.maker_exit_ratio_pct >= 82.0);
        assert!(rep.profit_factor >= 2.0);
        assert!(rep.cum_net_pnl_usdt > 0.0);
        assert_eq!(rep.parity_sync_drift_1e8, 0);
        assert!(rep.utc_funding_vetoes >= 2);
        assert!(rep.hawkes_pre_fill_aborts >= 20);
        assert!(rep.wave3_fibonacci_closes >= 200);
        assert!(rep.climax_exhaustion_harvests >= 90);
        assert!(rep.shadow_crest_harvests >= 20);
        assert!(rep.chandelier_profit_sl_wins >= 20);
        assert!(rep.wald_v2_zero_loss_escapes >= 20);
        assert!(rep.wald_v2_compressed_sl_closes >= 45);

        let mut eng = SlotExecutionEngine::new(3);
        let mut rt = ExecRuntimeState::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_700_000_010_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);
        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Ask,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(promote_verified_fill_state(
            &mut eng,
            TimestampNs(1_700_000_010_500_000_000),
            Some(Side::Ask),
            Some(PriceTicks(8_030)),
            Some(LotQty(26)),
        )
        .is_some());

        snap.mark_price = PriceTicks(9_007);
        snap.last_trade_price = PriceTicks(9_008);
        apply_post_fill_sl_or_breach(
            3, Side::Ask, PriceTicks(8_030), false, PriceTicks(8_110), &snap, &mut eng, &mut rt,
        );
        assert!(rt.emergency_flatten_sl[3].is_some_and(|sl| sl.0 >= 9_011));
    }

    #[test]
    fn test_live_chandelier_ack_rollback_wave3_partial_sync_and_green_sl_win() {
        let shm_path = MappedShmRegion::ephemeral_self_test_path("eh_62_chand");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());

        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let mut gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let mut rt = ExecRuntimeState::new();
        rt.halt_locked = false;
        let forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        snaps[3].symbol_id = APPROVED_SPECS[3].symbol_id;
        snaps[3].realized_vol_5m = ScaledBps(5_000);
        snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(400) };
        snaps[3].asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snaps[3].mark_price = PriceTicks(8_200);
        snaps[3].last_trade_price = PriceTicks(8_200);

        let verdict = SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(700),
        };
        assert!(engines[3].try_arm_maker_entry(&snaps[3], verdict, LotQty(26)).is_ok());
        assert!(engines[3].on_maker_entry_filled(TimestampNs(1_000_000_000)).is_ok());
        if let OrderLifecycleState::PositionOpenMakerExit {
            resting_exit_price: ref mut rx_p,
            sl_trigger_price,
            ..
        } = engines[3].state
        {
            *rx_p = PriceTicks(8_205);
            rt.verified_exchange_sl[3] = sl_trigger_price;
        }
        let orig_verified_sl = rt.verified_exchange_sl[3];
        let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
        let (vault_tx, _vault_rx) = mpsc::sync_channel::<VaultPersistTask>(16);

        if let Ok(ref r) = reg {
            handle_gateway_event(
                GatewayEvent::SlotStillOpenSynced {
                    slot_idx: 3,
                    real_side: Side::Bid,
                    verified_avg: Some(PriceTicks(8_200)),
                    real_lots: LotQty(16),
                    active_resting_tp: PriceTicks(8_255),
                },
                TimestampNs(2_000_000_000),
                None,
                &mut rt,
                &snaps,
                &forensics,
                &mut engines,
                &mut gov,
                &mut ledger,
                r.arena(),
                &tg_tx,
                &vault_tx,
            );
            assert_eq!(engines[3].tranche1_harvests_count, 1);

            engines[3].last_eff_sl_ticks = PriceTicks(8_226);
            engines[3].chandelier_profit_sl_armed = true;
            handle_gateway_event(
                GatewayEvent::StopLossSynced {
                    slot_idx: 3,
                    sl_price: PriceTicks(8_226),
                    verified: false,
                },
                TimestampNs(3_000_000_000),
                None,
                &mut rt,
                &snaps,
                &forensics,
                &mut engines,
                &mut gov,
                &mut ledger,
                r.arena(),
                &tg_tx,
                &vault_tx,
            );
            assert_eq!(engines[3].last_eff_sl_ticks, orig_verified_sl);
            assert!(!engines[3].chandelier_profit_sl_armed);
            assert!(rt.sl_needs_sync[3]);

            handle_gateway_event(
                GatewayEvent::StopLossSynced {
                    slot_idx: 3,
                    sl_price: PriceTicks(8_226),
                    verified: true,
                },
                TimestampNs(4_000_000_000),
                None,
                &mut rt,
                &snaps,
                &forensics,
                &mut engines,
                &mut gov,
                &mut ledger,
                r.arena(),
                &tg_tx,
                &vault_tx,
            );
            assert_eq!(rt.verified_exchange_sl[3], PriceTicks(8_226));
            assert!(engines[3].chandelier_profit_sl_armed);
            assert!(!rt.sl_needs_sync[3]);

            finalize_and_emit_live_close(
                3,
                VerifiedCloseResolution {
                    kind: ClosedTradeKind::ServerMarkPriceStopLoss,
                    sub_reason: ExitSubReason::ChandelierProfitLockSl,
                    verified_entry_price: PriceTicks(8_200),
                    verified_exit_price: PriceTicks(8_226),
                    closed_lots: LotQty(26),
                    legs_count: 2,
                    exchange_closed_pnl_1e8: None,
                },
                TimestampNs(5_000_000_000),
                &snaps,
                &forensics,
                &mut engines,
                &mut gov,
                &mut ledger,
                r.arena(),
                &tg_tx,
                &vault_tx,
                &mut rt.halt_locked,
            );
            assert!(!rt.halt_locked);
            let (_, sl_st, win_st) = gov.slot_direction_diagnostics(3, Side::Bid);
            assert_eq!(sl_st, 0);
            assert_eq!(win_st, 1);
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
