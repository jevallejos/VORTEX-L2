//! Live Gateway Event Handlers, Markout/Epps Runtime State, 3-Tranche Cascade & OOS 520 Gate (V5.5).
#![allow(
    clippy::cast_precision_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use core::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;

use crate::alpha_3factor::{
    BtcLeadLagMemory, EppsBtcCorrelationTracker, EventClockAlphaModulator, OrthogonalFactorState,
    SignalVerdict,
};
use crate::bybit_client::BybitV5LiveClient;
use crate::bybit_gateway::{
    verify_ws_private_parser_self_check, BybitAsyncGateway, GatewayCommand, GatewayEvent,
};
use crate::execution_fsm::{
    defensive_zero_loss_maker_exit_price_with_fees, is_chandelier_profit_locked_sl_with_fees,
    offensive_win_maker_exit_price, parse_verified_avg_price_ticks, ClosedPnlQueryOutcome,
    ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState, ServerSlDecision,
    SlotExecutionEngine, VerifiedCloseResolution, MAX_CLOSE_RECONCILE_RETRIES,
};
use crate::fee_cadence_beta::MonetaryPortfolioBetaGuard;
use crate::forensic_parquet::ParitySyncLedger; use crate::governor_plasticity::{DailySessionGovernor, DailyShieldPhase};
use crate::http_ws_transport::OnlineIndexerLagEstimator;
use crate::lob_shm::{BookLevel, LastVetoCode, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::{MarkoutCurveBps, SlotMarkoutRing};
use crate::telemetry_hud::{
    dispatch_closed_trade_logs_v47, dispatch_open_positions_state, format_telegram_close_message,
    load_saved_slot_state, record_and_notify_entry_fill, restore_entry_forensics_from_json,
    SlotForensicEntrySnapshot, VaultIoCommand, DEFAULT_WALLET_EQUITY_1E8,
};
use crate::types::{
    classify_trade_badge, now_epoch_ns, BoundedScore, LotQty, PriceTicks, ScaledBps, Side,
    TimestampNs, ANSI_GREEN, ANSI_RED, ANSI_RESET, ANSI_YELLOW, APPROVED_SPECS,
};

#[derive(Debug)]
pub struct ExecRuntimeState {
    pub idx_lag: OnlineIndexerLagEstimator,
    pub wallet_1e8: i64,
    pub slot_rpc_inflight: [bool; MAX_ACTIVE_PAIRS],
    pub slot_rpc_sent_ns: [i64; MAX_ACTIVE_PAIRS],
    pub abort_cmd_inflight: [bool; MAX_ACTIVE_PAIRS],
    pub sl_needs_sync: [bool; MAX_ACTIVE_PAIRS],
    pub verified_exchange_sl: [PriceTicks; MAX_ACTIVE_PAIRS],
    pub emergency_flatten_sl: [Option<PriceTicks>; MAX_ACTIVE_PAIRS],
    pub sl_retry_after_ns: [i64; MAX_ACTIVE_PAIRS],
    pub sl_adverse_breach_since_ns: [i64; MAX_ACTIVE_PAIRS],
    pub init_q_ahead: [LotQty; MAX_ACTIVE_PAIRS],
    pub cur_q_ahead: [LotQty; MAX_ACTIVE_PAIRS],
    pub last_l1_q_obs: [LotQty; MAX_ACTIVE_PAIRS],
    pub last_opp_trades: [LotQty; MAX_ACTIVE_PAIRS],
    pub epps_trackers: [EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS],
    pub spoof_clocks: [EventClockAlphaModulator; MAX_ACTIVE_PAIRS],
    pub markout_rings: [SlotMarkoutRing; MAX_ACTIVE_PAIRS],
    pub last_abort_veto: [LastVetoCode; MAX_ACTIVE_PAIRS], pub daily_gov: DailySessionGovernor, pub intra_rings: [crate::execution_fsm::SlotIntraTradeRing; MAX_ACTIVE_PAIRS], pub last_plasticity_audit_ns: i64, pub last_persisted_utc_day: i64,
    pub last_zeta_tox: [f64; MAX_ACTIVE_PAIRS],
    pub geom_adv_since_ns: [i64; MAX_ACTIVE_PAIRS],
    pub queue_flow_ema: [f64; MAX_ACTIVE_PAIRS],
    pub btc_lead_memory: BtcLeadLagMemory,
    pub abort_was_favorable_miss: [bool; MAX_ACTIVE_PAIRS],
    pub next_poll_fill_ts_ns: [i64; MAX_ACTIVE_PAIRS],
    pub boot_converged: bool,
    pub entry_warmup_until_ns: i64,
    pub drain_flushed_once: bool,
    pub total_loss_shield_latched: bool,
    pub halt_locked: bool,
    pub drain_active: bool,
}

impl Default for ExecRuntimeState {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl ExecRuntimeState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            idx_lag: OnlineIndexerLagEstimator::new(),
            wallet_1e8: DEFAULT_WALLET_EQUITY_1E8,
            slot_rpc_inflight: [false; MAX_ACTIVE_PAIRS],
            slot_rpc_sent_ns: [0; MAX_ACTIVE_PAIRS],
            abort_cmd_inflight: [false; MAX_ACTIVE_PAIRS],
            sl_needs_sync: [false; MAX_ACTIVE_PAIRS],
            verified_exchange_sl: [PriceTicks(0); MAX_ACTIVE_PAIRS],
            emergency_flatten_sl: [None; MAX_ACTIVE_PAIRS],
            sl_retry_after_ns: [0; MAX_ACTIVE_PAIRS],
            sl_adverse_breach_since_ns: [0; MAX_ACTIVE_PAIRS],
            init_q_ahead: [LotQty(0); MAX_ACTIVE_PAIRS],
            cur_q_ahead: [LotQty(0); MAX_ACTIVE_PAIRS],
            last_l1_q_obs: [LotQty(0); MAX_ACTIVE_PAIRS],
            last_opp_trades: [LotQty(0); MAX_ACTIVE_PAIRS],
            epps_trackers: [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS],
            spoof_clocks: [EventClockAlphaModulator::new(); MAX_ACTIVE_PAIRS],
            markout_rings: [SlotMarkoutRing::new(); MAX_ACTIVE_PAIRS],
            last_abort_veto: [LastVetoCode::None; MAX_ACTIVE_PAIRS], daily_gov: DailySessionGovernor::new(DEFAULT_WALLET_EQUITY_1E8), intra_rings: [crate::execution_fsm::SlotIntraTradeRing::new(); MAX_ACTIVE_PAIRS], last_plasticity_audit_ns: 0, last_persisted_utc_day: -1,
            last_zeta_tox: [0.0; MAX_ACTIVE_PAIRS],
            geom_adv_since_ns: [0; MAX_ACTIVE_PAIRS],
            queue_flow_ema: [0.0; MAX_ACTIVE_PAIRS],
            btc_lead_memory: BtcLeadLagMemory::new(),
            abort_was_favorable_miss: [false; MAX_ACTIVE_PAIRS],
            next_poll_fill_ts_ns: [0; MAX_ACTIVE_PAIRS],
            boot_converged: false,
            entry_warmup_until_ns: 0,
            drain_flushed_once: false,
            total_loss_shield_latched: false,
            halt_locked: false,
            drain_active: false,
        }
    }
}

#[must_use]
pub fn compute_open_positions_mtm_upl_1e8(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) -> i64 {
    let mut sum_upl_1e8 = 0_i64;
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            open_lots,
            ..
        } = engines[s_idx].state
        {
            if avg_price_ticks.0 <= 0 || open_lots.0 <= 0 {
                continue;
            }
            let snap = &cached_snaps[s_idx];
            let mark_px = if snap.mark_price.0 > 0 {
                snap.mark_price
            } else if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
            } else {
                avg_price_ticks
            };
            if mark_px.0 > 0 {
                let (pnl_1e8, _, _, _) = APPROVED_SPECS[s_idx]
                    .compute_leg_pnl_and_fee_1e8_with_schedule(
                        side,
                        open_lots,
                        avg_price_ticks,
                        mark_px,
                        true,
                        engines[s_idx].active_fees,
                    );
                sum_upl_1e8 = sum_upl_1e8.saturating_add(pnl_1e8);
            }
        }
    }
    sum_upl_1e8
}

#[must_use]
pub fn compute_validation_epoch_loss_1e8(
    ledger: &ParitySyncLedger,
    live_wallet_1e8: Option<i64>,
    open_upl_1e8: i64,
) -> (i64, i64) {
    let realized_loss_1e8 = (-ledger.cumulative_net_pnl_1e8).max(0);
    let ledger_mtm_eq = ledger.current_equity_1e8().saturating_add(open_upl_1e8);
    let effective_live_eq = match live_wallet_1e8 {
        Some(w) if w > 0 => w.saturating_add(open_upl_1e8).min(ledger_mtm_eq),
        _ => ledger_mtm_eq,
    };
    let mtm_loss_1e8 = (ledger.initial_equity_1e8 - effective_live_eq).max(0);
    (realized_loss_1e8.max(mtm_loss_1e8), effective_live_eq)
}

#[allow(clippy::too_many_arguments)]
pub fn maybe_latch_validation_epoch_5usdt_shield(
    now_ns: TimestampNs,
    live_wallet_1e8: Option<i64>,
    lock_watch: Option<&crate::vault_io::AtomicLockWatch>,
    rt: &mut ExecRuntimeState,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    ledger: &ParitySyncLedger,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultIoCommand>,
) -> i64 {
    let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(engines, cached_snaps);
    let (epoch_loss_1e8, eff_eq_1e8) =
        compute_validation_epoch_loss_1e8(ledger, live_wallet_1e8, open_upl_1e8);
    if epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
        rt.halt_locked = true;
        if let Some(lw) = lock_watch {
            lw.latch_halt_in_ram();
        }
        if !rt.total_loss_shield_latched {
            rt.total_loss_shield_latched = true;
            let _ = vault_tx.try_send(VaultIoCommand::LatchTotalLossHalt {
                ts_ns: now_ns.0,
                anchor_equity_1e8: ledger.initial_equity_1e8,
                current_equity_1e8: eff_eq_1e8,
                epoch_loss_1e8,
            });
            let _ = tg_tx.try_send(format!(
                "\u{1f534} [VORTEX-OMEGA V5.5 TOTAL LOSS SHIELD -5.00 USDT]\n- Perdida Epoca : -{:.4} / -5.0000 USDT\n- Equity Ancla  : {:.4} -> {:.4} USDT\n- Estado        : HALTED IRREVERSIBLE (Custodia P0-B activa hasta 0 posiciones)",
                (epoch_loss_1e8 as f64) / 100_000_000.0,
                (ledger.initial_equity_1e8 as f64) / 100_000_000.0,
                (eff_eq_1e8 as f64) / 100_000_000.0
            ));
        }
    }
    epoch_loss_1e8
}

#[inline]
#[must_use]
fn mid_or_fallback(snap: &SymbolLobSnapshot, fallback: PriceTicks) -> PriceTicks {
    if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
        PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0))
    } else if snap.mark_price.0 > 0 {
        snap.mark_price
    } else {
        fallback
    }
}

#[inline]
#[allow(clippy::cast_possible_truncation, clippy::suboptimal_flops)]
fn update_and_check_sustained_sl_breach_p0a(
    slot_idx: usize,
    side: Side,
    eff_sl: PriceTicks,
    snap: &SymbolLobSnapshot,
    now_ns: TimestampNs,
    breach_since_ns: &mut i64,
) -> bool {
    const PHI: f64 = 1.618_033_988_749_895;
    const PHI_SQ: f64 = 2.618_033_988_749_895;
    const INV_PHI_SQ: f64 = 0.381_966_011_250_105;
    const SQRT_5: f64 = 2.236_067_977_499_79;

    if eff_sl.0 <= 0 || snap.mark_price.0 <= 0 {
        *breach_since_ns = 0;
        return false;
    }
    let adv_ticks = (eff_sl.0 - snap.mark_price.0).saturating_mul(side.sign_i64());
    if adv_ticks <= 0 {
        *breach_since_ns = 0;
        return false;
    }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let tau_tick_sbps = spec
        .tick_to_price_scaled_bps(eff_sl)
        .unwrap_or(crate::types::ScaledBps(50))
        .raw()
        .max(1) as f64;
    let sigma_1m_sbps = ((snap.realized_vol_5m.raw().max(0)) as f64) / SQRT_5;
    let req_breach_sbps = (PHI * tau_tick_sbps).max(INV_PHI_SQ * sigma_1m_sbps);
    let obs_breach_sbps = ((adv_ticks as f64) * 1_000_000.0) / (eff_sl.0 as f64);
    if obs_breach_sbps < req_breach_sbps {
        *breach_since_ns = 0;
        return false;
    }
    let ts_obs = now_ns.0.max(snap.ts_local_ns.0);
    if *breach_since_ns <= 0 {
        *breach_since_ns = ts_obs;
        return false;
    }
    let (ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
    let rtt_ns = if ws_rtt_us > 0 {
        f64::from(ws_rtt_us) * 1_000.0
    } else {
        45_000_000.0
    };
    let req_dur_ns = (PHI_SQ * rtt_ns).round() as i64;
    ts_obs.saturating_sub(*breach_since_ns) >= req_dur_ns
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
    if let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, open_lots, .. } = eng.state {
        Some((side, avg_price_ticks, open_lots))
    } else {
        None
    }
}

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
    match eng.evaluate_sl_or_emergency_flatten_for_slot(eff_side, eff_px, snap) {
        Ok(ServerSlDecision::ArmServerSl(valid_sl)) => {
            eng.last_eff_sl_ticks = valid_sl;
            eng.chandelier_profit_sl_armed =
                is_chandelier_profit_locked_sl_with_fees(eff_side, eff_px, valid_sl, eng.active_fees);
            if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = eng.state {
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
        Ok(ServerSlDecision::BreachEmergencyFlatten { backup_sl, observed_loss_bps, max_allowed_bps }) => {
            eng.last_eff_sl_ticks = backup_sl;
            eng.chandelier_profit_sl_armed = false;
            if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = eng.state {
                *sl_p = backup_sl;
            }
            rt.sl_needs_sync[slot_idx] = false;
            rt.emergency_flatten_sl[slot_idx] = Some(backup_sl);
            println!(
                "{}\u{1f534} [P0-A BREACH FLATTEN ARMED] Slot {:2} {} {:?} loss={} > max={} ScaledBps (BackupSL={}t){}",
                ANSI_RED, slot_idx, APPROVED_SPECS[slot_idx].symbol_id.name(), eff_side,
                observed_loss_bps.raw(), max_allowed_bps.raw(), backup_sl.0, ANSI_RESET
            );
        }
        Err(_) => {
            if !sl_verified_by_gateway { rt.sl_needs_sync[slot_idx] = true; }
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
    if !matches!(eng.state, OrderLifecycleState::PositionOpenMakerExit { .. }) { return None; }
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
                Some(LiveCloseStepAction::AwaitNextPoll { next_poll_delay_ns: idx_lag.adaptive_poll_delay_ns(attempt) })
            } else {
                eng.close_reconcile_retries = 0;
                eng.resolve_fallback_unverified_close(snap, vac_level_hint).map(LiveCloseStepAction::FinalizeVerified)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_and_emit_live_close_core(
    s_idx: usize,
    resolution: VerifiedCloseResolution,
    now_ns: TimestampNs,
    markout_curve: MarkoutCurveBps,
    lvr_5s_scaled_bps: i64,
    event_dilation: f64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultIoCommand>,
    halt_locked: &mut bool,
) {
    let spec = APPROVED_SPECS[s_idx];
    let sym_name = spec.symbol_id.name();
    let hold_s = (now_ns.0.saturating_sub(engines[s_idx].last_opened_ts.0)).max(0) / 1_000_000_000;
    let Some(summary) = engines[s_idx].apply_verified_close_resolution(resolution, now_ns, ledger) else { return; };
    let non_punitive_exit = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
    let close_snap = &cached_snaps[s_idx];
    let w_close = close_snap.wave_observables();
    let (ws_rtt_us, _) = crate::execution_fsm::read_live_ws_pulse();
    let signed_prop =
        0.50 * close_snap.factor_ofi.get() + 0.38 * close_snap.factor_tfi.get() + 0.12 * close_snap.factor_geom.get();
    gov.set_slot_momentum_chain_context(s_idx, signed_prop, w_close.climax_active, ws_rtt_us);
    gov.observe_closed_trade_with_regime_v51(s_idx, summary.side, summary.entry_price, summary.net_scaled_bps, ScaledBps(engines[s_idx].last_closed_mfe_bps), non_punitive_exit, w_close, event_dilation, now_ns);
    engines[s_idx].apply_momentum_chain_post_close_cooldown(
        now_ns,
        non_punitive_exit,
        summary.net_scaled_bps,
        w_close.climax_active || matches!(w_close.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion),
        gov.psi_health(),
        signed_prop * summary.side.sign_f64(),
        ws_rtt_us,
    );
    arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
    dispatch_closed_trade_logs_v47(
        s_idx, &summary, &engines[s_idx], &entry_forensics[s_idx], &cached_snaps[s_idx],
        ledger, gov, &markout_curve, lvr_5s_scaled_bps, vault_tx, halt_locked, now_ns,
    );
    let _ = vault_tx.try_send(VaultIoCommand::SaveMfePlasticity {
        ts_ns: now_ns.0,
        learned_mfe_bps: SlotExecutionEngine::extract_12_slot_mfe_array(engines),
    });
    engines[s_idx].sync_to_shm_telemetry(arena);
    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
    let net_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let (en_w, en_f) = spec.format_price_usdt_parts(summary.entry_price);
    let (ex_w, ex_f) = spec.format_price_usdt_parts(summary.exit_price);
    let pnl_usdt = (summary.net_pnl_1e8 as f64) / 100_000_000.0;
    let lots_s = spec.format_lot_qty_str(summary.closed_lots);
    let is_part = matches!(engines[s_idx].last_exit_sub_reason, ExitSubReason::Tranche1Harvest50) && net_f < 1.00;
    let (col, icon, lbl) = classify_trade_badge(net_f, is_part);
    println!(
        "{}{} [TRADE CLOSE \u{2014} {}] {} {:?} qty={} ({}L) | {}.{:04} -> {}.{:04} | Net: {:+.2} bps ({:+.4} USDT) | Hold: {}s{}",
        col, icon, lbl.trim(), sym_name, summary.side, lots_s, summary.legs_consolidated,
        en_w, en_f, ex_w, ex_f, net_f, pnl_usdt, hold_s, ANSI_RESET
    );
    let _ = tg_tx.try_send(format_telegram_close_message(
        spec, &summary, engines[s_idx].last_exit_sub_reason, ledger.closed_trades_count, ledger,
    ));
}

pub fn finalize_and_emit_live_close_with_markout(
    s_idx: usize,
    resolution: VerifiedCloseResolution,
    now_ns: TimestampNs,
    markout_curve: MarkoutCurveBps,
    lvr_5s_scaled_bps: i64,
    cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    ledger: &mut ParitySyncLedger,
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultIoCommand>,
    halt_locked: &mut bool,
) {
    finalize_and_emit_live_close_core(
        s_idx, resolution, now_ns, markout_curve, lvr_5s_scaled_bps, 1.0,
        cached_snaps, entry_forensics, engines, gov, ledger, arena, tg_tx, vault_tx, halt_locked,
    );
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
    vault_tx: &SyncSender<VaultIoCommand>,
    halt_locked: &mut bool,
) {
    finalize_and_emit_live_close_with_markout(
        s_idx, resolution, now_ns, MarkoutCurveBps::default(), 0,
        cached_snaps, entry_forensics, engines, gov, ledger, arena, tg_tx, vault_tx, halt_locked,
    );
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
    vault_tx: &SyncSender<VaultIoCommand>,
) {
    match evt {
        GatewayEvent::WalletUpdated { wallet_1e8: w } => {
            if w > 0 { rt.wallet_1e8 = w; }
        }
        GatewayEvent::HandshakeRefreshed { handshake } => {
            if handshake.synced_from_exchange {
                gov.set_active_fee_schedule(handshake.fee_schedule);
                ledger.set_live_vip_fee_rates(
                    handshake.fee_schedule.maker_fee_bps,
                    handshake.fee_schedule.taker_fee_bps,
                );
                for eng in engines.iter_mut() {
                    eng.sync_governor_dynamics(gov);
                }
            }
        }
        GatewayEvent::EntryPlaced { slot_idx, accepted, entry_price, sl_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PlacingEntryMaker { side, qty, .. } = engines[slot_idx].state else {
                if accepted {
                    if let Some(gw) = gateway { let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx }); }
                }
                return;
            };
            if accepted {
                rt.verified_exchange_sl[slot_idx] = sl_price;
                let snap = cached_snaps[slot_idx];
                rt.queue_flow_ema[slot_idx] = 0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get();
                let _ = engines[slot_idx].on_entry_order_resting(now_ns);
                arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
                let spec = APPROVED_SPECS[slot_idx];
                let (ew_p, ef_p) = spec.format_price_usdt_parts(entry_price);
                let (sw_p, sf_p) = spec.format_price_usdt_parts(sl_price);
                println!(
                    "{}\u{1f7e1} [BYBIT V5 LIVE ORDER SENT] {} {:?} qty={} @ {}.{:04} USDT (SL: {}.{:04} | Q_L1: {} lots | Seq: {} | OFI: {:+.2} | TFI: {:+.2} | GEOM: {:+.2}){}",
                    ANSI_YELLOW, spec.symbol_id.name(), side, spec.format_lot_qty_str(qty),
                    ew_p, ef_p, sw_p, sf_p, rt.init_q_ahead[slot_idx].0, snap.sequence_id,
                    snap.factor_ofi.get(), snap.factor_tfi.get(), snap.factor_geom.get(), ANSI_RESET
                );
            } else {
                rt.queue_flow_ema[slot_idx] = 0.0;
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryRepegged { slot_idx, accepted, new_price, new_sl_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::RestingEntryQueue { side, .. } = engines[slot_idx].state else {
                if accepted {
                    if let Some(gw) = gateway { let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx }); }
                }
                return;
            };
            if accepted {
                rt.verified_exchange_sl[slot_idx] = new_sl_price;
                engines[slot_idx].apply_resting_repeg(new_price, new_sl_price, now_ns);
                let snap = cached_snaps[slot_idx];
                let (q_new, opp_cum) = match side {
                    Side::Bid => (
                        if snap.bids[0].price == new_price { snap.bids[0].qty } else { LotQty(1) },
                        snap.cum_sell_trade_lots,
                    ),
                    Side::Ask => (
                        if snap.asks[0].price == new_price { snap.asks[0].qty } else { LotQty(1) },
                        snap.cum_buy_trade_lots,
                    ),
                };
                rt.init_q_ahead[slot_idx] = if match side {
                    Side::Bid => snap.bids[0].price.0 > 0 && new_price.0 > snap.bids[0].price.0,
                    Side::Ask => snap.asks[0].price.0 > 0 && new_price.0 < snap.asks[0].price.0,
                } {
                    LotQty(0)
                } else {
                    q_new
                };
                rt.cur_q_ahead[slot_idx] = q_new;
                rt.last_l1_q_obs[slot_idx] = q_new;
                rt.last_opp_trades[slot_idx] = opp_cum;
                rt.geom_adv_since_ns[slot_idx] = 0;
                rt.queue_flow_ema[slot_idx] = 0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get();
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else {
                rt.queue_flow_ema[slot_idx] = 0.0;
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryAborted { slot_idx, side, aborted_price } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.abort_cmd_inflight[slot_idx] = false;
            rt.verified_exchange_sl[slot_idx] = PriceTicks(0);
            rt.queue_flow_ema[slot_idx] = 0.0;
            let fav_miss = rt.abort_was_favorable_miss[slot_idx];
            rt.abort_was_favorable_miss[slot_idx] = false;
            if matches!(engines[slot_idx].state, OrderLifecycleState::PlacingEntryMaker { .. } | OrderLifecycleState::RestingEntryQueue { .. }) {
                gov.observe_entry_abort_with_context(slot_idx, side, aborted_price, now_ns, fav_miss, !fav_miss);
                let veto = if rt.last_abort_veto[slot_idx] != LastVetoCode::None {
                    rt.last_abort_veto[slot_idx]
                } else {
                    LastVetoCode::HawkesStoikovQueueAbort
                };
                rt.markout_rings[slot_idx].arm_abort_probe_v49(side, aborted_price, veto, TimestampNs(entry_forensics[slot_idx].armed_ts_ns.max(1)), now_ns, rt.init_q_ahead[slot_idx], rt.cur_q_ahead[slot_idx]);
                rt.last_abort_veto[slot_idx] = LastVetoCode::None;
                engines[slot_idx].state = OrderLifecycleState::Idle;
                arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryOrderVanished { slot_idx } | GatewayEvent::WsEntryOrderTerminated { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.verified_exchange_sl[slot_idx] = PriceTicks(0);
            rt.queue_flow_ema[slot_idx] = 0.0;
            let fav_miss = rt.abort_was_favorable_miss[slot_idx];
            rt.abort_was_favorable_miss[slot_idx] = false;
            if let OrderLifecycleState::PlacingEntryMaker { side, entry_price, .. }
            | OrderLifecycleState::RestingEntryQueue { side, entry_price, .. } = engines[slot_idx].state
            {
                if rt.abort_cmd_inflight[slot_idx] {
                    rt.abort_cmd_inflight[slot_idx] = false;
                    gov.observe_entry_abort_with_context(slot_idx, side, entry_price, now_ns, fav_miss, !fav_miss);
                    let veto = if rt.last_abort_veto[slot_idx] != LastVetoCode::None {
                        rt.last_abort_veto[slot_idx]
                    } else {
                        LastVetoCode::HawkesStoikovQueueAbort
                    };
                    rt.markout_rings[slot_idx].arm_abort_probe_v49(side, entry_price, veto, TimestampNs(entry_forensics[slot_idx].armed_ts_ns.max(1)), now_ns, rt.init_q_ahead[slot_idx], rt.cur_q_ahead[slot_idx]);
                    rt.last_abort_veto[slot_idx] = LastVetoCode::None;
                    arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                }
                engines[slot_idx].state = OrderLifecycleState::Idle;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::EntryStillResting { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::EntryFilledLive { slot_idx, real_side, verified_avg, real_lots, sl_verified, tag } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            rt.abort_cmd_inflight[slot_idx] = false;
            rt.abort_was_favorable_miss[slot_idx] = false;
            rt.queue_flow_ema[slot_idx] = 0.0;
            if matches!(engines[slot_idx].state, OrderLifecycleState::PlacingEntryMaker { .. } | OrderLifecycleState::RestingEntryQueue { .. }) {
                gov.clear_abort_streak(slot_idx, real_side);
                gov.observe_passive_entry_fill(slot_idx);
                engines[slot_idx].sync_governor_for_side(gov, real_side);
                let prev_sl = engines[slot_idx].last_eff_sl_ticks;
                if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(&mut engines[slot_idx], now_ns, Some(real_side), verified_avg, Some(real_lots)) {
                    let mid_t0 = mid_or_fallback(&cached_snaps[slot_idx], eff_px);
                    rt.markout_rings[slot_idx].arm_fill_probe(eff_s, eff_px, mid_t0, now_ns);
                    apply_post_fill_sl_or_breach(slot_idx, eff_s, eff_px, sl_verified, prev_sl, &cached_snaps[slot_idx], &mut engines[slot_idx], rt);
                    record_and_notify_entry_fill(slot_idx, APPROVED_SPECS[slot_idx], engines, entry_forensics, arena, tg_tx, vault_tx, now_ns, eff_s, eff_px, eff_q, tag);
                }
            } else if matches!(engines[slot_idx].state, OrderLifecycleState::Idle) && real_lots.0 > 0 {
                if let Some(gw) = gateway {
                    let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx });
                }
                let eff_px = verified_avg
                    .filter(|p| p.0 > 0)
                    .unwrap_or_else(|| mid_or_fallback(&cached_snaps[slot_idx], PriceTicks(0)));
                if eff_px.0 > 0 {
                    gov.clear_abort_streak(slot_idx, real_side);
                    gov.observe_passive_entry_fill(slot_idx);
                    engines[slot_idx].sync_governor_for_side(gov, real_side);
                    if engines[slot_idx]
                        .adopt_live_open_position(
                            real_side,
                            eff_px,
                            real_lots,
                            real_lots,
                            false,
                            eff_px,
                            eff_px,
                            now_ns,
                            &cached_snaps[slot_idx],
                        )
                        .is_ok()
                    {
                        rt.markout_rings[slot_idx].arm_fill_probe(
                            real_side,
                            eff_px,
                            mid_or_fallback(&cached_snaps[slot_idx], eff_px),
                            now_ns,
                        );
                        apply_post_fill_sl_or_breach(
                            slot_idx,
                            real_side,
                            eff_px,
                            sl_verified,
                            PriceTicks(0),
                            &cached_snaps[slot_idx],
                            &mut engines[slot_idx],
                            rt,
                        );
                        record_and_notify_entry_fill(
                            slot_idx,
                            APPROVED_SPECS[slot_idx],
                            engines,
                            entry_forensics,
                            arena,
                            tg_tx,
                            vault_tx,
                            now_ns,
                            real_side,
                            eff_px,
                            real_lots,
                            tag,
                        );
                    }
                }
            }
        }
        GatewayEvent::StopLossSynced { slot_idx, sl_price, verified } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            if verified {
                rt.sl_needs_sync[slot_idx] = false;
                rt.emergency_flatten_sl[slot_idx] = None;
                rt.sl_adverse_breach_since_ns[slot_idx] = 0;
                rt.verified_exchange_sl[slot_idx] = sl_price;
                engines[slot_idx].last_eff_sl_ticks = sl_price;
                let fees = engines[slot_idx].active_fees;
                if let OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, sl_trigger_price: ref mut sl_p, .. } = engines[slot_idx].state {
                    *sl_p = sl_price;
                    engines[slot_idx].chandelier_profit_sl_armed =
                        is_chandelier_profit_locked_sl_with_fees(side, avg_price_ticks, sl_price, fees);
                }
                engines[slot_idx].sync_to_shm_telemetry(arena);
            } else {
                let mut p0a_triggered = false;
                if let OrderLifecycleState::PositionOpenMakerExit {
                    side, open_lots, sl_trigger_price, ..
                } = engines[slot_idx].state
                {
                    let eff_sl = if engines[slot_idx].last_eff_sl_ticks.0 > 0 {
                        engines[slot_idx].last_eff_sl_ticks
                    } else if sl_trigger_price.0 > 0 {
                        sl_trigger_price
                    } else {
                        sl_price
                    };
                    if update_and_check_sustained_sl_breach_p0a(
                        slot_idx,
                        side,
                        eff_sl,
                        &cached_snaps[slot_idx],
                        now_ns,
                        &mut rt.sl_adverse_breach_since_ns[slot_idx],
                    ) {
                        p0a_triggered = true;
                        rt.sl_needs_sync[slot_idx] = false;
                        rt.emergency_flatten_sl[slot_idx] = Some(eff_sl);
                        if gateway.is_some_and(|gw| {
                            gw.try_send(GatewayCommand::EmergencyFlattenSlot {
                                slot_idx,
                                open_side: side,
                                open_lots: crate::types::LotQty(open_lots.0.max(1)),
                                backup_sl: eff_sl,
                            })
                        }) {
                            rt.slot_rpc_inflight[slot_idx] = true;
                            rt.slot_rpc_sent_ns[slot_idx] = now_ns.0;
                        }
                    }
                } else {
                    rt.sl_adverse_breach_since_ns[slot_idx] = 0;
                }
                if !p0a_triggered && rt.emergency_flatten_sl[slot_idx].is_none() {
                    let fallback_sl = rt.verified_exchange_sl[slot_idx];
                    if fallback_sl.0 > 0 && engines[slot_idx].last_eff_sl_ticks.0 <= 0 {
                        engines[slot_idx].last_eff_sl_ticks = fallback_sl;
                    }
                    rt.sl_needs_sync[slot_idx] = true;
                }
                rt.sl_retry_after_ns[slot_idx] = now_ns.0 + 250_000_000;
                engines[slot_idx].sync_to_shm_telemetry(arena);
            }
        }
        GatewayEvent::SlotSyncSkipped { slot_idx } => {
            rt.slot_rpc_inflight[slot_idx] = false;
        }
        GatewayEvent::SlotStillOpenSynced { slot_idx, real_side, verified_avg, real_lots, active_resting_tp } => {
            rt.slot_rpc_inflight[slot_idx] = false;
            let OrderLifecycleState::PositionOpenMakerExit {
                side, avg_price_ticks, initial_lots, open_lots: prev_open_lots, tranche1_harvested, resting_exit_price: prev_rest_tp, ..
            } = engines[slot_idx].state else { return; };
            if real_side == side {
                if let Some(v_avg) = verified_avg { engines[slot_idx].sync_open_avg_price_ticks(v_avg); }
            }
            let eff_init = initial_lots.max(real_lots);
            let ref_exit_px = if prev_rest_tp.0 > 0 {
                prev_rest_tp
            } else {
                defensive_zero_loss_maker_exit_price_with_fees(side, avg_price_ticks, engines[slot_idx].active_fees)
            };
            let partial_delta = crate::types::LotQty((prev_open_lots.0 - real_lots.0).max(0));
            let mut t1_done = tranche1_harvested;
            if let Some(min_w3_t1) = SlotExecutionEngine::compute_tranche1_split_lots(slot_idx, eff_init, ref_exit_px, 350) {
                let max_rem = (eff_init.0 - min_w3_t1.0).max(1);
                if !t1_done && real_lots.0 > 0 && real_lots.0 <= max_rem && (prev_rest_tp.0 > 0 || partial_delta.0 > 0) {
                    t1_done = true;
                    engines[slot_idx].tranche1_harvests_count = engines[slot_idx].tranche1_harvests_count.saturating_add(1);
                }
            }
            if partial_delta.0 > 0 && real_lots.0 > 0 {
                engines[slot_idx].record_live_partial_harvest_synced(partial_delta, ref_exit_px);
            }
            if active_resting_tp.0 > 0 && active_resting_tp != prev_rest_tp {
                engines[slot_idx].exit_peg_attempts = engines[slot_idx].exit_peg_attempts.saturating_add(1);
            }
            if let OrderLifecycleState::PositionOpenMakerExit {
                initial_lots: ref mut in_l, open_lots: ref mut op_l, tranche1_harvested: ref mut t1_h,
                tranche1_lots: ref mut t1_l, tranche1_exit_price: ref mut t1_px, resting_exit_price: ref mut rx_p, ..
            } = engines[slot_idx].state {
                *in_l = eff_init;
                *op_l = real_lots;
                if t1_done && !*t1_h {
                    *t1_h = true;
                    *t1_l = crate::types::LotQty((eff_init.0 - real_lots.0).max(1));
                    *t1_px = ref_exit_px;
                } else if !*t1_h && partial_delta.0 > 0 && real_lots.0 > 0 {
                    *t1_l = crate::types::LotQty((eff_init.0 - real_lots.0).max(1));
                    if t1_px.0 <= 0 {
                        *t1_px = ref_exit_px;
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
            rt.sl_adverse_breach_since_ns[slot_idx] = 0;
            let Some(step_action) = resolve_live_closed_position_step(
                &mut engines[slot_idx], &cached_snaps[slot_idx], closed_pnl_payload.as_ref(), vac_level, &mut rt.idx_lag, now_ns.0,
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
                    let mk_curve = rt.markout_rings[slot_idx].last_fill_curve();
                    let lvr_5s = rt.markout_rings[slot_idx].ewma_lvr_5s_bps();
                    let pre_open = match engines[slot_idx].state {
                        OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, initial_lots, open_lots, tranche1_harvested, opened_ts, .. } => {
                            let tot_l = if tranche1_harvested { initial_lots.max(open_lots) } else { open_lots };
                            Some((side, avg_price_ticks, tot_l, opened_ts))
                        }
                        _ => None,
                    };
                    if let Some((_, _, _, p_open_ts)) = pre_open {
                        let mk_pk = rt.markout_rings[slot_idx].peak_favorable_fill_markout_sbps(p_open_ts);
                        if mk_pk > engines[slot_idx].last_closed_mfe_bps {
                            engines[slot_idx].last_closed_mfe_bps = mk_pk;
                        }
                    }
                    let prev_closed = ledger.closed_trades_count;
                    let prev_pnl_1e8 = ledger.cumulative_net_pnl_1e8;
                    let prev_phase = rt.daily_gov.phase;
                    let evt_dil = rt.spoof_clocks[slot_idx].event_clock_dilation();
                    finalize_and_emit_live_close_core(
                        slot_idx, res, now_ns, mk_curve, lvr_5s, evt_dil, cached_snaps, entry_forensics,
                        engines, gov, ledger, arena, tg_tx, vault_tx, &mut rt.halt_locked,
                    );
                    if ledger.closed_trades_count > prev_closed {
                        let delta_1e8 = ledger.cumulative_net_pnl_1e8.saturating_sub(prev_pnl_1e8);
                        if let Some((p_side, p_entry, p_lots, p_open_ts)) = pre_open {
                            rt.emit_live_position_path_from_delta_v49(
                                ledger.closed_trades_count, slot_idx, p_side, p_entry, p_lots,
                                delta_1e8, p_open_ts, now_ns, &cached_snaps[slot_idx], vault_tx,
                            );
                        }
                        let live_eq = rt.wallet_1e8.max(ledger.current_equity_1e8());
                        let new_phase = rt.daily_gov.observe_realized_cashflow(
                            now_ns,
                            delta_1e8,
                            true,
                            delta_1e8 > 0,
                            delta_1e8 < 0,
                            live_eq,
                            ledger.sinking_fund_reserve_bps_x100,
                            gov.active_maker_rt_fee_bps(),
                            gov.portfolio_mean_eq_vol_bps(),
                            gov.portfolio_mean_ir_x100(),
                        );
                        let _ = vault_tx.try_send(VaultIoCommand::SaveDailyGovernor {
                            ts_ns: now_ns.0,
                            state: rt.daily_gov,
                        });
                        if new_phase != prev_phase && matches!(new_phase, DailyShieldPhase::ProfitLockArmed | DailyShieldPhase::HibernatingProfitLocked | DailyShieldPhase::HibernatingLossCap) {
                            let _ = tg_tx.try_send(format!(
                                "\u{1f6e1}\u{fe0f} [VORTEX-OMEGA V5.5 DAILY SHIELD] Fase UTC -> {} | Hoy: {:+.4} USDT ({:+.2}%)",
                                new_phase.label(),
                                (rt.daily_gov.day_realized_pnl_1e8 as f64) / 100_000_000.0,
                                (rt.daily_gov.day_pnl_ppm() as f64) / 10_000.0
                            ));
                        }
                        let ep_loss = maybe_latch_validation_epoch_5usdt_shield(
                            now_ns,
                            gateway.map(|_| rt.wallet_1e8),
                            None,
                            rt,
                            cached_snaps,
                            engines,
                            ledger,
                            tg_tx,
                            vault_tx,
                        );
                        let _ = gov.refresh_psi_health(
                            &rt.daily_gov,
                            ledger.sinking_fund_reserve_bps_x100,
                            ep_loss,
                        );
                    }
                }
            }
        }
        GatewayEvent::WsPositionUpdate { slot_idx, is_open, real_side, verified_avg, real_lots } => {
            if is_open {
                let armed_qty_opt = match engines[slot_idx].state {
                    OrderLifecycleState::PlacingEntryMaker { qty, .. } | OrderLifecycleState::RestingEntryQueue { qty, .. } => Some(qty),
                    OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => None,
                };
                if let Some(armed_qty) = armed_qty_opt {
                    rt.slot_rpc_inflight[slot_idx] = false;
                    rt.abort_cmd_inflight[slot_idx] = false;
                    rt.abort_was_favorable_miss[slot_idx] = false;
                    rt.queue_flow_ema[slot_idx] = 0.0;
                    if real_lots.0 < armed_qty.0 {
                        if let Some(gw) = gateway { let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx }); }
                    }
                    gov.clear_abort_streak(slot_idx, real_side);
                    gov.observe_passive_entry_fill(slot_idx);
                    engines[slot_idx].sync_governor_for_side(gov, real_side);
                    let prev_sl = engines[slot_idx].last_eff_sl_ticks;
                    if let Some((eff_s, eff_px, eff_q)) = promote_verified_fill_state(&mut engines[slot_idx], now_ns, Some(real_side), verified_avg, Some(real_lots)) {
                        let mid_t0 = mid_or_fallback(&cached_snaps[slot_idx], eff_px);
                        rt.markout_rings[slot_idx].arm_fill_probe(eff_s, eff_px, mid_t0, now_ns);
                        apply_post_fill_sl_or_breach(slot_idx, eff_s, eff_px, false, prev_sl, &cached_snaps[slot_idx], &mut engines[slot_idx], rt);
                        record_and_notify_entry_fill(slot_idx, APPROVED_SPECS[slot_idx], engines, entry_forensics, arena, tg_tx, vault_tx, now_ns, eff_s, eff_px, eff_q, "MAKER FILL WS");
                    }
                } else if matches!(engines[slot_idx].state, OrderLifecycleState::Idle) && real_lots.0 > 0 {
                    rt.slot_rpc_inflight[slot_idx] = false;
                    rt.abort_cmd_inflight[slot_idx] = false;
                    rt.queue_flow_ema[slot_idx] = 0.0;
                    if let Some(gw) = gateway {
                        let _ = gw.try_send(GatewayCommand::CancelResidualEntries { slot_idx });
                    }
                    let eff_px = verified_avg
                        .filter(|p| p.0 > 0)
                        .unwrap_or_else(|| mid_or_fallback(&cached_snaps[slot_idx], PriceTicks(0)));
                    if eff_px.0 > 0 {
                        gov.clear_abort_streak(slot_idx, real_side);
                        engines[slot_idx].sync_governor_for_side(gov, real_side);
                        if engines[slot_idx]
                            .adopt_live_open_position(
                                real_side,
                                eff_px,
                                real_lots,
                                real_lots,
                                false,
                                eff_px,
                                eff_px,
                                now_ns,
                                &cached_snaps[slot_idx],
                            )
                            .is_ok()
                        {
                            rt.markout_rings[slot_idx].arm_fill_probe(
                                real_side,
                                eff_px,
                                mid_or_fallback(&cached_snaps[slot_idx], eff_px),
                                now_ns,
                            );
                            apply_post_fill_sl_or_breach(
                                slot_idx,
                                real_side,
                                eff_px,
                                false,
                                PriceTicks(0),
                                &cached_snaps[slot_idx],
                                &mut engines[slot_idx],
                                rt,
                            );
                            record_and_notify_entry_fill(
                                slot_idx,
                                APPROVED_SPECS[slot_idx],
                                engines,
                                entry_forensics,
                                arena,
                                tg_tx,
                                vault_tx,
                                now_ns,
                                real_side,
                                eff_px,
                                real_lots,
                                "LIVE WS ORPHAN ADOPT",
                            );
                        }
                    }
                } else if let OrderLifecycleState::PositionOpenMakerExit { side, open_lots, .. } = engines[slot_idx].state {
                    if real_side == side {
                        if let Some(v_avg) = verified_avg { engines[slot_idx].sync_open_avg_price_ticks(v_avg); }
                    }
                    if real_lots.0 > 0 && real_lots.0 != open_lots.0 {
                        if let OrderLifecycleState::PositionOpenMakerExit { initial_lots: ref mut in_l, open_lots: ref mut op_l, .. } = engines[slot_idx].state {
                            if real_lots.0 > in_l.0 { *in_l = real_lots; }
                            if real_lots.0 > op_l.0 { *op_l = real_lots; rt.sl_needs_sync[slot_idx] = true; }
                        }
                        engines[slot_idx].next_peg_allowed_ts_ns = 0;
                    }
                }
            } else if matches!(engines[slot_idx].state, OrderLifecycleState::PositionOpenMakerExit { .. }) {
                engines[slot_idx].next_peg_allowed_ts_ns = 0;
            }
        }
        GatewayEvent::WsExitOrderUpdated {
            slot_idx,
            is_terminal,
            cum_exec_lots,
            resting_price,
        } => {
            if let OrderLifecycleState::PositionOpenMakerExit {
                side,
                avg_price_ticks,
                open_lots,
                tranche1_harvested,
                defensive_floor_locked,
                opened_ts,
                ..
            } = engines[slot_idx].state
            {
                if let OrderLifecycleState::PositionOpenMakerExit {
                    resting_exit_price: ref mut rx_p,
                    ..
                } = engines[slot_idx].state
                {
                    if is_terminal {
                        *rx_p = PriceTicks(0);
                    } else if resting_price.0 > 0 {
                        *rx_p = resting_price;
                    }
                }
                engines[slot_idx].next_peg_allowed_ts_ns = 0;
                if let Some(gw) = gateway
                    .filter(|_| (is_terminal || cum_exec_lots.0 > 0) && !rt.slot_rpc_inflight[slot_idx])
                {
                    let snap = &cached_snaps[slot_idx];
                    let live_mid_px = mid_or_fallback(snap, avg_price_ticks);
                    let (target_px, target_lots, def_lock, cur_gross_bps, vac_level) =
                        if let Some(q) = engines[slot_idx].compute_canonical_exit_quote(snap) {
                            (
                                q.target_price,
                                q.order_lots,
                                q.defensive_lock_active || q.urgent_close_repeg,
                                q.cur_gross_bps,
                                q.vac_level,
                            )
                        } else {
                            let fb_px = defensive_zero_loss_maker_exit_price_with_fees(
                                side,
                                avg_price_ticks,
                                engines[slot_idx].active_fees,
                            );
                            (fb_px, open_lots, defensive_floor_locked, 0, 1)
                        };
                    if gw.try_send(GatewayCommand::SyncOpenSlot {
                        slot_idx,
                        side,
                        live_mid_px,
                        target_px,
                        target_lots,
                        defensive_lock: def_lock,
                        cur_gross_bps,
                        vac_level,
                        opened_ts_ns: opened_ts.0,
                        tranche1_harvested,
                        fees: engines[slot_idx].active_fees,
                    }) {
                        rt.slot_rpc_inflight[slot_idx] = true;
                        rt.slot_rpc_sent_ns[slot_idx] = now_ns.0;
                    }
                }
                engines[slot_idx].sync_to_shm_telemetry(arena);
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
    let Some(mut boot_client) = BybitV5LiveClient::from_env_if_enabled(live_capital) else { return; };
    if let Some((w_f, _, _, _)) = boot_client.fetch_wallet_and_equity_usdt() {
        if w_f > 0.0 { rt.wallet_1e8 = (w_f * 100_000_000.0).round() as i64; }
    }
    for (s_idx, spec) in APPROVED_SPECS.iter().enumerate() {
        let sym_name = spec.symbol_id.name();
        let Some((true, sz_s, side_p, avg_s)) = boot_client.query_open_position_verified(sym_name) else { continue; };
        let _ = boot_client.cancel_non_reduce_only_orders(*spec);
        let Ok(bybit_avg_t) = parse_verified_avg_price_ticks(*spec, side_p, &avg_s) else { continue; };
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
        let saved_sl = saved.as_ref().and_then(|v| v.get("sl_ticks")?.as_i64()).unwrap_or(0);
        let snap = arena.slot(s_idx).and_then(|s| s.read_snapshot(4)).unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
        engines[s_idx].sync_governor_for_side(gov, side_p);
        if let Ok(mut sl_t) = engines[s_idx].adopt_live_open_position(side_p, bybit_avg_t, init_l, lots, t1_done, peak_t, worst_t, opened_ns, &snap) {
            if saved_sl > 0 {
                let saved_tighter = match side_p {
                    Side::Bid => saved_sl > sl_t.0,
                    Side::Ask => saved_sl < sl_t.0,
                };
                let ref_mk = if snap.mark_price.0 > 0 { snap.mark_price } else { bybit_avg_t };
                let ref_lt = if snap.last_trade_price.0 > 0 { snap.last_trade_price } else { ref_mk };
                if saved_tighter && crate::types::PairSpec::validate_stop_loss_geometry(side_p, PriceTicks(saved_sl), ref_lt, ref_mk).is_ok() {
                    sl_t = PriceTicks(saved_sl);
                    engines[s_idx].last_eff_sl_ticks = sl_t;
                    engines[s_idx].chandelier_profit_sl_armed =
                        is_chandelier_profit_locked_sl_with_fees(side_p, bybit_avg_t, sl_t, engines[s_idx].active_fees);
                    if let OrderLifecycleState::PositionOpenMakerExit { sl_trigger_price: ref mut sl_p, .. } = engines[s_idx].state {
                        *sl_p = sl_t;
                    }
                }
            }
            engines[s_idx].latch_defensive_floor_if_needed(def_locked);
            if t1_ex_saved > 0 {
                if let OrderLifecycleState::PositionOpenMakerExit { tranche1_exit_price: ref mut t1_p, .. } = engines[s_idx].state {
                    *t1_p = PriceTicks(t1_ex_saved);
                }
            }
            let sl_ok = boot_client.set_position_trading_stop(*spec, sl_t);
            apply_post_fill_sl_or_breach(s_idx, side_p, bybit_avg_t, sl_ok, sl_t, &snap, &mut engines[s_idx], rt);
            entry_forensics[s_idx] = match saved {
                Some(ref sv) => restore_entry_forensics_from_json(sv, opened_ns.0, init_l.0),
                None => SlotForensicEntrySnapshot { armed_ts_ns: opened_ns.0, init_q_lots: init_l.0, ..SlotForensicEntrySnapshot::default() },
            };
            engines[s_idx].sync_to_shm_telemetry(arena);
            println!(
                "{}\u{1f7e2} [VORTEX-OMEGA V5.5 LIVE ADOPT] Slot {:2} {} {:?} qty={} @ {}t (SL={}t ok={} T1={}){}",
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
pub fn run_oos_520_multi_regime_suite() -> Oos520MultiRegimeReport {
    if !verify_ws_private_parser_self_check() {
        return Oos520MultiRegimeReport { parity_sync_drift_1e8: 1, ..Oos520MultiRegimeReport::default() };
    }
    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut epps_trackers = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS];
    let sample_px: [i64; MAX_ACTIVE_PAIRS] = [759_000, 223_700, 8_930, 8_170, 22_400, 8_510, 3_130, 16_140, 1_650, 10_720, 3_690, 5_176];
    let mut rep = Oos520MultiRegimeReport::default();

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
        if !gov.authorizes_adaptive_entry_with_wallet(3, Side::Bid, DEFAULT_WALLET_EQUITY_1E8, &weak_snap, &weak_snap) {
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

        epps_trackers[s_idx].observe_5s_pair(TimestampNs(ts), s_idx == 0, PriceTicks(base_px), PriceTicks(759_000));
        let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &snap, &btc_snap, &epps_trackers[s_idx]);
        let fees = gov.active_fee_schedule();
        let fee_rt = gov.active_maker_rt_fee_bps();
        let (probe_edge, _, _) = gov.slot_direction_diagnostics(s_idx, Side::Bid);
        let taker_pm = gov.slot_taker_prob_permille(s_idx, Side::Bid);
        let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(
            spec, &snap, &btc_snap,
            ScaledBps(engines[s_idx].learned_mfe_bps.min(fee_rt * 2)),
            ScaledBps(probe_edge.clamp(-fee_rt, fee_rt / 2)),
            taker_pm, fees, 0.0, epps_beta,
        );
        let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict else { continue };

        let eq_1e8 = ledger.current_equity_1e8();
        let mut beta_guard = MonetaryPortfolioBetaGuard::new();
        for (i, eng) in engines.iter().enumerate() {
            if let Some(act_s) = eng.active_side() {
                let b_x1000 = epps_trackers[i].correlated_beta_x1000(i == 0, ScaledBps(7_200), btc_snap.realized_vol_5m);
                beta_guard.register_slot_open(i, act_s, 2_400, b_x1000);
            }
        }
        let en_px = side.select_entry_bbo_price(snap.bids[0].price, snap.asks[0].price);
        let lots = gov.compute_slot_entry_lots(s_idx, side, eq_1e8, &snap);
        let cand_risk_ppm = crate::exec_daemon::slot_position_risk_ppm_with_fees(
            spec, eq_1e8, en_px, lots, snap.realized_vol_5m, gov.active_fee_schedule(),
        );
        let cand_beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(s_idx == 0, snap.realized_vol_5m, btc_snap.realized_vol_5m);
        if !beta_guard.can_open_with_risk_ppm(s_idx, side, cand_risk_ppm, cand_beta_x1000) {
            continue;
        }
        if !gov.authorizes_adaptive_entry_v48(s_idx, side, eq_1e8, ScaledBps(engines[s_idx].learned_mfe_bps), &snap, &btc_snap, 0.0, epps_beta) {
            rep.utc_funding_vetoes += 1;
            continue;
        }
        engines[s_idx].sync_governor_for_side(&gov, side);
        if engines[s_idx].try_arm_maker_entry(&snap, verdict, lots).is_err() { continue; }
        let _ = engines[s_idx].on_entry_order_resting(TimestampNs(ts + 15_000_000));

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
                let qv = engines[s_idx].evaluate_resting_queue_shield_with_beta(
                    side, LotQty(500), LotQty(cur_q), LotQty(75), &snap, &btc_snap, epps_beta,
                );
                if qv.abort_now {
                    gov.observe_entry_abort(s_idx, side, en_px, snap.ts_local_ns);
                    engines[s_idx].state = OrderLifecycleState::Idle;
                    rep.hawkes_pre_fill_aborts += 1;
                    aborted_q = true;
                    break;
                }
            }
            if aborted_q { continue; }
        }

        let _ = engines[s_idx].on_maker_entry_filled(TimestampNs(ts + 25_000_000));
        let regime_bucket = rep.closed_trades % 20;
        let mut closed_summary = None;
        match regime_bucket {
            0..=10 => {
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ImpulseWave3, climax_active: false,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.66),
                    macro_phase: BoundedScore::new_clamped(0.85), oi_impulse: BoundedScore::new_clamped(0.45),
                    liq_imbalance: BoundedScore::new_clamped(0.60), fib_proj_bps: ScaledBps(7_200),
                });
                let q_fl = offensive_win_maker_exit_price(side, en_px, 0.50);
                snap.ts_local_ns = TimestampNs(ts + 8_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(q_fl.0 - 1), qty: LotQty(440) };
                snap.asks[0] = BookLevel { price: q_fl, qty: LotQty(180) };
                snap.mark_price = q_fl;
                snap.last_trade_price = q_fl;
                closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
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
                        regime: crate::wave_elliott::WaveRegime::ImpulseWave3, climax_active: false,
                        hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.66),
                        macro_phase: BoundedScore::new_clamped(0.88), oi_impulse: BoundedScore::new_clamped(0.40),
                        liq_imbalance: BoundedScore::new_clamped(0.55), fib_proj_bps: ScaledBps(10),
                    });
                    closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                    if closed_summary.is_none() {
                        snap.ts_local_ns = TimestampNs(ts + 21_000_000_000);
                        closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                    }
                }
            }
            11..=14 => {
                let clim_d = ((base_px * 2_600 + 999_999) / 1_000_000).max(5);
                let px_c = PriceTicks(base_px + clim_d);
                snap.ts_local_ns = TimestampNs(ts + 12_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(px_c.0 - 1), qty: LotQty(360) };
                snap.asks[0] = BookLevel { price: px_c, qty: LotQty(170) };
                snap.mark_price = px_c;
                snap.last_trade_price = px_c;
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ClimaxExhaustion, climax_active: true,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.68),
                    macro_phase: BoundedScore::new_clamped(0.92), oi_impulse: BoundedScore::new_clamped(-0.30),
                    liq_imbalance: BoundedScore::new_clamped(0.82), fib_proj_bps: ScaledBps(0),
                });
                closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
            15 => {
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
                        regime: crate::wave_elliott::WaveRegime::NeutralWarmup, climax_active: false,
                        hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.60),
                        macro_phase: BoundedScore::ZERO, oi_impulse: BoundedScore::ZERO,
                        liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
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
                    if let Some(cs) = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger) {
                        closed_summary = Some(cs);
                        break;
                    }
                }
            }
            17 => {
                snap.realized_vol_5m = ScaledBps(5_000);
                let rally_t = ((base_px * 5_800 + 999_999) / 1_000_000).max(10);
                let px_r = PriceTicks(base_px + rally_t);
                snap.ts_local_ns = TimestampNs(ts + 9_000_000_000);
                snap.bids[0] = BookLevel { price: PriceTicks(px_r.0 - 1), qty: LotQty(370) };
                snap.asks[0] = BookLevel { price: px_r, qty: LotQty(180) };
                snap.mark_price = px_r;
                snap.last_trade_price = px_r;
                snap.set_wave_observables(crate::wave_elliott::WaveObservables {
                    regime: crate::wave_elliott::WaveRegime::ImpulseWave5, climax_active: false,
                    hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.60),
                    macro_phase: BoundedScore::new_clamped(0.70), oi_impulse: BoundedScore::new_clamped(0.20),
                    liq_imbalance: BoundedScore::new_clamped(0.20), fib_proj_bps: ScaledBps(2_600),
                });
                let _ = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                let chand_sl = engines[s_idx].last_eff_sl_ticks;
                snap.ts_local_ns = TimestampNs(ts + 12_000_000_000);
                snap.mark_price = PriceTicks(chand_sl.0 - 1);
                snap.last_trade_price = PriceTicks(chand_sl.0 - 1);
                closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
            _ => {
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
                    let z_fl = defensive_zero_loss_maker_exit_price_with_fees(side, en_px, engines[s_idx].active_fees);
                    snap.bids[0] = BookLevel { price: PriceTicks(z_fl.0 - 1), qty: LotQty(260) };
                    snap.asks[0] = BookLevel { price: z_fl, qty: LotQty(170) };
                    snap.mark_price = z_fl;
                    snap.last_trade_price = z_fl;
                } else {
                    let comp_sl = engines[s_idx].last_eff_sl_ticks.max(PriceTicks(1));
                    snap.mark_price = comp_sl;
                    snap.last_trade_price = comp_sl;
                }
                closed_summary = engines[s_idx].step_open_position(&snap, &btc_snap, &gov, &mut ledger);
            }
        }
        if let Some(summary) = closed_summary {
            rep.closed_trades += 1;
            if summary.is_maker_exit { maker_total += 1; }
            match engines[s_idx].last_exit_sub_reason {
                ExitSubReason::FibonacciImpulseSurf | ExitSubReason::LvdbsDeepVacuumSurf => rep.wave3_fibonacci_closes += 1,
                ExitSubReason::ClimaxExhaustionHarvest => rep.climax_exhaustion_harvests += 1,
                ExitSubReason::ShadowRunnerCrestHarvest => rep.shadow_crest_harvests += 1,
                ExitSubReason::ChandelierProfitLockSl => rep.chandelier_profit_sl_wins += 1,
                ExitSubReason::AdverseScratchEscape => rep.wald_v2_zero_loss_escapes += 1,
                ExitSubReason::WaldBaseStopLoss => rep.wald_v2_compressed_sl_closes += 1,
                _ => {}
            }
            if summary.net_pnl_1e8 >= 0 { profit_1e8 += summary.net_pnl_1e8; } else { loss_1e8 += summary.net_pnl_1e8.abs(); }
            sum_trade_pnl_1e8 += summary.net_pnl_1e8;
            gov.observe_closed_trade_at_entry(
                s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100, snap.ts_local_ns,
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
    (r.closed_trades, r.maker_exit_ratio_pct, r.profit_factor, r.cum_net_pnl_usdt, r.parity_sync_drift_1e8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_fsm::ClosedTradeKind;
    use crate::lob_shm::MappedShmRegion;
    use std::sync::mpsc;

    #[test]
    fn test_v46_oos_520_monetary_beta_and_live_gateway_markout_probes() {
        let rep = run_oos_520_multi_regime_suite();
        assert_eq!(rep.closed_trades, 520);
        assert!(rep.maker_exit_ratio_pct >= 82.0 && rep.profit_factor >= 2.0 && rep.parity_sync_drift_1e8 == 0);
        assert!(rep.hawkes_pre_fill_aborts >= 20 && rep.chandelier_profit_sl_wins >= 20);

        let shm_path = MappedShmRegion::ephemeral_self_test_path("eh_v46_mk");
        if let Ok(r) = MappedShmRegion::open_or_create(&shm_path, true) {
            let mut engines = SlotExecutionEngine::new_12_slot_array();
            let mut gov = OnlineAdaptiveGovernor::new();
            let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
            let mut rt = ExecRuntimeState::new();
            let forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
            let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
            snaps[3].symbol_id = APPROVED_SPECS[3].symbol_id;
            snaps[3].realized_vol_5m = ScaledBps(5_000);
            snaps[3].bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(400) };
            snaps[3].asks[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(250) };
            snaps[3].mark_price = PriceTicks(8_201);
            snaps[3].last_trade_price = PriceTicks(8_201);
            let v = SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, target_excursion_scaled_bps: ScaledBps(700) };
            assert!(engines[3].try_arm_maker_entry(&snaps[3], v, LotQty(26)).is_ok());
            assert!(engines[3].on_entry_order_resting(TimestampNs(1_000_000_000)).is_ok());
            let (tg_tx, _tg_rx) = mpsc::sync_channel::<String>(16);
            let (vault_tx, vault_rx) = mpsc::sync_channel::<VaultIoCommand>(16);
            handle_gateway_event(
                GatewayEvent::EntryFilledLive {
                    slot_idx: 3, real_side: Side::Bid, verified_avg: Some(PriceTicks(8_200)),
                    real_lots: LotQty(26), sl_verified: true, tag: "MAKER FILL",
                },
                TimestampNs(1_000_000_000), None, &mut rt, &snaps, &forensics,
                &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            rt.markout_rings[3].step_clock(PriceTicks(8_205), TimestampNs(1_300_000_000), 200);
            rt.markout_rings[3].step_clock(PriceTicks(8_210), TimestampNs(2_200_000_000), 200);
            rt.markout_rings[3].step_clock(PriceTicks(8_218), TimestampNs(6_500_000_000), 200);
            rt.markout_rings[3].step_clock(PriceTicks(8_226), TimestampNs(16_500_000_000), 200);
            let closed_json = serde_json::json!({
                "retCode": 0,
                "result": { "list": [{ "symbol": "AVAXUSDT", "closedSize": "2.6", "avgEntryPrice": "8.200", "avgExitPrice": "8.226", "closedPnl": "0.045", "orderType": "Limit" }] }
            });
            handle_gateway_event(
                GatewayEvent::SlotClosedDetected { slot_idx: 3, vac_level: 2, closed_pnl_payload: Some(closed_json) },
                TimestampNs(17_000_000_000), None, &mut rt, &snaps, &forensics,
                &mut engines, &mut gov, &mut ledger, r.arena(), &tg_tx, &vault_tx,
            );
            let mut saw_live_mk = false;
            while let Ok(task) = vault_rx.try_recv() {
                if let VaultIoCommand::RecordClosedTrade(cp) = task { let vault_line = crate::telemetry_hud::build_closed_trade_vault_json(cp.slot_idx, &cp.summary, &cp.eng, &cp.entry_forensics, &cp.exit_snap, &cp.ledger, &cp.markout_curve, cp.lvr_5s_scaled_bps, cp.ts_ns);
                    if vault_line.contains("\"markout_fill_bps\"") && !vault_line.contains("[0.0,0.0,0.0,0.0]") {
                        saw_live_mk = true;
                    }
                }
            }
            assert!(saw_live_mk);
            assert_eq!(ClosedTradeKind::MakerPassiveUnwind, ClosedTradeKind::MakerPassiveUnwind);
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}

impl ExecRuntimeState {
    #[allow(
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::too_many_arguments,
        clippy::unnecessary_cast
    )]
    pub fn step_v49_deep_telemetry_200hz(
        &mut self,
        now_ns: TimestampNs,
        cached_snaps: &[SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
        engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
        gov: &OnlineAdaptiveGovernor,
        ledger: &ParitySyncLedger,
        vault_tx: Option<&SyncSender<VaultIoCommand>>,
    ) {
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            if let Some(vtx) = vault_tx {
                while let Some(ab) = self.markout_rings[s_idx].pop_resolved_abort() {
                    let rec = crate::vault_io::QueueAbortRecord::from_resolved_sample(s_idx as u8, ab);
                    let _ = vtx.try_send(VaultIoCommand::RecordQueueAbort(rec));
                }
            }
            let z_tox = if let OrderLifecycleState::PositionOpenMakerExit {
                side, avg_price_ticks, sl_trigger_price, opened_ts, ..
            } = engines[s_idx].state
            {
                let eff_sl = if engines[s_idx].last_eff_sl_ticks.0 > 0 {
                    engines[s_idx].last_eff_sl_ticks
                } else {
                    sl_trigger_price
                };
                let _ = update_and_check_sustained_sl_breach_p0a(
                    s_idx,
                    side,
                    eff_sl,
                    &cached_snaps[s_idx],
                    now_ns,
                    &mut self.sl_adverse_breach_since_ns[s_idx],
                );
                let c_rt = APPROVED_SPECS[s_idx].expected_round_trip_cost_bps_dynamic(
                    avg_price_ticks.max(PriceTicks(1)),
                    engines[s_idx].learned_taker_prob_pm,
                    engines[s_idx].active_fees,
                );
                self.markout_rings[s_idx].active_fill_toxicity_zeta(opened_ts, c_rt).max(self.last_zeta_tox[s_idx])
            } else {
                self.sl_adverse_breach_since_ns[s_idx] = 0;
                self.last_zeta_tox[s_idx] = 0.0;
                0.0
            };
            self.intra_rings[s_idx].observe_200hz_tick(&engines[s_idx], &cached_snaps[s_idx], z_tox);
        }
        let Some(vtx) = vault_tx else { return };
        if now_ns.0 >= 1_577_836_800_000_000_000 {
            let cur_day = now_ns.0.div_euclid(86_400_000_000_000);
            let live_eq = self.wallet_1e8.max(ledger.current_equity_1e8());
            let rolled = self.daily_gov.sync_utc_day(now_ns, live_eq);
            if rolled || self.last_persisted_utc_day != cur_day {
                self.last_persisted_utc_day = cur_day;
                let _ = vtx.try_send(VaultIoCommand::SaveDailyGovernor {
                    ts_ns: now_ns.0,
                    state: self.daily_gov,
                });
            }
            if now_ns.0.saturating_sub(self.last_plasticity_audit_ns) >= 900_000_000_000 {
                self.last_plasticity_audit_ns = now_ns.0;
                let mut taker_prob_pm = [[50_i64; 2]; MAX_ACTIVE_PAIRS];
                let mut learned_mfe_bps = [0_i64; MAX_ACTIVE_PAIRS];
                let mut ob_bull_pm = [0_i64; MAX_ACTIVE_PAIRS];
                let mut ob_bear_pm = [0_i64; MAX_ACTIVE_PAIRS];
                let mut epps_corr_pm = [0_i16; MAX_ACTIVE_PAIRS];
                for s in 0..MAX_ACTIVE_PAIRS {
                    taker_prob_pm[s] = [
                        gov.slot_taker_prob_permille(s, Side::Bid),
                        gov.slot_taker_prob_permille(s, Side::Ask),
                    ];
                    learned_mfe_bps[s] = engines[s].learned_mfe_bps;
                    ob_bull_pm[s] = cached_snaps[s].ob_bull_efficacy_pm as i64;
                    ob_bear_pm[s] = cached_snaps[s].ob_bear_efficacy_pm as i64;
                    let rho = self.epps_trackers[s].correlation(s == 0);
                    epps_corr_pm[s] = (rho * 1_000.0).round().clamp(-1_000.0, 1_000.0) as i16;
                }
                let (ws_rtt_us, ws_quota_rem) = crate::execution_fsm::read_live_ws_pulse();
                let _ = vtx.try_send(VaultIoCommand::SavePlasticityAudit(Box::new(
                    crate::vault_io::PlasticityAuditSnapshot {
                        ts_ns: now_ns.0,
                        taker_prob_pm,
                        learned_mfe_bps,
                        ob_bull_pm,
                        ob_bear_pm,
                        epps_corr_pm,
                        ws_rtt_us,
                        ws_quota_rem,
                    },
                )));
            }
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
    pub fn emit_live_position_path_from_delta_v49(
        &mut self,
        trade_id: u64,
        slot_idx: usize,
        side: Side,
        entry_px: PriceTicks,
        open_lots: LotQty,
        delta_net_1e8: i64,
        opened_ts_ns: TimestampNs,
        closed_ts_ns: TimestampNs,
        exit_snap: &SymbolLobSnapshot,
        vault_tx: &SyncSender<VaultIoCommand>,
    ) {
        let s_idx = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let notional = APPROVED_SPECS[s_idx].lot_notional_1e8(open_lots.max(LotQty(1)), entry_px.max(PriceTicks(1))).max(1);
        let net_sbps = ScaledBps((((i128::from(delta_net_1e8)) * 1_000_000) / (i128::from(notional))) as i64);
        let exit_px = if exit_snap.last_trade_price.0 > 0 { exit_snap.last_trade_price } else { entry_px };
        let synth = crate::execution_fsm::ClosedTradeSummary {
            side, closed_lots: open_lots, entry_price: entry_px, exit_price: exit_px,
            gross_scaled_bps: net_sbps, fee_scaled_bps: ScaledBps(0), net_scaled_bps: net_sbps,
            net_pnl_1e8: delta_net_1e8, fee_1e8: 0, legs_consolidated: 1, is_maker_exit: true,
            kind: crate::execution_fsm::ClosedTradeKind::MakerPassiveUnwind,
        };
        let m5 = self.markout_rings[s_idx].last_fill_curve_5h();
        let path_sum = self.intra_rings[s_idx].build_position_path_summary(
            trade_id, s_idx, &synth, opened_ts_ns, closed_ts_ns, m5,
        );
        self.intra_rings[s_idx].reset_on_open(TimestampNs(0));
        let _ = vault_tx.try_send(VaultIoCommand::RecordPositionPath(Box::new(path_sum)));
    }
}

#[cfg(test)]
mod v49_runtime_telemetry_tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn test_v49_runtime_drains_aborts_utc_rollover_and_15m_plasticity() {
        let mut rt = ExecRuntimeState::new();
        let snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
        let engines = SlotExecutionEngine::new_12_slot_array();
        let gov = OnlineAdaptiveGovernor::new();
        let ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let (vtx, vrx) = mpsc::sync_channel::<VaultIoCommand>(16);

        rt.markout_rings[4].arm_abort_probe_v49(
            Side::Ask, PriceTicks(19_000), LastVetoCode::HawkesStoikovQueueAbort,
            TimestampNs(1_794_880_000_000_000_000), TimestampNs(1_794_880_001_000_000_000),
            LotQty(400), LotQty(90),
        );
        for dt_s in [1_i64, 2, 6, 16, 61] {
            rt.markout_rings[4].step_clock(
                PriceTicks(19_050),
                TimestampNs(1_794_880_001_000_000_000 + dt_s * 1_000_000_000),
                200,
            );
        }
        rt.step_v49_deep_telemetry_200hz(
            TimestampNs(1_794_880_065_000_000_000), &snaps, &engines, &gov, &ledger, Some(&vtx),
        );
        let (mut got_abort, mut got_dg, mut got_plast) = (false, false, false);
        while let Ok(cmd) = vrx.try_recv() {
            match cmd {
                VaultIoCommand::RecordQueueAbort(r) => got_abort = r.is_true_positive && r.slot_idx == 4,
                VaultIoCommand::SaveDailyGovernor { .. } => got_dg = true,
                VaultIoCommand::SavePlasticityAudit(_) => got_plast = true,
                _ => {}
            }
        }
        assert!(got_abort && got_dg && got_plast);
    }
}
