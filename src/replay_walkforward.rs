//! Continuous 1:1 Walk-Forward PAR1 Replay Engine & Replay CLI Certification Gate.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::execution_fsm::{
    check_queue_depletion_velocity_abort, ClosedTradeKind, ExitOrderRole, OnlineAdaptiveGovernor,
    OrderLifecycleState, SlotExecutionEngine,
};
use crate::fifo_sim::{
    run_200k_fifo_benchmark, run_ab_economic_ablation_model, AblationMetrics, AblationMode,
    FifoQueueSimulator, SimulatedOrderState, INITIAL_REPLAY_EQUITY_1E8,
};
use crate::forensic_parquet::{ColumnarParquetBatch, MarketEventKind, ParitySyncLedger};
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{
    classify_pnl_badge, print_auto_aligned_box, LotQty, PortfolioBetaGuard, PriceTicks, ScaledBps,
    Side, ANSI_BLUE, ANSI_GREEN, ANSI_ORANGE, ANSI_RED, ANSI_RESET, ANSI_YELLOW, APPROVED_SPECS,
};

const SCAR_MEMORY_VAULT_PATH: &str = "data/vault/vortex_governor_scar_memory.jsonl";
const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";

#[derive(Copy, Clone, Debug, Default)]
pub struct WalkForwardReport {
    pub files_count: usize,
    pub total_events: u64,
    pub l2_events: u64,
    pub trade_events: u64,
    pub spoof_vetoes: u64,
    pub armed: u64,
    pub queue_aborts: u64,
    pub post_only_rejects: u64,
    pub fills: u64,
    pub closed: u64,
    pub maker_closes: u64,
    pub wins: u64,
    pub scratches: u64,
    pub losses: u64,
    pub cum_net_bps_x100: i64,
    pub gross_profit_1e8: i64,
    pub gross_loss_1e8: i64,
    pub cum_net_pnl_1e8: i64,
}

#[must_use]
pub fn run_parquet_walk_forward_on_dir(data_dir: &Path, verbose: bool) -> WalkForwardReport {
    let mut rep = WalkForwardReport::default();
    let Ok(entries) = fs::read_dir(data_dir) else {
        return rep;
    };
    let mut files: Vec<_> = entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|ext| ext.eq_ignore_ascii_case("parquet")))
        .map(|e| e.path())
        .collect();
    files.sort();
    if files.is_empty() {
        return rep;
    }
    rep.files_count = files.len();

    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut wave_states = [crate::wave_elliott::CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS];
    if verbose {
        let _ = gov.rehydrate_from_scar_and_loss_files(
            Path::new(SCAR_MEMORY_VAULT_PATH),
            Path::new(RED_LOSS_BOX_PATH),
            Some(Path::new("data/vault/vortex_ai_forensic_vault.jsonl")),
        );
    }
    let mut ledger = ParitySyncLedger::new(INITIAL_REPLAY_EQUITY_1E8);
    let mut entry_fifo = [FifoQueueSimulator::new(); MAX_ACTIVE_PAIRS];
    let mut exit_fifo = [FifoQueueSimulator::new(); MAX_ACTIVE_PAIRS];
    let mut exit_role_latch = [ExitOrderRole::None; MAX_ACTIVE_PAIRS];
    let mut init_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut armed_ts_latch = [0_i64; MAX_ACTIVE_PAIRS];
    let mut first_ring_ts_ns = 0_i64;
    let require_ring_warmup = files.len() >= 64;

    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let init_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4);
    for s in 0..MAX_ACTIVE_PAIRS {
        snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
        snaps[s].is_eligible = true;
        snaps[s].realized_vol_5m = init_vol;
    }

    for f_path in &files {
        let Ok((batch, _)) = ColumnarParquetBatch::read_from_file(f_path) else { continue };
        for i in 0..batch.len() {
            let Some(row) = batch.get_row(i) else { continue };
            let Some(s_idx) = row.symbol_id.slot_index() else { continue };
            rep.total_events += 1;
            row.apply_to_lob_snapshot(&mut snaps[s_idx]);
            if snaps[s_idx].bids[0].price.0 > 0 && snaps[s_idx].asks[0].price.0 > 0 {
                let mid_w = PriceTicks(i64::midpoint(
                    snaps[s_idx].bids[0].price.0, snaps[s_idx].asks[0].price.0,
                ));
                let w_obs = wave_states[s_idx].observe_price_tick(
                    APPROVED_SPECS[s_idx], mid_w, snaps[s_idx].ts_local_ns.0, snaps[s_idx].realized_vol_5m,
                );
                snaps[s_idx].set_wave_observables(w_obs);
            }
            if first_ring_ts_ns == 0 && snaps[s_idx].ts_local_ns.0 > 0 {
                first_ring_ts_ns = snaps[s_idx].ts_local_ns.0;
            }
            gov.observe_market_price(s_idx, &snaps[s_idx]);
            match row.event_kind {
                MarketEventKind::L2BookSnapshot => {
                    rep.l2_events += 1;
                    entry_fifo[s_idx].on_lob_snapshot(&snaps[s_idx]);
                    exit_fifo[s_idx].on_lob_snapshot(&snaps[s_idx]);
                }
                MarketEventKind::PublicTradePrint => {
                    rep.trade_events += 1;
                    entry_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                    exit_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                }
            }
            let cur_snap = snaps[s_idx];
            let btc_snap = snaps[0];
            let spec = APPROVED_SPECS[s_idx];

            match engines[s_idx].state {
                OrderLifecycleState::Idle => {
                    let mut beta_guard = PortfolioBetaGuard::new();
                    for eng in &engines {
                        if let Some(active_s) = eng.active_side() {
                            beta_guard.observe_active_side(active_s);
                        }
                    }
                    let comp_raw = OrthogonalFactorState::snr_composite_score(&cur_snap);
                    let probe_side = if comp_raw >= 0.0 { Side::Bid } else { Side::Ask };
                    let (dir_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_elastic(
                        spec, &cur_snap, &btc_snap,
                        ScaledBps(engines[s_idx].learned_mfe_bps), ScaledBps(dir_edge),
                    );
                    match verdict {
                        SignalVerdict::SpoofingVeto => rep.spoof_vetoes += 1,
                        SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                            let eq_1e8 = ledger.current_equity_1e8();
                            if beta_guard.can_admit_side(side)
                                && gov.authorizes_adaptive_entry_with_wallet(
                                    s_idx, side, eq_1e8, &cur_snap, &btc_snap,
                                )
                            {
                                let warm_dt = cur_snap.ts_local_ns.0.saturating_sub(first_ring_ts_ns);
                                if require_ring_warmup && (first_ring_ts_ns <= 0 || warm_dt < 300_000_000_000) {
                                    continue;
                                }
                                let lots = gov.compute_slot_entry_lots(s_idx, side, eq_1e8, &cur_snap);
                                engines[s_idx].sync_governor_for_side(&gov, side);
                                if engines[s_idx].try_arm_maker_entry(&cur_snap, verdict, lots).is_ok()
                                    && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
                                {
                                    armed_ts_latch[s_idx] = cur_snap.ts_local_ns.0;
                                    entry_fifo[s_idx].reset();
                                }
                            }
                        }
                        SignalVerdict::NeutralNoAction => {}
                    }
                }
                OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                    match entry_fifo[s_idx].state {
                        SimulatedOrderState::Inactive => {
                            let wait_ns = engines[s_idx].elastic_preflight_window_ns(cur_snap.realized_vol_5m);
                            if cur_snap.ts_local_ns.0.saturating_sub(submitted_ts.0) < wait_ns {
                                continue;
                            }
                            if gov
                                .confirm_preflight_kinematics_non_choking(
                                    s_idx, side, ledger.current_equity_1e8(), &cur_snap, &btc_snap,
                                )
                                .is_err()
                            {
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                continue;
                            }
                            let Ok((synced_px, _)) = engines[s_idx].sync_preflight_l1_entry(&cur_snap, 8) else {
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                continue;
                            };
                            armed_ts_latch[s_idx] = cur_snap.ts_local_ns.0;
                            entry_fifo[s_idx].submit_post_only(side, synced_px, qty, cur_snap.ts_local_ns);
                            rep.armed += 1;
                        }
                        SimulatedOrderState::InFlightToExchange { .. } => {}
                        SimulatedOrderState::RejectedPostOnlyCross => {
                            rep.post_only_rejects += 1;
                            entry_fifo[s_idx].reset();
                            engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, .. } => {
                            init_q_ahead[s_idx] = queue_ahead_qty;
                            let _ = engines[s_idx].on_entry_order_resting(cur_snap.ts_local_ns);
                        }
                        SimulatedOrderState::FilledMaker { side: f_side, fill_price, filled_qty, fill_ts_ns } => {
                            gov.clear_abort_streak(s_idx, f_side);
                            let _ = engines[s_idx].on_maker_entry_filled_verified(
                                fill_ts_ns, Some(f_side), Some(fill_price), Some(filled_qty),
                            );
                            entry_fifo[s_idx].reset();
                            exit_fifo[s_idx].reset();
                            exit_role_latch[s_idx] = ExitOrderRole::None;
                            rep.fills += 1;
                        }
                    }
                }
                OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, resting_since_ts, .. } => {
                    match entry_fifo[s_idx].state {
                        SimulatedOrderState::RejectedPostOnlyCross => {
                            rep.post_only_rejects += 1;
                            entry_fifo[s_idx].reset();
                            engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                        SimulatedOrderState::FilledMaker { side: f_side, fill_price, filled_qty, fill_ts_ns } => {
                            gov.clear_abort_streak(s_idx, f_side);
                            let _ = engines[s_idx].on_maker_entry_filled_verified(
                                fill_ts_ns, Some(f_side), Some(fill_price), Some(filled_qty),
                            );
                            entry_fifo[s_idx].reset();
                            exit_fifo[s_idx].reset();
                            exit_role_latch[s_idx] = ExitOrderRole::None;
                            rep.fills += 1;
                        }
                        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, .. } => {
                            let elapsed_ns = cur_snap.ts_local_ns.0.saturating_sub(resting_since_ts.0);
                            let max_chase_ns = gov.max_total_entry_chase_ns(s_idx, cur_snap.realized_vol_5m);
                            let timeout_ns = (max_chase_ns * 3) / 4;
                            let total_chase_ns = cur_snap.ts_local_ns.0.saturating_sub(armed_ts_latch[s_idx]);
                            let at_l1 = match side {
                                Side::Bid => cur_snap.bids[0].price == entry_price,
                                Side::Ask => cur_snap.asks[0].price == entry_price,
                            };
                            let flow_imp = (0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get())
                                * side.sign_f64();
                            let btc_q_snr = OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64();
                            let q_eroded = init_q_ahead[s_idx].0 > 0
                                && queue_ahead_qty.0 * 100 <= init_q_ahead[s_idx].0 * 65;
                            let headwind_erosion = s_idx > 0
                                && elapsed_ns >= 2_200_000_000
                                && q_eroded
                                && (btc_q_snr < -0.02 || flow_imp < 0.38);
                            let dq_abort = headwind_erosion
                                || (elapsed_ns >= 25_000_000
                                    && at_l1
                                    && check_queue_depletion_velocity_abort(
                                        side, init_q_ahead[s_idx], queue_ahead_qty, &cur_snap,
                                    ))
                                || flow_imp <= -0.42;

                            if elapsed_ns >= timeout_ns || total_chase_ns >= max_chase_ns || dq_abort {
                                gov.observe_entry_abort(s_idx, side, entry_price, cur_snap.ts_local_ns);
                                entry_fifo[s_idx].reset();
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                rep.queue_aborts += 1;
                            } else if !at_l1
                                && elapsed_ns >= 2_500_000_000
                                && total_chase_ns + 2_500_000_000 < max_chase_ns
                                && cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                            {
                                let new_l1 = side.select_entry_bbo_price(cur_snap.bids[0].price, cur_snap.asks[0].price);
                                let moved_away = match side {
                                    Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= 15,
                                    Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= 15,
                                };
                                if moved_away
                                    && gov
                                        .confirm_preflight_kinematics_non_choking(
                                            s_idx, side, ledger.current_equity_1e8(), &cur_snap, &btc_snap,
                                        )
                                        .is_ok()
                                {
                                    if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&cur_snap, new_l1) {
                                        engines[s_idx].next_peg_allowed_ts_ns = cur_snap.ts_local_ns.0 + 1_500_000_000;
                                        engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, cur_snap.ts_local_ns);
                                        entry_fifo[s_idx].submit_post_only(side, new_l1, qty, cur_snap.ts_local_ns);
                                    }
                                }
                            }
                        }
                        SimulatedOrderState::InFlightToExchange { .. }
                        | SimulatedOrderState::Inactive => {}
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price,
                    sl_trigger_price, ..
                } => {
                    engines[s_idx].sync_governor_dynamics(&gov);
                    let ref_px = if cur_snap.last_trade_price.0 > 0 {
                        cur_snap.last_trade_price
                    } else {
                        match side {
                            Side::Bid => cur_snap.asks[0].price,
                            Side::Ask => cur_snap.bids[0].price,
                        }
                    };
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
                    let avg_d = avg_price_ticks.0.max(1);
                    engines[s_idx].last_closed_mfe_bps =
                        ((peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_d;
                    engines[s_idx].last_closed_mae_bps =
                        ((worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_d;

                    let sl_hit = match side {
                        Side::Bid => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 <= sl_trigger_price.0,
                        Side::Ask => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 >= sl_trigger_price.0,
                    };
                    let mut closed_opt = None;
                    if sl_hit {
                        let sl_slip_px = match side {
                            Side::Bid => PriceTicks((sl_trigger_price.0 - 1).max(1)),
                            Side::Ask => PriceTicks(sl_trigger_price.0.saturating_add(1)),
                        };
                        exit_fifo[s_idx].reset();
                        closed_opt = engines[s_idx].finalize_open_position_close(
                            ClosedTradeKind::ServerMarkPriceStopLoss, sl_slip_px, cur_snap.ts_local_ns, &mut ledger,
                        );
                    } else if let SimulatedOrderState::FilledMaker { fill_price, fill_ts_ns, .. } = exit_fifo[s_idx].state {
                        exit_fifo[s_idx].reset();
                        if matches!(exit_role_latch[s_idx], ExitOrderRole::Tranche1Harvest50) {
                            let _ = engines[s_idx].record_tranche1_harvest(fill_price);
                            engines[s_idx].next_peg_allowed_ts_ns = 0;
                        } else {
                            closed_opt = engines[s_idx].finalize_open_position_close(
                                ClosedTradeKind::MakerPassiveUnwind, fill_price, fill_ts_ns, &mut ledger,
                            );
                        }
                    }
                    if let Some(summary) = closed_opt {
                        rep.closed += 1;
                        if summary.is_maker_exit { rep.maker_closes += 1; }
                        let net_r = summary.net_scaled_bps.raw();
                        if net_r < -50 { rep.losses += 1; } else if net_r >= 100 { rep.wins += 1; } else { rep.scratches += 1; }
                        if summary.net_pnl_1e8 >= 0 {
                            rep.gross_profit_1e8 = rep.gross_profit_1e8.saturating_add(summary.net_pnl_1e8);
                        } else {
                            rep.gross_loss_1e8 = rep.gross_loss_1e8.saturating_add(summary.net_pnl_1e8.abs());
                        }
                        rep.cum_net_bps_x100 = rep.cum_net_bps_x100.saturating_add(net_r);
                        gov.observe_closed_trade_at_entry(
                            s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                            summary.is_maker_exit, cur_snap.ts_local_ns,
                        );
                        if verbose {
                            let net_f = (net_r as f64) / 100.0;
                            let (col, icon, lbl) = classify_pnl_badge(net_f);
                            println!(
                                "  {}{} [REAL PAR1 TRADE #{} {}] {:<10} {:?} | {}t->{}t ({}L) | Net: {:+.2} bps (Maker={}){}",
                                col, icon, rep.closed, lbl, spec.symbol_id.name(), summary.side,
                                summary.entry_price.0, summary.exit_price.0, summary.legs_consolidated,
                                net_f, summary.is_maker_exit, ANSI_RESET
                            );
                        }
                        continue;
                    }
                    if matches!(exit_fifo[s_idx].state, SimulatedOrderState::RejectedPostOnlyCross) {
                        rep.post_only_rejects += 1;
                        exit_fifo[s_idx].reset();
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&cur_snap) {
                        engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                        let (rest_tp, rest_q) = exit_fifo[s_idx].resting_price_and_qty();
                        let tp_diff = (quote.target_price.0 - rest_tp.0).abs();
                        let moving_closer = match side {
                            Side::Bid => rest_tp.0 == 0 || quote.target_price.0 < rest_tp.0,
                            Side::Ask => rest_tp.0 == 0 || quote.target_price.0 > rest_tp.0,
                        };
                        let peg_ready = cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns;
                        let should_peg = rest_tp.0 == 0
                            || rest_q != quote.order_lots
                            || (peg_ready
                                && ((moving_closer && tp_diff >= 1 && (quote.urgent_close_repeg || tp_diff >= 2))
                                    || (!moving_closer && quote.cur_gross_bps < 200 && tp_diff >= 4)));
                        if should_peg {
                            engines[s_idx].next_peg_allowed_ts_ns = cur_snap.ts_local_ns.0 + 1_350_000_000;
                            exit_role_latch[s_idx] = quote.role;
                            exit_fifo[s_idx].submit_post_only(
                                side.opposite(), quote.target_price, quote.order_lots, cur_snap.ts_local_ns,
                            );
                            if let OrderLifecycleState::PositionOpenMakerExit { resting_exit_price: ref mut rx_p, .. } = engines[s_idx].state {
                                *rx_p = quote.target_price;
                            }
                        }
                    }
                }
            }
        }
    }
    rep.cum_net_pnl_1e8 = ledger.cumulative_net_pnl_1e8;
    if verbose {
        print_walk_forward_summary(&rep);
    }
    rep
}

pub fn print_walk_forward_summary(rep: &WalkForwardReport) {
    let mkr_pct = if rep.closed > 0 { (rep.maker_closes as f64 * 100.0) / (rep.closed as f64) } else { 0.0 };
    let pf_str = if rep.closed == 0 {
        "N/A (0 closed)".to_string()
    } else if rep.gross_loss_1e8 == 0 {
        "INF (0 losses)".to_string()
    } else {
        format!("{:.2}", (rep.gross_profit_1e8 as f64) / (rep.gross_loss_1e8 as f64))
    };
    let pnl_usdt = (rep.cum_net_pnl_1e8 as f64) / 100_000_000.0;
    println!(
        "  -> Live PAR1 Archives Verified: {} files | {} total events ({} L2 / {} Trades)",
        rep.files_count, rep.total_events, rep.l2_events, rep.trade_events
    );
    println!(
        "  -> Live Walk-Forward Replay   : {}SpoofVetoes={} | {}Armed={} | Aborts={} | Rej={} | Fills={} | {}Closed={} (W:{}/S:{}/L:{}){}",
        ANSI_ORANGE, rep.spoof_vetoes, ANSI_YELLOW, rep.armed, rep.queue_aborts, rep.post_only_rejects,
        rep.fills, ANSI_GREEN, rep.closed, rep.wins, rep.scratches, rep.losses, ANSI_RESET
    );
    let (wf_col, wf_ico) = if rep.cum_net_bps_x100 >= 0 && rep.losses == 0 {
        (ANSI_GREEN, "\u{1f7e2}")
    } else {
        (ANSI_RED, "\u{1f534}")
    };
    println!(
        "  {}{} -> Live Walk-Forward PnL      : Net={:+.2} bps ({:+.4} USDT) | MakerExit={:.2}% | PF={}{}",
        wf_col, wf_ico, (rep.cum_net_bps_x100 as f64) / 100.0, pnl_usdt, mkr_pct, pf_str, ANSI_RESET
    );
}

fn fmt_ablation_row(label: &str, m: &AblationMetrics, fee_drag_pct: f64) -> String {
    format!(
        "  \u{2022} {:<37}: Net: {:+6.2} bps | FeeDrag: {:5.1}% | Hold: {:4}s | W/S/L: {:2}/{:2}/{} | T1:{:2} Def:{:2} LVDBS:{:2}",
        label, (m.cum_net_bps_x100 as f64) / 100.0, fee_drag_pct, m.mean_hold_secs,
        m.wins, m.scratches, m.losses, m.tranche1_harvests, m.defensive_latches, m.lvdbs_deep_surfs
    )
}

#[must_use]
pub fn run_replay_cli_gate() -> ExitCode {
    let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
    let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);
    println!("{}", sep);
    println!("[VORTEX-REPLAY V3.0] DETERMINISTIC L1..L4 FIFO & A/B/A+B ECONOMIC SIMULATOR");
    println!("{}", sep);
    println!("   -> Simulated Events Processed : 200000 events in {:.3} ms", (elapsed_us as f64) / 1_000.0);
    println!("   -> Deterministic Throughput   : {} events/sec (Gate >= 100,000 ev/s)", ev_per_sec);
    println!("   -> Verified FIFO Maker Fills  : {} | PostOnly Rejects: {}", fills, rejects);
    println!("{}", dash);
    let mod_a = run_ab_economic_ablation_model(AblationMode::BaselineFixedL1A);
    let mod_b = run_ab_economic_ablation_model(AblationMode::ElasticMultiTrancheB);
    let mod_ab = run_ab_economic_ablation_model(AblationMode::FullVortexL3AB);
    let fd_a = (mod_a.cum_fee_bps_x100 as f64 * 100.0) / (mod_a.cum_gross_bps_x100.max(1) as f64);
    let fd_b = (mod_b.cum_fee_bps_x100 as f64 * 100.0) / (mod_b.cum_gross_bps_x100.max(1) as f64);
    let fd_ab = (mod_ab.cum_fee_bps_x100 as f64 * 100.0) / (mod_ab.cum_gross_bps_x100.max(1) as f64);
    let sec1 = vec![format!(
        "{}\u{1f52c} [VORTEX-L3 ECONOMIC ABLATION LAB - A / B / A+B OVER 24 MULTI-SLOT EPISODES]{}",
        ANSI_BLUE, ANSI_RESET
    )];
    let row_ab = fmt_ablation_row("MODEL A+B (VORTEX-L3 T1+Def+LVDBS)", &mod_ab, fd_ab);
    let sec2 = vec![
        fmt_ablation_row("MODEL A   (Baseline Fixed L1 Exit)", &mod_a, fd_a),
        fmt_ablation_row("MODEL B   (A + T1 50% + Elastic Def)", &mod_b, fd_b),
        format!("{}{}{}", ANSI_GREEN, row_ab, ANSI_RESET),
    ];
    print_auto_aligned_box(&[sec1, sec2]);
    let _ = run_parquet_walk_forward_on_dir(Path::new("data"), true);
    println!("{}", sep);
    let ablation_invalid = mod_ab.closed_trades < 24 || mod_a.losses > 0 || mod_b.losses > 0 || mod_ab.losses > 0;
    if ev_per_sec < 100_000 || fills < 1_000 || rejects > 0 || ablation_invalid {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forensic_parquet::ColumnarMarketRow;
    use crate::lob_shm::BookLevel;
    use crate::types::{BoundedScore, TimestampNs};

    #[test]
    fn test_f10_walk_forward_par1_1to1_preflight_fifo_25ms_and_zero_60s_wipe() {
        let tmp_dir = std::env::temp_dir().join(format!("vortex_l3_wf_23a_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp_dir);
        assert!(fs::create_dir_all(&tmp_dir).is_ok());

        let mut btc = SymbolLobSnapshot::new_empty();
        btc.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc.sequence_id = 1;
        btc.ts_exchange_ns = TimestampNs(1_000_000_000);
        btc.ts_local_ns = TimestampNs(1_000_000_000);
        btc.mark_price = PriceTicks(759_000);
        btc.realized_vol_5m = ScaledBps(6_500);
        btc.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(360) };
        btc.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(260) };
        btc.factor_ofi = BoundedScore::new_clamped(0.62);
        btc.factor_tfi = BoundedScore::new_clamped(0.68);
        btc.factor_geom = BoundedScore::new_clamped(0.35);

        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.sequence_id = 1;
        avax.ts_exchange_ns = TimestampNs(2_000_000_000);
        avax.ts_local_ns = TimestampNs(2_000_000_000);
        avax.mark_price = PriceTicks(8_200);
        avax.realized_vol_5m = ScaledBps(6_500);
        for lvl in 0..4 {
            avax.bids[lvl] = BookLevel { price: PriceTicks(8_200 - lvl as i64), qty: LotQty(360) };
            avax.asks[lvl] = BookLevel { price: PriceTicks(8_201 + lvl as i64), qty: LotQty(260) };
        }
        avax.factor_ofi = BoundedScore::new_clamped(0.58);
        avax.factor_tfi = BoundedScore::new_clamped(0.65);
        avax.factor_geom = BoundedScore::new_clamped(0.32);

        let mut seg1 = ColumnarParquetBatch::new();
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&btc, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(759_000), LotQty(360))).is_ok());
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_200), LotQty(360))).is_ok());
        avax.sequence_id = 2; avax.ts_local_ns = TimestampNs(2_035_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_200), LotQty(360))).is_ok());
        avax.sequence_id = 3; avax.ts_local_ns = TimestampNs(2_045_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::PublicTradePrint, Side::Ask, PriceTicks(8_200), LotQty(500))).is_ok());
        avax.sequence_id = 4; avax.ts_local_ns = TimestampNs(2_065_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_200), LotQty(360))).is_ok());
        avax.sequence_id = 5; avax.ts_local_ns = TimestampNs(2_075_000_000); avax.bids[0].qty = LotQty(200);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_200), LotQty(200))).is_ok());
        avax.sequence_id = 6; avax.ts_local_ns = TimestampNs(2_095_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::PublicTradePrint, Side::Ask, PriceTicks(8_200), LotQty(350))).is_ok());
        avax.sequence_id = 7; avax.ts_local_ns = TimestampNs(25_000_000_000); avax.mark_price = PriceTicks(8_205);
        avax.bids[0] = BookLevel { price: PriceTicks(8_203), qty: LotQty(420) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(100) };
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_205), LotQty(420))).is_ok());
        avax.sequence_id = 8; avax.ts_local_ns = TimestampNs(25_030_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_205), LotQty(420))).is_ok());
        avax.sequence_id = 9; avax.ts_local_ns = TimestampNs(25_050_000_000);
        assert!(seg1.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::PublicTradePrint, Side::Bid, PriceTicks(8_205), LotQty(140))).is_ok());
        assert!(seg1.write_to_file(&tmp_dir.join("vortex_l2_segment_000001.parquet")).is_ok());

        let mut seg2 = ColumnarParquetBatch::new();
        avax.sequence_id = 10; avax.ts_local_ns = TimestampNs(90_500_000_000); avax.mark_price = PriceTicks(8_201);
        for (idx, &ap) in [8_201_i64, 8_208, 8_216, 8_224].iter().enumerate() {
            avax.bids[idx] = BookLevel { price: PriceTicks(8_200 - idx as i64), qty: LotQty(420) };
            avax.asks[idx] = BookLevel { price: PriceTicks(ap), qty: LotQty(80) };
        }
        avax.factor_ofi = BoundedScore::new_clamped(0.70);
        avax.factor_tfi = BoundedScore::new_clamped(0.82);
        assert!(seg2.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_201), LotQty(420))).is_ok());
        avax.sequence_id = 11; avax.ts_local_ns = TimestampNs(90_530_000_000);
        assert!(seg2.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(8_201), LotQty(420))).is_ok());
        avax.sequence_id = 12; avax.ts_local_ns = TimestampNs(90_550_000_000);
        assert!(seg2.push_row(&ColumnarMarketRow::from_lob_snapshot(&avax, MarketEventKind::PublicTradePrint, Side::Bid, PriceTicks(8_224), LotQty(110))).is_ok());
        assert!(seg2.write_to_file(&tmp_dir.join("vortex_l2_segment_000002.parquet")).is_ok());

        let rep = run_parquet_walk_forward_on_dir(&tmp_dir, false);
        let _ = fs::remove_dir_all(&tmp_dir);
        assert_eq!(rep.closed, 1);
        assert_eq!(rep.wins, 1);
        assert_eq!(rep.losses, 0);
        assert!(rep.cum_net_bps_x100 >= 1_200);
    }
}
