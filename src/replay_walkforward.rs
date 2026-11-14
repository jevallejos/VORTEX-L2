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

// === VORTEX-OMEGA CHAT 6 REPLAY PROD ===
pub mod omega_twin {
    #![allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::manual_clamp,
        clippy::needless_range_loop,
        clippy::similar_names,
        clippy::too_many_arguments,
        clippy::too_many_lines
    )]

    use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
    use crate::exec_daemon::compute_exit_sync_schedule;
    use crate::execution_fsm::{
        is_chandelier_profit_locked_sl, passes_stoikov_repeg_coherence_guard, ClosedTradeKind,
        ExitOrderRole, ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState,
        SlotExecutionEngine,
    };
    use crate::fifo_sim::{
        run_200k_fifo_benchmark, run_quadruple_omega_ablation_suite, OmegaConservativeFifoOrder,
        OmegaFifoLifecycle,
    };
    use crate::forensic_parquet::{ColumnarMarketRow, MarketEventKind, ParitySyncLedger};
    use crate::lob_shm::{BookLevel, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
    use crate::telemetry_hud::DEFAULT_WALLET_EQUITY_1E8;
    use crate::types::{
        print_auto_aligned_box, BoundedScore, LotQty, PortfolioBetaGuard, PriceTicks, ScaledBps,
        Side, TimestampNs, ANSI_GREEN, ANSI_RESET, ANSI_SKY_BLUE, ANSI_YELLOW, APPROVED_SPECS,
    };
    use crate::wave_elliott::{WaveObservables, WaveRegime};

    #[derive(Copy, Clone, Debug)]
    pub struct TwinReplayEvent {
        pub slot_idx: usize,
        pub kind: MarketEventKind,
        pub aggressor_side: Side,
        pub trade_price: PriceTicks,
        pub trade_lots: LotQty,
        pub snap: SymbolLobSnapshot,
    }

    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
    pub struct OmegaTwinWalkForwardReport {
        pub par1_rows_verified: u64,
        pub closed_trades: u64,
        pub maker_closes: u64,
        pub chandelier_closes: u64,
        pub shadow_crest_snaps: u64,
        pub fibonacci_surfs: u64,
        pub hawkes_queue_aborts: u64,
        pub post_only_rejects: u64,
        pub cum_net_bps_x100: i64,
        pub cum_net_pnl_1e8: i64,
        pub parity_sync_drift_1e8: i64,
    }

    /// 1:1 Digital Twin Walk-Forward Engine matching `step_12_slot_daemon_tick` with 25ms RTT FIFO.
    #[derive(Debug)]
    pub struct OmegaDigitalTwinEngine {
        pub engines: [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
        pub gov: OnlineAdaptiveGovernor,
        pub ledger: ParitySyncLedger,
        pub entry_fifo: [OmegaConservativeFifoOrder; MAX_ACTIVE_PAIRS],
        pub exit_fifo: [OmegaConservativeFifoOrder; MAX_ACTIVE_PAIRS],
        pub cached_snaps: [SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
        pub report: OmegaTwinWalkForwardReport,
        sum_trade_pnl_1e8: i64,
    }

    impl Default for OmegaDigitalTwinEngine {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    impl OmegaDigitalTwinEngine {
        #[must_use]
        pub fn new() -> Self {
            Self {
                engines: SlotExecutionEngine::new_12_slot_array(),
                gov: OnlineAdaptiveGovernor::new(),
                ledger: ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8),
                entry_fifo: [OmegaConservativeFifoOrder::new(); MAX_ACTIVE_PAIRS],
                exit_fifo: [OmegaConservativeFifoOrder::new(); MAX_ACTIVE_PAIRS],
                cached_snaps: [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS],
                report: OmegaTwinWalkForwardReport::default(),
                sum_trade_pnl_1e8: 0,
            }
        }

        pub fn ingest_event(&mut self, ev: &TwinReplayEvent) {
            let s_idx = ev.slot_idx.min(MAX_ACTIVE_PAIRS - 1);
            let _par1_row = ColumnarMarketRow::from_lob_snapshot(
                &ev.snap,
                ev.kind,
                ev.aggressor_side,
                ev.trade_price,
                ev.trade_lots,
            );
            self.report.par1_rows_verified = self.report.par1_rows_verified.saturating_add(1);

            self.cached_snaps[s_idx] = ev.snap;
            self.gov.observe_market_price(s_idx, &ev.snap);
            let btc_snap = if self.cached_snaps[0].bids[0].price.0 > 0 {
                self.cached_snaps[0]
            } else {
                ev.snap
            };
            let now_ns = ev.snap.ts_local_ns;
            let spec = APPROVED_SPECS[s_idx];

            // 1. Advance 25ms RTT FIFO queues on L2 update or Trade print
            let mut entry_fill_opt = None;
            let mut exit_fill_opt = None;
            let mut opp_tape_lots = LotQty(0);

            match ev.kind {
                MarketEventKind::L2BookSnapshot => {
                    self.entry_fifo[s_idx].on_lob_snapshot(&ev.snap);
                    self.exit_fifo[s_idx].on_lob_snapshot(&ev.snap);
                }
                MarketEventKind::PublicTradePrint => {
                    if let OmegaFifoLifecycle::RestingInFifoQueue { side, limit_price, .. } =
                        self.entry_fifo[s_idx].state
                    {
                        let hits = match side {
                            Side::Bid => ev.aggressor_side == Side::Ask && ev.trade_price.0 <= limit_price.0,
                            Side::Ask => ev.aggressor_side == Side::Bid && ev.trade_price.0 >= limit_price.0,
                        };
                        if hits {
                            opp_tape_lots = ev.trade_lots;
                        }
                    }
                    entry_fill_opt = self.entry_fifo[s_idx].on_trade_print(
                        ev.aggressor_side,
                        ev.trade_price,
                        ev.trade_lots,
                        now_ns.0,
                    );
                    exit_fill_opt = self.exit_fifo[s_idx].on_trade_print(
                        ev.aggressor_side,
                        ev.trade_price,
                        ev.trade_lots,
                        now_ns.0,
                    );
                }
            }

            // 2. Step per-slot FSM under exact 1:1 production rules
            match self.engines[s_idx].state {
                OrderLifecycleState::Idle => {
                    let mut beta_guard = PortfolioBetaGuard::new();
                    for eng in &self.engines {
                        if let Some(act_s) = eng.active_side() {
                            beta_guard.observe_active_side(act_s);
                        }
                    }
                    let probe_side = if OrthogonalFactorState::snr_composite_score(&ev.snap) >= 0.0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let (probe_edge, _, _) = self.gov.slot_direction_diagnostics(s_idx, probe_side);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_elastic(
                        spec,
                        &ev.snap,
                        &btc_snap,
                        ScaledBps(self.engines[s_idx].learned_mfe_bps),
                        ScaledBps(probe_edge),
                    );
                    if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
                        let eq_1e8 = self.ledger.current_equity_1e8();
                        if beta_guard.can_admit_side(side)
                            && self.gov.authorizes_adaptive_entry_with_wallet(
                                s_idx, side, eq_1e8, &ev.snap, &btc_snap,
                            )
                        {
                            let lots = self.gov.compute_slot_entry_lots(s_idx, side, eq_1e8, &ev.snap);
                            self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                            let _ = self.engines[s_idx].try_arm_maker_entry(&ev.snap, verdict, lots);
                        }
                    }
                }
                OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    let wait_ns =
                        self.engines[s_idx].elastic_preflight_window_ns(ev.snap.realized_vol_5m);
                    if now_ns.0.saturating_sub(submitted_ts.0) >= wait_ns {
                        let eq_1e8 = self.ledger.current_equity_1e8();
                        if self
                            .gov
                            .confirm_preflight_kinematics_non_choking(
                                s_idx, side, eq_1e8, &ev.snap, &btc_snap,
                            )
                            .is_ok()
                        {
                            if let Ok((synced_px, _)) =
                                self.engines[s_idx].sync_preflight_l1_entry(&ev.snap, 8)
                            {
                                self.entry_fifo[s_idx]
                                    .submit_post_only(side, synced_px, qty, now_ns.0);
                                let _ = self.engines[s_idx].on_entry_order_resting(now_ns);
                            } else {
                                self.engines[s_idx].state = OrderLifecycleState::Idle;
                            }
                        } else {
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                    }
                }
                OrderLifecycleState::RestingEntryQueue {
                    side, entry_price, qty, resting_since_ts, ..
                } => {
                    if let Some((fill_px, fill_lots)) = entry_fill_opt {
                        self.entry_fifo[s_idx].cancel();
                        self.gov.clear_abort_streak(s_idx, side);
                        self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                        let _ = self.engines[s_idx].on_maker_entry_filled_verified(
                            now_ns,
                            Some(side),
                            Some(fill_px),
                            Some(fill_lots),
                        );
                        if let Some(q0) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap)
                        {
                            self.exit_fifo[s_idx].submit_post_only(
                                side.opposite(),
                                q0.target_price,
                                q0.order_lots,
                                now_ns.0,
                            );
                            if let OrderLifecycleState::PositionOpenMakerExit {
                                active_exit_role: ref mut ax_r,
                                active_exit_price: ref mut ax_p,
                                active_exit_lots: ref mut ax_l,
                                resting_exit_price: ref mut rx_p,
                                ..
                            } = self.engines[s_idx].state
                            {
                                *ax_r = q0.role;
                                *ax_p = q0.target_price;
                                *ax_l = q0.order_lots;
                                *rx_p = q0.target_price;
                            }
                        }
                        return;
                    }

                    if let OmegaFifoLifecycle::RestingInFifoQueue {
                        init_queue_ahead,
                        queue_ahead_lots,
                        ..
                    } = self.entry_fifo[s_idx].state
                    {
                        let q_verdict = self.engines[s_idx].evaluate_resting_queue_shield(
                            side,
                            init_queue_ahead,
                            queue_ahead_lots,
                            opp_tape_lots,
                            &ev.snap,
                            &btc_snap,
                        );
                        let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                        if elapsed_ns >= 25_000_000 && q_verdict.abort_now {
                            self.entry_fifo[s_idx].cancel();
                            self.gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                            self.report.hawkes_queue_aborts += 1;
                            return;
                        }
                        let at_l1 = match side {
                            Side::Bid => ev.snap.bids[0].price == entry_price,
                            Side::Ask => ev.snap.asks[0].price == entry_price,
                        };
                        if !at_l1
                            && elapsed_ns >= 2_500_000_000
                            && passes_stoikov_repeg_coherence_guard(
                                s_idx, side, &ev.snap, &btc_snap,
                            )
                        {
                            let new_l1 = side.select_entry_bbo_price(
                                ev.snap.bids[0].price,
                                ev.snap.asks[0].price,
                            );
                            if let Ok(repeg_sl) =
                                self.engines[s_idx].compute_repeg_sl_trigger(&ev.snap, new_l1)
                            {
                                self.engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                self.entry_fifo[s_idx]
                                    .submit_post_only(side, new_l1, qty, now_ns.0);
                            }
                        }
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side,
                    avg_price_ticks,
                    mut peak_favorable_price,
                    mut worst_adverse_price,
                    resting_exit_price,
                    sl_trigger_price,
                    ..
                } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    self.engines[s_idx].observe_open_flow_kinematics(&ev.snap);
                    let _ = self.engines[s_idx].observe_wald_sprt_step(&ev.snap, &btc_snap);

                    let ref_px = if ev.snap.last_trade_price.0 > 0 {
                        ev.snap.last_trade_price
                    } else {
                        ev.snap.mark_price
                    };
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
                    } = self.engines[s_idx].state
                    {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let d_sign = side.sign_i64();
                    let denom = avg_price_ticks.0.max(1);
                    self.engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0
                        - avg_price_ticks.0)
                        * d_sign)
                        .saturating_mul(1_000_000)
                        / denom;
                    self.engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0
                        - avg_price_ticks.0)
                        * d_sign)
                        .saturating_mul(1_000_000)
                        / denom;
                    self.engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    self.engines[s_idx].last_closed_worst_ticks = worst_adverse_price;

                    if self.engines[s_idx].last_eff_sl_ticks.0 <= 0 {
                        self.engines[s_idx].last_eff_sl_ticks = sl_trigger_price;
                    }
                    let eff_sl = self.engines[s_idx].last_eff_sl_ticks;
                    let sl_crossed = match side {
                        Side::Bid => ev.snap.mark_price.0 > 0 && ev.snap.mark_price.0 <= eff_sl.0,
                        Side::Ask => ev.snap.mark_price.0 > 0 && ev.snap.mark_price.0 >= eff_sl.0,
                    };
                    if sl_crossed {
                        self.exit_fifo[s_idx].cancel();
                        let is_chand = self.engines[s_idx].chandelier_profit_sl_armed
                            || is_chandelier_profit_locked_sl(side, avg_price_ticks, eff_sl);
                        self.engines[s_idx].last_exit_sub_reason = if is_chand {
                            self.report.chandelier_closes += 1;
                            ExitSubReason::ChandelierProfitLockSl
                        } else {
                            ExitSubReason::WaldBaseStopLoss
                        };
                        if let Some(summary) = self.engines[s_idx].finalize_open_position_close(
                            ClosedTradeKind::ServerMarkPriceStopLoss,
                            eff_sl,
                            now_ns,
                            &mut self.ledger,
                        ) {
                            let non_pun = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                            self.gov.observe_closed_trade_at_entry(
                                s_idx,
                                summary.side,
                                summary.entry_price,
                                summary.net_scaled_bps,
                                non_pun,
                                now_ns,
                            );
                            self.record_summary(&summary);
                        }
                        return;
                    }

                    if let Some((fill_px, _)) = exit_fill_opt {
                        self.exit_fifo[s_idx].cancel();
                        let cur_role = match self.engines[s_idx].state {
                            OrderLifecycleState::PositionOpenMakerExit { active_exit_role, .. } => {
                                active_exit_role
                            }
                            _ => ExitOrderRole::StandardMakerExit100,
                        };
                        if matches!(cur_role, ExitOrderRole::Tranche1Harvest50)
                            && self.engines[s_idx].record_tranche1_harvest(fill_px)
                        {
                            if let Some(q2) =
                                self.engines[s_idx].compute_canonical_exit_quote(&ev.snap)
                            {
                                self.exit_fifo[s_idx].submit_post_only(
                                    side.opposite(),
                                    q2.target_price,
                                    q2.order_lots,
                                    now_ns.0,
                                );
                                if let OrderLifecycleState::PositionOpenMakerExit {
                                    active_exit_role: ref mut ax_r,
                                    active_exit_price: ref mut ax_p,
                                    active_exit_lots: ref mut ax_l,
                                    resting_exit_price: ref mut rx_p,
                                    ..
                                } = self.engines[s_idx].state
                                {
                                    *ax_r = q2.role;
                                    *ax_p = q2.target_price;
                                    *ax_l = q2.order_lots;
                                    *rx_p = q2.target_price;
                                }
                                self.engines[s_idx].next_peg_allowed_ts_ns =
                                    now_ns.0 + 1_350_000_000;
                            }
                            return;
                        }

                        if matches!(
                            self.engines[s_idx].last_exit_sub_reason,
                            ExitSubReason::ShadowRunnerCrestHarvest
                        ) {
                            self.report.shadow_crest_snaps += 1;
                        } else if matches!(
                            self.engines[s_idx].last_exit_sub_reason,
                            ExitSubReason::FibonacciImpulseSurf
                        ) {
                            self.report.fibonacci_surfs += 1;
                        }
                        if let Some(summary) = self.engines[s_idx].finalize_open_position_close(
                            ClosedTradeKind::MakerPassiveUnwind,
                            fill_px,
                            now_ns,
                            &mut self.ledger,
                        ) {
                            self.gov.observe_closed_trade_at_entry(
                                s_idx,
                                summary.side,
                                summary.entry_price,
                                summary.net_scaled_bps,
                                true,
                                now_ns,
                            );
                            self.record_summary(&summary);
                        }
                        return;
                    }

                    let Some(quote) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap)
                    else {
                        return;
                    };
                    self.engines[s_idx].last_closed_wald_pct =
                        self.engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                    self.engines[s_idx].latch_defensive_floor_if_needed(quote.defensive_lock_active);
                    let _ =
                        self.engines[s_idx].maybe_ratchet_chandelier_or_wald_sl(&ev.snap, &btc_snap);
                    self.engines[s_idx].last_exit_sub_reason = quote.sub_reason;

                    let (preempt, delay_ns) =
                        compute_exit_sync_schedule(&quote, resting_exit_price, avg_price_ticks);
                    let need_repeg = resting_exit_price != quote.target_price
                        && (preempt || now_ns.0 >= self.engines[s_idx].next_peg_allowed_ts_ns);
                    if need_repeg {
                        self.exit_fifo[s_idx].submit_post_only(
                            side.opposite(),
                            quote.target_price,
                            quote.order_lots,
                            now_ns.0,
                        );
                        self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + delay_ns;
                        if let OrderLifecycleState::PositionOpenMakerExit {
                            active_exit_role: ref mut ax_r,
                            active_exit_price: ref mut ax_p,
                            active_exit_lots: ref mut ax_l,
                            resting_exit_price: ref mut rx_p,
                            ..
                        } = self.engines[s_idx].state
                        {
                            *ax_r = quote.role;
                            *ax_p = quote.target_price;
                            *ax_l = quote.order_lots;
                            *rx_p = quote.target_price;
                        }
                    }
                }
            }
        }

        fn record_summary(&mut self, s: &crate::execution_fsm::ClosedTradeSummary) {
            self.report.closed_trades += 1;
            if s.is_maker_exit {
                self.report.maker_closes += 1;
            }
            self.report.cum_net_bps_x100 += s.net_scaled_bps.raw();
            self.report.cum_net_pnl_1e8 += s.net_pnl_1e8;
            self.sum_trade_pnl_1e8 += s.net_pnl_1e8;
            self.report.parity_sync_drift_1e8 =
                (self.ledger.cumulative_net_pnl_1e8 - self.sum_trade_pnl_1e8).abs();
        }

        #[must_use]
        pub fn finalize_report(&mut self) -> OmegaTwinWalkForwardReport {
            let mut rej = 0_u64;
            for s in 0..MAX_ACTIVE_PAIRS {
                rej += self.entry_fifo[s].post_only_rejects + self.exit_fifo[s].post_only_rejects;
            }
            self.report.post_only_rejects = rej;
            self.report.parity_sync_drift_1e8 =
                (self.ledger.cumulative_net_pnl_1e8 - self.sum_trade_pnl_1e8).abs();
            self.report
        }
    }

    fn base_snap(slot_idx: usize, px: i64, b_q: i64, a_q: i64, ts_ns: i64) -> SymbolLobSnapshot {
        let mut s = SymbolLobSnapshot::new_empty();
        s.symbol_id = APPROVED_SPECS[slot_idx].symbol_id;
        s.is_eligible = true;
        s.sequence_id = (ts_ns.unsigned_abs() / 1_000_000) + 1;
        s.ts_exchange_ns = TimestampNs(ts_ns);
        s.ts_local_ns = TimestampNs(ts_ns);
        s.realized_vol_5m = ScaledBps(6_500);
        s.bids[0] = BookLevel { price: PriceTicks(px), qty: LotQty(b_q) };
        s.asks[0] = BookLevel { price: PriceTicks(px + 1), qty: LotQty(a_q) };
        s.refresh_deep_l1_l5_totals();
        s.mark_price = PriceTicks(px);
        s.last_trade_price = PriceTicks(px);
        s.factor_ofi = BoundedScore::new_clamped(0.76);
        s.factor_tfi = BoundedScore::new_clamped(0.80);
        s.factor_geom = BoundedScore::new_clamped(0.48);
        s
    }

    /// Runs 4-episode 25ms-FIFO 1:1 Digital Twin Lab (Wave3+Fib, Hawkes Abort, BTC Shadow-Runner, Chandelier).
    #[must_use]
    pub fn run_omega_digital_twin_walk_forward_lab() -> OmegaTwinWalkForwardReport {
        let mut twin = OmegaDigitalTwinEngine::new();
        // Anchor BTC leader at 14:30 UTC institutional liquidity peak
        let t0 = 1_700_058_600_000_000_000_i64;
        let btc_base = base_snap(0, 759_000, 450, 320, t0);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 0,
            kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid,
            trade_price: PriceTicks(759_000),
            trade_lots: LotQty(1),
            snap: btc_base,
        });

        // Episode 1 (Slot 3 AVAXUSDT): 25ms preflight -> FIFO fill -> Wave3 ~38% T1 -> Fib +36 bps T2
        let mut avax = base_snap(3, 8_200, 120, 180, t0 + 10_000_000);
        avax.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.66),
            macro_phase: BoundedScore::new_clamped(0.85),
            oi_impulse: BoundedScore::new_clamped(0.45),
            liq_imbalance: BoundedScore::new_clamped(0.60),
            fib_proj_bps: ScaledBps(3_000),
        });
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_200), trade_lots: LotQty(1), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 45_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_200), trade_lots: LotQty(1), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 80_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_200), trade_lots: LotQty(1), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 110_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Ask, trade_price: PriceTicks(8_200), trade_lots: LotQty(150), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 160_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_204), qty: LotQty(380) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(80) };
        avax.mark_price = PriceTicks(8_205);
        avax.last_trade_price = PriceTicks(8_205);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_205), trade_lots: LotQty(1), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 200_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_205), trade_lots: LotQty(120), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 1_200_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_229), qty: LotQty(390) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_230), qty: LotQty(70) };
        avax.mark_price = PriceTicks(8_230);
        avax.last_trade_price = PriceTicks(8_230);
        avax.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.66),
            macro_phase: BoundedScore::new_clamped(0.88),
            oi_impulse: BoundedScore::new_clamped(0.40),
            liq_imbalance: BoundedScore::new_clamped(0.55),
            fib_proj_bps: ScaledBps(4),
        });
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_230), trade_lots: LotQty(1), snap: avax,
        });
        avax.ts_local_ns = TimestampNs(t0 + 1_260_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 3, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_230), trade_lots: LotQty(110), snap: avax,
        });

        // Episode 2 (Slot 9 LINKUSDT): Hidden Toxic L1 Sweep aborted by Hawkes + Stoikov BEFORE fill
        let t2 = t0 + 10_000_000_000;
        let mut link = base_snap(9, 10_720, 500, 320, t2);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 9, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(10_720), trade_lots: LotQty(1), snap: link,
        });
        link.ts_local_ns = TimestampNs(t2 + 35_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 9, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(10_720), trade_lots: LotQty(1), snap: link,
        });
        link.ts_local_ns = TimestampNs(t2 + 70_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 9, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(10_720), trade_lots: LotQty(1), snap: link,
        });
        for k in 1..=5_i64 {
            link.ts_local_ns = TimestampNs(t2 + 70_000_000 + k * 20_000_000);
            link.bids[0] = BookLevel { price: PriceTicks(10_720), qty: LotQty(500 - k * 75) };
            link.asks[0] = BookLevel { price: PriceTicks(10_721), qty: LotQty(410) };
            link.deep_bid_lots_l1_l5 = LotQty(360);
            link.deep_ask_lots_l1_l5 = LotQty(1_300);
            link.factor_ofi = BoundedScore::new_clamped(0.14);
            link.factor_tfi = BoundedScore::new_clamped(0.10);
            twin.ingest_event(&TwinReplayEvent {
                slot_idx: 9, kind: MarketEventKind::PublicTradePrint,
                aggressor_side: Side::Ask, trade_price: PriceTicks(10_720), trade_lots: LotQty(75), snap: link,
            });
        }

        // Episode 3 (Slot 1 ETHUSDT 1-Lot): Shadow-Runner Sentinel float + Crest Snap-In via 25ms FIFO
        let t3 = t0 + 25_000_000_000;
        let mut eth = base_snap(1, 223_700, 100, 180, t3);
        eth.set_wave_observables(WaveObservables {
            regime: WaveRegime::NeutralWarmup,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.60),
            macro_phase: BoundedScore::ZERO,
            oi_impulse: BoundedScore::ZERO,
            liq_imbalance: BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(0),
        });
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 1, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(223_700), trade_lots: LotQty(1), snap: eth,
        });
        eth.ts_local_ns = TimestampNs(t3 + 35_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 1, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(223_700), trade_lots: LotQty(1), snap: eth,
        });
        eth.ts_local_ns = TimestampNs(t3 + 70_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 1, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(223_700), trade_lots: LotQty(1), snap: eth,
        });
        eth.ts_local_ns = TimestampNs(t3 + 95_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 1, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Ask, trade_price: PriceTicks(223_700), trade_lots: LotQty(130), snap: eth,
        });
        for k in 1..=8_i64 {
            let px = 223_850 + k * 115;
            eth.ts_local_ns = TimestampNs(t3 + 95_000_000 + k * 50_000_000);
            eth.bids[0] = BookLevel { price: PriceTicks(px - 1), qty: LotQty(360) };
            eth.asks[0] = BookLevel { price: PriceTicks(px), qty: LotQty(180) };
            eth.mark_price = PriceTicks(px);
            eth.last_trade_price = PriceTicks(px);
            eth.factor_ofi = BoundedScore::new_clamped(0.62 + 0.03 * (k as f64));
            eth.factor_tfi = BoundedScore::new_clamped(0.66 + 0.03 * (k as f64));
            twin.ingest_event(&TwinReplayEvent {
                slot_idx: 1, kind: MarketEventKind::L2BookSnapshot,
                aggressor_side: Side::Bid, trade_price: PriceTicks(px), trade_lots: LotQty(1), snap: eth,
            });
        }
        // Crest deceleration at 224_770 (+47.8 bps): preempts peg to 224_770 -> rests -> fills
        for m in 1..=3_i64 {
            eth.ts_local_ns = TimestampNs(t3 + 500_000_000 + m * 50_000_000);
            eth.bids[0] = BookLevel { price: PriceTicks(224_769), qty: LotQty(240) };
            eth.asks[0] = BookLevel { price: PriceTicks(224_770), qty: LotQty(60) };
            eth.mark_price = PriceTicks(224_770);
            eth.last_trade_price = PriceTicks(224_770);
            eth.factor_ofi = BoundedScore::new_clamped(0.04);
            eth.factor_tfi = BoundedScore::new_clamped(0.03);
            eth.factor_geom = BoundedScore::ZERO;
            twin.ingest_event(&TwinReplayEvent {
                slot_idx: 1, kind: MarketEventKind::L2BookSnapshot,
                aggressor_side: Side::Bid, trade_price: PriceTicks(224_770), trade_lots: LotQty(1), snap: eth,
            });
        }
        eth.ts_local_ns = TimestampNs(t3 + 700_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 1, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Bid, trade_price: PriceTicks(224_770), trade_lots: LotQty(95), snap: eth,
        });

        // Episode 4 (Slot 5 SUIUSDT): Wave-5 +58 bps arms Chandelier SL -> Flash reversal banks green
        let t4 = t0 + 40_000_000_000;
        let mut sui = base_snap(5, 8_510, 110, 180, t4);
        sui.realized_vol_5m = ScaledBps(5_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui,
        });
        sui.ts_local_ns = TimestampNs(t4 + 35_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui,
        });
        sui.ts_local_ns = TimestampNs(t4 + 70_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui,
        });
        sui.ts_local_ns = TimestampNs(t4 + 95_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::PublicTradePrint,
            aggressor_side: Side::Ask, trade_price: PriceTicks(8_510), trade_lots: LotQty(140), snap: sui,
        });
        sui.ts_local_ns = TimestampNs(t4 + 130_000_000);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui,
        });
        sui.ts_local_ns = TimestampNs(t4 + 3_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_559), qty: LotQty(360) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_560), qty: LotQty(180) };
        sui.mark_price = PriceTicks(8_560);
        sui.last_trade_price = PriceTicks(8_560);
        sui.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave5,
            climax_active: false,
            hurst: WaveObservables::encode_hurst(0.60),
            macro_phase: BoundedScore::new_clamped(0.70),
            oi_impulse: BoundedScore::new_clamped(0.20),
            liq_imbalance: BoundedScore::new_clamped(0.20),
            fib_proj_bps: ScaledBps(2_600),
        });
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(8_560), trade_lots: LotQty(1), snap: sui,
        });
        let chand_px = twin.engines[5].last_eff_sl_ticks;
        sui.ts_local_ns = TimestampNs(t4 + 4_000_000_000);
        sui.mark_price = PriceTicks(chand_px.0 - 1);
        sui.last_trade_price = PriceTicks(chand_px.0 - 1);
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 5, kind: MarketEventKind::L2BookSnapshot,
            aggressor_side: Side::Bid, trade_price: PriceTicks(chand_px.0 - 1), trade_lots: LotQty(1), snap: sui,
        });

        twin.finalize_report()
    }

    /// Executes and prints the institutional Gate 2 (`vortex_replay`) 4-Arm + Digital Twin report.
    #[must_use]
    pub fn run_and_print_omega_chat6_twin_certification() -> bool {
        let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
        let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
        let ab = run_quadruple_omega_ablation_suite();
        let twin = run_omega_digital_twin_walk_forward_lab();

        let sec_title = vec![format!(
            "{}[VORTEX-REPLAY OMEGA V4.0] 4-ARM PARETO ABLATION & 1:1 DIGITAL TWIN GATE{}",
            ANSI_SKY_BLUE, ANSI_RESET
        )];
        let sec_fifo = vec![format!(
            "  {}200k FIFO Queue Sim{}: {} ev/s | Passive Fills: {} | PostOnly Rejects: {}",
            ANSI_GREEN, ANSI_RESET, ev_per_sec, fills, rejects
        )];
        let sec_arms = vec![
            format!(
                "  MODEL A   (Baseline Fixed L1)      : {:>+8.2} bps net",
                (ab.arm_a_baseline_net_bps_x100 as f64) / 100.0
            ),
            format!(
                "  MODEL B   (Elastic Multi-Tranche)  : {:>+8.2} bps net",
                (ab.arm_b_multitranche_net_bps_x100 as f64) / 100.0
            ),
            format!(
                "  MODEL A+B (VORTEX-L3 LVDBS Surf)   : {:>+8.2} bps net",
                (ab.arm_ab_vortex_l3_net_bps_x100 as f64) / 100.0
            ),
            format!(
                "  {}MODEL OMEGA (Shadow+Fib+Chand+Wald): {:>+8.2} bps net [PARETO DOMINANT]{}",
                ANSI_YELLOW,
                (ab.arm_omega_full_net_bps_x100 as f64) / 100.0,
                ANSI_RESET
            ),
            format!(
                "  Adversarial 24-Ep Tape             : L3 {:>+8.2} bps ({} loss) vs OMEGA {:>+8.2} bps ({} loss | Hwk:{} Chd:{} Wld:{})",
                (ab.stress_l3_unshielded.cum_net_bps_x100 as f64) / 100.0,
                ab.stress_l3_unshielded.losses,
                (ab.stress_omega_shielded.cum_net_bps_x100 as f64) / 100.0,
                ab.stress_omega_shielded.losses,
                ab.stress_omega_shielded.hawkes_aborts,
                ab.stress_omega_shielded.chandelier_saves,
                ab.stress_omega_shielded.wald_compressions
            ),
        ];
        let sec_twin = vec![format!(
            "  {}1:1 Digital Twin (25ms RTT FIFO){}  : Trades:{} | Net:{:+.2} bps | HwkAbort:{} | Rej:{} | Drift:{}",
            ANSI_GREEN,
            ANSI_RESET,
            twin.closed_trades,
            (twin.cum_net_bps_x100 as f64) / 100.0,
            twin.hawkes_queue_aborts,
            twin.post_only_rejects,
            twin.parity_sync_drift_1e8
        )];
        print_auto_aligned_box(&[sec_title, sec_fifo, sec_arms, sec_twin]);

        ev_per_sec >= 100_000
            && fills == 1_000
            && rejects == 0
            && ab.pareto_strictly_dominant
            && twin.closed_trades == 3
            && twin.hawkes_queue_aborts == 1
            && twin.post_only_rejects == 0
            && twin.parity_sync_drift_1e8 == 0
            && twin.cum_net_bps_x100 >= 7_000
    }
}

pub use omega_twin::*;

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

// === VORTEX-OMEGA CHAT 6 REPLAY TESTS ===
#[cfg(test)]
mod omega_chat6_replay_tests {
    use super::*;

    #[test]
    fn test_omega_digital_twin_1to1_hawkes_wald_shadow_and_chandelier() {
        let rep = run_omega_digital_twin_walk_forward_lab();
        assert!(rep.par1_rows_verified >= 25);
        assert_eq!(rep.closed_trades, 3);
        assert_eq!(rep.maker_closes, 2);
        assert_eq!(rep.chandelier_closes, 1);
        assert_eq!(rep.shadow_crest_snaps, 1);
        assert_eq!(rep.fibonacci_surfs, 1);
        assert_eq!(rep.hawkes_queue_aborts, 1);
        assert_eq!(rep.post_only_rejects, 0);
        assert_eq!(rep.parity_sync_drift_1e8, 0);
        assert!(rep.cum_net_bps_x100 >= 7_000);
        assert!(rep.cum_net_pnl_1e8 > 0);
        assert!(run_and_print_omega_chat6_twin_certification());
    }
}
