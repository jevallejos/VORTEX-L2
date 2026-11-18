//! Continuous 1:1 Walk-Forward PAR1/PAR2 Replay Engine, Elastic Daily Shield, Quantum EV & Digital Twin (V4.8).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use std::fs;
use std::path::Path;

use crate::alpha_3factor::{
    EppsBtcCorrelationTracker, EventClockAlphaModulator, OrthogonalFactorState, SignalVerdict,
};
use crate::bybit_client::HANDSHAKE_VAULT_CACHE_PATH;
use crate::bybit_gateway::should_repeg_resting_exit_tp;
use crate::exec_daemon::{
    build_monetary_beta_guard_with_epps, can_stage_same_direction_entry,
    compute_exit_sync_schedule_with_fees, elastic_resting_queue_timers_ns,
    slot_position_risk_ppm_with_fees,
};
use crate::execution_fsm::{
    check_queue_depletion_velocity_abort, is_chandelier_profit_locked_sl_with_fees,
    passes_elliott_macro_coherence_guard, passes_order_block_wall_guard_dynamic,
    passes_stoikov_repeg_coherence_guard_with_beta, ClosedTradeKind, ExitOrderRole, ExitSubReason,
    OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
};
use crate::fee_cadence_beta::FeeSchedule;
use crate::fifo_sim::{FifoQueueSimulator, SimulatedOrderState, INITIAL_REPLAY_EQUITY_1E8};
use crate::forensic_parquet::{
    ColumnarMarketRow, ColumnarParquetBatch, Par2EventKind, ParitySyncLedger,
};
use crate::governor_plasticity::DailySessionGovernor;
use crate::lob_shm::{LastVetoCode, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::SlotMarkoutRing;
use crate::order_block::{CausalOrderBlockEngine, OB_PLASTICITY_VAULT_PATH};
use crate::types::{
    classify_pnl_badge, LotQty, PairSpec, PriceTicks, ScaledBps, Side, ANSI_GREEN, ANSI_ORANGE,
    ANSI_RED, ANSI_RESET, ANSI_YELLOW, APPROVED_SPECS,
};
use crate::vault_io::MFE_PLASTICITY_VAULT_PATH;
use crate::wave_elliott::CausalElliottWaveState;

const SCAR_MEMORY_VAULT_PATH: &str = "data/vault/vortex_governor_scar_memory.jsonl";
const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";
pub const MIN_FUNDING_EPOCH_NS: i64 = 1_577_836_800_000_000_000;

#[derive(Copy, Clone, Debug, Default)]
pub struct WalkForwardReport {
    pub files_count: usize,
    pub total_events: u64,
    pub l2_events: u64,
    pub trade_events: u64,
    pub par2_ticker_events: u64,
    pub par2_liq_events: u64,
    pub spoof_vetoes: u64,
    pub quantum_ev_vetoes: u64,
    pub daily_hibernations: u64,
    pub armed: u64,
    pub queue_aborts: u64,
    pub tp_aborts_verified: u64,
    pub post_only_rejects: u64,
    pub fills: u64,
    pub closed: u64,
    pub maker_closes: u64,
    pub wins: u64,
    pub scratches: u64,
    pub losses: u64,
    pub funding_settlements: u64,
    pub ewma_lvr_5s_bps: i64,
    pub cum_net_bps_x100: i64,
    pub gross_profit_1e8: i64,
    pub gross_loss_1e8: i64,
    pub cum_net_pnl_1e8: i64,
}

fn load_cached_fee_schedule_if_present(path: &Path) -> Option<FeeSchedule> {
    let txt = fs::read_to_string(path).ok()?;
    let val = serde_json::from_str::<serde_json::Value>(&txt).ok()?;
    let m = val.get("maker_fee_scaled_bps")?.as_i64()?;
    let t = val.get("taker_fee_scaled_bps")?.as_i64()?;
    Some(FeeSchedule::from_live_rates_clamped(ScaledBps(m), ScaledBps(t)))
}

/// Settles exact 1e8 funding cashflow once per crossed 8h UTC boundary (`00:00 / 08:00 / 16:00 UTC`).
#[inline]
pub fn settle_crossed_funding_boundary(
    spec: PairSpec,
    side: Side,
    open_lots: LotQty,
    avg_price_ticks: PriceTicks,
    opened_ts_ns: i64,
    snap: &SymbolLobSnapshot,
    last_settled_cut: &mut i64,
    ledger: &mut ParitySyncLedger,
) -> Option<i64> {
    if open_lots.0 <= 0 || opened_ts_ns < MIN_FUNDING_EPOCH_NS || snap.ts_local_ns.0 <= opened_ts_ns {
        return None;
    }
    let open_cut = opened_ts_ns.div_euclid(1_000_000_000).div_euclid(28_800);
    if *last_settled_cut < open_cut {
        *last_settled_cut = open_cut;
    }
    let cur_cut = snap.ts_local_ns.0.div_euclid(1_000_000_000).div_euclid(28_800);
    if cur_cut <= *last_settled_cut {
        return None;
    }
    *last_settled_cut = cur_cut;
    if snap.funding_rate_scaled_bps.0 == 0 {
        return None;
    }
    let mark_px = if snap.mark_price.0 > 0 { snap.mark_price } else { avg_price_ticks };
    let pos_notional_1e8 = spec.lot_notional_1e8(open_lots, mark_px);
    if pos_notional_1e8 <= 0 {
        return None;
    }
    let recv_rate_sbps = -side.sign_i64().saturating_mul(snap.funding_rate_scaled_bps.0);
    let f_num = (pos_notional_1e8 as i128) * (recv_rate_sbps as i128);
    let funding_1e8 = if f_num >= 0 {
        ((f_num + 500_000) / 1_000_000) as i64
    } else {
        -((-f_num + 500_000) / 1_000_000) as i64
    };
    if funding_1e8 == 0 {
        return None;
    }
    ledger.record_funding_settlement(funding_1e8, pos_notional_1e8);
    Some(funding_1e8)
}

#[inline]
pub(crate) fn step_par2_observables_on_row(
    s_idx: usize,
    row: &ColumnarMarketRow,
    snap: &mut SymbolLobSnapshot,
    wave_state: &mut CausalElliottWaveState,
    ob_engine: &mut CausalOrderBlockEngine,
    markout_ring: &mut SlotMarkoutRing,
    fees: FeeSchedule,
    taker_prob_pm: i64,
) {
    let spec = APPROVED_SPECS[s_idx];
    let (prev_funding, prev_next_f, prev_oi) = (snap.funding_rate_scaled_bps, snap.next_funding_ts_ns, snap.open_interest_x100);
    row.apply_to_lob_snapshot(snap);
    ob_engine.set_realized_vol_5m(snap.realized_vol_5m);
    ob_engine.set_fee_and_taker_context(fees, taker_prob_pm);
    if matches!(row.par2_kind, Par2EventKind::TickerMeta) {
        snap.funding_rate_scaled_bps = row.funding_rate_scaled_bps;
        if row.next_funding_ts_ns.0 > 0 { snap.next_funding_ts_ns = row.next_funding_ts_ns; }
        if row.open_interest_x100 > 0 { snap.open_interest_x100 = row.open_interest_x100; }
    } else {
        if snap.funding_rate_scaled_bps.0 == 0 {
            snap.funding_rate_scaled_bps = if row.funding_rate_scaled_bps.0 != 0 { row.funding_rate_scaled_bps } else { prev_funding };
        }
        if snap.next_funding_ts_ns.0 == 0 && prev_next_f.0 > 0 { snap.next_funding_ts_ns = prev_next_f; }
        if snap.open_interest_x100 == 0 && prev_oi > 0 { snap.open_interest_x100 = prev_oi; }
    }
    if row.open_interest_x100 > 0 {
        let oi_contracts = (row.open_interest_x100 as f64) / 100.0;
        ob_engine.observe_open_interest(oi_contracts);
        if matches!(row.par2_kind, Par2EventKind::TickerMeta) || row.open_interest_x100 != prev_oi {
            snap.set_wave_observables(wave_state.observe_open_interest(spec, oi_contracts, snap.ts_local_ns.0));
        }
    }
    match row.par2_kind {
        Par2EventKind::PublicTradePrint if row.price_ticks.0 > 0 => {
            wave_state.observe_public_trade_notional(spec, snap.ts_local_ns, row.price_ticks, row.lot_qty);
            let ob_obs = ob_engine.observe_live_tick(spec, row.price_ticks, row.lot_qty, snap.ts_local_ns.0);
            let (bull_pm, bear_pm) = ob_engine.directional_efficacy_pm();
            snap.set_order_block_observables_directional(ob_obs, bull_pm, bear_pm);
        }
        Par2EventKind::LiquidationPrint if row.lot_qty.0 > 0 => {
            let eff_liq_px = if row.price_ticks.0 > 0 { row.price_ticks } else { snap.mark_price };
            snap.set_wave_observables(wave_state.observe_liquidation_with_price(spec, row.side, row.lot_qty, eff_liq_px, snap.ts_local_ns.0));
        }
        _ => {}
    }
    if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
        let mid_w = PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0));
        snap.set_wave_observables(wave_state.observe_price_tick(spec, mid_w, snap.ts_local_ns.0, snap.realized_vol_5m));
        let ob_mid = ob_engine.observe_mid_quote(spec, mid_w);
        if ob_mid.bull_wall_ticks.0 > 0 || ob_mid.bear_wall_ticks.0 > 0 {
            let (bull_pm, bear_pm) = ob_engine.directional_efficacy_pm();
            snap.set_order_block_observables_directional(ob_mid, bull_pm, bear_pm);
        }
        markout_ring.step_clock(mid_w, snap.ts_local_ns, fees.maker_fee_bps.raw());
    }
}

#[must_use]
pub fn run_parquet_walk_forward_on_dir(data_dir: &Path, verbose: bool) -> WalkForwardReport {
    let mut rep = WalkForwardReport::default();
    let Ok(entries) = fs::read_dir(data_dir) else { return rep; };
    let mut files: Vec<_> = entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|ext| ext.eq_ignore_ascii_case("parquet")))
        .map(|e| e.path())
        .collect();
    files.sort_by(|a, b| {
        let sa = crate::par2_archiver::parse_segment_seq_from_path(a);
        let sb = crate::par2_archiver::parse_segment_seq_from_path(b);
        sa.cmp(&sb).then_with(|| a.cmp(b))
    });
    if files.is_empty() { return rep; }
    rep.files_count = files.len();

    let mut engines = SlotExecutionEngine::new_12_slot_array();
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut daily_gov = DailySessionGovernor::new(INITIAL_REPLAY_EQUITY_1E8);
    let mut wave_states = [CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS];
    let mut ob_engines = [CausalOrderBlockEngine::new(); MAX_ACTIVE_PAIRS];
    let mut markout_rings = [SlotMarkoutRing::new(); MAX_ACTIVE_PAIRS];
    let mut epps_trackers = [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS];
    let mut spoof_clocks = [EventClockAlphaModulator::new(); MAX_ACTIVE_PAIRS];
    let mut last_funding_cut = [0_i64; MAX_ACTIVE_PAIRS];

    if verbose {
        let _ = gov.rehydrate_from_scar_and_loss_files(
            Path::new(SCAR_MEMORY_VAULT_PATH), Path::new(RED_LOSS_BOX_PATH),
            Some(Path::new("data/vault/vortex_ai_forensic_vault.jsonl")),
        );
        if let Some(cached_fees) = load_cached_fee_schedule_if_present(Path::new(HANDSHAKE_VAULT_CACHE_PATH)) {
            gov.set_active_fee_schedule(cached_fees);
        }
        let _ = CausalOrderBlockEngine::load_plasticity_array_from_path(&mut ob_engines, Path::new(OB_PLASTICITY_VAULT_PATH));
        for eng in &mut engines { eng.sync_governor_dynamics(&gov); }
        SlotExecutionEngine::hydrate_12_slot_mfe_from_path(&mut engines, Path::new(MFE_PLASTICITY_VAULT_PATH));
    }
    let fees_init = gov.active_fee_schedule();
    let mut ledger = ParitySyncLedger::new(INITIAL_REPLAY_EQUITY_1E8);
    ledger.set_live_vip_fee_rates(fees_init.maker_fee_bps, fees_init.taker_fee_bps);

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
        let (b_pm, a_pm) = ob_engines[s].directional_efficacy_pm();
        snaps[s].set_order_block_observables_directional(ob_engines[s].observables(), b_pm, a_pm);
    }

    for f_path in &files {
        let Ok((batch, _)) = ColumnarParquetBatch::read_from_file(f_path) else { continue; };
        for i in 0..batch.len() {
            let Some(row) = batch.get_row(i) else { continue };
            let Some(s_idx) = row.symbol_id.slot_index() else { continue };
            rep.total_events += 1;

            let fees = gov.active_fee_schedule();
            let slot_tkr_pm = i64::midpoint(gov.slot_taker_prob_permille(s_idx, Side::Bid), gov.slot_taker_prob_permille(s_idx, Side::Ask));
            step_par2_observables_on_row(s_idx, &row, &mut snaps[s_idx], &mut wave_states[s_idx], &mut ob_engines[s_idx], &mut markout_rings[s_idx], fees, slot_tkr_pm);
            if first_ring_ts_ns == 0 && snaps[s_idx].ts_local_ns.0 > 0 { first_ring_ts_ns = snaps[s_idx].ts_local_ns.0; }

            let _ = spoof_clocks[s_idx].step_event_clock_and_spoof(snaps[s_idx].ts_local_ns, snaps[s_idx].factor_ofi, snaps[s_idx].factor_tfi);
            let mid_now = PriceTicks(i64::midpoint(snaps[s_idx].bids[0].price.0.max(1), snaps[s_idx].asks[0].price.0.max(1)));
            let mid_btc = PriceTicks(i64::midpoint(snaps[0].bids[0].price.0.max(1), snaps[0].asks[0].price.0.max(1)));
            epps_trackers[s_idx].observe_5s_pair(snaps[s_idx].ts_local_ns, s_idx == 0, mid_now, mid_btc);
            gov.observe_market_price(s_idx, &snaps[s_idx]);

            match row.par2_kind {
                Par2EventKind::L2BookSnapshot => {
                    rep.l2_events += 1;
                    entry_fifo[s_idx].on_lob_snapshot(&snaps[s_idx]);
                    exit_fifo[s_idx].on_lob_snapshot(&snaps[s_idx]);
                }
                Par2EventKind::PublicTradePrint => {
                    rep.trade_events += 1;
                    entry_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                    exit_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                }
                Par2EventKind::TickerMeta => { rep.par2_ticker_events += 1; }
                Par2EventKind::LiquidationPrint => { rep.par2_liq_events += 1; }
            }

            let cur_snap = snaps[s_idx];
            let btc_snap = snaps[0];
            let spec = APPROVED_SPECS[s_idx];
            let spoof_d = spoof_clocks[s_idx].spoofing_density();
            let evt_dil = spoof_clocks[s_idx].event_clock_dilation();
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &cur_snap, &btc_snap, &epps_trackers[s_idx]);

            let eq_now = ledger.current_equity_1e8();
            let was_sleeping = daily_gov.is_hibernating();
            let p_day = gov.slot_temporal_cadence(0, &btc_snap).intraday_percentile;
            let _ = daily_gov.evaluate_intraday_gate(
                cur_snap.ts_local_ns, eq_now, ledger.sinking_fund_reserve_bps_x100,
                gov.active_maker_rt_fee_bps(), gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(), p_day,
            );
            if !was_sleeping && daily_gov.is_hibernating() { rep.daily_hibernations += 1; }
            let block_new_entries = daily_gov.is_hibernating();

            match engines[s_idx].state {
                OrderLifecycleState::Idle => {
                    if block_new_entries { continue; }
                    let beta_guard = build_monetary_beta_guard_with_epps(eq_now, &engines, &snaps, &epps_trackers, fees);
                    let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&cur_snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
                    let (dir_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
                    let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
                    let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(
                        spec, &cur_snap, &btc_snap, mfe_s, ScaledBps(dir_edge), taker_pm, fees, spoof_d, epps_beta,
                    );
                    match verdict {
                        SignalVerdict::SpoofingVeto => rep.spoof_vetoes += 1,
                        SignalVerdict::AuthorizeMakerEntry { side, .. } => {
                            let warm_dt = cur_snap.ts_local_ns.0.saturating_sub(first_ring_ts_ns);
                            if require_ring_warmup && (first_ring_ts_ns <= 0 || warm_dt < 300_000_000_000) { continue; }
                            let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                            let cand_px = side.select_entry_bbo_price(cur_snap.bids[0].price, cur_snap.asks[0].price);
                            if !spec.passes_quantum_ev_growth_gate(eq_now, cand_px, cur_snap.realized_vol_5m, mfe_s, s_taker_pm, fees) {
                                rep.quantum_ev_vetoes += 1;
                                continue;
                            }
                            if !passes_elliott_macro_coherence_guard(side, &cur_snap)
                                || !passes_order_block_wall_guard_dynamic(s_idx, side, &cur_snap, s_taker_pm, fees)
                                || !can_stage_same_direction_entry(s_idx, side, cur_snap.ts_local_ns, &engines, &snaps)
                            {
                                continue;
                            }
                            let daily_damper_pm = daily_gov.kelly_boundary_damper_permille(
                                ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
                                gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
                            );
                            let lots = gov.compute_slot_entry_lots_v48(s_idx, side, eq_now, &cur_snap, daily_damper_pm);
                            let risk_ppm = slot_position_risk_ppm_with_fees(spec, eq_now, cand_px, lots, cur_snap.realized_vol_5m, fees);
                            let beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(
                                s_idx == 0, cur_snap.realized_vol_5m, ScaledBps(btc_snap.realized_vol_5m.0.max(2_500)),
                            );
                            if beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000)
                                && gov.authorizes_adaptive_entry_v49(s_idx, side, eq_now, daily_damper_pm, mfe_s, &cur_snap, &btc_snap, spoof_d, epps_beta).is_ok()
                            {
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
                    engines[s_idx].sync_governor_dynamics(&gov);
                    match entry_fifo[s_idx].state {
                        SimulatedOrderState::Inactive => {
                            if block_new_entries { engines[s_idx].state = OrderLifecycleState::Idle; continue; }
                            if cur_snap.ts_local_ns.0.saturating_sub(submitted_ts.0) < engines[s_idx].elastic_preflight_window_ns(cur_snap.realized_vol_5m) {
                                continue;
                            }
                            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                            if gov.confirm_preflight_kinematics_v49(s_idx, side, eq_now, qty, mfe_s, &cur_snap, &btc_snap, spoof_d, epps_beta).is_err() {
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                continue;
                            }
                            let Ok((synced_px, _)) = engines[s_idx].sync_preflight_l1_entry_auto(&cur_snap) else {
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
                            engines[s_idx].sync_governor_for_side(&gov, f_side);
                            let _ = engines[s_idx].on_maker_entry_filled_verified(fill_ts_ns, Some(f_side), Some(fill_price), Some(filled_qty));
                            markout_rings[s_idx].arm_fill_probe(f_side, fill_price, mid_now, fill_ts_ns);
                            last_funding_cut[s_idx] = 0;
                            entry_fifo[s_idx].reset();
                            exit_fifo[s_idx].reset();
                            exit_role_latch[s_idx] = ExitOrderRole::None;
                            rep.fills += 1;
                        }
                    }
                }
                OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, resting_since_ts, .. } => {
                    engines[s_idx].sync_governor_dynamics(&gov);
                    match entry_fifo[s_idx].state {
                        SimulatedOrderState::RejectedPostOnlyCross => {
                            rep.post_only_rejects += 1;
                            entry_fifo[s_idx].reset();
                            engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                        SimulatedOrderState::FilledMaker { side: f_side, fill_price, filled_qty, fill_ts_ns } => {
                            gov.clear_abort_streak(s_idx, f_side);
                            engines[s_idx].sync_governor_for_side(&gov, f_side);
                            let _ = engines[s_idx].on_maker_entry_filled_verified(fill_ts_ns, Some(f_side), Some(fill_price), Some(filled_qty));
                            markout_rings[s_idx].arm_fill_probe(f_side, fill_price, mid_now, fill_ts_ns);
                            last_funding_cut[s_idx] = 0;
                            entry_fifo[s_idx].reset();
                            exit_fifo[s_idx].reset();
                            exit_role_latch[s_idx] = ExitOrderRole::None;
                            rep.fills += 1;
                        }
                        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, .. } => {
                            let elapsed_ns = cur_snap.ts_local_ns.0.saturating_sub(resting_since_ts.0);
                            let base_chase_ns = gov.max_total_entry_chase_ns(s_idx, cur_snap.realized_vol_5m);
                            let q_timers = elastic_resting_queue_timers_ns(base_chase_ns);
                            let total_chase_ns = cur_snap.ts_local_ns.0.saturating_sub(armed_ts_latch[s_idx]);
                            let at_l1 = match side { Side::Bid => cur_snap.bids[0].price == entry_price, Side::Ask => cur_snap.asks[0].price == entry_price };
                            let flow_imp = (0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get()) * side.sign_f64();
                            let signed_slow = crate::types::BoundedScore::new_clamped(flow_imp);
                            let dt_s = ((elapsed_ns.max(5_000_000) as f64) * 1e-9).max(0.05);
                            let consumed_q = init_q_ahead[s_idx].0.saturating_sub(queue_ahead_qty.0).max(0);
                            let hwk_lam = evt_dil * (1.0 + 1.618_033_988_749_895 * ((consumed_q as f64) / ((init_q_ahead[s_idx].0.max(1) as f64) * dt_s))).clamp(0.382, 6.854);
                            let max_chase_ns = if at_l1 { gov.elastic_resting_l1_patience_ns_v51(s_idx, side, cur_snap.realized_vol_5m, evt_dil, hwk_lam, signed_slow).max(base_chase_ns) } else { base_chase_ns };
                            let fast_sweep = at_l1 && crate::exec_daemon::evaluate_toxic_sweep_velocity_abort_v51(spec, side, entry_price, init_q_ahead[s_idx], queue_ahead_qty, elapsed_ns, &cur_snap, evt_dil, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees, 75_000);
                            let btc_aligned = epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                            let q_eroded = init_q_ahead[s_idx].0 > 0 && queue_ahead_qty.0 * 100 <= init_q_ahead[s_idx].0 * 65;
                            let headwind_erosion = s_idx > 0 && elapsed_ns >= q_timers.headwind_abort_ns && q_eroded && btc_aligned < -0.08 && flow_imp < 0.15;
                            let macro_contradiction = elapsed_ns >= q_timers.macro_abort_ns
                                && (!passes_elliott_macro_coherence_guard(side, &cur_snap)
                                    || !passes_order_block_wall_guard_dynamic(s_idx, side, &cur_snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees));
                            let chase_expired = (!at_l1 && elapsed_ns >= (base_chase_ns * 3) / 4) || elapsed_ns >= max_chase_ns || total_chase_ns >= max_chase_ns.saturating_add(base_chase_ns / 2);
                            let dq_abort = block_new_entries || fast_sweep || headwind_erosion || macro_contradiction
                                || (elapsed_ns >= 25_000_000 && at_l1 && check_queue_depletion_velocity_abort(side, init_q_ahead[s_idx], queue_ahead_qty, &cur_snap))
                                || flow_imp <= -0.48;
                            if chase_expired || dq_abort {
                                gov.observe_entry_abort(s_idx, side, entry_price, cur_snap.ts_local_ns);
                                let veto_c = if block_new_entries { LastVetoCode::WarmupOrLock } else { LastVetoCode::HawkesStoikovQueueAbort };
                                markout_rings[s_idx].arm_abort_probe(side, entry_price, veto_c, cur_snap.ts_local_ns);
                                entry_fifo[s_idx].reset();
                                engines[s_idx].state = OrderLifecycleState::Idle;
                                rep.queue_aborts += 1;
                            } else if !at_l1
                                && elapsed_ns >= q_timers.repeg_wait_ns
                                && total_chase_ns + q_timers.repeg_wait_ns < max_chase_ns
                                && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, &cur_snap, &btc_snap, epps_beta)
                                && passes_elliott_macro_coherence_guard(side, &cur_snap)
                                && passes_order_block_wall_guard_dynamic(s_idx, side, &cur_snap, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees)
                                && cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                            {
                                let new_l1 = side.select_entry_bbo_price(cur_snap.bids[0].price, cur_snap.asks[0].price);
                                let max_chase_ticks = spec.max_repeg_chase_ticks(entry_price, engines[s_idx].learned_taker_prob_pm, engines[s_idx].active_fees);
                                let moved_away = match side {
                                    Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase_ticks,
                                    Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase_ticks,
                                };
                                let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                                if moved_away && gov.confirm_preflight_kinematics_v49(s_idx, side, eq_now, qty, mfe_s, &cur_snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                                    if let Ok(repeg_sl) = engines[s_idx].compute_repeg_sl_trigger(&cur_snap, new_l1) {
                                        engines[s_idx].next_peg_allowed_ts_ns = cur_snap.ts_local_ns.0 + q_timers.repeg_cooldown_ns;
                                        engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, cur_snap.ts_local_ns);
                                        entry_fifo[s_idx].submit_post_only(side, new_l1, qty, cur_snap.ts_local_ns);
                                    }
                                }
                            }
                        }
                        SimulatedOrderState::InFlightToExchange { .. } | SimulatedOrderState::Inactive => {}
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit { side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price, open_lots, sl_trigger_price, opened_ts, .. } => {
                    engines[s_idx].sync_governor_dynamics(&gov);
                    if let Some(f_1e8) = settle_crossed_funding_boundary(spec, side, open_lots, avg_price_ticks, opened_ts.0, &cur_snap, &mut last_funding_cut[s_idx], &mut ledger) {
                        rep.funding_settlements += 1;
                        let _ = daily_gov.observe_realized_cashflow(
                            cur_snap.ts_local_ns, f_1e8, false, false, false, ledger.current_equity_1e8(),
                            ledger.sinking_fund_reserve_bps_x100, gov.active_maker_rt_fee_bps(),
                            gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
                        );
                    }
                    engines[s_idx].observe_open_flow_kinematics(&cur_snap);
                    let ref_px = if cur_snap.last_trade_price.0 > 0 { cur_snap.last_trade_price } else {
                        match side { Side::Bid => cur_snap.asks[0].price, Side::Ask => cur_snap.bids[0].price }
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
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = engines[s_idx].state {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let avg_d = avg_price_ticks.0.max(1);
                    engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_d;
                    engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64()).saturating_mul(1_000_000) / avg_d;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v49(&cur_snap, &btc_snap, epps_beta, &markout_rings[s_idx], evt_dil);

                    if engines[s_idx].last_eff_sl_ticks.0 <= 0 { engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let eff_sl_px = engines[s_idx].last_eff_sl_ticks;
                    let sl_hit = match side {
                        Side::Bid => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 <= eff_sl_px.0,
                        Side::Ask => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 >= eff_sl_px.0,
                    };
                    let mut closed_opt = None;
                    if sl_hit {
                        let is_chand = engines[s_idx].chandelier_profit_sl_armed
                            || is_chandelier_profit_locked_sl_with_fees(side, avg_price_ticks, eff_sl_px, engines[s_idx].active_fees);
                        engines[s_idx].last_exit_sub_reason = if is_chand { ExitSubReason::ChandelierProfitLockSl } else { ExitSubReason::WaldBaseStopLoss };
                        let sl_slip_px = match side { Side::Bid => PriceTicks((eff_sl_px.0 - 1).max(1)), Side::Ask => PriceTicks(eff_sl_px.0.saturating_add(1)) };
                        exit_fifo[s_idx].reset();
                        closed_opt = engines[s_idx].finalize_open_position_close(ClosedTradeKind::ServerMarkPriceStopLoss, sl_slip_px, cur_snap.ts_local_ns, &mut ledger);
                    } else if let SimulatedOrderState::FilledMaker { fill_price, fill_ts_ns, .. } = exit_fifo[s_idx].state {
                        exit_fifo[s_idx].reset();
                        if matches!(exit_role_latch[s_idx], ExitOrderRole::Tranche1Harvest50) {
                            let _ = engines[s_idx].record_tranche1_harvest(fill_price);
                            engines[s_idx].next_peg_allowed_ts_ns = 0;
                        } else {
                            closed_opt = engines[s_idx].finalize_open_position_close(ClosedTradeKind::MakerPassiveUnwind, fill_price, fill_ts_ns, &mut ledger);
                        }
                    }

                    if let Some(summary) = closed_opt {
                        rep.closed += 1;
                        if summary.is_maker_exit { rep.maker_closes += 1; }
                        let net_r = summary.net_scaled_bps.raw();
                        if net_r < -50 { rep.losses += 1; } else if net_r >= 100 { rep.wins += 1; } else { rep.scratches += 1; }
                        if summary.net_pnl_1e8 >= 0 { rep.gross_profit_1e8 = rep.gross_profit_1e8.saturating_add(summary.net_pnl_1e8); }
                        else { rep.gross_loss_1e8 = rep.gross_loss_1e8.saturating_add(summary.net_pnl_1e8.abs()); }
                        rep.cum_net_bps_x100 = rep.cum_net_bps_x100.saturating_add(net_r);
                        gov.observe_closed_trade_with_regime_v51(
                            s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                            ScaledBps(engines[s_idx].last_closed_mfe_bps),
                            summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100,
                            cur_snap.wave_observables(), evt_dil, cur_snap.ts_local_ns,
                        );
                        let _ = daily_gov.observe_realized_cashflow(
                            cur_snap.ts_local_ns, summary.net_pnl_1e8, true, summary.net_pnl_1e8 > 0, summary.net_pnl_1e8 < 0,
                            ledger.current_equity_1e8(), ledger.sinking_fund_reserve_bps_x100,
                            gov.active_maker_rt_fee_bps(), gov.portfolio_mean_eq_vol_bps(), gov.portfolio_mean_ir_x100(),
                        );
                        if verbose {
                            let net_f = (net_r as f64) / 100.0;
                            let (col, icon, lbl) = classify_pnl_badge(net_f);
                            println!(
                                "  {}{} [REAL PAR2 TRADE #{} {}] {:<10} {:?} | {}t->{}t ({}L) | Net: {:+.2} bps (Maker={}){}",
                                col, icon, rep.closed, lbl, spec.symbol_id.name(), summary.side,
                                summary.entry_price.0, summary.exit_price.0, summary.legs_consolidated, net_f, summary.is_maker_exit, ANSI_RESET
                            );
                        }
                        continue;
                    }
                    if matches!(exit_fifo[s_idx].state, SimulatedOrderState::RejectedPostOnlyCross) {
                        rep.post_only_rejects += 1;
                        exit_fifo[s_idx].reset();
                    }
                    if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&cur_snap) {
                        engines[s_idx].last_closed_wald_pct = engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                        engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                        if quote.defensive_lock_active { engines[s_idx].latch_defensive_floor_if_needed(true); } else if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = engines[s_idx].state { *defensive_floor_locked = false; }
                        let _ = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&cur_snap, &btc_snap, epps_beta, zeta_tox);
                        let (rest_tp, rest_q) = exit_fifo[s_idx].resting_price_and_qty();
                        let (preempt, delay_ns) = compute_exit_sync_schedule_with_fees(&quote, rest_tp, avg_price_ticks, engines[s_idx].active_fees);
                        let peg_ready = preempt || cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns;
                        if rest_tp.0 == 0 || rest_q != quote.order_lots
                            || (peg_ready && should_repeg_resting_exit_tp(
                                spec, side, avg_price_ticks, rest_tp, rest_q, quote.target_price,
                                quote.order_lots, quote.urgent_close_repeg, quote.cur_gross_bps, engines[s_idx].active_fees,
                            ))
                        {
                            engines[s_idx].next_peg_allowed_ts_ns = cur_snap.ts_local_ns.0 + delay_ns;
                            exit_role_latch[s_idx] = quote.role;
                            exit_fifo[s_idx].submit_post_only(side.opposite(), quote.target_price, quote.order_lots, cur_snap.ts_local_ns);
                            if let OrderLifecycleState::PositionOpenMakerExit { resting_exit_price: ref mut rx_p, .. } = engines[s_idx].state {
                                *rx_p = quote.target_price;
                            }
                        }
                    }
                }
            }
        }
    }

    let (mut lvr_sum, mut lvr_cnt) = (0_i64, 0_i64);
    for mr in &markout_rings {
        rep.tp_aborts_verified += mr.true_positive_aborts().0;
        if mr.ewma_lvr_5s_bps() != 0 {
            lvr_sum += mr.ewma_lvr_5s_bps();
            lvr_cnt += 1;
        }
    }
    rep.ewma_lvr_5s_bps = if lvr_cnt > 0 { lvr_sum / lvr_cnt } else { 0 };
    rep.cum_net_pnl_1e8 = ledger.cumulative_net_pnl_1e8;
    if verbose { print_walk_forward_summary(&rep); }
    rep
}

pub fn print_walk_forward_summary(rep: &WalkForwardReport) {
    let mkr_pct = if rep.closed > 0 { (rep.maker_closes as f64 * 100.0) / (rep.closed as f64) } else { 0.0 };
    let pf_str = if rep.closed == 0 { "N/A (0 closed)".to_string() }
        else if rep.gross_loss_1e8 == 0 { "INF (0 losses)".to_string() }
        else { format!("{:.2}", (rep.gross_profit_1e8 as f64) / (rep.gross_loss_1e8 as f64)) };
    println!(
        "  -> Live PAR1/PAR2 Archives   : {} files | {} events ({} L2 / {} Trades / {} Tkr / {} Liq / {} Fnd)",
        rep.files_count, rep.total_events, rep.l2_events, rep.trade_events,
        rep.par2_ticker_events, rep.par2_liq_events, rep.funding_settlements
    );
    println!(
        "  -> Live Walk-Forward Replay  : {ANSI_ORANGE}Spoof={} QEv={} DaySlp={} | {ANSI_YELLOW}Arm={} | Abr={}(TP:{}) | Rej={} | Fil={} | {ANSI_GREEN}Cls={} (W:{}/S:{}/L:{}) | LVR5s={:+.2}bp{ANSI_RESET}",
        rep.spoof_vetoes, rep.quantum_ev_vetoes, rep.daily_hibernations, rep.armed, rep.queue_aborts, rep.tp_aborts_verified, rep.post_only_rejects,
        rep.fills, rep.closed, rep.wins, rep.scratches, rep.losses, (rep.ewma_lvr_5s_bps as f64) / 100.0
    );
    let (wf_col, wf_ico) = if rep.cum_net_bps_x100 >= 0 && rep.losses == 0 { (ANSI_GREEN, "\u{1f7e2}") } else { (ANSI_RED, "\u{1f534}") };
    println!(
        "  {wf_col}{wf_ico} -> Live Walk-Forward PnL      : Net={:+.2} bps ({:+.4} USDT) | MakerExit={mkr_pct:.2}% | PF={pf_str}{ANSI_RESET}",
        (rep.cum_net_bps_x100 as f64) / 100.0, (rep.cum_net_pnl_1e8 as f64) / 100_000_000.0
    );
}

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

    use super::{settle_crossed_funding_boundary, step_par2_observables_on_row};
    use crate::alpha_3factor::{
        EppsBtcCorrelationTracker, EventClockAlphaModulator, OrthogonalFactorState, SignalVerdict,
    };
    use crate::exec_daemon::{
        build_monetary_beta_guard_with_epps, can_stage_same_direction_entry,
        compute_exit_sync_schedule_with_fees, elastic_resting_queue_timers_ns,
        slot_position_risk_ppm_with_fees,
    };
    use crate::execution_fsm::{
        is_chandelier_profit_locked_sl_with_fees, passes_elliott_macro_coherence_guard,
        passes_order_block_wall_guard_dynamic, passes_stoikov_repeg_coherence_guard_with_beta,
        ClosedTradeKind, ExitOrderRole, ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState,
        SlotExecutionEngine,
    };
    use crate::fifo_sim::{
        run_200k_fifo_benchmark, run_quadruple_omega_ablation_suite, OmegaConservativeFifoOrder,
        OmegaFifoLifecycle,
    };
    use crate::forensic_parquet::{ColumnarMarketRow, MarketEventKind, ParitySyncLedger};
    use crate::governor_plasticity::DailySessionGovernor;
    use crate::lob_shm::{BookLevel, LastVetoCode, SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
    use crate::markout_tracker::SlotMarkoutRing;
    use crate::order_block::CausalOrderBlockEngine;
    use crate::telemetry_hud::DEFAULT_WALLET_EQUITY_1E8;
    use crate::types::{
        print_auto_aligned_box, BoundedScore, LotQty, PriceTicks, ScaledBps, Side, TimestampNs,
        ANSI_GREEN, ANSI_RESET, ANSI_SKY_BLUE, ANSI_YELLOW, APPROVED_SPECS,
    };
    use crate::wave_elliott::{CausalElliottWaveState, WaveObservables, WaveRegime};

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
        pub funding_settlements: u64,
        pub cum_net_bps_x100: i64,
        pub cum_net_pnl_1e8: i64,
        pub parity_sync_drift_1e8: i64,
    }

    #[derive(Debug)]
    pub struct OmegaDigitalTwinEngine {
        pub engines: [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
        pub gov: OnlineAdaptiveGovernor,
        pub daily_gov: DailySessionGovernor,
        pub ledger: ParitySyncLedger,
        pub entry_fifo: [OmegaConservativeFifoOrder; MAX_ACTIVE_PAIRS],
        pub exit_fifo: [OmegaConservativeFifoOrder; MAX_ACTIVE_PAIRS],
        pub cached_snaps: [SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
        pub wave_states: [CausalElliottWaveState; MAX_ACTIVE_PAIRS],
        pub ob_engines: [CausalOrderBlockEngine; MAX_ACTIVE_PAIRS],
        pub markout_rings: [SlotMarkoutRing; MAX_ACTIVE_PAIRS],
        pub epps_trackers: [EppsBtcCorrelationTracker; MAX_ACTIVE_PAIRS],
        pub spoof_clocks: [EventClockAlphaModulator; MAX_ACTIVE_PAIRS],
        pub last_funding_cut: [i64; MAX_ACTIVE_PAIRS],
        pub report: OmegaTwinWalkForwardReport,
        sum_trade_pnl_1e8: i64,
    }

    impl Default for OmegaDigitalTwinEngine {
        #[inline]
        fn default() -> Self { Self::new() }
    }

    impl OmegaDigitalTwinEngine {
        #[must_use]
        pub fn new() -> Self {
            Self {
                engines: SlotExecutionEngine::new_12_slot_array(),
                gov: OnlineAdaptiveGovernor::new(),
                daily_gov: DailySessionGovernor::new(DEFAULT_WALLET_EQUITY_1E8),
                ledger: ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8),
                entry_fifo: [OmegaConservativeFifoOrder::new(); MAX_ACTIVE_PAIRS],
                exit_fifo: [OmegaConservativeFifoOrder::new(); MAX_ACTIVE_PAIRS],
                cached_snaps: [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS],
                wave_states: [CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS],
                ob_engines: [CausalOrderBlockEngine::new(); MAX_ACTIVE_PAIRS],
                markout_rings: [SlotMarkoutRing::new(); MAX_ACTIVE_PAIRS],
                epps_trackers: [EppsBtcCorrelationTracker::new(); MAX_ACTIVE_PAIRS],
                spoof_clocks: [EventClockAlphaModulator::new(); MAX_ACTIVE_PAIRS],
                last_funding_cut: [0; MAX_ACTIVE_PAIRS],
                report: OmegaTwinWalkForwardReport::default(),
                sum_trade_pnl_1e8: 0,
            }
        }

        pub fn ingest_par2_row(&mut self, row: &ColumnarMarketRow) {
            let Some(s_idx) = row.symbol_id.slot_index() else { return; };
            let mut snap = self.cached_snaps[s_idx];
            if snap.symbol_id.0 == 0 {
                snap.symbol_id = APPROVED_SPECS[s_idx].symbol_id;
                snap.is_eligible = true;
            }
            let fees = self.gov.active_fee_schedule();
            let slot_tkr_pm = i64::midpoint(
                self.gov.slot_taker_prob_permille(s_idx, Side::Bid),
                self.gov.slot_taker_prob_permille(s_idx, Side::Ask),
            );
            step_par2_observables_on_row(
                s_idx, row, &mut snap, &mut self.wave_states[s_idx],
                &mut self.ob_engines[s_idx], &mut self.markout_rings[s_idx], fees, slot_tkr_pm,
            );
            self.ingest_event(&TwinReplayEvent {
                slot_idx: s_idx,
                kind: row.par2_kind.legacy_event_kind(),
                aggressor_side: row.side,
                trade_price: row.price_ticks,
                trade_lots: row.lot_qty,
                snap,
            });
        }

        pub fn ingest_event(&mut self, ev: &TwinReplayEvent) {
            let s_idx = ev.slot_idx.min(MAX_ACTIVE_PAIRS - 1);
            self.report.par1_rows_verified = self.report.par1_rows_verified.saturating_add(1);
            self.cached_snaps[s_idx] = ev.snap;
            let fees = self.gov.active_fee_schedule();
            self.ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
            let _ = self.spoof_clocks[s_idx].step_event_clock_and_spoof(ev.snap.ts_local_ns, ev.snap.factor_ofi, ev.snap.factor_tfi);
            self.gov.observe_market_price(s_idx, &ev.snap);
            let mid_now = PriceTicks(i64::midpoint(ev.snap.bids[0].price.0.max(1), ev.snap.asks[0].price.0.max(1)));
            self.markout_rings[s_idx].step_clock(mid_now, ev.snap.ts_local_ns, fees.maker_fee_bps.raw());
            let btc_snap = if self.cached_snaps[0].bids[0].price.0 > 0 { self.cached_snaps[0] } else { ev.snap };
            let mid_btc = PriceTicks(i64::midpoint(btc_snap.bids[0].price.0.max(1), btc_snap.asks[0].price.0.max(1)));
            self.epps_trackers[s_idx].observe_5s_pair(ev.snap.ts_local_ns, s_idx == 0, mid_now, mid_btc);
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &ev.snap, &btc_snap, &self.epps_trackers[s_idx]);
            let spoof_d = self.spoof_clocks[s_idx].spoofing_density();
            let evt_dil = self.spoof_clocks[s_idx].event_clock_dilation();
            let now_ns = ev.snap.ts_local_ns;
            let spec = APPROVED_SPECS[s_idx];

            let eq_now = self.ledger.current_equity_1e8();
            let p_day = self.gov.slot_temporal_cadence(0, &btc_snap).intraday_percentile;
            let _ = self.daily_gov.evaluate_intraday_gate(
                now_ns, eq_now, self.ledger.sinking_fund_reserve_bps_x100,
                self.gov.active_maker_rt_fee_bps(), self.gov.portfolio_mean_eq_vol_bps(),
                self.gov.portfolio_mean_ir_x100(), p_day,
            );
            let block_new_entries = self.daily_gov.is_hibernating();

            let (mut entry_fill_opt, mut exit_fill_opt, mut opp_tape_lots) = (None, None, LotQty(0));
            match ev.kind {
                MarketEventKind::L2BookSnapshot => {
                    self.entry_fifo[s_idx].on_lob_snapshot(&ev.snap);
                    self.exit_fifo[s_idx].on_lob_snapshot(&ev.snap);
                }
                MarketEventKind::PublicTradePrint => {
                    if let OmegaFifoLifecycle::RestingInFifoQueue { side, limit_price, .. } = self.entry_fifo[s_idx].state {
                        let hits = match side {
                            Side::Bid => ev.aggressor_side == Side::Ask && ev.trade_price.0 <= limit_price.0,
                            Side::Ask => ev.aggressor_side == Side::Bid && ev.trade_price.0 >= limit_price.0,
                        };
                        if hits { opp_tape_lots = ev.trade_lots; }
                    }
                    entry_fill_opt = self.entry_fifo[s_idx].on_trade_print(ev.aggressor_side, ev.trade_price, ev.trade_lots, now_ns.0);
                    exit_fill_opt = self.exit_fifo[s_idx].on_trade_print(ev.aggressor_side, ev.trade_price, ev.trade_lots, now_ns.0);
                }
            }

            match self.engines[s_idx].state {
                OrderLifecycleState::Idle => {
                    if block_new_entries { return; }
                    let beta_guard = build_monetary_beta_guard_with_epps(eq_now, &self.engines, &self.cached_snaps, &self.epps_trackers, fees);
                    let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&ev.snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
                    let (probe_edge, _, _) = self.gov.slot_direction_diagnostics(s_idx, probe_side);
                    let taker_pm = self.gov.slot_taker_prob_permille(s_idx, probe_side);
                    let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_v46(
                        spec, &ev.snap, &btc_snap, mfe_s, ScaledBps(probe_edge), taker_pm, fees, spoof_d, epps_beta,
                    );
                    if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
                        let s_taker_pm = self.gov.slot_taker_prob_permille(s_idx, side);
                        let cand_px = side.select_entry_bbo_price(ev.snap.bids[0].price, ev.snap.asks[0].price);
                        if !spec.passes_quantum_ev_growth_gate(eq_now, cand_px, ev.snap.realized_vol_5m, mfe_s, s_taker_pm, fees)
                            || !passes_elliott_macro_coherence_guard(side, &ev.snap)
                            || !passes_order_block_wall_guard_dynamic(s_idx, side, &ev.snap, s_taker_pm, fees)
                            || !can_stage_same_direction_entry(s_idx, side, now_ns, &self.engines, &self.cached_snaps)
                        {
                            return;
                        }
                        let daily_damper_pm = self.daily_gov.kelly_boundary_damper_permille(
                            self.ledger.sinking_fund_reserve_bps_x100, self.gov.active_maker_rt_fee_bps(),
                            self.gov.portfolio_mean_eq_vol_bps(), self.gov.portfolio_mean_ir_x100(),
                        );
                        let lots = self.gov.compute_slot_entry_lots_v48(s_idx, side, eq_now, &ev.snap, daily_damper_pm);
                        let risk_ppm = slot_position_risk_ppm_with_fees(spec, eq_now, cand_px, lots, ev.snap.realized_vol_5m, fees);
                        let beta_x1000 = self.epps_trackers[s_idx].correlated_beta_x1000(
                            s_idx == 0, ev.snap.realized_vol_5m, ScaledBps(btc_snap.realized_vol_5m.0.max(2_500)),
                        );
                        if beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000)
                            && self.gov.authorizes_adaptive_entry_v49(s_idx, side, eq_now, daily_damper_pm, mfe_s, &ev.snap, &btc_snap, spoof_d, epps_beta).is_ok()
                        {
                            self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                            let _ = self.engines[s_idx].try_arm_maker_entry(&ev.snap, verdict, lots);
                        }
                    }
                }
                OrderLifecycleState::PlacingEntryMaker { side, qty, submitted_ts, .. } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    if block_new_entries { self.engines[s_idx].state = OrderLifecycleState::Idle; return; }
                    if now_ns.0.saturating_sub(submitted_ts.0) >= self.engines[s_idx].elastic_preflight_window_ns(ev.snap.realized_vol_5m) {
                        let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                        if self.gov.confirm_preflight_kinematics_v49(s_idx, side, eq_now, match self.engines[s_idx].state { OrderLifecycleState::PlacingEntryMaker { qty: q, .. } => q, _ => LotQty(1) }, mfe_s, &ev.snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                            if let Ok((synced_px, _)) = self.engines[s_idx].sync_preflight_l1_entry_auto(&ev.snap) {
                                self.entry_fifo[s_idx].submit_post_only(side, synced_px, qty, now_ns.0);
                                let _ = self.engines[s_idx].on_entry_order_resting(now_ns);
                            } else {
                                self.engines[s_idx].state = OrderLifecycleState::Idle;
                            }
                        } else {
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                    }
                }
                OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, resting_since_ts, .. } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    if let Some((fill_px, fill_lots)) = entry_fill_opt {
                        self.entry_fifo[s_idx].cancel();
                        self.gov.clear_abort_streak(s_idx, side);
                        self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                        let _ = self.engines[s_idx].on_maker_entry_filled_verified(now_ns, Some(side), Some(fill_px), Some(fill_lots));
                        self.markout_rings[s_idx].arm_fill_probe(side, fill_px, mid_now, now_ns);
                        self.last_funding_cut[s_idx] = 0;
                        if let Some(q0) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap) {
                            self.exit_fifo[s_idx].submit_post_only(side.opposite(), q0.target_price, q0.order_lots, now_ns.0);
                            if let OrderLifecycleState::PositionOpenMakerExit {
                                active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p,
                                active_exit_lots: ref mut ax_l, resting_exit_price: ref mut rx_p, ..
                            } = self.engines[s_idx].state {
                                *ax_r = q0.role; *ax_p = q0.target_price; *ax_l = q0.order_lots; *rx_p = q0.target_price;
                            }
                        }
                        return;
                    }
                    if matches!(self.entry_fifo[s_idx].state, OmegaFifoLifecycle::PostOnlyRejected { .. }) {
                        self.entry_fifo[s_idx].cancel();
                        self.engines[s_idx].state = OrderLifecycleState::Idle;
                        return;
                    }
                    if let OmegaFifoLifecycle::RestingInFifoQueue { init_queue_ahead, queue_ahead_lots, .. } = self.entry_fifo[s_idx].state {
                        let q_verdict = self.engines[s_idx].evaluate_resting_queue_shield_with_beta(
                            side, init_queue_ahead, queue_ahead_lots, opp_tape_lots, &ev.snap, &btc_snap, epps_beta,
                        );
                        let elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                        let max_chase_ns = self.gov.max_total_entry_chase_ns(s_idx, ev.snap.realized_vol_5m);
                        let q_timers = elastic_resting_queue_timers_ns(max_chase_ns);
                        let macro_contradiction = elapsed_ns >= q_timers.macro_abort_ns
                            && (!passes_elliott_macro_coherence_guard(side, &ev.snap)
                                || !passes_order_block_wall_guard_dynamic(s_idx, side, &ev.snap, self.engines[s_idx].learned_taker_prob_pm, self.engines[s_idx].active_fees));
                        let at_l1 = match side { Side::Bid => ev.snap.bids[0].price == entry_price, Side::Ask => ev.snap.asks[0].price == entry_price };
                        let chase_expired = (!at_l1 && elapsed_ns >= (max_chase_ns * 3) / 4) || elapsed_ns >= max_chase_ns;
                        if block_new_entries || chase_expired || (elapsed_ns >= 25_000_000 && q_verdict.abort_now) || macro_contradiction {
                            self.entry_fifo[s_idx].cancel();
                            self.gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                            let veto_c = if block_new_entries { LastVetoCode::WarmupOrLock } else { LastVetoCode::HawkesStoikovQueueAbort };
                            self.markout_rings[s_idx].arm_abort_probe(side, entry_price, veto_c, now_ns);
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                            self.report.hawkes_queue_aborts += 1;
                            return;
                        }
                        if !at_l1
                            && elapsed_ns >= q_timers.repeg_wait_ns
                            && now_ns.0 >= self.engines[s_idx].next_peg_allowed_ts_ns
                            && passes_elliott_macro_coherence_guard(side, &ev.snap)
                            && passes_order_block_wall_guard_dynamic(s_idx, side, &ev.snap, self.engines[s_idx].learned_taker_prob_pm, self.engines[s_idx].active_fees)
                            && passes_stoikov_repeg_coherence_guard_with_beta(s_idx, side, &ev.snap, &btc_snap, epps_beta)
                        {
                            let new_l1 = side.select_entry_bbo_price(ev.snap.bids[0].price, ev.snap.asks[0].price);
                            let max_chase = spec.max_repeg_chase_ticks(entry_price, self.engines[s_idx].learned_taker_prob_pm, self.engines[s_idx].active_fees);
                            let moved_away = match side {
                                Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= max_chase,
                                Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= max_chase,
                            };
                            let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                            if moved_away && self.gov.confirm_preflight_kinematics_v49(s_idx, side, eq_now, match self.engines[s_idx].state { OrderLifecycleState::RestingEntryQueue { qty: q, .. } => q, _ => LotQty(1) }, mfe_s, &ev.snap, &btc_snap, spoof_d, epps_beta).is_ok() {
                                if let Ok(repeg_sl) = self.engines[s_idx].compute_repeg_sl_trigger(&ev.snap, new_l1) {
                                    self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + q_timers.repeg_cooldown_ns;
                                    self.engines[s_idx].apply_resting_repeg(new_l1, repeg_sl, now_ns);
                                    self.entry_fifo[s_idx].submit_post_only(side, new_l1, qty, now_ns.0);
                                }
                            }
                        }
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price,
                    open_lots, resting_exit_price, sl_trigger_price, opened_ts, ..
                } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    if let Some(f_pnl) = settle_crossed_funding_boundary(
                        spec, side, open_lots, avg_price_ticks, opened_ts.0, &ev.snap,
                        &mut self.last_funding_cut[s_idx], &mut self.ledger,
                    ) {
                        self.report.funding_settlements += 1;
                        self.report.cum_net_pnl_1e8 = self.report.cum_net_pnl_1e8.saturating_add(f_pnl);
                        self.sum_trade_pnl_1e8 = self.sum_trade_pnl_1e8.saturating_add(f_pnl);
                        self.report.parity_sync_drift_1e8 = (self.ledger.cumulative_net_pnl_1e8 - self.sum_trade_pnl_1e8).abs();
                        let _ = self.daily_gov.observe_realized_cashflow(
                            now_ns, f_pnl, false, false, false, self.ledger.current_equity_1e8(),
                            self.ledger.sinking_fund_reserve_bps_x100, self.gov.active_maker_rt_fee_bps(),
                            self.gov.portfolio_mean_eq_vol_bps(), self.gov.portfolio_mean_ir_x100(),
                        );
                    }
                    self.engines[s_idx].observe_open_flow_kinematics(&ev.snap);
                    let ref_px = if ev.snap.last_trade_price.0 > 0 { ev.snap.last_trade_price } else { ev.snap.mark_price };
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
                    if let OrderLifecycleState::PositionOpenMakerExit { peak_favorable_price: ref mut pk, worst_adverse_price: ref mut wr, .. } = self.engines[s_idx].state {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let d_sign = side.sign_i64();
                    let denom = avg_price_ticks.0.max(1);
                    self.engines[s_idx].last_closed_mfe_bps = ((peak_favorable_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    self.engines[s_idx].last_closed_mae_bps = ((worst_adverse_price.0 - avg_price_ticks.0) * d_sign).saturating_mul(1_000_000) / denom;
                    self.engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    self.engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                    let zeta_tox = self.engines[s_idx].observe_post_fill_toxicity_and_wald_v49(&ev.snap, &btc_snap, epps_beta, &self.markout_rings[s_idx], evt_dil);

                    if self.engines[s_idx].last_eff_sl_ticks.0 <= 0 { self.engines[s_idx].last_eff_sl_ticks = sl_trigger_price; }
                    let eff_sl = self.engines[s_idx].last_eff_sl_ticks;
                    let sl_crossed = match side {
                        Side::Bid => ev.snap.mark_price.0 > 0 && ev.snap.mark_price.0 <= eff_sl.0,
                        Side::Ask => ev.snap.mark_price.0 > 0 && ev.snap.mark_price.0 >= eff_sl.0,
                    };
                    if sl_crossed {
                        self.exit_fifo[s_idx].cancel();
                        let is_chand = self.engines[s_idx].chandelier_profit_sl_armed
                            || is_chandelier_profit_locked_sl_with_fees(side, avg_price_ticks, eff_sl, self.engines[s_idx].active_fees);
                        self.engines[s_idx].last_exit_sub_reason = if is_chand {
                            self.report.chandelier_closes += 1;
                            ExitSubReason::ChandelierProfitLockSl
                        } else {
                            ExitSubReason::WaldBaseStopLoss
                        };
                        if let Some(summary) = self.engines[s_idx].finalize_open_position_close(
                            ClosedTradeKind::ServerMarkPriceStopLoss, eff_sl, now_ns, &mut self.ledger,
                        ) {
                            let non_pun = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                            self.gov.observe_closed_trade_with_regime_v51(
                                s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                                ScaledBps(self.engines[s_idx].last_closed_mfe_bps), non_pun,
                                ev.snap.wave_observables(), evt_dil, now_ns,
                            );
                            let _ = self.daily_gov.observe_realized_cashflow(
                                now_ns, summary.net_pnl_1e8, true, summary.net_pnl_1e8 > 0, summary.net_pnl_1e8 < 0,
                                self.ledger.current_equity_1e8(), self.ledger.sinking_fund_reserve_bps_x100,
                                self.gov.active_maker_rt_fee_bps(), self.gov.portfolio_mean_eq_vol_bps(), self.gov.portfolio_mean_ir_x100(),
                            );
                            self.record_summary(&summary);
                        }
                        return;
                    }
                    if let Some((fill_px, _)) = exit_fill_opt {
                        self.exit_fifo[s_idx].cancel();
                        let cur_role = match self.engines[s_idx].state {
                            OrderLifecycleState::PositionOpenMakerExit { active_exit_role, .. } => active_exit_role,
                            _ => ExitOrderRole::StandardMakerExit100,
                        };
                        if matches!(cur_role, ExitOrderRole::Tranche1Harvest50) && self.engines[s_idx].record_tranche1_harvest(fill_px) {
                            if let Some(q2) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap) {
                                self.engines[s_idx].last_exit_sub_reason = q2.sub_reason;
                                self.exit_fifo[s_idx].submit_post_only(side.opposite(), q2.target_price, q2.order_lots, now_ns.0);
                                if let OrderLifecycleState::PositionOpenMakerExit {
                                    active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p,
                                    active_exit_lots: ref mut ax_l, resting_exit_price: ref mut rx_p, ..
                                } = self.engines[s_idx].state {
                                    *ax_r = q2.role; *ax_p = q2.target_price; *ax_l = quote_lots_safe(q2.order_lots); *rx_p = q2.target_price;
                                }
                                self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_350_000_000;
                            }
                            return;
                        }
                        if matches!(self.engines[s_idx].last_exit_sub_reason, ExitSubReason::ShadowRunnerCrestHarvest) {
                            self.report.shadow_crest_snaps += 1;
                        } else if matches!(self.engines[s_idx].last_exit_sub_reason, ExitSubReason::FibonacciImpulseSurf) {
                            self.report.fibonacci_surfs += 1;
                        }
                        if let Some(summary) = self.engines[s_idx].finalize_open_position_close(
                            ClosedTradeKind::MakerPassiveUnwind, fill_px, now_ns, &mut self.ledger,
                        ) {
                            self.gov.observe_closed_trade_with_regime_v51(
                                s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                                ScaledBps(self.engines[s_idx].last_closed_mfe_bps), true,
                                ev.snap.wave_observables(), evt_dil, now_ns,
                            );
                            let _ = self.daily_gov.observe_realized_cashflow(
                                now_ns, summary.net_pnl_1e8, true, summary.net_pnl_1e8 > 0, summary.net_pnl_1e8 < 0,
                                self.ledger.current_equity_1e8(), self.ledger.sinking_fund_reserve_bps_x100,
                                self.gov.active_maker_rt_fee_bps(), self.gov.portfolio_mean_eq_vol_bps(), self.gov.portfolio_mean_ir_x100(),
                            );
                            self.record_summary(&summary);
                        }
                        return;
                    }
                    let mut eff_rest_exit = resting_exit_price;
                    if matches!(self.exit_fifo[s_idx].state, OmegaFifoLifecycle::PostOnlyRejected { .. }) {
                        self.exit_fifo[s_idx].cancel();
                        eff_rest_exit = PriceTicks(0);
                    }
                    let Some(quote) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap) else { return; };
                    self.engines[s_idx].last_closed_wald_pct = self.engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                    if quote.defensive_lock_active { self.engines[s_idx].latch_defensive_floor_if_needed(true); } else if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.engines[s_idx].state { *defensive_floor_locked = false; }
                    let _ = self.engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&ev.snap, &btc_snap, epps_beta, zeta_tox);
                    self.engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                    let (preempt, delay_ns) = compute_exit_sync_schedule_with_fees(&quote, eff_rest_exit, avg_price_ticks, self.engines[s_idx].active_fees);
                    if eff_rest_exit != quote.target_price && (preempt || now_ns.0 >= self.engines[s_idx].next_peg_allowed_ts_ns) {
                        self.exit_fifo[s_idx].submit_post_only(side.opposite(), quote.target_price, quote.order_lots, now_ns.0);
                        self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + delay_ns;
                        if let OrderLifecycleState::PositionOpenMakerExit {
                            active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p,
                            active_exit_lots: ref mut ax_l, resting_exit_price: ref mut rx_p, ..
                        } = self.engines[s_idx].state {
                            *ax_r = quote.role; *ax_p = quote.target_price; *ax_l = quote.order_lots; *rx_p = quote.target_price;
                        }
                    }
                }
            }
        }

        fn record_summary(&mut self, s: &crate::execution_fsm::ClosedTradeSummary) {
            self.report.closed_trades += 1;
            if s.is_maker_exit { self.report.maker_closes += 1; }
            self.report.cum_net_bps_x100 += s.net_scaled_bps.raw();
            self.report.cum_net_pnl_1e8 += s.net_pnl_1e8;
            self.sum_trade_pnl_1e8 += s.net_pnl_1e8;
            self.report.parity_sync_drift_1e8 = (self.ledger.cumulative_net_pnl_1e8 - self.sum_trade_pnl_1e8).abs();
        }

        #[must_use]
        pub fn finalize_report(&mut self) -> OmegaTwinWalkForwardReport {
            let mut rej = 0_u64;
            for s in 0..MAX_ACTIVE_PAIRS {
                rej += self.entry_fifo[s].post_only_rejects + self.exit_fifo[s].post_only_rejects;
            }
            self.report.post_only_rejects = rej;
            self.report.parity_sync_drift_1e8 = (self.ledger.cumulative_net_pnl_1e8 - self.sum_trade_pnl_1e8).abs();
            self.report
        }
    }

    #[inline]
    const fn quote_lots_safe(lots: LotQty) -> LotQty {
        if lots.0 > 0 { lots } else { LotQty(1) }
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

    #[must_use]
    pub fn run_omega_digital_twin_walk_forward_lab() -> OmegaTwinWalkForwardReport {
        let mut twin = OmegaDigitalTwinEngine::new();
        let t0 = 1_700_058_600_000_000_000_i64;
        let btc_base = base_snap(0, 759_000, 450, 320, t0);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 0, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(759_000), trade_lots: LotQty(1), snap: btc_base });

        let mut avax = base_snap(3, 8_200, 120, 180, t0 + 10_000_000);
        avax.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.66),
            macro_phase: BoundedScore::new_clamped(0.85), oi_impulse: BoundedScore::new_clamped(0.45),
            liq_imbalance: BoundedScore::new_clamped(0.60), fib_proj_bps: ScaledBps(3_000),
        });
        for dt in [10_000_000_i64, 45_000_000, 80_000_000] {
            avax.ts_local_ns = TimestampNs(t0 + dt);
            twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_200), trade_lots: LotQty(1), snap: avax });
        }
        avax.ts_local_ns = TimestampNs(t0 + 110_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Ask, trade_price: PriceTicks(8_200), trade_lots: LotQty(150), snap: avax });
        avax.ts_local_ns = TimestampNs(t0 + 160_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_204), qty: LotQty(380) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(80) };
        avax.mark_price = PriceTicks(8_205);
        avax.last_trade_price = PriceTicks(8_205);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_205), trade_lots: LotQty(1), snap: avax });
        avax.ts_local_ns = TimestampNs(t0 + 200_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Bid, trade_price: PriceTicks(8_205), trade_lots: LotQty(120), snap: avax });
        avax.ts_local_ns = TimestampNs(t0 + 1_200_000_000);
        avax.bids[0] = BookLevel { price: PriceTicks(8_229), qty: LotQty(390) };
        avax.asks[0] = BookLevel { price: PriceTicks(8_230), qty: LotQty(70) };
        avax.mark_price = PriceTicks(8_230);
        avax.last_trade_price = PriceTicks(8_230);
        avax.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave3, climax_active: false, hurst: WaveObservables::encode_hurst(0.66),
            macro_phase: BoundedScore::new_clamped(0.88), oi_impulse: BoundedScore::new_clamped(0.40),
            liq_imbalance: BoundedScore::new_clamped(0.55), fib_proj_bps: ScaledBps(4),
        });
        twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_230), trade_lots: LotQty(1), snap: avax });
        avax.ts_local_ns = TimestampNs(t0 + 1_260_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 3, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Bid, trade_price: PriceTicks(8_230), trade_lots: LotQty(110), snap: avax });

        let t2 = t0 + 10_000_000_000;
        let mut link = base_snap(9, 10_720, 500, 320, t2);
        for dt in [0_i64, 35_000_000, 70_000_000] {
            link.ts_local_ns = TimestampNs(t2 + dt);
            twin.ingest_event(&TwinReplayEvent { slot_idx: 9, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(10_720), trade_lots: LotQty(1), snap: link });
        }
        for k in 1..=5_i64 {
            link.ts_local_ns = TimestampNs(t2 + 70_000_000 + k * 20_000_000);
            link.bids[0] = BookLevel { price: PriceTicks(10_720), qty: LotQty(500 - k * 75) };
            link.asks[0] = BookLevel { price: PriceTicks(10_721), qty: LotQty(410) };
            link.deep_bid_lots_l1_l5 = LotQty(360);
            link.deep_ask_lots_l1_l5 = LotQty(1_300);
            link.factor_ofi = BoundedScore::new_clamped(0.14);
            link.factor_tfi = BoundedScore::new_clamped(0.10);
            twin.ingest_event(&TwinReplayEvent { slot_idx: 9, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Ask, trade_price: PriceTicks(10_720), trade_lots: LotQty(75), snap: link });
        }

        let t3 = t0 + 25_000_000_000;
        let mut eth = base_snap(1, 223_700, 100, 180, t3);
        eth.set_wave_observables(WaveObservables {
            regime: WaveRegime::NeutralWarmup, climax_active: false, hurst: WaveObservables::encode_hurst(0.60),
            macro_phase: BoundedScore::ZERO, oi_impulse: BoundedScore::ZERO, liq_imbalance: BoundedScore::ZERO, fib_proj_bps: ScaledBps(0),
        });
        for dt in [0_i64, 35_000_000, 70_000_000] {
            eth.ts_local_ns = TimestampNs(t3 + dt);
            twin.ingest_event(&TwinReplayEvent { slot_idx: 1, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(223_700), trade_lots: LotQty(1), snap: eth });
        }
        eth.ts_local_ns = TimestampNs(t3 + 95_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 1, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Ask, trade_price: PriceTicks(223_700), trade_lots: LotQty(130), snap: eth });
        for k in 1..=8_i64 {
            let px = 223_850 + k * 115;
            eth.ts_local_ns = TimestampNs(t3 + 95_000_000 + k * 50_000_000);
            eth.bids[0] = BookLevel { price: PriceTicks(px - 1), qty: LotQty(360) };
            eth.asks[0] = BookLevel { price: PriceTicks(px), qty: LotQty(180) };
            eth.mark_price = PriceTicks(px);
            eth.last_trade_price = PriceTicks(px);
            eth.factor_ofi = BoundedScore::new_clamped(0.62 + 0.03 * (k as f64));
            eth.factor_tfi = BoundedScore::new_clamped(0.66 + 0.03 * (k as f64));
            twin.ingest_event(&TwinReplayEvent { slot_idx: 1, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(px), trade_lots: LotQty(1), snap: eth });
        }
        for m in 1..=3_i64 {
            eth.ts_local_ns = TimestampNs(t3 + 500_000_000 + m * 50_000_000);
            eth.bids[0] = BookLevel { price: PriceTicks(224_769), qty: LotQty(240) };
            eth.asks[0] = BookLevel { price: PriceTicks(224_770), qty: LotQty(60) };
            eth.mark_price = PriceTicks(224_770);
            eth.last_trade_price = PriceTicks(224_770);
            eth.factor_ofi = BoundedScore::new_clamped(0.04);
            eth.factor_tfi = BoundedScore::new_clamped(0.03);
            eth.factor_geom = BoundedScore::ZERO;
            twin.ingest_event(&TwinReplayEvent { slot_idx: 1, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(224_770), trade_lots: LotQty(1), snap: eth });
        }
        eth.ts_local_ns = TimestampNs(t3 + 700_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 1, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Bid, trade_price: PriceTicks(224_770), trade_lots: LotQty(95), snap: eth });

        let t4 = t0 + 40_000_000_000;
        let mut sui = base_snap(5, 8_510, 110, 180, t4);
        sui.realized_vol_5m = ScaledBps(5_000);
        for dt in [0_i64, 35_000_000, 70_000_000] {
            sui.ts_local_ns = TimestampNs(t4 + dt);
            twin.ingest_event(&TwinReplayEvent { slot_idx: 5, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui });
        }
        sui.ts_local_ns = TimestampNs(t4 + 95_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 5, kind: MarketEventKind::PublicTradePrint, aggressor_side: Side::Ask, trade_price: PriceTicks(8_510), trade_lots: LotQty(140), snap: sui });
        sui.ts_local_ns = TimestampNs(t4 + 130_000_000);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 5, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_510), trade_lots: LotQty(1), snap: sui });
        sui.ts_local_ns = TimestampNs(t4 + 3_000_000_000);
        sui.bids[0] = BookLevel { price: PriceTicks(8_559), qty: LotQty(360) };
        sui.asks[0] = BookLevel { price: PriceTicks(8_560), qty: LotQty(180) };
        sui.mark_price = PriceTicks(8_560);
        sui.last_trade_price = PriceTicks(8_560);
        sui.set_wave_observables(WaveObservables {
            regime: WaveRegime::ImpulseWave5, climax_active: false, hurst: WaveObservables::encode_hurst(0.60),
            macro_phase: BoundedScore::new_clamped(0.70), oi_impulse: BoundedScore::new_clamped(0.20),
            liq_imbalance: BoundedScore::new_clamped(0.20), fib_proj_bps: ScaledBps(2_600),
        });
        twin.ingest_event(&TwinReplayEvent { slot_idx: 5, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(8_560), trade_lots: LotQty(1), snap: sui });
        let chand_px = twin.engines[5].last_eff_sl_ticks;
        sui.ts_local_ns = TimestampNs(t4 + 4_000_000_000);
        sui.mark_price = PriceTicks(chand_px.0 - 1);
        sui.last_trade_price = PriceTicks(chand_px.0 - 1);
        twin.ingest_event(&TwinReplayEvent { slot_idx: 5, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid, trade_price: PriceTicks(chand_px.0 - 1), trade_lots: LotQty(1), snap: sui });

        twin.finalize_report()
    }

    #[must_use]
    pub fn run_and_print_omega_chat6_twin_certification() -> bool {
        let (elapsed_us, fills, rejects) = run_200k_fifo_benchmark();
        let ev_per_sec = (200_000_u128 * 1_000_000) / elapsed_us.max(1);
        let ab = run_quadruple_omega_ablation_suite();
        let twin = run_omega_digital_twin_walk_forward_lab();

        let sec_title = vec![format!("{ANSI_SKY_BLUE}[VORTEX-REPLAY OMEGA V4.8] 4-ARM PARETO ABLATION & 1:1 PAR2 DIGITAL TWIN GATE{ANSI_RESET}")];
        let sec_fifo = vec![format!("  {ANSI_GREEN}200k FIFO Queue Sim{ANSI_RESET}: {ev_per_sec} ev/s | Passive Fills: {fills} | PostOnly Rejects: {rejects}")];
        let sec_arms = vec![
            format!("  MODEL A   (Baseline Fixed L1)      : {:>+8.2} bps net", (ab.arm_a_baseline_net_bps_x100 as f64) / 100.0),
            format!("  MODEL B   (Elastic Multi-Tranche)  : {:>+8.2} bps net", (ab.arm_b_multitranche_net_bps_x100 as f64) / 100.0),
            format!("  MODEL A+B (VORTEX-L3 LVDBS Surf)   : {:>+8.2} bps net", (ab.arm_ab_vortex_l3_net_bps_x100 as f64) / 100.0),
            format!("  {ANSI_YELLOW}MODEL OMEGA (Shadow+Fib+Chand+Wald): {:>+8.2} bps net [PARETO DOMINANT]{ANSI_RESET}", (ab.arm_omega_full_net_bps_x100 as f64) / 100.0),
            format!(
                "  Adversarial 24-Ep Tape             : L3 {:>+8.2} bps ({} loss) vs OMEGA {:>+8.2} bps ({} loss | Hwk:{} Chd:{} Wld:{})",
                (ab.stress_l3_unshielded.cum_net_bps_x100 as f64) / 100.0, ab.stress_l3_unshielded.losses,
                (ab.stress_omega_shielded.cum_net_bps_x100 as f64) / 100.0, ab.stress_omega_shielded.losses,
                ab.stress_omega_shielded.hawkes_aborts, ab.stress_omega_shielded.chandelier_saves, ab.stress_omega_shielded.wald_compressions
            ),
        ];
        let sec_twin = vec![format!(
            "  {ANSI_GREEN}1:1 Digital Twin (25ms RTT FIFO){ANSI_RESET}  : Trades:{} | Net:{:+.2} bps | HwkAbort:{} | Rej:{} | Drift:{}",
            twin.closed_trades, (twin.cum_net_bps_x100 as f64) / 100.0, twin.hawkes_queue_aborts, twin.post_only_rejects, twin.parity_sync_drift_1e8
        )];
        print_auto_aligned_box(&[sec_title, sec_fifo, sec_arms, sec_twin]);

        ev_per_sec >= 100_000 && fills == 1_000 && rejects == 0 && ab.pareto_strictly_dominant
            && twin.closed_trades == 3 && twin.hawkes_queue_aborts == 1 && twin.post_only_rejects == 0
            && twin.parity_sync_drift_1e8 == 0 && twin.cum_net_bps_x100 >= 7_000
    }
}

pub use omega_twin::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forensic_parquet::MarketEventKind;
    use crate::lob_shm::BookLevel;
    use crate::types::{BoundedScore, TimestampNs};
    use crate::wave_elliott::WaveRegime;

    #[test]
    fn test_v48_par2_wave_climax_quantum_btc_veto_and_daily_twin_parity() {
        assert!(run_and_print_omega_chat6_twin_certification());
        let mut twin = OmegaDigitalTwinEngine::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[8].symbol_id;
        snap.realized_vol_5m = ScaledBps(5_000);

        for (idx, &px) in [1_600_i64, 1_620, 1_607, 1_645, 1_632, 1_650].iter().enumerate() {
            snap.sequence_id = (idx + 1) as u64;
            snap.ts_local_ns = TimestampNs((idx as i64 + 1) * 2_000_000_000);
            for lvl in 0..5 {
                snap.bids[lvl] = BookLevel { price: PriceTicks(px - lvl as i64), qty: LotQty(400) };
                snap.asks[lvl] = BookLevel { price: PriceTicks(px + 1 + lvl as i64), qty: LotQty(380) };
            }
            let r = ColumnarMarketRow::from_lob_snapshot(&snap, MarketEventKind::L2BookSnapshot, Side::Bid, PriceTicks(px), LotQty(400));
            twin.ingest_par2_row(&r);
        }
        snap.sequence_id = 10;
        snap.ts_local_ns = TimestampNs(14_000_000_000);
        twin.ingest_par2_row(&ColumnarMarketRow::from_ticker_meta(&snap, PriceTicks(1_650), 10_000_000, ScaledBps(135), TimestampNs(20_000_000_000)));
        snap.sequence_id = 11;
        snap.ts_local_ns = TimestampNs(16_000_000_000);
        twin.ingest_par2_row(&ColumnarMarketRow::from_ticker_meta(&snap, PriceTicks(1_650), 9_700_000, ScaledBps(135), TimestampNs(20_000_000_000)));
        snap.sequence_id = 12;
        snap.ts_local_ns = TimestampNs(17_000_000_000);
        twin.ingest_par2_row(&ColumnarMarketRow::from_liquidation_print(&snap, Side::Bid, PriceTicks(1_651), LotQty(6_000), 9_700_000));

        let w_obs = twin.cached_snaps[8].wave_observables();
        assert_eq!(twin.cached_snaps[8].funding_rate_scaled_bps, ScaledBps(135));
        assert!(w_obs.oi_impulse.get() < -0.10 && w_obs.regime == WaveRegime::ClimaxExhaustion && w_obs.climax_active);

        // V4.8 Quantum EV Gate in Twin: oversized 0.001 BTC at ~$65.14 USDT remains strictly Idle
        let mut btc_t13 = SymbolLobSnapshot::new_empty();
        btc_t13.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_t13.is_eligible = true;
        btc_t13.sequence_id = 50;
        btc_t13.ts_local_ns = TimestampNs(1_794_790_000_000_000_000);
        btc_t13.realized_vol_5m = ScaledBps(3_200);
        btc_t13.bids[0] = BookLevel { price: PriceTicks(646_312), qty: LotQty(900) };
        btc_t13.asks[0] = BookLevel { price: PriceTicks(646_313), qty: LotQty(1_200) };
        btc_t13.refresh_deep_l1_l5_totals();
        btc_t13.mark_price = PriceTicks(646_312);
        btc_t13.last_trade_price = PriceTicks(646_312);
        btc_t13.factor_ofi = BoundedScore::new_clamped(0.82);
        btc_t13.factor_tfi = BoundedScore::new_clamped(0.88);
        btc_t13.factor_geom = BoundedScore::new_clamped(0.65);
        twin.ledger.initial_equity_1e8 = 6_513_770_000;
        twin.engines[0].learned_mfe_bps = 568;
        twin.ingest_event(&TwinReplayEvent {
            slot_idx: 0, kind: MarketEventKind::L2BookSnapshot, aggressor_side: Side::Bid,
            trade_price: PriceTicks(646_312), trade_lots: LotQty(1), snap: btc_t13,
        });
        assert!(matches!(twin.engines[0].state, OrderLifecycleState::Idle));

        let cut_ns = 1_794_672_000_000_000_000_i64;
        let mut f_snap = SymbolLobSnapshot::new_empty();
        f_snap.mark_price = PriceTicks(10_000);
        f_snap.funding_rate_scaled_bps = ScaledBps(200);
        f_snap.ts_local_ns = TimestampNs(cut_ns + 5_000_000_000);
        let mut last_cut = 0_i64;
        let mut led = ParitySyncLedger::new(6_543_090_000);
        assert_eq!(settle_crossed_funding_boundary(APPROVED_SPECS[3], Side::Ask, LotQty(20), PriceTicks(10_000), cut_ns - 10_000_000_000, &f_snap, &mut last_cut, &mut led), Some(400_000));
        assert_eq!(led.current_equity_1e8() - led.initial_equity_1e8, 400_000);
        assert!(settle_crossed_funding_boundary(APPROVED_SPECS[3], Side::Ask, LotQty(20), PriceTicks(10_000), cut_ns - 10_000_000_000, &f_snap, &mut last_cut, &mut led).is_none());
    }
}

#[cfg(test)]
mod v49_replay_twin_parity_tests {
    use super::*;

    #[test]
    fn test_v49_digital_twin_blocks_tao17_macro_and_preserves_4arm_pareto() {
        assert!(run_and_print_omega_chat6_twin_certification());
        let gov = OnlineAdaptiveGovernor::new();
        let mut s = SymbolLobSnapshot::new_empty();
        s.symbol_id = APPROVED_SPECS[4].symbol_id;
        s.realized_vol_5m = ScaledBps(2_600);
        s.bids[0] = crate::lob_shm::BookLevel { price: PriceTicks(19_335), qty: LotQty(800) };
        s.asks[0] = crate::lob_shm::BookLevel { price: PriceTicks(19_336), qty: LotQty(800) };
        s.set_wave_observables(crate::wave_elliott::WaveObservables {
            regime: crate::wave_elliott::WaveRegime::ImpulseWave3,
            climax_active: false,
            hurst: crate::wave_elliott::WaveObservables::encode_hurst(0.55),
            macro_phase: crate::types::BoundedScore::new_clamped(0.90),
            oi_impulse: crate::types::BoundedScore::ZERO,
            liq_imbalance: crate::types::BoundedScore::ZERO,
            fib_proj_bps: ScaledBps(5_500),
        });
        assert!(gov.authorizes_adaptive_entry_v49(4, Side::Ask, 6_502_850_000, 1_000, ScaledBps(594), &s, &s, 0.0, 1.0).is_err());
    }
}

/// Restores up to `max_segments` from cold `.par2z` archives and runs 1:1 `Walk-Forward` replay.
#[must_use]
pub fn run_par2z_archive_walk_forward(
    archive_root: &Path,
    max_segments: usize,
    verbose: bool,
) -> WalkForwardReport {
    let eph = std::env::temp_dir().join(format!(
        "vortex_par2z_wf_{}_{}",
        std::process::id(),
        crate::types::now_epoch_ns().0
    ));
    let _ = fs::remove_dir_all(&eph);
    let restored = crate::par2_archiver::restore_par2z_archive_to_dir(
        archive_root,
        &eph,
        max_segments.max(1),
    )
    .unwrap_or(0);
    let rep = if restored > 0 {
        run_parquet_walk_forward_on_dir(&eph, verbose)
    } else {
        WalkForwardReport::default()
    };
    let _ = fs::remove_dir_all(&eph);
    rep
}

#[cfg(test)]
mod v49_par2z_walkforward_tests {
    use super::*;
    use crate::forensic_parquet::{ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind};

    #[test]
    fn test_v49_par2z_cold_archive_walk_forward_end_to_end() {
        let root = std::env::temp_dir().join(format!("vortex_wf_arc_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let raw_dir = root.join("raw");
        let arc_dir = root.join("archive");
        assert!(fs::create_dir_all(&raw_dir).is_ok());

        let mut s = SymbolLobSnapshot::new_empty();
        s.symbol_id = APPROVED_SPECS[0].symbol_id;
        s.ts_local_ns = crate::types::TimestampNs(1_763_350_000_000_000_000);
        s.bids[0] = crate::lob_shm::BookLevel { price: PriceTicks(650_000), qty: LotQty(250) };
        s.asks[0] = crate::lob_shm::BookLevel { price: PriceTicks(650_001), qty: LotQty(250) };
        s.mark_price = PriceTicks(650_000);
        let mut b = ColumnarParquetBatch::new();
        assert!(b.push_row(&ColumnarMarketRow::from_lob_snapshot(&s, MarketEventKind::L2BookSnapshot, Side::Bid, s.mark_price, LotQty(250))).is_ok());
        let seg = raw_dir.join("vortex_l2_segment_000001.parquet");
        assert!(b.write_to_file(&seg).is_ok());
        assert!(crate::par2_archiver::archive_parquet_segment_file(&seg, &arc_dir).is_ok());

        let rep = run_par2z_archive_walk_forward(&arc_dir, 16, false);
        assert_eq!(rep.files_count, 1);
        assert_eq!(rep.total_events, 1);
        let _ = fs::remove_dir_all(&root);
    }
}
