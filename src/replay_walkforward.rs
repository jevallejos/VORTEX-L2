//! Continuous 1:1 Walk-Forward PAR1/PAR2 Replay Engine, 3-Tranche Cascade & Anti-Hostage Parity (V5.5).
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
use std::path::{Path, PathBuf};

use crate::alpha_3factor::{
    BtcLeadLagMemory, EppsBtcCorrelationTracker, EventClockAlphaModulator, OrthogonalFactorState,
    SignalVerdict,
};
use crate::bybit_client::HANDSHAKE_VAULT_CACHE_PATH;
use crate::bybit_gateway::should_repeg_resting_exit_tp_with_open_lots;
use crate::candle_store::{CandleTimeframe, PerpetualCandleStore, Vkl1CandleRecord};
use crate::exec_daemon::{
    build_monetary_beta_guard_with_epps, can_stage_same_direction_entry_v542,
    compute_exit_sync_schedule_elastic, compute_moallemi_yuan_queue_priority_c1,
    compute_offensive_entry_utility_score_x1000_v55, compute_open_positions_mtm_upl_1e8,
    compute_vacuum_l2_l3_c1, compute_validation_epoch_loss_1e8, elastic_resting_queue_timers_ns,
    evaluate_resting_repeg_candidate_v554, favorable_bbo_miss_ticks, is_at_or_ahead_of_l1,
    is_resting_headwind_erosion_v554, passes_resting_elliott_macro_hysteresis_c1,
    slot_position_risk_ppm_with_fees, sort_offensive_candidates_zero_alloc,
};
use crate::execution_fsm::{
    compute_agile_repeg_cooldown_ns_v542, compute_aligned_entry_propulsion_c1,
    is_chandelier_profit_locked_sl_with_fees, passes_elliott_macro_coherence_guard_with_health,
    passes_order_block_wall_guard_dynamic, ClosedTradeKind, ExitOrderRole, ExitSubReason,
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

const SCAR_MEMORY_FILENAME: &str = "vortex_governor_scar_memory.jsonl";
const RED_LOSS_BOX_FILENAME: &str = "vortex_red_loss_box.jsonl";
const AI_FORENSIC_FILENAME: &str = "vortex_ai_forensic_vault.jsonl";
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

const MAX_VKL1_PREWARM_BARS: usize = 200;
const MAX_IMPLICIT_VKL1_GAP_NS: i64 = 7 * 86_400_000_000_000;

fn store_has_any_vkl1(store: &PerpetualCandleStore) -> bool {
    (0..MAX_ACTIVE_PAIRS).any(|s| {
        store.record_count(s, CandleTimeframe::Min5) > 0
            || store.record_count(s, CandleTimeframe::Min1) > 0
    })
}

fn resolve_live_sibling_hydration_dirs(
    data_dir: &Path,
    verbose: bool,
) -> Option<(PathBuf, PathBuf)> {
    if !verbose {
        return None;
    }
    let parent = match data_dir.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => data_dir
            .canonicalize()
            .ok()
            .and_then(|c| c.parent().map(Path::to_path_buf))?,
    };
    let candles_dir = parent.join("candles");
    let vault_dir = parent.join("vault");
    if candles_dir.is_dir() && vault_dir.is_dir() {
        Some((candles_dir, vault_dir))
    } else {
        None
    }
}

fn load_fee_schedule_from_handshake_path(path: &Path) -> Option<FeeSchedule> {
    let txt = fs::read_to_string(path).ok()?;
    let val = serde_json::from_str::<serde_json::Value>(&txt).ok()?;
    let m = val.get("maker_fee_scaled_bps").and_then(serde_json::Value::as_i64)?;
    let t = val.get("taker_fee_scaled_bps").and_then(serde_json::Value::as_i64)?;
    Some(FeeSchedule::from_live_rates_clamped(ScaledBps(m), ScaledBps(t)))
}

fn peek_first_parquet_ts_ns(files: &[PathBuf]) -> i64 {
    for f_path in files {
        if let Ok((batch, _)) = ColumnarParquetBatch::read_from_file(f_path) {
            for i in 0..batch.len() {
                if let Some(row) = batch.get_row(i) {
                    if row.ts_local_ns.0 > 0 {
                        return row.ts_local_ns.0;
                    }
                }
            }
        }
    }
    0
}

fn records_to_bybit_kline_json(spec: PairSpec, recs: &[Vkl1CandleRecord]) -> serde_json::Value {
    let fmt_px = |ticks: i64| -> String {
        let v1e8 = i128::from(ticks) * i128::from(spec.tick_size_1e8);
        format!("{}.{:08}", v1e8 / 100_000_000, (v1e8 % 100_000_000).abs())
    };
    let mut list = Vec::with_capacity(recs.len());
    for r in recs.iter().rev() {
        let vol_coins = (r.volume_lots as f64) * (spec.lot_step_1e8 as f64) / 100_000_000.0;
        let turn_usdt =
            vol_coins * (r.close_ticks as f64) * (spec.tick_size_1e8 as f64) / 100_000_000.0;
        list.push(serde_json::json!([
            (r.open_ts_ns / 1_000_000).to_string(),
            fmt_px(r.open_ticks),
            fmt_px(r.high_ticks),
            fmt_px(r.low_ticks),
            fmt_px(r.close_ticks),
            format!("{vol_coins:.4}"),
            format!("{turn_usdt:.2}")
        ]));
    }
    serde_json::json!({ "result": { "list": list } })
}

fn export_causal_vkl1_bundle(
    store: &PerpetualCandleStore,
    slot_idx: usize,
    tf: CandleTimeframe,
    first_wf_ts_ns: i64,
    allow_unbounded_age: bool,
    limit: usize,
) -> Option<serde_json::Value> {
    if store.record_count(slot_idx, tf) == 0 {
        return None;
    }
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    if first_wf_ts_ns > tf.duration_ns() {
        let max_open_ns = first_wf_ts_ns.saturating_sub(tf.duration_ns());
        if let Ok(recs) = store.query_range(slot_idx, tf, 0, max_open_ns, usize::MAX) {
            if let Some(last) = recs.last() {
                if allow_unbounded_age
                    || first_wf_ts_ns.saturating_sub(last.open_ts_ns) <= MAX_IMPLICIT_VKL1_GAP_NS
                {
                    let tail = &recs[recs.len().saturating_sub(limit)..];
                    return Some(records_to_bybit_kline_json(spec, tail));
                }
                return None;
            }
        }
    }
    if allow_unbounded_age {
        store.export_bybit_kline_bundle(slot_idx, tf, limit).ok()
    } else {
        None
    }
}

fn hydrate_wave_and_ob_from_vkl1(
    store: &PerpetualCandleStore,
    first_wf_ts_ns: i64,
    allow_unbounded_age: bool,
    wave_states: &mut [CausalElliottWaveState; MAX_ACTIVE_PAIRS],
    ob_engines: &mut [CausalOrderBlockEngine; MAX_ACTIVE_PAIRS],
) {
    for s in 0..MAX_ACTIVE_PAIRS {
        let k5 = export_causal_vkl1_bundle(
            store,
            s,
            CandleTimeframe::Min5,
            first_wf_ts_ns,
            allow_unbounded_age,
            MAX_VKL1_PREWARM_BARS,
        );
        let k1 = export_causal_vkl1_bundle(
            store,
            s,
            CandleTimeframe::Min1,
            first_wf_ts_ns,
            allow_unbounded_age,
            MAX_VKL1_PREWARM_BARS,
        );
        if k5.is_some() || k1.is_some() {
            let spec = APPROVED_SPECS[s];
            let _ = wave_states[s].warm_start_from_bybit_bundle(
                spec,
                k5.as_ref(),
                k1.as_ref(),
                None,
            );
            let _ = ob_engines[s].warm_start_from_bybit_bundle(
                spec,
                k5.as_ref(),
                k1.as_ref(),
                None,
            );
        }
    }
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
            let prev_w_oi = snap.wave_observables();
            let prev_valid_oi = !matches!(prev_w_oi.regime, crate::wave_elliott::WaveRegime::NeutralWarmup)
                || prev_w_oi.climax_active
                || prev_w_oi.fib_proj_bps.0 != 0
                || prev_w_oi.macro_phase.get().abs() > 1e-6;
            let w_oi = wave_state.observe_open_interest(spec, oi_contracts, snap.ts_local_ns.0);
            if prev_valid_oi
                && matches!(w_oi.regime, crate::wave_elliott::WaveRegime::NeutralWarmup)
                && !w_oi.climax_active
            {
                let mut merged = prev_w_oi;
                merged.oi_impulse = w_oi.oi_impulse;
                snap.set_wave_observables(merged);
            } else {
                snap.set_wave_observables(w_oi);
            }
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
        let prev_w = snap.wave_observables();
        let prev_has_valid_wave = !matches!(prev_w.regime, crate::wave_elliott::WaveRegime::NeutralWarmup)
            || prev_w.climax_active
            || prev_w.fib_proj_bps.0 != 0
            || prev_w.macro_phase.get().abs() > 1e-6;
        let w_tick = wave_state.observe_price_tick(spec, mid_w, snap.ts_local_ns.0, snap.realized_vol_5m);
        let w_tick_is_cold = matches!(w_tick.regime, crate::wave_elliott::WaveRegime::NeutralWarmup)
            && !w_tick.climax_active
            && w_tick.fib_proj_bps.0 == 0
            && w_tick.oi_impulse.get().abs() <= 1e-6
            && w_tick.liq_imbalance.get().abs() <= 1e-6;
        if !(prev_has_valid_wave && w_tick_is_cold) {
            snap.set_wave_observables(w_tick);
        }
        let ob_mid = ob_engine.observe_mid_quote(spec, mid_w);
        if ob_mid.bull_wall_ticks.0 > 0 || ob_mid.bear_wall_ticks.0 > 0 {
            let (bull_pm, bear_pm) = ob_engine.directional_efficacy_pm();
            snap.set_order_block_observables_directional(ob_mid, bull_pm, bear_pm);
        }
        markout_ring.step_clock(mid_w, snap.ts_local_ns, fees.maker_fee_bps.raw());
    }
}

#[derive(Copy, Clone)]
struct ReplayStagedOffensiveCandidate {
    side: Side,
    verdict: SignalVerdict,
    lots: LotQty,
    risk_ppm: i64,
    beta_x1000: i64,
    epps_corr: f64,
    btc_lead_opt: Option<f64>,
    ts_local_ns: i64,
    geom_raw: f64,
    init_flow: f64,
}

#[inline]
fn sync_replay_exit_fifo_and_state(
    eng: &mut SlotExecutionEngine,
    exit_fifo: &mut FifoQueueSimulator,
    exit_role_latch: &mut ExitOrderRole,
    pos_side: Side,
    quote: &crate::execution_fsm::CanonicalExitQuote,
    submit_ts: crate::types::TimestampNs,
) {
    let safe_lots = LotQty(quote.order_lots.0.max(1));
    *exit_role_latch = quote.role;
    exit_fifo.submit_post_only(pos_side.opposite(), quote.target_price, safe_lots, submit_ts);
    if let OrderLifecycleState::PositionOpenMakerExit {
        active_exit_role: ref mut ax_r,
        active_exit_price: ref mut ax_p,
        active_exit_lots: ref mut ax_l,
        resting_exit_price: ref mut rx_p,
        ..
    } = eng.state
    {
        *ax_r = quote.role;
        *ax_p = quote.target_price;
        *ax_l = safe_lots;
        *rx_p = quote.target_price;
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

    let live_hydration_dirs = resolve_live_sibling_hydration_dirs(data_dir, verbose);
    if let Some((ref candles_dir, ref vault_dir)) = live_hydration_dirs {
        if let Some(hs_name) = Path::new(HANDSHAKE_VAULT_CACHE_PATH).file_name() {
            if let Some(fees_hs) = load_fee_schedule_from_handshake_path(&vault_dir.join(hs_name)) {
                gov.set_active_fee_schedule(fees_hs);
            }
        }
        let scar_path = vault_dir.join(SCAR_MEMORY_FILENAME);
        let loss_path = vault_dir.join(RED_LOSS_BOX_FILENAME);
        let forensic_path = vault_dir.join(AI_FORENSIC_FILENAME);
        let _ = gov.rehydrate_from_scar_and_loss_files(
            &scar_path,
            &loss_path,
            forensic_path.is_file().then_some(forensic_path.as_path()),
        );
        if let Some(audit_name) = Path::new(crate::vault_io::PLASTICITY_AUDIT_VAULT_PATH).file_name() {
            let _ = crate::vault_io::rehydrate_epps_trackers_from_audit_path(
                &mut epps_trackers,
                &vault_dir.join(audit_name),
                crate::types::now_epoch_ns(),
            );
        }
        let c_store = PerpetualCandleStore::new(candles_dir);
        if store_has_any_vkl1(&c_store) {
            let first_wf_ts_ns = peek_first_parquet_ts_ns(&files);
            hydrate_wave_and_ob_from_vkl1(
                &c_store,
                first_wf_ts_ns,
                true,
                &mut wave_states,
                &mut ob_engines,
            );
        }
        if let Some(ob_name) = Path::new(OB_PLASTICITY_VAULT_PATH).file_name() {
            let _ = CausalOrderBlockEngine::load_plasticity_array_from_path(
                &mut ob_engines,
                &vault_dir.join(ob_name),
            );
        }
    }
    for eng in &mut engines {
        eng.sync_governor_dynamics(&gov);
    }
    if let Some((_, ref vault_dir)) = live_hydration_dirs {
        if let Some(mfe_name) = Path::new(MFE_PLASTICITY_VAULT_PATH).file_name() {
            SlotExecutionEngine::hydrate_12_slot_mfe_from_path(
                &mut engines,
                &vault_dir.join(mfe_name),
            );
        }
        crate::telemetry_hud::rehydrate_cold_mfe_seeds_from_scar_vault(
            &mut engines,
            &vault_dir.join(SCAR_MEMORY_FILENAME),
        );
    }
    let fees_init = gov.active_fee_schedule();
    let mut ledger = ParitySyncLedger::new(INITIAL_REPLAY_EQUITY_1E8);
    ledger.set_live_vip_fee_rates(fees_init.maker_fee_bps, fees_init.taker_fee_bps);

    let mut entry_fifo = [FifoQueueSimulator::new(); MAX_ACTIVE_PAIRS];
    let mut exit_fifo = [FifoQueueSimulator::new(); MAX_ACTIVE_PAIRS];
    let mut exit_role_latch = [ExitOrderRole::None; MAX_ACTIVE_PAIRS];
    let mut init_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut repeg_pending_init_q = [false; MAX_ACTIVE_PAIRS];
    let mut prev_slot_ts_ns = [0_i64; MAX_ACTIVE_PAIRS];
    let mut armed_ts_latch = [0_i64; MAX_ACTIVE_PAIRS];
    let mut armed_geom_latch = [0.0_f64; MAX_ACTIVE_PAIRS];
    let mut geom_adv_since_ns = [0_i64; MAX_ACTIVE_PAIRS];
    let mut queue_flow_ema = [0.0_f64; MAX_ACTIVE_PAIRS];
    let mut btc_lead_memory = BtcLeadLagMemory::new();
    let mut halt_locked = ledger.cumulative_net_pnl_1e8 <= -crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8;
    let mut first_ring_ts_ns = 0_i64;
    let require_ring_warmup = files.len() >= 64;

    let mut snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let init_vol = ScaledBps(ScaledBps::MIN_REQUIRED_VOL_5M_BPS.raw() * 4);
    for s in 0..MAX_ACTIVE_PAIRS {
        snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
        snaps[s].is_eligible = true;
        snaps[s].realized_vol_5m = init_vol;
        snaps[s].set_wave_observables(wave_states[s].observables());
        let (b_pm, a_pm) = ob_engines[s].directional_efficacy_pm();
        snaps[s].set_order_block_observables_directional(ob_engines[s].observables(), b_pm, a_pm);
    }

    const REPLAY_WS_RTT_US: u32 = 70_000;
    const REPLAY_WS_QUOTA_REM: u16 = 100;
    const SLICE_BUCKET_NS: i64 = 5_000_000;
    if crate::execution_fsm::read_live_ws_pulse().0 == 0 {
        crate::execution_fsm::record_live_ws_pulse(REPLAY_WS_RTT_US, REPLAY_WS_QUOTA_REM);
    }

    for f_path in &files {
        let Ok((batch, _)) = ColumnarParquetBatch::read_from_file(f_path) else { continue; };
        let mut cursor = 0_usize;
        while cursor < batch.len() {
            let Some(anchor_row) = batch.get_row(cursor) else {
                cursor += 1;
                continue;
            };
            let Some(anchor_slot) = anchor_row.symbol_id.slot_index() else {
                cursor += 1;
                continue;
            };
            let anchor_ts = anchor_row.ts_local_ns.0;
            let mut seen_in_slice = [false; MAX_ACTIVE_PAIRS];
            seen_in_slice[anchor_slot] = true;
            let mut slice_end = cursor + 1;
            if anchor_ts > 0 {
                while slice_end < batch.len() {
                    let Some(nxt) = batch.get_row(slice_end) else { break };
                    let Some(nxt_slot) = nxt.symbol_id.slot_index() else { break };
                    let dt = nxt.ts_local_ns.0.saturating_sub(anchor_ts);
                    if nxt.ts_local_ns.0 <= 0 || !(0..SLICE_BUCKET_NS).contains(&dt) || seen_in_slice[nxt_slot] {
                        break;
                    }
                    seen_in_slice[nxt_slot] = true;
                    slice_end += 1;
                }
            }

            // Pass 1: Ingest BTC leader row first if present in this 5 ms (200.0 Hz) slice so O2 BtcLeadLagMemory
            // and Epps 5s trackers reflect simultaneous leader state before follower slots evaluate.
            let mut idle_touched = [false; MAX_ACTIVE_PAIRS];
            let mut btc_updated_in_slice = false;
            let mut slice_latest_ts_ns = anchor_ts;
            for pass_btc_first in [true, false] {
                for i in cursor..slice_end {
                    let Some(row) = batch.get_row(i) else { continue };
                    let Some(s_idx) = row.symbol_id.slot_index() else { continue };
                    if (s_idx == 0) != pass_btc_first {
                        continue;
                    }
                    if s_idx == 0 {
                        btc_updated_in_slice = true;
                    }
                    rep.total_events += 1;
                    let prev_ts_ns = prev_slot_ts_ns[s_idx];

                    let fees = gov.active_fee_schedule();
                    ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
                    let slot_tkr_pm = i64::midpoint(
                        gov.slot_taker_prob_permille(s_idx, Side::Bid),
                        gov.slot_taker_prob_permille(s_idx, Side::Ask),
                    );
                    step_par2_observables_on_row(
                        s_idx,
                        &row,
                        &mut snaps[s_idx],
                        &mut wave_states[s_idx],
                        &mut ob_engines[s_idx],
                        &mut markout_rings[s_idx],
                        fees,
                        slot_tkr_pm,
                    );
                    let slot_dt_ns = if prev_ts_ns > 0 && snaps[s_idx].ts_local_ns.0 > prev_ts_ns {
                        snaps[s_idx].ts_local_ns.0 - prev_ts_ns
                    } else {
                        5_000_000
                    };
                    if snaps[s_idx].ts_local_ns.0 > 0 {
                        prev_slot_ts_ns[s_idx] = snaps[s_idx].ts_local_ns.0;
                    }
                    if let Some(mk_pk) = markout_rings[s_idx].take_uncensored_fill_peak_sbps() {
                        engines[s_idx].observe_markout_ring_favorable_peak(mk_pk);
                    }
                    if let Some((mk_side, mk_fav_60s)) = markout_rings[s_idx].take_resolved_60s_fill_markout() {
                        let _ = gov.observe_markout_60s_continuation(
                            s_idx,
                            mk_side,
                            engines[s_idx].last_closed_mfe_bps,
                            mk_fav_60s,
                            snaps[s_idx].realized_vol_5m,
                        );
                    }
                    if first_ring_ts_ns == 0 && snaps[s_idx].ts_local_ns.0 > 0 {
                        first_ring_ts_ns = snaps[s_idx].ts_local_ns.0;
                    }
                    if snaps[s_idx].ts_local_ns.0 > slice_latest_ts_ns {
                        slice_latest_ts_ns = snaps[s_idx].ts_local_ns.0;
                    }

                    let _ = spoof_clocks[s_idx].step_event_clock_and_spoof(
                        snaps[s_idx].ts_local_ns,
                        snaps[s_idx].factor_ofi,
                        snaps[s_idx].factor_tfi,
                    );
                    let cur_snap = snaps[s_idx];
                    let btc_snap = if snaps[0].bids[0].price.0 > 0 { snaps[0] } else { cur_snap };
                    let mid_now = PriceTicks(i64::midpoint(
                        cur_snap.bids[0].price.0.max(1),
                        cur_snap.asks[0].price.0.max(1),
                    ));
                    let mid_btc = PriceTicks(i64::midpoint(
                        btc_snap.bids[0].price.0.max(1),
                        btc_snap.asks[0].price.0.max(1),
                    ));
                    epps_trackers[s_idx].observe_5s_pair(cur_snap.ts_local_ns, s_idx == 0, mid_now, mid_btc);
                    gov.observe_market_price(s_idx, &cur_snap);

                    match row.par2_kind {
                        Par2EventKind::L2BookSnapshot => {
                            rep.l2_events += 1;
                            entry_fifo[s_idx].on_lob_snapshot(&cur_snap);
                            exit_fifo[s_idx].on_lob_snapshot(&cur_snap);
                        }
                        Par2EventKind::PublicTradePrint => {
                            rep.trade_events += 1;
                            entry_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                            exit_fifo[s_idx].on_trade_print(row.ts_local_ns, row.side, row.price_ticks, row.lot_qty);
                        }
                        Par2EventKind::TickerMeta => {
                            rep.par2_ticker_events += 1;
                        }
                        Par2EventKind::LiquidationPrint => {
                            rep.par2_liq_events += 1;
                        }
                    }

                    if s_idx == 0 {
                        let _ = btc_lead_memory.observe_btc_snapshot_with_rtt(&btc_snap, REPLAY_WS_RTT_US);
                    } else {
                        let _ = btc_lead_memory.observe_slot_snapshot(s_idx, &cur_snap, REPLAY_WS_RTT_US);
                    }
                    let spec = APPROVED_SPECS[s_idx];
                    let spoof_d = spoof_clocks[s_idx].spoofing_density();
                    engines[s_idx].hawkes_tracker.set_spoof_density(spoof_d);
                    let evt_dil = spoof_clocks[s_idx].event_clock_dilation();
                    let epps_corr = if s_idx == 0 { 0.0 } else { epps_trackers[s_idx].correlation(false) };
                    let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(
                        s_idx,
                        &cur_snap,
                        &btc_snap,
                        &epps_trackers[s_idx],
                    );
                    let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };

                    let eq_now = ledger.current_equity_1e8();
                    let was_sleeping = daily_gov.is_hibernating();
                    let p_day = gov.slot_temporal_cadence(0, &btc_snap).intraday_percentile;
                    let _ = daily_gov.evaluate_intraday_gate(
                        cur_snap.ts_local_ns,
                        eq_now,
                        ledger.sinking_fund_reserve_bps_x100,
                        gov.active_maker_rt_fee_bps(),
                        gov.portfolio_mean_eq_vol_bps(),
                        gov.portfolio_mean_ir_x100(),
                        p_day,
                    );
                    if !was_sleeping && daily_gov.is_hibernating() {
                        rep.daily_hibernations += 1;
                    }
                    let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(&engines, &snaps);
                    let (epoch_loss_1e8, _) = compute_validation_epoch_loss_1e8(&ledger, None, open_upl_1e8);
                    if epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
                        halt_locked = true;
                    }
                    let psi_health = gov.refresh_psi_health(
                        &daily_gov,
                        ledger.sinking_fund_reserve_bps_x100,
                        epoch_loss_1e8,
                    );
                    let block_new_entries = halt_locked || daily_gov.is_hibernating();
                    if halt_locked {
                        for k in 0..MAX_ACTIVE_PAIRS {
                            match engines[k].state {
                                OrderLifecycleState::PlacingEntryMaker { .. } => {
                                    repeg_pending_init_q[k] = false;
                                    entry_fifo[k].reset();
                                    engines[k].state = OrderLifecycleState::Idle;
                                }
                                OrderLifecycleState::RestingEntryQueue {
                                    side: ab_s,
                                    entry_price: ab_px,
                                    resting_since_ts: ab_rs,
                                    ..
                                } => {
                                    let rem_q_k = match entry_fifo[k].state {
                                        SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, .. } => queue_ahead_qty,
                                        _ => LotQty(0),
                                    };
                                    let arm_ts_k = crate::types::TimestampNs(if armed_ts_latch[k] > 0 {
                                        armed_ts_latch[k]
                                    } else {
                                        ab_rs.0.max(1)
                                    });
                                    gov.observe_entry_abort(k, ab_s, ab_px, cur_snap.ts_local_ns);
                                    markout_rings[k].arm_abort_probe_v49(
                                        ab_s,
                                        ab_px,
                                        LastVetoCode::WarmupOrLock,
                                        arm_ts_k,
                                        cur_snap.ts_local_ns,
                                        init_q_ahead[k],
                                        rem_q_k,
                                    );
                                    repeg_pending_init_q[k] = false;
                                    entry_fifo[k].reset();
                                    engines[k].state = OrderLifecycleState::Idle;
                                    rep.queue_aborts += 1;
                                }
                                OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => {}
                            }
                        }
                    }

                    match engines[s_idx].state {
                        OrderLifecycleState::Idle => {
                            idle_touched[s_idx] = true;
                        }
                        OrderLifecycleState::PlacingEntryMaker {
                            side,
                            qty,
                            target_excursion,
                            submitted_ts,
                            ..
                        } => {
                            engines[s_idx].sync_governor_dynamics(&gov);
                            match entry_fifo[s_idx].state {
                                SimulatedOrderState::Inactive => {
                                    if block_new_entries {
                                        engines[s_idx].state = OrderLifecycleState::Idle;
                                        continue;
                                    }
                                    if cur_snap.ts_local_ns.0.saturating_sub(submitted_ts.0)
                                        < engines[s_idx].elastic_preflight_window_ns(cur_snap.realized_vol_5m)
                                    {
                                        continue;
                                    }
                                    let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                                    let btc_lead_opt = (s_idx > 0).then(|| {
                                        btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                                    });
                                    let ll_bonus = btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                                        s_idx,
                                        side,
                                        &cur_snap,
                                        &btc_snap,
                                        REPLAY_WS_RTT_US,
                                        epps_beta,
                                        epps_corr,
                                        psi_health,
                                        fees,
                                    );
                                    if gov
                                        .confirm_preflight_kinematics_v55(
                                            s_idx,
                                            side,
                                            eq_now,
                                            qty,
                                            mfe_s,
                                            &cur_snap,
                                            &btc_snap,
                                            spoof_d,
                                            epps_beta,
                                            epps_corr,
                                            btc_lead_opt,
                                            ll_bonus,
                                        )
                                        .is_err()
                                    {
                                        engines[s_idx].state = OrderLifecycleState::Idle;
                                        continue;
                                    }
                                    let Ok((mut synced_px, _)) = engines[s_idx].sync_preflight_l1_entry_auto_v542(
                                        &cur_snap,
                                        &btc_snap,
                                        epps_corr,
                                        btc_lead_opt,
                                        psi_health,
                                    ) else {
                                        engines[s_idx].state = OrderLifecycleState::Idle;
                                        continue;
                                    };
                                    let cur_l1_px = side.select_entry_bbo_price(cur_snap.bids[0].price, cur_snap.asks[0].price);
                                    let ev_ok = gov
                                        .evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
                                            s_idx,
                                            side,
                                            qty,
                                            synced_px,
                                            mfe_s,
                                            target_excursion,
                                            &cur_snap,
                                            &btc_snap,
                                            epps_beta,
                                        )
                                        .is_some()
                                        || (synced_px != cur_l1_px
                                            && engines[s_idx]
                                                .sync_preflight_l1_entry_auto_v542(&cur_snap, &btc_snap, epps_corr, btc_lead_opt, 0.0)
                                                .map(|(p, _)| synced_px = p)
                                                .is_ok()
                                            && gov
                                                .evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
                                                    s_idx,
                                                    side,
                                                    qty,
                                                    synced_px,
                                                    mfe_s,
                                                    target_excursion,
                                                    &cur_snap,
                                                    &btc_snap,
                                                    epps_beta,
                                                )
                                                .is_some());
                                    if !ev_ok {
                                        engines[s_idx].state = OrderLifecycleState::Idle;
                                        continue;
                                    }
                                    armed_ts_latch[s_idx] = cur_snap.ts_local_ns.0;
                                    armed_geom_latch[s_idx] = cur_snap.factor_geom.get();
                                    geom_adv_since_ns[s_idx] = 0;
                                    repeg_pending_init_q[s_idx] = false;
                                    queue_flow_ema[s_idx] = 0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get();
                                    entry_fifo[s_idx].submit_post_only(side, synced_px, qty, cur_snap.ts_local_ns);
                                    rep.armed += 1;
                                }
                                SimulatedOrderState::InFlightToExchange { .. } => {}
                                SimulatedOrderState::RejectedPostOnlyCross => {
                                    rep.post_only_rejects += 1;
                                    repeg_pending_init_q[s_idx] = false;
                                    entry_fifo[s_idx].reset();
                                    engines[s_idx].state = OrderLifecycleState::Idle;
                                }
                                SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, .. } => {
                                    init_q_ahead[s_idx] = queue_ahead_qty;
                                    repeg_pending_init_q[s_idx] = false;
                                    let _ = engines[s_idx].on_entry_order_resting(cur_snap.ts_local_ns);
                                }
                                SimulatedOrderState::FilledMaker { side: f_side, fill_price, filled_qty, fill_ts_ns } => {
                                    repeg_pending_init_q[s_idx] = false;
                                    gov.clear_abort_streak(s_idx, f_side);
                                    gov.observe_passive_entry_fill(s_idx);
                                    engines[s_idx].sync_governor_for_side(&gov, f_side);
                                    let _ = engines[s_idx].on_maker_entry_filled_verified(
                                        fill_ts_ns,
                                        Some(f_side),
                                        Some(fill_price),
                                        Some(filled_qty),
                                    );
                                    let vol_fill = if engines[s_idx].open_vol_5m_bps.0 > 0 {
                                        engines[s_idx].open_vol_5m_bps
                                    } else {
                                        cur_snap.realized_vol_5m
                                    };
                                    let c_rt_mkr = ScaledBps(engines[s_idx].active_maker_rt_fee_bps());
                                    markout_rings[s_idx].arm_fill_probe_with_vol(
                                        f_side,
                                        fill_price,
                                        mid_now,
                                        fill_ts_ns,
                                        vol_fill,
                                        c_rt_mkr,
                                    );
                                    last_funding_cut[s_idx] = 0;
                                    entry_fifo[s_idx].reset();
                                    exit_fifo[s_idx].reset();
                                    exit_role_latch[s_idx] = ExitOrderRole::None;
                                    if let Some(q0) = engines[s_idx].compute_canonical_exit_quote(&cur_snap) {
                                        sync_replay_exit_fifo_and_state(
                                            &mut engines[s_idx],
                                            &mut exit_fifo[s_idx],
                                            &mut exit_role_latch[s_idx],
                                            f_side,
                                            &q0,
                                            fill_ts_ns,
                                        );
                                    }
                                    rep.fills += 1;
                                }
                            }
                        }
                        OrderLifecycleState::RestingEntryQueue {
                            side,
                            entry_price,
                            qty,
                            target_excursion,
                            sl_trigger_price,
                            resting_since_ts,
                        } => {
                            engines[s_idx].sync_governor_dynamics(&gov);
                            match entry_fifo[s_idx].state {
                                SimulatedOrderState::RejectedPostOnlyCross => {
                                    rep.post_only_rejects += 1;
                                    repeg_pending_init_q[s_idx] = false;
                                    entry_fifo[s_idx].reset();
                                    engines[s_idx].state = OrderLifecycleState::Idle;
                                }
                                SimulatedOrderState::FilledMaker { side: f_side, fill_price, filled_qty, fill_ts_ns } => {
                                    repeg_pending_init_q[s_idx] = false;
                                    gov.clear_abort_streak(s_idx, f_side);
                                    gov.observe_passive_entry_fill(s_idx);
                                    engines[s_idx].sync_governor_for_side(&gov, f_side);
                                    let _ = engines[s_idx].on_maker_entry_filled_verified(
                                        fill_ts_ns,
                                        Some(f_side),
                                        Some(fill_price),
                                        Some(filled_qty),
                                    );
                                    let vol_fill = if engines[s_idx].open_vol_5m_bps.0 > 0 {
                                        engines[s_idx].open_vol_5m_bps
                                    } else {
                                        cur_snap.realized_vol_5m
                                    };
                                    let c_rt_mkr = ScaledBps(engines[s_idx].active_maker_rt_fee_bps());
                                    markout_rings[s_idx].arm_fill_probe_with_vol(
                                        f_side,
                                        fill_price,
                                        mid_now,
                                        fill_ts_ns,
                                        vol_fill,
                                        c_rt_mkr,
                                    );
                                    last_funding_cut[s_idx] = 0;
                                    entry_fifo[s_idx].reset();
                                    exit_fifo[s_idx].reset();
                                    exit_role_latch[s_idx] = ExitOrderRole::None;
                                    if let Some(q0) = engines[s_idx].compute_canonical_exit_quote(&cur_snap) {
                                        sync_replay_exit_fifo_and_state(
                                            &mut engines[s_idx],
                                            &mut exit_fifo[s_idx],
                                            &mut exit_role_latch[s_idx],
                                            f_side,
                                            &q0,
                                            fill_ts_ns,
                                        );
                                    }
                                    rep.fills += 1;
                                }
                                SimulatedOrderState::RestingInFifoQueue { queue_ahead_qty, last_observed_lvl_qty, .. } => {
                                    let eff_resting_since = if repeg_pending_init_q[s_idx] {
                                        init_q_ahead[s_idx] = queue_ahead_qty;
                                        engines[s_idx].apply_resting_repeg(
                                            entry_price,
                                            sl_trigger_price,
                                            cur_snap.ts_local_ns,
                                        );
                                        repeg_pending_init_q[s_idx] = false;
                                        cur_snap.ts_local_ns
                                    } else {
                                        resting_since_ts
                                    };
                                    let elapsed_ns = cur_snap.ts_local_ns.0.saturating_sub(eff_resting_since.0);
                                    let base_chase_ns = gov.max_total_entry_chase_ns(s_idx, cur_snap.realized_vol_5m);
                                    let q_timers = elastic_resting_queue_timers_ns(base_chase_ns);
                                    let total_chase_ns = cur_snap.ts_local_ns.0.saturating_sub(armed_ts_latch[s_idx]);
                                    let (exact_at_l1, live_l1_q) = match side {
                                        Side::Bid => (cur_snap.bids[0].price == entry_price, cur_snap.bids[0].qty),
                                        Side::Ask => (cur_snap.asks[0].price == entry_price, cur_snap.asks[0].qty),
                                    };
                                    let at_l1 = is_at_or_ahead_of_l1(side, entry_price, &cur_snap);
                                    let fav_bbo_miss = crate::kinematic_surface::is_bbo_favorably_past_resting_entry(
                                        side,
                                        entry_price,
                                        &cur_snap,
                                    );
                                    let raw_flow = 0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get();
                                    let flow_imp = raw_flow * side.sign_f64();
                                    queue_flow_ema[s_idx] = crate::execution_fsm::step_intrinsic_slow_flow_ema(
                                        crate::types::BoundedScore::new_clamped(queue_flow_ema[s_idx]),
                                        crate::types::BoundedScore::new_clamped(raw_flow),
                                        slot_dt_ns,
                                        engines[s_idx].intrinsic_tau_mkt_s,
                                        evt_dil,
                                        u64::from(REPLAY_WS_RTT_US),
                                    )
                                    .get();
                                    let signed_slow = crate::types::BoundedScore::new_clamped(
                                        queue_flow_ema[s_idx] * side.sign_f64(),
                                    );
                                    let opp_hit = if matches!(row.par2_kind, Par2EventKind::PublicTradePrint) {
                                        let hits = match side {
                                            Side::Bid => {
                                                row.side == Side::Ask
                                                    && row.price_ticks.0 > 0
                                                    && row.price_ticks.0 <= entry_price.0
                                            }
                                            Side::Ask => row.side == Side::Bid && row.price_ticks.0 >= entry_price.0,
                                        };
                                        if hits { row.lot_qty } else { LotQty(0) }
                                    } else {
                                        LotQty(0)
                                    };
                                    let mut q_shield = engines[s_idx].evaluate_resting_queue_shield_with_beta(
                                        side,
                                        init_q_ahead[s_idx],
                                        queue_ahead_qty,
                                        opp_hit,
                                        &cur_snap,
                                        &btc_snap,
                                        epps_beta,
                                    );
                                    let wall_collapsed = exact_at_l1
                                        && (crate::alpha_3factor::should_abort_l1_cancellation_collapse_v51(
                                            spec,
                                            side,
                                            last_observed_lvl_qty,
                                            live_l1_q,
                                            opp_hit,
                                            entry_price,
                                            signed_slow,
                                            engines[s_idx].active_fees,
                                        ) || (opp_hit.0 == 0
                                            && crate::alpha_3factor::should_abort_collapsed_resting_l1_wall_dynamic(
                                                spec,
                                                side,
                                                entry_price,
                                                &cur_snap,
                                                init_q_ahead[s_idx],
                                                engines[s_idx].learned_taker_prob_pm,
                                                engines[s_idx].active_fees,
                                            )));
                                    if wall_collapsed {
                                        q_shield.abort_now = true;
                                    }
                                    let dt_s = ((elapsed_ns.max(5_000_000) as f64) * 1e-9).max(0.05);
                                    let hwk_lam = crate::exec_daemon::compute_resting_l1_hawkes_lambda_v531(
                                        evt_dil,
                                        opp_hit,
                                        init_q_ahead[s_idx],
                                        dt_s,
                                        q_shield.hawkes_lambda_ratio,
                                        signed_slow,
                                    );
                                    let (max_chase_ns, chase_expired) =
                                        crate::exec_daemon::evaluate_resting_queue_chase_budget_v532(
                                            &gov,
                                            s_idx,
                                            side,
                                            cur_snap.realized_vol_5m,
                                            evt_dil,
                                            hwk_lam,
                                            signed_slow,
                                            init_q_ahead[s_idx],
                                            queue_ahead_qty,
                                            base_chase_ns,
                                            at_l1,
                                            elapsed_ns,
                                            total_chase_ns,
                                        );
                                    let fast_sweep = exact_at_l1
                                        && crate::exec_daemon::evaluate_toxic_sweep_velocity_abort_v51(
                                            spec,
                                            side,
                                            entry_price,
                                            init_q_ahead[s_idx],
                                            queue_ahead_qty,
                                            elapsed_ns,
                                            &cur_snap,
                                            evt_dil,
                                            engines[s_idx].learned_taker_prob_pm,
                                            engines[s_idx].active_fees,
                                            REPLAY_WS_RTT_US,
                                        );
                                    let btc_lead_opt = (s_idx > 0).then(|| {
                                        btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                                    });
                                    let ll_bonus = btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                                        s_idx,
                                        side,
                                        &cur_snap,
                                        &btc_snap,
                                        REPLAY_WS_RTT_US,
                                        epps_beta,
                                        epps_corr,
                                        psi_health,
                                        fees,
                                    );
                                    let v_prio = compute_moallemi_yuan_queue_priority_c1(
                                        init_q_ahead[s_idx],
                                        queue_ahead_qty,
                                    );
                                    let btc_aligned =
                                        epps_beta * (OrthogonalFactorState::snr_composite_score(&btc_snap) * side.sign_f64());
                                    let headwind_erosion = is_resting_headwind_erosion_v554(
                                        s_idx,
                                        side,
                                        elapsed_ns,
                                        q_timers.headwind_abort_ns,
                                        init_q_ahead[s_idx],
                                        queue_ahead_qty,
                                        fast_sweep,
                                        btc_aligned,
                                        flow_imp,
                                        &cur_snap,
                                        signed_slow,
                                    );
                                    let btc_w3 = crate::alpha_3factor::compute_btc_leader_aligned_score(
                                        side,
                                        &cur_snap,
                                        &btc_snap,
                                        if s_idx > 0 { epps_corr } else { 0.0 },
                                        btc_lead_opt,
                                    );
                                    let opp_wave3_now =
                                        crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
                                            side,
                                            &cur_snap,
                                            btc_w3,
                                            psi_health,
                                            gov.slot_eq_vol_5m_bps(s_idx),
                                            engines[s_idx].learned_taker_prob_pm,
                                            engines[s_idx].active_fees,
                                        );
                                    let macro_bad = opp_wave3_now
                                        || (elapsed_ns >= q_timers.macro_abort_ns
                                            && !passes_resting_elliott_macro_hysteresis_c1(
                                                side,
                                                &cur_snap,
                                                &btc_snap,
                                                epps_corr,
                                                btc_lead_opt,
                                                psi_health,
                                                v_prio,
                                            ));
                                    let ob_bad = elapsed_ns >= q_timers.macro_abort_ns
                                        && !passes_order_block_wall_guard_dynamic(
                                            s_idx,
                                            side,
                                            &cur_snap,
                                            engines[s_idx].learned_taker_prob_pm,
                                            engines[s_idx].active_fees,
                                        );
                                    let macro_contradiction = macro_bad || ob_bad;
                                    let geom_flip = crate::exec_daemon::evaluate_elastic_spoof_geom_abort_with_entry_v54(
                                        side,
                                        entry_price,
                                        &cur_snap,
                                        armed_geom_latch[s_idx],
                                        elapsed_ns,
                                        signed_slow,
                                        spoof_d,
                                        evt_dil,
                                        engines[s_idx].learned_taker_prob_pm,
                                        REPLAY_WS_RTT_US,
                                        cur_snap.ts_local_ns.0,
                                        &mut geom_adv_since_ns[s_idx],
                                        init_q_ahead[s_idx],
                                        queue_ahead_qty,
                                    );
                                    let adv_fl_abort = crate::exec_daemon::should_abort_queue_on_adverse_flow_c1(
                                        flow_imp,
                                        queue_flow_ema[s_idx] * side.sign_f64(),
                                        engines[s_idx].learned_taker_prob_pm,
                                        engines[s_idx].learned_ir_x100,
                                        spoof_d,
                                    );
                                    let toxic_q = fast_sweep
                                        || headwind_erosion
                                        || geom_flip
                                        || adv_fl_abort
                                        || opp_wave3_now
                                        || (elapsed_ns >= 25_000_000 && exact_at_l1 && q_shield.abort_now);
                                    let dq_abort = block_new_entries || macro_contradiction || toxic_q;
                                    let fav_miss_ticks = favorable_bbo_miss_ticks(side, entry_price, &cur_snap);
                                    let fav_miss_1_2 = fav_miss_ticks >= 1 && fav_miss_ticks <= 2;
                                    let mut repegged_now = false;
                                    if !dq_abort {
                                        let prop_now = compute_aligned_entry_propulsion_c1(
                                            side,
                                            &cur_snap,
                                            btc_lead_opt.unwrap_or(0.0),
                                        );
                                        let eta_fill = gov.slot_fill_rate_eta(s_idx);
                                        let agile_mode = fav_miss_1_2
                                            || (psi_health > crate::alpha_3factor::INV_PHI_SQ
                                                && fav_bbo_miss
                                                && prop_now > crate::alpha_3factor::INV_PHI_CUBE);
                                        let agile_cd_ns =
                                            compute_agile_repeg_cooldown_ns_v542(u64::from(REPLAY_WS_RTT_US), eta_fill);
                                        let eff_repeg_wait = if agile_mode { agile_cd_ns } else { q_timers.repeg_wait_ns };
                                        let eff_repeg_cd = if agile_mode { agile_cd_ns } else { q_timers.repeg_cooldown_ns };
                                        let repeg_window_open = !at_l1
                                            && cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                                            && !opp_wave3_now
                                            && if fav_miss_1_2 {
                                                (elapsed_ns >= eff_repeg_wait || chase_expired)
                                                    && total_chase_ns < max_chase_ns.saturating_add(base_chase_ns / 2)
                                            } else {
                                                !chase_expired
                                                    && elapsed_ns >= eff_repeg_wait
                                                    && total_chase_ns + eff_repeg_wait < max_chase_ns
                                            };
                                        if repeg_window_open {
                                            let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                                            if let Some((chosen_repeg, _)) = evaluate_resting_repeg_candidate_v554(
                                                &gov,
                                                s_idx,
                                                side,
                                                eq_now,
                                                qty,
                                                entry_price,
                                                engines[s_idx].original_armed_entry_price,
                                                mfe_s,
                                                target_excursion,
                                                engines[s_idx].learned_taker_prob_pm,
                                                engines[s_idx].active_fees,
                                                &cur_snap,
                                                &btc_snap,
                                                spoof_d,
                                                epps_beta,
                                                epps_corr,
                                                btc_lead_opt,
                                                ll_bonus,
                                                psi_health,
                                                prop_now,
                                                v_prio,
                                            ) {
                                                if let Ok(repeg_sl) =
                                                    engines[s_idx].compute_repeg_sl_trigger(&cur_snap, chosen_repeg)
                                                {
                                                    let inside_repeg = match side {
                                                        Side::Bid => {
                                                            cur_snap.bids[0].price.0 > 0
                                                                && chosen_repeg.0 > cur_snap.bids[0].price.0
                                                        }
                                                        Side::Ask => {
                                                            cur_snap.asks[0].price.0 > 0
                                                                && chosen_repeg.0 < cur_snap.asks[0].price.0
                                                        }
                                                    };
                                                    init_q_ahead[s_idx] = if inside_repeg {
                                                        LotQty(0)
                                                    } else {
                                                        live_l1_q.max(LotQty(1))
                                                    };
                                                    repeg_pending_init_q[s_idx] = true;
                                                    engines[s_idx].next_peg_allowed_ts_ns =
                                                        cur_snap.ts_local_ns.0 + eff_repeg_cd;
                                                    engines[s_idx].apply_resting_repeg(
                                                        chosen_repeg,
                                                        repeg_sl,
                                                        cur_snap.ts_local_ns,
                                                    );
                                                    armed_geom_latch[s_idx] = cur_snap.factor_geom.get();
                                                    geom_adv_since_ns[s_idx] = 0;
                                                    queue_flow_ema[s_idx] =
                                                        0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get();
                                                    entry_fifo[s_idx].submit_post_only(
                                                        side,
                                                        chosen_repeg,
                                                        qty,
                                                        cur_snap.ts_local_ns,
                                                    );
                                                    repegged_now = true;
                                                }
                                            }
                                        }
                                    }
                                    if !repegged_now && (chase_expired || dq_abort) {
                                        gov.observe_entry_abort_with_context(
                                            s_idx,
                                            side,
                                            entry_price,
                                            cur_snap.ts_local_ns,
                                            fav_bbo_miss && !block_new_entries,
                                            toxic_q || block_new_entries,
                                        );
                                        let veto_c = if block_new_entries {
                                            LastVetoCode::WarmupOrLock
                                        } else if macro_bad {
                                            LastVetoCode::MacroElliottContradiction
                                        } else if ob_bad {
                                            LastVetoCode::OpposingOrderBlockWall
                                        } else if geom_flip {
                                            LastVetoCode::SpoofingVeto
                                        } else if headwind_erosion {
                                            LastVetoCode::BtcLeadLagHeadwind
                                        } else {
                                            LastVetoCode::HawkesStoikovQueueAbort
                                        };
                                        let arm_ts = crate::types::TimestampNs(if armed_ts_latch[s_idx] > 0 {
                                            armed_ts_latch[s_idx]
                                        } else {
                                            eff_resting_since.0.max(1)
                                        });
                                        markout_rings[s_idx].arm_abort_probe_v49(
                                            side,
                                            entry_price,
                                            veto_c,
                                            arm_ts,
                                            cur_snap.ts_local_ns,
                                            init_q_ahead[s_idx],
                                            queue_ahead_qty,
                                        );
                                        repeg_pending_init_q[s_idx] = false;
                                        entry_fifo[s_idx].reset();
                                        engines[s_idx].state = OrderLifecycleState::Idle;
                                        rep.queue_aborts += 1;
                                    }
                                }
                                SimulatedOrderState::InFlightToExchange { .. } | SimulatedOrderState::Inactive => {}
                            }
                        }
                        OrderLifecycleState::PositionOpenMakerExit {
                            side,
                            avg_price_ticks,
                            mut peak_favorable_price,
                            mut worst_adverse_price,
                            open_lots,
                            sl_trigger_price,
                            opened_ts,
                            ..
                        } => {
                            engines[s_idx].sync_governor_dynamics(&gov);
                            if let Some(f_1e8) = settle_crossed_funding_boundary(
                                spec,
                                side,
                                open_lots,
                                avg_price_ticks,
                                opened_ts.0,
                                &cur_snap,
                                &mut last_funding_cut[s_idx],
                                &mut ledger,
                            ) {
                                rep.funding_settlements += 1;
                                let _ = daily_gov.observe_realized_cashflow(
                                    cur_snap.ts_local_ns,
                                    f_1e8,
                                    false,
                                    false,
                                    false,
                                    ledger.current_equity_1e8(),
                                    ledger.sinking_fund_reserve_bps_x100,
                                    gov.active_maker_rt_fee_bps(),
                                    gov.portfolio_mean_eq_vol_bps(),
                                    gov.portfolio_mean_ir_x100(),
                                );
                            }
                            engines[s_idx].observe_open_flow_kinematics(&cur_snap);
                            let ref_px = if matches!(row.par2_kind, Par2EventKind::PublicTradePrint)
                                && cur_snap.last_trade_price.0 > 0
                            {
                                cur_snap.last_trade_price
                            } else if cur_snap.bids[0].price.0 > 0 && cur_snap.asks[0].price.0 > 0 {
                                mid_now
                            } else if cur_snap.mark_price.0 > 0 {
                                cur_snap.mark_price
                            } else {
                                cur_snap.last_trade_price
                            };
                            let prev_peak = peak_favorable_price;
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
                            engines[s_idx].update_peak_favorable_timestamp_if_improved(
                                prev_peak,
                                peak_favorable_price,
                                side,
                                cur_snap.ts_local_ns,
                            );
                            if let OrderLifecycleState::PositionOpenMakerExit {
                                peak_favorable_price: ref mut pk,
                                worst_adverse_price: ref mut wr,
                                ..
                            } = engines[s_idx].state
                            {
                                *pk = peak_favorable_price;
                                *wr = worst_adverse_price;
                            }
                            let avg_d = avg_price_ticks.0.max(1);
                            engines[s_idx].last_closed_mfe_bps =
                                ((peak_favorable_price.0 - avg_price_ticks.0) * side.sign_i64())
                                    .saturating_mul(1_000_000)
                                    / avg_d;
                            engines[s_idx].last_closed_mae_bps =
                                ((worst_adverse_price.0 - avg_price_ticks.0) * side.sign_i64())
                                    .saturating_mul(1_000_000)
                                    / avg_d;
                            engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                            engines[s_idx].last_closed_worst_ticks = worst_adverse_price;
                            let mk_pk = markout_rings[s_idx].peak_favorable_fill_markout_sbps(opened_ts);
                            if mk_pk > engines[s_idx].last_closed_mfe_bps {
                                engines[s_idx].last_closed_mfe_bps = mk_pk;
                            }
                            let zeta_tox = engines[s_idx].observe_post_fill_toxicity_and_wald_v49(
                                &cur_snap,
                                &btc_snap,
                                epps_beta,
                                &markout_rings[s_idx],
                                evt_dil,
                            );

                            if engines[s_idx].last_eff_sl_ticks.0 <= 0 {
                                engines[s_idx].last_eff_sl_ticks = sl_trigger_price;
                            }
                            let eff_sl_px = engines[s_idx].last_eff_sl_ticks;
                            let sl_hit = match side {
                                Side::Bid => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 <= eff_sl_px.0,
                                Side::Ask => cur_snap.mark_price.0 > 0 && cur_snap.mark_price.0 >= eff_sl_px.0,
                            };
                            let mut closed_opt = None;
                            if sl_hit {
                                let is_chand = engines[s_idx].chandelier_profit_sl_armed
                                    || is_chandelier_profit_locked_sl_with_fees(
                                        side,
                                        avg_price_ticks,
                                        eff_sl_px,
                                        engines[s_idx].active_fees,
                                    );
                                engines[s_idx].last_exit_sub_reason = if is_chand {
                                    ExitSubReason::ChandelierProfitLockSl
                                } else {
                                    ExitSubReason::WaldBaseStopLoss
                                };
                                let sl_slip_px = match side {
                                    Side::Bid => PriceTicks((eff_sl_px.0 - 1).max(1)),
                                    Side::Ask => PriceTicks(eff_sl_px.0.saturating_add(1)),
                                };
                                exit_fifo[s_idx].reset();
                                closed_opt = engines[s_idx].finalize_open_position_close(
                                    ClosedTradeKind::ServerMarkPriceStopLoss,
                                    sl_slip_px,
                                    cur_snap.ts_local_ns,
                                    &mut ledger,
                                );
                            } else if let SimulatedOrderState::FilledMaker { fill_price, fill_ts_ns, .. } =
                                exit_fifo[s_idx].state
                            {
                                exit_fifo[s_idx].reset();
                                let cur_role = match engines[s_idx].state {
                                    OrderLifecycleState::PositionOpenMakerExit { active_exit_role, .. }
                                        if exit_role_latch[s_idx] == ExitOrderRole::None =>
                                    {
                                        active_exit_role
                                    }
                                    _ => exit_role_latch[s_idx],
                                };
                                if (matches!(cur_role, ExitOrderRole::Tranche1Harvest50)
                                    && engines[s_idx].record_tranche1_harvest(fill_price))
                                    || (matches!(cur_role, ExitOrderRole::Tranche2HarvestPhi)
                                        && engines[s_idx].record_tranche2_harvest(fill_price))
                                {
                                    engines[s_idx].next_peg_allowed_ts_ns = 0;
                                } else {
                                    closed_opt = engines[s_idx].finalize_open_position_close(
                                        ClosedTradeKind::MakerPassiveUnwind,
                                        fill_price,
                                        fill_ts_ns,
                                        &mut ledger,
                                    );
                                }
                            }

                            if let Some(summary) = closed_opt {
                                rep.closed += 1;
                                if summary.is_maker_exit {
                                    rep.maker_closes += 1;
                                }
                                let net_r = summary.net_scaled_bps.raw();
                                if net_r < -50 {
                                    rep.losses += 1;
                                } else if net_r >= 100 {
                                    rep.wins += 1;
                                } else {
                                    rep.scratches += 1;
                                }
                                if summary.net_pnl_1e8 >= 0 {
                                    rep.gross_profit_1e8 = rep.gross_profit_1e8.saturating_add(summary.net_pnl_1e8);
                                } else {
                                    rep.gross_loss_1e8 = rep.gross_loss_1e8.saturating_add(summary.net_pnl_1e8.abs());
                                }
                                rep.cum_net_bps_x100 = rep.cum_net_bps_x100.saturating_add(net_r);
                                let non_pun = summary.is_maker_exit || summary.net_scaled_bps.raw() >= 100;
                                let w_close = cur_snap.wave_observables();
                                let signed_prop = 0.50 * cur_snap.factor_ofi.get()
                                    + 0.38 * cur_snap.factor_tfi.get()
                                    + 0.12 * cur_snap.factor_geom.get();
                                gov.set_slot_momentum_chain_context(
                                    s_idx,
                                    signed_prop,
                                    w_close.climax_active,
                                    REPLAY_WS_RTT_US,
                                );
                                gov.observe_closed_trade_with_regime_v51(
                                    s_idx,
                                    summary.side,
                                    summary.entry_price,
                                    summary.net_scaled_bps,
                                    ScaledBps(engines[s_idx].last_closed_mfe_bps),
                                    non_pun,
                                    w_close,
                                    evt_dil,
                                    cur_snap.ts_local_ns,
                                );
                                engines[s_idx].apply_momentum_chain_post_close_cooldown(
                                    cur_snap.ts_local_ns,
                                    non_pun,
                                    summary.net_scaled_bps,
                                    w_close.climax_active
                                        || matches!(w_close.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion),
                                    gov.psi_health(),
                                    signed_prop * summary.side.sign_f64(),
                                    REPLAY_WS_RTT_US,
                                );
                                let _ = daily_gov.observe_realized_cashflow(
                                    cur_snap.ts_local_ns,
                                    summary.net_pnl_1e8,
                                    true,
                                    summary.net_pnl_1e8 > 0,
                                    summary.net_pnl_1e8 < 0,
                                    ledger.current_equity_1e8(),
                                    ledger.sinking_fund_reserve_bps_x100,
                                    gov.active_maker_rt_fee_bps(),
                                    gov.portfolio_mean_eq_vol_bps(),
                                    gov.portfolio_mean_ir_x100(),
                                );
                                let post_upl_1e8 = compute_open_positions_mtm_upl_1e8(&engines, &snaps);
                                let (post_epoch_loss_1e8, _) =
                                    compute_validation_epoch_loss_1e8(&ledger, None, post_upl_1e8);
                                if post_epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
                                    halt_locked = true;
                                }
                                let _ = gov.refresh_psi_health(
                                    &daily_gov,
                                    ledger.sinking_fund_reserve_bps_x100,
                                    post_epoch_loss_1e8,
                                );
                                if verbose {
                                    let net_f = (net_r as f64) / 100.0;
                                    let (col, icon, lbl) = classify_pnl_badge(net_f);
                                    println!(
                                        "  {}{} [REAL PAR2 TRADE #{} {}] {:<10} {:?} | {}t->{}t ({}L) | Net: {:+.2} bps (Maker={}){}",
                                        col,
                                        icon,
                                        rep.closed,
                                        lbl,
                                        spec.symbol_id.name(),
                                        summary.side,
                                        summary.entry_price.0,
                                        summary.exit_price.0,
                                        summary.legs_consolidated,
                                        net_f,
                                        summary.is_maker_exit,
                                        ANSI_RESET
                                    );
                                }
                                continue;
                            }
                            if matches!(exit_fifo[s_idx].state, SimulatedOrderState::RejectedPostOnlyCross) {
                                rep.post_only_rejects += 1;
                                exit_fifo[s_idx].reset();
                            }
                            if let Some(quote) = engines[s_idx].compute_canonical_exit_quote(&cur_snap) {
                                engines[s_idx].last_closed_wald_pct =
                                    engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                                engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                                if quote.defensive_lock_active {
                                    engines[s_idx].latch_defensive_floor_if_needed(true);
                                } else if let OrderLifecycleState::PositionOpenMakerExit {
                                    ref mut defensive_floor_locked,
                                    ..
                                } = engines[s_idx].state
                                {
                                    *defensive_floor_locked = false;
                                }
                                let _ = engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(
                                    &cur_snap,
                                    &btc_snap,
                                    epps_beta,
                                    zeta_tox,
                                );
                                let (rest_tp, rest_q) = exit_fifo[s_idx].resting_price_and_qty();
                                let (preempt, delay_ns) = compute_exit_sync_schedule_elastic(
                                    &quote,
                                    rest_tp,
                                    avg_price_ticks,
                                    REPLAY_WS_RTT_US,
                                    REPLAY_WS_QUOTA_REM,
                                );
                                let peg_ready = preempt || cur_snap.ts_local_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns;
                                let qty_force_sync = rest_tp.0 == 0
                                    || rest_q.0 <= 0
                                    || rest_q.0 > open_lots.0
                                    || rest_q.0.abs_diff(quote.order_lots.0) > 1;
                                if qty_force_sync
                                    || (peg_ready
                                        && should_repeg_resting_exit_tp_with_open_lots(
                                            spec,
                                            side,
                                            mid_now,
                                            rest_tp,
                                            rest_q,
                                            quote.target_price,
                                            quote.order_lots,
                                            quote.urgent_close_repeg,
                                            quote.cur_gross_bps,
                                            engines[s_idx].active_fees,
                                            open_lots,
                                        ))
                                {
                                    engines[s_idx].next_peg_allowed_ts_ns = cur_snap.ts_local_ns.0 + delay_ns;
                                    sync_replay_exit_fifo_and_state(
                                        &mut engines[s_idx],
                                        &mut exit_fifo[s_idx],
                                        &mut exit_role_latch[s_idx],
                                        side,
                                        &quote,
                                        cur_snap.ts_local_ns,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Pass 2: 200.0 Hz Zero-Alloc Multi-Lot Offensive Priority Router (V5.5 O1 + O2 + O4)
            let active_open_or_pending = engines
                .iter()
                .filter(|e| !matches!(e.state, OrderLifecycleState::Idle))
                .count();
            let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(&engines, &snaps);
            let (epoch_loss_1e8, _) = compute_validation_epoch_loss_1e8(&ledger, None, open_upl_1e8);
            if epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
                halt_locked = true;
            }
            let psi_health = gov.refresh_psi_health(
                &daily_gov,
                ledger.sinking_fund_reserve_bps_x100,
                epoch_loss_1e8,
            );
            let block_new_entries = halt_locked || daily_gov.is_hibernating();
            if !block_new_entries && active_open_or_pending < crate::types::MAX_PORTFOLIO_OPEN_SLOTS_TOTAL {
                let fees = gov.active_fee_schedule();
                let eq_now = ledger.current_equity_1e8();
                let mut beta_guard =
                    build_monetary_beta_guard_with_epps(eq_now, &engines, &snaps, &epps_trackers, fees);
                let raw_daily_damper_pm = daily_gov.kelly_boundary_damper_permille(
                    ledger.sinking_fund_reserve_bps_x100,
                    gov.active_maker_rt_fee_bps(),
                    gov.portfolio_mean_eq_vol_bps(),
                    gov.portfolio_mean_ir_x100(),
                );
                let epoch_5u_damper =
                    crate::governor_plasticity::compute_epoch_5u_loss_lot_damper_c1(epoch_loss_1e8);
                let daily_damper_pm = ((raw_daily_damper_pm as f64) * epoch_5u_damper).round() as i64;

                let mut ranked_candidates = [(0_usize, 0_i64); MAX_ACTIVE_PAIRS];
                let mut staged_payloads: [Option<ReplayStagedOffensiveCandidate>; MAX_ACTIVE_PAIRS] =
                    [None; MAX_ACTIVE_PAIRS];
                let mut cand_count = 0_usize;

                for s_idx in 0..MAX_ACTIVE_PAIRS {
                    let o2_btc_wake = btc_updated_in_slice
                        && s_idx > 0
                        && snaps[s_idx].sequence_id > 0
                        && snaps[s_idx].bids[0].price.0 > 0
                        && snaps[s_idx].asks[0].price.0 > 0
                        && slice_latest_ts_ns.saturating_sub(snaps[s_idx].ts_local_ns.0)
                            <= (crate::exec_daemon::SHM_HEARTBEAT_DROP_NS as i64);
                    if (!idle_touched[s_idx] && !o2_btc_wake)
                        || !matches!(engines[s_idx].state, OrderLifecycleState::Idle)
                    {
                        continue;
                    }
                    let mut cur_snap = snaps[s_idx];
                    if !idle_touched[s_idx] && slice_latest_ts_ns > cur_snap.ts_local_ns.0 {
                        cur_snap.ts_local_ns = crate::types::TimestampNs(slice_latest_ts_ns);
                    }
                    if cur_snap.ts_local_ns.0 < engines[s_idx].cooldown_until_ns.0 {
                        continue;
                    }
                    let btc_snap = if snaps[0].bids[0].price.0 > 0 { snaps[0] } else { cur_snap };
                    let spec = APPROVED_SPECS[s_idx];
                    let spoof_d = spoof_clocks[s_idx].spoofing_density();
                    engines[s_idx].hawkes_tracker.set_spoof_density(spoof_d);
                    let epps_corr = if s_idx == 0 { 0.0 } else { epps_trackers[s_idx].correlation(false) };
                    let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(
                        s_idx,
                        &cur_snap,
                        &btc_snap,
                        &epps_trackers[s_idx],
                    );
                    let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
                    let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&cur_snap, spoof_d).get()
                        >= 0.0
                    {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let probe_btc_lead = (s_idx > 0).then(|| {
                        btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, probe_side)
                    });
                    let probe_ll_bonus = btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                        s_idx,
                        probe_side,
                        &cur_snap,
                        &btc_snap,
                        REPLAY_WS_RTT_US,
                        epps_beta,
                        epps_corr,
                        psi_health,
                        fees,
                    );
                    let (dir_edge, _, _) = gov.slot_direction_diagnostics(s_idx, probe_side);
                    let taker_pm = gov.slot_taker_prob_permille(s_idx, probe_side);
                    let probe_ir_x100 = gov.slot_information_ratio_x100(s_idx, probe_side);
                    let probe_tau_mkt_s =
                        CausalElliottWaveState::endogenous_tau_mkt_secs(cur_snap.realized_vol_5m);
                    let probe_kappa_evt = spoof_clocks[s_idx].event_clock_dilation();
                    let mfe_s = ScaledBps(engines[s_idx].learned_mfe_bps);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_v554(
                        spec,
                        &cur_snap,
                        &btc_snap,
                        mfe_s,
                        ScaledBps(dir_edge),
                        taker_pm,
                        probe_ir_x100,
                        probe_tau_mkt_s,
                        fees,
                        spoof_d,
                        epps_beta,
                        epps_corr,
                        probe_btc_lead,
                        psi_health,
                        probe_ll_bonus,
                        probe_kappa_evt,
                    );
                    match verdict {
                        SignalVerdict::SpoofingVeto => {
                            if idle_touched[s_idx] {
                                rep.spoof_vetoes += 1;
                            }
                        }
                        SignalVerdict::AuthorizeMakerEntry {
                            side,
                            target_excursion_scaled_bps,
                        } => {
                            let warm_dt = cur_snap.ts_local_ns.0.saturating_sub(first_ring_ts_ns);
                            if require_ring_warmup && (first_ring_ts_ns <= 0 || warm_dt < 300_000_000_000) {
                                continue;
                            }
                            let s_taker_pm = gov.slot_taker_prob_permille(s_idx, side);
                            let btc_lead_opt = (s_idx > 0).then(|| {
                                btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                            });
                            let ll_bonus = btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                                s_idx,
                                side,
                                &cur_snap,
                                &btc_snap,
                                REPLAY_WS_RTT_US,
                                epps_beta,
                                epps_corr,
                                psi_health,
                                fees,
                            );
                            let cand_px =
                                side.select_entry_bbo_price(cur_snap.bids[0].price, cur_snap.asks[0].price);
                            let is_foll = s_idx > 0 && cur_snap.symbol_id != btc_snap.symbol_id;
                            let btc_ice_al = if is_foll {
                                (if epps_beta < -1e-6 { -1.0 } else { 1.0 })
                                    * OrthogonalFactorState::snr_composite_score(&btc_snap)
                                    * side.sign_f64()
                            } else {
                                0.0
                            };
                            let s_ice = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
                                cur_snap.factor_ofi.get() * side.sign_f64(),
                                cur_snap.factor_tfi.get() * side.sign_f64(),
                                btc_ice_al,
                            );
                            let eff_q_mfe = ScaledBps(
                                ((mfe_s.raw() as f64) * (1.0 - crate::alpha_3factor::INV_PHI * s_ice))
                                    .round() as i64,
                            );
                            if s_ice >= crate::alpha_3factor::INV_PHI
                                || !spec.passes_quantum_ev_growth_gate(
                                    eq_now,
                                    cand_px,
                                    cur_snap.realized_vol_5m,
                                    eff_q_mfe,
                                    s_taker_pm,
                                    fees,
                                )
                            {
                                if idle_touched[s_idx] {
                                    rep.quantum_ev_vetoes += 1;
                                }
                                continue;
                            }
                            if !passes_elliott_macro_coherence_guard_with_health(
                                side,
                                &cur_snap,
                                &btc_snap,
                                epps_corr,
                                btc_lead_opt,
                                psi_health,
                            ) || !passes_order_block_wall_guard_dynamic(
                                s_idx,
                                side,
                                &cur_snap,
                                s_taker_pm,
                                fees,
                            ) || !can_stage_same_direction_entry_v542(
                                s_idx,
                                side,
                                cur_snap.ts_local_ns,
                                &engines,
                                &snaps,
                                &epps_trackers,
                                psi_health,
                                s_taker_pm,
                            ) {
                                continue;
                            }
                            let lots = gov.compute_slot_entry_lots_v48(
                                s_idx,
                                side,
                                eq_now,
                                &cur_snap,
                                daily_damper_pm,
                            );
                            let risk_ppm = slot_position_risk_ppm_with_fees(
                                spec,
                                eq_now,
                                cand_px,
                                lots,
                                cur_snap.realized_vol_5m,
                                fees,
                            );
                            let beta_x1000 = epps_trackers[s_idx].correlated_beta_x1000(
                                s_idx == 0,
                                cur_snap.realized_vol_5m,
                                ScaledBps(btc_snap.realized_vol_5m.0.max(2_500)),
                            );
                            if !beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000)
                                || !crate::exec_daemon::passes_elastic_daily_tail_risk_budget(
                                    s_idx,
                                    side,
                                    risk_ppm,
                                    eq_now,
                                    &daily_gov,
                                    ledger.sinking_fund_reserve_bps_x100,
                                    &engines,
                                    &snaps,
                                    &gov,
                                )
                                || gov
                                    .authorizes_adaptive_entry_v55(
                                        s_idx,
                                        side,
                                        eq_now,
                                        daily_damper_pm,
                                        mfe_s,
                                        &cur_snap,
                                        &btc_snap,
                                        spoof_d,
                                        epps_beta,
                                        epps_corr,
                                        btc_lead_opt,
                                        ll_bonus,
                                    )
                                    .is_err()
                            {
                                continue;
                            }
                            let Some(ev_net_sbps) =
                                gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
                                    s_idx,
                                    side,
                                    lots,
                                    cand_px,
                                    mfe_s,
                                    target_excursion_scaled_bps,
                                    &cur_snap,
                                    &btc_snap,
                                    epps_beta,
                                )
                            else {
                                continue;
                            };
                            let vac_l2_l3 = compute_vacuum_l2_l3_c1(side, &cur_snap);
                            let eta_fill = gov.slot_fill_rate_eta(s_idx);
                            let u_score = compute_offensive_entry_utility_score_x1000_v55(
                                ev_net_sbps,
                                lots,
                                psi_health,
                                vac_l2_l3,
                                eta_fill,
                            );
                            ranked_candidates[cand_count] = (s_idx, u_score);
                            staged_payloads[s_idx] = Some(ReplayStagedOffensiveCandidate {
                                side,
                                verdict,
                                lots,
                                risk_ppm,
                                beta_x1000,
                                epps_corr,
                                btc_lead_opt,
                                ts_local_ns: cur_snap.ts_local_ns.0,
                                geom_raw: cur_snap.factor_geom.get(),
                                init_flow: 0.55 * cur_snap.factor_ofi.get() + 0.45 * cur_snap.factor_tfi.get(),
                            });
                            cand_count += 1;
                        }
                        SignalVerdict::NeutralNoAction => {}
                    }
                }

                sort_offensive_candidates_zero_alloc(&mut ranked_candidates, cand_count);
                for &(s_idx, _) in &ranked_candidates[..cand_count] {
                    if active_open_or_pending >= crate::types::MAX_PORTFOLIO_OPEN_SLOTS_TOTAL {
                        break;
                    }
                    let Some(stg) = staged_payloads[s_idx] else { continue };
                    let mut cur_snap = snaps[s_idx];
                    if stg.ts_local_ns > cur_snap.ts_local_ns.0 {
                        cur_snap.ts_local_ns = crate::types::TimestampNs(stg.ts_local_ns);
                    }
                    let btc_snap = if snaps[0].bids[0].price.0 > 0 { snaps[0] } else { cur_snap };
                    engines[s_idx].sync_governor_for_side(&gov, stg.side);
                    if engines[s_idx]
                        .try_arm_maker_entry_v542(
                            &cur_snap,
                            &btc_snap,
                            stg.verdict,
                            stg.lots,
                            stg.epps_corr,
                            stg.btc_lead_opt,
                            psi_health,
                        )
                        .is_ok()
                        && matches!(engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
                    {
                        armed_ts_latch[s_idx] = stg.ts_local_ns;
                        armed_geom_latch[s_idx] = stg.geom_raw;
                        geom_adv_since_ns[s_idx] = 0;
                        repeg_pending_init_q[s_idx] = false;
                        queue_flow_ema[s_idx] = stg.init_flow;
                        entry_fifo[s_idx].reset();
                        beta_guard.register_slot_open(s_idx, stg.side, stg.risk_ppm, stg.beta_x1000);
                        break;
                    }
                }
            }
            cursor = slice_end;
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
        BtcLeadLagMemory, EppsBtcCorrelationTracker, EventClockAlphaModulator,
        OrthogonalFactorState, SignalVerdict,
    };
    use crate::bybit_gateway::should_repeg_resting_exit_tp_with_open_lots;
    use crate::exec_daemon::{
        build_monetary_beta_guard_with_epps, can_stage_same_direction_entry_v542,
        compute_exit_sync_schedule_elastic, compute_moallemi_yuan_queue_priority_c1,
        compute_open_positions_mtm_upl_1e8, compute_validation_epoch_loss_1e8,
        elastic_resting_queue_timers_ns, evaluate_resting_repeg_candidate_v554,
        favorable_bbo_miss_ticks, is_at_or_ahead_of_l1,
        passes_resting_elliott_macro_hysteresis_c1, slot_position_risk_ppm_with_fees,
    };
    use crate::execution_fsm::{
        compute_agile_repeg_cooldown_ns_v542, compute_aligned_entry_propulsion_c1,
        is_chandelier_profit_locked_sl_with_fees, passes_elliott_macro_coherence_guard_with_health,
        passes_order_block_wall_guard_dynamic, ClosedTradeKind, ExitOrderRole, ExitSubReason,
        OnlineAdaptiveGovernor, OrderLifecycleState, SlotExecutionEngine,
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
        pub btc_lead_memory: BtcLeadLagMemory,
        pub halt_locked: bool,
        pub last_funding_cut: [i64; MAX_ACTIVE_PAIRS],
        pub armed_ts_latch: [i64; MAX_ACTIVE_PAIRS],
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
                btc_lead_memory: BtcLeadLagMemory::new(),
                halt_locked: false,
                last_funding_cut: [0; MAX_ACTIVE_PAIRS],
                armed_ts_latch: [0; MAX_ACTIVE_PAIRS],
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
            if let Some(mk_pk) = self.markout_rings[s_idx].take_uncensored_fill_peak_sbps() {
                self.engines[s_idx].observe_markout_ring_favorable_peak(mk_pk);
            }
            if let Some((mk_side, mk_fav_60s)) = self.markout_rings[s_idx].take_resolved_60s_fill_markout() {
                let _ = self.gov.observe_markout_60s_continuation(
                    s_idx,
                    mk_side,
                    self.engines[s_idx].last_closed_mfe_bps,
                    mk_fav_60s,
                    ev.snap.realized_vol_5m,
                );
            }
            let btc_snap = if self.cached_snaps[0].bids[0].price.0 > 0 { self.cached_snaps[0] } else { ev.snap };
            const TWIN_WS_RTT_US: u32 = 70_000;
            const TWIN_WS_QUOTA_REM: u16 = 100;
            if s_idx == 0 {
                let _ = self.btc_lead_memory.observe_btc_snapshot_with_rtt(&btc_snap, TWIN_WS_RTT_US);
            } else {
                let _ = self.btc_lead_memory.observe_slot_snapshot(s_idx, &ev.snap, TWIN_WS_RTT_US);
            }
            let mid_btc = PriceTicks(i64::midpoint(btc_snap.bids[0].price.0.max(1), btc_snap.asks[0].price.0.max(1)));
            self.epps_trackers[s_idx].observe_5s_pair(ev.snap.ts_local_ns, s_idx == 0, mid_now, mid_btc);
            let epps_corr = if s_idx == 0 { 0.0 } else { self.epps_trackers[s_idx].correlation(false) };
            let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(s_idx, &ev.snap, &btc_snap, &self.epps_trackers[s_idx]);
            let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
            let spoof_d = self.spoof_clocks[s_idx].spoofing_density();
            self.engines[s_idx].hawkes_tracker.set_spoof_density(spoof_d);
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
            let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(&self.engines, &self.cached_snaps);
            let (epoch_loss_1e8, _) = compute_validation_epoch_loss_1e8(&self.ledger, None, open_upl_1e8);
            if epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
                self.halt_locked = true;
            }
            let psi_health = self.gov.refresh_psi_health(
                &self.daily_gov,
                self.ledger.sinking_fund_reserve_bps_x100,
                epoch_loss_1e8,
            );
            let block_new_entries = self.halt_locked || self.daily_gov.is_hibernating();
            if self.halt_locked {
                for k in 0..MAX_ACTIVE_PAIRS {
                    match self.engines[k].state {
                        OrderLifecycleState::PlacingEntryMaker { .. } => {
                            self.entry_fifo[k].cancel();
                            self.engines[k].state = OrderLifecycleState::Idle;
                        }
                        OrderLifecycleState::RestingEntryQueue {
                            side: ab_s,
                            entry_price: ab_px,
                            resting_since_ts: ab_rs,
                            ..
                        } => {
                            let (init_q_k, rem_q_k) = match self.entry_fifo[k].state {
                                OmegaFifoLifecycle::RestingInFifoQueue {
                                    init_queue_ahead,
                                    queue_ahead_lots,
                                    ..
                                } => (init_queue_ahead, queue_ahead_lots),
                                _ => (LotQty(0), LotQty(0)),
                            };
                            let arm_ts_k = TimestampNs(if self.armed_ts_latch[k] > 0 {
                                self.armed_ts_latch[k]
                            } else {
                                ab_rs.0.max(1)
                            });
                            self.entry_fifo[k].cancel();
                            self.gov.observe_entry_abort(k, ab_s, ab_px, now_ns);
                            self.markout_rings[k].arm_abort_probe_v49(
                                ab_s,
                                ab_px,
                                LastVetoCode::WarmupOrLock,
                                arm_ts_k,
                                now_ns,
                                init_q_k,
                                rem_q_k,
                            );
                            self.engines[k].state = OrderLifecycleState::Idle;
                            self.report.hawkes_queue_aborts += 1;
                        }
                        OrderLifecycleState::Idle | OrderLifecycleState::PositionOpenMakerExit { .. } => {}
                    }
                }
            }

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
                    if block_new_entries || now_ns.0 < self.engines[s_idx].cooldown_until_ns.0 { return; }
                    let beta_guard = build_monetary_beta_guard_with_epps(eq_now, &self.engines, &self.cached_snaps, &self.epps_trackers, fees);
                    let probe_side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&ev.snap, spoof_d).get() >= 0.0 { Side::Bid } else { Side::Ask };
                    let probe_btc_lead = (s_idx > 0).then(|| {
                        self.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, probe_side)
                    });
                    let probe_ll_bonus = self.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                        s_idx, probe_side, &ev.snap, &btc_snap, TWIN_WS_RTT_US, epps_beta, epps_corr, psi_health, fees,
                    );
                    let (probe_edge, _, _) = self.gov.slot_direction_diagnostics(s_idx, probe_side);
                    let taker_pm = self.gov.slot_taker_prob_permille(s_idx, probe_side);
                    let probe_ir_x100 = self.gov.slot_information_ratio_x100(s_idx, probe_side);
                    let probe_tau_mkt_s =
                        crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(ev.snap.realized_vol_5m);
                    let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                    let verdict = OrthogonalFactorState::evaluate_entry_signal_v554(
                        spec,
                        &ev.snap,
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
                        psi_health,
                        probe_ll_bonus,
                        evt_dil,
                    );
                    if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
                        let s_taker_pm = self.gov.slot_taker_prob_permille(s_idx, side);
                        let btc_lead_opt = (s_idx > 0).then(|| {
                            self.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                        });
                        let ll_bonus = self.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                            s_idx, side, &ev.snap, &btc_snap, TWIN_WS_RTT_US, epps_beta, epps_corr, psi_health, fees,
                        );
                        let cand_px = side.select_entry_bbo_price(ev.snap.bids[0].price, ev.snap.asks[0].price);
                        let is_foll = s_idx > 0 && ev.snap.symbol_id != btc_snap.symbol_id;
                        let btc_ice_al = if is_foll {
                            (if epps_beta < -1e-6 { -1.0 } else { 1.0 })
                                * OrthogonalFactorState::snr_composite_score(&btc_snap)
                                * side.sign_f64()
                        } else {
                            0.0
                        };
                        let s_ice = crate::alpha_3factor::compute_passive_iceberg_absorption_penalty_c1(
                            ev.snap.factor_ofi.get() * side.sign_f64(),
                            ev.snap.factor_tfi.get() * side.sign_f64(),
                            btc_ice_al,
                        );
                        let eff_q_mfe = ScaledBps(
                            ((mfe_s.raw() as f64) * (1.0 - crate::alpha_3factor::INV_PHI * s_ice)).round() as i64,
                        );
                        if s_ice >= crate::alpha_3factor::INV_PHI
                            || !spec.passes_quantum_ev_growth_gate(eq_now, cand_px, ev.snap.realized_vol_5m, eff_q_mfe, s_taker_pm, fees)
                            || !passes_elliott_macro_coherence_guard_with_health(side, &ev.snap, &btc_snap, epps_corr, btc_lead_opt, psi_health)
                            || !passes_order_block_wall_guard_dynamic(s_idx, side, &ev.snap, s_taker_pm, fees)
                            || !can_stage_same_direction_entry_v542(s_idx, side, now_ns, &self.engines, &self.cached_snaps, &self.epps_trackers, psi_health, s_taker_pm)
                        {
                            return;
                        }
                        let raw_daily_damper_pm = self.daily_gov.kelly_boundary_damper_permille(
                            self.ledger.sinking_fund_reserve_bps_x100, self.gov.active_maker_rt_fee_bps(),
                            self.gov.portfolio_mean_eq_vol_bps(), self.gov.portfolio_mean_ir_x100(),
                        );
                        let epoch_5u_damper = crate::governor_plasticity::compute_epoch_5u_loss_lot_damper_c1(epoch_loss_1e8);
                        let daily_damper_pm = ((raw_daily_damper_pm as f64) * epoch_5u_damper).round() as i64;
                        let lots = self.gov.compute_slot_entry_lots_v48(s_idx, side, eq_now, &ev.snap, daily_damper_pm);
                        let risk_ppm = slot_position_risk_ppm_with_fees(spec, eq_now, cand_px, lots, ev.snap.realized_vol_5m, fees);
                        let beta_x1000 = self.epps_trackers[s_idx].correlated_beta_x1000(
                            s_idx == 0, ev.snap.realized_vol_5m, ScaledBps(btc_snap.realized_vol_5m.0.max(2_500)),
                        );
                        if beta_guard.can_open_with_risk_ppm(s_idx, side, risk_ppm, beta_x1000)
                            && crate::exec_daemon::passes_elastic_daily_tail_risk_budget(
                                s_idx,
                                side,
                                risk_ppm,
                                eq_now,
                                &self.daily_gov,
                                self.ledger.sinking_fund_reserve_bps_x100,
                                &self.engines,
                                &self.cached_snaps,
                                &self.gov,
                            )
                            && self.gov.authorizes_adaptive_entry_v55(s_idx, side, eq_now, daily_damper_pm, mfe_s, &ev.snap, &btc_snap, spoof_d, epps_beta, epps_corr, btc_lead_opt, ll_bonus).is_ok()
                        {
                            self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                            if self.engines[s_idx].try_arm_maker_entry_v542(&ev.snap, &btc_snap, verdict, lots, epps_corr, btc_lead_opt, psi_health).is_ok()
                                && matches!(self.engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
                            {
                                self.armed_ts_latch[s_idx] = now_ns.0;
                            }
                        }
                    }
                }
                OrderLifecycleState::PlacingEntryMaker { side, qty, target_excursion, submitted_ts, .. } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    if block_new_entries { self.engines[s_idx].state = OrderLifecycleState::Idle; return; }
                    if now_ns.0.saturating_sub(submitted_ts.0) >= self.engines[s_idx].elastic_preflight_window_ns(ev.snap.realized_vol_5m) {
                        let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                        let cur_qty = match self.engines[s_idx].state { OrderLifecycleState::PlacingEntryMaker { qty: q, .. } => q, _ => LotQty(1) };
                        let btc_lead_opt = (s_idx > 0).then(|| {
                            self.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                        });
                        let ll_bonus = self.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                            s_idx, side, &ev.snap, &btc_snap, TWIN_WS_RTT_US, epps_beta, epps_corr, psi_health, fees,
                        );
                        if self.gov.confirm_preflight_kinematics_v55(s_idx, side, eq_now, cur_qty, mfe_s, &ev.snap, &btc_snap, spoof_d, epps_beta, epps_corr, btc_lead_opt, ll_bonus).is_ok() {
                            if let Ok((mut synced_px, _)) = self.engines[s_idx].sync_preflight_l1_entry_auto_v542(&ev.snap, &btc_snap, epps_corr, btc_lead_opt, psi_health) {
                                let cur_l1_px = side.select_entry_bbo_price(ev.snap.bids[0].price, ev.snap.asks[0].price);
                                let ev_ok = self.gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(s_idx, side, cur_qty, synced_px, mfe_s, target_excursion, &ev.snap, &btc_snap, epps_beta).is_some()
                                    || (synced_px != cur_l1_px
                                        && self.engines[s_idx].sync_preflight_l1_entry_auto_v542(&ev.snap, &btc_snap, epps_corr, btc_lead_opt, 0.0).map(|(p, _)| synced_px = p).is_ok()
                                        && self.gov.evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(s_idx, side, cur_qty, synced_px, mfe_s, target_excursion, &ev.snap, &btc_snap, epps_beta).is_some());
                                if ev_ok {
                                    if self.armed_ts_latch[s_idx] <= 0 {
                                        self.armed_ts_latch[s_idx] = submitted_ts.0.max(now_ns.0);
                                    }
                                    self.entry_fifo[s_idx].submit_post_only(side, synced_px, qty, now_ns.0);
                                    let _ = self.engines[s_idx].on_entry_order_resting(now_ns);
                                } else {
                                    self.engines[s_idx].state = OrderLifecycleState::Idle;
                                }
                            } else {
                                self.engines[s_idx].state = OrderLifecycleState::Idle;
                            }
                        } else {
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                        }
                    }
                }
                OrderLifecycleState::RestingEntryQueue { side, entry_price, qty, target_excursion, resting_since_ts, .. } => {
                    self.engines[s_idx].sync_governor_dynamics(&self.gov);
                    if let Some((fill_px, fill_lots)) = entry_fill_opt {
                        self.entry_fifo[s_idx].cancel();
                        self.gov.clear_abort_streak(s_idx, side);
                        self.gov.observe_passive_entry_fill(s_idx);
                        self.engines[s_idx].sync_governor_for_side(&self.gov, side);
                        let _ = self.engines[s_idx].on_maker_entry_filled_verified(now_ns, Some(side), Some(fill_px), Some(fill_lots));
                        let vol_fill = if self.engines[s_idx].open_vol_5m_bps.0 > 0 {
                            self.engines[s_idx].open_vol_5m_bps
                        } else {
                            ev.snap.realized_vol_5m
                        };
                        let c_rt_mkr = ScaledBps(self.engines[s_idx].active_maker_rt_fee_bps());
                        self.markout_rings[s_idx].arm_fill_probe_with_vol(side, fill_px, mid_now, now_ns, vol_fill, c_rt_mkr);
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
                        let base_chase_ns = self.gov.max_total_entry_chase_ns(s_idx, ev.snap.realized_vol_5m);
                        let q_timers = elastic_resting_queue_timers_ns(base_chase_ns);
                        let btc_lead_opt = (s_idx > 0).then(|| {
                            self.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                        });
                        let ll_bonus = self.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                            s_idx, side, &ev.snap, &btc_snap, TWIN_WS_RTT_US, epps_beta, epps_corr, psi_health, fees,
                        );
                        let btc_w3 = crate::alpha_3factor::compute_btc_leader_aligned_score(
                            side,
                            &ev.snap,
                            &btc_snap,
                            if s_idx > 0 { epps_corr } else { 0.0 },
                            btc_lead_opt,
                        );
                        let v_prio = compute_moallemi_yuan_queue_priority_c1(
                            init_queue_ahead,
                            queue_ahead_lots,
                        );
                        let opp_wave3_now = crate::alpha_3factor::is_opposing_impulse_wave3_entry_hazard_v552(
                            side,
                            &ev.snap,
                            btc_w3,
                            psi_health,
                            self.gov.slot_eq_vol_5m_bps(s_idx),
                            self.engines[s_idx].learned_taker_prob_pm,
                            self.engines[s_idx].active_fees,
                        );
                        let macro_contradiction = opp_wave3_now
                            || (elapsed_ns >= q_timers.macro_abort_ns
                                && (!passes_resting_elliott_macro_hysteresis_c1(
                                    side,
                                    &ev.snap,
                                    &btc_snap,
                                    epps_corr,
                                    btc_lead_opt,
                                    psi_health,
                                    v_prio,
                                ) || !passes_order_block_wall_guard_dynamic(s_idx, side, &ev.snap, self.engines[s_idx].learned_taker_prob_pm, self.engines[s_idx].active_fees)));
                        let exact_at_l1 = match side {
                            Side::Bid => ev.snap.bids[0].price == entry_price,
                            Side::Ask => ev.snap.asks[0].price == entry_price,
                        };
                        let at_l1 = is_at_or_ahead_of_l1(side, entry_price, &ev.snap);
                        let fav_bbo_miss = crate::kinematic_surface::is_bbo_favorably_past_resting_entry(side, entry_price, &ev.snap);
                        let fav_miss_ticks = favorable_bbo_miss_ticks(side, entry_price, &ev.snap);
                        let fav_miss_1_2 = fav_miss_ticks >= 1 && fav_miss_ticks <= 2;
                        let signed_slow = BoundedScore::new_clamped(
                            (0.55 * ev.snap.factor_ofi.get() + 0.45 * ev.snap.factor_tfi.get()) * side.sign_f64(),
                        );
                        let dt_s = ((elapsed_ns.max(5_000_000) as f64) * 1e-9).max(0.05);
                        let hwk_lam = crate::exec_daemon::compute_resting_l1_hawkes_lambda_v531(
                            evt_dil,
                            opp_tape_lots,
                            init_queue_ahead,
                            dt_s,
                            q_verdict.hawkes_lambda_ratio,
                            signed_slow,
                        );
                        let (max_chase_ns, chase_expired) = crate::exec_daemon::evaluate_resting_queue_chase_budget_v532(
                            &self.gov,
                            s_idx,
                            side,
                            ev.snap.realized_vol_5m,
                            evt_dil,
                            hwk_lam,
                            signed_slow,
                            init_queue_ahead,
                            queue_ahead_lots,
                            base_chase_ns,
                            at_l1,
                            elapsed_ns,
                            elapsed_ns,
                        );
                        let toxic_hwk = (elapsed_ns >= 25_000_000 && exact_at_l1 && q_verdict.abort_now) || opp_wave3_now;
                        let hard_abort = block_new_entries || toxic_hwk || macro_contradiction;
                        if !hard_abort {
                            let prop_now = compute_aligned_entry_propulsion_c1(side, &ev.snap, btc_lead_opt.unwrap_or(0.0));
                            let eta_fill = self.gov.slot_fill_rate_eta(s_idx);
                            let agile_mode = fav_miss_1_2
                                || (psi_health > crate::alpha_3factor::INV_PHI_SQ
                                    && fav_bbo_miss
                                    && prop_now > crate::alpha_3factor::INV_PHI_CUBE);
                            let agile_cd_ns = compute_agile_repeg_cooldown_ns_v542(u64::from(TWIN_WS_RTT_US), eta_fill);
                            let eff_repeg_wait = if agile_mode { agile_cd_ns } else { q_timers.repeg_wait_ns };
                            let eff_repeg_cd = if agile_mode { agile_cd_ns } else { q_timers.repeg_cooldown_ns };
                            let repeg_window_open = !at_l1
                                && now_ns.0 >= self.engines[s_idx].next_peg_allowed_ts_ns
                                && !opp_wave3_now
                                && if fav_miss_1_2 {
                                    (elapsed_ns >= eff_repeg_wait || chase_expired)
                                        && elapsed_ns < max_chase_ns.saturating_add(base_chase_ns / 2)
                                } else {
                                    !chase_expired && elapsed_ns >= eff_repeg_wait
                                };
                            if repeg_window_open {
                                let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                                let cur_qty = match self.engines[s_idx].state { OrderLifecycleState::RestingEntryQueue { qty: q, .. } => q, _ => LotQty(1) };
                                if let Some((chosen_repeg, _)) = evaluate_resting_repeg_candidate_v554(
                                    &self.gov,
                                    s_idx,
                                    side,
                                    eq_now,
                                    cur_qty,
                                    entry_price,
                                    self.engines[s_idx].original_armed_entry_price,
                                    mfe_s,
                                    target_excursion,
                                    self.engines[s_idx].learned_taker_prob_pm,
                                    self.engines[s_idx].active_fees,
                                    &ev.snap,
                                    &btc_snap,
                                    spoof_d,
                                    epps_beta,
                                    epps_corr,
                                    btc_lead_opt,
                                    ll_bonus,
                                    psi_health,
                                    prop_now,
                                    v_prio,
                                ) {
                                    if let Ok(repeg_sl) = self.engines[s_idx].compute_repeg_sl_trigger(&ev.snap, chosen_repeg) {
                                        self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + eff_repeg_cd;
                                        self.engines[s_idx].apply_resting_repeg(chosen_repeg, repeg_sl, now_ns);
                                        self.entry_fifo[s_idx].submit_post_only(side, chosen_repeg, qty, now_ns.0);
                                        return;
                                    }
                                }
                            }
                        }
                        if hard_abort || chase_expired {
                            self.entry_fifo[s_idx].cancel();
                            self.gov.observe_entry_abort_with_context(
                                s_idx,
                                side,
                                entry_price,
                                now_ns,
                                fav_bbo_miss && !block_new_entries,
                                toxic_hwk || block_new_entries,
                            );
                            let veto_c = if block_new_entries {
                                LastVetoCode::WarmupOrLock
                            } else if macro_contradiction {
                                LastVetoCode::MacroElliottContradiction
                            } else {
                                LastVetoCode::HawkesStoikovQueueAbort
                            };
                            let arm_ts = TimestampNs(if self.armed_ts_latch[s_idx] > 0 {
                                self.armed_ts_latch[s_idx]
                            } else {
                                resting_since_ts.0.max(1)
                            });
                            self.markout_rings[s_idx].arm_abort_probe_v49(
                                side,
                                entry_price,
                                veto_c,
                                arm_ts,
                                now_ns,
                                init_queue_ahead,
                                queue_ahead_lots,
                            );
                            self.engines[s_idx].state = OrderLifecycleState::Idle;
                            self.report.hawkes_queue_aborts += 1;
                        }
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side, avg_price_ticks, mut peak_favorable_price, mut worst_adverse_price,
                    open_lots, sl_trigger_price, opened_ts, ..
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
                    let prev_peak = peak_favorable_price;
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
                    self.engines[s_idx].update_peak_favorable_timestamp_if_improved(prev_peak, peak_favorable_price, side, now_ns);
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
                    let mk_pk = self.markout_rings[s_idx].peak_favorable_fill_markout_sbps(opened_ts);
                    if mk_pk > self.engines[s_idx].last_closed_mfe_bps {
                        self.engines[s_idx].last_closed_mfe_bps = mk_pk;
                    }
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
                            let w_close = ev.snap.wave_observables();
                            let signed_prop = 0.50 * ev.snap.factor_ofi.get() + 0.38 * ev.snap.factor_tfi.get() + 0.12 * ev.snap.factor_geom.get();
                            self.gov.set_slot_momentum_chain_context(s_idx, signed_prop, w_close.climax_active, TWIN_WS_RTT_US);
                            self.gov.observe_closed_trade_with_regime_v51(
                                s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                                ScaledBps(self.engines[s_idx].last_closed_mfe_bps), non_pun,
                                w_close, evt_dil, now_ns,
                            );
                            self.engines[s_idx].apply_momentum_chain_post_close_cooldown(
                                now_ns,
                                non_pun,
                                summary.net_scaled_bps,
                                w_close.climax_active || matches!(w_close.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion),
                                self.gov.psi_health(),
                                signed_prop * summary.side.sign_f64(),
                                TWIN_WS_RTT_US,
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
                                let (_, t1_delay_ns) = compute_exit_sync_schedule_elastic(&q2, PriceTicks(0), avg_price_ticks, TWIN_WS_RTT_US, TWIN_WS_QUOTA_REM);
                                self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + t1_delay_ns;
                            }
                            return;
                        }
                        if matches!(cur_role, ExitOrderRole::Tranche2HarvestPhi) && self.engines[s_idx].record_tranche2_harvest(fill_px) {
                            if let Some(q3) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap) {
                                self.engines[s_idx].last_exit_sub_reason = q3.sub_reason;
                                self.exit_fifo[s_idx].submit_post_only(side.opposite(), q3.target_price, q3.order_lots, now_ns.0);
                                if let OrderLifecycleState::PositionOpenMakerExit {
                                    active_exit_role: ref mut ax_r, active_exit_price: ref mut ax_p,
                                    active_exit_lots: ref mut ax_l, resting_exit_price: ref mut rx_p, ..
                                } = self.engines[s_idx].state {
                                    *ax_r = q3.role; *ax_p = q3.target_price; *ax_l = quote_lots_safe(q3.order_lots); *rx_p = q3.target_price;
                                }
                                let (_, t2_delay_ns) = compute_exit_sync_schedule_elastic(&q3, PriceTicks(0), avg_price_ticks, TWIN_WS_RTT_US, TWIN_WS_QUOTA_REM);
                                self.engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + t2_delay_ns;
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
                            let w_close = ev.snap.wave_observables();
                            let signed_prop = 0.50 * ev.snap.factor_ofi.get() + 0.38 * ev.snap.factor_tfi.get() + 0.12 * ev.snap.factor_geom.get();
                            self.gov.set_slot_momentum_chain_context(s_idx, signed_prop, w_close.climax_active, TWIN_WS_RTT_US);
                            self.gov.observe_closed_trade_with_regime_v51(
                                s_idx, summary.side, summary.entry_price, summary.net_scaled_bps,
                                ScaledBps(self.engines[s_idx].last_closed_mfe_bps), true,
                                w_close, evt_dil, now_ns,
                            );
                            self.engines[s_idx].apply_momentum_chain_post_close_cooldown(
                                now_ns,
                                true,
                                summary.net_scaled_bps,
                                w_close.climax_active || matches!(w_close.regime, crate::wave_elliott::WaveRegime::ClimaxExhaustion),
                                self.gov.psi_health(),
                                signed_prop * summary.side.sign_f64(),
                                TWIN_WS_RTT_US,
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
                    let (eff_rest_exit, eff_rest_lots) = match self.exit_fifo[s_idx].state {
                        OmegaFifoLifecycle::InFlightToExchange { limit_price, order_lots, .. }
                        | OmegaFifoLifecycle::RestingInFifoQueue { limit_price, order_lots, .. } => {
                            (limit_price, order_lots)
                        }
                        OmegaFifoLifecycle::Inactive
                        | OmegaFifoLifecycle::FilledMaker { .. }
                        | OmegaFifoLifecycle::PostOnlyRejected { .. } => {
                            self.exit_fifo[s_idx].cancel();
                            (PriceTicks(0), LotQty(0))
                        }
                    };
                    let Some(quote) = self.engines[s_idx].compute_canonical_exit_quote(&ev.snap) else { return; };
                    self.engines[s_idx].last_closed_wald_pct = self.engines[s_idx].last_closed_wald_pct.max(quote.wald_pct);
                    if quote.defensive_lock_active { self.engines[s_idx].latch_defensive_floor_if_needed(true); } else if let OrderLifecycleState::PositionOpenMakerExit { ref mut defensive_floor_locked, .. } = self.engines[s_idx].state { *defensive_floor_locked = false; }
                    let _ = self.engines[s_idx].maybe_ratchet_sl_with_toxicity_v48(&ev.snap, &btc_snap, epps_beta, zeta_tox);
                    self.engines[s_idx].last_exit_sub_reason = quote.sub_reason;
                    let (preempt, delay_ns) = compute_exit_sync_schedule_elastic(
                        &quote,
                        eff_rest_exit,
                        avg_price_ticks,
                        TWIN_WS_RTT_US,
                        TWIN_WS_QUOTA_REM,
                    );
                    let exit_already_crossed = eff_rest_exit.0 > 0
                        && match side {
                            Side::Bid => ev.snap.bids[0].price >= eff_rest_exit,
                            Side::Ask => ev.snap.asks[0].price.0 > 0 && ev.snap.asks[0].price <= eff_rest_exit,
                        };
                    let peg_ready = preempt || now_ns.0 >= self.engines[s_idx].next_peg_allowed_ts_ns;
                    let qty_force_sync = eff_rest_exit.0 == 0
                        || eff_rest_lots.0 <= 0
                        || eff_rest_lots.0 > open_lots.0
                        || eff_rest_lots.0.abs_diff(quote.order_lots.0) > 1;
                    if !exit_already_crossed
                        && (qty_force_sync
                            || (peg_ready
                                && should_repeg_resting_exit_tp_with_open_lots(
                                    spec,
                                    side,
                                    mid_now,
                                    eff_rest_exit,
                                    eff_rest_lots,
                                    quote.target_price,
                                    quote.order_lots,
                                    quote.urgent_close_repeg,
                                    quote.cur_gross_bps,
                                    self.engines[s_idx].active_fees,
                                    open_lots,
                                )))
                    {
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
            let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(&self.engines, &self.cached_snaps);
            let (epoch_loss_1e8, _) = compute_validation_epoch_loss_1e8(&self.ledger, None, open_upl_1e8);
            if epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 {
                self.halt_locked = true;
            }
            let _ = self.gov.refresh_psi_health(
                &self.daily_gov,
                self.ledger.sinking_fund_reserve_bps_x100,
                epoch_loss_1e8,
            );
        }

        /// Ingests a simultaneous `200.0 Hz` market event slice with 1:1 stack-resident multi-lot priority routing (`[(usize, i64); MAX_ACTIVE_PAIRS]`, V5.5 O1).
        pub fn ingest_simultaneous_tick_slice(&mut self, events: &[TwinReplayEvent]) -> Option<usize> {
            const SLICE_RTT_US: u32 = 70_000;
            let mut ranked = [(0_usize, 0_i64); MAX_ACTIVE_PAIRS];
            let mut ev_for_slot: [Option<usize>; MAX_ACTIVE_PAIRS] = [None; MAX_ACTIVE_PAIRS];
            let mut cand_count = 0_usize;
            for (idx, ev) in events.iter().enumerate() {
                if ev.slot_idx >= MAX_ACTIVE_PAIRS {
                    continue;
                }
                if ev.slot_idx == 0 || !matches!(self.engines[ev.slot_idx].state, OrderLifecycleState::Idle) {
                    self.ingest_event(ev);
                } else {
                    ev_for_slot[ev.slot_idx] = Some(idx);
                }
            }
            let eq_now = self.ledger.current_equity_1e8();
            let psi_health = self.gov.psi_health();
            let btc_snap = self.cached_snaps[0];
            let open_upl_1e8 = compute_open_positions_mtm_upl_1e8(&self.engines, &self.cached_snaps);
            let (epoch_loss_1e8, _) = compute_validation_epoch_loss_1e8(&self.ledger, None, open_upl_1e8);
            let raw_damper = self.daily_gov.kelly_boundary_damper_permille(
                self.ledger.sinking_fund_reserve_bps_x100,
                self.gov.active_maker_rt_fee_bps(),
                self.gov.portfolio_mean_eq_vol_bps(),
                self.gov.portfolio_mean_ir_x100(),
            );
            let daily_damper_pm = ((raw_damper as f64)
                * crate::governor_plasticity::compute_epoch_5u_loss_lot_damper_c1(epoch_loss_1e8))
            .round() as i64;
            for s_idx in 0..MAX_ACTIVE_PAIRS {
                let Some(ev_i) = ev_for_slot[s_idx] else { continue };
                let ev = &events[ev_i];
                let spoof_d = self.spoof_clocks[s_idx].spoofing_density();
                let epps_corr = if s_idx == 0 { 0.0 } else { self.epps_trackers[s_idx].correlation(false) };
                let epps_beta = OrthogonalFactorState::epps_correlated_lead_lag_beta(
                    s_idx,
                    &ev.snap,
                    &btc_snap,
                    &self.epps_trackers[s_idx],
                );
                let corr_sign = if s_idx > 0 && epps_corr < 0.0 { -1.0 } else { 1.0 };
                let side = if OrthogonalFactorState::adaptive_snr_from_snapshot(&ev.snap, spoof_d).get() >= 0.0 {
                    Side::Bid
                } else {
                    Side::Ask
                };
                let cand_px = side.select_entry_bbo_price(ev.snap.bids[0].price, ev.snap.asks[0].price);
                let lots = self.gov.compute_slot_entry_lots_v48(s_idx, side, eq_now, &ev.snap, daily_damper_pm);
                let mfe_s = ScaledBps(self.engines[s_idx].learned_mfe_bps);
                let _ = self.btc_lead_memory.observe_slot_snapshot(s_idx, &ev.snap, SLICE_RTT_US);
                let btc_lead_opt = (s_idx > 0).then(|| {
                    self.btc_lead_memory.effective_aligned_impulse(&btc_snap, corr_sign, side)
                });
                let fees = self.gov.active_fee_schedule();
                let ll_bonus = self.btc_lead_memory.compute_lead_lag_ignition_bonus_c1(
                    s_idx, side, &ev.snap, &btc_snap, SLICE_RTT_US, epps_beta, epps_corr, psi_health, fees,
                );
                let (dir_edge, _, _) = self.gov.slot_direction_diagnostics(s_idx, side);
                let taker_pm = self.gov.slot_taker_prob_permille(s_idx, side);
                let ir_x100 = self.gov.slot_information_ratio_x100(s_idx, side);
                let tau_mkt_s =
                    crate::wave_elliott::CausalElliottWaveState::endogenous_tau_mkt_secs(ev.snap.realized_vol_5m);
                let kappa_evt = self.spoof_clocks[s_idx].event_clock_dilation();
                let target_exc = match OrthogonalFactorState::evaluate_entry_signal_v554(
                    APPROVED_SPECS[s_idx],
                    &ev.snap,
                    &btc_snap,
                    mfe_s,
                    ScaledBps(dir_edge),
                    taker_pm,
                    ir_x100,
                    tau_mkt_s,
                    fees,
                    spoof_d,
                    epps_beta,
                    epps_corr,
                    btc_lead_opt,
                    psi_health,
                    ll_bonus,
                    kappa_evt,
                ) {
                    SignalVerdict::AuthorizeMakerEntry {
                        side: sig_s,
                        target_excursion_scaled_bps,
                    } if sig_s == side => target_excursion_scaled_bps,
                    _ => ScaledBps(0),
                };
                let ev_sbps = self
                    .gov
                    .evaluate_slot_pilar2_ex_ante_ev_with_leader_v553_sbps(
                        s_idx, side, lots, cand_px, mfe_s, target_exc, &ev.snap, &btc_snap, epps_beta,
                    )
                    .unwrap_or(0);
                let vac = crate::exec_daemon::compute_vacuum_l2_l3_c1(side, &ev.snap);
                let eta = self.gov.slot_fill_rate_eta(s_idx);
                let u_score = crate::exec_daemon::compute_offensive_entry_utility_score_x1000_v55(
                    ev_sbps, lots, psi_health, vac, eta,
                );
                ranked[cand_count] = (s_idx, u_score);
                cand_count += 1;
            }
            crate::exec_daemon::sort_offensive_candidates_zero_alloc(&mut ranked, cand_count);
            let mut winner = None;
            for &(s_idx, _) in &ranked[..cand_count] {
                if let Some(ev_i) = ev_for_slot[s_idx] {
                    self.ingest_event(&events[ev_i]);
                    if winner.is_none()
                        && matches!(self.engines[s_idx].state, OrderLifecycleState::PlacingEntryMaker { .. })
                    {
                        winner = Some(s_idx);
                    }
                }
            }
            winner
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

        let sec_title = vec![format!("{ANSI_SKY_BLUE}[VORTEX-REPLAY OMEGA V5.5] 4-ARM PARETO ABLATION & 1:1 PAR2 DIGITAL TWIN GATE{ANSI_RESET}")];
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

    #[test]
    fn test_v551_parquet_walk_forward_o1_priority_o2_btc_o3_3tranche_and_o7_aging() {
        use crate::types::{BoundedScore, TimestampNs};
        let dir = std::env::temp_dir().join(format!("vortex_wf_v551_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        assert!(fs::create_dir_all(&dir).is_ok());

        let mk_row = |slot: usize,
                      px: i64,
                      b_q: i64,
                      a_q: i64,
                      l2_ask_q: i64,
                      ofi: f64,
                      tfi: f64,
                      geom: f64,
                      ts: i64,
                      kind: MarketEventKind,
                      tr_side: Side,
                      tr_px: i64,
                      tr_q: i64|
         -> ColumnarMarketRow {
            let mut s = SymbolLobSnapshot::new_empty();
            s.symbol_id = APPROVED_SPECS[slot].symbol_id;
            s.is_eligible = true;
            s.sequence_id = (ts.unsigned_abs() / 1_000_000) + 1;
            s.ts_exchange_ns = TimestampNs(ts);
            s.ts_local_ns = TimestampNs(ts);
            s.realized_vol_5m = ScaledBps(6_800);
            for lvl in 0..5_usize {
                s.bids[lvl] = crate::lob_shm::BookLevel {
                    price: PriceTicks(px - lvl as i64),
                    qty: LotQty(if lvl == 0 { b_q } else { 400 }),
                };
                s.asks[lvl] = crate::lob_shm::BookLevel {
                    price: PriceTicks(px + 1 + lvl as i64),
                    qty: LotQty(if lvl == 0 { a_q } else { l2_ask_q }),
                };
            }
            s.refresh_deep_l1_l5_totals();
            s.mark_price = PriceTicks(px);
            s.last_trade_price = PriceTicks(tr_px);
            s.factor_ofi = BoundedScore::new_clamped(ofi);
            s.factor_tfi = BoundedScore::new_clamped(tfi);
            s.factor_geom = BoundedScore::new_clamped(geom);
            ColumnarMarketRow::from_lob_snapshot(&s, kind, tr_side, PriceTicks(tr_px), LotQty(tr_q))
        };

        // Prime at NYC bell UTC so temporal cadence is prime:
        let t0 = (20_776_i64 * 86_400 + 14 * 3_600 + 2_700) * 1_000_000_000;
        let mut batch = ColumnarParquetBatch::new();
        // Simultaneous 5ms slice: Slot 1 (ETH, 1-lot indivisible, heavy ask book) appears BEFORE
        // Slot 3 (AVAX, multi-lot divisible, thin L2/L3 ask vacuum) and Slot 0 (BTC bullish impulse).
        // O1 + O2 must prioritize AVAX (Slot 3) over ETH (Slot 1) and run a full 3-tranche Maker cascade!
        assert!(batch
            .push_row(&mk_row(1, 223_700, 140, 260, 900, 0.80, 0.84, 0.58, t0, MarketEventKind::L2BookSnapshot, Side::Bid, 223_700, 140))
            .is_ok());
        assert!(batch
            .push_row(&mk_row(3, 8_200, 120, 180, 70, 0.82, 0.86, 0.62, t0 + 1_000_000, MarketEventKind::L2BookSnapshot, Side::Bid, 8_200, 120))
            .is_ok());
        assert!(batch
            .push_row(&mk_row(0, 652_000, 600, 320, 300, 0.72, 0.76, 0.52, t0 + 2_000_000, MarketEventKind::L2BookSnapshot, Side::Bid, 652_000, 600))
            .is_ok());
        for dt in [45_000_000_i64, 85_000_000, 120_000_000] {
            assert!(batch
                .push_row(&mk_row(3, 8_200, 120, 180, 70, 0.82, 0.86, 0.62, t0 + dt, MarketEventKind::L2BookSnapshot, Side::Bid, 8_200, 120))
                .is_ok());
        }
        // Passive entry fill on AVAX Bid @ 8_200:
        assert!(batch
            .push_row(&mk_row(3, 8_200, 120, 180, 70, 0.82, 0.86, 0.62, t0 + 160_000_000, MarketEventKind::PublicTradePrint, Side::Ask, 8_200, 200))
            .is_ok());
        // Step up through T1 (~8_205), T2 (~8_209..8_212), and T3 crest harvest (~8_235):
        for &(dt, px, ofi, tfi) in &[
            (220_000_000_i64, 8_204_i64, 0.78_f64, 0.82_f64),
            (260_000_000, 8_206, 0.80, 0.84),
            (310_000_000, 8_208, 0.82, 0.85),
            (460_000_000, 8_211, 0.84, 0.86),
            (520_000_000, 8_214, 0.84, 0.86),
            (700_000_000, 8_232, 0.75, 0.78),
            (900_000_000, 8_235, 0.02, 0.01),
            (1_050_000_000, 8_235, 0.01, 0.00),
            (1_200_000_000, 8_236, 0.01, 0.00),
        ] {
            assert!(batch
                .push_row(&mk_row(3, px, 280, 80, 60, ofi, tfi, 0.35, t0 + dt, MarketEventKind::L2BookSnapshot, Side::Bid, px, 280))
                .is_ok());
            assert!(batch
                .push_row(&mk_row(3, px, 280, 80, 60, ofi, tfi, 0.35, t0 + dt + 30_000_000, MarketEventKind::PublicTradePrint, Side::Bid, px + 1, 150))
                .is_ok());
        }

        struct HermeticCwdVkl1Fixture {
            created_data_dir: bool,
            created_candles_dir: bool,
        }
        impl Drop for HermeticCwdVkl1Fixture {
            fn drop(&mut self) {
                if self.created_candles_dir {
                    let _ = fs::remove_dir_all("data/candles");
                }
                if self.created_data_dir {
                    let _ = fs::remove_dir("data");
                }
            }
        }
        let cwd_data_existed = Path::new("data").exists();
        let cwd_candles_existed = Path::new("data/candles").exists();
        let cwd_guard = HermeticCwdVkl1Fixture {
            created_data_dir: !cwd_data_existed,
            created_candles_dir: !cwd_candles_existed,
        };
        if !cwd_candles_existed {
            let cwd_store = PerpetualCandleStore::default_store();
            let mut list = Vec::with_capacity(64);
            for k in (1..=64_i64).rev() {
                let open_ms = ((t0 - k * 300_000_000_000) / 1_000_000).to_string();
                let px_hi = 28.50_f64 - (64 - k) as f64 * 0.14;
                let px_lo = px_hi - 0.35;
                list.push(serde_json::json!([
                    open_ms,
                    format!("{px_hi:.4}"),
                    format!("{:.4}", px_hi + 0.05),
                    format!("{px_lo:.4}"),
                    format!("{:.4}", px_lo + 0.02),
                    "25000.0",
                    "600000.0"
                ]));
            }
            let adv_bundle = serde_json::json!({ "result": { "list": list } });
            let _ = cwd_store.backfill_from_bybit_kline_json(
                3,
                CandleTimeframe::Min5,
                &adv_bundle,
            );
            assert!(store_has_any_vkl1(&cwd_store));
        }

        let seg_p = dir.join("vortex_l2_segment_000001.parquet");
        assert!(batch.write_to_file(&seg_p).is_ok());
        let rep = run_parquet_walk_forward_on_dir(&dir, false);
        assert_eq!(rep.files_count, 1);
        assert!(rep.armed >= 1 && rep.fills >= 1 && rep.closed >= 1);
        assert_eq!(rep.losses, 0);
        assert!(rep.cum_net_pnl_1e8 > 0 && rep.cum_net_bps_x100 > 0);
        drop(cwd_guard);
        let _ = fs::remove_dir_all(&dir);
    }
}
