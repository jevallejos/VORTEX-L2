//! Tear-Free SHM v15 Live HUD, Full 360 Executive Dashboard, Daily Shield & Zero-Alloc Vault Dispatch (V5.4).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::suboptimal_flops,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use core::sync::atomic::Ordering;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::thread;

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict, INV_PHI_FOURTH};
use crate::bybit_client::{BybitV5LiveClient, HANDSHAKE_VAULT_CACHE_PATH};
use crate::execution_fsm::{
    defensive_zero_loss_maker_exit_price_with_fees, dynamic_maker_floor_price, ClosedTradeKind,
    ClosedTradeSummary, ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState,
    SlotExecutionEngine,
};
use crate::exit_quoter::compute_elastic_offensive_win_floor_bps;
use crate::fee_cadence_beta::FeeSchedule;
use crate::forensic_parquet::{ColumnarParquetBatch, ParitySyncLedger};
use crate::governor_plasticity::{DailySessionGovernor, DailyShieldPhase, DAILY_GOVERNOR_VAULT_PATH};
use crate::http_ws_transport::{load_dotenv_map, parse_endpoint_url, KeepAliveHttpConn};
use crate::lob_shm::{LastVetoCode, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::MarkoutCurveBps;
use crate::types::{
    classify_trade_badge, now_epoch_ns, print_auto_aligned_box, side_from_str,
    slot_from_symbol_name, visible_terminal_width, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TimestampNs, ANSI_BLUE, ANSI_DARK_GREEN, ANSI_GRAY, ANSI_GREEN, ANSI_ORANGE, ANSI_RED,
    ANSI_RESET, ANSI_SKY_BLUE, ANSI_YELLOW, APPROVED_SPECS,
};
use crate::vault_io::{
    atomic_write_bytes_sync, is_catastrophic_loss_breach, load_daily_governor_from_path,
    AtomicLockWatch, ClosedTradePersistTask, OpenPositionsPersistTask,
};

pub use crate::vault_io::VaultIoCommand;

pub const DEFAULT_WALLET_EQUITY_1E8: i64 = 6_628_080_000;
pub const FALLBACK_NOTIONAL_1E8: i64 = 1_922_140_000;
pub const TELEGRAM_QUEUE_CAP: usize = 64;
pub const AI_FORENSIC_VAULT_PATH: &str = "data/vault/vortex_ai_forensic_vault.jsonl";
pub const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";
pub const SCAR_MEMORY_VAULT_PATH: &str = "data/vault/vortex_governor_scar_memory.jsonl";
pub const OPEN_POS_STATE_PATH: &str = "data/vault/vortex_open_positions_state.json";
pub const DRAIN_LOCK_PATH: &str = "data/vault/DRAIN_MODE.lock";
pub const HALT_LOCK_PATH: &str = "data/vault/HALTED_ON_LOSS.lock";
pub const DEFAULT_LEARNED_MFE_SCALED_BPS: i64 = 613;
pub const SCAR_MFE_REHYDRATE_CAP_MULTIPLIER: i64 = 12;
pub const SCAR_MFE_REHYDRATE_CAP_SCALED_BPS: i64 =
    SCAR_MFE_REHYDRATE_CAP_MULTIPLIER * (ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2);
const BPS_TO_SCALED_BPS_F64: f64 = 100.0;
const LEGACY_SCAR_MAKER_FEE_RT_BPS: f64 =
    ((ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2) as f64) / BPS_TO_SCALED_BPS_F64;
const SCAR_MFE_REHYDRATE_FLOOR_BPS: f64 =
    ((ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 * 2 + ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 / 4) as f64)
        / BPS_TO_SCALED_BPS_F64;

#[inline]
#[must_use]
pub fn load_hud_fee_schedule() -> FeeSchedule {
    let Ok(txt) = fs::read_to_string(HANDSHAKE_VAULT_CACHE_PATH) else { return FeeSchedule::VIP0_DEFAULT };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) else { return FeeSchedule::VIP0_DEFAULT };
    match (
        val.get("maker_fee_scaled_bps").and_then(serde_json::Value::as_i64),
        val.get("taker_fee_scaled_bps").and_then(serde_json::Value::as_i64),
    ) {
        (Some(m), Some(t)) => FeeSchedule::from_live_rates_clamped(ScaledBps(m), ScaledBps(t)),
        _ => FeeSchedule::VIP0_DEFAULT,
    }
}

#[inline]
#[must_use]
pub const fn format_veto_badge(code: LastVetoCode) -> &'static str {
    match code {
        LastVetoCode::None => "NONE_CLEAR",
        LastVetoCode::WarmupOrLock => "WARMUP_LOCK",
        LastVetoCode::BetaOrSameSideStagger => "BETA_STAGGER",
        LastVetoCode::SpoofingVeto => "SPOOF_VETO",
        LastVetoCode::Friction3xOrEligibility => "FRICTION_3X",
        LastVetoCode::QueueDepthOrOversizedLot => "QUANTUM_EV",
        LastVetoCode::KinematicSurfaceOrCadence => "KIN_CADENCE",
        LastVetoCode::ScarCooldownOrContagion => "SCAR_CD",
        LastVetoCode::BtcLeadLagHeadwind => "BTC_HEADW",
        LastVetoCode::MacroElliottContradiction => "MACRO_C1",
        LastVetoCode::OpposingOrderBlockWall => "OB_WALL",
        LastVetoCode::PreflightDriftOrDecay => "PREFL_DRIFT",
        LastVetoCode::HawkesStoikovQueueAbort => "HAWKES_ABT",
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct SlotForensicEntrySnapshot {
    pub armed_ts_ns: i64, pub init_q_lots: i64, pub ofi_in: f64, pub tfi_in: f64,
    pub geom_in: f64, pub snr_in: f64, pub vol5m_in: f64, pub spread_in: i64,
    pub btc_comp_in: f64, pub gov_ewma_bps: f64, pub win_streak: u32, pub sl_streak: u32,
    pub drift_bps: f64, pub ext_bps: f64, pub kin_accel_net_bps: f64,
    pub kin_energy_in_bps: f64, pub req_kin_energy_in_bps: f64, pub ev_ex_ante_bps: f64,
}

#[must_use]
pub fn spawn_bounded_telegram_notifier() -> SyncSender<String> {
    let (tx, rx) = mpsc::sync_channel::<String>(TELEGRAM_QUEUE_CAP);
    let env_map = load_dotenv_map();
    let token = env_map.get("TELEGRAM_BOT_TOKEN").cloned().unwrap_or_default();
    let chat_id = env_map.get("TELEGRAM_CHAT_ID").cloned().unwrap_or_default();
    let tg_base = env_map.get("TELEGRAM_API_BASE").cloned().unwrap_or_else(|| "https://api.telegram.org".to_string());
    thread::spawn(move || {
        let mut conn_opt = parse_endpoint_url(&tg_base).map(KeepAliveHttpConn::new);
        let path = format!("/bot{token}/sendMessage");
        while let Ok(msg) = rx.recv() {
            if token.is_empty() || chat_id.is_empty() { continue; }
            if let Some(ref mut conn) = conn_opt {
                let body = serde_json::json!({ "chat_id": chat_id, "text": msg }).to_string();
                let _ = conn.send_json_request("POST", &path, &[], &body);
            }
        }
    });
    tx
}

#[must_use]
pub fn serialize_open_positions_state(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
) -> Option<String> {
    let mut arr = Vec::new();
    for eng in engines {
        if let OrderLifecycleState::PositionOpenMakerExit {
            side, entry_price, avg_price_ticks, peak_favorable_price, worst_adverse_price,
            initial_lots, open_lots, tranche1_harvested, tranche1_lots, tranche1_exit_price,
            defensive_floor_locked, structural_tp_price, resting_exit_price, sl_trigger_price, opened_ts, ..
        } = eng.state {
            let ef = entry_forensics[eng.slot_idx];
            arr.push(serde_json::json!({
                "slot_idx": eng.slot_idx, "side": if matches!(side, Side::Ask) { "Ask" } else { "Bid" },
                "entry_ticks": entry_price.0, "avg_ticks": avg_price_ticks.0,
                "peak_ticks": peak_favorable_price.0, "worst_ticks": worst_adverse_price.0,
                "initial_lots": initial_lots.0, "open_lots": open_lots.0,
                "tranche1_harvested": tranche1_harvested, "tranche1_lots": tranche1_lots.0,
                "tranche1_exit_ticks": tranche1_exit_price.0, "defensive_floor_locked": defensive_floor_locked,
                "structural_tp_ticks": structural_tp_price.0, "tp_ticks": resting_exit_price.0,
                "sl_ticks": sl_trigger_price.0, "opened_ts_ns": opened_ts.0,
                "armed_ts_ns": ef.armed_ts_ns, "init_q_lots": ef.init_q_lots,
                "ofi_in": ef.ofi_in, "tfi_in": ef.tfi_in, "geom_in": ef.geom_in, "snr_in": ef.snr_in,
                "vol5m_in": ef.vol5m_in, "spread_in": ef.spread_in, "btc_comp_in": ef.btc_comp_in,
                "gov_ewma_bps": ef.gov_ewma_bps, "win_streak": ef.win_streak, "sl_streak": ef.sl_streak,
                "drift_bps": ef.drift_bps, "ext_bps": ef.ext_bps, "kin_accel_net_bps": ef.kin_accel_net_bps,
                "kin_energy_in_bps": ef.kin_energy_in_bps, "req_kin_energy_in_bps": ef.req_kin_energy_in_bps,
                "ev_ex_ante_bps": ef.ev_ex_ante_bps,
            }));
        }
    }
    serde_json::to_string_pretty(&arr).ok()
}

pub fn dispatch_open_positions_state(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    vault_tx: Option<&SyncSender<VaultIoCommand>>,
) {
    if let Some(tx) = vault_tx {
        let cmd = VaultIoCommand::SaveOpenPositions(Box::new(OpenPositionsPersistTask {
            engines: engines.clone(),
            entry_forensics: *entry_forensics,
        }));
        if tx.try_send(cmd).is_ok() { return; }
    }
    if let Some(txt) = serialize_open_positions_state(engines, entry_forensics) {
        let _ = atomic_write_bytes_sync(Path::new(OPEN_POS_STATE_PATH), txt.as_bytes());
    }
}

#[must_use]
pub fn load_saved_slot_state(slot_idx: usize) -> Option<serde_json::Value> {
    let txt = fs::read_to_string(OPEN_POS_STATE_PATH).ok()?;
    let arr = serde_json::from_str::<Vec<serde_json::Value>>(&txt).ok()?;
    arr.into_iter().find(|v| v.get("slot_idx").and_then(serde_json::Value::as_u64) == Some(slot_idx as u64))
}

#[must_use]
pub fn restore_entry_forensics_from_json(saved: &serde_json::Value, opened_ns: i64, init_lots: i64) -> SlotForensicEntrySnapshot {
    let f64_at = |k: &str| saved.get(k).and_then(serde_json::Value::as_f64).unwrap_or(0.0);
    let i64_at = |k: &str, d: i64| saved.get(k).and_then(serde_json::Value::as_i64).unwrap_or(d);
    let u32_at = |k: &str| saved.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0).min(999) as u32;
    SlotForensicEntrySnapshot {
        armed_ts_ns: i64_at("armed_ts_ns", opened_ns), init_q_lots: i64_at("init_q_lots", init_lots),
        ofi_in: f64_at("ofi_in"), tfi_in: f64_at("tfi_in"), geom_in: f64_at("geom_in"), snr_in: f64_at("snr_in"),
        vol5m_in: f64_at("vol5m_in"), spread_in: i64_at("spread_in", 1), btc_comp_in: f64_at("btc_comp_in"),
        gov_ewma_bps: f64_at("gov_ewma_bps"), win_streak: u32_at("win_streak"), sl_streak: u32_at("sl_streak"),
        drift_bps: f64_at("drift_bps"), ext_bps: f64_at("ext_bps"), kin_accel_net_bps: f64_at("kin_accel_net_bps"),
        kin_energy_in_bps: f64_at("kin_energy_in_bps"), req_kin_energy_in_bps: f64_at("req_kin_energy_in_bps"),
        ev_ex_ante_bps: f64_at("ev_ex_ante_bps"),
    }
}

pub fn record_and_notify_entry_fill(
    s_idx: usize, spec: PairSpec, engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS], arena: &VortexShmArena,
    tg_tx: &SyncSender<String>, vault_tx: &SyncSender<VaultIoCommand>, now_ns: TimestampNs,
    eff_side: Side, eff_entry_px: PriceTicks, eff_qty: LotQty, tag: &str,
) {
    let entry_f = &entry_forensics[s_idx];
    arena.exec_fill_count.fetch_add(1, Ordering::Relaxed);
    engines[s_idx].sync_to_shm_telemetry(arena);
    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
    let sym_name = spec.symbol_id.name();
    let (en_w, en_f) = spec.format_price_usdt_parts(eff_entry_px);
    let eff_win_floor = compute_elastic_offensive_win_floor_bps(engines[s_idx].active_maker_rt_fee_bps(), engines[s_idx].open_vol_5m_bps);
    let win_fl_t = dynamic_maker_floor_price(eff_side, eff_entry_px, eff_win_floor);
    let (tp_w, tp_f) = spec.format_price_usdt_parts(win_fl_t);
    let (sl_w, sl_f) = spec.format_price_usdt_parts(engines[s_idx].last_eff_sl_ticks);
    let q_ms = (now_ns.0.saturating_sub(entry_f.armed_ts_ns)).max(0) / 1_000_000;
    let qty_s = spec.format_lot_qty_str(eff_qty);
    println!(
        "{ANSI_YELLOW}\u{1f7e1} [{tag}] {sym_name} {eff_side:?} qty={qty_s} filled passively @ {en_w}.{en_f:04} USDT ({}t | WIN_TP: {tp_w}.{tp_f:04} | SL: {sl_w}.{sl_f:04} | Wait: {q_ms}ms){ANSI_RESET}",
        eff_entry_px.0
    );
    let _ = tg_tx.try_send(format!(
        concat!(
            "\u{1f7e1} [VORTEX-OMEGA V5.4 {}]\n",
            "\u{2022} Par y Lado : {} ({:?}) | Lote: {}\n",
            "\u{2022} Entrada L1 : {}.{:04} USDT ({}t) | Cola: {} ms\n",
            "\u{2022} Techos/SL  : WIN Floor: {}.{:04} USDT | SL: {}.{:04} USDT\n",
            "\u{2022} Microestr. : OFI:{:+.2} | TFI:{:+.2} | GEOM:{:+.2} | SNR:{:+.2} | Vol5m:{:.0}bp"
        ),
        tag, sym_name, eff_side, qty_s, en_w, en_f, eff_entry_px.0, q_ms, tp_w, tp_f,
        sl_w, sl_f, entry_f.ofi_in, entry_f.tfi_in, entry_f.geom_in, entry_f.snr_in, entry_f.vol5m_in
    ));
}

#[must_use]
pub fn extract_vault_row_pnl_and_fee_1e8(
    v: &serde_json::Value, s_idx_opt: Option<usize>, side: Side,
    net_sc: ScaledBps, fee_sc: ScaledBps, fees: FeeSchedule,
) -> (i64, i64, PriceTicks) {
    let en_t = PriceTicks(v.get("entry_ticks").and_then(serde_json::Value::as_i64).unwrap_or(0));
    let ex_t = PriceTicks(v.get("exit_ticks").and_then(serde_json::Value::as_i64).unwrap_or(0));
    if let (Some(pnl), Some(fee)) = (
        v.get("net_pnl_1e8").and_then(serde_json::Value::as_i64),
        v.get("fee_1e8").and_then(serde_json::Value::as_i64),
    ) {
        return (pnl, fee, en_t);
    }
    if let Some(s_idx) = s_idx_opt {
        if en_t.0 > 0 && ex_t.0 > 0 {
            let spec = APPROVED_SPECS[s_idx];
            let lots_raw = v.get("closed_lots").and_then(serde_json::Value::as_i64).unwrap_or(0);
            let lots = if lots_raw > 0 {
                LotQty(lots_raw)
            } else {
                spec.compute_risk_parity_kelly_lots_with_fees(
                    DEFAULT_WALLET_EQUITY_1E8, en_t, ScaledBps(6_000),
                    fees.round_trip_maker_exit_bps().raw().max(100), 33, 1_000, false, fees,
                )
            };
            let (pnl_1e8, fee_1e8, _, _) = spec.compute_leg_pnl_and_fee_1e8(side, lots, en_t, ex_t, fee_sc);
            return (pnl_1e8, fee_1e8, en_t);
        }
    }
    let pnl_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(net_sc.raw())) / 1_000_000;
    let fee_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(fee_sc.raw())) / 1_000_000;
    (pnl_fb, fee_fb, en_t)
}

#[must_use]
pub fn format_telegram_close_message(
    spec: PairSpec, summary: &ClosedTradeSummary, sub_reason: ExitSubReason,
    trade_num: u64, ledger: &ParitySyncLedger,
) -> String {
    let sym_name = spec.symbol_id.name();
    let net_bps_f = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let gross_f = (summary.gross_scaled_bps.raw() as f64) / 100.0;
    let fee_f = (summary.fee_scaled_bps.raw() as f64) / 100.0;
    let (en_w, en_f) = spec.format_price_usdt_parts(summary.entry_price);
    let (ex_w, ex_f) = spec.format_price_usdt_parts(summary.exit_price);
    let pnl_usdt = (summary.net_pnl_1e8 as f64) / 100_000_000.0;
    let qty_s = spec.format_lot_qty_str(summary.closed_lots);
    let is_partial = matches!(sub_reason, ExitSubReason::Tranche1Harvest50) && net_bps_f < 1.00;
    let (_, icon, lbl) = classify_trade_badge(net_bps_f, is_partial);
    format!(
        concat!(
            "{} [VORTEX-OMEGA V5.4 {} #{} \u{2014} {:?}]\n",
            "- Ganancia : {:+.2}% @10x | {:+.4} USDT\n",
            "- Par y Lado : {} ({:?}) qty={} ({}L) | Maker: {}\n",
            "- Precios    : {}.{:04} -> {}.{:04} USDT ({}t -> {}t)\n",
            "- Balance    : Bruto: {:+.2} bps | Fee: {:+.2} bps | Neto: {:+.2} bps\n",
            "- Boveda 70/30: Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps"
        ),
        icon, lbl.trim(), trade_num, sub_reason, net_bps_f / 10.0, pnl_usdt,
        sym_name, summary.side, qty_s, summary.legs_consolidated, summary.is_maker_exit,
        en_w, en_f, ex_w, ex_f, summary.entry_price.0, summary.exit_price.0,
        gross_f, -fee_f, net_bps_f,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0
    )
}

pub fn rehydrate_cold_mfe_seeds_from_reader<R: BufRead>(
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    reader: R,
) {
    const PHI_REHYD: f64 = 1.618_033_988_749_895;
    const INV_PHI_REHYD: f64 = 0.618_033_988_749_895;

    let mut cold_mask = [false; MAX_ACTIVE_PAIRS];
    let mut sunk_mask = [false; MAX_ACTIVE_PAIRS];
    let mut has_any_eligible = false;
    let mut ewma_sbps = [DEFAULT_LEARNED_MFE_SCALED_BPS as f64; MAX_ACTIVE_PAIRS];
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let cur = engines[s_idx].learned_mfe_bps;
        let active_fee_rt_mkr = engines[s_idx].active_maker_rt_fee_bps().max(100);
        if cur == DEFAULT_LEARNED_MFE_SCALED_BPS {
            cold_mask[s_idx] = true;
            ewma_sbps[s_idx] = cur as f64;
            has_any_eligible = true;
        } else if cur < ((PHI_REHYD * (active_fee_rt_mkr as f64)).round() as i64) {
            sunk_mask[s_idx] = true;
            ewma_sbps[s_idx] = cur as f64;
            has_any_eligible = true;
        }
    }
    if !has_any_eligible {
        return;
    }
    let mut sample_counts = [0_u32; MAX_ACTIVE_PAIRS];
    for line in reader.lines().map_while(Result::ok) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let is_maker_exit = v
            .get("maker_exit")
            .or_else(|| v.get("is_maker_exit"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !is_maker_exit {
            continue;
        }
        if matches!(
            v.get("exit_sub_reason").and_then(serde_json::Value::as_str),
            Some("AdverseScratchEscape")
        ) {
            continue;
        }
        let Some(s_idx) = v
            .get("symbol")
            .and_then(serde_json::Value::as_str)
            .and_then(slot_from_symbol_name)
        else {
            continue;
        };
        if !cold_mask[s_idx] && !sunk_mask[s_idx] {
            continue;
        }
        let active_fee_rt_mkr = engines[s_idx].active_maker_rt_fee_bps().max(100);
        let fee_rt_bps = (active_fee_rt_mkr as f64) / BPS_TO_SCALED_BPS_F64;
        let floor_bps = ((active_fee_rt_mkr + active_fee_rt_mkr / 8) as f64)
            / BPS_TO_SCALED_BPS_F64;
        let explicit_mfe_opt = v
            .get("mfe_bps")
            .and_then(serde_json::Value::as_f64)
            .filter(|&m| m > 0.0);
        let mfe_bps_opt = explicit_mfe_opt.or_else(|| {
            v.get("net_bps")
                .and_then(serde_json::Value::as_f64)
                .map(|n| n + fee_rt_bps.max(LEGACY_SCAR_MAKER_FEE_RT_BPS))
        });
        let Some(obs_mfe_bps) = mfe_bps_opt else {
            continue;
        };
        if !obs_mfe_bps.is_finite() || obs_mfe_bps <= floor_bps.min(SCAR_MFE_REHYDRATE_FLOOR_BPS) {
            continue;
        }
        let obs_sbps = obs_mfe_bps * BPS_TO_SCALED_BPS_F64;
        let cap_pct_opt = v.get("mfe_capture_pct").and_then(serde_json::Value::as_f64);
        let gross_opt = v.get("gross_bps").and_then(serde_json::Value::as_f64);
        let right_censored = explicit_mfe_opt.is_none()
            || cap_pct_opt.is_some_and(|pct| pct >= 100.0 * INV_PHI_REHYD)
            || gross_opt.is_some_and(|g| g > fee_rt_bps && g >= INV_PHI_REHYD * obs_mfe_bps);
        if obs_sbps < ewma_sbps[s_idx] && (right_censored || sunk_mask[s_idx]) {
            continue;
        }
        ewma_sbps[s_idx] += INV_PHI_FOURTH * (obs_sbps - ewma_sbps[s_idx]);
        sample_counts[s_idx] = sample_counts[s_idx].saturating_add(1);
    }
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        if sample_counts[s_idx] == 0 {
            continue;
        }
        let active_fee_rt_mkr = engines[s_idx].active_maker_rt_fee_bps().max(100);
        let cap_sbps = SCAR_MFE_REHYDRATE_CAP_MULTIPLIER.saturating_mul(active_fee_rt_mkr);
        let cand_sbps = ewma_sbps[s_idx].round() as i64;
        if cold_mask[s_idx] {
            engines[s_idx].learned_mfe_bps = cand_sbps.clamp(DEFAULT_LEARNED_MFE_SCALED_BPS, cap_sbps);
        } else if sunk_mask[s_idx] && cand_sbps > engines[s_idx].learned_mfe_bps {
            engines[s_idx].learned_mfe_bps = cand_sbps.clamp(engines[s_idx].learned_mfe_bps, cap_sbps);
        }
    }
}

pub fn rehydrate_cold_mfe_seeds_from_scar_vault(
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    scar_vault_path: &Path,
) {
    if let Ok(file) = File::open(scar_vault_path) {
        rehydrate_cold_mfe_seeds_from_reader(engines, BufReader::new(file));
    }
}

pub fn rehydrate_shadow_ledger(
    ledger: &mut ParitySyncLedger,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
) {
    let (scar_max_ts_ns, scar_rows_ingested) = gov.rehydrate_from_scar_and_loss_files(
        Path::new(SCAR_MEMORY_VAULT_PATH), Path::new(RED_LOSS_BOX_PATH), Some(Path::new(AI_FORENSIC_VAULT_PATH)),
    );
    let fees = gov.active_fee_schedule();
    let fee_rt = gov.active_maker_rt_fee_bps();
    let (mut l3_vault_count, mut funding_rows) = (0_u64, 0_u64);
    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            match v.get("event").and_then(serde_json::Value::as_str) {
                Some("EPOCH_RESET") => {
                    let eq_a = v.get("anchor_equity_1e8").and_then(serde_json::Value::as_i64).unwrap_or(DEFAULT_WALLET_EQUITY_1E8);
                    *ledger = ParitySyncLedger::new(eq_a);
                    ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
                    l3_vault_count = 0;
                    funding_rows = 0;
                }
                Some("TRADE_CLOSE") => {
                    l3_vault_count += 1;
                    let sym = v.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("");
                    let side = side_from_str(v.get("side").and_then(serde_json::Value::as_str).unwrap_or("Bid"));
                    let net_f = v.get("net_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let fee_f = v.get("fee_bps").and_then(serde_json::Value::as_f64).unwrap_or((fee_rt as f64) / 100.0);
                    let gross_f = v.get("gross_bps").and_then(serde_json::Value::as_f64).unwrap_or(net_f + fee_f);
                    let mfe_f = v.get("mfe_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let is_mkr = v
                        .get("is_maker_exit")
                        .or_else(|| v.get("maker_exit"))
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    let is_scratch = matches!(
                        v.get("exit_sub_reason").and_then(serde_json::Value::as_str),
                        Some("AdverseScratchEscape")
                    );
                    let gross_sc = (gross_f * 100.0).round() as i64;
                    let net_sc = ScaledBps((net_f * 100.0).round() as i64);
                    let fee_sc = ScaledBps((fee_f * 100.0).round() as i64);
                    let s_opt = slot_from_symbol_name(sym);
                    let (pnl_1e8, fee_1e8, _) = extract_vault_row_pnl_and_fee_1e8(&v, s_opt, side, net_sc, fee_sc, fees);
                    ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
                    if let Some(s_idx) = s_opt {
                        let mfe_sc = (mfe_f * 100.0).round() as i64;
                        if mfe_sc > 0 {
                            engines[s_idx].observe_mfe_sample_uncensored(
                                gross_sc, mfe_sc, is_mkr, is_scratch, fee_rt,
                            );
                        }
                    }
                }
                Some("FUNDING_SETTLEMENT") => {
                    let f_1e8 = v.get("funding_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let n_1e8 = v.get("notional_1e8").and_then(serde_json::Value::as_i64).unwrap_or(FALLBACK_NOTIONAL_1E8);
                    if f_1e8 != 0 {
                        ledger.record_funding_settlement(f_1e8, n_1e8);
                        funding_rows += 1;
                    }
                }
                _ => {}
            }
        }
    }
    rehydrate_cold_mfe_seeds_from_scar_vault(engines, Path::new(SCAR_MEMORY_VAULT_PATH));
    ledger.closed_trades_count = l3_vault_count;
    arena.exec_closed_count.store(l3_vault_count, Ordering::Release);
    println!(
        "{ANSI_GREEN}\u{1f7e2} [JSONL REHYDRATE V5.4] Session: {l3_vault_count} trades (+{funding_rows} funding) | Scars: {scar_rows_ingested} unique (Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps | ScarTs: {scar_max_ts_ns}){ANSI_RESET}",
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0,
    );
}

#[must_use]
pub fn rehydrate_daily_governor_from_vault(
    now_ns: TimestampNs, live_equity_1e8: i64,
    ledger: &ParitySyncLedger, gov: &OnlineAdaptiveGovernor,
) -> DailySessionGovernor {
    let cur_day = DailySessionGovernor::utc_day_from_ns(now_ns);
    let saved = load_daily_governor_from_path(Path::new(DAILY_GOVERNOR_VAULT_PATH), live_equity_1e8);
    if cur_day > 0 && saved.utc_day_idx == cur_day {
        return saved;
    }
    let mut dg = DailySessionGovernor::new(live_equity_1e8);
    if cur_day <= 0 { return dg; }
    let mut day_deltas: Vec<(i64, i64, bool, bool, bool)> = Vec::new();
    let mut sum_today_1e8 = 0_i64;
    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let ts = v.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
            if DailySessionGovernor::utc_day_from_ns(TimestampNs(ts)) != cur_day { continue; }
            match v.get("event").and_then(serde_json::Value::as_str) {
                Some("EPOCH_RESET") => { day_deltas.clear(); sum_today_1e8 = 0; }
                Some("TRADE_CLOSE") => {
                    let pnl = v.get("net_pnl_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let net_bps = v.get("net_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    sum_today_1e8 = sum_today_1e8.saturating_add(pnl);
                    day_deltas.push((ts, pnl, true, net_bps >= 1.0, net_bps < -0.50));
                }
                Some("FUNDING_SETTLEMENT") => {
                    let f_pnl = v.get("funding_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    sum_today_1e8 = sum_today_1e8.saturating_add(f_pnl);
                    day_deltas.push((ts, f_pnl, false, false, false));
                }
                _ => {}
            }
        }
    }
    let open_eq = live_equity_1e8.saturating_sub(sum_today_1e8).max(100_000_000);
    dg.day_open_equity_1e8 = open_eq;
    dg.utc_day_idx = cur_day;
    let mut run_eq = open_eq;
    let fee_rt = gov.active_maker_rt_fee_bps();
    let eq_vol = gov.portfolio_mean_eq_vol_bps();
    let port_ir = gov.portfolio_mean_ir_x100();
    for (ts, dp, is_tr, is_w, is_l) in day_deltas {
        run_eq = run_eq.saturating_add(dp);
        let _ = dg.observe_realized_cashflow(
            TimestampNs(ts), dp, is_tr, is_w, is_l, run_eq,
            ledger.sinking_fund_reserve_bps_x100, fee_rt, eq_vol, port_ir,
        );
    }
    dg
}

#[must_use]
fn build_closed_trade_vault_json_v48_core(
    slot_idx: usize, summary: &ClosedTradeSummary, eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot, exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger, markout_curve: &MarkoutCurveBps,
    lvr_5s_scaled_bps: i64, ts_ns: TimestampNs,
) -> String {
    let spec = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
    let sym = spec.symbol_id.name();
    let side_str = if matches!(summary.side, Side::Ask) { "Ask" } else { "Bid" };
    let kind_str = match summary.kind {
        ClosedTradeKind::MakerPassiveUnwind => "MakerPassiveUnwind",
        ClosedTradeKind::ServerMarkPriceStopLoss => "ServerMarkPriceStopLoss",
    };
    let net_bps = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let gross_bps = (summary.gross_scaled_bps.raw() as f64) / 100.0;
    let fee_bps = (summary.fee_scaled_bps.raw() as f64) / 100.0;
    let fee_drag_pct = if gross_bps > 0.01 { (fee_bps / gross_bps) * 100.0 } else { 100.0 };
    let mfe_bps = ((eng.last_closed_mfe_bps as f64) / 100.0).max(gross_bps);
    let mae_bps = ((eng.last_closed_mae_bps as f64) / 100.0).min(gross_bps.min(0.0));
    let mfe_capture_pct = if mfe_bps > 0.01 { ((gross_bps / mfe_bps) * 100.0).clamp(0.0, 100.0) } else { 0.0 };
    let hold_ms = (ts_ns.0.saturating_sub(eng.last_opened_ts.0)).max(0) / 1_000_000;
    let q_wait_ms = if entry_f.armed_ts_ns > 0 && eng.last_opened_ts.0 >= entry_f.armed_ts_ns {
        (eng.last_opened_ts.0 - entry_f.armed_ts_ns) / 1_000_000
    } else { 0 };
    let q_floor = defensive_zero_loss_maker_exit_price_with_fees(summary.side, summary.entry_price, eng.active_fees);
    let w_obs = exit_snap.wave_observables();
    let ob_obs = exit_snap.order_block_observables();
    let mk_arr = [
        (markout_curve.samples_bps[0] as f64) / 100.0, (markout_curve.samples_bps[1] as f64) / 100.0,
        (markout_curve.samples_bps[2] as f64) / 100.0, (markout_curve.samples_bps[3] as f64) / 100.0,
    ];
    let mut base_val = serde_json::json!({
        "event": "TRADE_CLOSE", "schema": "V5.4", "trade_id": ledger.closed_trades_count,
        "armed_ts_ns": entry_f.armed_ts_ns, "opened_ts_ns": eng.last_opened_ts.0, "ts_ns": ts_ns.0,
        "queue_wait_ms": q_wait_ms, "hold_ms": hold_ms, "symbol": sym, "slot_idx": slot_idx, "side": side_str,
        "closed_lots": summary.closed_lots.0, "legs_consolidated": summary.legs_consolidated,
        "entry_ticks": summary.entry_price.0, "exit_ticks": summary.exit_price.0,
        "floor_ticks": q_floor.0, "quantum_floor_ticks": q_floor.0,
        "gross_bps": gross_bps, "fee_bps": fee_bps, "net_bps": net_bps,
        "fee_drag_pct": (fee_drag_pct * 10.0).round() / 10.0,
        "net_pnl_1e8": summary.net_pnl_1e8, "fee_1e8": summary.fee_1e8,
        "cum_net_pnl_1e8": ledger.cumulative_net_pnl_1e8, "cum_funding_1e8": ledger.cumulative_funding_1e8,
        "kind": kind_str, "exit_sub_reason": format!("{:?}", eng.last_exit_sub_reason),
        "maker_exit": summary.is_maker_exit, "peak_ticks": eng.last_closed_peak_ticks.0, "mfe_bps": mfe_bps,
        "worst_ticks": eng.last_closed_worst_ticks.0, "mae_bps": mae_bps,
        "mfe_capture_pct": (mfe_capture_pct * 10.0).round() / 10.0, "init_q_lots": entry_f.init_q_lots,
        "ofi_in": (entry_f.ofi_in * 100.0).round() / 100.0, "tfi_in": (entry_f.tfi_in * 100.0).round() / 100.0,
        "geom_in": (entry_f.geom_in * 100.0).round() / 100.0, "snr_in": (entry_f.snr_in * 100.0).round() / 100.0,
        "vol5m_in": entry_f.vol5m_in, "spread_in": entry_f.spread_in,
        "btc_comp_in": (entry_f.btc_comp_in * 100.0).round() / 100.0,
        "gov_ewma_bps": (entry_f.gov_ewma_bps * 100.0).round() / 100.0,
        "win_streak": entry_f.win_streak, "sl_streak": entry_f.sl_streak,
        "drift_bps": (entry_f.drift_bps * 100.0).round() / 100.0, "ext_bps": (entry_f.ext_bps * 100.0).round() / 100.0,
        "kin_accel_net_bps": (entry_f.kin_accel_net_bps * 100.0).round() / 100.0,
        "kin_energy_in_bps": (entry_f.kin_energy_in_bps * 100.0).round() / 100.0,
        "req_kin_energy_in_bps": (entry_f.req_kin_energy_in_bps * 100.0).round() / 100.0,
        "ofi_out": (exit_snap.factor_ofi.get() * 100.0).round() / 100.0,
        "tfi_out": (exit_snap.factor_tfi.get() * 100.0).round() / 100.0,
        "geom_out": (exit_snap.factor_geom.get() * 100.0).round() / 100.0,
        "vol5m_out": (exit_snap.realized_vol_5m.raw() as f64) / 100.0,
        "slot_mfe_bps": (eng.learned_mfe_bps as f64) / 100.0,
        "wald_contraction_pct": eng.last_closed_wald_pct, "lvdbs_surfs": eng.lvdbs_deep_surfs_count,
        "exit_peg_attempts": eng.exit_peg_attempts, "wave_regime": format!("{:?}", w_obs.regime),
        "wave_climax": w_obs.climax_active, "wave_hurst": (w_obs.hurst_exponent() * 1000.0).round() / 1000.0,
        "wave_macro": (w_obs.macro_phase.get() * 100.0).round() / 100.0,
        "wave_oi_imp": (w_obs.oi_impulse.get() * 100.0).round() / 100.0,
        "wave_liq_imb": (w_obs.liq_imbalance.get() * 100.0).round() / 100.0,
        "wave_fib_bps": (w_obs.fib_proj_bps.raw() as f64) / 100.0,
        "ob_bull_bps": (ob_obs.dist_bull_ob_bps.raw() as f64) / 100.0,
        "ob_bear_bps": (ob_obs.dist_bear_ob_bps.raw() as f64) / 100.0,
        "ob_bull_score": (ob_obs.bull_ob_score.get() * 100.0).round() / 100.0,
        "ob_bear_score": (ob_obs.bear_ob_score.get() * 100.0).round() / 100.0,
        "ob_bull_pm": exit_snap.ob_bull_efficacy_pm, "ob_bear_pm": exit_snap.ob_bear_efficacy_pm,
        "ob_efficacy_pm": exit_snap.ob_learned_efficacy_pm,
        "funding_bps": (exit_snap.funding_rate_scaled_bps.raw() as f64) / 100.0,
        "taker_prob_pm": eng.learned_taker_prob_pm,
        "active_fee_rt_bps": (eng.active_fees.round_trip_maker_exit_bps().raw() as f64) / 100.0,
        "lvr_5s_bps": (lvr_5s_scaled_bps as f64) / 100.0, "markout_fill_bps": mk_arr,
    });
    let pre_txt = base_val.to_string();
    let fnv = ColumnarParquetBatch::compute_fnv1a64(pre_txt.as_bytes());
    if let Some(obj) = base_val.as_object_mut() {
        obj.insert("fnv1a64_hex".to_string(), serde_json::Value::String(format!("0x{fnv:016X}")));
    }
    base_val.to_string()
}

pub fn dispatch_closed_trade_logs_with_locks(
    slot_idx: usize, summary: &ClosedTradeSummary, eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot, exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger, gov: &OnlineAdaptiveGovernor,
    markout_curve: &MarkoutCurveBps, lvr_5s_scaled_bps: i64,
    vault_tx: &SyncSender<VaultIoCommand>, locks_opt: Option<&AtomicLockWatch>,
    halt_locked: &mut bool, ts_ns: TimestampNs,
) {
    let eq_ref = ledger.current_equity_1e8().saturating_sub(summary.net_pnl_1e8);
    let is_catastrophic_halt = is_catastrophic_loss_breach(summary.net_pnl_1e8, eq_ref);
    if is_catastrophic_halt {
        *halt_locked = true;
        if let Some(lw) = locks_opt { lw.latch_halt_in_ram(); }
    }
    let task = Box::new(ClosedTradePersistTask {
        slot_idx, summary: *summary, eng: eng.clone(), entry_forensics: *entry_f,
        exit_snap: *exit_snap, ledger: *ledger, gov: gov.clone(),
        markout_curve: *markout_curve, lvr_5s_scaled_bps, is_catastrophic_halt, ts_ns,
    });
    if let Err(mpsc::TrySendError::Full(cmd) | mpsc::TrySendError::Disconnected(cmd)) =
        vault_tx.try_send(VaultIoCommand::RecordClosedTrade(task))
    {
        if let VaultIoCommand::RecordClosedTrade(fb_task) = cmd {
            crate::vault_io::persist_closed_trade_bundle_sync(&fb_task, locks_opt);
        }
    }
}

#[inline]
pub fn dispatch_closed_trade_logs_v47(
    slot_idx: usize, summary: &ClosedTradeSummary, eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot, exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger, gov: &OnlineAdaptiveGovernor,
    markout_curve: &MarkoutCurveBps, lvr_5s_scaled_bps: i64,
    vault_tx: &SyncSender<VaultIoCommand>, halt_locked: &mut bool, ts_ns: TimestampNs,
) {
    dispatch_closed_trade_logs_with_locks(
        slot_idx, summary, eng, entry_f, exit_snap, ledger, gov,
        markout_curve, lvr_5s_scaled_bps, vault_tx, None, halt_locked, ts_ns,
    );
}

fn pad_visible_right(s: &str, target_w: usize) -> String {
    let w = visible_terminal_width(s);
    format!("{s}{}", " ".repeat(target_w.saturating_sub(w)))
}

fn fmt_3col(c1: String, c2: String, c3: String) -> String {
    format!("  \u{2022} {} \u{2502} {} \u{2502} {}", pad_visible_right(&c1, 56), pad_visible_right(&c2, 40), c3)
}

fn format_position_duration(secs: u64) -> String {
    if secs < 60 { format!("{secs}s") }
    else if secs < 3_600 { format!("{}m {:02}s", secs / 60, secs % 60) }
    else { format!("{}h {:02}m {:02}s", secs / 3_600, (secs % 3_600) / 60, secs % 60) }
}

fn make_ascii_bar(pct: f64, width: usize) -> String {
    let filled = (((pct.clamp(0.0, 100.0) / 100.0) * (width as f64)).round() as usize).min(width);
    format!("{}{}", "\u{2588}".repeat(filled), "\u{2591}".repeat(width - filled))
}

pub const V48_INCEPTION_UTC_DAY: i64 = 20_773;

#[inline]
#[must_use]
pub fn classify_live_upl_color(net_scaled_bps: i64) -> &'static str {
    let pct_10x_centi = ((net_scaled_bps as f64) / 10.0).round() as i64;
    if pct_10x_centi >= 10 {
        ANSI_GREEN
    } else if pct_10x_centi >= 1 || (pct_10x_centi == 0 && net_scaled_bps >= 0) {
        ANSI_SKY_BLUE
    } else if pct_10x_centi >= -5 {
        ANSI_ORANGE
    } else {
        ANSI_RED
    }
}

#[must_use]
pub fn rehydrate_last_funding_cuts_from_vault() -> [i64; MAX_ACTIVE_PAIRS] {
    let mut cuts = [0_i64; MAX_ACTIVE_PAIRS];
    let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) else {
        return cuts;
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
        match v.get("event").and_then(serde_json::Value::as_str) {
            Some("EPOCH_RESET") => {
                cuts = [0_i64; MAX_ACTIVE_PAIRS];
            }
            Some("FUNDING_SETTLEMENT") => {
                let s_opt = v
                    .get("slot_idx")
                    .and_then(serde_json::Value::as_u64)
                    .map(|u| u as usize)
                    .filter(|&u| u < MAX_ACTIVE_PAIRS)
                    .or_else(|| {
                        v.get("symbol")
                            .and_then(serde_json::Value::as_str)
                            .and_then(slot_from_symbol_name)
                    });
                let cut_id = v.get("funding_cut_id").and_then(serde_json::Value::as_i64).unwrap_or(0);
                if let Some(s_idx) = s_opt {
                    if cut_id > cuts[s_idx] {
                        cuts[s_idx] = cut_id;
                    }
                }
            }
            _ => {}
        }
    }
    cuts
}

pub fn render_live_hud(arena: &crate::lob_shm::ReadOnlyShmArena) {
    let ts_now = now_epoch_ns();
    let now_ns = ts_now.0.max(0) as u64;
    let hb_ns = arena.heartbeat_ns();
    let hb_age_ms = if hb_ns > 0 && now_ns >= hb_ns { (now_ns - hb_ns) / 1_000_000 } else { 0 };
    let (armed_c, abort_c, fill_c, closed_c, resyncs) = arena.counters();
    let drain_active = Path::new(DRAIN_LOCK_PATH).exists();
    let halt_active = Path::new(HALT_LOCK_PATH).exists();
    let fees = load_hud_fee_schedule();
    let fee_rt = fees.round_trip_maker_exit_bps().raw().max(100);
    let mut dg = load_daily_governor_from_path(Path::new(DAILY_GOVERNOR_VAULT_PATH), DEFAULT_WALLET_EQUITY_1E8);
    let w_est = dg.day_open_equity_1e8.saturating_add(dg.day_realized_pnl_1e8).max(5_000_000_000);
    let _ = dg.sync_utc_day(ts_now, w_est);
    let dg_col = match dg.phase {
        DailyShieldPhase::ActiveHunting => ANSI_GREEN,
        DailyShieldPhase::ProfitLockArmed => ANSI_SKY_BLUE,
        DailyShieldPhase::HibernatingProfitLocked => ANSI_DARK_GREEN,
        DailyShieldPhase::HibernatingLossCap => ANSI_ORANGE,
    };
    let sec_title = vec![format!(
        "{ANSI_SKY_BLUE}\u{1f4e1} [VORTEX-OMEGA V5.4 LIVE HUD] SHM v{}: /dev/shm/vortex_lob.mmap \u{2502} HB Age: {hb_age_ms} ms \u{2502} Resyncs: {resyncs}{ANSI_RESET}",
        arena.version()
    )];
    let sec_hdr = vec![concat!(
        "SLOT  \u{2502} SYMBOL     \u{2502}   PyG UPL (%@10x) \u{2502} POSITION TIME \u{2502} ",
        "      SEQ_ID \u{2502}     BEST_BID \u{2502}     BEST_ASK \u{2502} SPR \u{2502} ",
        " X_OFI \u{2502}  X_TFI \u{2502} WAVE / OB+- / FND    \u{2502} VERDICT / VETO   "
    ).to_string()];
    let mut sec_rows = Vec::with_capacity(MAX_ACTIVE_PAIRS);
    let (mut open_slots_count, mut queued_slots_count) = (0_usize, 0_usize);
    let btc_snap = arena.read_slot(0, 4).unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = arena.read_slot(s_idx, 4).unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
        let tel = arena.read_telemetry(s_idx, 4).unwrap_or_default();
        let (bw, bf) = spec.format_price_usdt_parts(snap.bids[0].price);
        let (aw, af) = spec.format_price_usdt_parts(snap.asks[0].price);
        let spr = (snap.asks[0].price.0 - snap.bids[0].price.0).max(0);
        let w_obs = snap.wave_observables();
        let w_short = match w_obs.regime {
            crate::wave_elliott::WaveRegime::ImpulseWave3 | crate::wave_elliott::WaveRegime::ImpulseWave5 => "Impu",
            crate::wave_elliott::WaveRegime::CorrectiveAbc | crate::wave_elliott::WaveRegime::CorrectiveWave4 => "Corr",
            crate::wave_elliott::WaveRegime::ClimaxExhaustion => "Clim",
            crate::wave_elliott::WaveRegime::NeutralWarmup => "Warm",
        };
        let fnd_b = (snap.funding_rate_scaled_bps.raw() as f64) / 100.0;
        let wave_ob_col = format!("{w_short} {:03}/{:03} {:+4.1}b", snap.ob_bull_efficacy_pm, snap.ob_bear_efficacy_pm, fnd_b);
        let mut row_col = ANSI_GRAY;
        let mut pyg_col = format!("{ANSI_GRAY}{:>17}{ANSI_RESET}", "-");
        let mut time_col = format!("{:>13}", "-");
        let mut verd_col = format!("{ANSI_GRAY}\u{26aa} NEUTRAL_WAIT  {ANSI_RESET}");
        if tel.state_code >= 3 && tel.avg_price_ticks > 0 {
            open_slots_count += 1;
            row_col = ANSI_BLUE;
            let pos_side = if tel.side_code == 1 { Side::Ask } else { Side::Bid };
            let ref_px = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { tel.avg_price_ticks };
            let gross_bps = ((ref_px - tel.avg_price_ticks) * pos_side.sign_i64()).saturating_mul(1_000_000) / tel.avg_price_ticks.max(1);
            let net_sbps = gross_bps - fee_rt;
            let net_bps_f = (net_sbps as f64) / 100.0;
            let upl_c = classify_live_upl_color(net_sbps);
            pyg_col = format!("{upl_c}{:>+7.2}bp({:>+5.2}%){ANSI_RESET}", net_bps_f, net_bps_f / 10.0);
            let hold_s = if tel.resting_since_ns > 0 && now_ns >= tel.resting_since_ns { (now_ns - tel.resting_since_ns) / 1_000_000_000 } else { 0 };
            time_col = format!("{:>13}", format_position_duration(hold_s));
            let q_fl = defensive_zero_loss_maker_exit_price_with_fees(pos_side, PriceTicks(tel.avg_price_ticks), fees);
            verd_col = format!("{ANSI_BLUE}\u{1f535} OPEN TP:{}t{ANSI_RESET}", if tel.tp_ticks > 0 { tel.tp_ticks } else { q_fl.0 });
        } else if tel.state_code == 1 || tel.state_code == 2 {
            queued_slots_count += 1;
            row_col = ANSI_YELLOW;
            let q_s = if tel.resting_since_ns > 0 && now_ns >= tel.resting_since_ns { (now_ns - tel.resting_since_ns) / 1_000_000_000 } else { 0 };
            time_col = format!("{:>13}", format!("Q:{}s ({}L)", q_s, tel.queue_ahead_lots));
            verd_col = format!("{ANSI_YELLOW}\u{1f7e1} QUEUE_L1_WAIT {ANSI_RESET}");
        } else if snap.sequence_id > 0 {
            let mid_px = PriceTicks(i64::midpoint(snap.bids[0].price.0.max(1), snap.asks[0].price.0.max(1)));
            let q_ok = spec.passes_quantum_ev_growth_gate(w_est, mid_px, snap.realized_vol_5m, ScaledBps((fee_rt * 17) / 10), 50, fees);
            let vc = LastVetoCode::from_code(tel.last_veto_code);
            if halt_active {
                row_col = ANSI_RED;
                verd_col = format!("{ANSI_RED}\u{1f534} HALT_LOCKED   {ANSI_RESET}");
            } else if drain_active {
                row_col = ANSI_ORANGE;
                verd_col = format!("{ANSI_ORANGE}\u{1f7e0} DRAIN_MODE    {ANSI_RESET}");
            } else if dg.is_hibernating() {
                verd_col = if q_ok {
                    format!("{ANSI_GRAY}\u{26aa} SLEEP_DAILY   {ANSI_RESET}")
                } else {
                    format!("{ANSI_GRAY}\u{26aa} QUANTUM_EV    {ANSI_RESET}")
                };
            } else if vc != LastVetoCode::None {
                if !q_ok {
                    verd_col = format!("{ANSI_GRAY}\u{26aa} QUANTUM_EV    {ANSI_RESET}");
                } else if vc == LastVetoCode::SpoofingVeto {
                    row_col = ANSI_ORANGE;
                    verd_col = format!("{ANSI_ORANGE}\u{1f7e0} SPOOF_VETO    {ANSI_RESET}");
                } else {
                    verd_col = format!("{ANSI_GRAY}\u{26aa} {:<14}{ANSI_RESET}", format_veto_badge(vc));
                }
            } else {
                let beta_hud = OrthogonalFactorState::epps_btc_lead_lag_beta(s_idx, &snap, &btc_snap);
                match OrthogonalFactorState::evaluate_entry_signal_v46(
                    spec, &snap, &btc_snap, ScaledBps((fee_rt * 17) / 10), ScaledBps(fee_rt / 2), 50, fees, 0.0, beta_hud,
                ) {
                    SignalVerdict::SpoofingVeto => {
                        row_col = ANSI_ORANGE;
                        verd_col = format!("{ANSI_ORANGE}\u{1f7e0} SPOOF_VETO    {ANSI_RESET}");
                    }
                    SignalVerdict::AuthorizeMakerEntry { .. } if !q_ok => {
                        verd_col = format!("{ANSI_GRAY}\u{26aa} QUANTUM_EV    {ANSI_RESET}");
                    }
                    SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. } => {
                        row_col = ANSI_GREEN;
                        verd_col = format!("{ANSI_GREEN}\u{1f7e2} ARM_BID_L1    {ANSI_RESET}");
                    }
                    SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, .. } => {
                        row_col = ANSI_RED;
                        verd_col = format!("{ANSI_RED}\u{1f534} ARM_ASK_L1    {ANSI_RESET}");
                    }
                    SignalVerdict::NeutralNoAction => {
                        if !q_ok {
                            verd_col = format!("{ANSI_GRAY}\u{26aa} QUANTUM_EV    {ANSI_RESET}");
                        }
                    }
                }
            }
        }
        let sym_name = spec.symbol_id.name();
        let seq_v = snap.sequence_id;
        let ofi_v = snap.factor_ofi.get();
        let tfi_v = snap.factor_tfi.get();
        sec_rows.push(format!(
            "{row_col}{s_idx:>5}{ANSI_RESET} \u{2502} {row_col}{sym_name:<10}{ANSI_RESET} \u{2502} {} \u{2502} {row_col}{time_col}{ANSI_RESET} \u{2502} {row_col}{seq_v:>12}{ANSI_RESET} \u{2502} {row_col}{bw:>7}.{bf:04}{ANSI_RESET} \u{2502} {row_col}{aw:>7}.{af:04}{ANSI_RESET} \u{2502} {row_col}{spr:>3}{ANSI_RESET} \u{2502} {row_col}{ofi_v:>+6.2}{ANSI_RESET} \u{2502} {row_col}{tfi_v:>+6.2}{ANSI_RESET} \u{2502} {row_col}{wave_ob_col:<20}{ANSI_RESET} \u{2502} {}",
            pad_visible_right(&pyg_col, 17),
            pad_visible_right(&verd_col, 17)
        ));
    }
    let mode_badge = if halt_active { format!("{ANSI_RED}CATASTROPHIC_HALT{ANSI_RESET}") }
        else if drain_active { format!("{ANSI_ORANGE}DRAIN_MODE{ANSI_RESET}") }
        else if dg.is_hibernating() { format!("{dg_col}{}{ANSI_RESET}", dg.phase.label()) }
        else { format!("{ANSI_GREEN}ACTIVE_200HZ{ANSI_RESET}") };
    let sec_ftr = vec![format!(
        "  Mode: {mode_badge} \u{2502} Open:{open_slots_count}/6 Q:{queued_slots_count} \u{2502} Arm:{armed_c} Abr:{abort_c} Fil:{fill_c} Cls:{closed_c} \u{2502} Day:{dg_col}{:+.2}%{ANSI_RESET} ({}) \u{2502} FeeRT:{:.2}bp",
        (dg.day_pnl_ppm() as f64) / 10_000.0, dg.phase.label(), (fee_rt as f64) / 100.0
    )];
    print_auto_aligned_box(&[sec_title, sec_hdr, sec_rows, sec_ftr]);
}

#[cfg(test)]
mod hud_color_v48_tests {
    use super::*;

    #[test]
    fn test_v48_live_hud_upl_4band_colors() {
        assert_eq!(classify_live_upl_color(100), ANSI_GREEN);     // +1.00bp -> +0.10% @10x
        assert_eq!(classify_live_upl_color(836), ANSI_GREEN);     // +8.36bp -> +0.84% @10x
        assert_eq!(classify_live_upl_color(90), ANSI_SKY_BLUE);   // +0.90bp -> +0.09% @10x
        assert_eq!(classify_live_upl_color(10), ANSI_SKY_BLUE);   // +0.10bp -> +0.01% @10x
        assert_eq!(classify_live_upl_color(0), ANSI_SKY_BLUE);    // +0.00bp -> +0.00% @10x
        assert_eq!(classify_live_upl_color(-10), ANSI_ORANGE);    // -0.10bp -> -0.01% @10x
        assert_eq!(classify_live_upl_color(-50), ANSI_ORANGE);    // -0.50bp -> -0.05% @10x
        assert_eq!(classify_live_upl_color(-60), ANSI_RED);       // -0.60bp -> -0.06% @10x
        assert_eq!(classify_live_upl_color(-1_200), ANSI_RED);    // -12.00bp -> -1.20% @10x
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct ScalingSemaphoreReport {
    pub v48_trades: usize,
    pub v48_days: usize,
    pub v48_maker_closes: usize,
    pub v48_mkr_pct: f64,
    pub global_mkr_pct: f64,
    pub pf_v48: f64,
    pub pf_infinite: bool,
    pub wallet_usdt: f64,
    pub vault_70_ok: bool,
    pub defended_days: usize,
    pub catastrophic_breaches: usize,
    pub parity_drift_1e8: i64,
    pub halt_active: bool,
    pub gate1_ok: bool,
    pub gate2_ok: bool,
    pub gate3_ok: bool,
    pub gate4_ok: bool,
    pub gates_passed: usize,
}

impl ScalingSemaphoreReport {
    #[inline]
    #[must_use]
    pub const fn all_green(&self) -> bool {
        self.gates_passed == 4
    }
}

#[must_use]
pub fn evaluate_capital_scaling_semaphore(
    vault_path: &Path,
    wallet_1e8: i64,
    vault_locked_bps_x100: i64,
    global_mkr_pct: f64,
    parity_drift_1e8: i64,
    halt_active: bool,
    live_dg: &DailySessionGovernor,
    fees: FeeSchedule,
) -> ScalingSemaphoreReport {
    use std::collections::{BTreeMap, BTreeSet};
    let mut v48_trades = 0_usize;
    let mut v48_maker_closes = 0_usize;
    let mut v48_profit_1e8 = 0_i64;
    let mut v48_loss_1e8 = 0_i64;
    let mut catastrophic_breaches = 0_usize;
    let mut v48_days_set = BTreeSet::new();
    type DailyCashflowSample = (i64, i64, bool, bool, bool); let mut day_events: BTreeMap<i64, Vec<DailyCashflowSample>> = BTreeMap::new();
    if live_dg.utc_day_idx >= V48_INCEPTION_UTC_DAY {
        v48_days_set.insert(live_dg.utc_day_idx);
    }
    let def_fee_f = (fees.round_trip_maker_exit_bps().raw() as f64) / 100.0;
    let mut sim_ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    sim_ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
    if let Ok(file) = File::open(vault_path) {
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let ts = v.get("ts_ns").and_then(serde_json::Value::as_i64).unwrap_or(0);
            let u_day = DailySessionGovernor::utc_day_from_ns(TimestampNs(ts));
            let is_v48 = matches!(
                v.get("schema").and_then(serde_json::Value::as_str),
                Some("V4.8" | "V4.9" | "V5.0" | "V5.1" | "V5.2" | "V5.3" | "V5.3.1" | "V5.3.2" | "V5.4")
            );
            match v.get("event").and_then(serde_json::Value::as_str) {
                Some("EPOCH_RESET") => {
                    v48_trades = 0; v48_maker_closes = 0; v48_profit_1e8 = 0; v48_loss_1e8 = 0; catastrophic_breaches = 0;
                    v48_days_set.clear(); day_events.clear();
                    if live_dg.utc_day_idx >= V48_INCEPTION_UTC_DAY { v48_days_set.insert(live_dg.utc_day_idx); }
                    let eq_a = v.get("anchor_equity_1e8").and_then(serde_json::Value::as_i64).unwrap_or(DEFAULT_WALLET_EQUITY_1E8);
                    sim_ledger = ParitySyncLedger::new(eq_a);
                    sim_ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
                }
                Some("TRADE_CLOSE") => {
                    let sym = v.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("UNKNOWN");
                    let side_s = v.get("side").and_then(serde_json::Value::as_str).unwrap_or("Bid");
                    let n_bps = v.get("net_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let f_bps = v.get("fee_bps").and_then(serde_json::Value::as_f64).unwrap_or(def_fee_f);
                    let mkr = v.get("maker_exit").and_then(serde_json::Value::as_bool).unwrap_or(true);
                    let net_sc = ScaledBps((n_bps * 100.0).round() as i64);
                    let fee_sc = ScaledBps((f_bps * 100.0).round() as i64);
                    let (pnl_1e8, fee_1e8, _) = extract_vault_row_pnl_and_fee_1e8(&v, slot_from_symbol_name(sym), side_from_str(side_s), net_sc, fee_sc, fees);
                    let eq_before = sim_ledger.current_equity_1e8();
                    sim_ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
                    if is_v48 {
                        v48_trades += 1;
                        if mkr { v48_maker_closes += 1; }
                        if pnl_1e8 >= 0 { v48_profit_1e8 = v48_profit_1e8.saturating_add(pnl_1e8); }
                        else { v48_loss_1e8 = v48_loss_1e8.saturating_add(pnl_1e8.abs()); }
                        if u_day > 0 { v48_days_set.insert(u_day); }
                    }
                    if u_day >= V48_INCEPTION_UTC_DAY {
                        if is_catastrophic_loss_breach(pnl_1e8, eq_before) { catastrophic_breaches += 1; }
                        day_events.entry(u_day).or_default().push((ts, pnl_1e8, true, n_bps >= 1.0, n_bps < -0.50));
                    }
                }
                Some("FUNDING_SETTLEMENT") => {
                    let f_1e8 = v.get("funding_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let n_1e8 = v.get("notional_1e8").and_then(serde_json::Value::as_i64).unwrap_or(FALLBACK_NOTIONAL_1E8);
                    if f_1e8 != 0 {
                        sim_ledger.record_funding_settlement(f_1e8, n_1e8);
                        if is_v48 && u_day > 0 { v48_days_set.insert(u_day); }
                        if u_day >= V48_INCEPTION_UTC_DAY {
                            day_events.entry(u_day).or_default().push((ts, f_1e8, false, false, false));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let mut defended_set = BTreeSet::new();
    if live_dg.utc_day_idx >= V48_INCEPTION_UTC_DAY && live_dg.is_hibernating() {
        defended_set.insert(live_dg.utc_day_idx);
    }
    let fee_rt = fees.round_trip_maker_exit_bps().raw().max(100);
    let base_eq = wallet_1e8.max(DEFAULT_WALLET_EQUITY_1E8);
    for (&d_idx, evs) in &day_events {
        let mut sim_dg = DailySessionGovernor::new(base_eq);
        sim_dg.utc_day_idx = d_idx;
        let mut eq_r = base_eq;
        for &(ts_e, dp, is_tr, is_w, is_l) in evs {
            eq_r = eq_r.saturating_add(dp);
            let _ = sim_dg.observe_realized_cashflow(TimestampNs(ts_e), dp, is_tr, is_w, is_l, eq_r, 0, fee_rt, 3_600, 33);
            if sim_dg.is_hibernating() { defended_set.insert(d_idx); }
        }
    }
    let pf_infinite = v48_trades > 0 && v48_loss_1e8 == 0 && v48_profit_1e8 > 0;
    let pf_v48 = if v48_trades == 0 { 0.0 } else if pf_infinite { 99.99 } else { (v48_profit_1e8 as f64) / (v48_loss_1e8.max(1) as f64) };
    let v48_mkr_pct = if v48_trades > 0 { (v48_maker_closes as f64 * 100.0) / (v48_trades as f64) } else { 0.0 };
    let v48_days = v48_days_set.len();
    let defended_days = defended_set.len();
    let vault_70_ok = vault_locked_bps_x100 > 1_250;
    let gate1_ok = v48_trades >= 50 && v48_days >= 12;
    let gate2_ok = v48_trades >= 10 && pf_v48 >= 1.35 && wallet_1e8 >= 6_700_000_000 && vault_70_ok;
    let gate3_ok = v48_trades >= 10 && v48_mkr_pct >= 82.0 && !halt_active && parity_drift_1e8 == 0;
    let gate4_ok = defended_days >= 2 && catastrophic_breaches == 0 && !halt_active;
    let gates_passed = usize::from(gate1_ok) + usize::from(gate2_ok) + usize::from(gate3_ok) + usize::from(gate4_ok);
    ScalingSemaphoreReport {
        v48_trades, v48_days, v48_maker_closes, v48_mkr_pct, global_mkr_pct, pf_v48, pf_infinite,
        wallet_usdt: (wallet_1e8 as f64) / 100_000_000.0, vault_70_ok, defended_days,
        catastrophic_breaches, parity_drift_1e8, halt_active, gate1_ok, gate2_ok, gate3_ok, gate4_ok, gates_passed,
    }
}
// === VORTEX_V48_DASHBOARD_SPLIT_ANCHOR ===

pub fn render_capital_scaling_semaphore(
    rep: &ScalingSemaphoreReport,
    dg: &DailySessionGovernor,
) {
    let fmt_gate = |idx: u8, ok: bool, pending_tag: &str, name: &str, c2: String, c3: String| -> String {
        let (col, ico, st_lbl) = if ok {
            (ANSI_GREEN, "\u{1f7e2}", "CUMPLIDO   ")
        } else {
            (ANSI_YELLOW, "\u{1f7e1}", pending_tag)
        };
        let c1 = format!("{col}{ico} [REQ {idx}/4 {st_lbl}] {name:<18}{ANSI_RESET}");
        fmt_3col(c1, c2, c3)
    };
    let g1_pct = (((rep.v48_trades.min(50) as f64) / 50.0) * 50.0
        + ((rep.v48_days.min(12) as f64) / 12.0) * 50.0)
        .clamp(0.0, 100.0);
    let g1_c2 = format!(
        "[{}] {:>2}/50t ({:>5.1}%)",
        make_ascii_bar(g1_pct, 14),
        rep.v48_trades,
        g1_pct
    );
    let g1_c3 = format!("Dias UTC V5.4: {:>2}/12 dias", rep.v48_days);

    let w_pct = ((rep.wallet_usdt / 67.0) * 100.0).clamp(0.0, 100.0);
    let g2_c2 = format!(
        "[{}] {:>5.2}/67.00\x24 ({:>5.1}%)",
        make_ascii_bar(w_pct, 14),
        rep.wallet_usdt,
        w_pct
    );
    let pf_txt = if rep.v48_trades < 10 {
        format!("N/A ({}/10t)", rep.v48_trades)
    } else if rep.pf_infinite {
        "INF (0 loss)".to_string()
    } else {
        format!("{:.2}", rep.pf_v48)
    };
    let v70_txt = if rep.vault_70_ok { "Vault>12.5b OK" } else { "Vault<=12.5b" };
    let g2_c3 = format!("PF V5.4: {pf_txt:<12} | {v70_txt}");

    let mkr_ref = if rep.v48_trades >= 10 { rep.v48_mkr_pct } else { rep.global_mkr_pct };
    let g3_pct = ((mkr_ref / 82.0) * 100.0).clamp(0.0, 100.0);
    let g3_tag = if rep.v48_trades < 10 { "MUESTRA MIN" } else { "EN PROGRESO" };
    let g3_c2 = format!("[{}] {:>5.1}%/82.0%", make_ascii_bar(g3_pct, 14), mkr_ref);
    let mkr_v48_txt = if rep.v48_trades == 0 {
        "N/A (0t)".to_string()
    } else {
        format!("{:.1}% ({}/{}t)", rep.v48_mkr_pct, rep.v48_maker_closes, rep.v48_trades)
    };
    let g3_c3 = format!(
        "V5.4 Mkr: {mkr_v48_txt} | Drift:{} Halt:{}",
        rep.parity_drift_1e8,
        usize::from(rep.halt_active)
    );

    let g4_pct = (((rep.defended_days.min(2) as f64) / 2.0) * 100.0).clamp(0.0, 100.0);
    let g4_c2 = format!(
        "[{}] {:>2}/2 defensas ({:>3.0}%)",
        make_ascii_bar(g4_pct, 14),
        rep.defended_days,
        g4_pct
    );
    let g4_c3 = format!(
        "Hoy: {} ({} brechas)",
        dg.phase.label(),
        rep.catastrophic_breaches
    );

    let all_pct = (rep.gates_passed as f64) * 25.0;
    let wake_note = if dg.is_hibernating() {
        " (Despierta 00:00 UTC / 21:00 CL)"
    } else {
        ""
    };
    let verdict_line = if rep.all_green() {
        format!(
            "  \u{2022} Veredicto     : {ANSI_GREEN}\u{1f7e2} SEMAFORO VERDE (4/4) \u{2014} MOTOR VALIDADO: Apto para inyectar Escalon 1 (\x24155-\x24165 USDT -> BTC 12/12).{ANSI_RESET}"
        )
    } else {
        format!(
            "  \u{2022} Veredicto     : {ANSI_YELLOW}\u{1f7e1} EN VALIDACION ({}/4) \u{2014} Mantener capital base (\x24{:.2} USDT) sin inyectar aun.{ANSI_RESET}",
            rep.gates_passed, rep.wallet_usdt
        )
    };
    let sec_hdr = vec![format!(
        "{ANSI_SKY_BLUE}\u{1f6a6} [VORTEX-OMEGA V5.4 CAPITAL SCALING SEMAPHORE \u{2014} HITO DE INYECCION (\x24155-\x24165 USDT)]{ANSI_RESET}"
    )];
    let sec_gates = vec![
        fmt_gate(1, rep.gate1_ok, "EN PROGRESO", "Muestra Pura V5.4", g1_c2, g1_c3),
        fmt_gate(2, rep.gate2_ok, "EN PROGRESO", "Expectativa & Cap.", g2_c2, g2_c3),
        fmt_gate(3, rep.gate3_ok, g3_tag, "Ejecucion Maker", g3_c2, g3_c3),
        fmt_gate(4, rep.gate4_ok, "EN PROGRESO", "Resiliencia Escudo", g4_c2, g4_c3),
    ];
    let sec_ftr = vec![
        format!(
            "  \u{2022} Avance Global : [{}] {}/4 Requisitos en Verde \u{2502} Estado Hoy: {}{wake_note}",
            make_ascii_bar(all_pct, 20),
            rep.gates_passed,
            dg.phase.label()
        ),
        verdict_line,
    ];
    print_auto_aligned_box(&[sec_hdr, sec_gates, sec_ftr]);
}

pub fn render_trades_and_executive_dashboard() {
    let mut rows_out = Vec::new();
    let (mut total, mut wins, mut partials, mut scratches, mut losses, mut maker_closes) = (0_usize, 0_usize, 0_usize, 0_usize, 0_usize, 0_usize);
    let (mut cum_gross, mut cum_fee, mut cum_net, mut sum_mfe, mut sum_mae, mut sum_hold_s) = (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
    let mut sum_cashflow_1e8 = 0_i64;
    let fees = load_hud_fee_schedule();
    let mut dash_ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    dash_ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
    let def_fee_f = (fees.round_trip_maker_exit_bps().raw() as f64) / 100.0;
    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            match v.get("event").and_then(serde_json::Value::as_str) {
                Some("EPOCH_RESET") => {
                    rows_out.clear();
                    total = 0; wins = 0; partials = 0; scratches = 0; losses = 0; maker_closes = 0;
                    cum_gross = 0.0; cum_fee = 0.0; cum_net = 0.0; sum_mfe = 0.0; sum_mae = 0.0; sum_hold_s = 0.0; sum_cashflow_1e8 = 0;
                    let eq_a = v.get("anchor_equity_1e8").and_then(serde_json::Value::as_i64).unwrap_or(DEFAULT_WALLET_EQUITY_1E8);
                    dash_ledger = ParitySyncLedger::new(eq_a);
                    dash_ledger.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
                }
                Some("FUNDING_SETTLEMENT") => {
                    let f_1e8 = v.get("funding_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let n_1e8 = v.get("notional_1e8").and_then(serde_json::Value::as_i64).unwrap_or(FALLBACK_NOTIONAL_1E8);
                    if f_1e8 != 0 {
                        dash_ledger.record_funding_settlement(f_1e8, n_1e8);
                        sum_cashflow_1e8 = sum_cashflow_1e8.saturating_add(f_1e8);
                    }
                }
                Some("TRADE_CLOSE") => {
                    total += 1;
                    let sym = v.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("UNKNOWN");
                    let side_s = v.get("side").and_then(serde_json::Value::as_str).unwrap_or("Bid");
                    let en_t = v.get("entry_ticks").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let ex_t = v.get("exit_ticks").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let g_bps = v.get("gross_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let f_bps = v.get("fee_bps").and_then(serde_json::Value::as_f64).unwrap_or(def_fee_f);
                    let n_bps = v.get("net_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let mfe = v.get("mfe_bps").and_then(serde_json::Value::as_f64).unwrap_or(g_bps);
                    let mae = v.get("mae_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let hold_ms = v.get("hold_ms").and_then(serde_json::Value::as_i64).unwrap_or(0);
                    let sub_r = v.get("exit_sub_reason").and_then(serde_json::Value::as_str).unwrap_or("MakerExit");
                    let mkr = v.get("maker_exit").and_then(serde_json::Value::as_bool).unwrap_or(true);
                    if mkr { maker_closes += 1; }
                    let is_partial = (sub_r.contains("Tranche1Harvest50") || sub_r.contains("Partial")) && n_bps < 1.00;
                    if is_partial { partials += 1; } else if n_bps >= 1.00 { wins += 1; } else if n_bps >= -0.50 { scratches += 1; } else { losses += 1; }
                    cum_gross += g_bps; cum_fee += f_bps; cum_net += n_bps;
                    sum_mfe += mfe; sum_mae += mae; sum_hold_s += (hold_ms as f64) / 1_000.0;
                    let net_sc = ScaledBps((n_bps * 100.0).round() as i64);
                    let fee_sc = ScaledBps((f_bps * 100.0).round() as i64);
                    let (pnl_1e8, fee_1e8, _) = extract_vault_row_pnl_and_fee_1e8(&v, slot_from_symbol_name(sym), side_from_str(side_s), net_sc, fee_sc, fees);
                    dash_ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
                    sum_cashflow_1e8 = sum_cashflow_1e8.saturating_add(pnl_1e8);
                    let (col, icon, lbl) = classify_trade_badge(n_bps, is_partial);
                    rows_out.push(format!(
                        "{col}{icon} [TRADE #{total:<3} {lbl}] {sym:<10} {side_s:<3} \u{2502} {en_t:>7}t->{ex_t:<7}t \u{2502} G:{g_bps:>+6.2} F:{f_bps:>5.2} N:{n_bps:>+6.2}bp ({:>+5.2}%@10x) \u{2502} MFE:{mfe:>+6.2} MAE:{mae:>+6.2} \u{2502} {:>4}s \u{2502} {sub_r:<21} (Mkr={mkr}){ANSI_RESET}",
                        n_bps / 10.0, hold_ms / 1_000
                    ));
                }
                _ => {}
            }
        }
    }
    if rows_out.is_empty() {
        rows_out.push(format!("  {ANSI_GRAY}\u{26aa} [LEDGER EMPTY] Waiting for first closed trade in VORTEX-OMEGA V5.4...{ANSI_RESET}"));
    }
    let mut live_client = BybitV5LiveClient::from_env_if_enabled(true);
    let fb_w = (DEFAULT_WALLET_EQUITY_1E8 as f64) / 100_000_000.0;
    let (w_usdt, eq_usdt, av_usdt, upl_usdt) = live_client.as_mut().and_then(BybitV5LiveClient::fetch_wallet_and_equity_usdt).unwrap_or((fb_w, fb_w, fb_w, 0.0));
    let w_1e8 = ((w_usdt.max(eq_usdt)) * 100_000_000.0).round() as i64;
    let eff_notional = (PairSpec::base_kelly_notional_1e8(w_1e8) as f64) / 100_000_000.0;
    let trade_goal: usize = if total < 25 { 25 } else if total < 50 { 50 } else if total < 100 { 100 } else if total < 250 { 250 } else { ((total / 250) + 1) * 250 };
    let pct_t = ((total.min(trade_goal) as f64) / (trade_goal as f64)) * 100.0;
    let goal_anchor = w_usdt.max(eq_usdt);
    let pct_95 = ((goal_anchor / 95.0) * 100.0).clamp(0.0, 100.0);
    let reserve_bps = (dash_ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0;
    let locked_bps = (dash_ledger.vault_locked_profit_bps_x100 as f64) / 100.0;
    let net_usdt = (dash_ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    let fee_usdt = (dash_ledger.cumulative_fees_1e8 as f64) / 100_000_000.0;
    let fnd_usdt = (dash_ledger.cumulative_funding_1e8 as f64) / 100_000_000.0;
    let denom_t = (total.max(1)) as f64;
    let mkr_pct = if total > 0 { (maker_closes as f64 * 100.0) / denom_t } else { 100.0 };
    let gov_ref = OnlineAdaptiveGovernor::new();
    let dg = rehydrate_daily_governor_from_vault(now_epoch_ns(), w_1e8, &dash_ledger, &gov_ref);
    let fee_rt = fees.round_trip_maker_exit_bps().raw().max(100);
    let stop_pct = (dg.elastic_loss_stop_ppm(dash_ledger.sinking_fund_reserve_bps_x100, fee_rt) as f64) / 10_000.0;
    let arm_ppm = dg.elastic_goal_arm_ppm(dash_ledger.sinking_fund_reserve_bps_x100, fee_rt, 3_600, 33);
    let arm_pct = (arm_ppm as f64) / 10_000.0;
    let lock_pct = (dg.locked_profit_floor_ppm(arm_ppm) as f64) / 10_000.0;
    let day_usdt = (dg.day_realized_pnl_1e8 as f64) / 100_000_000.0;
    let day_pct = (dg.day_pnl_ppm() as f64) / 10_000.0;
    let parity_drift_1e8 = (dash_ledger.cumulative_net_pnl_1e8 - sum_cashflow_1e8).abs();
    let halt_active = Path::new(HALT_LOCK_PATH).exists();

    let sec_hdr = vec![format!("{ANSI_SKY_BLUE}[VORTEX-OMEGA V5.4 EXECUTIVE LEDGER, PROGRESS BARS & ELASTIC DAILY SHIELD]{ANSI_RESET}")];
    let sec_kpi = vec![
        fmt_3col(
            format!("Wallet: {w_usdt:.4} USDT (Eq: {eq_usdt:.4} | Avail: {av_usdt:.4})"),
            format!("Open UPL: {upl_usdt:+.4} USDT | Noc.10x: {eff_notional:.2}\x24"),
            format!("Fees: Mkr {:+.2}bp / Tkr {:.2}bp", (fees.maker_fee_bps.raw() as f64) / 100.0, (fees.taker_fee_bps.raw() as f64) / 100.0),
        ),
        fmt_3col(
            format!("Meta {trade_goal:<3} Tiros: [{}] {total:>3}/{trade_goal} ({pct_t:>5.1}%)", make_ascii_bar(pct_t, 20)),
            format!("Faltan Meta {trade_goal}: {:>3} tiros", trade_goal.saturating_sub(total)),
            format!("Salidas Maker: {mkr_pct:.2}%"),
        ),
        fmt_3col(
            format!("Meta 95 USDT  : [{}] {goal_anchor:>5.2}/95 ({pct_95:>4.1}%)", make_ascii_bar(pct_95, 20)),
            format!("Faltan Meta 95 : {:>7.4} USDT", (95.0 - goal_anchor).max(0.0)),
            format!("Avg Hold: {:.1}s | MFE:{:+.2}/MAE:{:+.2}bp", sum_hold_s / denom_t, sum_mfe / denom_t, sum_mae / denom_t),
        ),
        fmt_3col(
            format!("Escudo Diario : Hoy {day_usdt:+.4} USDT ({day_pct:+.2}% | W:{} L:{})", dg.day_wins, dg.day_losses),
            format!("Stop: {stop_pct:+.2}% | Meta: +{arm_pct:.2}% (Lock:+{lock_pct:.2}%)"),
            format!("Fase UTC: {}", dg.phase.label()),
        ),
        fmt_3col(
            format!("Cum Gross: {cum_gross:+.2}bp | Fee: {:+.2}bp ({fee_usdt:+.4}\x24) | Fnd:{fnd_usdt:+.4}\x24", -cum_fee),
            format!("Cum Net: {cum_net:+.2}bp ({:+.2}%@10x | {net_usdt:+.4} USDT)", cum_net / 10.0),
            format!("{ANSI_DARK_GREEN}Vault 70%: {locked_bps:+.2}bp{ANSI_RESET} | Res 30%: {reserve_bps:+.2}bp"),
        ),
        format!(
            "  \u{2022} Conteo Tiros  : Total: {total:<3} \u{2502} {ANSI_GREEN}WIN: {wins} ({:.1}%){ANSI_RESET} \u{2502} {ANSI_BLUE}PART: {partials} ({:.1}%){ANSI_RESET} \u{2502} {ANSI_SKY_BLUE}SCR: {scratches} ({:.1}%){ANSI_RESET} \u{2502} {ANSI_RED}LOSS: {losses} ({:.1}%){ANSI_RESET}",
            (wins as f64 * 100.0) / denom_t, (partials as f64 * 100.0) / denom_t,
            (scratches as f64 * 100.0) / denom_t, (losses as f64 * 100.0) / denom_t
        ),
    ];
    print_auto_aligned_box(&[sec_hdr, rows_out, sec_kpi]);
    let sem_rep = evaluate_capital_scaling_semaphore(
        Path::new(AI_FORENSIC_VAULT_PATH),
        w_1e8,
        dash_ledger.vault_locked_profit_bps_x100,
        mkr_pct,
        parity_drift_1e8,
        halt_active,
        &dg,
        fees,
    );
    render_capital_scaling_semaphore(&sem_rep, &dg);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v48_capital_scaling_semaphore_yellow_to_green_transition() {
        let tmp_vault = std::env::temp_dir().join(format!("vortex_sem_v48_{}.jsonl", std::process::id()));
        let _ = fs::remove_file(&tmp_vault);
        let mut dg = DailySessionGovernor::new(6_513_770_000);
        dg.utc_day_idx = V48_INCEPTION_UTC_DAY;
        dg.phase = DailyShieldPhase::HibernatingLossCap;

        let legacy_row = serde_json::json!({
            "event": "TRADE_CLOSE", "schema": "V4.7", "ts_ns": V48_INCEPTION_UTC_DAY * 86_400_000_000_000 + 3_600_000_000_000,
            "symbol": "SUIUSDT", "side": "Ask", "entry_ticks": 7279, "exit_ticks": 7270,
            "net_bps": 8.36, "fee_bps": 4.0, "net_pnl_1e8": 1_600_000, "fee_1e8": 760_000, "maker_exit": true
        }).to_string();
        assert!(fs::write(&tmp_vault, format!("{legacy_row}\n")).is_ok());

        let rep0 = evaluate_capital_scaling_semaphore(
            &tmp_vault, 6_513_770_000, 1_250, 84.62, 0, false, &dg, FeeSchedule::VIP0_DEFAULT,
        );
        assert_eq!((rep0.v48_trades, rep0.v48_days, rep0.defended_days, rep0.gates_passed), (0, 1, 1, 0));
        assert!(!rep0.all_green());

        let mut lines = String::new();
        for idx in 0..52_i64 {
            let day_idx = V48_INCEPTION_UTC_DAY + 1 + (idx / 4);
            let ts_ns = day_idx * 86_400_000_000_000 + (idx % 4 + 1) * 3_600_000_000_000;
            let (is_mkr, net_bps, pnl_1e8) = if idx == 4 {
                (true, 18.0, 95_000_000_i64)
            } else if idx == 5 {
                (false, -12.0, -35_000_000_i64)
            } else if idx % 9 == 8 {
                (false, -19.5, -3_800_000_i64)
            } else {
                (true, 4.5, 950_000_i64)
            };
            let row = serde_json::json!({
                "event": "TRADE_CLOSE", "schema": "V5.0", "ts_ns": ts_ns,
                "symbol": "AVAXUSDT", "side": "Bid", "entry_ticks": 8200, "exit_ticks": 8207,
                "net_bps": net_bps, "fee_bps": if is_mkr { 4.0 } else { 7.5 },
                "net_pnl_1e8": pnl_1e8, "fee_1e8": 750_000, "maker_exit": is_mkr
            });
            lines.push_str(&row.to_string());
            lines.push('\n');
        }
        assert!(fs::write(&tmp_vault, lines).is_ok());

        let rep_green = evaluate_capital_scaling_semaphore(
            &tmp_vault, 6_745_000_000, 2_840, 88.46, 0, false, &dg, FeeSchedule::VIP0_DEFAULT,
        );
        render_capital_scaling_semaphore(&rep_green, &dg);
        assert!(rep_green.gate1_ok && rep_green.gate2_ok && rep_green.gate3_ok && rep_green.gate4_ok);
        assert!(rep_green.all_green());
        let _ = fs::remove_file(&tmp_vault);
    }
}

/// Cold analytical ex-ante net Expected Value (`ScaledBps`, `1 bp = 100`) without optimistic bias (Law IX).
#[inline]
#[must_use]
pub fn compute_ex_ante_ev_scaled_bps(
    eng: &SlotExecutionEngine,
    snap: &SymbolLobSnapshot,
) -> i64 {
    let p_tkr_pm = eng.learned_taker_prob_pm.clamp(1, 950);
    let p_mkr_pm = 1_000_i64.saturating_sub(p_tkr_pm);
    let mkr_net_sbps = eng
        .learned_mfe_bps
        .saturating_sub(eng.active_fees.round_trip_maker_exit_bps().raw());
    let raw_srv_sl =
        PairSpec::elastic_server_sl_distance_bps_with_fees(snap.realized_vol_5m, eng.active_fees)
            .raw();
    let sl_total_sbps = ((raw_srv_sl.saturating_mul(618)) / 1_000)
        .saturating_add(eng.active_fees.round_trip_taker_exit_bps().raw());
    p_mkr_pm
        .saturating_mul(mkr_net_sbps)
        .saturating_sub(p_tkr_pm.saturating_mul(sl_total_sbps))
        / 1_000
}

/// Off-thread `TRADE_CLOSE` JSONL builder stamping V4.9 mask, `zeta_tox`, `ev_ex_ante_bps`, WS RTT & FNV-1a.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn build_closed_trade_vault_json(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    markout_curve: &crate::markout_tracker::MarkoutCurveBps,
    lvr_5s_scaled_bps: i64,
    ts_ns: TimestampNs,
) -> String {
    let base_str = build_closed_trade_vault_json_v48_core(
        slot_idx,
        summary,
        eng,
        entry_f,
        exit_snap,
        ledger,
        markout_curve,
        lvr_5s_scaled_bps,
        ts_ns,
    );
    let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&base_str) else {
        return base_str;
    };
    let s_idx = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
    let c_rt = APPROVED_SPECS[s_idx].expected_round_trip_cost_bps_dynamic(
        summary.entry_price.max(PriceTicks(1)),
        eng.learned_taker_prob_pm,
        eng.active_fees,
    );
    let z_tox = (crate::markout_tracker::glosten_milgrom_toxicity_zeta(*markout_curve, c_rt)
        * 1_000.0)
        .round()
        / 1_000.0;
    let ev_bps = if entry_f.ev_ex_ante_bps.abs() > 1e-9 {
        (entry_f.ev_ex_ante_bps * 100.0).round() / 100.0
    } else {
        (compute_ex_ante_ev_scaled_bps(eng, exit_snap) as f64) / 100.0
    };
    let (ws_rtt_us, ws_quota_rem) = crate::execution_fsm::read_live_ws_pulse();
    if let Some(obj) = val.as_object_mut() {
        let _ = obj.remove("fnv1a64_hex");
        obj.insert("telemetry_rev".to_string(), serde_json::json!("V5.4"));
        obj.insert(
            "markout_resolved_mask".to_string(),
            serde_json::json!(markout_curve.resolved_mask),
        );
        obj.insert(
            "initial_edge_bps".to_string(),
            serde_json::json!((markout_curve.initial_edge_bps as f64) / 100.0),
        );
        obj.insert("zeta_tox".to_string(), serde_json::json!(z_tox));
        obj.insert("ev_ex_ante_bps".to_string(), serde_json::json!(ev_bps));
        obj.insert("ws_rtt_us".to_string(), serde_json::json!(ws_rtt_us));
        obj.insert("ws_quota_rem".to_string(), serde_json::json!(ws_quota_rem));
    }
    let pre = val.to_string();
    let fnv = crate::forensic_parquet::ColumnarParquetBatch::compute_fnv1a64(pre.as_bytes());
    if let Some(obj) = val.as_object_mut() {
        obj.insert(
            "fnv1a64_hex".to_string(),
            serde_json::Value::String(format!("0x{fnv:016X}")),
        );
    }
    val.to_string()
}

/// Extended 5-horizon (`+250ms..+60s`) `TRADE_CLOSE` serializer with re-stamped FNV-1a 64-bit.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::too_many_arguments)]
pub fn build_closed_trade_vault_json_v49(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    markout_5h: &crate::markout_tracker::MarkoutCurve5hBps,
    lvr_5s_scaled_bps: i64,
    ts_ns: TimestampNs,
) -> String {
    let m4 = markout_5h.to_legacy_4h();
    let base_str = build_closed_trade_vault_json(
        slot_idx,
        summary,
        eng,
        entry_f,
        exit_snap,
        ledger,
        &m4,
        lvr_5s_scaled_bps,
        ts_ns,
    );
    let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&base_str) else {
        return base_str;
    };
    let m5_f: [f64; crate::markout_tracker::MARKOUT_HORIZONS_V49] =
        core::array::from_fn(|k| (markout_5h.samples_bps[k] as f64) / 100.0);
    if let Some(obj) = val.as_object_mut() {
        let _ = obj.remove("fnv1a64_hex");
        obj.insert("markout_5h_bps".to_string(), serde_json::json!(m5_f));
        obj.insert(
            "markout_resolved_mask".to_string(),
            serde_json::json!(markout_5h.resolved_mask),
        );
        obj.insert(
            "peak_zeta_tox".to_string(),
            serde_json::json!((f64::from(markout_5h.peak_zeta_tox_pm)) / 1_000.0),
        );
    }
    let pre = val.to_string();
    let fnv = crate::forensic_parquet::ColumnarParquetBatch::compute_fnv1a64(pre.as_bytes());
    if let Some(obj) = val.as_object_mut() {
        obj.insert(
            "fnv1a64_hex".to_string(),
            serde_json::Value::String(format!("0x{fnv:016X}")),
        );
    }
    val.to_string()
}

#[cfg(test)]
mod v49_hud_telemetry_tests {
    use super::*;
    use crate::execution_fsm::record_live_ws_pulse;
    use crate::markout_tracker::MarkoutCurve5hBps;

    #[test]
    fn test_v49_closed_trade_vault_json_resolved_mask_zeta_ws_pulse_and_ev() {
        record_live_ws_pulse(1_310, 97);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.realized_vol_5m = ScaledBps(2_400);

        let mut bad_eng = SlotExecutionEngine::new(1);
        bad_eng.learned_mfe_bps = 647;
        bad_eng.learned_taker_prob_pm = 220;
        assert!(compute_ex_ante_ev_scaled_bps(&bad_eng, &snap) < 0);

        let mut good_eng = SlotExecutionEngine::new(5);
        good_eng.learned_mfe_bps = 1_350;
        good_eng.learned_taker_prob_pm = 45;
        assert!(compute_ex_ante_ev_scaled_bps(&good_eng, &snap) > 300);

        let sum = ClosedTradeSummary {
            side: Side::Ask,
            closed_lots: LotQty(20),
            entry_price: PriceTicks(7_279),
            exit_price: PriceTicks(7_270),
            gross_scaled_bps: ScaledBps(1_236),
            fee_scaled_bps: ScaledBps(400),
            net_scaled_bps: ScaledBps(836),
            net_pnl_1e8: 1_218_040,
            fee_1e8: 581_960,
            legs_consolidated: 2,
            is_maker_exit: true,
            kind: ClosedTradeKind::MakerPassiveUnwind,
        };
        let m5 = MarkoutCurve5hBps {
            samples_bps: [-35, -80, 140, 420, 910],
            resolved_mask: 0x1F,
            initial_edge_bps: 30,
            peak_zeta_tox_pm: 185,
        };
        let ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let entry_f = SlotForensicEntrySnapshot::default();
        let line = build_closed_trade_vault_json_v49(
            5,
            &sum,
            &good_eng,
            &entry_f,
            &snap,
            &ledger,
            &m5,
            140,
            TimestampNs(1_794_920_000_000_000_000),
        );
        assert!(line.contains("\"telemetry_rev\":\"V5.4\""));
        assert!(line.contains("\"markout_resolved_mask\":31"));
        assert!(line.contains("\"markout_5h_bps\":[-0.35,-0.8,1.4,4.2,9.1]"));
        assert!(line.contains("\"ws_rtt_us\":1310") && line.contains("\"fnv1a64_hex\":\"0x"));
    }

    #[test]
    fn test_v531_rehydrate_cold_mfe_seeds_from_scar_jsonl_preserves_warm_and_clamps_613_to_820() {
        let mut engines: [SlotExecutionEngine; MAX_ACTIVE_PAIRS] =
            core::array::from_fn(|idx| {
                let mut eng = SlotExecutionEngine::new(idx);
                eng.learned_mfe_bps = DEFAULT_LEARNED_MFE_SCALED_BPS;
                eng
            });
        // Scratch-sunk (< PHI * 400 = 647.2) vs truly warm (>= 648) pairs
        engines[0].learned_mfe_bps = 568;
        engines[1].learned_mfe_bps = 647;
        engines[3].learned_mfe_bps = 628;
        engines[4].learned_mfe_bps = 710;
        engines[5].learned_mfe_bps = 890;

        let scar_jsonl = concat!(
            "{\"symbol\":\"BTCUSDT\",\"maker_exit\":true,\"mfe_bps\":14.0}\n",
            "{\"symbol\":\"SUIUSDT\",\"maker_exit\":true,\"mfe_bps\":5.0}\n",
            "{\"symbol\":\"SOLUSDT\",\"maker_exit\":true,\"mfe_bps\":8.40}\n",
            "{\"symbol\":\"SOLUSDT\",\"maker_exit\":false,\"mfe_bps\":12.0}\n",
            "{\"symbol\":\"SOLUSDT\",\"maker_exit\":true,\"mfe_bps\":4.20}\n",
            "{\"symbol\":\"LINKUSDT\",\"maker_exit\":true,\"net_bps\":3.50}\n",
            "{\"symbol\":\"LINKUSDT\",\"maker_exit\":true,\"net_bps\":0.30}\n",
            "{\"symbol\":\"WIFUSDT\",\"maker_exit\":true,\"mfe_bps\":35.0}\n",
            "{\"symbol\":\"WIFUSDT\",\"maker_exit\":true,\"mfe_bps\":35.0}\n",
            "{\"symbol\":\"ORDIUSDT\",\"maker_exit\":true,\"mfe_bps\":250.0}\n",
            "{\"symbol\":\"ORDIUSDT\",\"maker_exit\":true,\"mfe_bps\":250.0}\n",
            "{\"symbol\":\"ORDIUSDT\",\"maker_exit\":true,\"mfe_bps\":250.0}\n",
            "{\"symbol\":\"NEARUSDT\",\"maker_exit\":true,\"mfe_bps\":5.00}\n",
        );
        rehydrate_cold_mfe_seeds_from_reader(&mut engines, std::io::Cursor::new(scar_jsonl.as_bytes()));

        assert!(engines[0].learned_mfe_bps > 568);                                      // BTC: sunk < PHI*400 rescued by 14.0bp Maker win
        assert_eq!(engines[1].learned_mfe_bps, 647);                                    // ETH: 0 rows -> preserved
        assert_eq!(engines[3].learned_mfe_bps, 628);                                    // AVAX: 0 rows -> preserved
        assert_eq!(engines[4].learned_mfe_bps, 710);                                    // TAO: warm >= PHI*400 untouched
        assert_eq!(engines[5].learned_mfe_bps, 890);                                    // SUI: warm >= PHI*400 untouched
        assert!(engines[2].learned_mfe_bps > 640 && engines[2].learned_mfe_bps < 660); // SOL: 613 -> ~646
        assert!(engines[9].learned_mfe_bps > 630 && engines[9].learned_mfe_bps < 640); // LINK: net 3.5+4.0=7.5 -> ~633
        assert!(engines[8].learned_mfe_bps > 820 && engines[8].learned_mfe_bps <= SCAR_MFE_REHYDRATE_CAP_SCALED_BPS);
        assert_eq!(engines[6].learned_mfe_bps, SCAR_MFE_REHYDRATE_CAP_SCALED_BPS);     // DOGE: capped at elastic 12 * active_fee_rt_mkr (4800)
        assert_eq!(engines[10].learned_mfe_bps, DEFAULT_LEARNED_MFE_SCALED_BPS);       // NEAR: floored at 613
        assert_eq!(engines[11].learned_mfe_bps, DEFAULT_LEARNED_MFE_SCALED_BPS);       // LTC: 0 samples -> 613
    }
}
