//! Tear-Free SHM v15 Live HUD, Executive Dashboard, OB/Markout Forensic Vault & Telegram Notifier.

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

use core::sync::atomic::Ordering;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::thread;

use crate::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use crate::bybit_client::BybitV5LiveClient;
use crate::execution_fsm::{
    defensive_zero_loss_maker_exit_price, dynamic_maker_floor_price, ClosedTradeKind,
    ClosedTradeSummary, ExitSubReason, OnlineAdaptiveGovernor, OrderLifecycleState,
    SlotExecutionEngine,
};
use crate::exit_quoter::compute_elastic_offensive_win_floor_bps;
use crate::forensic_parquet::{ColumnarParquetBatch, ParitySyncLedger};
use crate::http_ws_transport::{load_dotenv_map, parse_endpoint_url, KeepAliveHttpConn};
use crate::lob_shm::{LastVetoCode, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS};
use crate::markout_tracker::MarkoutCurveBps;
use crate::types::{
    classify_trade_badge, now_epoch_ns, print_auto_aligned_box, side_from_str,
    slot_from_symbol_name, visible_terminal_width, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TimestampNs, ANSI_BLUE, ANSI_DARK_GREEN, ANSI_GRAY, ANSI_GREEN, ANSI_ORANGE, ANSI_RED,
    ANSI_RESET, ANSI_SKY_BLUE, ANSI_YELLOW, APPROVED_SPECS,
};
use crate::vault_io::{append_jsonl_line_sync, atomic_write_bytes_sync};

pub const DEFAULT_WALLET_EQUITY_1E8: i64 = 6_628_080_000;
pub const FALLBACK_NOTIONAL_1E8: i64 = 1_922_140_000;
pub const TELEGRAM_QUEUE_CAP: usize = 64;

pub const AI_FORENSIC_VAULT_PATH: &str = "data/vault/vortex_ai_forensic_vault.jsonl";
pub const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";
pub const SCAR_MEMORY_VAULT_PATH: &str = "data/vault/vortex_governor_scar_memory.jsonl";
pub const OPEN_POS_STATE_PATH: &str = "data/vault/vortex_open_positions_state.json";
pub const DRAIN_LOCK_PATH: &str = "data/vault/DRAIN_MODE.lock";
pub const HALT_LOCK_PATH: &str = "data/vault/HALTED_ON_LOSS.lock";

/// Maps SHM v15 `LastVetoCode` to fixed-width HUD diagnostic badge without heap allocation.
#[inline]
#[must_use]
pub const fn format_veto_badge(code: LastVetoCode) -> &'static str {
    match code {
        LastVetoCode::None => "NONE_CLEAR",
        LastVetoCode::WarmupOrLock => "WARMUP_LOCK",
        LastVetoCode::BetaOrSameSideStagger => "BETA_STAGGER",
        LastVetoCode::SpoofingVeto => "SPOOF_VETO",
        LastVetoCode::Friction3xOrEligibility => "FRICTION_3X",
        LastVetoCode::QueueDepthOrOversizedLot => "LOT_Q_CAP",
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
    pub armed_ts_ns: i64,
    pub init_q_lots: i64,
    pub ofi_in: f64,
    pub tfi_in: f64,
    pub geom_in: f64,
    pub snr_in: f64,
    pub vol5m_in: f64,
    pub spread_in: i64,
    pub btc_comp_in: f64,
    pub gov_ewma_bps: f64,
    pub win_streak: u32,
    pub sl_streak: u32,
    pub drift_bps: f64,
    pub ext_bps: f64,
    pub kin_accel_net_bps: f64,
    pub kin_energy_in_bps: f64,
    pub req_kin_energy_in_bps: f64,
}

#[derive(Clone, Debug)]
pub enum VaultPersistTask {
    SaveOpenPositions(String),
    AppendClosedTrade {
        vault_line: String,
        scar_line: String,
        is_red_loss: bool,
    },
    FlushSync(SyncSender<()>),
}

#[must_use]
pub fn spawn_bounded_telegram_notifier() -> SyncSender<String> {
    let (tx, rx) = mpsc::sync_channel::<String>(TELEGRAM_QUEUE_CAP);
    let env_map = load_dotenv_map();
    let token = env_map.get("TELEGRAM_BOT_TOKEN").cloned().unwrap_or_default();
    let chat_id = env_map.get("TELEGRAM_CHAT_ID").cloned().unwrap_or_default();
    let tg_base = env_map
        .get("TELEGRAM_API_BASE")
        .cloned()
        .unwrap_or_else(|| "https://api.telegram.org".to_string());
    thread::spawn(move || {
        let mut conn_opt = parse_endpoint_url(&tg_base).map(KeepAliveHttpConn::new);
        let path = format!("/bot{}/sendMessage", token);
        while let Ok(msg) = rx.recv() {
            if token.is_empty() || chat_id.is_empty() {
                continue;
            }
            if let Some(ref mut conn) = conn_opt {
                let body = serde_json::json!({
                    "chat_id": chat_id,
                    "text": msg,
                })
                .to_string();
                let _ = conn.send_json_request("POST", &path, &[], &body);
            }
        }
    });
    tx
}

pub fn write_closed_trade_to_disk(vault_line: &str, scar_line: &str, is_red_loss: bool) {
    let _ = append_jsonl_line_sync(Path::new(AI_FORENSIC_VAULT_PATH), vault_line);
    let _ = append_jsonl_line_sync(Path::new(SCAR_MEMORY_VAULT_PATH), scar_line);
    if is_red_loss {
        let _ = append_jsonl_line_sync(Path::new(RED_LOSS_BOX_PATH), vault_line);
        let _ = atomic_write_bytes_sync(Path::new(HALT_LOCK_PATH), vault_line.as_bytes());
    }
}

#[must_use]
pub fn spawn_bounded_vault_persister() -> SyncSender<VaultPersistTask> {
    let (tx, rx) = mpsc::sync_channel::<VaultPersistTask>(128);
    thread::spawn(move || {
        let _ = fs::create_dir_all("data/vault");
        while let Ok(task) = rx.recv() {
            match task {
                VaultPersistTask::FlushSync(ack_tx) => {
                    let _ = ack_tx.send(());
                }
                VaultPersistTask::SaveOpenPositions(json_txt) => {
                    let _ = atomic_write_bytes_sync(
                        Path::new(OPEN_POS_STATE_PATH),
                        json_txt.as_bytes(),
                    );
                }
                VaultPersistTask::AppendClosedTrade {
                    vault_line,
                    scar_line,
                    is_red_loss,
                } => {
                    write_closed_trade_to_disk(&vault_line, &scar_line, is_red_loss);
                }
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
            side,
            entry_price,
            avg_price_ticks,
            peak_favorable_price,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            defensive_floor_locked,
            structural_tp_price,
            resting_exit_price,
            sl_trigger_price,
            opened_ts,
            ..
        } = eng.state
        {
            let ef = entry_forensics[eng.slot_idx];
            arr.push(serde_json::json!({
                "slot_idx": eng.slot_idx,
                "side": if matches!(side, Side::Ask) { "Ask" } else { "Bid" },
                "entry_ticks": entry_price.0,
                "avg_ticks": avg_price_ticks.0,
                "peak_ticks": peak_favorable_price.0,
                "worst_ticks": worst_adverse_price.0,
                "initial_lots": initial_lots.0,
                "open_lots": open_lots.0,
                "tranche1_harvested": tranche1_harvested,
                "tranche1_lots": tranche1_lots.0,
                "tranche1_exit_ticks": tranche1_exit_price.0,
                "defensive_floor_locked": defensive_floor_locked,
                "structural_tp_ticks": structural_tp_price.0,
                "tp_ticks": resting_exit_price.0,
                "sl_ticks": sl_trigger_price.0,
                "opened_ts_ns": opened_ts.0,
                "armed_ts_ns": ef.armed_ts_ns,
                "init_q_lots": ef.init_q_lots,
                "ofi_in": ef.ofi_in,
                "tfi_in": ef.tfi_in,
                "geom_in": ef.geom_in,
                "snr_in": ef.snr_in,
                "vol5m_in": ef.vol5m_in,
                "spread_in": ef.spread_in,
                "btc_comp_in": ef.btc_comp_in,
                "gov_ewma_bps": ef.gov_ewma_bps,
                "win_streak": ef.win_streak,
                "sl_streak": ef.sl_streak,
                "drift_bps": ef.drift_bps,
                "ext_bps": ef.ext_bps,
                "kin_accel_net_bps": ef.kin_accel_net_bps,
                "kin_energy_in_bps": ef.kin_energy_in_bps,
                "req_kin_energy_in_bps": ef.req_kin_energy_in_bps,
            }));
        }
    }
    serde_json::to_string_pretty(&arr).ok()
}

pub fn dispatch_open_positions_state(
    engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    vault_tx: Option<&SyncSender<VaultPersistTask>>,
) {
    let Some(txt) = serialize_open_positions_state(engines, entry_forensics) else {
        return;
    };
    if let Some(tx) = vault_tx {
        let task = VaultPersistTask::SaveOpenPositions(txt);
        if let Err(mpsc::TrySendError::Full(t) | mpsc::TrySendError::Disconnected(t)) =
            tx.try_send(task)
        {
            if let VaultPersistTask::SaveOpenPositions(fb_txt) = t {
                let _ = atomic_write_bytes_sync(
                    Path::new(OPEN_POS_STATE_PATH),
                    fb_txt.as_bytes(),
                );
            }
        }
    } else {
        let _ = atomic_write_bytes_sync(Path::new(OPEN_POS_STATE_PATH), txt.as_bytes());
    }
}

#[must_use]
pub fn load_saved_slot_state(slot_idx: usize) -> Option<serde_json::Value> {
    let txt = fs::read_to_string(OPEN_POS_STATE_PATH).ok()?;
    let arr = serde_json::from_str::<Vec<serde_json::Value>>(&txt).ok()?;
    arr.into_iter()
        .find(|v| v.get("slot_idx").and_then(|s| s.as_u64()) == Some(slot_idx as u64))
}

#[must_use]
pub fn restore_entry_forensics_from_json(
    saved: &serde_json::Value,
    opened_ns: i64,
    init_lots: i64,
) -> SlotForensicEntrySnapshot {
    let f64_at = |k: &str| saved.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let i64_at = |k: &str, d: i64| saved.get(k).and_then(|v| v.as_i64()).unwrap_or(d);
    let u32_at = |k: &str| saved.get(k).and_then(|v| v.as_u64()).unwrap_or(0).min(999) as u32;
    SlotForensicEntrySnapshot {
        armed_ts_ns: i64_at("armed_ts_ns", opened_ns),
        init_q_lots: i64_at("init_q_lots", init_lots),
        ofi_in: f64_at("ofi_in"),
        tfi_in: f64_at("tfi_in"),
        geom_in: f64_at("geom_in"),
        snr_in: f64_at("snr_in"),
        vol5m_in: f64_at("vol5m_in"),
        spread_in: i64_at("spread_in", 1),
        btc_comp_in: f64_at("btc_comp_in"),
        gov_ewma_bps: f64_at("gov_ewma_bps"),
        win_streak: u32_at("win_streak"),
        sl_streak: u32_at("sl_streak"),
        drift_bps: f64_at("drift_bps"),
        ext_bps: f64_at("ext_bps"),
        kin_accel_net_bps: f64_at("kin_accel_net_bps"),
        kin_energy_in_bps: f64_at("kin_energy_in_bps"),
        req_kin_energy_in_bps: f64_at("req_kin_energy_in_bps"),
    }
}

#[must_use]
pub fn extract_vault_row_pnl_and_fee_1e8(
    v: &serde_json::Value,
    s_idx_opt: Option<usize>,
    side: Side,
    net_sc: ScaledBps,
    fee_sc: ScaledBps,
) -> (i64, i64, PriceTicks) {
    let en_t = PriceTicks(v.get("entry_ticks").and_then(|n| n.as_i64()).unwrap_or(0));
    let ex_t = PriceTicks(v.get("exit_ticks").and_then(|n| n.as_i64()).unwrap_or(0));
    if let (Some(pnl), Some(fee)) = (
        v.get("net_pnl_1e8").and_then(|n| n.as_i64()),
        v.get("fee_1e8").and_then(|n| n.as_i64()),
    ) {
        return (pnl, fee, en_t);
    }
    if let Some(s_idx) = s_idx_opt {
        if en_t.0 > 0 && ex_t.0 > 0 {
            let spec = APPROVED_SPECS[s_idx];
            let lots_raw = v.get("closed_lots").and_then(|n| n.as_i64()).unwrap_or(0);
            let lots = if lots_raw > 0 {
                LotQty(lots_raw)
            } else {
                spec.compute_compounding_kelly_lots(DEFAULT_WALLET_EQUITY_1E8, en_t, 200, false)
            };
            let (pnl_1e8, fee_1e8, _, _) =
                spec.compute_leg_pnl_and_fee_1e8(side, lots, en_t, ex_t, fee_sc);
            return (pnl_1e8, fee_1e8, en_t);
        }
    }
    let pnl_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(net_sc.raw())) / 1_000_000;
    let fee_fb = (FALLBACK_NOTIONAL_1E8.saturating_mul(fee_sc.raw())) / 1_000_000;
    (pnl_fb, fee_fb, en_t)
}

#[must_use]
pub fn format_telegram_close_message(
    spec: PairSpec,
    summary: &ClosedTradeSummary,
    sub_reason: ExitSubReason,
    trade_num: u64,
    ledger: &ParitySyncLedger,
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
            "{} [VORTEX-OMEGA V4.5 {} #{} \u{2014} {:?}]\n",
            "- Ganancia : {:+.2}% @10x | {:+.4} USDT\n",
            "- Par y Lado : {} ({:?}) qty={} ({}L) | Maker: {}\n",
            "- Precios    : {}.{:04} -> {}.{:04} USDT ({}t -> {}t)\n",
            "- Balance    : Bruto: {:+.2} bps | Fee: -{:.2} bps | Neto: {:+.2} bps\n",
            "- Boveda 70/30: Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps"
        ),
        icon,
        lbl.trim(),
        trade_num,
        sub_reason,
        net_bps_f / 10.0,
        pnl_usdt,
        sym_name,
        summary.side,
        qty_s,
        summary.legs_consolidated,
        summary.is_maker_exit,
        en_w,
        en_f,
        ex_w,
        ex_f,
        summary.entry_price.0,
        summary.exit_price.0,
        gross_f,
        fee_f,
        net_bps_f,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0
    )
}

pub fn rehydrate_shadow_ledger(
    ledger: &mut ParitySyncLedger,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
) {
    let (scar_max_ts_ns, scar_rows_ingested) = gov.rehydrate_from_scar_and_loss_files(
        Path::new(SCAR_MEMORY_VAULT_PATH),
        Path::new(RED_LOSS_BOX_PATH),
        Some(Path::new(AI_FORENSIC_VAULT_PATH)),
    );
    let mut l3_vault_count = 0_u64;
    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if v.get("event").and_then(|e| e.as_str()) != Some("TRADE_CLOSE") {
                continue;
            }
            l3_vault_count += 1;
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            let side_s = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
            let net_f = v.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let fee_f = v.get("fee_bps").and_then(|n| n.as_f64()).unwrap_or(4.0);
            let mfe_f = v.get("mfe_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let side = side_from_str(side_s);
            let net_sc = ScaledBps((net_f * 100.0).round() as i64);
            let fee_sc = ScaledBps((fee_f * 100.0).round() as i64);
            let s_opt = slot_from_symbol_name(sym);
            let (pnl_1e8, fee_1e8, _) =
                extract_vault_row_pnl_and_fee_1e8(&v, s_opt, side, net_sc, fee_sc);
            ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
            if let Some(s_idx) = s_opt {
                let mfe_sc = (mfe_f * 100.0).round() as i64;
                if mfe_sc > 0 {
                    engines[s_idx].observe_mfe_sample(mfe_sc);
                }
            }
        }
    }
    ledger.closed_trades_count = l3_vault_count;
    arena.exec_closed_count.store(l3_vault_count, Ordering::Release);
    println!(
        "{}\u{1f7e2} [JSONL REHYDRATE V4.5] Session: {} trades | Scars: {} unique (Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps | ScarTs: {}){}",
        ANSI_GREEN,
        l3_vault_count,
        scar_rows_ingested,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0,
        scar_max_ts_ns,
        ANSI_RESET
    );
}

pub fn record_and_notify_entry_fill(
    s_idx: usize,
    spec: PairSpec,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    entry_forensics: &[SlotForensicEntrySnapshot; MAX_ACTIVE_PAIRS],
    arena: &VortexShmArena,
    tg_tx: &SyncSender<String>,
    vault_tx: &SyncSender<VaultPersistTask>,
    now_ns: TimestampNs,
    eff_side: Side,
    eff_entry_px: PriceTicks,
    eff_qty: LotQty,
    tag: &str,
) {
    let entry_f = &entry_forensics[s_idx];
    arena.exec_fill_count.fetch_add(1, Ordering::Relaxed);
    engines[s_idx].sync_to_shm_telemetry(arena);
    dispatch_open_positions_state(engines, entry_forensics, Some(vault_tx));
    let sym_name = spec.symbol_id.name();
    let (en_w, en_f) = spec.format_price_usdt_parts(eff_entry_px);
    let eff_win_floor = compute_elastic_offensive_win_floor_bps(
        engines[s_idx].active_fees.round_trip_maker_exit_bps().raw(),
        engines[s_idx].open_vol_5m_bps,
    );
    let win_fl_t = dynamic_maker_floor_price(eff_side, eff_entry_px, eff_win_floor);
    let (tp_w, tp_f) = spec.format_price_usdt_parts(win_fl_t);
    let (sl_w, sl_f) = spec.format_price_usdt_parts(engines[s_idx].last_eff_sl_ticks);
    let q_ms = (now_ns.0.saturating_sub(entry_f.armed_ts_ns)).max(0) / 1_000_000;
    let qty_s = spec.format_lot_qty_str(eff_qty);
    println!(
        "{}\u{1f7e1} [{}] {} {:?} qty={} filled passively @ {}.{:04} USDT ({}t | WIN_TP: {}.{:04} | SL: {}.{:04} | Wait: {}ms){}",
        ANSI_YELLOW,
        tag,
        sym_name,
        eff_side,
        qty_s,
        en_w,
        en_f,
        eff_entry_px.0,
        tp_w,
        tp_f,
        sl_w,
        sl_f,
        q_ms,
        ANSI_RESET
    );
    let _ = tg_tx.try_send(format!(
        concat!(
            "\u{1f7e1} [VORTEX-OMEGA V4.5 {}]\n",
            "\u{2022} Par y Lado : {} ({:?}) | Lote: {}\n",
            "\u{2022} Entrada L1 : {}.{:04} USDT ({}t) | Cola: {} ms\n",
            "\u{2022} Techos/SL  : WIN Floor: {}.{:04} USDT | SL: {}.{:04} USDT\n",
            "\u{2022} Microestr. : OFI:{:+.2} | TFI:{:+.2} | GEOM:{:+.2} | SNR:{:+.2} | Vol5m:{:.0}bp"
        ),
        tag,
        sym_name,
        eff_side,
        qty_s,
        en_w,
        en_f,
        eff_entry_px.0,
        q_ms,
        tp_w,
        tp_f,
        sl_w,
        sl_f,
        entry_f.ofi_in,
        entry_f.tfi_in,
        entry_f.geom_in,
        entry_f.snr_in,
        entry_f.vol5m_in
    ));
}
#[allow(clippy::similar_names)]
#[must_use]
pub fn build_closed_trade_vault_json(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    markout_curve: &MarkoutCurveBps,
    lvr_5s_scaled_bps: i64,
    ts_ns: TimestampNs,
) -> String {
    let spec = APPROVED_SPECS[slot_idx];
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
    let mfe_capture_pct = if mfe_bps > 0.01 {
        ((gross_bps / mfe_bps) * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    let hold_ms = (ts_ns.0.saturating_sub(eng.last_opened_ts.0)).max(0) / 1_000_000;
    let q_wait_ms = if entry_f.armed_ts_ns > 0 && eng.last_opened_ts.0 >= entry_f.armed_ts_ns {
        (eng.last_opened_ts.0 - entry_f.armed_ts_ns) / 1_000_000
    } else {
        0
    };
    let q_floor = defensive_zero_loss_maker_exit_price(summary.side, summary.entry_price);
    let w_obs = exit_snap.wave_observables();
    let ob_obs = exit_snap.order_block_observables();
    let ref_mid = if exit_snap.bids[0].price.0 > 0 && exit_snap.asks[0].price.0 > 0 {
        i64::midpoint(exit_snap.bids[0].price.0, exit_snap.asks[0].price.0)
    } else {
        summary.exit_price.0.max(1)
    };
    let mid_f = ref_mid.max(1) as f64;
    let ob_bull_bps = if ob_obs.bull_wall_ticks.0 > 0 {
        ((((ref_mid - ob_obs.bull_wall_ticks.0) as f64) * 10_000.0 / mid_f) * 10.0).round() / 10.0
    } else {
        0.0
    };
    let ob_bear_bps = if ob_obs.bear_wall_ticks.0 > 0 {
        ((((ob_obs.bear_wall_ticks.0 - ref_mid) as f64) * 10_000.0 / mid_f) * 10.0).round() / 10.0
    } else {
        0.0
    };
    let funding_bps = (exit_snap.funding_rate_scaled_bps.raw() as f64) / 100.0;
    let active_fee_rt_bps = (eng.active_fees.round_trip_maker_exit_bps().raw() as f64) / 100.0;
    let mk_arr = [
        (markout_curve.samples_bps[0] as f64) / 100.0,
        (markout_curve.samples_bps[1] as f64) / 100.0,
        (markout_curve.samples_bps[2] as f64) / 100.0,
        (markout_curve.samples_bps[3] as f64) / 100.0,
    ];
    let mut payload = serde_json::json!({
        "event": "TRADE_CLOSE",
        "trade_id": ledger.closed_trades_count,
        "armed_ts_ns": entry_f.armed_ts_ns,
        "opened_ts_ns": eng.last_opened_ts.0,
        "ts_ns": ts_ns.0,
        "queue_wait_ms": q_wait_ms,
        "hold_ms": hold_ms,
        "symbol": sym,
        "slot_idx": slot_idx,
        "side": side_str,
        "closed_lots": summary.closed_lots.0,
        "legs_consolidated": summary.legs_consolidated,
        "entry_ticks": summary.entry_price.0,
        "exit_ticks": summary.exit_price.0,
        "floor_ticks": q_floor.0,
        "quantum_floor_ticks": q_floor.0,
        "gross_bps": gross_bps,
        "fee_bps": fee_bps,
        "net_bps": net_bps,
        "fee_drag_pct": (fee_drag_pct * 10.0).round() / 10.0,
        "net_pnl_1e8": summary.net_pnl_1e8,
        "fee_1e8": summary.fee_1e8,
        "cum_net_pnl_1e8": ledger.cumulative_net_pnl_1e8,
        "kind": kind_str,
        "exit_sub_reason": format!("{:?}", eng.last_exit_sub_reason),
        "maker_exit": summary.is_maker_exit,
        "peak_ticks": eng.last_closed_peak_ticks.0,
        "mfe_bps": mfe_bps,
        "worst_ticks": eng.last_closed_worst_ticks.0,
        "mae_bps": mae_bps,
        "mfe_capture_pct": (mfe_capture_pct * 10.0).round() / 10.0,
        "init_q_lots": entry_f.init_q_lots,
        "ofi_in": (entry_f.ofi_in * 100.0).round() / 100.0,
        "tfi_in": (entry_f.tfi_in * 100.0).round() / 100.0,
        "geom_in": (entry_f.geom_in * 100.0).round() / 100.0,
        "snr_in": (entry_f.snr_in * 100.0).round() / 100.0,
        "vol5m_in": entry_f.vol5m_in,
        "spread_in": entry_f.spread_in,
        "btc_comp_in": (entry_f.btc_comp_in * 100.0).round() / 100.0,
        "ofi_out": (exit_snap.factor_ofi.get() * 100.0).round() / 100.0,
        "tfi_out": (exit_snap.factor_tfi.get() * 100.0).round() / 100.0,
        "geom_out": (exit_snap.factor_geom.get() * 100.0).round() / 100.0,
        "vol5m_out": (exit_snap.realized_vol_5m.raw() as f64) / 100.0,
        "slot_mfe_bps": (eng.learned_mfe_bps as f64) / 100.0,
        "gov_ewma_bps": entry_f.gov_ewma_bps,
        "win_streak": entry_f.win_streak,
        "sl_streak": entry_f.sl_streak,
        "drift_bps": entry_f.drift_bps,
        "ext_bps": entry_f.ext_bps,
        "kin_accel_net_bps": entry_f.kin_accel_net_bps,
        "kin_energy_in_bps": entry_f.kin_energy_in_bps,
        "req_kin_energy_in_bps": entry_f.req_kin_energy_in_bps,
        "exit_peg_attempts": eng.exit_peg_attempts,
        "wald_contraction_pct": eng.last_closed_wald_pct,
        "lvdbs_surfs": eng.lvdbs_deep_surfs_count,
        "wave_regime": w_obs.regime.label(),
        "wave_climax": w_obs.climax_active,
        "wave_hurst": (w_obs.hurst_exponent() * 1000.0).round() / 1000.0,
        "wave_macro": (w_obs.macro_phase.get() * 100.0).round() / 100.0,
        "wave_oi_imp": (w_obs.oi_impulse.get() * 100.0).round() / 100.0,
        "wave_liq_imb": (w_obs.liq_imbalance.get() * 100.0).round() / 100.0,
        "wave_fib_bps": (w_obs.fib_proj_bps.raw() as f64) / 100.0,
        "ob_bull_bps": ob_bull_bps,
        "ob_bear_bps": ob_bear_bps,
        "ob_bull_score": (ob_obs.bull_ob_score.get() * 100.0).round() / 100.0,
        "ob_bear_score": (ob_obs.bear_ob_score.get() * 100.0).round() / 100.0,
        "ob_bull_pm": exit_snap.ob_bull_efficacy_pm,
        "ob_bear_pm": exit_snap.ob_bear_efficacy_pm,
        "ob_efficacy_pm": exit_snap.ob_learned_efficacy_pm,
        "funding_bps": funding_bps,
        "taker_prob_pm": eng.learned_taker_prob_pm,
        "active_fee_rt_bps": active_fee_rt_bps,
        "lvr_5s_bps": (lvr_5s_scaled_bps as f64) / 100.0,
        "markout_fill_bps": mk_arr,
    });
    let raw_pre = payload.to_string();
    let fnv = ColumnarParquetBatch::compute_fnv1a64(raw_pre.as_bytes());
    if let Some(map) = payload.as_object_mut() {
        map.insert(
            "fnv1a64_hex".to_string(),
            serde_json::Value::String(format!("{fnv:016x}")),
        );
    }
    payload.to_string()
}

pub fn dispatch_closed_trade_logs(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    gov: &OnlineAdaptiveGovernor,
    vault_tx: &SyncSender<VaultPersistTask>,
    halt_locked: &mut bool,
    ts_ns: TimestampNs,
) {
    let vault_line = build_closed_trade_vault_json(
        slot_idx,
        summary,
        eng,
        entry_f,
        exit_snap,
        ledger,
        &MarkoutCurveBps::default(),
        0,
        ts_ns,
    );
    let scar_line = gov.to_scar_snapshot_json(ts_ns).to_string();
    let is_red_loss = summary.net_scaled_bps.raw() < -50;
    if is_red_loss {
        *halt_locked = true;
    }
    let task = VaultPersistTask::AppendClosedTrade {
        vault_line,
        scar_line,
        is_red_loss,
    };
    if let Err(mpsc::TrySendError::Full(t) | mpsc::TrySendError::Disconnected(t)) =
        vault_tx.try_send(task)
    {
        if let VaultPersistTask::AppendClosedTrade {
            vault_line: vl,
            scar_line: sl,
            is_red_loss: rl,
        } = t
        {
            write_closed_trade_to_disk(&vl, &sl, rl);
        }
    }
}

fn pad_visible_right(s: &str, target_w: usize) -> String {
    let w = visible_terminal_width(s);
    format!("{}{}", s, " ".repeat(target_w.saturating_sub(w)))
}

fn fmt_3col(c1: String, c2: String, c3: String) -> String {
    format!(
        "   \u{2022} {} \u{2502} {} \u{2502} {}",
        pad_visible_right(&c1, 58),
        pad_visible_right(&c2, 41),
        c3
    )
}

fn format_position_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h {:02}m {:02}s", secs / 3_600, (secs % 3_600) / 60, secs % 60)
    }
}

#[allow(clippy::similar_names)]
pub fn render_live_hud(arena: &VortexShmArena) {
    let now_ns = now_epoch_ns().0.max(0) as u64;
    let hb_ns = arena.heartbeat_ns.load(Ordering::Acquire);
    let hb_age_ms = if hb_ns > 0 && now_ns >= hb_ns {
        (now_ns - hb_ns) / 1_000_000
    } else {
        0
    };
    let resyncs = arena.tap_seq_resync_count.load(Ordering::Acquire);
    let drain_active = Path::new(DRAIN_LOCK_PATH).exists();
    let halt_active = Path::new(HALT_LOCK_PATH).exists();
    let sec_title = vec![format!(
        "\u{1f4e1} [VORTEX-OMEGA V4.5 LIVE HUD] SHM v{}: /dev/shm/vortex_lob.mmap \u{2502} HB Age: {} ms \u{2502} Resyncs: {}",
        arena.version, hb_age_ms, resyncs
    )];
    let sec_hdr = vec![concat!(
        "SLOT  \u{2502} SYMBOL     \u{2502}      PyG (UPL) \u{2502} POSITION TIME \u{2502} ",
        "      SEQ_ID \u{2502}     BEST_BID \u{2502}     BEST_ASK \u{2502} SPR \u{2502} ",
        " X_OFI \u{2502}  X_TFI \u{2502} WAVE / OB+- / FND    \u{2502} VERDICT / VETO   "
    )
    .to_string()];
    let mut sec_rows = Vec::with_capacity(MAX_ACTIVE_PAIRS);
    let mut open_slots_count = 0_usize;
    let mut queued_slots_count = 0_usize;
    let btc_snap = arena
        .slot(0)
        .and_then(|s| s.read_snapshot(4))
        .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());

    for s_idx in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s_idx];
        let snap = arena
            .slot(s_idx)
            .and_then(|s| s.read_snapshot(4))
            .unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
        let tel_snap = arena
            .telemetry(s_idx)
            .map(|t| t.read_consistent_or_default(4))
            .unwrap_or_default();
        let tel_state = tel_snap.state_code;
        let is_slot_open = (3..=5).contains(&tel_state);
        let is_slot_queued = tel_state == 1 || tel_state == 2;
        let mut pyg_col_cell = format!("{}{:>14}{}", ANSI_GRAY, "-", ANSI_RESET);
        let mut pos_time_cell = format!("{}{:>13}{}", ANSI_GRAY, "-", ANSI_RESET);
        if is_slot_open {
            open_slots_count += 1;
            let is_ask = tel_snap.side_code == 1;
            let avg_t = tel_snap.avg_price_ticks;
            let cur_t = if snap.mark_price.0 > 0 {
                snap.mark_price.0
            } else if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0)
            } else {
                avg_t
            };
            let diff_t = if is_ask { avg_t - cur_t } else { cur_t - avg_t };
            let unit_usdt = ((spec.tick_size_1e8 as f64) / 100_000_000.0)
                * ((spec.lot_step_1e8 as f64) / 100_000_000.0);
            let upl_usdt = (diff_t as f64) * (tel_snap.open_lots as f64) * unit_usdt;
            let upl_pct = ((diff_t as f64) / (avg_t.max(1) as f64)) * 1_000.0;
            let pyg_raw = format!("{upl_usdt:+.2}({upl_pct:+.1}%)");
            let pyg_col = if (diff_t.saturating_mul(1_000_000)) / avg_t.max(1) >= 440 {
                ANSI_GREEN
            } else if diff_t >= 0 {
                ANSI_YELLOW
            } else {
                ANSI_RED
            };
            pyg_col_cell = format!("{pyg_col}{pyg_raw:>14}{ANSI_RESET}");
            let hold_secs = if tel_snap.resting_since_ns > 0 && now_ns >= tel_snap.resting_since_ns {
                (now_ns - tel_snap.resting_since_ns) / 1_000_000_000
            } else {
                0
            };
            pos_time_cell = format!(
                "{}{:>13}{}",
                ANSI_YELLOW,
                format_position_duration(hold_secs),
                ANSI_RESET
            );
        } else if is_slot_queued {
            queued_slots_count += 1;
            pyg_col_cell = format!("{ANSI_SKY_BLUE}{:>14}{ANSI_RESET}", "IN_QUEUE_L1");
            let q_secs = if tel_snap.resting_since_ns > 0 && now_ns >= tel_snap.resting_since_ns {
                (now_ns - tel_snap.resting_since_ns) / 1_000_000_000
            } else {
                0
            };
            pos_time_cell = format!(
                "{ANSI_SKY_BLUE}{:>13}{ANSI_RESET}",
                format!("Q:{q_secs}s({})", tel_snap.queue_ahead_lots)
            );
        }
        let slot_col = if is_slot_open {
            format!("{ANSI_YELLOW}\u{1f7e1} {s_idx:2}{ANSI_RESET}")
        } else if is_slot_queued {
            format!("{ANSI_SKY_BLUE}\u{1fa75} {s_idx:2}{ANSI_RESET}")
        } else {
            format!("   {s_idx:2}")
        };
        let (bw, bf) = spec.format_price_usdt_parts(snap.bids[0].price);
        let (aw, af) = spec.format_price_usdt_parts(snap.asks[0].price);
        let spr = (snap.asks[0].price.0 - snap.bids[0].price.0).max(0);
        let w_lbl = snap.wave_observables().regime.label();
        let w_short: String = w_lbl.chars().take(4).collect();
        let bull_pm = if snap.ob_bull_efficacy_pm > 0 { snap.ob_bull_efficacy_pm } else { 618 };
        let bear_pm = if snap.ob_bear_efficacy_pm > 0 { snap.ob_bear_efficacy_pm } else { 618 };
        let fnd_bp = (snap.funding_rate_scaled_bps.raw() as f64) / 100.0;
        let macro_cell = format!("{w_short:<4} {bull_pm:3}/{bear_pm:3} {fnd_bp:+5.1}b");

        let veto_code = LastVetoCode::from_code(tel_snap.last_veto_code);
        let raw_sig =
            OrthogonalFactorState::evaluate_entry_signal_with_leader(spec, &snap, &btc_snap);
        let verdict_str = if is_slot_open {
            format!("{ANSI_GREEN}\u{1f7e2} CUSTODY_OPEN  {ANSI_RESET}")
        } else if is_slot_queued {
            format!("{ANSI_SKY_BLUE}\u{1fa75} RESTING_L1_Q  {ANSI_RESET}")
        } else {
            match (raw_sig, veto_code) {
                (SignalVerdict::SpoofingVeto, _) | (_, LastVetoCode::SpoofingVeto) => {
                    format!("{ANSI_ORANGE}\u{1f7e0} SPOOF_VETO    {ANSI_RESET}")
                }
                (SignalVerdict::AuthorizeMakerEntry { .. }, v) if v != LastVetoCode::None => {
                    format!("{ANSI_ORANGE}\u{1f6e1} {:<14}{ANSI_RESET}", format_veto_badge(v))
                }
                (SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. }, _) => {
                    format!("{ANSI_YELLOW}\u{1f7e1} AUTH_LONG_MKR {ANSI_RESET}")
                }
                (SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, .. }, _) => {
                    format!("{ANSI_YELLOW}\u{1f7e1} AUTH_SHORT_MKR{ANSI_RESET}")
                }
                (SignalVerdict::NeutralNoAction, v) if v != LastVetoCode::None => {
                    format!("{ANSI_GRAY}\u{26aa} {:<14}{ANSI_RESET}", format_veto_badge(v))
                }
                (SignalVerdict::NeutralNoAction, _) => {
                    format!("{ANSI_GRAY}\u{26aa} NEUTRAL_WAIT  {ANSI_RESET}")
                }
            }
        };
        sec_rows.push(format!(
            "{} \u{2502} {:<10} \u{2502} {} \u{2502} {} \u{2502} {:>12} \u{2502} {:>7}.{:04} \u{2502} {:>7}.{:04} \u{2502} {:>3} \u{2502} {:>+6.2} \u{2502} {:>+6.2} \u{2502} {:<20} \u{2502} {}",
            slot_col,
            spec.symbol_id.name(),
            pyg_col_cell,
            pos_time_cell,
            snap.sequence_id,
            bw,
            bf,
            aw,
            af,
            spr,
            snap.factor_ofi.get(),
            snap.factor_tfi.get(),
            macro_cell,
            verdict_str
        ));
    }
    let state_banner = if halt_active {
        format!("{ANSI_RED}\u{1f534} HALTED_ON_LOSS ({open_slots_count} open custody){ANSI_RESET}")
    } else if drain_active {
        format!("{ANSI_GREEN}\u{1f7e2} DRAINING_OPEN_SESSIONS ({open_slots_count} open){ANSI_RESET}")
    } else if open_slots_count > 0 || queued_slots_count > 0 {
        format!(
            "{ANSI_YELLOW}\u{1f7e1} ACTIVE_MULTI_SLOT ({open_slots_count} open / {queued_slots_count} queued){ANSI_RESET}"
        )
    } else {
        format!("{ANSI_GRAY}\u{26aa} IDLE (SCANNING 12 SLOTS){ANSI_RESET}")
    };
    let sec_footer = vec![format!(
        "[EXEC FSM V4.5] State: {} \u{2502} [FLOW] \u{1f7e1}Arm:{:<3} \u{1f7e0}Abr:{:<3} \u{1f7e1}Fil:{:<3} \u{1f7e2}Cls:{:<3}",
        pad_visible_right(&state_banner, 45),
        arena.exec_armed_count.load(Ordering::Acquire),
        arena.exec_abort_count.load(Ordering::Acquire),
        arena.exec_fill_count.load(Ordering::Acquire),
        arena.exec_closed_count.load(Ordering::Acquire)
    )];
    print_auto_aligned_box(&[sec_title, sec_hdr, sec_rows, sec_footer]);
}

#[allow(clippy::similar_names)]
pub fn render_trades_and_executive_dashboard() {
    let mut rows_out = Vec::new();
    let (mut total, mut wins, mut partials, mut scratches, mut losses, mut maker_closes) =
        (0_usize, 0_usize, 0_usize, 0_usize, 0_usize, 0_usize);
    let (mut cum_gross, mut cum_fee, mut cum_net, mut sum_mfe, mut sum_mae, mut sum_hold_s) =
        (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
    let mut dash_ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);

    if let Ok(file) = File::open(AI_FORENSIC_VAULT_PATH) {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if v.get("event").and_then(|e| e.as_str()) != Some("TRADE_CLOSE") {
                continue;
            }
            total += 1;
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
            let side_s = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
            let en_t = v.get("entry_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let ex_t = v.get("exit_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let g_bps = v.get("gross_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let f_bps = v.get("fee_bps").and_then(|n| n.as_f64()).unwrap_or(4.0);
            let n_bps = v.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let mfe = v.get("mfe_bps").and_then(|n| n.as_f64()).unwrap_or(g_bps);
            let mae = v.get("mae_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let hold_ms = v.get("hold_ms").and_then(|n| n.as_i64()).unwrap_or(0);
            let sub_r = v.get("exit_sub_reason").and_then(|s| s.as_str()).unwrap_or("MakerExit");
            let mkr = v.get("maker_exit").and_then(|b| b.as_bool()).unwrap_or(true);
            if mkr {
                maker_closes += 1;
            }
            let is_partial =
                (sub_r.contains("Tranche1Harvest50") || sub_r.contains("Partial")) && n_bps < 1.00;
            if is_partial {
                partials += 1;
            } else if n_bps >= 1.00 {
                wins += 1;
            } else if n_bps >= -0.50 {
                scratches += 1;
            } else {
                losses += 1;
            }
            cum_gross += g_bps;
            cum_fee += f_bps;
            cum_net += n_bps;
            sum_mfe += mfe;
            sum_mae += mae;
            sum_hold_s += (hold_ms as f64) / 1_000.0;
            let net_sc = ScaledBps((n_bps * 100.0).round() as i64);
            let fee_sc = ScaledBps((f_bps * 100.0).round() as i64);
            let (pnl_1e8, fee_1e8, _) = extract_vault_row_pnl_and_fee_1e8(
                &v,
                slot_from_symbol_name(sym),
                side_from_str(side_s),
                net_sc,
                fee_sc,
            );
            dash_ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
            let (col, icon, lbl) = classify_trade_badge(n_bps, is_partial);
            rows_out.push(format!(
                "{}{} [TRADE #{:<3} {}] {:<10} {:<3} \u{2502} {:>7}t->{:<7}t \u{2502} G:{:>+6.2} F:{:>4.2} N:{:>+6.2}bp ({:>+5.2}%@10x) \u{2502} MFE:{:>+6.2} MAE:{:>+6.2} \u{2502} {:>4}s \u{2502} {:<21} (Mkr={}){}",
                col, icon, total, lbl, sym, side_s, en_t, ex_t, g_bps, f_bps, n_bps,
                n_bps / 10.0, mfe, mae, hold_ms / 1_000, sub_r, mkr, ANSI_RESET
            ));
        }
    }
    if rows_out.is_empty() {
        rows_out.push(
            "  \u{26aa} [LEDGER EMPTY] Waiting for first closed trade in VORTEX-OMEGA V4.5..."
                .to_string(),
        );
    }
    let mut live_client = BybitV5LiveClient::from_env_if_enabled(true);
    let fb_w = (DEFAULT_WALLET_EQUITY_1E8 as f64) / 100_000_000.0;
    let (w_usdt, eq_usdt, av_usdt, upl_usdt) = live_client
        .as_mut()
        .and_then(BybitV5LiveClient::fetch_wallet_and_equity_usdt)
        .unwrap_or((fb_w, fb_w, fb_w, 0.0));
    let eff_notional_usdt = (w_usdt * 0.29).max(18.0);
    let bps_to_usdt = eff_notional_usdt / 10_000.0;
    let reserve_bps = (dash_ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0;
    let locked_bps = (dash_ledger.vault_locked_profit_bps_x100 as f64) / 100.0;
    let net_usdt = (dash_ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    let fee_usdt = (dash_ledger.cumulative_fees_1e8 as f64) / 100_000_000.0;
    let gross_usdt = net_usdt + fee_usdt;
    let trade_goal: usize = if total < 25 {
        25
    } else if total < 50 {
        50
    } else if total < 100 {
        100
    } else if total < 250 {
        250
    } else if total < 500 {
        500
    } else if total < 1_000 {
        1_000
    } else {
        ((total / 500) + 1) * 500
    };
    let prog_25 = total.min(trade_goal);
    let pct_25 = ((prog_25 as f64) / (trade_goal as f64)) * 100.0;
    let bars_25 = ((pct_25 / 5.0).round() as usize).min(20);
    let bar_25_str = format!("{}{}", "\u{2588}".repeat(bars_25), "\u{2591}".repeat(20 - bars_25));
    let mkr_pct = if total > 0 {
        (maker_closes as f64 * 100.0) / (total as f64)
    } else {
        100.0
    };
    let goal_anchor_usdt = w_usdt.max(eq_usdt);
    let goal_pct = ((goal_anchor_usdt / 95.0) * 100.0).clamp(0.0, 100.0);
    let filled_bars = ((goal_pct / 5.0).round() as usize).min(20);
    let bar_95 = format!("{}{}", "\u{2588}".repeat(filled_bars), "\u{2591}".repeat(20 - filled_bars));
    let upl_col = if upl_usdt < 0.0 { ANSI_RED } else { ANSI_GREEN };
    let denom_t = (total.max(1)) as f64;

    let sec_summary = vec![format!(
        "{}\u{1f7e2} [LEDGER SUMMARY] Total: {} (\u{1f7e2} Wins: {} \u{2502} {}\u{1f332} Partials: {}{} \u{2502} {}\u{1fa75} Scratches: {}{} \u{2502} {}\u{1f534} Losses: {}{}) \u{2502} CumNet: {:+.2} bps ({:+.4} USDT){}",
        ANSI_GREEN, total, wins, ANSI_DARK_GREEN, partials, ANSI_GREEN, ANSI_SKY_BLUE,
        scratches, ANSI_GREEN, ANSI_RED, losses, ANSI_GREEN, cum_net, net_usdt, ANSI_RESET
    )];
    let sec_dash_hdr = vec![format!(
        "{}\u{1f48e} VORTEX-OMEGA V4.5 EXECUTIVE DASHBOARD \u{2014} BYBIT UTA V5 LIVE & SHM v15 TELEMETRY{}",
        ANSI_BLUE, ANSI_RESET
    )];
    let sec_wallet = vec![
        "\u{1f4b0} [1. DINERO REAL EN BILLETERA Y GESTION DE MARGEN MONETARIA (USDT)]".to_string(),
        fmt_3col(
            format!("{ANSI_GREEN}Saldo Billetera (Wallet) : {w_usdt:>10.4} USDT{ANSI_RESET}"),
            format!("{ANSI_YELLOW}Equity Total Vivo     : {eq_usdt:>10.4} USDT{ANSI_RESET}"),
            "Estado Cuenta  : ACTIVA (10x)".to_string(),
        ),
        fmt_3col(
            format!("Margen Disponible Libre  : {av_usdt:>10.4} USDT"),
            format!("{upl_col}PnL Abierto (UPL)     : {upl_usdt:>+10.4} USDT{ANSI_RESET}"),
            "Beta Monetario : <=144bp (Max 6)".to_string(),
        ),
        fmt_3col(
            format!("Nocional Compuesto (10x) : {eff_notional_usdt:>10.4} USDT"),
            format!("Margen Base / Tiro    : {:>10.4} USDT", eff_notional_usdt * 0.10),
            "Paridad Riesgo : 24bp + DST/Fnd".to_string(),
        ),
    ];
    let sec_perf = vec![
        "\u{1f4c8} [2. PROGRESO DE RACHA Y RENDIMIENTO CONSOLIDADO DE SESION]".to_string(),
        fmt_3col(
            format!("{:<15}: [{}] {:>3}/{} ({:>5.1}%)", format!("Meta {trade_goal} Tiros"), bar_25_str, prog_25, trade_goal, pct_25),
            format!("{:<22}: {:>10} tiros", format!("Faltan Meta {trade_goal}"), trade_goal.saturating_sub(prog_25)),
            format!("Salidas Maker  : {mkr_pct:>6.1}%"),
        ),
        fmt_3col(
            format!("Meta 95 USDT   : [{bar_95}] {goal_anchor_usdt:>5.2}/95 ({goal_pct:>4.1}%)"),
            format!("Faltan Meta 95        : {:>10.4} USDT", (95.0 - goal_anchor_usdt).max(0.0)),
            "Anclaje Meta   : Wallet Floor".to_string(),
        ),
        fmt_3col(
            format!("Balance (bps)  : Bruto: {cum_gross:>+10.2} bps"),
            format!("Comisiones (Fees)     : {:>+10.2} bps", -cum_fee),
            format!("Neto Sesion    : {:>+7.2} bps ({:+.2}% @10x)", cum_net, cum_net / 10.0),
        ),
        fmt_3col(
            format!("Balance (USDT) : Bruto: {gross_usdt:>+10.4} USDT"),
            format!("Comisiones (USDT)     : {:>+10.4} USDT", -fee_usdt),
            format!("Neto Exacto    : {net_usdt:>+7.4} USDT"),
        ),
        fmt_3col(
            format!("Fondo Saneam.  : Reserva(30%): {:>+6.2} bps (+{:.4}$)", reserve_bps, reserve_bps * bps_to_usdt),
            format!("Blindado Boveda (70%) : {locked_bps:>+10.2} bps"),
            format!("Blindado USDT  : +{:>.4} USDT", locked_bps * bps_to_usdt),
        ),
        fmt_3col(
            format!("Excursiones    : MFE Medio: {:>+8.2} bps", sum_mfe / denom_t),
            format!("MAE Medio             : {:>+10.2} bps", sum_mae / denom_t),
            format!("Custodia Media : {:>6.1} s", sum_hold_s / denom_t),
        ),
        format!(
            "   \u{2022} Conteo Tiros   : Total: {:<3} \u{2502} {}\u{1f7e2} WIN: {:<3} ({:>5.1}%){} \u{2502} {}\u{1f332} PART: {:<3} ({:>5.1}%){} \u{2502} {}\u{1fa75} SCR: {:<3} ({:>5.1}%){} \u{2502} {}\u{1f534} LOSS: {:<3} ({:>5.1}%){}",
            total,
            ANSI_GREEN, wins, (wins as f64 * 100.0) / denom_t, ANSI_RESET,
            ANSI_DARK_GREEN, partials, (partials as f64 * 100.0) / denom_t, ANSI_RESET,
            ANSI_SKY_BLUE, scratches, (scratches as f64 * 100.0) / denom_t, ANSI_RESET,
            ANSI_RED, losses, (losses as f64 * 100.0) / denom_t, ANSI_RESET
        ),
    ];
    print_auto_aligned_box(&[rows_out, sec_summary, sec_dash_hdr, sec_wallet, sec_perf]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::{BookLevel, MappedShmRegion, SlotTelemetrySnapshot};

    #[test]
    fn test_f05_f07_vault_exact_lot_pnl_forensic_json_and_70_30_vault() {
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let row_v = serde_json::json!({
            "entry_ticks": 759_000,
            "exit_ticks": 759_600,
            "closed_lots": 1
        });
        let (pnl, fee, en_t) = extract_vault_row_pnl_and_fee_1e8(
            &row_v,
            Some(0),
            Side::Bid,
            ScaledBps(390),
            ScaledBps(400),
        );
        assert_eq!(en_t, PriceTicks(759_000));
        assert_eq!(fee, 3_037_200);
        assert_eq!(pnl, 2_962_800);
        ledger.record_closed_trade_with_sinking_split(pnl, fee, ScaledBps(390));
        let locked_before = ledger.vault_locked_profit_bps_x100;
        ledger.record_closed_trade_with_sinking_split(-900_000, 500_000, ScaledBps(-900));
        assert_eq!(ledger.vault_locked_profit_bps_x100, locked_before);

        let mut engines = SlotExecutionEngine::new_12_slot_array();
        let mut forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
        let mut avax = SymbolLobSnapshot::new_empty();
        avax.symbol_id = APPROVED_SPECS[3].symbol_id;
        avax.realized_vol_5m = ScaledBps(7_000);
        avax.mark_price = PriceTicks(9_007);
        avax.last_trade_price = PriceTicks(9_008);
        assert!(engines[3]
            .adopt_live_open_position(
                Side::Ask,
                PriceTicks(8_030),
                LotQty(26),
                LotQty(26),
                false,
                PriceTicks(8_028),
                PriceTicks(9_007),
                TimestampNs(1_794_224_613_323_033_417),
                &avax,
            )
            .is_ok());
        forensics[3].init_q_lots = 1_748;
        forensics[3].ofi_in = -0.75;
        let json_opt = serialize_open_positions_state(&engines, &forensics);
        assert!(json_opt.is_some());
        if let Some(j_txt) = json_opt {
            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&j_txt) {
                assert_eq!(arr.len(), 1);
                let rest_f = restore_entry_forensics_from_json(&arr[0], 0, 0);
                assert_eq!(rest_f.init_q_lots, 1_748);
                assert!((rest_f.ofi_in - (-0.75)).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn test_v45_hud_veto_badges_and_forensic_ob_funding_markout_vault() {
        assert_eq!(format_veto_badge(LastVetoCode::OpposingOrderBlockWall), "OB_WALL");
        assert_eq!(format_veto_badge(LastVetoCode::MacroElliottContradiction), "MACRO_C1");
        assert_eq!(format_veto_badge(LastVetoCode::HawkesStoikovQueueAbort), "HAWKES_ABT");

        let mut eng = SlotExecutionEngine::new(3);
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        let summary = eng.finalize_close_with_lots(
            ClosedTradeKind::MakerPassiveUnwind,
            Side::Bid,
            PriceTicks(8_200),
            PriceTicks(8_208),
            LotQty(26),
            1,
            None,
            TimestampNs(1_700_000_050_000_000_000),
            &mut ledger,
        );
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.bids[0] = BookLevel { price: PriceTicks(8_207), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_209), qty: LotQty(240) };
        snap.ob_bull_efficacy_pm = 720;
        snap.ob_bear_efficacy_pm = 530;
        snap.ob_learned_efficacy_pm = 625;
        snap.funding_rate_scaled_bps = ScaledBps(-125);

        let curve = MarkoutCurveBps {
            samples_bps: [180, 320, 410, 590],
            resolved_mask: 0x0F,
            initial_edge_bps: 120,
        };
        let vault_txt = build_closed_trade_vault_json(
            3,
            &summary,
            &eng,
            &SlotForensicEntrySnapshot::default(),
            &snap,
            &ledger,
            &curve,
            -85,
            TimestampNs(1_700_000_050_000_000_000),
        );
        let parsed = serde_json::from_str::<serde_json::Value>(&vault_txt);
        assert!(parsed.is_ok());
        if let Ok(v) = parsed {
            assert_eq!(v.get("ob_bull_pm").and_then(|x| x.as_i64()), Some(720));
            assert_eq!(v.get("ob_bear_pm").and_then(|x| x.as_i64()), Some(530));
            assert_eq!(v.get("funding_bps").and_then(|x| x.as_f64()), Some(-1.25));
            assert!(v.get("taker_prob_pm").and_then(|x| x.as_i64()).unwrap_or(0) > 0);
            assert!(v.get("fnv1a64_hex").and_then(|x| x.as_str()).is_some_and(|s| s.len() == 16));
            assert_eq!(
                v.get("markout_fill_bps").and_then(|x| x.as_array()).map(Vec::len),
                Some(4)
            );
        }

        let shm_path = MappedShmRegion::ephemeral_self_test_path("v45_hud_shm15");
        if let Ok(reg) = MappedShmRegion::open_or_create(&shm_path, true) {
            if let Ok(t3) = reg.arena().telemetry(3) {
                t3.write_consistent(&SlotTelemetrySnapshot {
                    last_veto_code: LastVetoCode::OpposingOrderBlockWall as u8,
                    ..SlotTelemetrySnapshot::default()
                });
            }
            render_live_hud(reg.arena());
        }
        let _ = fs::remove_file(&shm_path);
    }
}
