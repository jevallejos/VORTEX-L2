//! Loopback-Only Observatory HTTP/SSE Backend (`127.0.0.1:8787`, Tear-Free SHM v15 & VKL1 Reader, V5.4.2).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::suboptimal_flops,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

use crate::candle_store::{CandleTimeframe, PerpetualCandleStore, DEFAULT_CANDLE_DIR};
use crate::fee_cadence_beta::FeeSchedule;
use crate::forensic_parquet::{ColumnarParquetBatch, ParitySyncLedger};
use crate::lob_shm::{
    LastVetoCode, MappedShmRegion, ReadOnlyShmRegion, SlotTelemetrySnapshot, SymbolLobSnapshot,
    MAX_ACTIVE_PAIRS, VORTEX_SHM_VERSION,
};
use crate::telemetry_hud::{
    extract_vault_row_pnl_and_fee_1e8, format_veto_badge, load_hud_fee_schedule,
    DEFAULT_WALLET_EQUITY_1E8, FALLBACK_NOTIONAL_1E8,
};
use crate::types::{
    now_epoch_ns, side_from_str, slot_from_symbol_name, PairSpec, ScaledBps, Side, VortexError,
    APPROVED_SPECS,
};
use crate::vault_io::load_daily_governor_from_path;
use crate::wave_elliott::WaveRegime;

pub const DEFAULT_WEB_BIND_IP: [u8; 4] = [127, 0, 0, 1];
pub const DEFAULT_WEB_PORT: u16 = 8787;
pub const CSRF_HEADER_NAME: &str = "x-vortex-csrf";
const MAX_CONCURRENT_WEB_CLIENTS: usize = 32;

#[derive(Copy, Clone, Debug, Default)]
struct CachedForensicSummary {
    file_len: u64,
    mtime_ns: u128,
    maker_fee_bps: i64,
    taker_fee_bps: i64,
    ledger: Option<ParitySyncLedger>,
    total: usize,
    wins: usize,
    scratches: usize,
    losses: usize,
    mkr_cnt: usize,
    ws_rtt_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebApiError {
    NonLoopbackBindForbidden,
    CsrfRejected,
    MalformedRequest,
    RouteNotFound,
    LabAlreadyRunning,
    IoFailure,
    Engine(VortexError),
}

#[derive(Clone, Debug)]
pub struct VortexWebConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub shm_path: PathBuf,
    pub candle_dir: PathBuf,
    pub vault_dir: PathBuf,
    pub data_dir: PathBuf,
    pub ui_html_path: PathBuf,
}

impl Default for VortexWebConfig {
    fn default() -> Self {
        Self {
            bind_ip: IpAddr::V4(Ipv4Addr::from(DEFAULT_WEB_BIND_IP)),
            port: DEFAULT_WEB_PORT,
            shm_path: MappedShmRegion::default_shm_path(),
            candle_dir: PathBuf::from(DEFAULT_CANDLE_DIR),
            vault_dir: PathBuf::from("data/vault"),
            data_dir: PathBuf::from("data"),
            ui_html_path: PathBuf::from("ui/vortex_observatory.html"),
        }
    }
}

impl VortexWebConfig {
    pub const fn validate_loopback_only(&self) -> Result<(), WebApiError> {
        if self.bind_ip.is_loopback() { Ok(()) } else { Err(WebApiError::NonLoopbackBindForbidden) }
    }
}

#[derive(Debug)]
pub struct VortexWebContext {
    pub cfg: VortexWebConfig,
    pub csrf_token: String,
    pub lab_running: AtomicBool,
    active_conns: AtomicUsize,
    shm_cache: Mutex<Option<(u128, ReadOnlyShmRegion)>>,
    vault_cache: Mutex<CachedForensicSummary>,
}

impl VortexWebContext {
    #[must_use]
    pub fn new(cfg: VortexWebConfig) -> Self {
        let csrf_token = generate_ephemeral_csrf_hex(now_epoch_ns().0);
        Self {
            cfg,
            csrf_token,
            lab_running: AtomicBool::new(false),
            active_conns: AtomicUsize::new(0),
            shm_cache: Mutex::new(None),
            vault_cache: Mutex::new(CachedForensicSummary::default()),
        }
    }

    pub fn verify_csrf_header(&self, headers: &[(String, String)]) -> Result<(), WebApiError> {
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("origin") {
                let ok_orig = v.starts_with("http://127.0.0.1") || v.starts_with("http://localhost");
                if !ok_orig { return Err(WebApiError::CsrfRejected); }
            }
        }
        let provided = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(CSRF_HEADER_NAME))
            .map_or("", |(_, v)| v.trim());
        if constant_time_eq_str(provided, &self.csrf_token) { Ok(()) } else { Err(WebApiError::CsrfRejected) }
    }
}

#[must_use]
pub fn generate_ephemeral_csrf_hex(seed_ns: i64) -> String {
    let (now, pid) = (now_epoch_ns().0, u64::from(std::process::id()));
    let mix = |salt: u64| -> u64 {
        let mut b = [0_u8; 32];
        b[0..8].copy_from_slice(&seed_ns.to_le_bytes());
        b[8..16].copy_from_slice(&now.to_le_bytes());
        b[16..24].copy_from_slice(&pid.to_le_bytes());
        b[24..32].copy_from_slice(&salt.to_le_bytes());
        ColumnarParquetBatch::compute_fnv1a64(&b)
    };
    format!("{:016x}{:016x}{:016x}{:016x}", mix(0xA101), mix(0xB202), mix(0xC303), mix(0xD404))
}

#[must_use]
pub fn constant_time_eq_str(a: &str, b: &str) -> bool {
    let (ab, bb) = (a.as_bytes(), b.as_bytes());
    if ab.is_empty() || ab.len() != bb.len() { return false; }
    let mut diff = 0_u8;
    for (&x, &y) in ab.iter().zip(bb.iter()) { diff |= x ^ y; }
    diff == 0
}

#[inline] fn px_f64(spec: PairSpec, t: i64) -> f64 { (t as f64) * (spec.tick_size_1e8 as f64) / 100_000_000.0 }
#[inline] fn qty_f64(spec: PairSpec, l: i64) -> f64 { (l as f64) * (spec.lot_step_1e8 as f64) / 100_000_000.0 }

fn read_jsonl_rows(path: &Path) -> Vec<serde_json::Value> {
    let Ok(file) = File::open(path) else { return Vec::new() };
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter_map(|ln| serde_json::from_str::<serde_json::Value>(&ln).ok())
        .collect()
}

fn file_len_and_mtime_ns(path: &Path) -> Option<(u64, u128)> {
    let md = fs::metadata(path).ok()?;
    let mt = md.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map_or(0, |d| d.as_nanos());
    Some((md.len(), mt))
}

fn load_web_fee_schedule(ctx: &VortexWebContext) -> FeeSchedule {
    let p = ctx.cfg.vault_dir.join("vortex_bybit_handshake.json");
    if let Ok(txt) = fs::read_to_string(p) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
            if let (Some(m), Some(t)) = (
                val.get("maker_fee_scaled_bps").and_then(serde_json::Value::as_i64),
                val.get("taker_fee_scaled_bps").and_then(serde_json::Value::as_i64),
            ) {
                return FeeSchedule::from_live_rates_clamped(ScaledBps(m), ScaledBps(t));
            }
        }
    }
    load_hud_fee_schedule()
}

#[must_use]
pub fn build_live_state_json(ctx: &VortexWebContext) -> serde_json::Value {
    let now = now_epoch_ns();
    let now_u64 = now.0.max(0) as u64;
    let fees = load_web_fee_schedule(ctx);
    let fee_rt = fees.round_trip_maker_exit_bps().0.max(100);
    let drain_mode = ctx.cfg.vault_dir.join("DRAIN_MODE.lock").exists();
    let halt_locked = ctx.cfg.vault_dir.join("HALTED_ON_LOSS.lock").exists();
    let (mut shm_online, mut hb_age_ms, mut armed, mut aborts, mut fills, mut closed_shm, mut resyncs) =
        (false, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64);
    let (mut open_slots, mut queued_slots) = (0_usize, 0_usize);
    let mut raw_slots = [(SymbolLobSnapshot::new_empty(), SlotTelemetrySnapshot::default()); MAX_ACTIVE_PAIRS];

    if let Ok(mut guard) = ctx.shm_cache.lock() {
        if let Some((_, mt)) = file_len_and_mtime_ns(&ctx.cfg.shm_path) {
            let need_reopen = guard.as_ref().is_none_or(|(prev_mt, _)| *prev_mt == 0 && mt > 0);
            if need_reopen {
                *guard = MappedShmRegion::open_existing_read_only(&ctx.cfg.shm_path).ok().map(|r| (mt.max(1), r));
            }
        } else {
            *guard = None;
        }
        if let Some((_, ref reg)) = *guard {
            shm_online = true;
            let ar = reg.arena();
            hb_age_ms = ar.heartbeat_age_ns(now).unwrap_or(0) / 1_000_000;
            let (c_arm, c_abt, c_fil, c_cls, c_res) = ar.counters();
            armed = c_arm; aborts = c_abt; fills = c_fil; closed_shm = c_cls; resyncs = c_res;
            for (s_idx, pair_slot) in raw_slots.iter_mut().enumerate() {
                let sn = ar.read_slot(s_idx, 4).unwrap_or_else(|_| SymbolLobSnapshot::new_empty());
                let tl = ar.read_telemetry(s_idx, 4).unwrap_or_default();
                *pair_slot = (sn, tl);
            }
        }
    }

    let vault_path = ctx.cfg.vault_dir.join("vortex_ai_forensic_vault.jsonl");
    let (f_len, f_mt) = file_len_and_mtime_ns(&vault_path).unwrap_or((0, 0));
    let (ledger, total, wins, scratches, losses, mkr_cnt, ws_rtt_us) = if let Ok(mut vc) = ctx.vault_cache.lock() {
        let hit = vc.ledger.is_some()
            && vc.file_len == f_len
            && vc.mtime_ns == f_mt
            && vc.maker_fee_bps == fees.maker_fee_bps.0
            && vc.taker_fee_bps == fees.taker_fee_bps.0;
        if !hit {
            let mut led = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
            led.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
            let (mut tot, mut wn, mut sc, mut ls, mut mk, mut rtt) = (0_usize, 0_usize, 0_usize, 0_usize, 0_usize, 0_u64);
            if f_len > 0 {
                for v in read_jsonl_rows(&vault_path) {
                    match v.get("event").and_then(serde_json::Value::as_str) {
                        Some("EPOCH_RESET") => {
                            let eq_a = v.get("anchor_equity_1e8").and_then(serde_json::Value::as_i64).unwrap_or(DEFAULT_WALLET_EQUITY_1E8);
                            led = ParitySyncLedger::new(eq_a);
                            led.set_live_vip_fee_rates(fees.maker_fee_bps, fees.taker_fee_bps);
                            tot = 0; wn = 0; sc = 0; ls = 0; mk = 0; rtt = 0;
                        }
                        Some("FUNDING_SETTLEMENT") => {
                            let f = v.get("funding_1e8").and_then(serde_json::Value::as_i64).unwrap_or(0);
                            let n = v.get("notional_1e8").and_then(serde_json::Value::as_i64).unwrap_or(FALLBACK_NOTIONAL_1E8);
                            if f != 0 { led.record_funding_settlement(f, n); }
                        }
                        Some("TRADE_CLOSE") => {
                            tot += 1;
                            let sym = v.get("symbol").and_then(serde_json::Value::as_str).unwrap_or("");
                            let side_s = v.get("side").and_then(serde_json::Value::as_str).unwrap_or("Bid");
                            let n_bps = v.get("net_bps").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                            let f_bps = v.get("fee_bps").and_then(serde_json::Value::as_f64).unwrap_or(4.0);
                            if v.get("maker_exit").and_then(serde_json::Value::as_bool).unwrap_or(true) { mk += 1; }
                            if let Some(r) = v.get("ws_rtt_us").and_then(serde_json::Value::as_u64) { rtt = r; }
                            if n_bps >= 1.00 { wn += 1; } else if n_bps >= -0.50 { sc += 1; } else { ls += 1; }
                            let (pnl_1e8, fee_1e8, _) = extract_vault_row_pnl_and_fee_1e8(
                                &v, slot_from_symbol_name(sym), side_from_str(side_s),
                                ScaledBps((n_bps * 100.0).round() as i64), ScaledBps((f_bps * 100.0).round() as i64), fees,
                            );
                            led.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, ScaledBps((n_bps * 100.0).round() as i64));
                        }
                        _ => {}
                    }
                }
            }
            *vc = CachedForensicSummary {
                file_len: f_len, mtime_ns: f_mt, maker_fee_bps: fees.maker_fee_bps.0, taker_fee_bps: fees.taker_fee_bps.0,
                ledger: Some(led), total: tot, wins: wn, scratches: sc, losses: ls, mkr_cnt: mk, ws_rtt_us: rtt,
            };
        }
        let led_out = vc.ledger.unwrap_or_else(|| ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8));
        (led_out, vc.total, vc.wins, vc.scratches, vc.losses, vc.mkr_cnt, vc.ws_rtt_us)
    } else {
        (ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8), 0, 0, 0, 0, 0, 0)
    };

    let mut eq_1e8 = ledger.current_equity_1e8();
    let mut dg = load_daily_governor_from_path(&ctx.cfg.vault_dir.join("vortex_daily_governor.json"), eq_1e8);
    if f_len == 0 && dg.day_open_equity_1e8 > 100_000_000 {
        eq_1e8 = dg.day_open_equity_1e8.saturating_add(dg.day_realized_pnl_1e8);
    }
    let _ = dg.sync_utc_day(now, eq_1e8);

    let mut slots_json = Vec::with_capacity(MAX_ACTIVE_PAIRS);
    let mut open_mtm_upl_1e8 = 0_i64;
    let pre_shield_5u_halt = halt_locked
        || (-ledger.cumulative_net_pnl_1e8).max(0) >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8;
    for (s_idx, &spec) in APPROVED_SPECS.iter().enumerate() {
        let (snap, tel) = raw_slots[s_idx];
        let w = snap.wave_observables();
        let w_lbl = match w.regime {
            WaveRegime::ImpulseWave3 => "ImpuW3", WaveRegime::ImpulseWave5 => "ImpuW5",
            WaveRegime::CorrectiveAbc => "CorrABC", WaveRegime::CorrectiveWave4 => "CorrW4",
            WaveRegime::ClimaxExhaustion => "Climax", WaveRegime::NeutralWarmup => "Warmup",
        };
        let (mut upl_bps, mut upl_pct_10x, mut upl_band, mut hold_s) = (0.0, 0.0, "IDLE", 0_u64);
        if tel.state_code >= 3 && tel.avg_price_ticks > 0 {
            open_slots += 1;
            let p_side = if tel.side_code == 1 { Side::Ask } else { Side::Bid };
            let ref_t = if snap.mark_price.0 > 0 { snap.mark_price.0 } else { tel.avg_price_ticks };
            if tel.open_lots > 0 {
                let (gross_1e8, est_fee_1e8, _, _) = spec.compute_leg_pnl_and_fee_1e8(
                    p_side,
                    crate::types::LotQty(tel.open_lots),
                    crate::types::PriceTicks(tel.avg_price_ticks),
                    crate::types::PriceTicks(ref_t),
                    fees.round_trip_maker_exit_bps(),
                );
                open_mtm_upl_1e8 = open_mtm_upl_1e8.saturating_add(gross_1e8.saturating_sub(est_fee_1e8));
            }
            let g_bps = ((ref_t - tel.avg_price_ticks) * p_side.sign_i64()).saturating_mul(1_000_000) / tel.avg_price_ticks.max(1);
            upl_bps = ((g_bps - fee_rt) as f64) / 100.0;
            upl_pct_10x = upl_bps / 10.0;
            upl_band = if upl_pct_10x >= 0.50 { "GOLD_SURGE" } else if upl_pct_10x >= 0.10 { "GREEN_WIN" } else if upl_pct_10x >= -0.05 { "CYAN_SCRATCH" } else { "RED_HEAT" };
            if tel.resting_since_ns > 0 && now_u64 >= tel.resting_since_ns { hold_s = (now_u64 - tel.resting_since_ns) / 1_000_000_000; }
        } else if tel.state_code == 1 || tel.state_code == 2 {
            queued_slots += 1;
            if tel.resting_since_ns > 0 && now_u64 >= tel.resting_since_ns { hold_s = (now_u64 - tel.resting_since_ns) / 1_000_000_000; }
        }
        let vc = LastVetoCode::from_code(tel.last_veto_code);
        let badge: &str = if tel.state_code >= 3 {
            if tel.tranche1_harvested { "OPEN_T1_SURF" } else { "OPEN_MAKER" }
        } else if tel.state_code == 1 || tel.state_code == 2 {
            "QUEUE_L1"
        } else if !shm_online {
            "SHM_OFFLINE"
        } else if pre_shield_5u_halt {
            "HALT_LOCKED"
        } else if drain_mode {
            "DRAIN_P0B"
        } else if dg.is_hibernating() {
            "SLEEP_DAILY"
        } else if vc == LastVetoCode::None {
            "READY"
        } else {
            format_veto_badge(vc)
        };
        let bids_l5: Vec<_> = (0..5).map(|i| [px_f64(spec, snap.bids[i].price.0), qty_f64(spec, snap.bids[i].qty.0)]).collect();
        let asks_l5: Vec<_> = (0..5).map(|i| [px_f64(spec, snap.asks[i].price.0), qty_f64(spec, snap.asks[i].qty.0)]).collect();
        slots_json.push(serde_json::json!({
            "slot_idx": s_idx, "symbol": spec.symbol_id.name(), "seq_id": snap.sequence_id,
            "bid": px_f64(spec, snap.bids[0].price.0), "ask": px_f64(spec, snap.asks[0].price.0),
            "mark": px_f64(spec, snap.mark_price.0), "spread_ticks": (snap.asks[0].price.0 - snap.bids[0].price.0).max(0),
            "ofi": snap.factor_ofi.get(), "tfi": snap.factor_tfi.get(), "geom": snap.factor_geom.get(),
            "vol5m_bps": (snap.realized_vol_5m.0 as f64) / 100.0, "hurst": w.hurst_exponent(), "wave_regime": w_lbl,
            "ob_bull_pm": snap.ob_bull_efficacy_pm, "ob_bear_pm": snap.ob_bear_efficacy_pm,
            "ob_bull_px": px_f64(spec, snap.ob_bull_wall_ticks.0), "ob_bear_px": px_f64(spec, snap.ob_bear_wall_ticks.0),
            "funding_bps": (snap.funding_rate_scaled_bps.0 as f64) / 100.0, "oi_x100": snap.open_interest_x100,
            "state_code": tel.state_code, "side": if tel.side_code == 1 { "Ask" } else { "Bid" },
            "entry_px": px_f64(spec, tel.avg_price_ticks), "tp_px": px_f64(spec, tel.tp_ticks), "sl_px": px_f64(spec, tel.sl_ticks),
            "open_qty": qty_f64(spec, tel.open_lots), "queue_ahead_lots": tel.queue_ahead_lots, "hold_sec": hold_s,
            "upl_bps": upl_bps, "upl_pct_10x": upl_pct_10x, "upl_band": upl_band,
            "veto_badge": badge,
            "bids_l5": bids_l5, "asks_l5": asks_l5,
        }));
    }

    let (epoch_loss_1e8, _) = crate::exec_gateway_handlers::compute_validation_epoch_loss_1e8(
        &ledger,
        None,
        open_mtm_upl_1e8,
    );
    let halt_effective = halt_locked || epoch_loss_1e8 >= crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8;
    let shield_5u_rem_1e8 = crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8
        .saturating_sub(epoch_loss_1e8.max(0))
        .max(0);
    let shield_5u_used_pct = ((epoch_loss_1e8.max(0) as f64) * 100.0
        / (crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8 as f64))
        .clamp(0.0, 999.9);
    let shield_5u_summary = crate::telemetry_hud::format_total_loss_shield_5u_plain(
        epoch_loss_1e8,
        halt_effective,
    );

    let mkr_pct = if total > 0 { (mkr_cnt as f64 * 100.0) / (total as f64) } else { 100.0 };
    let v70_bps = (ledger.vault_locked_profit_bps_x100 as f64) / 100.0;
    let r30_bps = (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0;
    let sem_rep = crate::telemetry_hud::evaluate_capital_scaling_semaphore(
        &vault_path, eq_1e8, ledger.vault_locked_profit_bps_x100, mkr_pct, 0, halt_effective, &dg, fees,
    );
    let (g1, g2, g3, g4) = (sem_rep.gate1_ok, sem_rep.gate2_ok, sem_rep.gate3_ok, sem_rep.gate4_ok);

    serde_json::json!({
        "ts_ns": now.0, "engine_version": "V5.4.2", "shm_version": VORTEX_SHM_VERSION, "shm_online": shm_online,
        "hb_age_ms": hb_age_ms, "resyncs": resyncs, "ws_rtt_us": ws_rtt_us,
        "drain_mode": drain_mode, "halt_locked": halt_effective,
        "lab_running": ctx.lab_running.load(Ordering::Acquire),
        "open_slots": open_slots, "queued_slots": queued_slots,
        "counters": { "armed": armed, "aborts": aborts, "fills": fills, "closed_shm": closed_shm },
        "ledger": {
            "equity_usdt": (eq_1e8 as f64) / 100_000_000.0,
            "net_pnl_usdt": (ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0,
            "vault_70_bps": v70_bps, "reserve_30_bps": r30_bps,
            "trades_total": total, "wins": wins, "scratches": scratches, "losses": losses,
            "maker_ratio_pct": mkr_pct, "sample_target": 25,
        },
        "daily_shield": {
            "phase": dg.phase.label(), "hibernating": dg.is_hibernating(),
            "day_pct": (dg.day_pnl_ppm() as f64) / 10_000.0,
            "day_usdt": (dg.day_realized_pnl_1e8 as f64) / 100_000_000.0,
            "stop_pct": (dg.elastic_loss_stop_ppm(ledger.sinking_fund_reserve_bps_x100, fee_rt) as f64) / 10_000.0,
            "wins": dg.day_wins, "losses": dg.day_losses,
        },
        "total_loss_shield": {
            "loss_1e8": epoch_loss_1e8.max(0),
            "max_loss_1e8": crate::vault_io::MAX_VALIDATION_EPOCH_LOSS_1E8,
            "loss_usdt": (epoch_loss_1e8.max(0) as f64) / 100_000_000.0,
            "max_loss_usdt": 5.0,
            "used_pct": shield_5u_used_pct,
            "remaining_usdt": (shield_5u_rem_1e8 as f64) / 100_000_000.0,
            "status": if halt_effective { "HALTED" } else { "ARMED" },
            "halted": halt_effective,
            "summary": shield_5u_summary,
        },
        "semaphore_4_4": {
            "oos_520_ok": g1, "vault_70_ok": g2, "maker_80_ok": g3, "sample_25_ok": g4,
            "passed": u8::from(g1) + u8::from(g2) + u8::from(g3) + u8::from(g4),
        },
        "slots": slots_json,
    })
}

pub fn query_candles_json(
    ctx: &VortexWebContext, symbol: &str, tf_label: &str, from_ns: Option<i64>, to_ns: Option<i64>, limit: usize,
) -> Result<serde_json::Value, WebApiError> {
    let Some(s_idx) = slot_from_symbol_name(symbol.trim()) else { return Err(WebApiError::MalformedRequest); };
    let Some(tf) = CandleTimeframe::from_label(tf_label) else { return Err(WebApiError::MalformedRequest); };
    let cap = limit.clamp(1, 4_096);
    let store = PerpetualCandleStore::new(&ctx.cfg.candle_dir);
    let rows = if from_ns.is_some() || to_ns.is_some() {
        store.query_range(s_idx, tf, from_ns.unwrap_or(0), to_ns.unwrap_or(i64::MAX), cap).map_err(WebApiError::Engine)?
    } else {
        store.read_tail(s_idx, tf, cap).map_err(WebApiError::Engine)?
    };
    let spec = APPROVED_SPECS[s_idx];
    let items: Vec<_> = rows.into_iter().map(|r| serde_json::json!({
        "time": r.open_ts_ns / 1_000_000_000, "ts_ms": r.open_ts_ns / 1_000_000,
        "open": px_f64(spec, r.open_ticks), "high": px_f64(spec, r.high_ticks),
        "low": px_f64(spec, r.low_ticks), "close": px_f64(spec, r.close_ticks),
        "volume": qty_f64(spec, r.volume_lots), "trades": r.trade_count,
        "ofi": (f64::from(r.ofi_mean_pm)) / 1_000.0, "tfi": (f64::from(r.tfi_mean_pm)) / 1_000.0,
        "geom": (f64::from(r.geom_mean_pm)) / 1_000.0, "funding_bps": (f64::from(r.funding_sbps)) / 100.0,
        "hurst": (f64::from(r.hurst_pm)) / 1_000.0, "ob_bull_pm": r.ob_bull_pm, "ob_bear_pm": r.ob_bear_pm,
        "spoof_vetoes": r.spoof_vetoes, "liq_buy": r.liq_buy_lots_c, "liq_sell": r.liq_sell_lots_c,
        "wave_regime": r.wave_regime, "oi_x100": r.open_interest_x100,
    })).collect();
    Ok(serde_json::json!({ "symbol": spec.symbol_id.name(), "tf": tf.as_str(), "count": items.len(), "candles": items }))
}

#[must_use]
pub fn query_recent_trades_json(ctx: &VortexWebContext, limit: usize) -> serde_json::Value {
    let mut closes = Vec::new();
    let mut seq_id = 0_u64;
    for mut v in read_jsonl_rows(&ctx.cfg.vault_dir.join("vortex_ai_forensic_vault.jsonl")) {
        if v.get("event").and_then(serde_json::Value::as_str) == Some("EPOCH_RESET") { closes.clear(); seq_id = 0; continue; }
        if v.get("event").and_then(serde_json::Value::as_str) == Some("TRADE_CLOSE") {
            seq_id += 1;
            if let Some(obj) = v.as_object_mut() { obj.entry("trade_id").or_insert_with(|| serde_json::json!(seq_id)); }
            closes.push(v);
        }
    }
    let take = limit.clamp(1, 500).min(closes.len());
    let tail: Vec<_> = closes.into_iter().rev().take(take).collect();
    let open_pos = fs::read_to_string(ctx.cfg.vault_dir.join("vortex_open_positions_state.json"))
        .ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!([]));
    serde_json::json!({ "total_closed": seq_id, "open_positions": open_pos, "trades": tail })
}

fn normalize_position_path_json(mut path_val: serde_json::Value) -> serde_json::Value {
    let needs_map = path_val.get("samples").and_then(serde_json::Value::as_array).is_none();
    if needs_map {
        if let Some(arr) = path_val.get("samples_1hz").and_then(serde_json::Value::as_array).cloned() {
            let mapped: Vec<serde_json::Value> = arr.into_iter().filter_map(|row| {
                if row.is_object() {
                    return Some(row);
                }
                let cols = row.as_array()?;
                if cols.len() < 8 {
                    return None;
                }
                Some(serde_json::json!({
                    "rel_sec": cols[0],
                    "mid_delta_sbps": cols[1],
                    "tp_delta_sbps": cols[2],
                    "sl_delta_sbps": cols[3],
                    "ofi_pm": cols[4],
                    "tfi_pm": cols[5],
                    "wald_shrink_pm": cols[6],
                    "zeta_tox_pm": cols[7],
                }))
            }).collect();
            if let Some(obj) = path_val.as_object_mut() {
                obj.insert("samples".to_string(), serde_json::Value::Array(mapped));
            }
        }
    }
    path_val
}

#[must_use]
pub fn query_position_path_json(ctx: &VortexWebContext, trade_id: Option<u64>) -> serde_json::Value {
    let rows = read_jsonl_rows(&ctx.cfg.vault_dir.join("vortex_position_paths.jsonl"));
    let matched = match trade_id {
        Some(tid) => rows.into_iter().rev().find(|v| v.get("trade_id").and_then(serde_json::Value::as_u64) == Some(tid)),
        None => rows.into_iter().last(),
    }.map(normalize_position_path_json);
    serde_json::json!({ "found": matched.is_some(), "path": matched.unwrap_or(serde_json::Value::Null) })
}

#[must_use]
pub fn query_queue_aborts_json(ctx: &VortexWebContext, limit: usize) -> serde_json::Value {
    let rows = read_jsonl_rows(&ctx.cfg.vault_dir.join("vortex_queue_aborts.jsonl"));
    let mut by_veto: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for r in &rows {
        let key = r
            .get("veto_name")
            .and_then(serde_json::Value::as_str)
            .or_else(|| r.get("veto_code").and_then(serde_json::Value::as_str))
            .or_else(|| r.get("veto").and_then(serde_json::Value::as_str))
            .map(ToString::to_string)
            .or_else(|| {
                r.get("veto_code")
                    .and_then(serde_json::Value::as_u64)
                    .map(|n| format_veto_badge(LastVetoCode::from_code((n.min(255)) as u8)).to_string())
            })
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let is_tp = r
            .get("true_positive")
            .and_then(serde_json::Value::as_bool)
            .or_else(|| r.get("is_true_positive").and_then(serde_json::Value::as_bool))
            .unwrap_or(false);
        let tp = u64::from(is_tp);
        let entry = by_veto.entry(key).or_insert((0, 0));
        entry.0 += tp;
        entry.1 += 1;
    }
    let stats: Vec<_> = by_veto.into_iter().map(|(k, (tp, tot))| serde_json::json!({
        "veto": k, "tp": tp, "total": tot, "precision_pct": if tot > 0 { (tp as f64 * 100.0) / (tot as f64) } else { 0.0 }
    })).collect();
    let take = limit.clamp(1, 300).min(rows.len());
    let recent: Vec<_> = rows.into_iter().rev().take(take).collect();
    serde_json::json!({ "shield_precision": stats, "aborts": recent })
}

const EMBEDDED_OBSERVATORY_HTML: &str = include_str!("../ui/vortex_observatory.html");

#[must_use]
pub fn render_observatory_html_with_csrf(ctx: &VortexWebContext) -> String {
    let raw = fs::read_to_string(&ctx.cfg.ui_html_path)
        .unwrap_or_else(|_| EMBEDDED_OBSERVATORY_HTML.to_string());
    raw.replace("__VORTEX_CSRF__", &ctx.csrf_token)
}

#[must_use]
pub fn build_markdown_360_clip(ctx: &VortexWebContext, trade_id: Option<u64>) -> String {
    use core::fmt::Write as _;
    let st = build_live_state_json(ctx);
    let lg = &st["ledger"];
    let ds = &st["daily_shield"];
    let sm = &st["semaphore_4_4"];
    let mut out = String::with_capacity(2_048);
    out.push_str("### VORTEX-OMEGA V5.4.2 \u{2014} Snapshot Ejecutivo 360 (Loopback Read-Only)\n");
    let _ = writeln!(
        out,
        "- **SHM v{}**: online={} | HB={} ms | Resyncs={} | WS_RTT={} us | Open={}/6 | Q={}",
        st["shm_version"], st["shm_online"], st["hb_age_ms"], st["resyncs"], st["ws_rtt_us"], st["open_slots"], st["queued_slots"]
    );
    let _ = writeln!(
        out,
        "- **Ledger & Boveda**: Equity={:.4} USDT | Net={:+.4} USDT | Vault70={:+.2} bps | Res30={:+.2} bps | Maker={:.2}% | Tiros={}/25 (W:{} S:{} L:{})",
        lg["equity_usdt"].as_f64().unwrap_or(0.0), lg["net_pnl_usdt"].as_f64().unwrap_or(0.0),
        lg["vault_70_bps"].as_f64().unwrap_or(0.0), lg["reserve_30_bps"].as_f64().unwrap_or(0.0),
        lg["maker_ratio_pct"].as_f64().unwrap_or(100.0), lg["trades_total"].as_u64().unwrap_or(0),
        lg["wins"].as_u64().unwrap_or(0), lg["scratches"].as_u64().unwrap_or(0), lg["losses"].as_u64().unwrap_or(0)
    );
    let _ = writeln!(
        out,
        "- **Escudo Diario & Semaforo**: Fase={} | Hoy={:+.2}% ({:+.4} USDT, Stop={:+.2}%) | Semaforo={}/4 | DRAIN={} | HALT={}",
        ds["phase"].as_str().unwrap_or("ACTIVE"), ds["day_pct"].as_f64().unwrap_or(0.0),
        ds["day_usdt"].as_f64().unwrap_or(0.0), ds["stop_pct"].as_f64().unwrap_or(0.0),
        sm["passed"].as_u64().unwrap_or(0), st["drain_mode"], st["halt_locked"]
    );
    if let Some(s5_txt) = st["total_loss_shield"]["summary"].as_str() {
        let _ = writeln!(out, "- **{s5_txt}**");
    }
    if let Some(arr) = st["slots"].as_array() {
        out.push_str("\n| Slot | Symbol | Bid | Ask | Spr | OFI | TFI | OB+/- | Estado / Veto |\n|---|---|---|---|---|---|---|---|---|\n");
        for s in arr {
            let _ = writeln!(
                out,
                "| {} | {} | {:.4} | {:.4} | {} | {:+.2} | {:+.2} | {}/{} | {} ({:+.2}bp) |",
                s["slot_idx"], s["symbol"].as_str().unwrap_or(""), s["bid"].as_f64().unwrap_or(0.0),
                s["ask"].as_f64().unwrap_or(0.0), s["spread_ticks"], s["ofi"].as_f64().unwrap_or(0.0),
                s["tfi"].as_f64().unwrap_or(0.0), s["ob_bull_pm"], s["ob_bear_pm"],
                s["veto_badge"].as_str().unwrap_or(""), s["upl_bps"].as_f64().unwrap_or(0.0)
            );
        }
    }
    if let Some(tid) = trade_id {
        let p = query_position_path_json(ctx, Some(tid));
        let _ = writeln!(out, "\n#### Autopsia Intra-Trade 1Hz (trade_id={tid})\n```json\n{}\n```", p["path"]);
    }
    out
}

pub fn handle_drain_mode_toggle(ctx: &VortexWebContext, enable: bool) -> Result<serde_json::Value, WebApiError> {
    fs::create_dir_all(&ctx.cfg.vault_dir).map_err(|_| WebApiError::IoFailure)?;
    let lock_path = ctx.cfg.vault_dir.join("DRAIN_MODE.lock");
    if enable {
        let msg = format!("DRAIN_MODE_V49 ts_ns={}\n", now_epoch_ns().0);
        crate::vault_io::atomic_write_bytes_sync(&lock_path, msg.as_bytes()).map_err(|_| WebApiError::IoFailure)?;
    } else if lock_path.exists() {
        fs::remove_file(&lock_path).map_err(|_| WebApiError::IoFailure)?;
    }
    let st = build_live_state_json(ctx);
    Ok(serde_json::json!({ "ok": true, "drain_mode": lock_path.exists(), "open_slots": st["open_slots"] }))
}

pub fn run_autonomous_lab_gate(ctx: &VortexWebContext, lab_id: u8) -> Result<serde_json::Value, WebApiError> {
    if !(1..=4).contains(&lab_id) { return Err(WebApiError::MalformedRequest); }
    if ctx.lab_running.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(WebApiError::LabAlreadyRunning);
    }
    let res = match lab_id {
        1 => {
            let wf = crate::replay_walkforward::run_parquet_walk_forward_on_dir(&ctx.cfg.data_dir, false);
            serde_json::json!({
                "lab_id": 1, "name": "PAR2_WALK_FORWARD", "ok": true,
                "files": wf.files_count, "events": wf.total_events,
                "closed": wf.closed, "net_bps": (wf.cum_net_bps_x100 as f64) / 100.0
            })
        }
        2 => {
            let arch = ctx.cfg.data_dir.join("archive");
            let mut wf = crate::replay_walkforward::run_par2z_archive_walk_forward(&arch, 64, false);
            if wf.files_count == 0 {
                wf = crate::replay_walkforward::run_parquet_walk_forward_on_dir(&ctx.cfg.data_dir, false);
            }
            serde_json::json!({
                "lab_id": 2, "name": "PAR2Z_COLD_ARCHIVE_WF", "ok": true,
                "files": wf.files_count, "events": wf.total_events,
                "closed": wf.closed, "net_bps": (wf.cum_net_bps_x100 as f64) / 100.0
            })
        }
        3 => {
            let oos = crate::exec_gateway_handlers::run_oos_520_multi_regime_suite();
            serde_json::json!({
                "lab_id": 3, "name": "OOS_520_PARITY_GATE",
                "ok": oos.closed_trades >= 500 && oos.parity_sync_drift_1e8 == 0,
                "closed": oos.closed_trades, "maker_pct": oos.maker_exit_ratio_pct,
                "pf": oos.profit_factor, "drift": oos.parity_sync_drift_1e8
            })
        }
        _ => {
            let ok = crate::replay_walkforward::run_and_print_omega_chat6_twin_certification();
            serde_json::json!({ "lab_id": 4, "name": "DIGITAL_TWIN_4ARM_GATE", "ok": ok })
        }
    };
    ctx.lab_running.store(false, Ordering::Release);
    Ok(res)
}

fn parse_query_map(q: &str) -> BTreeMap<String, String> {
    q.split('&').filter(|p| !p.is_empty()).filter_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        Some((k.to_string(), v.to_string()))
    }).collect()
}

#[must_use]
pub fn dispatch_http_request(
    ctx: &VortexWebContext, method: &str, raw_target: &str, headers: &[(String, String)], body: &str,
) -> (u16, &'static str, String) {
    let (path, q_str) = raw_target.split_once('?').unwrap_or((raw_target, ""));
    let q = parse_query_map(q_str);
    let lim = q.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(240);
    let tid = q.get("trade_id").or_else(|| q.get("id")).and_then(|s| s.parse::<u64>().ok());

    match (method, path) {
        ("GET", "/" | "/index.html") => (200, "text/html; charset=utf-8", render_observatory_html_with_csrf(ctx)),
        ("GET", "/api/state") => (200, "application/json", build_live_state_json(ctx).to_string()),
        ("GET", "/api/candles") => {
            let sym = q.get("symbol").map_or("BTCUSDT", String::as_str);
            let tf = q.get("tf").map_or("1m", String::as_str);
            let f_ns = q.get("from_ns").and_then(|s| s.parse::<i64>().ok());
            let t_ns = q.get("to_ns").and_then(|s| s.parse::<i64>().ok());
            match query_candles_json(ctx, sym, tf, f_ns, t_ns, lim) {
                Ok(v) => (200, "application/json", v.to_string()),
                Err(e) => (400, "application/json", serde_json::json!({ "ok": false, "error": format!("{e:?}") }).to_string()),
            }
        }
        ("GET", "/api/trades") => (200, "application/json", query_recent_trades_json(ctx, lim).to_string()),
        ("GET", "/api/paths") => (200, "application/json", query_position_path_json(ctx, tid).to_string()),
        ("GET", "/api/aborts") => (200, "application/json", query_queue_aborts_json(ctx, lim).to_string()),
        ("GET", "/api/clip/360" | "/api/clip/trade") => {
            let md = build_markdown_360_clip(ctx, tid);
            (200, "application/json", serde_json::json!({ "ok": true, "markdown": md }).to_string())
        }
        ("POST", "/api/cmd/drain") => {
            if ctx.verify_csrf_header(headers).is_err() {
                return (403, "application/json", "{\"ok\":false,\"error\":\"CsrfRejected\"}".to_string());
            }
            let en = serde_json::from_str::<serde_json::Value>(body).ok()
                .and_then(|v| v.get("enable").and_then(serde_json::Value::as_bool))
                .unwrap_or(false);
            match handle_drain_mode_toggle(ctx, en) {
                Ok(v) => (200, "application/json", v.to_string()),
                Err(e) => (500, "application/json", serde_json::json!({ "ok": false, "error": format!("{e:?}") }).to_string()),
            }
        }
        ("POST", "/api/cmd/lab") => {
            if ctx.verify_csrf_header(headers).is_err() {
                return (403, "application/json", "{\"ok\":false,\"error\":\"CsrfRejected\"}".to_string());
            }
            let lid = serde_json::from_str::<serde_json::Value>(body).ok()
                .and_then(|v| v.get("lab_id").and_then(serde_json::Value::as_u64))
                .and_then(|n| u8::try_from(n).ok())
                .unwrap_or(0);
            match run_autonomous_lab_gate(ctx, lid) {
                Ok(v) => (200, "application/json", v.to_string()),
                Err(WebApiError::LabAlreadyRunning) => (409, "application/json", "{\"ok\":false,\"error\":\"LabAlreadyRunning\"}".to_string()),
                Err(e) => (400, "application/json", serde_json::json!({ "ok": false, "error": format!("{e:?}") }).to_string()),
            }
        }
        _ => (404, "application/json", "{\"ok\":false,\"error\":\"RouteNotFound\"}".to_string()),
    }
}

pub fn stream_sse_5hz_loop(
    ctx: &std::sync::Arc<VortexWebContext>, stream: &mut std::net::TcpStream, max_frames: Option<usize>,
) -> std::io::Result<()> {
    use std::io::Write;
    let _ = stream.set_nodelay(true);
    let hdr = concat!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n",
        "Cache-Control: no-cache\r\nConnection: keep-alive\r\nX-Frame-Options: DENY\r\n\r\n"
    );
    stream.write_all(hdr.as_bytes())?;
    let mut sent = 0_usize;
    loop {
        let payload = build_live_state_json(ctx).to_string();
        let frame = format!("event: state\ndata: {payload}\n\n");
        stream.write_all(frame.as_bytes())?;
        stream.flush()?;
        sent += 1;
        if max_frames.is_some_and(|m| sent >= m) { return Ok(()); }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

struct ConnSlotGuard<'a>(&'a AtomicUsize);
impl Drop for ConnSlotGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R, max_bytes: usize) -> Option<String> {
    use std::io::Read;
    let mut out = String::new();
    let mut lim = reader.by_ref().take(max_bytes as u64);
    let n = lim.read_line(&mut out).ok()?;
    if n == 0 { None } else { Some(out) }
}

fn handle_tcp_client(ctx: std::sync::Arc<VortexWebContext>, mut stream: std::net::TcpStream) {
    use std::io::{Read, Write};
    let _guard = ConnSlotGuard(&ctx.active_conns);
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);
    let Ok(cloned) = stream.try_clone() else { return };
    let mut reader = BufReader::new(cloned);
    let Some(req_line) = read_bounded_line(&mut reader, 4_096) else { return };
    let mut parts = req_line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else { return };
    let (method_s, target_s) = (method.to_string(), target.to_string());
    let mut headers = Vec::with_capacity(16);
    let mut content_len = 0_usize;
    for _ in 0..64 {
        let Some(ln) = read_bounded_line(&mut reader, 4_096) else { break };
        if ln == "\r\n" || ln == "\n" { break; }
        if let Some((k, v)) = ln.split_once(':') {
            let (ks, vs) = (k.trim().to_string(), v.trim().to_string());
            if ks.eq_ignore_ascii_case("content-length") {
                content_len = vs.parse::<usize>().unwrap_or(0).min(16_384);
            }
            headers.push((ks, vs));
        }
    }
    if method_s == "GET" && target_s.starts_with("/api/stream") {
        let _ = stream.set_read_timeout(None);
        let _ = stream_sse_5hz_loop(&ctx, &mut stream, None);
        return;
    }
    let mut body_buf = vec![0_u8; content_len];
    if content_len > 0 && reader.read_exact(&mut body_buf).is_err() { return; }
    let body_s = String::from_utf8_lossy(&body_buf);
    let (code, ctype, resp_body) = dispatch_http_request(&ctx, &method_s, &target_s, &headers, &body_s);
    let status = match code {
        200 => "OK", 400 => "Bad Request", 403 => "Forbidden", 409 => "Conflict",
        500 => "Internal Server Error", _ => "Not Found",
    };
    let head = format!(
        "HTTP/1.1 {code} {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Vortex-Csrf: {}\r\nConnection: close\r\n\r\n",
        resp_body.len(), ctx.csrf_token
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(resp_body.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Write);
}

pub fn serve_observatory_blocking(
    ctx: std::sync::Arc<VortexWebContext>, shutdown: std::sync::Arc<AtomicBool>,
) -> Result<(), WebApiError> {
    ctx.cfg.validate_loopback_only()?;
    let listener = std::net::TcpListener::bind((ctx.cfg.bind_ip, ctx.cfg.port)).map_err(|_| WebApiError::IoFailure)?;
    let _ = listener.set_nonblocking(true);
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, peer)) if peer.ip().is_loopback() => {
                if ctx.active_conns.fetch_add(1, Ordering::AcqRel) >= MAX_CONCURRENT_WEB_CLIENTS {
                    ctx.active_conns.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
                let c = std::sync::Arc::clone(&ctx);
                std::thread::spawn(move || handle_tcp_client(c, stream));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(_) => return Err(WebApiError::IoFailure),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candle_store::Vkl1CandleRecord;

    #[test]
    fn test_v49_web_loopback_csrf_vkl1_drain_and_360_clip() {
        let base = std::env::temp_dir().join(format!("vortex_web_gate_{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let bad_cfg = VortexWebConfig {
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            ..VortexWebConfig::default()
        };
        assert_eq!(bad_cfg.validate_loopback_only(), Err(WebApiError::NonLoopbackBindForbidden));

        let cfg = VortexWebConfig {
            bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            shm_path: base.join("offline_shm.mmap"),
            candle_dir: base.join("candles"),
            vault_dir: base.join("vault"),
            data_dir: base.join("data"),
            ui_html_path: PathBuf::from("ui/vortex_observatory.html"),
        };
        let ctx = VortexWebContext::new(cfg);
        assert_eq!(ctx.csrf_token.len(), 64);

        let store = PerpetualCandleStore::new(&ctx.cfg.candle_dir);
        let rec = Vkl1CandleRecord {
            slot_idx: 0, tf_code: 1, open_ts_ns: 1_700_000_040_000_000_000,
            open_ticks: 650_000, high_ticks: 651_000, low_ticks: 649_500, close_ticks: 650_800,
            volume_lots: 120, ofi_mean_pm: 420, hurst_pm: 580, ob_bull_pm: 640, ob_bear_pm: 590,
            ..Vkl1CandleRecord::default()
        }.seal_with_checksum();
        assert!(store.append_or_update(&rec).is_ok());

        let (c_html, _, html) = dispatch_http_request(&ctx, "GET", "/", &[], "");
        assert_eq!(c_html, 200);
        assert!(html.contains(&ctx.csrf_token));

        let (c_kl, _, kl_body) = dispatch_http_request(&ctx, "GET", "/api/candles?symbol=BTCUSDT&tf=1m&limit=10", &[], "");
        assert_eq!(c_kl, 200);
        assert!(kl_body.contains("\"count\":1"));

        let (c_rej, _, _) = dispatch_http_request(&ctx, "POST", "/api/cmd/drain", &[], "{\"enable\":true}");
        assert_eq!(c_rej, 403);

        let hdrs = vec![(CSRF_HEADER_NAME.to_string(), ctx.csrf_token.clone())];
        let (c_ok, _, d_on) = dispatch_http_request(&ctx, "POST", "/api/cmd/drain", &hdrs, "{\"enable\":true}");
        assert_eq!(c_ok, 200);
        assert!(d_on.contains("\"drain_mode\":true"));
        let (c_off, _, d_off) = dispatch_http_request(&ctx, "POST", "/api/cmd/drain", &hdrs, "{\"enable\":false}");
        assert_eq!(c_off, 200);
        assert!(d_off.contains("\"drain_mode\":false"));

        let _ = fs::create_dir_all(&ctx.cfg.vault_dir);
        let _ = fs::write(
            ctx.cfg.vault_dir.join("vortex_position_paths.jsonl"),
            "{\"event\":\"POSITION_PATH_1HZ\",\"trade_id\":7,\"symbol\":\"ETHUSDT\",\"side\":\"Bid\",\"samples_1hz\":[[0,-15,650,-1800,420,510,280,110],[1,35,650,-1760,460,540,310,95]]}\n",
        );
        let _ = fs::write(
            ctx.cfg.vault_dir.join("vortex_queue_aborts.jsonl"),
            "{\"event\":\"QUEUE_ABORTED\",\"veto_code\":12,\"veto_name\":\"HawkesStoikovQueueAbort\",\"true_positive\":true}\n",
        );
        let (c_pt, _, pt_body) = dispatch_http_request(&ctx, "GET", "/api/paths?trade_id=7", &[], "");
        assert_eq!(c_pt, 200);
        assert!(pt_body.contains("\"found\":true") && pt_body.contains("\"rel_sec\":1") && pt_body.contains("\"mid_delta_sbps\":35"));

        let (c_ab, _, ab_body) = dispatch_http_request(&ctx, "GET", "/api/aborts?limit=10", &[], "");
        assert_eq!(c_ab, 200);
        assert!(ab_body.contains("HawkesStoikovQueueAbort") && ab_body.contains("\"precision_pct\":100.0"));

        let (c_st, _, st_body) = dispatch_http_request(&ctx, "GET", "/api/state", &[], "");
        assert_eq!(c_st, 200);
        assert!(st_body.contains("SHM_OFFLINE"));

        let (c_clip, _, clip_s) = dispatch_http_request(&ctx, "GET", "/api/clip/360", &[], "");
        assert_eq!(c_clip, 200);
        assert!(!clip_s.contains("\u{1b}["));
        let _ = fs::remove_dir_all(&base);
    }
}
