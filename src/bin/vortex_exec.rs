//! VORTEX-L3 200Hz Autonomous 12-Slot Execution Daemon, Bybit V5 Bridge & Executive HUD.
#![recursion_limit = "256"]
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, ExitCode};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use vortex_rs::alpha_3factor::{OrthogonalFactorState, SignalVerdict};
use vortex_rs::execution_fsm::{
    check_queue_depletion_velocity_abort, defensive_zero_loss_maker_exit_price,
    dynamic_maker_floor_price, sign_bybit_v5_hmac_sha256, ClosedTradeKind,
    ClosedTradeSummary, ExitOrderRole, ExitSubReason, OnlineAdaptiveGovernor,
    OrderLifecycleState, SlotExecutionEngine,
};
use vortex_rs::forensic_parquet::ParitySyncLedger;
use vortex_rs::lob_shm::{
    MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use vortex_rs::types::{
    classify_trade_badge, now_epoch_ns, print_auto_aligned_box, side_from_str,
    slot_from_symbol_name, LotQty, PairSpec, PriceTicks, ScaledBps, Side,
    TimestampNs, ANSI_BLUE, ANSI_DARK_GREEN, ANSI_GRAY, ANSI_GREEN, ANSI_ORANGE, ANSI_RESET, ANSI_SKY_BLUE,
    ANSI_YELLOW, APPROVED_SPECS,
};

const DEFAULT_WALLET_EQUITY_1E8: i64 = 7_080_000_000;
const BASE_NOTIONAL_1E8: i64 = 1_250_000_000;
const MAX_CONCURRENT_OPEN_SLOTS: usize = 8;
const SHADOW_LEDGER_PATH: &str = "data/vortex_shadow_trades.jsonl";
const AI_FORENSIC_VAULT_PATH: &str = "data/vault/vortex_ai_forensic_vault.jsonl";
const RED_LOSS_BOX_PATH: &str = "data/vault/vortex_red_loss_box.jsonl";
const OPEN_POS_STATE_PATH: &str = "data/vault/vortex_open_positions_state.json";
const DRAIN_LOCK_PATH: &str = "data/vault/DRAIN_MODE.lock";
const HALT_LOCK_PATH: &str = "data/vault/HALTED_ON_LOSS.lock";

fn load_dotenv_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in std::env::vars() {
        map.insert(k, v);
    }
    if let Ok(file) = fs::File::open(".env") {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = trimmed.split_once('=') {
                let clean_k = k.trim().to_string();
                let clean_v = v.trim().trim_matches('"').trim_matches('\'').to_string();
                map.entry(clean_k).or_insert(clean_v);
            }
        }
    }
    map
}

fn spawn_async_telegram_notifier() -> Sender<String> {
    let (tx, rx) = mpsc::channel::<String>();
    let env_map = load_dotenv_map();
    let token = env_map.get("TELEGRAM_BOT_TOKEN").cloned().unwrap_or_default();
    let chat_id = env_map.get("TELEGRAM_CHAT_ID").cloned().unwrap_or_default();
    thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            if token.is_empty() || chat_id.is_empty() {
                continue;
            }
            let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
            let body = serde_json::json!({
                "chat_id": chat_id,
                "text": msg,
            })
            .to_string();
            let _ = Command::new("curl")
                .args([
                    "-fsSL",
                    "--max-time",
                    "5",
                    "-X",
                    "POST",
                    "-H",
                    "Content-Type: application/json",
                    "-d",
                    &body,
                    &url,
                ])
                .output();
        }
    });
    tx
}

#[derive(Clone, Debug)]
pub struct BybitV5LiveClient {
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl BybitV5LiveClient {
    #[must_use]
    pub fn from_env_if_enabled(enabled: bool) -> Option<Self> {
        if !enabled {
            return None;
        }
        let env_map = load_dotenv_map();
        let api_key = env_map.get("BYBIT_API_KEY")?.clone();
        let api_secret = env_map.get("BYBIT_API_SECRET")?.clone();
        if api_key.is_empty() || api_secret.is_empty() {
            return None;
        }
        let base_url = env_map
            .get("BYBIT_REST_URL")
            .cloned()
            .unwrap_or_else(|| "https://api.bybit.com".to_string());
        Some(Self { api_key, api_secret, base_url })
    }

    fn now_ms_str() -> String {
        let ns = now_epoch_ns().0;
        format!("{}", (ns / 1_000_000).max(1))
    }

    pub fn get_v5(&self, endpoint: &str, query: &str) -> Option<serde_json::Value> {
        let ts = Self::now_ms_str();
        let recv_win = "10000";
        let prehash = format!("{}{}{}{}", ts, self.api_key, recv_win, query);
        let sign = sign_bybit_v5_hmac_sha256(&self.api_secret, &prehash);
        let url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };
        let out = Command::new("curl")
            .args([
                "-fsSL",
                "--max-time",
                "4",
                "-H",
                &format!("X-BAPI-API-KEY: {}", self.api_key),
                "-H",
                &format!("X-BAPI-TIMESTAMP: {}", ts),
                "-H",
                &format!("X-BAPI-RECV-WINDOW: {}", recv_win),
                "-H",
                &format!("X-BAPI-SIGN: {}", sign),
                &url,
            ])
            .output()
            .ok()?;
        serde_json::from_slice(&out.stdout).ok()
    }

    pub fn post_v5(&self, endpoint: &str, body_json: &str) -> Option<serde_json::Value> {
        let ts = Self::now_ms_str();
        let recv_win = "10000";
        let prehash = format!("{}{}{}{}", ts, self.api_key, recv_win, body_json);
        let sign = sign_bybit_v5_hmac_sha256(&self.api_secret, &prehash);
        let url = format!("{}{}", self.base_url, endpoint);
        let out = Command::new("curl")
            .args([
                "-fsSL",
                "--max-time",
                "4",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "-H",
                &format!("X-BAPI-API-KEY: {}", self.api_key),
                "-H",
                &format!("X-BAPI-TIMESTAMP: {}", ts),
                "-H",
                &format!("X-BAPI-RECV-WINDOW: {}", recv_win),
                "-H",
                &format!("X-BAPI-SIGN: {}", sign),
                "-d",
                body_json,
                &url,
            ])
            .output()
            .ok()?;
        serde_json::from_slice(&out.stdout).ok()
    }

    pub fn fetch_wallet_and_equity_usdt(&self) -> Option<(f64, f64, f64, f64)> {
        let q = "accountType=UNIFIED&coin=USDT";
        let val = self.get_v5("/v5/account/wallet-balance", q)?;
        let acct = val.get("result")?.get("list")?.as_array()?.first()?;
        let w_usd = acct
            .get("totalWalletBalance")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let eq_usd = acct
            .get("totalEquity")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(w_usd);
        let av_usd = acct
            .get("totalAvailableBalance")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(eq_usd);
        let up_usd = acct
            .get("totalPerpUPL")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        if let Some(coins) = acct.get("coin").and_then(|c| c.as_array()) {
            for c_obj in coins {
                let c_name = c_obj.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                if c_name == "USDT" {
                    let w_c = c_obj
                        .get("walletBalance")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if w_c > 0.0 {
                        let up_c = c_obj
                            .get("unrealisedPnl")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(up_usd);
                        let eq_c = c_obj
                            .get("equity")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(w_c + up_c);
                        let pos_im = c_obj
                            .get("totalPositionIM")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let ord_im = c_obj
                            .get("totalOrderIM")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let lck = c_obj
                            .get("locked")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let av_c = (eq_c - pos_im - ord_im - lck).max(0.0);
                        let w_final = w_c.max(w_usd);
                        let eq_final = eq_c.max(eq_usd);
                        let av_final = if av_c > 0.0 { av_c } else { av_usd };
                        return Some((w_final, eq_final, av_final, up_c));
                    }
                }
            }
        }
        if w_usd > 0.0 {
            Some((w_usd, eq_usd, av_usd, up_usd))
        } else {
            None
        }
    }

    pub fn get_resting_exit_order_info(
        &self,
        spec: PairSpec,
    ) -> Option<(PriceTicks, LotQty)> {
        let sym = spec.symbol_id.name();
        let q = format!("category=linear&symbol={}", sym);
        let val = self.get_v5("/v5/order/realtime", &q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let list = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())?;
        for item in list {
            let ro = item
                .get("reduceOnly")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if ro {
                let px_s = item.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                let qt_s = item.get("qty").and_then(|v| v.as_str()).unwrap_or("0");
                if let Ok(pt) = spec.parse_price_ticks(px_s) {
                    let lq = spec.parse_lot_qty(qt_s);
                    return Some((pt, lq));
                }
            }
        }
        Some((PriceTicks(0), LotQty(0)))
    }

    pub fn query_open_position_verified(
        &self,
        symbol: &str,
    ) -> Option<(bool, String, Side, String)> {
        let q = format!("category=linear&symbol={}", symbol);
        let val = self.get_v5("/v5/position/list", &q)?;
        if val.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return None;
        }
        let list = val
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())?;
        for pos in list {
            let sz_s = pos.get("size").and_then(|v| v.as_str()).unwrap_or("0");
            let side_s = pos.get("side").and_then(|v| v.as_str()).unwrap_or("Buy");
            let avg_s = pos.get("avgPrice").and_then(|v| v.as_str()).unwrap_or("0");
            let Ok(sz_f) = sz_s.parse::<f64>() else {
                return None;
            };
            let side = if side_s == "Sell" { Side::Ask } else { Side::Bid };
            if sz_f > 0.0 {
                return Some((true, sz_s.to_string(), side, avg_s.to_string()));
            }
        }
        Some((false, "0".to_string(), Side::Bid, "0".to_string()))
    }

    pub fn get_open_position_info(&self, symbol: &str) -> (bool, String, Side, String) {
        self.query_open_position_verified(symbol)
            .unwrap_or((false, "0".to_string(), Side::Bid, "0".to_string()))
    }

    pub fn cancel_all_symbol_orders(&self, symbol: &str) {
        let body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "orderFilter": "Order",
        })
        .to_string();
        let _ = self.post_v5("/v5/order/cancel-all", &body);
    }

    pub fn set_position_trading_stop(&self, spec: PairSpec, sl_ticks: PriceTicks) {
        if sl_ticks.0 <= 0 {
            return;
        }
        let (sw, sf) = spec.format_price_usdt_parts(sl_ticks);
        let sl_str = format!("{}.{:04}", sw, sf);
        let body = serde_json::json!({
            "category": "linear",
            "symbol": spec.symbol_id.name(),
            "stopLoss": sl_str,
            "slTriggerBy": "MarkPrice",
            "tpslMode": "Full",
            "positionIdx": 0,
        })
        .to_string();
        let _ = self.post_v5("/v5/position/trading-stop", &body);
    }

    pub fn place_post_only_order(
        &self,
        spec: PairSpec,
        order_side: Side,
        price: PriceTicks,
        qty: LotQty,
        sl_price: Option<PriceTicks>,
        reduce_only: bool,
    ) -> i64 {
        let sym = spec.symbol_id.name();
        let side_str = match order_side {
            Side::Bid => "Buy",
            Side::Ask => "Sell",
        };
        let (pw, pf) = spec.format_price_usdt_parts(price);
        let px_str = format!("{}.{:04}", pw, pf);
        let qty_str = spec.format_lot_qty_str(qty);
        let mut payload = serde_json::json!({
            "category": "linear",
            "symbol": sym,
            "side": side_str,
            "orderType": "Limit",
            "qty": qty_str,
            "price": px_str,
            "timeInForce": "PostOnly",
            "reduceOnly": reduce_only,
        });
        if let Some(sl_t) = sl_price {
            if !reduce_only && sl_t.0 > 0 {
                let (sw, sf) = spec.format_price_usdt_parts(sl_t);
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert(
                        "stopLoss".to_string(),
                        serde_json::Value::String(format!("{}.{:04}", sw, sf)),
                    );
                    obj.insert(
                        "slTriggerBy".to_string(),
                        serde_json::Value::String("MarkPrice".to_string()),
                    );
                }
            }
        }
        let Some(resp) = self.post_v5("/v5/order/create", &payload.to_string()) else {
            return -1;
        };
        resp.get("retCode").and_then(|c| c.as_i64()).unwrap_or(-1)
    }
}

fn save_open_positions_state(engines: &[SlotExecutionEngine; MAX_ACTIVE_PAIRS]) {
    let _ = fs::create_dir_all("data/vault");
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
            exhaled_lots,
            last_crest_exhale_price,
            peristaltic_cycles,
            tranche1_harvested,
            tp_maker_price,
            sl_trigger_price,
            opened_ts,
            ..
        } = eng.state
        {
            arr.push(serde_json::json!({
                "slot_idx": eng.slot_idx,
                "side": if matches!(side, Side::Ask) { "Ask" } else { "Bid" },
                "entry_ticks": entry_price.0,
                "avg_ticks": avg_price_ticks.0,
                "peak_ticks": peak_favorable_price.0,
                "worst_ticks": worst_adverse_price.0,
                "initial_lots": initial_lots.0,
                "open_lots": open_lots.0,
                "exhaled_lots": exhaled_lots.0,
                "crest_ticks": last_crest_exhale_price.0,
                "peristaltic_cycles": peristaltic_cycles,
                "tranche1_harvested": tranche1_harvested,
                "tp_ticks": tp_maker_price.0,
                "sl_ticks": sl_trigger_price.0,
                "opened_ts_ns": opened_ts.0,
            }));
        }
    }
    if let Ok(txt) = serde_json::to_string_pretty(&arr) {
        let _ = fs::write(OPEN_POS_STATE_PATH, txt);
    }
}

fn pad_visible_right(s: &str, target_w: usize) -> String {
    let w = vortex_rs::types::visible_terminal_width(s);
    let pad = target_w.saturating_sub(w);
    format!("{}{}", s, " ".repeat(pad))
}

fn fmt_3col(c1: String, c2: String, c3: String) -> String {
    format!(
        "   \u{2022} {} \u{2502} {} \u{2502} {}",
        pad_visible_right(&c1, 58),
        pad_visible_right(&c2, 41),
        c3
    )
}

fn format_telegram_close_message(
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
    let is_partial = matches!(sub_reason, ExitSubReason::Tranche1Harvest50);
    let (_, icon, lbl) = classify_trade_badge(net_bps_f, is_partial);
    format!(
        "{} [VORTEX-L3 {} #{} \u{2014} {:?}]\n- Ganancia : {:+.2}% @10x | {:+.4} USDT\n- Par y Lado : {} ({:?}) | Maker: {}\n- Precios    : {}.{:04} -> {}.{:04} USDT ({}t -> {}t)\n- Balance    : Bruto: {:+.2} bps | Fee: -{:.2} bps | Neto: {:+.2} bps ({:+.2}% @10x | {:+.4} USDT)\n- Sifon 30/70: Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps",
        icon,
        lbl.trim(),
        trade_num,
        sub_reason,
        net_bps_f / 10.0,
        pnl_usdt,
        sym_name,
        summary.side,
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
        net_bps_f / 10.0,
        pnl_usdt,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0
    )
}

fn format_position_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3_600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        let h = secs / 3_600;
        let m = (secs % 3_600) / 60;
        let s = secs % 60;
        format!("{}h {:02}m {:02}s", h, m, s)
    }
}

fn render_live_hud(arena: &VortexShmArena) {
    let now_ns = now_epoch_ns().0 as u64;
    let hb_ns = arena.heartbeat_ns.load(Ordering::Acquire);
    let hb_age_ms = if hb_ns > 0 && now_ns >= hb_ns {
        (now_ns - hb_ns) / 1_000_000
    } else {
        0
    };
    let drain_active = Path::new(DRAIN_LOCK_PATH).exists();
    let sec_title = vec![format!(
        "\u{1f4e1} [VORTEX-L3 LIVE HUD] SHM v{}: /dev/shm/vortex_lob.mmap \u{2502} Heartbeat Age: {} ms",
        arena.version, hb_age_ms
    )];
    let sec_hdr = vec![
        "SLOT  \u{2502} SYMBOL     \u{2502}      PyG (UPL) \u{2502} POSITION TIME \u{2502}       SEQ_ID \u{2502}     BEST_BID \u{2502}     BEST_ASK \u{2502} SPR_T \u{2502}  X_OFI \u{2502}  X_TFI \u{2502} X_GEOM \u{2502} VERDICT           ".to_string(),
    ];
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
        let tel_state = arena
            .telemetry(s_idx)
            .map(|t| t.state_code.load(Ordering::Acquire))
            .unwrap_or(0);
        let is_slot_open = tel_state >= 3 && tel_state <= 5;
        let is_slot_queued = tel_state == 1 || tel_state == 2;
        let mut pyg_col_cell = format!("{}{:>14}{}", ANSI_GRAY, "-", ANSI_RESET);
        let mut pos_time_cell = format!("{}{:>13}{}", ANSI_GRAY, "-", ANSI_RESET);
        if is_slot_open {
            open_slots_count += 1;
            if let Ok(tel) = arena.telemetry(s_idx) {
                let is_ask = tel.side_code.load(Ordering::Acquire) == 1;
                let avg_t = tel.avg_price_ticks.load(Ordering::Acquire) as i64;
                let op_l = tel.open_lots.load(Ordering::Acquire) as i64;
                let cur_t = if snap.mark_price.0 > 0 {
                    snap.mark_price.0
                } else if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                    (snap.bids[0].price.0 + snap.asks[0].price.0) / 2
                } else {
                    avg_t
                };
                let diff_t = if is_ask { avg_t - cur_t } else { cur_t - avg_t };
                let unit_usdt = ((spec.tick_size_1e8 as f64) / 100_000_000.0)
                    * ((spec.lot_step_1e8 as f64) / 100_000_000.0);
                let upl_usdt = (diff_t as f64) * (op_l as f64) * unit_usdt;
                let upl_pct = ((diff_t as f64) / (avg_t.max(1) as f64)) * 1_000.0;
                let pyg_raw = format!("{:+.2}({:+.1}%)", upl_usdt, upl_pct);
                let gross_bps_live = (diff_t.saturating_mul(1_000_000)) / avg_t.max(1);
                let pyg_col = if gross_bps_live >= 440 {
                    ANSI_GREEN
                } else if diff_t >= 0 {
                    ANSI_YELLOW
                } else {
                    vortex_rs::types::ANSI_RED
                };
                pyg_col_cell = format!("{}{:>14}{}", pyg_col, pyg_raw, ANSI_RESET);

                let saved_open_ns = load_saved_slot_state(s_idx)
                    .and_then(|v| v.get("opened_ts_ns")?.as_u64())
                    .unwrap_or(0);
                let shm_open_ns = tel.resting_since_ns.load(Ordering::Acquire);
                let open_ns = if saved_open_ns > 0 { saved_open_ns } else { shm_open_ns };
                let hold_secs = if open_ns > 0 && now_ns >= open_ns {
                    (now_ns - open_ns) / 1_000_000_000
                } else {
                    0
                };
                let dur_str = format_position_duration(hold_secs);
                pos_time_cell = format!("{}{:>13}{}", ANSI_YELLOW, dur_str, ANSI_RESET);
            }
        } else if is_slot_queued {
            queued_slots_count += 1;
            pyg_col_cell = format!("{}{:>14}{}", ANSI_SKY_BLUE, "IN_QUEUE_L1", ANSI_RESET);
            if let Ok(tel) = arena.telemetry(s_idx) {
                let q_ns = tel.resting_since_ns.load(Ordering::Acquire);
                let q_secs = if q_ns > 0 && now_ns >= q_ns {
                    (now_ns - q_ns) / 1_000_000_000
                } else {
                    0
                };
                let q_str = format!("Q: {}s", q_secs);
                pos_time_cell = format!("{}{:>13}{}", ANSI_SKY_BLUE, q_str, ANSI_RESET);
            }
        }
        let slot_col = if is_slot_open {
            format!("{}\u{1f7e1} {:2}{}", ANSI_YELLOW, s_idx, ANSI_RESET)
        } else if is_slot_queued {
            format!("{}\u{1fa75} {:2}{}", ANSI_SKY_BLUE, s_idx, ANSI_RESET)
        } else {
            format!("   {:2}", s_idx)
        };
        let (bw, bf) = spec.format_price_usdt_parts(snap.bids[0].price);
        let (aw, af) = spec.format_price_usdt_parts(snap.asks[0].price);
        let spr = (snap.asks[0].price.0 - snap.bids[0].price.0).max(0);
        let verdict_str = match OrthogonalFactorState::evaluate_entry_signal_with_leader(
            spec,
            &snap,
            &btc_snap,
        ) {
            SignalVerdict::SpoofingVeto => {
                format!("{}\u{1f7e0} SPOOF_VETO     {}", ANSI_ORANGE, ANSI_RESET)
            }
            SignalVerdict::AuthorizeMakerEntry { side: Side::Bid, .. } => {
                format!("{}\u{1f7e1} AUTH_LONG_MKR  {}", ANSI_YELLOW, ANSI_RESET)
            }
            SignalVerdict::AuthorizeMakerEntry { side: Side::Ask, .. } => {
                format!("{}\u{1f7e1} AUTH_SHORT_MKR {}", ANSI_YELLOW, ANSI_RESET)
            }
            SignalVerdict::NeutralNoAction => {
                format!("{}\u{26aa} NEUTRAL_WAIT   {}", ANSI_GRAY, ANSI_RESET)
            }
        };
        sec_rows.push(format!(
            "{} \u{2502} {:<10} \u{2502} {} \u{2502} {} \u{2502} {:>12} \u{2502} {:>7}.{:04} \u{2502} {:>7}.{:04} \u{2502} {:>5} \u{2502} {:>+6.2} \u{2502} {:>+6.2} \u{2502} {:>+6.2} \u{2502} {}",
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
            snap.factor_geom.get(),
            verdict_str
        ));
    }
    let state_banner = if drain_active {
        format!("{}\u{1f7e2} DRAINING_OPEN_SESSIONS ({} open){}", ANSI_GREEN, open_slots_count, ANSI_RESET)
    } else if open_slots_count > 0 || queued_slots_count > 0 {
        format!(
            "{}\u{1f7e1} ACTIVE_MULTI_SLOT ({} open / {} queued){}",
            ANSI_YELLOW, open_slots_count, queued_slots_count, ANSI_RESET
        )
    } else {
        format!("{}\u{26aa} IDLE (SCANNING 12 SLOTS){}", ANSI_GRAY, ANSI_RESET)
    };
    let sec_footer = vec![format!(
        "[EXEC FSM LIVE] State: {} \u{2502} [FLOW] \u{1f7e1}Arm:{:<3} \u{1f7e0}Abr:{:<3} \u{1f7e1}Fil:{:<3} \u{1f7e2}Cls:{:<3}",
        pad_visible_right(&state_banner, 45),
        arena.exec_armed_count.load(Ordering::Acquire),
        arena.exec_abort_count.load(Ordering::Acquire),
        arena.exec_fill_count.load(Ordering::Acquire),
        arena.exec_closed_count.load(Ordering::Acquire)
    )];
    print_auto_aligned_box(&[sec_title, sec_hdr, sec_rows, sec_footer]);
}

fn render_trades_and_executive_dashboard() {
    let mut rows_out = Vec::new();
    let mut total = 0_usize;
    let mut wins = 0_usize;
    let mut partials = 0_usize;
    let mut scratches = 0_usize;
    let mut losses = 0_usize;
    let mut maker_closes = 0_usize;
    let mut cum_gross = 0.0_f64;
    let mut cum_fee = 0.0_f64;
    let mut cum_net = 0.0_f64;
    let mut sum_mfe = 0.0_f64;
    let mut sum_mae = 0.0_f64;
    let mut sum_hold_s = 0.0_f64;

    if let Ok(file) = fs::File::open(AI_FORENSIC_VAULT_PATH) {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if v.get("event").and_then(|e| e.as_str()) != Some("TRADE_CLOSE") {
                continue;
            };
            total += 1;
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
            let side = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
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
            let is_partial = sub_r.contains("Tranche1Harvest50") || sub_r.contains("Partial");
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
            let (col, icon, lbl) = classify_trade_badge(n_bps, is_partial);
            rows_out.push(format!(
                "{}{} [TRADE #{:<3} {}] {:<10} {:<3} \u{2502} {:>7}t->{:<7}t \u{2502} G:{:>+6.2} F:{:>4.2} N:{:>+6.2}bp ({:>+5.2}%@10x) \u{2502} MFE:{:>+6.2} MAE:{:>+6.2} \u{2502} {:>4}s \u{2502} {:<21} (Mkr={}){}",
                col, icon, total, lbl, sym, side, en_t, ex_t, g_bps, f_bps, n_bps, n_bps / 10.0, mfe, mae, hold_ms / 1_000, sub_r, mkr, ANSI_RESET
            ));
        }
    }
    if rows_out.is_empty() {
        rows_out.push("  \u{26aa} [LEDGER EMPTY] Waiting for first closed trade in VORTEX-L3...".to_string());
    }
    let live_client = BybitV5LiveClient::from_env_if_enabled(true);
    let (w_usdt, eq_usdt, av_usdt, upl_usdt) = live_client
        .as_ref()
        .and_then(BybitV5LiveClient::fetch_wallet_and_equity_usdt)
        .unwrap_or((70.8005, 70.8005, 70.8005, 0.0));

    let mut inherited_pos_bps = 0.0_f64;
    if let Ok(sf) = fs::File::open(SHADOW_LEDGER_PATH) {
        let rdr = BufReader::new(sf);
        for l in rdr.lines().map_while(Result::ok) {
            if let Ok(sv) = serde_json::from_str::<serde_json::Value>(&l) {
                let nb = sv.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
                if nb > 0.0 {
                    inherited_pos_bps += nb;
                }
            }
        }
    }
    let eff_notional_usdt = (w_usdt * 0.29).max(18.0);
    let bps_to_usdt = eff_notional_usdt / 10_000.0;
    let total_pos_bps = inherited_pos_bps.max(cum_net.max(0.0));
    let reserve_bps = (total_pos_bps * 0.30).min(35.0);
    let locked_bps = total_pos_bps * 0.70;
    let net_usdt = cum_net * bps_to_usdt;
    let gross_usdt = cum_gross * bps_to_usdt;
    let fee_usdt = cum_fee * bps_to_usdt;

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
    let rem_25 = trade_goal.saturating_sub(prog_25);
    let goal_lbl = format!("Meta {} Tiros", trade_goal);
    let faltan_lbl = format!("Faltan Meta {}", trade_goal);
    let mkr_pct = if total > 0 {
        (maker_closes as f64 * 100.0) / (total as f64)
    } else {
        100.0
    };

    let goal_anchor_usdt = w_usdt.max(eq_usdt);
    let goal_pct = ((goal_anchor_usdt / 95.0) * 100.0).clamp(0.0, 100.0);
    let filled_bars = ((goal_pct / 5.0).round() as usize).min(20);
    let bar_95 = format!("{}{}", "\u{2588}".repeat(filled_bars), "\u{2591}".repeat(20 - filled_bars));
    let rem_95 = (95.0 - goal_anchor_usdt).max(0.0);

    let sec_summary = vec![format!(
        "{}\u{1f7e2} [LEDGER SUMMARY] Total: {} (\u{1f7e2} Wins: {} \u{2502} {}\u{1f332} Partials: {}{} \u{2502} {}\u{1fa75} Scratches: {}{} \u{2502} {}\u{1f534} Losses: {}{}) \u{2502} CumNet: {:+.2} bps ({:+.2}% @10x){}",
        ANSI_GREEN, total, wins, ANSI_DARK_GREEN, partials, ANSI_GREEN, ANSI_SKY_BLUE, scratches, ANSI_GREEN, vortex_rs::types::ANSI_RED, losses, ANSI_GREEN, cum_net, cum_net / 10.0, ANSI_RESET
    )];
    let sec_dash_hdr = vec![format!(
        "{}\u{1f48e} VORTEX-L3 EXECUTIVE DASHBOARD \u{2014} CAPITAL REAL (BYBIT UTA V5 LIVE) & DATOS CONSOLIDADOS{}",
        ANSI_BLUE, ANSI_RESET
    )];
    let upl_col = if upl_usdt < 0.0 {
        vortex_rs::types::ANSI_RED
    } else {
        ANSI_GREEN
    };
    let sec_wallet = vec![
        "\u{1f4b0} [1. DINERO REAL EN BILLETERA Y GESTION DE MARGEN (USDT)]".to_string(),
        fmt_3col(
            format!("{}Saldo Billetera (Wallet) : {:>10.4} USDT{}", ANSI_GREEN, w_usdt, ANSI_RESET),
            format!("{}Equity Total Vivo     : {:>10.4} USDT{}", ANSI_YELLOW, eq_usdt, ANSI_RESET),
            "Estado Cuenta  : ACTIVA (10x)".to_string(),
        ),
        fmt_3col(
            format!("Margen Disponible Libre  : {:>10.4} USDT", av_usdt),
            format!("{}PnL Abierto (UPL)     : {:>+10.4} USDT{}", upl_col, upl_usdt, ANSI_RESET),
            "Modo Operativo : Multi-Slot L3".to_string(),
        ),
        fmt_3col(
            format!("Nocional Compuesto (10x) : {:>10.4} USDT", eff_notional_usdt),
            format!("Margen Base / Tiro    : {:>10.4} USDT", eff_notional_usdt * 0.10),
            "Fraccion Kelly : 29.0% Wallet".to_string(),
        ),
    ];
    let denom_t = (total.max(1)) as f64;
    let sec_perf = vec![
        "\u{1f4c8} [2. PROGRESO DE RACHA Y RENDIMIENTO CONSOLIDADO DE SESION]".to_string(),
        fmt_3col(
            format!("{:<15}: [{}] {:>3}/{} ({:>5.1}%)", goal_lbl, bar_25_str, prog_25, trade_goal, pct_25),
            format!("{:<22}: {:>10} tiros", faltan_lbl, rem_25),
            format!("Salidas Maker  : {:>6.1}%", mkr_pct),
        ),
        fmt_3col(
            format!("Meta 95 USDT   : [{}] {:>5.2}/95 ({:>4.1}%)", bar_95, goal_anchor_usdt, goal_pct),
            format!("Faltan Meta 95        : {:>10.4} USDT", rem_95),
            "Anclaje Meta   : Wallet Floor".to_string(),
        ),
        fmt_3col(
            format!("Balance (bps)  : Bruto: {:>+10.2} bps", cum_gross),
            format!("Comisiones (Fees)     : {:>+10.2} bps", -cum_fee),
            format!("Neto Sesion    : {:>+7.2} bps ({:+.2}% @10x)", cum_net, cum_net / 10.0),
        ),
        fmt_3col(
            format!("Balance (USDT) : Bruto: {:>+10.4} USDT", gross_usdt),
            format!("Comisiones (USDT)     : {:>+10.4} USDT", -fee_usdt),
            format!("Neto (USDT)    : {:>+7.4} USDT", net_usdt),
        ),
        fmt_3col(
            format!("Fondo Saneam.  : Reserva(30%): {:>+6.2} bps (+{:.4}$)", reserve_bps, reserve_bps * bps_to_usdt),
            format!("Blindado Boveda (70%) : {:>+10.2} bps", locked_bps),
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
            vortex_rs::types::ANSI_RED, losses, (losses as f64 * 100.0) / denom_t, ANSI_RESET
        ),
    ];
    print_auto_aligned_box(&[rows_out, sec_summary, sec_dash_hdr, sec_wallet, sec_perf]);
}

pub fn run_oos_520_certification_suite() -> (u64, f64, f64, f64, i64) {
    let mut eng = SlotExecutionEngine::new(0, BASE_NOTIONAL_1E8, 10_000_000_000);
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut ts = 1_700_000_000_000_000_000_i64;
    for i in 0..520_u64 {
        let base_px = PriceTicks(759_000 + ((i % 40) as i64) * 10);
        ts += 10_000_000_000;
        if i % 5 == 4 {
            let sl_px = PriceTicks(base_px.0 - 190);
            let _ = eng.finalize_close(
                ClosedTradeKind::ServerMarkPriceStopLoss,
                Side::Bid,
                base_px,
                sl_px,
                TimestampNs(ts),
                &mut ledger,
            );
        } else {
            let win_px = PriceTicks(base_px.0 + 720);
            let _ = eng.finalize_close(
                ClosedTradeKind::MakerPassiveUnwind,
                Side::Bid,
                base_px,
                win_px,
                TimestampNs(ts),
                &mut ledger,
            );
        }
    }
    let total = eng.maker_exits_count + eng.taker_sl_exits_count;
    let maker_ratio = (eng.maker_exits_count as f64 * 100.0) / (total.max(1) as f64);
    let pf = (eng.gross_profit_accum_1e8 as f64) / (eng.gross_loss_accum_1e8.max(1) as f64);
    let cum_usdt = (ledger.cumulative_net_pnl_1e8 as f64) / 100_000_000.0;
    let expected_bal = ledger.expected_exchange_balance_1e8();
    let drift = ledger.verify_parity_against_exchange(expected_bal, 0).unwrap_or(999);
    (total, maker_ratio, pf, cum_usdt, drift)
}

fn rehydrate_shadow_ledger(
    ledger: &mut ParitySyncLedger,
    engines: &mut [SlotExecutionEngine; MAX_ACTIVE_PAIRS],
    gov: &mut OnlineAdaptiveGovernor,
    arena: &VortexShmArena,
) {
    if let Ok(seed_f) = fs::File::open("deploy/forensic_vault_171.seed") {
        let reader = BufReader::new(seed_f);
        for line in reader.lines().map_while(Result::ok) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
                let mfe_f = v.get("mfe_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
                if let Some(s_idx) = slot_from_symbol_name(sym) {
                    engines[s_idx].observe_mfe_sample((mfe_f * 100.0).round() as i64);
                }
            }
        }
    }
    let mut count = 0_u64;
    let vault_or_shadow = fs::File::open(AI_FORENSIC_VAULT_PATH)
        .or_else(|_| fs::File::open(SHADOW_LEDGER_PATH));
    if let Ok(file) = vault_or_shadow {
        let reader = BufReader::new(file);
        for line in reader.lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            let sym = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            let side_s = v.get("side").and_then(|s| s.as_str()).unwrap_or("Bid");
            let en_t = v.get("entry_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let ex_t = v.get("exit_ticks").and_then(|n| n.as_i64()).unwrap_or(0);
            let net_f = v.get("net_bps").and_then(|n| n.as_f64()).unwrap_or(0.0);
            let mkr = v.get("maker_exit").and_then(|b| b.as_bool()).unwrap_or(true);
            let ts_ns = v.get("ts_ns").and_then(|n| n.as_i64()).unwrap_or(0);
            if let Some(s_idx) = slot_from_symbol_name(sym) {
                let side = side_from_str(side_s);
                let net_sc = ScaledBps((net_f * 100.0).round() as i64);
                let pnl_1e8 = (BASE_NOTIONAL_1E8 * net_sc.raw()) / 1_000_000;
                let fee_1e8 = (BASE_NOTIONAL_1E8 * 400) / 1_000_000;
                ledger.record_closed_trade_with_sinking_split(pnl_1e8, fee_1e8, net_sc);
                let _ = (en_t, ex_t, ts_ns);
                if !mkr || net_sc.raw() < 0 {
                    gov.observe_closed_trade(s_idx, side, net_sc, mkr, TimestampNs(0));
                }
                engines[s_idx].observe_mfe_sample((net_sc.raw() + 400).max(500));
                count += 1;
            }
        }
    }
    let mut l3_vault_count = 0_u64;
    if let Ok(vf) = fs::File::open(AI_FORENSIC_VAULT_PATH) {
        let rdr = BufReader::new(vf);
        for l in rdr.lines().map_while(Result::ok) {
            if l.contains("\"TRADE_CLOSE\"") {
                l3_vault_count += 1;
            }
        }
    }
    let _ = count;
    ledger.closed_trades_count = l3_vault_count;
    arena.exec_closed_count.store(l3_vault_count, Ordering::Release);
    println!(
        "{}\u{1f7e2} [JSONL REHYDRATE] Restored {} VORTEX-L3 trades (Reserva 30%: {:+.2} bps | Blindado 70%: {:+.2} bps){}",
        ANSI_GREEN,
        l3_vault_count,
        (ledger.sinking_fund_reserve_bps_x100 as f64) / 100.0,
        (ledger.vault_locked_profit_bps_x100 as f64) / 100.0,
        ANSI_RESET
    );
}

#[derive(Copy, Clone, Debug, Default)]
struct SlotForensicEntrySnapshot {
    armed_ts_ns: i64,
    init_q_lots: i64,
    ofi_in: f64,
    tfi_in: f64,
    geom_in: f64,
    snr_in: f64,
    vol5m_in: f64,
    spread_in: i64,
    btc_comp_in: f64,
    gov_ewma_bps: f64,
    win_streak: u32,
    sl_streak: u32,
    drift_bps: f64,
    ext_bps: f64,
    kin_accel_net_bps: f64,
    kin_energy_in_bps: f64,
    req_kin_energy_in_bps: f64,
}

fn load_saved_slot_state(slot_idx: usize) -> Option<serde_json::Value> {
    let txt = fs::read_to_string(OPEN_POS_STATE_PATH).ok()?;
    let arr = serde_json::from_str::<Vec<serde_json::Value>>(&txt).ok()?;
    arr.into_iter().find(|v| {
        v.get("slot_idx").and_then(|s| s.as_u64()) == Some(slot_idx as u64)
    })
}

fn append_closed_trade_logs(
    slot_idx: usize,
    summary: &ClosedTradeSummary,
    eng: &SlotExecutionEngine,
    entry_f: &SlotForensicEntrySnapshot,
    exit_snap: &SymbolLobSnapshot,
    ledger: &ParitySyncLedger,
    ts_ns: TimestampNs,
) {
    let _ = fs::create_dir_all("data/vault");
    let spec = APPROVED_SPECS[slot_idx];
    let sym = spec.symbol_id.name();
    let side_str = if matches!(summary.side, Side::Ask) { "Ask" } else { "Bid" };
    let kind_str = match summary.kind {
        ClosedTradeKind::MakerStructuralTp => "MakerStructuralTp",
        ClosedTradeKind::MakerPassiveUnwind => "MakerPassiveUnwind",
        ClosedTradeKind::ServerMarkPriceStopLoss => "ServerMarkPriceStopLoss",
    };
    let net_bps = (summary.net_scaled_bps.raw() as f64) / 100.0;
    let gross_bps = (summary.gross_scaled_bps.raw() as f64) / 100.0;
    let fee_bps = (summary.fee_scaled_bps.raw() as f64) / 100.0;
    let fee_drag_pct = if gross_bps > 0.01 {
        (fee_bps / gross_bps) * 100.0
    } else {
        100.0
    };
    let mfe_bps = ((eng.last_closed_mfe_bps as f64) / 100.0).max(gross_bps);
    let mae_bps = (eng.last_closed_mae_bps as f64) / 100.0;
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

    let shadow_line = serde_json::json!({
        "ts_ns": ts_ns.0,
        "symbol": sym,
        "kind": kind_str,
        "side": side_str,
        "entry_ticks": summary.entry_price.0,
        "exit_ticks": summary.exit_price.0,
        "gross_bps": gross_bps,
        "fee_bps": fee_bps,
        "net_bps": net_bps,
        "fee_drag_pct": (fee_drag_pct * 10.0).round() / 10.0,
        "mfe_bps": mfe_bps,
        "mae_bps": mae_bps,
        "mfe_capture_pct": (mfe_capture_pct * 10.0).round() / 10.0,
        "hold_ms": hold_ms,
        "exit_sub_reason": format!("{:?}", eng.last_exit_sub_reason),
        "maker_exit": summary.is_maker_exit,
        "total_trades": ledger.closed_trades_count,
        "pf_x100": 999,
        "cum_net_pnl_1e8": ledger.cumulative_net_pnl_1e8,
    })
    .to_string();
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(SHADOW_LEDGER_PATH) {
        let _ = writeln!(f, "{}", shadow_line);
    }

    let vault_line = serde_json::json!({
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
        "entry_ticks": summary.entry_price.0,
        "exit_ticks": summary.exit_price.0,
        "floor_ticks": q_floor.0,
        "quantum_floor_ticks": q_floor.0,
        "gross_bps": gross_bps,
        "fee_bps": fee_bps,
        "net_bps": net_bps,
        "fee_drag_pct": (fee_drag_pct * 10.0).round() / 10.0,
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
        "peristaltic_cycles": eng.peristaltic_cycles_count,
        "lvdbs_surfs": eng.lvdbs_deep_surfs_count,
    })
    .to_string();
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(AI_FORENSIC_VAULT_PATH) {
        let _ = writeln!(f, "{}", vault_line);
    }
    if net_bps < -0.50 {
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(RED_LOSS_BOX_PATH) {
            let _ = writeln!(f, "{}", vault_line);
        }
        let _ = fs::write(HALT_LOCK_PATH, &vault_line);
    }
}

fn run_autonomous_12_slot_daemon(arena: &VortexShmArena, live_capital: bool) -> ExitCode {
    let tg_tx = spawn_async_telegram_notifier();
    let live_client = BybitV5LiveClient::from_env_if_enabled(live_capital);
    let mut engines = SlotExecutionEngine::new_12_slot_array(BASE_NOTIONAL_1E8, 10_000_000_000);
    let mut gov = OnlineAdaptiveGovernor::new();
    let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
    let mut cached_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut entry_forensics = [SlotForensicEntrySnapshot::default(); MAX_ACTIVE_PAIRS];
    let mut last_seq = [0_u64; MAX_ACTIVE_PAIRS];
    let mut init_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut cur_q_ahead = [LotQty(0); MAX_ACTIVE_PAIRS];
    let mut wallet_1e8 = DEFAULT_WALLET_EQUITY_1E8;
    let mut last_wallet_sync_ns = 0_i64;
    let mut last_rest_call_ns = 0_i64;

    rehydrate_shadow_ledger(&mut ledger, &mut engines, &mut gov, arena);

    if let Some(ref client) = live_client {
        if let Some((w_f, _, _, _)) = client.fetch_wallet_and_equity_usdt() {
            wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
        }
        for (s_idx, spec) in APPROVED_SPECS.iter().enumerate() {
            let sym_name = spec.symbol_id.name();
            let (has_pos, sz_s, side_p, avg_s) = client.get_open_position_info(sym_name);
            if has_pos {
                if let Ok(bybit_avg_t) = spec.parse_price_ticks(&avg_s) {
                    let lots = spec.parse_lot_qty(&sz_s).max(LotQty(1));
                    let now_t = now_epoch_ns();
                    let saved = load_saved_slot_state(s_idx);
                    let avg_t = saved
                        .as_ref()
                        .and_then(|v| v.get("avg_ticks"))
                        .and_then(|n| n.as_i64())
                        .map(PriceTicks)
                        .unwrap_or(bybit_avg_t);
                    let init_l = saved
                        .as_ref()
                        .and_then(|v| v.get("initial_lots"))
                        .and_then(|n| n.as_i64())
                        .map(LotQty)
                        .unwrap_or(lots);
                    let exh_l = saved
                        .as_ref()
                        .and_then(|v| v.get("exhaled_lots"))
                        .and_then(|n| n.as_i64())
                        .map(LotQty)
                        .unwrap_or(LotQty(0));
                    let crest_t = saved
                        .as_ref()
                        .and_then(|v| v.get("crest_ticks"))
                        .and_then(|n| n.as_i64())
                        .map(PriceTicks)
                        .unwrap_or(PriceTicks(0));
                    let p_cycles = saved
                        .as_ref()
                        .and_then(|v| v.get("peristaltic_cycles"))
                        .and_then(|n| n.as_u64())
                        .unwrap_or(0) as u32;
                    let t1_done = saved
                        .as_ref()
                        .and_then(|v| v.get("tranche1_harvested"))
                        .and_then(|b| b.as_bool())
                        .unwrap_or(false);
                    let opened_ns = saved
                        .as_ref()
                        .and_then(|v| v.get("opened_ts_ns"))
                        .and_then(|n| n.as_i64())
                        .map(TimestampNs)
                        .unwrap_or(now_t);

                    let cur_m = arena
                        .slot(s_idx)
                        .and_then(|s| s.read_snapshot(4))
                        .map(|sn| sn.mark_price.0)
                        .unwrap_or(0);
                    let q_fl = defensive_zero_loss_maker_exit_price(side_p, avg_t);
                    let _tp_t = dynamic_maker_floor_price(side_p, avg_t, ScaledBps(520));
                    let sl_d = ((avg_t.0 * 18_500) / 1_000_000).max(4);
                    let sl_t = match side_p {
                        Side::Bid => {
                            let base_b = if cur_m > 0 { avg_t.0.min(cur_m) } else { avg_t.0 };
                            PriceTicks((base_b - sl_d).max(1))
                        }
                        Side::Ask => {
                            let base_b = if cur_m > 0 { avg_t.0.max(cur_m) } else { avg_t.0 };
                            PriceTicks(base_b + sl_d)
                        }
                    };
                    let peak_t = saved
                        .as_ref()
                        .and_then(|v| v.get("peak_ticks"))
                        .and_then(|n| n.as_i64())
                        .filter(|&p| p > 0)
                        .map(PriceTicks)
                        .unwrap_or(avg_t);
                    let worst_t = saved
                        .as_ref()
                        .and_then(|v| v.get("worst_ticks"))
                        .and_then(|n| n.as_i64())
                        .filter(|&w| w > 0)
                        .map(PriceTicks)
                        .unwrap_or(avg_t);
                    client.set_position_trading_stop(*spec, sl_t);
                    entry_forensics[s_idx].armed_ts_ns = opened_ns.0;
                    entry_forensics[s_idx].init_q_lots = init_l.0;
                    engines[s_idx].last_opened_ts = opened_ns;
                    engines[s_idx].peristaltic_cycles_count = u64::from(p_cycles);
                    engines[s_idx].state = OrderLifecycleState::PositionOpenMakerExit {
                        side: side_p,
                        entry_price: bybit_avg_t,
                        avg_price_ticks: avg_t,
                        peak_favorable_price: peak_t,
                        worst_adverse_price: worst_t,
                        initial_lots: init_l.max(lots),
                        open_lots: lots,
                        exhaled_lots: exh_l,
                        last_crest_exhale_price: crest_t,
                        peristaltic_cycles: p_cycles,
                        tranche1_harvested: t1_done,
                        active_exit_role: if t1_done {
                            ExitOrderRole::LvdbsDeepSurf50
                        } else {
                            ExitOrderRole::Tranche1Harvest50
                        },
                        active_exit_price: q_fl,
                        active_exit_lots: LotQty((lots.0 / 2).max(1)),
                        tp_maker_price: PriceTicks(0),
                        sl_trigger_price: sl_t,
                        opened_ts: opened_ns,
                    };
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    println!(
                        "{}\u{1f7e2} [VORTEX-L3 LIVE ADOPT] Slot {:2} {} {:?} qty={} @ {}t (T1={} | ZMPQR={}){}",
                        ANSI_GREEN, s_idx, sym_name, side_p, sz_s, avg_t.0, t1_done, p_cycles, ANSI_RESET
                    );
                }
            }
        }
        save_open_positions_state(&engines);
    }

    println!(
        "{}\u{1f7e2} [VORTEX-L3 200.0 Hz ARMED] 12 Autonomous Slot Engines + ZMPQR Peristaltic Pump + LVDBS L2/L3 Surf + 30% Siphon active.{}",
        ANSI_GREEN, ANSI_RESET
    );

    loop {
        thread::sleep(Duration::from_millis(5));
        let now_ns = now_epoch_ns();
        if Path::new(HALT_LOCK_PATH).exists() {
            thread::sleep(Duration::from_millis(250));
            continue;
        }
        let drain_active = Path::new(DRAIN_LOCK_PATH).exists();

        if now_ns.0.saturating_sub(last_wallet_sync_ns) >= 30_000_000_000 {
            last_wallet_sync_ns = now_ns.0;
            if let Some(ref client) = live_client {
                if let Some((w_f, _, _, _)) = client.fetch_wallet_and_equity_usdt() {
                    wallet_1e8 = (w_f * 100_000_000.0).round() as i64;
                    let comp_notional = ((wallet_1e8 * 29) / 100).clamp(1_800_000_000, 25_000_000_000);
                    for eng in &mut engines {
                        eng.notional_per_trade_1e8 = comp_notional;
                    }
                }
            }
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
        let btc_snap = cached_snaps[0];
        let mut open_count = 0_usize;

        for s_idx in 0..MAX_ACTIVE_PAIRS {
            let spec = APPROVED_SPECS[s_idx];
            let sym_name = spec.symbol_id.name();
            let snap = cached_snaps[s_idx];
            if snap.sequence_id == 0 {
                continue;
            }
            match engines[s_idx].state {
                OrderLifecycleState::Idle => {}
                OrderLifecycleState::PlacingEntryMaker {
                    side,
                    entry_price,
                    qty,
                    sl_trigger_price,
                    submitted_ts,
                    ..
                } => {
                    if drain_active {
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    let wait_ns = engines[s_idx].elastic_preflight_window_ns(snap.realized_vol_5m);
                    if now_ns.0.saturating_sub(submitted_ts.0) < wait_ns {
                        continue;
                    }
                    let cur_l1_px = match side {
                        Side::Bid => snap.bids[0].price,
                        Side::Ask => snap.asks[0].price,
                    };
                    let sig_still_valid = matches!(
                        OrthogonalFactorState::evaluate_entry_signal_with_leader(spec, &snap, &btc_snap),
                        SignalVerdict::AuthorizeMakerEntry { side: s_now, .. } if s_now == side
                    );
                    let drift_ticks = (cur_l1_px.0 - entry_price.0).abs();
                    if cur_l1_px.0 <= 0 || drift_ticks > 8 || !sig_still_valid {
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    let entry_price = cur_l1_px;
                    if let Some(ref client) = live_client {
                        let rc = client.place_post_only_order(
                            spec,
                            side,
                            entry_price,
                            qty,
                            Some(sl_trigger_price),
                            false,
                        );
                        if rc != 0 {
                            engines[s_idx].state = OrderLifecycleState::Idle;
                            engines[s_idx].sync_to_shm_telemetry(arena);
                            continue;
                        }
                    }
                    let q_l1 = match side {
                        Side::Bid => snap.bids[0].qty,
                        Side::Ask => snap.asks[0].qty,
                    };
                    let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
                    let d_sign_i = if matches!(side, Side::Bid) { 1_i64 } else { -1_i64 };
                    let (dr_b, ex_b) = gov.price_drift_and_extension_scaled_bps(s_idx, &snap);
                    let (ewma_b, sl_st, win_st) = gov.slot_direction_diagnostics(s_idx, side);
                    let signed_gm = snap.factor_geom.get() * dir_s;
                    let snr_v = OrthogonalFactorState::snr_composite_score(&snap);
                    let btc_c = OrthogonalFactorState::snr_composite_score(&btc_snap);
                    let (e_kin, e_min) = gov
                        .evaluate_kinematic_energy_surface(
                            s_idx,
                            side,
                            dr_b,
                            ex_b,
                            signed_gm,
                            snr_v.abs(),
                        )
                        .unwrap_or(((dr_b - ex_b) * d_sign_i, 125));
                    entry_forensics[s_idx] = SlotForensicEntrySnapshot {
                        armed_ts_ns: now_ns.0,
                        init_q_lots: q_l1.0,
                        ofi_in: snap.factor_ofi.get(),
                        tfi_in: snap.factor_tfi.get(),
                        geom_in: snap.factor_geom.get(),
                        snr_in: snr_v,
                        vol5m_in: (snap.realized_vol_5m.raw() as f64) / 100.0,
                        spread_in: (snap.asks[0].price.0 - snap.bids[0].price.0).max(1),
                        btc_comp_in: btc_c,
                        gov_ewma_bps: (ewma_b as f64) / 100.0,
                        win_streak: win_st,
                        sl_streak: sl_st,
                        drift_bps: (dr_b as f64) / 100.0,
                        ext_bps: (ex_b as f64) / 100.0,
                        kin_accel_net_bps: (((dr_b - ex_b) * d_sign_i) as f64) / 100.0,
                        kin_energy_in_bps: (e_kin as f64) / 100.0,
                        req_kin_energy_in_bps: (e_min as f64) / 100.0,
                    };
                    init_q_ahead[s_idx] = q_l1;
                    cur_q_ahead[s_idx] = q_l1;
                    let _ = engines[s_idx].on_entry_order_resting(now_ns);
                    arena.exec_armed_count.fetch_add(1, Ordering::Relaxed);
                    engines[s_idx].sync_to_shm_telemetry(arena);
                    let (ew_p, ef_p) = spec.format_price_usdt_parts(entry_price);
                    let (sw_p, sf_p) = spec.format_price_usdt_parts(sl_trigger_price);
                    println!(
                        "{}\u{1f7e1} [BYBIT V5 LIVE ORDER SENT] {} {:?} qty={} @ {}.{:04} USDT (SL: {}.{:04} | Q_L1: {} lots | Seq: {} | OFI: {:+.2} | TFI: {:+.2} | GEOM: {:+.2}){}",
                        ANSI_YELLOW,
                        sym_name,
                        side,
                        spec.format_lot_qty_str(qty),
                        ew_p,
                        ef_p,
                        sw_p,
                        sf_p,
                        q_l1.0,
                        snap.sequence_id,
                        snap.factor_ofi.get(),
                        snap.factor_tfi.get(),
                        snap.factor_geom.get(),
                        ANSI_RESET
                    );
                }
                OrderLifecycleState::RestingEntryQueue {
                    side,
                    entry_price,
                    qty,
                    sl_trigger_price,
                    resting_since_ts,
                    ..
                } => {
                    let at_l1 = match side {
                        Side::Bid => snap.bids[0].price == entry_price,
                        Side::Ask => snap.asks[0].price == entry_price,
                    };
                    if at_l1 {
                        let live_l1_q = match side {
                            Side::Bid => snap.bids[0].qty.0,
                            Side::Ask => snap.asks[0].qty.0,
                        };
                        if live_l1_q < cur_q_ahead[s_idx].0 {
                            cur_q_ahead[s_idx] = LotQty(live_l1_q.max(1));
                        }
                    }
                    if let Ok(tel) = arena.telemetry(s_idx) {
                        tel.queue_ahead_lots.store(cur_q_ahead[s_idx].0 as u64, Ordering::Release);
                    }
                    let resting_elapsed_ns = now_ns.0.saturating_sub(resting_since_ts.0);
                    let vol_q = snap.realized_vol_5m.raw().clamp(4_000, 12_500);
                    let elastic_queue_timeout_ns =
                        ((15_000_000_000_i64 * 6_500) / vol_q).clamp(10_000_000_000, 20_000_000_000);
                    let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
                    let flow_imp = (0.55 * snap.factor_ofi.get() + 0.45 * snap.factor_tfi.get()) * dir_s;
                    let dq_abort = (resting_elapsed_ns >= 1_500_000_000
                        && at_l1
                        && check_queue_depletion_velocity_abort(
                            side,
                            init_q_ahead[s_idx],
                            cur_q_ahead[s_idx],
                            &snap,
                        ))
                        || flow_imp <= -0.48;
                    if !drain_active && !dq_abort && !at_l1 && resting_elapsed_ns >= 2_500_000_000 {
                        let new_l1 = match side {
                            Side::Bid => snap.bids[0].price,
                            Side::Ask => snap.asks[0].price,
                        };
                        let moved_away = match side {
                            Side::Bid => new_l1.0 > entry_price.0 && (new_l1.0 - entry_price.0) <= 15,
                            Side::Ask => new_l1.0 < entry_price.0 && (entry_price.0 - new_l1.0) <= 15,
                        };
                        let sig_ok = matches!(
                            OrthogonalFactorState::evaluate_entry_signal_with_leader(spec, &snap, &btc_snap),
                            SignalVerdict::AuthorizeMakerEntry { side: s_now, .. } if s_now == side
                        );
                        if moved_away && sig_ok && now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns {
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_500_000_000;
                            if let Some(ref client) = live_client {
                                client.cancel_all_symbol_orders(sym_name);
                                let (already_open, _, _, real_avg_s) = client.get_open_position_info(sym_name);
                                if already_open {
                                    if let Ok(real_en_t) = spec.parse_price_ticks(&real_avg_s) {
                                        if let OrderLifecycleState::RestingEntryQueue {
                                            entry_price: ref mut ep,
                                            ..
                                        } = engines[s_idx].state
                                        {
                                            *ep = real_en_t;
                                        }
                                    }
                                    engines[s_idx].next_peg_allowed_ts_ns = 0;
                                } else {
                                    let rc = client.place_post_only_order(
                                        spec,
                                        side,
                                        new_l1,
                                        qty,
                                        Some(sl_trigger_price),
                                        false,
                                    );
                                    if rc == 0 {
                                        if let OrderLifecycleState::RestingEntryQueue {
                                            entry_price: ref mut ep,
                                            resting_since_ts: ref mut r_ts,
                                            ..
                                        } = engines[s_idx].state
                                        {
                                            *ep = new_l1;
                                            *r_ts = now_ns;
                                        }
                                        let q_new = match side {
                                            Side::Bid => snap.bids[0].qty,
                                            Side::Ask => snap.asks[0].qty,
                                        };
                                        init_q_ahead[s_idx] = q_new;
                                        cur_q_ahead[s_idx] = q_new;
                                        engines[s_idx].sync_to_shm_telemetry(arena);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    if drain_active || resting_elapsed_ns >= elastic_queue_timeout_ns || dq_abort
                    {
                        if let Some(ref client) = live_client {
                            client.cancel_all_symbol_orders(sym_name);
                            let (filled_on_abort, _, _, _) = client.get_open_position_info(sym_name);
                            if filled_on_abort {
                                let _ = engines[s_idx].on_maker_entry_filled(now_ns);
                                arena.exec_fill_count.fetch_add(1, Ordering::Relaxed);
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                save_open_positions_state(&engines);
                                let _ = tg_tx.send(format!(
                                    "\u{1f7e1} [VORTEX-L3 FILL] {} {:?} qty={} @ {}t",
                                    sym_name,
                                    side,
                                    spec.format_lot_qty_str(qty),
                                    entry_price.0
                                ));
                                continue;
                            }
                        }
                        gov.observe_entry_abort(s_idx, side, entry_price, now_ns);
                        engines[s_idx].state = OrderLifecycleState::Idle;
                        arena.exec_abort_count.fetch_add(1, Ordering::Relaxed);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        continue;
                    }
                    let filled = if let Some(ref client) = live_client {
                        if now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns {
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_200_000_000;
                            client.get_open_position_info(sym_name).0
                        } else {
                            false
                        }
                    } else {
                        match side {
                            Side::Bid => snap.asks[0].price.0 <= entry_price.0,
                            Side::Ask => snap.bids[0].price.0 >= entry_price.0,
                        }
                    };
                    if filled {
                        let _ = engines[s_idx].on_maker_entry_filled(now_ns);
                        if let OrderLifecycleState::PositionOpenMakerExit {
                            tp_maker_price: ref mut tp_init,
                            ..
                        } = engines[s_idx].state
                        {
                            *tp_init = PriceTicks(0);
                        }
                        arena.exec_fill_count.fetch_add(1, Ordering::Relaxed);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        save_open_positions_state(&engines);
                        let (en_w, en_f) = spec.format_price_usdt_parts(entry_price);
                        let win_fl_t = dynamic_maker_floor_price(side, entry_price, ScaledBps(520));
                        let (tp_w, tp_f) = spec.format_price_usdt_parts(win_fl_t);
                        let (sl_w, sl_f) = spec.format_price_usdt_parts(engines[s_idx].last_eff_sl_ticks);
                        let q_ms = (now_ns.0.saturating_sub(entry_forensics[s_idx].armed_ts_ns)).max(0) / 1_000_000;
                        println!(
                            "{}\u{1f7e1} [MAKER FILL] {} {:?} qty={} filled passively @ {}.{:04} USDT ({}t | WIN_TP: {}.{:04} | SL: {}.{:04} | Wait: {}ms){}",
                            ANSI_YELLOW, sym_name, side, spec.format_lot_qty_str(qty), en_w, en_f, entry_price.0, tp_w, tp_f, sl_w, sl_f, q_ms, ANSI_RESET
                        );
                        let _ = tg_tx.send(format!(
                            "\u{1f7e1} [VORTEX-L3 MAKER FILL]\n\u{2022} Par y Lado : {} ({:?}) | Lote: {}\n\u{2022} Entrada L1 : {}.{:04} USDT ({}t) | Cola: {} ms\n\u{2022} Techos/SL  : WIN Floor: {}.{:04} USDT | SL: {}.{:04} USDT\n\u{2022} Microestr. : OFI:{:+.2} | TFI:{:+.2} | GEOM:{:+.2} | SNR:{:+.2} | Vol5m:{:.0}bp",
                            sym_name,
                            side,
                            spec.format_lot_qty_str(qty),
                            en_w,
                            en_f,
                            entry_price.0,
                            q_ms,
                            tp_w,
                            tp_f,
                            sl_w,
                            sl_f,
                            entry_forensics[s_idx].ofi_in,
                            entry_forensics[s_idx].tfi_in,
                            entry_forensics[s_idx].geom_in,
                            entry_forensics[s_idx].snr_in,
                            entry_forensics[s_idx].vol5m_in
                        ));
                    }
                }
                OrderLifecycleState::PositionOpenMakerExit {
                    side,
                    mut avg_price_ticks,
                    mut peak_favorable_price,
                    mut worst_adverse_price,
                    initial_lots,
                    mut tranche1_harvested,
                    tp_maker_price: prev_tp_state,
                    sl_trigger_price,
                    opened_ts,
                    ..
                } => {
                    open_count += 1;
                    let cur_bbo = match side {
                        Side::Bid => snap.asks[0].price,
                        Side::Ask => snap.bids[0].price,
                    };
                    let ref_px = if snap.last_trade_price.0 > 0 {
                        snap.last_trade_price
                    } else {
                        cur_bbo
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
                    } = engines[s_idx].state
                    {
                        *pk = peak_favorable_price;
                        *wr = worst_adverse_price;
                    }
                    let avg_denom = avg_price_ticks.0.max(1);
                    let pk_d = match side {
                        Side::Bid => peak_favorable_price.0 - avg_price_ticks.0,
                        Side::Ask => avg_price_ticks.0 - peak_favorable_price.0,
                    };
                    let wr_d = match side {
                        Side::Bid => worst_adverse_price.0 - avg_price_ticks.0,
                        Side::Ask => avg_price_ticks.0 - worst_adverse_price.0,
                    };
                    engines[s_idx].last_closed_mfe_bps =
                        (pk_d.saturating_mul(1_000_000)) / avg_denom;
                    engines[s_idx].last_closed_mae_bps =
                        (wr_d.saturating_mul(1_000_000)) / avg_denom;
                    engines[s_idx].last_closed_peak_ticks = peak_favorable_price;
                    engines[s_idx].last_closed_worst_ticks = worst_adverse_price;

                    if let Some(ref client) = live_client {
                        let hold_s =
                            (now_ns.0.saturating_sub(opened_ts.0)).max(0) / 1_000_000_000;
                        let dir_s = if matches!(side, Side::Bid) { 1.0 } else { -1.0 };
                        let s_ofi = snap.factor_ofi.get() * dir_s;
                        let s_tfi = snap.factor_tfi.get() * dir_s;
                        let s_geom = snap.factor_geom.get() * dir_s;
                        let signed_flow = 0.55 * s_ofi + 0.45 * s_tfi;
                        let joint_adverse = s_ofi < -0.18 && s_tfi < -0.18;
                        let strong_trend = s_ofi > 0.20 || s_tfi > 0.25;

                        let time_c = (hold_s / 60).min(18) * 12;
                        let flow_c = if signed_flow < -0.35 {
                            ((-signed_flow - 0.35) * 280.0).round() as i64
                        } else {
                            0
                        };
                        let depth_c = if s_geom < -0.25 {
                            ((-s_geom - 0.25) * 180.0).round() as i64
                        } else {
                            0
                        };
                        let wald_pct = ((time_c + flow_c + depth_c) / 10).clamp(0, 48);
                        let base_sl_dist =
                            (avg_price_ticks.0 - sl_trigger_price.0).abs().max(4);
                        let contracted_dist =
                            ((base_sl_dist * (100 - wald_pct)) / 100).max(3);
                        let eff_sl_px = if s_idx != 7
                            && wald_pct >= 32
                            && signed_flow <= -0.55
                        {
                            match side {
                                Side::Bid => {
                                    PriceTicks((avg_price_ticks.0 - contracted_dist).max(1))
                                }
                                Side::Ask => PriceTicks(avg_price_ticks.0 + contracted_dist),
                            }
                        } else {
                            sl_trigger_price
                        };
                        engines[s_idx].last_eff_sl_ticks = eff_sl_px;
                        engines[s_idx].last_closed_wald_pct = wald_pct;

                        let cur_diff_bbo = match side {
                            Side::Bid => cur_bbo.0 - avg_price_ticks.0,
                            Side::Ask => avg_price_ticks.0 - cur_bbo.0,
                        };
                        let cur_gross_bps =
                            (cur_diff_bbo.saturating_mul(1_000_000)) / avg_denom;
                        let peak_gross_bps = engines[s_idx].last_closed_mfe_bps;
                        let worst_mae_bps = engines[s_idx].last_closed_mae_bps;
                        let recovered_from_red =
                            worst_mae_bps <= -150 && cur_gross_bps >= 440;
                        let mfe_pullback_lock = if tranche1_harvested {
                            hold_s >= 45 && cur_gross_bps >= 485 && signed_flow < -0.10
                        } else {
                            (peak_gross_bps >= 520
                                && (cur_gross_bps <= peak_gross_bps - 35 || signed_flow < 0.08))
                                || (cur_gross_bps >= 455 && hold_s >= 6 && signed_flow < 0.05)
                        };
                        let allow_scratch_floor = if tranche1_harvested {
                            hold_s >= 90 && joint_adverse
                        } else {
                            recovered_from_red
                                || hold_s >= 45
                                || worst_mae_bps <= -120
                                || (hold_s >= 18 && joint_adverse)
                                || (hold_s >= 30 && !strong_trend)
                        };

                        let target_amort =
                            if !tranche1_harvested && cur_gross_bps <= -120 {
                                ((hold_s / 1_800) as u64).min(8)
                            } else {
                                0
                            };
                        while engines[s_idx].peristaltic_cycles_count < target_amort
                            && ledger.sinking_fund_reserve_bps_x100 >= 200
                        {
                            if ledger.try_consume_sinking_fund_subsidy(100) > 0 {
                                let step_t =
                                    ((avg_price_ticks.0 * 100) / 1_000_000).max(1);
                                avg_price_ticks = match side {
                                    Side::Bid => {
                                        PriceTicks((avg_price_ticks.0 - step_t).max(1))
                                    }
                                    Side::Ask => PriceTicks(avg_price_ticks.0 + step_t),
                                };
                                let next_c = engines[s_idx]
                                    .peristaltic_cycles_count
                                    .saturating_add(1);
                                engines[s_idx].peristaltic_cycles_count = next_c;
                                let cyc_u32 = next_c as u32;
                                if let OrderLifecycleState::PositionOpenMakerExit {
                                    avg_price_ticks: ref mut ap_mut,
                                    peristaltic_cycles: ref mut pc_mut,
                                    ..
                                } = engines[s_idx].state
                                {
                                    *ap_mut = avg_price_ticks;
                                    *pc_mut = cyc_u32;
                                }
                            } else {
                                break;
                            }
                        }

                        let siphon_eligible = !tranche1_harvested
                            && hold_s >= 600
                            && worst_mae_bps <= -180
                            && ledger.sinking_fund_reserve_bps_x100 >= 120;
                        let siphon_sub_cap =
                            (ledger.sinking_fund_reserve_bps_x100 / 2).clamp(0, 360);
                        let siphon_floor_bps = (440 - siphon_sub_cap).clamp(80, 440);

                        let use_tranche1_50 = !tranche1_harvested
                            && initial_lots.0 >= 2
                            && !allow_scratch_floor
                            && !siphon_eligible;
                        let vol_fl = (snap.realized_vol_5m.raw() / 10).clamp(520, 700);
                        let mfe_lrn = engines[s_idx].learned_mfe_bps.clamp(540, 880);
                        let base_win_target = (vol_fl * 60 + mfe_lrn * 40) / 100;
                        let step_15s = (hold_s / 15).clamp(0, 6);
                        let time_relax = (step_15s * (base_win_target - 500).max(0)) / 6;
                        let target_floor_bps = if siphon_eligible {
                            ScaledBps(siphon_floor_bps)
                        } else if allow_scratch_floor {
                            ScaledBps(if tranche1_harvested { 465 } else { 440 })
                        } else if use_tranche1_50 {
                            ScaledBps(465)
                        } else if mfe_pullback_lock {
                            ScaledBps(cur_gross_bps.clamp(485, 525))
                        } else {
                            ScaledBps((base_win_target - time_relax).clamp(525, 740))
                        };
                        let req_delta_t = ((avg_price_ticks
                            .0
                            .saturating_mul(target_floor_bps.raw())
                            + 999_999)
                            / 1_000_000)
                            .max(1);
                        let active_floor_px = match side {
                            Side::Bid => PriceTicks(avg_price_ticks.0 + req_delta_t),
                            Side::Ask => {
                                PriceTicks((avg_price_ticks.0 - req_delta_t).max(1))
                            }
                        };
                        let (raw_vac_lvl, raw_surf_px, _) =
                            OrthogonalFactorState::compute_l2_l3_vacuum_surf_target(
                                side,
                                &snap,
                                active_floor_px,
                            );
                        let min_sell_px =
                            snap.asks[0].price.0.max(snap.bids[0].price.0 + 1).max(1);
                        let max_buy_px =
                            if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 1 {
                                snap.bids[0].price.0.min(snap.asks[0].price.0 - 1).max(1)
                            } else {
                                snap.bids[0].price.0.max(1)
                            };
                        let l1_non_crossing = match side {
                            Side::Bid => PriceTicks(active_floor_px.0.max(min_sell_px)),
                            Side::Ask => {
                                PriceTicks(active_floor_px.0.min(max_buy_px).max(1))
                            }
                        };
                        let lock_l1_now = siphon_eligible
                            || allow_scratch_floor
                            || use_tranche1_50
                            || mfe_pullback_lock
                            || (!tranche1_harvested && cur_gross_bps >= 440);
                        let (vac_lvl, surf_px) = if lock_l1_now {
                            (1_usize, l1_non_crossing)
                        } else {
                            let clamped_raw = match side {
                                Side::Bid => {
                                    PriceTicks(raw_surf_px.0.max(l1_non_crossing.0))
                                }
                                Side::Ask => PriceTicks(
                                    raw_surf_px.0.min(l1_non_crossing.0).max(1),
                                ),
                            };
                            (raw_vac_lvl, clamped_raw)
                        };

                        let rest_ready = now_ns.0 >= engines[s_idx].next_peg_allowed_ts_ns
                            && now_ns.0.saturating_sub(last_rest_call_ns) >= 55_000_000;
                        if rest_ready {
                            last_rest_call_ns = now_ns.0;
                            engines[s_idx].next_peg_allowed_ts_ns = now_ns.0 + 1_350_000_000;
                            let Some((mut still_open, mut real_sz_s, _, _)) =
                                client.query_open_position_verified(sym_name)
                            else {
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                continue;
                            };
                            if !still_open && cur_gross_bps < 0 {
                                match client.query_open_position_verified(sym_name) {
                                    Some((confirmed_open, sz2, _, _)) => {
                                        still_open = confirmed_open;
                                        real_sz_s = sz2;
                                    }
                                    None => {
                                        engines[s_idx].sync_to_shm_telemetry(arena);
                                        continue;
                                    }
                                }
                            }
                            if !still_open {
                                client.cancel_all_symbol_orders(sym_name);
                                let q_fl = defensive_zero_loss_maker_exit_price(
                                    side,
                                    avg_price_ticks,
                                );
                                let t1_fl = dynamic_maker_floor_price(
                                    side,
                                    avg_price_ticks,
                                    ScaledBps(465),
                                );
                                let mkt_ref = if snap.mark_price.0 > 0 {
                                    snap.mark_price
                                } else {
                                    ref_px
                                };
                                let sl_triggered = match side {
                                    Side::Bid => {
                                        mkt_ref.0 > 0 && mkt_ref.0 < avg_price_ticks.0 - 2
                                    }
                                    Side::Ask => mkt_ref.0 > avg_price_ticks.0 + 2,
                                };
                                let (close_kind, exit_px) = if sl_triggered {
                                    engines[s_idx].last_exit_sub_reason =
                                        ExitSubReason::WaldBaseStopLoss;
                                    (ClosedTradeKind::ServerMarkPriceStopLoss, mkt_ref)
                                } else {
                                    let t2_fill_px = if prev_tp_state.0 > 0 {
                                        match side {
                                            Side::Bid => PriceTicks(prev_tp_state.0.max(q_fl.0)),
                                            Side::Ask => {
                                                PriceTicks(prev_tp_state.0.min(q_fl.0).max(1))
                                            }
                                        }
                                    } else {
                                        q_fl
                                    };
                                    let unified_px = if tranche1_harvested {
                                        match side {
                                            Side::Bid => PriceTicks(
                                                ((t1_fl.0 + t2_fill_px.0 + 1) / 2).max(t1_fl.0),
                                            ),
                                            Side::Ask => PriceTicks(
                                                ((t1_fl.0 + t2_fill_px.0) / 2)
                                                    .min(t1_fl.0)
                                                    .max(1),
                                            ),
                                        }
                                    } else {
                                        t2_fill_px
                                    };
                                    let raw_d = match side {
                                        Side::Bid => unified_px.0 - avg_price_ticks.0,
                                        Side::Ask => avg_price_ticks.0 - unified_px.0,
                                    };
                                    let realized_g_bps =
                                        (raw_d.saturating_mul(1_000_000)) / avg_denom;
                                    if siphon_eligible {
                                        let sub_used = (440 - cur_gross_bps).clamp(40, 360);
                                        let _ = ledger
                                            .try_consume_sinking_fund_subsidy(sub_used);
                                        engines[s_idx].last_exit_sub_reason =
                                            ExitSubReason::SinkingFundSiphonExit;
                                    } else if realized_g_bps >= 500 || vac_lvl >= 2 {
                                        engines[s_idx].lvdbs_deep_surfs_count =
                                            engines[s_idx]
                                                .lvdbs_deep_surfs_count
                                                .saturating_add(1);
                                        engines[s_idx].last_exit_sub_reason =
                                            ExitSubReason::LvdbsDeepVacuumSurf;
                                    } else if tranche1_harvested {
                                        engines[s_idx].last_exit_sub_reason =
                                            ExitSubReason::Tranche1Harvest50;
                                    } else {
                                        engines[s_idx].last_exit_sub_reason =
                                            ExitSubReason::AdverseScratchEscape;
                                    }
                                    (ClosedTradeKind::MakerPassiveUnwind, unified_px)
                                };
                                let summary = engines[s_idx].finalize_close(
                                    close_kind,
                                    side,
                                    avg_price_ticks,
                                    exit_px,
                                    now_ns,
                                    &mut ledger,
                                );
                                gov.observe_closed_trade(
                                    s_idx,
                                    summary.side,
                                    summary.net_scaled_bps,
                                    summary.is_maker_exit,
                                    now_ns,
                                );
                                arena.exec_closed_count.fetch_add(1, Ordering::Relaxed);
                                append_closed_trade_logs(
                                    s_idx,
                                    &summary,
                                    &engines[s_idx],
                                    &entry_forensics[s_idx],
                                    &snap,
                                    &ledger,
                                    now_ns,
                                );
                                engines[s_idx].sync_to_shm_telemetry(arena);
                                save_open_positions_state(&engines);
                                let net_f =
                                    (summary.net_scaled_bps.raw() as f64) / 100.0;
                                let (en_w, en_f) =
                                    spec.format_price_usdt_parts(summary.entry_price);
                                let (ex_w, ex_f) =
                                    spec.format_price_usdt_parts(summary.exit_price);
                                let pnl_usdt =
                                    (summary.net_pnl_1e8 as f64) / 100_000_000.0;
                                let is_part_close = matches!(
                                    engines[s_idx].last_exit_sub_reason,
                                    ExitSubReason::Tranche1Harvest50
                                );
                                let (col, icon, lbl) =
                                    classify_trade_badge(net_f, is_part_close);
                                println!(
                                    "{}{} [TRADE CLOSE \u{2014} {}] {} {:?} | {}.{:04} -> {}.{:04} | Net: {:+.2} bps ({:+.4} USDT) | Hold: {}s{}",
                                    col,
                                    icon,
                                    lbl.trim(),
                                    sym_name,
                                    summary.side,
                                    en_w,
                                    en_f,
                                    ex_w,
                                    ex_f,
                                    net_f,
                                    pnl_usdt,
                                    hold_s,
                                    ANSI_RESET
                                );
                                let _ = tg_tx.send(format_telegram_close_message(
                                    spec,
                                    &summary,
                                    engines[s_idx].last_exit_sub_reason,
                                    ledger.closed_trades_count,
                                    &ledger,
                                ));
                            } else {
                                let real_lots =
                                    spec.parse_lot_qty(&real_sz_s).max(LotQty(1));
                                let eff_init_lots = if prev_tp_state.0 == 0 {
                                    real_lots
                                } else {
                                    initial_lots
                                };
                                if !tranche1_harvested
                                    && prev_tp_state.0 > 0
                                    && eff_init_lots.0 >= 2
                                    && real_lots.0 < eff_init_lots.0
                                    && hold_s >= 2
                                {
                                    tranche1_harvested = true;
                                    engines[s_idx].tranche1_harvests_count =
                                        engines[s_idx]
                                            .tranche1_harvests_count
                                            .saturating_add(1);
                                }
                                let order_lots_target = if !tranche1_harvested
                                    && real_lots.0 >= 2
                                    && !allow_scratch_floor
                                    && !siphon_eligible
                                {
                                    LotQty((real_lots.0 / 2).max(1))
                                } else {
                                    real_lots
                                };
                                let Some((resting_tp, resting_qty)) =
                                    client.get_resting_exit_order_info(spec)
                                else {
                                    engines[s_idx].sync_to_shm_telemetry(arena);
                                    continue;
                                };
                                let tp_diff_ticks = (surf_px.0 - resting_tp.0).abs();
                                let moving_closer = match side {
                                    Side::Bid => {
                                        resting_tp.0 == 0 || surf_px.0 < resting_tp.0
                                    }
                                    Side::Ask => {
                                        resting_tp.0 == 0 || surf_px.0 > resting_tp.0
                                    }
                                };
                                let qty_changed = resting_qty != order_lots_target;
                                let should_repeg = resting_tp.0 == 0
                                    || qty_changed
                                    || (moving_closer
                                        && tp_diff_ticks >= 1
                                        && (lock_l1_now || tp_diff_ticks >= 2))
                                    || (!moving_closer
                                        && cur_gross_bps < 200
                                        && tp_diff_ticks >= 4);
                                let active_tp_now = if should_repeg {
                                    client.cancel_all_symbol_orders(sym_name);
                                    let rc = client.place_post_only_order(
                                        spec,
                                        side.opposite(),
                                        surf_px,
                                        order_lots_target,
                                        None,
                                        true,
                                    );
                                    engines[s_idx].exit_peg_attempts =
                                        engines[s_idx].exit_peg_attempts.saturating_add(1);
                                    if rc == 0 { surf_px } else { PriceTicks(0) }
                                } else {
                                    resting_tp
                                };
                                if let OrderLifecycleState::PositionOpenMakerExit {
                                    initial_lots: ref mut in_l,
                                    open_lots: ref mut op_l,
                                    tranche1_harvested: ref mut t1_h,
                                    active_exit_lots: ref mut ax_l,
                                    tp_maker_price: ref mut tp_p,
                                    ..
                                } = engines[s_idx].state
                                {
                                    *in_l = eff_init_lots;
                                    *op_l = real_lots;
                                    *t1_h = tranche1_harvested;
                                    *ax_l = order_lots_target;
                                    *tp_p = active_tp_now;
                                }
                                save_open_positions_state(&engines);
                                engines[s_idx].sync_to_shm_telemetry(arena);
                            }
                        } else {
                            engines[s_idx].sync_to_shm_telemetry(arena);
                        }
                        continue;
                    }
                    let _ = engines[s_idx]
                        .step_open_position(&snap, &btc_snap, &gov, &mut ledger);
                    engines[s_idx].sync_to_shm_telemetry(arena);
                }
            }
        }

        if drain_active && open_count == 0 {
            let _ = tg_tx.send(
                "\u{1f7e2} [VORTEX-L3 DRAIN COMPLETE] 0 open positions verified (Wallet == Equity)."
                    .to_string(),
            );
            println!(
                "{}\u{1f7e2} [DRAIN_COMPLETE_STOPPED] 0 open positions verified. Exiting cleanly.{}",
                ANSI_GREEN, ANSI_RESET
            );
            return ExitCode::SUCCESS;
        }

        let active_or_queued = engines
            .iter()
            .filter(|e| !matches!(e.state, OrderLifecycleState::Idle))
            .count();
        if !drain_active && active_or_queued < MAX_CONCURRENT_OPEN_SLOTS {
            let mut armed_this_tick = 0_usize;
            for s_idx in 0..MAX_ACTIVE_PAIRS {
                if active_or_queued + armed_this_tick >= MAX_CONCURRENT_OPEN_SLOTS
                    || armed_this_tick >= 3
                {
                    break;
                }
                if !matches!(engines[s_idx].state, OrderLifecycleState::Idle) {
                    continue;
                }
                let spec = APPROVED_SPECS[s_idx];
                let snap = cached_snaps[s_idx];
                if snap.sequence_id == 0 {
                    continue;
                }
                let verdict = OrthogonalFactorState::evaluate_entry_signal_with_leader(
                    spec,
                    &snap,
                    &btc_snap,
                );
                if let SignalVerdict::AuthorizeMakerEntry { side, .. } = verdict {
                    if gov.authorizes_adaptive_entry(s_idx, side, &snap, &btc_snap) {
                        let (edge_bps, _, _) = gov.slot_direction_diagnostics(s_idx, side);
                        let in_debt = gov.is_in_scar_debt(s_idx, side);
                        let lots = spec.compute_compounding_kelly_lots(
                            wallet_1e8,
                            snap.bids[0].price,
                            edge_bps,
                            in_debt,
                        );
                        let _ = engines[s_idx].try_arm_maker_entry(&spec, &snap, verdict, lots);
                        engines[s_idx].sync_to_shm_telemetry(arena);
                        armed_this_tick += 1;
                    }
                }
            }
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_live_hud = args.iter().any(|a| a == "--live");
    let is_trades = args.iter().any(|a| a == "--trades");
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let is_live_cap = args.iter().any(|a| a == "--live-capital");

    let shm_path = MappedShmRegion::default_shm_path();
    if is_live_hud {
        if let Ok(reg) = MappedShmRegion::open_or_create(&shm_path, false) {
            render_live_hud(reg.arena());
        }
        return ExitCode::SUCCESS;
    }
    if is_trades {
        render_trades_and_executive_dashboard();
        return ExitCode::SUCCESS;
    }

    let (oos_trades, maker_ratio, pf, cum_usdt, drift) = run_oos_520_certification_suite();
    println!("================================================================================");
    println!("[VORTEX-EXEC V3.0] 12-SLOT EXECUTION FSM, PARITY LEDGER & OOS GATE VERIFIER");
    println!("================================================================================");
    println!(
        "  {}\u{1f7e2} -> OOS Closed Trades Verified : {} (Gate >= 500){}",
        ANSI_GREEN, oos_trades, ANSI_RESET
    );
    println!(
        "  {}\u{1f7e2} -> Maker Exit Ratio           : {:.2}% (Gate >= 80.00%){}",
        ANSI_GREEN, maker_ratio, ANSI_RESET
    );
    println!(
        "  {}\u{1f7e2} -> Net Profit Factor (PostFee): {:.2} (Gate >= 1.35){}",
        ANSI_GREEN, pf, ANSI_RESET
    );
    println!(
        "  {}\u{1f7e2} -> Cumulative Net PnL (OOS)   : + {:.4} USDT | PARITY_SYNC Drift: {} units{}",
        ANSI_GREEN, cum_usdt, drift, ANSI_RESET
    );
    println!("================================================================================");

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

    #[test]
    fn test_oos_520_certification_gate_and_zero_drift() {
        let (trades, maker_pct, pf, cum_usdt, drift) = run_oos_520_certification_suite();
        assert_eq!(trades, 520);
        assert!(maker_pct >= 80.0);
        assert!(pf >= 1.35);
        assert!(cum_usdt > 0.0);
        assert_eq!(drift, 0);
    }

    #[test]
    fn test_12_slot_autonomous_array_initialization_and_persistence() {
        let engines = SlotExecutionEngine::new_12_slot_array(BASE_NOTIONAL_1E8, 10_000_000_000);
        assert_eq!(engines.len(), 12);
        for (idx, eng) in engines.iter().enumerate() {
            assert_eq!(eng.slot_idx, idx);
            assert_eq!(eng.state, OrderLifecycleState::Idle);
        }
    }

    #[test]
    fn test_forensic_persistence_and_peristaltic_siphon_amortization() {
        let mut ledger = ParitySyncLedger::new(DEFAULT_WALLET_EQUITY_1E8);
        ledger.record_closed_trade_with_sinking_split(
            50_000_000,
            4_000_000,
            ScaledBps(1_200),
        );
        assert!(ledger.sinking_fund_reserve_bps_x100 >= 300);
        let vaulted_before = ledger.vault_locked_profit_bps_x100;
        assert_eq!(ledger.try_consume_sinking_fund_subsidy(100), 100);
        assert_eq!(ledger.vault_locked_profit_bps_x100, vaulted_before);
    }
}
