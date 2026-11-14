//! VORTEX-L3 Immortal 12-Pair WebSocket Ingestor Daemon (Delegates Ring & Gate to tap_ring).

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

#[cfg(target_os = "linux")]
use vortex_rs::alpha_3factor::OrthogonalFactorState;
#[cfg(target_os = "linux")]
use vortex_rs::forensic_parquet::{ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind};
#[cfg(target_os = "linux")]
use vortex_rs::lob_shm::{
    classify_l2_sequence_transition, BookLevel, L2SeqDecision, SymbolLobSnapshot, MAX_ACTIVE_PAIRS,
};
use vortex_rs::lob_shm::{MappedShmRegion, VortexShmArena, VORTEX_SHM_VERSION};
#[cfg(target_os = "linux")]
use vortex_rs::tap_ring::SAMPLE_VOL_5M_BPS;
use vortex_rs::tap_ring::{
    inspect_live_shm_readonly, prune_expired_parquet_segment, run_isolated_tap_self_test,
    AsyncParquetRingWriter,
};
use vortex_rs::types::now_epoch_ns;
#[cfg(target_os = "linux")]
use vortex_rs::types::{
    slot_from_symbol_name, LotQty, PriceTicks, ScaledBps, Side, TimestampNs, APPROVED_SPECS,
};

#[cfg(target_os = "linux")]
const BYBIT_PUBLIC_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";

#[cfg(target_os = "linux")]
fn configure_ws_tcp_stream(
    socket: &mut tungstenite::WebSocket<
        tungstenite::stream::MaybeTlsStream<std::net::TcpStream>,
    >,
) {
    let tcp_opt = match socket.get_mut() {
        tungstenite::stream::MaybeTlsStream::Plain(s) => Some(s),
        tungstenite::stream::MaybeTlsStream::NativeTls(tls) => Some(tls.get_mut()),
        _ => None,
    };
    if let Some(tcp) = tcp_opt {
        let _ = tcp.set_nodelay(true);
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
        let _ = tcp.set_write_timeout(Some(Duration::from_secs(5)));
    }
}

#[cfg(not(target_os = "linux"))]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, _writer: AsyncParquetRingWriter) -> ! {
    loop {
        let local_ns = now_epoch_ns();
        arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
        thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(target_os = "linux")]
fn fetch_bybit_public_json(query_path: &str) -> Option<serde_json::Value> {
    use std::io::{Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    let mut addrs = ("api.bybit.com", 443_u16).to_socket_addrs().ok()?;
    let addr = addrs.next()?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).ok()?;
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(4)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(4)));
    let connector = native_tls::TlsConnector::new().ok()?;
    let mut tls = connector.connect("api.bybit.com", tcp).ok()?;
    let req = format!(
        concat!(
            "GET {} HTTP/1.1\r\nHost: api.bybit.com\r\n",
            "User-Agent: vortex-l3-tap/1.0\r\nAccept: application/json\r\n",
            "Connection: close\r\n\r\n"
        ),
        query_path
    );
    tls.write_all(req.as_bytes()).ok()?;
    tls.flush().ok()?;
    let mut raw = Vec::with_capacity(16_384);
    tls.take(524_288).read_to_end(&mut raw).ok()?;
    let sep = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let body = &raw[sep + 4..];
    let start = body.iter().position(|&b| b == b'{')?;
    let end = body.iter().rposition(|&b| b == b'}')?;
    if end < start {
        return None;
    }
    serde_json::from_slice(&body[start..=end]).ok()
}

#[cfg(target_os = "linux")]
fn warm_start_elliott_wave_states(
    wave_states: &mut [vortex_rs::wave_elliott::CausalElliottWaveState; MAX_ACTIVE_PAIRS],
    active_snaps: &mut [SymbolLobSnapshot; MAX_ACTIVE_PAIRS],
) {
    for s in 0..MAX_ACTIVE_PAIRS {
        let spec = APPROVED_SPECS[s];
        let sym = spec.symbol_id.name();
        let p5 = format!("/v5/market/kline?category=linear&symbol={}&interval=5&limit=120", sym);
        let p1 = format!("/v5/market/kline?category=linear&symbol={}&interval=1&limit=90", sym);
        let po = format!(
            "/v5/market/open-interest?category=linear&symbol={}&intervalTime=5min&limit=60",
            sym
        );
        let k5 = fetch_bybit_public_json(&p5);
        let k1 = fetch_bybit_public_json(&p1);
        let oi = fetch_bybit_public_json(&po);
        if wave_states[s]
            .warm_start_from_bybit_bundle(spec, k5.as_ref(), k1.as_ref(), oi.as_ref())
            > 0
        {
            active_snaps[s].set_wave_observables(wave_states[s].observables());
        }
    }
}

#[cfg(target_os = "linux")]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena, writer: AsyncParquetRingWriter) -> ! {
    let mut factor_states = [OrthogonalFactorState::new(); MAX_ACTIVE_PAIRS];
    let mut wave_states =
        [vortex_rs::wave_elliott::CausalElliottWaveState::new(); MAX_ACTIVE_PAIRS];
    let mut active_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut has_live_mark_px = [false; MAX_ACTIVE_PAIRS];
    let mut has_book_snapshot = [false; MAX_ACTIVE_PAIRS];
    let mut batch = Box::<ColumnarParquetBatch>::default();

    loop {
        for s in 0..MAX_ACTIVE_PAIRS {
            factor_states[s].reset();
            wave_states[s].reset();
            active_snaps[s] = SymbolLobSnapshot::new_empty();
            active_snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
            active_snaps[s].is_eligible = true;
            active_snaps[s].realized_vol_5m = ScaledBps(SAMPLE_VOL_5M_BPS[s]);
            has_live_mark_px[s] = false;
            has_book_snapshot[s] = false;
        }
        warm_start_elliott_wave_states(&mut wave_states, &mut active_snaps);
        if let Ok((mut socket, _)) = tungstenite::connect(BYBIT_PUBLIC_LINEAR_WS) {
            configure_ws_tcp_stream(&mut socket);
            let mut topics = Vec::with_capacity(MAX_ACTIVE_PAIRS * 4);
            for spec in &APPROVED_SPECS {
                let sym = spec.symbol_id.name();
                topics.push(format!("\"orderbook.50.{}\"", sym));
                topics.push(format!("\"publicTrade.{}\"", sym));
                topics.push(format!("\"tickers.{}\"", sym));
                topics.push(format!("\"allLiquidation.{}\"", sym));
            }
            let sub_msg = format!("{{\"op\":\"subscribe\",\"args\":[{}]}}", topics.join(","));
            if socket.send(tungstenite::Message::Text(sub_msg)).is_err() {
                thread::sleep(Duration::from_millis(500));
                continue;
            }
            let mut last_ping_ns = now_epoch_ns().0;
            while let Ok(msg) = socket.read() {
                let local_ns = now_epoch_ns();
                arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
                if local_ns.0.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    if socket
                        .send(tungstenite::Message::Text("{\"op\":\"ping\"}".to_string()))
                        .is_err()
                    {
                        break;
                    }
                    last_ping_ns = local_ns.0;
                }
                let txt = match msg {
                    tungstenite::Message::Text(t) => t,
                    tungstenite::Message::Ping(payload) => {
                        let _ = socket.send(tungstenite::Message::Pong(payload));
                        continue;
                    }
                    tungstenite::Message::Close(_) => break,
                    _ => continue,
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) else {
                    continue;
                };
                let Some(topic) = val.get("topic").and_then(|t| t.as_str()) else {
                    continue;
                };
                let exch_ms = val.get("ts").and_then(|t| t.as_i64()).unwrap_or(0);
                let exch_ns = TimestampNs(if exch_ms > 0 {
                    exch_ms.saturating_mul(1_000_000)
                } else {
                    local_ns.0
                });
                if let Some(sym_name) = topic.strip_prefix("orderbook.50.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else { continue };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    let is_snapshot =
                        val.get("type").and_then(|t| t.as_str()).is_some_and(|t| t == "snapshot");
                    let Some(data) = val.get("data") else { continue };
                    let incoming_u = data.get("u").and_then(|u| u.as_u64()).unwrap_or(0);
                    match classify_l2_sequence_transition(
                        has_book_snapshot[slot_idx],
                        snap.sequence_id,
                        is_snapshot,
                        incoming_u,
                    ) {
                        L2SeqDecision::ResetAndApplySnapshot => {
                            snap.bids = [BookLevel::default(); 50];
                            snap.asks = [BookLevel::default(); 50];
                            factor_states[slot_idx].reset_l2_book_only();
                            has_book_snapshot[slot_idx] = true;
                        }
                        L2SeqDecision::ApplyMonotonicDelta => {}
                        L2SeqDecision::IgnorePreSnapshotOrStaleDelta => continue,
                    }
                    if incoming_u > 0 {
                        snap.sequence_id = incoming_u;
                    }
                    if let Some(bids) = data.get("b").and_then(|b| b.as_array()) {
                        for arr in bids.iter().filter_map(|e| e.as_array()) {
                            if arr.len() >= 2 {
                                if let Ok(px) = spec.parse_price_ticks(arr[0].as_str().unwrap_or("")) {
                                    let qty = spec.parse_lot_qty(arr[1].as_str().unwrap_or(""));
                                    snap.apply_level_update(Side::Bid, px, qty);
                                }
                            }
                        }
                    }
                    if let Some(asks) = data.get("a").and_then(|a| a.as_array()) {
                        for arr in asks.iter().filter_map(|e| e.as_array()) {
                            if arr.len() >= 2 {
                                if let Ok(px) = spec.parse_price_ticks(arr[0].as_str().unwrap_or("")) {
                                    let qty = spec.parse_lot_qty(arr[1].as_str().unwrap_or(""));
                                    snap.apply_level_update(Side::Ask, px, qty);
                                }
                            }
                        }
                    }
                    if snap.bbo_and_spread_ticks().is_none() {
                        continue;
                    }
                    let mid_l1 = PriceTicks(i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0));
                    if !has_live_mark_px[slot_idx] || snap.mark_price.0 <= 0 {
                        snap.mark_price = mid_l1;
                    }
                    if snap.last_trade_price.0 <= 0 {
                        snap.last_trade_price = mid_l1;
                    }
                    snap.realized_vol_5m = factor_states[slot_idx].observe_mid_and_step_elastic_vol(
                        spec, mid_l1, local_ns.0, snap.realized_vol_5m,
                    );
                    snap.ts_exchange_ns = exch_ns;
                    snap.ts_local_ns = local_ns;
                    let w_obs =
                        wave_states[slot_idx].observe_price_tick(spec, mid_l1, local_ns.0, snap.realized_vol_5m);
                    snap.set_wave_observables(w_obs);
                    factor_states[slot_idx].on_l2_book_update(snap);
                    if let Ok(slot_ref) = arena.slot(slot_idx) {
                        slot_ref.write_snapshot(snap);
                    }
                    let row = ColumnarMarketRow::from_lob_snapshot(
                        snap, MarketEventKind::L2BookSnapshot, Side::Bid, snap.mark_price, snap.bids[0].qty,
                    );
                    if batch.push_row(&row).is_err() {
                        let _ = writer.try_flush_and_clear(&mut batch);
                        let _ = batch.push_row(&row);
                    }
                } else if let Some(sym_name) = topic.strip_prefix("publicTrade.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else { continue };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    if let Some(trades) = val.get("data").and_then(|d| d.as_array()) {
                        for tr in trades {
                            let p_s = tr.get("p").and_then(|v| v.as_str()).unwrap_or("");
                            let q_s = tr.get("v").and_then(|v| v.as_str()).unwrap_or("");
                            let s_s = tr.get("S").and_then(|v| v.as_str()).unwrap_or("Buy");
                            if let Ok(px) = spec.parse_price_ticks(p_s) {
                                let qty = spec.parse_lot_qty(q_s).max(LotQty(1));
                                let side = if s_s == "Sell" { Side::Ask } else { Side::Bid };
                                snap.last_trade_price = px;
                                snap.ts_exchange_ns = exch_ns;
                                snap.ts_local_ns = local_ns;
                                factor_states[slot_idx].on_trade_print(side, qty, snap);
                                if !has_book_snapshot[slot_idx] || snap.bbo_and_spread_ticks().is_none() {
                                    continue;
                                }
                                if let Ok(slot_ref) = arena.slot(slot_idx) {
                                    slot_ref.write_snapshot(snap);
                                }
                                let row = ColumnarMarketRow::from_lob_snapshot(
                                    snap, MarketEventKind::PublicTradePrint, side, px, qty,
                                );
                                if batch.push_row(&row).is_err() {
                                    let _ = writer.try_flush_and_clear(&mut batch);
                                    let _ = batch.push_row(&row);
                                }
                            }
                        }
                    }
                } else if let Some(sym_name) = topic.strip_prefix("allLiquidation.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else { continue };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    if let Some(data_v) = val.get("data") {
                        let items: Vec<&serde_json::Value> = if let Some(arr) = data_v.as_array() {
                            arr.iter().collect()
                        } else {
                            vec![data_v]
                        };
                        for liq in items {
                            let s_str = liq.get("s").or_else(|| liq.get("side")).and_then(|v| v.as_str()).unwrap_or("Buy");
                            let q_str = liq.get("v").or_else(|| liq.get("size")).and_then(|v| v.as_str()).unwrap_or("0");
                            let l_side = if s_str == "Sell" { Side::Ask } else { Side::Bid };
                            let l_qty = spec.parse_lot_qty(q_str);
                            let w_obs = wave_states[slot_idx].observe_liquidation_print(spec, l_side, l_qty, local_ns.0);
                            snap.set_wave_observables(w_obs);
                        }
                    }
                } else if let Some(sym_name) = topic.strip_prefix("tickers.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else { continue };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    let oi_opt = val.get("data").and_then(|d| {
                        d.get("openInterest").and_then(|v| v.as_str()).or_else(|| {
                            d.as_array().and_then(|a| a.first()).and_then(|o| o.get("openInterest")).and_then(|v| v.as_str())
                        })
                    });
                    if let Some(Ok(oi_f)) = oi_opt.map(|s| s.trim().parse::<f64>()) {
                        let w_obs = wave_states[slot_idx].observe_open_interest(spec, oi_f, local_ns.0);
                        snap.set_wave_observables(w_obs);
                    }
                    let mp_opt = val.get("data").and_then(|d| {
                        d.get("markPrice").and_then(|v| v.as_str()).or_else(|| {
                            d.as_array().and_then(|a| a.first()).and_then(|o| o.get("markPrice")).and_then(|v| v.as_str())
                        })
                    });
                    if let Some(mp_s) = mp_opt {
                        if let Ok(mp_t) = spec.parse_price_ticks(mp_s) {
                            snap.mark_price = mp_t;
                            snap.ts_exchange_ns = exch_ns;
                            snap.ts_local_ns = local_ns;
                            has_live_mark_px[slot_idx] = true;
                            if has_book_snapshot[slot_idx] && snap.bbo_and_spread_ticks().is_some() {
                                if let Ok(slot_ref) = arena.slot(slot_idx) {
                                    slot_ref.write_snapshot(snap);
                                }
                            }
                        }
                    }
                }
            }
        }
        let _ = writer.try_flush_and_clear(&mut batch);
        thread::sleep(Duration::from_millis(500));
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let shm_path = MappedShmRegion::default_shm_path();
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);
    println!("{}", sep);
    println!(
        "[VORTEX-TAP V3.0] 12-PAIR ELIGIBILITY & SEQLOCK DAEMON (SHM v{}: {})",
        VORTEX_SHM_VERSION,
        shm_path.display()
    );
    println!("{}", sep);
    let Ok((approved, rows, checksum)) = run_isolated_tap_self_test(true) else {
        return ExitCode::FAILURE;
    };
    println!("{}", dash);
    println!(
        concat!(
            "[SEQLOCK & PAR1 OK] Isolated Gate Slots: {} | Rows Verified: {} | ",
            "FNV1a64: 0x{:016X}"
        ),
        approved, rows, checksum
    );
    if !is_daemon {
        inspect_live_shm_readonly(&shm_path);
        println!("{}", sep);
        return ExitCode::SUCCESS;
    }
    println!("{}", sep);
    prune_expired_parquet_segment(Path::new("data"), 0);
    let Ok(region) = MappedShmRegion::open_or_create(&shm_path, true) else {
        return ExitCode::FAILURE;
    };
    let writer = AsyncParquetRingWriter::spawn(PathBuf::from("data"), true);
    run_live_bybit_ws_daemon(region.arena(), writer);
}
