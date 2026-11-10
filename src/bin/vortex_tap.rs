//! VORTEX-L3 Immortal 12-Pair WebSocket Ingestor, Isolated Audit Gate & PAR1 Ring Writer.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

#[cfg(target_os = "linux")]
use vortex_rs::alpha_3factor::OrthogonalFactorState;
use vortex_rs::forensic_parquet::{
    ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind,
};
#[cfg(target_os = "linux")]
use vortex_rs::lob_shm::{classify_l2_sequence_transition, L2SeqDecision};
use vortex_rs::lob_shm::{
    BookLevel, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
    VORTEX_SHM_VERSION,
};
use vortex_rs::types::{
    now_epoch_ns, BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, SymbolId,
    VortexError, ANSI_GREEN, ANSI_RED, ANSI_RESET, APPROVED_SPECS,
};
#[cfg(target_os = "linux")]
use vortex_rs::types::{slot_from_symbol_name, TimestampNs};

const MAX_PARQUET_RING_FILES: u64 = 4_096;
#[cfg(target_os = "linux")]
const BYBIT_PUBLIC_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";

#[derive(Copy, Clone, Debug)]
struct VetoedCandidate {
    name: &'static str,
    spec: PairSpec,
    ref_price_ticks: PriceTicks,
    vol_5m: ScaledBps,
}

const VETOED_CANDIDATES: [VetoedCandidate; 3] = [
    VetoedCandidate {
        name: "XRPUSDT",
        spec: PairSpec {
            symbol_id: SymbolId(901),
            tick_size_1e8: 10_000,
            lot_step_1e8: 100_000_000,
            empirical_taker_exit_prob_pct: 15,
        },
        ref_price_ticks: PriceTicks(5_520),
        vol_5m: ScaledBps(4_800),
    },
    VetoedCandidate {
        name: "ZEREBROUSDT",
        spec: PairSpec {
            symbol_id: SymbolId(902),
            tick_size_1e8: 1_000,
            lot_step_1e8: 100_000_000,
            empirical_taker_exit_prob_pct: 25,
        },
        ref_price_ticks: PriceTicks(2_976),
        vol_5m: ScaledBps(9_500),
    },
    VetoedCandidate {
        name: "WALUSDT",
        spec: PairSpec {
            symbol_id: SymbolId(903),
            tick_size_1e8: 1_000,
            lot_step_1e8: 100_000_000,
            empirical_taker_exit_prob_pct: 25,
        },
        ref_price_ticks: PriceTicks(2_976),
        vol_5m: ScaledBps(9_200),
    },
];

const SAMPLE_PRICES_TICKS: [i64; MAX_ACTIVE_PAIRS] = [
    759_200, 223_500, 8_930, 8_180, 22_350, 8_510,
    3_140, 16_150, 1_595, 10_710, 3_680, 5_160,
];

const SAMPLE_VOL_5M_BPS: [i64; MAX_ACTIVE_PAIRS] = [
    5_800, 6_000, 6_400, 6_200, 7_200, 6_800,
    7_500, 7_100, 8_200, 6_100, 6_500, 5_600,
];

pub fn verify_and_seed_12_institutional_slots(
    arena: &VortexShmArena,
    batch: &mut ColumnarParquetBatch,
    verbose: bool,
) -> Result<usize, VortexError> {
    let now_ns = now_epoch_ns();
    let mut approved_count = 0_usize;
    for (slot_idx, spec) in APPROVED_SPECS.iter().enumerate() {
        let px = PriceTicks(SAMPLE_PRICES_TICKS[slot_idx]);
        let vol = ScaledBps(SAMPLE_VOL_5M_BPS[slot_idx]);
        let tick_bps = spec.validate_pair_eligibility(px, vol)?;
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.is_eligible = true;
        snap.sequence_id = 1;
        snap.ts_exchange_ns = now_ns;
        snap.ts_local_ns = now_ns;
        snap.mark_price = px;
        snap.last_trade_price = px;
        snap.realized_vol_5m = vol;
        snap.factor_ofi = BoundedScore::ZERO;
        snap.factor_tfi = BoundedScore::ZERO;
        snap.factor_geom = BoundedScore::ZERO;
        for lvl in 0..5 {
            snap.bids[lvl] = BookLevel {
                price: PriceTicks(px.0 - (lvl as i64)),
                qty: LotQty(300 + (lvl as i64) * 50),
            };
            snap.asks[lvl] = BookLevel {
                price: PriceTicks(px.0 + 1 + (lvl as i64)),
                qty: LotQty(280 + (lvl as i64) * 50),
            };
        }
        snap.refresh_deep_l1_l5_totals();
        let slot_ref = arena.slot(slot_idx)?;
        slot_ref.write_snapshot(&snap);
        let verified_snap = slot_ref.read_snapshot(4)?;
        if verified_snap.symbol_id != spec.symbol_id {
            return Err(VortexError::InvalidSymbolSlot);
        }
        let row = ColumnarMarketRow {
            symbol_id: spec.symbol_id,
            event_kind: MarketEventKind::L2BookSnapshot,
            side: Side::Bid,
            sequence_id: 1,
            ts_exchange_ns: now_ns,
            ts_local_ns: now_ns,
            price_ticks: snap.bids[0].price,
            lot_qty: snap.bids[0].qty,
            best_bid_ticks: snap.bids[0].price,
            best_ask_ticks: snap.asks[0].price,
            best_bid_qty: snap.bids[0].qty,
            best_ask_qty: snap.asks[0].qty,
            mark_price_ticks: snap.mark_price,
            ofi_bps: 0,
            tfi_bps: 0,
            geom_bps: 0,
        };
        batch.push_row(&row)?;
        approved_count += 1;
        if verbose {
            println!(
                concat!(
                    "  {}\u{1f7e2} [APPROVED -> SLOT {:2}] {:<10} | ",
                    "Tick_bps: {:>5.2} bps (Elastic) | Vol5m: {} bps{}"
                ),
                ANSI_GREEN,
                slot_idx,
                spec.symbol_id.name(),
                (tick_bps.raw() as f64) / 100.0,
                vol.raw() / 100,
                ANSI_RESET
            );
        }
    }
    if verbose {
        for v in &VETOED_CANDIDATES {
            if let Err(reason) = v.spec.validate_pair_eligibility(v.ref_price_ticks, v.vol_5m) {
                println!(
                    "  {}\u{1f534} [VETOED BY GUARD   ] {:<10} | Reason: {:?}{}",
                    ANSI_RED, v.name, reason, ANSI_RESET
                );
            }
        }
    }
    arena.heartbeat_ns.store(now_ns.0 as u64, Ordering::Release);
    Ok(approved_count)
}

/// Runs the 12-pair + Seqlock + PAR1 gate inside an isolated ephemeral arena (0 live SHM writes).
pub fn run_isolated_tap_self_test(verbose: bool) -> Result<(usize, usize, u64), VortexError> {
    let eph_shm = MappedShmRegion::ephemeral_self_test_path("tap_gate");
    let ts_u = now_epoch_ns().0.unsigned_abs();
    let eph_par = std::env::temp_dir().join(format!(
        "vortex_l3_tap_gate_{}_{}.parquet",
        std::process::id(),
        ts_u
    ));
    let mut batch = ColumnarParquetBatch::new();
    let res = (|| -> Result<(usize, usize, u64), VortexError> {
        let region = MappedShmRegion::open_or_create(&eph_shm, true)?;
        let approved = verify_and_seed_12_institutional_slots(region.arena(), &mut batch, verbose)?;
        let checksum = batch.write_to_file(&eph_par)?;
        let (reloaded, verified_crc) = ColumnarParquetBatch::read_from_file(&eph_par)?;
        if verified_crc != checksum || reloaded.len() != approved {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        Ok((approved, reloaded.len(), checksum))
    })();
    let _ = fs::remove_file(&eph_shm);
    let _ = fs::remove_file(&eph_par);
    res
}

fn inspect_live_shm_readonly(shm_path: &Path) {
    if !shm_path.exists() {
        return;
    }
    if let Ok(ro_reg) = MappedShmRegion::open_existing_read_only(shm_path) {
        let arena = ro_reg.arena();
        let now_ns = now_epoch_ns();
        let hb_age_ms = arena
            .heartbeat_age_ns(now_ns)
            .map(|ns| ns / 1_000_000)
            .unwrap_or(0);
        let mut live_slots = 0_usize;
        for idx in 0..MAX_ACTIVE_PAIRS {
            if let Ok(slot) = arena.slot(idx) {
                if let Ok(sn) = slot.read_snapshot(2) {
                    if sn.sequence_id > 0 {
                        live_slots += 1;
                    }
                }
            }
        }
        println!(
            "[LIVE SHM READ-ONLY] {} | Active Live Slots: {}/{} | Heartbeat Age: {} ms",
            shm_path.display(),
            live_slots,
            MAX_ACTIVE_PAIRS,
            hb_age_ms
        );
    }
}

pub fn prune_expired_parquet_segment(data_dir: &Path, parquet_seq: u64) {
    if parquet_seq > MAX_PARQUET_RING_FILES {
        let expired_seq = parquet_seq - MAX_PARQUET_RING_FILES;
        let expired_path =
            data_dir.join(format!("vortex_l2_segment_{:06}.parquet", expired_seq));
        let _ = fs::remove_file(&expired_path);
    }
    let legacy_lake = data_dir.join("lakehouse_par1");
    if legacy_lake.exists() {
        let _ = fs::remove_dir_all(&legacy_lake);
    }
}

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

#[cfg(target_os = "linux")]
fn flush_parquet_batch(batch: &mut ColumnarParquetBatch, parquet_seq: &mut u64) {
    if batch.is_empty() {
        return;
    }
    let data_dir = Path::new("data");
    let _ = fs::create_dir_all(data_dir);
    *parquet_seq = parquet_seq.wrapping_add(1);
    let seg_name = format!("vortex_l2_segment_{:06}.parquet", *parquet_seq);
    let file_path = data_dir.join(seg_name);
    if let Ok(checksum) = batch.write_to_file(&file_path) {
        println!(
            "[PAR1 FLUSH] Wrote {} live events to {} (FNV1a64: 0x{:016X})",
            batch.len(),
            file_path.display(),
            checksum
        );
    }
    prune_expired_parquet_segment(data_dir, *parquet_seq);
    batch.clear();
}

#[cfg(target_os = "linux")]
fn discover_latest_parquet_seq() -> u64 {
    let mut max_seq = 0_u64;
    if let Ok(entries) = fs::read_dir("data") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if let Some(rest) = s.strip_prefix("vortex_l2_segment_") {
                if let Some(num_s) = rest.strip_suffix(".parquet") {
                    if let Ok(seq) = num_s.parse::<u64>() {
                        if seq > max_seq {
                            max_seq = seq;
                        }
                    }
                }
            }
        }
    }
    max_seq
}

#[cfg(not(target_os = "linux"))]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena) -> ! {
    loop {
        let local_ns = now_epoch_ns();
        arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
        thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(target_os = "linux")]
fn run_live_bybit_ws_daemon(arena: &VortexShmArena) -> ! {
    let mut factor_states = [OrthogonalFactorState::new(); MAX_ACTIVE_PAIRS];
    let mut active_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut vol_anchor_px = [0_i64; MAX_ACTIVE_PAIRS];
    let mut vol_anchor_ts_ns = [0_i64; MAX_ACTIVE_PAIRS];
    let mut has_live_mark_px = [false; MAX_ACTIVE_PAIRS];
    let mut has_book_snapshot = [false; MAX_ACTIVE_PAIRS];
    let mut batch = ColumnarParquetBatch::new();
    let mut parquet_seq = discover_latest_parquet_seq();

    loop {
        for s in 0..MAX_ACTIVE_PAIRS {
            factor_states[s].reset();
            active_snaps[s] = SymbolLobSnapshot::new_empty();
            active_snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
            active_snaps[s].is_eligible = true;
            active_snaps[s].realized_vol_5m = ScaledBps(SAMPLE_VOL_5M_BPS[s]);
            has_live_mark_px[s] = false;
            has_book_snapshot[s] = false;
        }
        if let Ok((mut socket, _)) = tungstenite::connect(BYBIT_PUBLIC_LINEAR_WS) {
            configure_ws_tcp_stream(&mut socket);
            let mut topics = Vec::with_capacity(MAX_ACTIVE_PAIRS * 3);
            for spec in &APPROVED_SPECS {
                let sym = spec.symbol_id.name();
                topics.push(format!("\"orderbook.50.{}\"", sym));
                topics.push(format!("\"publicTrade.{}\"", sym));
                topics.push(format!("\"tickers.{}\"", sym));
            }
            let sub_msg = format!(
                "{{\"op\":\"subscribe\",\"args\":[{}]}}",
                topics.join(",")
            );
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
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                        continue;
                    };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    let is_snapshot = val
                        .get("type")
                        .and_then(|t| t.as_str())
                        .map(|t| t == "snapshot")
                        .unwrap_or(false);
                    let Some(data) = val.get("data") else {
                        continue;
                    };
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
                            factor_states[slot_idx].reset();
                            has_book_snapshot[slot_idx] = true;
                        }
                        L2SeqDecision::ApplyMonotonicDelta => {}
                        L2SeqDecision::IgnorePreSnapshotOrStaleDelta => continue,
                    }
                    if incoming_u > 0 {
                        snap.sequence_id = incoming_u;
                    }
                    if let Some(bids) = data.get("b").and_then(|b| b.as_array()) {
                        for b_entry in bids {
                            if let Some(arr) = b_entry.as_array() {
                                if arr.len() >= 2 {
                                    let p_s = arr[0].as_str().unwrap_or("");
                                    let q_s = arr[1].as_str().unwrap_or("");
                                    if let Ok(px) = spec.parse_price_ticks(p_s) {
                                        let qty = spec.parse_lot_qty(q_s);
                                        snap.apply_level_update(Side::Bid, px, qty);
                                    }
                                }
                            }
                        }
                    }
                    if let Some(asks) = data.get("a").and_then(|a| a.as_array()) {
                        for a_entry in asks {
                            if let Some(arr) = a_entry.as_array() {
                                if arr.len() >= 2 {
                                    let p_s = arr[0].as_str().unwrap_or("");
                                    let q_s = arr[1].as_str().unwrap_or("");
                                    if let Ok(px) = spec.parse_price_ticks(p_s) {
                                        let qty = spec.parse_lot_qty(q_s);
                                        snap.apply_level_update(Side::Ask, px, qty);
                                    }
                                }
                            }
                        }
                    }
                    if snap.bbo_and_spread_ticks().is_none() {
                        continue;
                    }
                    let mid_l1 = PriceTicks((snap.bids[0].price.0 + snap.asks[0].price.0) / 2);
                    if !has_live_mark_px[slot_idx] || snap.mark_price.0 <= 0 {
                        snap.mark_price = mid_l1;
                    }
                    if snap.last_trade_price.0 <= 0 {
                        snap.last_trade_price = mid_l1;
                    }
                    let dt_v = local_ns.0.saturating_sub(vol_anchor_ts_ns[slot_idx]);
                    if vol_anchor_px[slot_idx] <= 0 {
                        vol_anchor_px[slot_idx] = mid_l1.0;
                        vol_anchor_ts_ns[slot_idx] = local_ns.0;
                    } else if dt_v >= 5_000_000_000 {
                        snap.realized_vol_5m = OrthogonalFactorState::update_realized_vol_5m(
                            snap.realized_vol_5m,
                            PriceTicks(vol_anchor_px[slot_idx]),
                            mid_l1,
                            dt_v,
                        );
                        vol_anchor_px[slot_idx] = mid_l1.0;
                        vol_anchor_ts_ns[slot_idx] = local_ns.0;
                    }
                    snap.ts_exchange_ns = exch_ns;
                    snap.ts_local_ns = local_ns;
                    factor_states[slot_idx].on_l2_book_update(snap);
                    if let Ok(slot_ref) = arena.slot(slot_idx) {
                        slot_ref.write_snapshot(snap);
                    }
                    let row = ColumnarMarketRow {
                        symbol_id: snap.symbol_id,
                        event_kind: MarketEventKind::L2BookSnapshot,
                        side: Side::Bid,
                        sequence_id: snap.sequence_id,
                        ts_exchange_ns: exch_ns,
                        ts_local_ns: local_ns,
                        price_ticks: snap.mark_price,
                        lot_qty: snap.bids[0].qty,
                        best_bid_ticks: snap.bids[0].price,
                        best_ask_ticks: snap.asks[0].price,
                        best_bid_qty: snap.bids[0].qty,
                        best_ask_qty: snap.asks[0].qty,
                        mark_price_ticks: snap.mark_price,
                        ofi_bps: snap.factor_ofi.to_bps_i32(),
                        tfi_bps: snap.factor_tfi.to_bps_i32(),
                        geom_bps: snap.factor_geom.to_bps_i32(),
                    };
                    if batch.push_row(&row).is_err() {
                        flush_parquet_batch(&mut batch, &mut parquet_seq);
                        let _ = batch.push_row(&row);
                    }
                } else if let Some(sym_name) = topic.strip_prefix("publicTrade.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                        continue;
                    };
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
                                if !has_book_snapshot[slot_idx]
                                    || snap.bbo_and_spread_ticks().is_none()
                                {
                                    continue;
                                }
                                if let Ok(slot_ref) = arena.slot(slot_idx) {
                                    slot_ref.write_snapshot(snap);
                                }
                                let row = ColumnarMarketRow {
                                    symbol_id: snap.symbol_id,
                                    event_kind: MarketEventKind::PublicTradePrint,
                                    side,
                                    sequence_id: snap.sequence_id,
                                    ts_exchange_ns: exch_ns,
                                    ts_local_ns: local_ns,
                                    price_ticks: px,
                                    lot_qty: qty,
                                    best_bid_ticks: snap.bids[0].price,
                                    best_ask_ticks: snap.asks[0].price,
                                    best_bid_qty: snap.bids[0].qty,
                                    best_ask_qty: snap.asks[0].qty,
                                    mark_price_ticks: snap.mark_price,
                                    ofi_bps: snap.factor_ofi.to_bps_i32(),
                                    tfi_bps: snap.factor_tfi.to_bps_i32(),
                                    geom_bps: snap.factor_geom.to_bps_i32(),
                                };
                                if batch.push_row(&row).is_err() {
                                    flush_parquet_batch(&mut batch, &mut parquet_seq);
                                    let _ = batch.push_row(&row);
                                }
                            }
                        }
                    }
                } else if let Some(sym_name) = topic.strip_prefix("tickers.") {
                    let Some(slot_idx) = slot_from_symbol_name(sym_name) else {
                        continue;
                    };
                    let spec = APPROVED_SPECS[slot_idx];
                    let snap = &mut active_snaps[slot_idx];
                    let mp_opt = val.get("data").and_then(|d| {
                        d.get("markPrice")
                            .and_then(|v| v.as_str())
                            .or_else(|| {
                                d.as_array()
                                    .and_then(|a| a.first())
                                    .and_then(|o| o.get("markPrice"))
                                    .and_then(|v| v.as_str())
                            })
                    });
                    if let Some(mp_s) = mp_opt {
                        if let Ok(mp_t) = spec.parse_price_ticks(mp_s) {
                            snap.mark_price = mp_t;
                            snap.ts_exchange_ns = exch_ns;
                            snap.ts_local_ns = local_ns;
                            has_live_mark_px[slot_idx] = true;
                            if has_book_snapshot[slot_idx]
                                && snap.bbo_and_spread_ticks().is_some()
                            {
                                if let Ok(slot_ref) = arena.slot(slot_idx) {
                                    slot_ref.write_snapshot(snap);
                                }
                            }
                        }
                    }
                }
            }
        }
        flush_parquet_batch(&mut batch, &mut parquet_seq);
        thread::sleep(Duration::from_millis(500));
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let shm_path = MappedShmRegion::default_shm_path();

    let sep = "================================================================================";
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

    println!("--------------------------------------------------------------------------------");
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
    run_live_bybit_ws_daemon(region.arena());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vortex_tap_isolated_self_test_never_clobbers_live_shm() {
        let live_shm = MappedShmRegion::ephemeral_self_test_path("simulated_live_shm");
        {
            let reg = MappedShmRegion::open_or_create(&live_shm, true);
            assert!(reg.is_ok());
            if let Ok(r) = reg {
                let mut live_snap = SymbolLobSnapshot::new_empty();
                live_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
                live_snap.sequence_id = 888_777;
                live_snap.bids[0] = BookLevel { price: PriceTicks(912_345), qty: LotQty(500) };
                live_snap.asks[0] = BookLevel { price: PriceTicks(912_346), qty: LotQty(450) };
                if let Ok(slot0) = r.arena().slot(0) {
                    slot0.write_snapshot(&live_snap);
                }
            }
        }
        let gate_res = run_isolated_tap_self_test(false);
        assert!(gate_res.is_ok());
        if let Ok((approved, rows, crc)) = gate_res {
            assert_eq!(approved, 12);
            assert_eq!(rows, 12);
            assert_ne!(crc, 0);
        }
        inspect_live_shm_readonly(&live_shm);
        let ro = MappedShmRegion::open_existing_read_only(&live_shm);
        assert!(ro.is_ok());
        if let Ok(r_check) = ro {
            if let Ok(s0) = r_check.arena().slot(0) {
                let snap_after = s0.read_snapshot(4);
                assert!(snap_after.is_ok());
                if let Ok(sn) = snap_after {
                    assert_eq!(sn.sequence_id, 888_777);
                    assert_eq!(sn.bids[0].price, PriceTicks(912_345));
                }
            }
        }
        let _ = fs::remove_file(&live_shm);
    }

    #[test]
    fn test_parquet_ring_prunes_expired_without_lakehouse_accumulation() {
        let tmp_dir = std::env::temp_dir().join("vortex_l3_ring_prune_test");
        let _ = fs::remove_dir_all(&tmp_dir);
        let lake_dir = tmp_dir.join("lakehouse_par1");
        assert!(fs::create_dir_all(&lake_dir).is_ok());
        let seg1 = tmp_dir.join("vortex_l2_segment_000001.parquet");
        assert!(fs::write(&seg1, b"PAR1").is_ok());
        prune_expired_parquet_segment(&tmp_dir, MAX_PARQUET_RING_FILES + 1);
        assert!(!seg1.exists());
        assert!(!lake_dir.exists());
        let _ = fs::remove_dir_all(&tmp_dir);
    }
}
