//! VORTEX-L3 Immortal 12-Pair WebSocket Ingestor, Eligibility Guard & PAR1 Ring Writer.
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

use vortex_rs::alpha_3factor::OrthogonalFactorState;
use vortex_rs::forensic_parquet::{
    ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind,
};
use vortex_rs::lob_shm::{
    BookLevel, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
    VORTEX_SHM_VERSION,
};
use vortex_rs::types::{
    now_epoch_ns, slot_from_symbol_name, BoundedScore, LotQty, PairSpec, PriceTicks,
    ScaledBps, Side, SymbolId, TimestampNs, ANSI_GREEN, ANSI_RED, ANSI_RESET, APPROVED_SPECS,
};

const MAX_PARQUET_RING_FILES: u64 = 4_096;
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
) -> Result<usize, vortex_rs::types::VortexError> {
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
        arena.slot(slot_idx)?.write_snapshot(&snap);
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
                "  {}\u{1f7e2} [APPROVED -> SLOT {:2}] {:<10} | Tick_bps: {:>5.2} bps (<= 1.50) | Vol5m: {} bps{}",
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

fn flush_parquet_batch(batch: &mut ColumnarParquetBatch, parquet_seq: &mut u64) {
    if batch.is_empty() {
        return;
    }
    let data_dir = Path::new("data");
    let _ = fs::create_dir_all(data_dir);
    *parquet_seq = parquet_seq.wrapping_add(1);
    let file_path = data_dir.join(format!("vortex_l2_segment_{:06}.parquet", *parquet_seq));
    if let Ok(checksum) = batch.write_to_file(&file_path) {
        println!(
            "[PAR1 FLUSH] Wrote {} live events to {} (FNV1a64: 0x{:016X})",
            batch.len(),
            file_path.display(),
            checksum
        );
    }
    if *parquet_seq > MAX_PARQUET_RING_FILES {
        let expired_seq = *parquet_seq - MAX_PARQUET_RING_FILES;
        let expired_path =
            data_dir.join(format!("vortex_l2_segment_{:06}.parquet", expired_seq));
        let lake_dir = data_dir.join("lakehouse_par1");
        let _ = fs::create_dir_all(&lake_dir);
        let lake_path = lake_dir.join(format!("vortex_l2_segment_{:06}.parquet", expired_seq));
        if fs::rename(&expired_path, &lake_path).is_err() {
            let _ = fs::remove_file(&expired_path);
        }
    }
    batch.clear();
}

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

fn run_live_bybit_ws_daemon(arena: &VortexShmArena) -> ! {
    let mut factor_states = [OrthogonalFactorState::new(); MAX_ACTIVE_PAIRS];
    let mut active_snaps = [SymbolLobSnapshot::new_empty(); MAX_ACTIVE_PAIRS];
    let mut vol_anchor_px = [0_i64; MAX_ACTIVE_PAIRS];
    let mut vol_anchor_ts_ns = [0_i64; MAX_ACTIVE_PAIRS];
    let mut batch = ColumnarParquetBatch::new();
    let mut parquet_seq = discover_latest_parquet_seq();

    loop {
        for s in 0..MAX_ACTIVE_PAIRS {
            factor_states[s].reset();
            active_snaps[s] = SymbolLobSnapshot::new_empty();
            active_snaps[s].symbol_id = APPROVED_SPECS[s].symbol_id;
            active_snaps[s].is_eligible = true;
            active_snaps[s].realized_vol_5m = ScaledBps(SAMPLE_VOL_5M_BPS[s]);
        }
        if let Ok((mut socket, _)) = tungstenite::connect(BYBIT_PUBLIC_LINEAR_WS) {
            let mut topics = Vec::with_capacity(MAX_ACTIVE_PAIRS * 2);
            for spec in &APPROVED_SPECS {
                let sym = spec.symbol_id.name();
                topics.push(format!("\"orderbook.50.{}\"", sym));
                topics.push(format!("\"publicTrade.{}\"", sym));
            }
            let sub_msg = format!(
                "{{\"op\":\"subscribe\",\"args\":[{}]}}",
                topics.join(",")
            );
            let _ = socket.send(tungstenite::Message::Text(sub_msg));
            let mut last_ping_ns = now_epoch_ns().0;

            while let Ok(msg) = socket.read() {
                let local_ns = now_epoch_ns();
                arena.heartbeat_ns.store(local_ns.0 as u64, Ordering::Release);
                if local_ns.0.saturating_sub(last_ping_ns) >= 15_000_000_000 {
                    let _ = socket.send(tungstenite::Message::Text(
                        "{\"op\":\"ping\"}".to_string(),
                    ));
                    last_ping_ns = local_ns.0;
                }
                let tungstenite::Message::Text(txt) = msg else {
                    continue;
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
                    if is_snapshot {
                        snap.bids = [BookLevel::default(); 50];
                        snap.asks = [BookLevel::default(); 50];
                    }
                    if let Some(data) = val.get("data") {
                        if let Some(u_seq) = data.get("u").and_then(|u| u.as_u64()) {
                            snap.sequence_id = u_seq;
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
                    }
                    if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
                        snap.mark_price =
                            PriceTicks((snap.bids[0].price.0 + snap.asks[0].price.0) / 2);
                        if snap.last_trade_price.0 <= 0 {
                            snap.last_trade_price = snap.mark_price;
                        }
                        let dt_v = local_ns.0.saturating_sub(vol_anchor_ts_ns[slot_idx]);
                        if vol_anchor_px[slot_idx] <= 0 {
                            vol_anchor_px[slot_idx] = snap.mark_price.0;
                            vol_anchor_ts_ns[slot_idx] = local_ns.0;
                        } else if dt_v >= 5_000_000_000 {
                            let a_px = vol_anchor_px[slot_idx].max(1);
                            let dp = (snap.mark_price.0 - a_px).abs();
                            let ret_5s = (dp.saturating_mul(1_000_000)) / a_px;
                            let inst_v = (ret_5s.saturating_mul(12)).clamp(4_200, 12_500);
                            let prev_v = snap.realized_vol_5m.raw();
                            let next_v = ((prev_v * 92 + inst_v * 8) / 100)
                                .clamp(4_200, 12_500);
                            snap.realized_vol_5m = ScaledBps(next_v);
                            vol_anchor_px[slot_idx] = snap.mark_price.0;
                            vol_anchor_ts_ns[slot_idx] = local_ns.0;
                        }
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
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let shm_path = MappedShmRegion::default_shm_path();

    println!("================================================================================");
    println!(
        "[VORTEX-TAP V3.0] 12-PAIR ELIGIBILITY & SEQLOCK DAEMON (SHM v{}: {})",
        VORTEX_SHM_VERSION,
        shm_path.display()
    );
    println!("================================================================================");

    let Ok(region) = MappedShmRegion::open_or_create(&shm_path, true) else {
        return ExitCode::FAILURE;
    };
    let arena = region.arena();
    let mut batch = ColumnarParquetBatch::new();
    let Ok(approved) = verify_and_seed_12_institutional_slots(arena, &mut batch, true) else {
        return ExitCode::FAILURE;
    };
    let tmp_par = std::env::temp_dir().join("vortex_l3_tap_gate.parquet");
    let Ok(checksum) = batch.write_to_file(&tmp_par) else {
        return ExitCode::FAILURE;
    };
    let _ = fs::remove_file(&tmp_par);
    println!("--------------------------------------------------------------------------------");
    println!(
        "[SEQLOCK & PAR1 OK] Active Institutional Slots: {} | Rows Verified: {} | FNV1a64: 0x{:016X}",
        approved,
        batch.len(),
        checksum
    );
    println!("================================================================================");

    if is_daemon {
        run_live_bybit_ws_daemon(arena);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vortex_tap_12_slot_eligibility_and_shm_v12_sync() {
        let shm_path = std::env::temp_dir().join("vortex_l3_tap_unit_test.mmap");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());
        if let Ok(region) = reg {
            let mut batch = ColumnarParquetBatch::new();
            let res = verify_and_seed_12_institutional_slots(region.arena(), &mut batch, false);
            assert_eq!(res, Ok(12));
            assert_eq!(batch.len(), 12);
            for s in 0..12 {
                if let Ok(slot) = region.arena().slot(s) {
                    let snap = slot.read_snapshot(4);
                    assert!(snap.is_ok());
                    if let Ok(sn) = snap {
                        assert_eq!(sn.symbol_id, APPROVED_SPECS[s].symbol_id);
                        assert!(sn.deep_bid_lots_l1_l5.0 > 0);
                    }
                }
            }
        }
        let _ = fs::remove_file(&shm_path);
    }
}
