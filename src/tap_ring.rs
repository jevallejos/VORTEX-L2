//! Portable 12-Pair Eligibility Gate, SHM Read-Only Inspector & Async PAR1 Ring Writer.

#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use core::sync::atomic::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

use crate::forensic_parquet::{ColumnarMarketRow, ColumnarParquetBatch, MarketEventKind};
use crate::lob_shm::{
    BookLevel, MappedShmRegion, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::types::{
    now_epoch_ns, BoundedScore, LotQty, PairSpec, PriceTicks, ScaledBps, Side, SymbolId,
    VortexError, ANSI_GREEN, ANSI_RED, ANSI_RESET, APPROVED_SPECS,
};

pub const MAX_PARQUET_RING_FILES: u64 = 4_096;
pub const ASYNC_PARQUET_QUEUE_DEPTH: usize = 8;

pub const SAMPLE_PRICES_TICKS: [i64; MAX_ACTIVE_PAIRS] = [
    759_200, 223_500, 8_930, 8_180, 22_350, 8_510,
    3_140, 16_150, 1_595, 10_710, 3_680, 5_160,
];

pub const SAMPLE_VOL_5M_BPS: [i64; MAX_ACTIVE_PAIRS] = [
    5_800, 6_000, 6_400, 6_200, 7_200, 6_800,
    7_500, 7_100, 8_200, 6_100, 6_500, 5_600,
];

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

fn discover_latest_parquet_seq(data_dir: &Path) -> u64 {
    let mut max_seq = 0_u64;
    if let Ok(entries) = fs::read_dir(data_dir) {
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

fn flush_parquet_batch_to_dir(
    data_dir: &Path,
    batch: &ColumnarParquetBatch,
    parquet_seq: &mut u64,
    verbose_log: bool,
) {
    if batch.is_empty() {
        return;
    }
    let _ = fs::create_dir_all(data_dir);
    *parquet_seq = parquet_seq.wrapping_add(1);
    let seg_name = format!("vortex_l2_segment_{:06}.parquet", *parquet_seq);
    let file_path = data_dir.join(seg_name);
    if let Ok(checksum) = batch.write_to_file(&file_path) {
        if verbose_log {
            println!(
                "[PAR1 FLUSH] Wrote {} live events to {} (FNV1a64: 0x{:016X})",
                batch.len(),
                file_path.display(),
                checksum
            );
        }
    }
    prune_expired_parquet_segment(data_dir, *parquet_seq);
}

#[derive(Debug)]
pub struct AsyncParquetRingWriter {
    tx: Option<SyncSender<Box<ColumnarParquetBatch>>>,
    recycle_rx: mpsc::Receiver<Box<ColumnarParquetBatch>>,
    worker: Option<JoinHandle<()>>,
}

impl AsyncParquetRingWriter {
    #[must_use]
    pub fn spawn(data_dir: PathBuf, verbose_log: bool) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Box<ColumnarParquetBatch>>(ASYNC_PARQUET_QUEUE_DEPTH);
        let (recycle_tx, recycle_rx) =
            mpsc::sync_channel::<Box<ColumnarParquetBatch>>(ASYNC_PARQUET_QUEUE_DEPTH);
        for _ in 0..ASYNC_PARQUET_QUEUE_DEPTH {
            let _ = recycle_tx.try_send(Box::<ColumnarParquetBatch>::default());
        }
        let init_seq = discover_latest_parquet_seq(&data_dir);
        let worker = thread::spawn(move || {
            let mut seq = init_seq;
            while let Ok(mut batch_box) = rx.recv() {
                flush_parquet_batch_to_dir(&data_dir, &batch_box, &mut seq, verbose_log);
                batch_box.clear();
                let _ = recycle_tx.try_send(batch_box);
            }
        });
        Self {
            tx: Some(tx),
            recycle_rx,
            worker: Some(worker),
        }
    }

    #[inline]
    pub fn try_flush_and_clear(&self, batch: &mut Box<ColumnarParquetBatch>) -> bool {
        if batch.is_empty() {
            return true;
        }
        let Some(ref sender) = self.tx else {
            batch.clear();
            return false;
        };
        let replacement = self
            .recycle_rx
            .try_recv()
            .unwrap_or_else(|_| Box::<ColumnarParquetBatch>::default());
        let outgoing = core::mem::replace(batch, replacement);
        match sender.try_send(outgoing) {
            Ok(()) => true,
            Err(mpsc::TrySendError::Full(mut returned)
            | mpsc::TrySendError::Disconnected(mut returned)) => {
                returned.clear();
                *batch = returned;
                false
            }
        }
    }
}

impl Drop for AsyncParquetRingWriter {
    fn drop(&mut self) {
        let _ = self.tx.take();
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

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
        if slot_ref.read_snapshot(4)?.symbol_id != spec.symbol_id {
            return Err(VortexError::InvalidSymbolSlot);
        }
        let row = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::L2BookSnapshot,
            Side::Bid,
            snap.bids[0].price,
            snap.bids[0].qty,
        );
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

pub fn run_isolated_tap_self_test(verbose: bool) -> Result<(usize, usize, u64), VortexError> {
    let eph_shm = MappedShmRegion::ephemeral_self_test_path("tap_gate");
    let ts_u = now_epoch_ns().0.unsigned_abs();
    let eph_dir = std::env::temp_dir().join(format!(
        "vortex_l3_tap_gate_dir_{}_{}",
        std::process::id(),
        ts_u
    ));
    let mut batch = Box::<ColumnarParquetBatch>::default();
    let res = (|| -> Result<(usize, usize, u64), VortexError> {
        fs::create_dir_all(&eph_dir).map_err(|_| VortexError::ColumnarIoFailure)?;
        let region = MappedShmRegion::open_or_create(&eph_shm, true)?;
        let approved =
            verify_and_seed_12_institutional_slots(region.arena(), &mut batch, verbose)?;
        {
            let writer = AsyncParquetRingWriter::spawn(eph_dir.clone(), false);
            if !writer.try_flush_and_clear(&mut batch) {
                return Err(VortexError::ColumnarIoFailure);
            }
        }
        let seg_path = eph_dir.join("vortex_l2_segment_000001.parquet");
        let (reloaded, verified_crc) = ColumnarParquetBatch::read_from_file(&seg_path)?;
        if reloaded.len() != approved {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        Ok((approved, reloaded.len(), verified_crc))
    })();
    let _ = fs::remove_file(&eph_shm);
    let _ = fs::remove_dir_all(&eph_dir);
    res
}

pub fn inspect_live_shm_readonly(shm_path: &Path) {
    if !shm_path.exists() {
        return;
    }
    if let Ok(ro_reg) = MappedShmRegion::open_existing_read_only(shm_path) {
        let arena = ro_reg.arena();
        let hb_age_ms = arena
            .heartbeat_age_ns(now_epoch_ns())
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
                }
            }
        }
        let _ = fs::remove_file(&live_shm);
    }

    #[test]
    fn test_f10_f11_async_parquet_l1_l4_writer_and_ring_pruning() {
        let tmp_dir = std::env::temp_dir().join("vortex_l3_ring_async_f10_f11");
        let _ = fs::remove_dir_all(&tmp_dir);
        let lake_dir = tmp_dir.join("lakehouse_par1");
        assert!(fs::create_dir_all(&lake_dir).is_ok());
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.sequence_id = 42;
        snap.realized_vol_5m = ScaledBps(7_300);
        for lvl in 0..4 {
            snap.bids[lvl] = BookLevel { price: PriceTicks(8_200 - lvl as i64), qty: LotQty(300) };
            snap.asks[lvl] =
                BookLevel { price: PriceTicks(8_201 + (lvl as i64) * 3), qty: LotQty(200) };
        }
        let mut batch = Box::<ColumnarParquetBatch>::default();
        let row = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::L2BookSnapshot,
            Side::Bid,
            PriceTicks(8_200),
            LotQty(300),
        );
        assert!(batch.push_row(&row).is_ok());
        {
            let writer = AsyncParquetRingWriter::spawn(tmp_dir.clone(), false);
            assert!(writer.try_flush_and_clear(&mut batch));
            assert!(batch.is_empty());
        }
        let seg1 = tmp_dir.join("vortex_l2_segment_000001.parquet");
        assert!(seg1.exists());
        assert!(ColumnarParquetBatch::read_from_file(&seg1).is_ok());
        prune_expired_parquet_segment(&tmp_dir, MAX_PARQUET_RING_FILES + 1);
        assert!(!seg1.exists());
        assert!(!lake_dir.exists());
        let _ = fs::remove_dir_all(&tmp_dir);
    }
}
