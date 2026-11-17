//! Perpetual Binary Enriched Candle Store (`VKL1`, 96 B/row + FNV-1a 64-bit, `1s..1d`, Zero-Unsafe).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_clamp,
    clippy::module_name_repetitions,
    clippy::too_many_arguments
)]

use crate::forensic_parquet::ColumnarParquetBatch;
use crate::lob_shm::{SymbolLobSnapshot, MAX_ACTIVE_PAIRS};
use crate::types::{LotQty, PriceTicks, TimestampNs, VortexError};

pub const VKL1_MAGIC: u32 = 0x314C_4B56; // Little-endian b"VKL1"
pub const VKL1_RECORD_BYTES: usize = 96;
pub const CANDLE_TF_COUNT: usize = 7;
pub const DEFAULT_CANDLE_DIR: &str = "data/candles";

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum CandleTimeframe {
    Sec1 = 0,
    #[default]
    Min1 = 1,
    Min5 = 2,
    Min15 = 3,
    Hour1 = 4,
    Hour4 = 5,
    Day1 = 6,
}

impl CandleTimeframe {
    pub const ALL: [Self; CANDLE_TF_COUNT] = [
        Self::Sec1, Self::Min1, Self::Min5, Self::Min15, Self::Hour1, Self::Hour4, Self::Day1,
    ];

    #[inline]
    #[must_use]
    pub const fn duration_ns(self) -> i64 {
        match self {
            Self::Sec1 => 1_000_000_000,
            Self::Min1 => 60_000_000_000,
            Self::Min5 => 300_000_000_000,
            Self::Min15 => 900_000_000_000,
            Self::Hour1 => 3_600_000_000_000,
            Self::Hour4 => 14_400_000_000_000,
            Self::Day1 => 86_400_000_000_000,
        }
    }

    #[inline]
    #[must_use]
    pub const fn bucket_open_ns(self, ts_ns: TimestampNs) -> i64 {
        if ts_ns.0 <= 0 { 0 } else { (ts_ns.0 / self.duration_ns()) * self.duration_ns() }
    }

    #[inline]
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sec1 => "1s", Self::Min1 => "1m", Self::Min5 => "5m", Self::Min15 => "15m",
            Self::Hour1 => "1h", Self::Hour4 => "4h", Self::Day1 => "1d",
        }
    }

    #[inline]
    #[must_use]
    pub const fn bybit_interval(self) -> &'static str {
        match self {
            Self::Sec1 | Self::Min1 => "1", Self::Min5 => "5", Self::Min15 => "15",
            Self::Hour1 => "60", Self::Hour4 => "240", Self::Day1 => "D",
        }
    }

    #[inline]
    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Sec1), 1 => Some(Self::Min1), 2 => Some(Self::Min5),
            3 => Some(Self::Min15), 4 => Some(Self::Hour1), 5 => Some(Self::Hour4),
            6 => Some(Self::Day1), _ => None,
        }
    }

    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        match label.trim() {
            "1s" | "1S" => Some(Self::Sec1), "1m" | "1M" | "1" => Some(Self::Min1),
            "5m" | "5M" | "5" => Some(Self::Min5), "15m" | "15M" | "15" => Some(Self::Min15),
            "1h" | "1H" | "60" => Some(Self::Hour1), "4h" | "4H" | "240" => Some(Self::Hour4),
            "1d" | "1D" | "D" => Some(Self::Day1), _ => None,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Vkl1CandleRecord {
    pub magic: u32,
    pub slot_idx: u8,
    pub tf_code: u8,
    pub wave_regime: u8,
    pub flags: u8,
    pub open_ts_ns: i64,
    pub open_ticks: i64,
    pub high_ticks: i64,
    pub low_ticks: i64,
    pub close_ticks: i64,
    pub volume_lots: i64,
    pub open_interest_x100: i64,
    pub trade_count: u32,
    pub ofi_mean_pm: i16,
    pub tfi_mean_pm: i16,
    pub geom_mean_pm: i16,
    pub funding_sbps: i16,
    pub hurst_pm: u16,
    pub ob_bull_pm: u16,
    pub ob_bear_pm: u16,
    pub spoof_vetoes: u16,
    pub liq_buy_lots_c: u16,
    pub liq_sell_lots_c: u16,
    pub fnv1a64: u64,
}

const _: [(); VKL1_RECORD_BYTES] = [(); core::mem::size_of::<Vkl1CandleRecord>()];

#[inline] fn put_i64(b: &mut [u8; 96], o: usize, v: i64) { b[o..o + 8].copy_from_slice(&v.to_le_bytes()); }
#[inline] fn put_i16(b: &mut [u8; 96], o: usize, v: i16) { b[o..o + 2].copy_from_slice(&v.to_le_bytes()); }
#[inline] fn put_u16(b: &mut [u8; 96], o: usize, v: u16) { b[o..o + 2].copy_from_slice(&v.to_le_bytes()); }
#[inline] const fn get_i64(b: &[u8; 96], o: usize) -> i64 { i64::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3], b[o + 4], b[o + 5], b[o + 6], b[o + 7]]) }
#[inline] const fn get_i16(b: &[u8; 96], o: usize) -> i16 { i16::from_le_bytes([b[o], b[o + 1]]) }
#[inline] const fn get_u16(b: &[u8; 96], o: usize) -> u16 { u16::from_le_bytes([b[o], b[o + 1]]) }

impl Vkl1CandleRecord {
    #[inline] #[must_use] pub const fn open_ts(&self) -> TimestampNs { TimestampNs(self.open_ts_ns) }
    #[inline] #[must_use] pub const fn open_price(&self) -> PriceTicks { PriceTicks(self.open_ticks) }
    #[inline] #[must_use] pub const fn high_price(&self) -> PriceTicks { PriceTicks(self.high_ticks) }
    #[inline] #[must_use] pub const fn low_price(&self) -> PriceTicks { PriceTicks(self.low_ticks) }
    #[inline] #[must_use] pub const fn close_price(&self) -> PriceTicks { PriceTicks(self.close_ticks) }
    #[inline] #[must_use] pub const fn volume(&self) -> LotQty { LotQty(self.volume_lots) }

    fn encode_prefix_88(&self, out: &mut [u8; VKL1_RECORD_BYTES]) {
        out[0..4].copy_from_slice(&VKL1_MAGIC.to_le_bytes());
        out[4] = self.slot_idx; out[5] = self.tf_code; out[6] = self.wave_regime; out[7] = self.flags;
        put_i64(out, 8, self.open_ts_ns); put_i64(out, 16, self.open_ticks);
        put_i64(out, 24, self.high_ticks); put_i64(out, 32, self.low_ticks);
        put_i64(out, 40, self.close_ticks); put_i64(out, 48, self.volume_lots);
        put_i64(out, 56, self.open_interest_x100);
        out[64..68].copy_from_slice(&self.trade_count.to_le_bytes());
        put_i16(out, 68, self.ofi_mean_pm); put_i16(out, 70, self.tfi_mean_pm);
        put_i16(out, 72, self.geom_mean_pm); put_i16(out, 74, self.funding_sbps);
        put_u16(out, 76, self.hurst_pm); put_u16(out, 78, self.ob_bull_pm);
        put_u16(out, 80, self.ob_bear_pm); put_u16(out, 82, self.spoof_vetoes);
        put_u16(out, 84, self.liq_buy_lots_c); put_u16(out, 86, self.liq_sell_lots_c);
    }

    #[must_use]
    pub fn seal_with_checksum(mut self) -> Self {
        self.magic = VKL1_MAGIC;
        let mut buf = [0_u8; VKL1_RECORD_BYTES];
        self.encode_prefix_88(&mut buf);
        self.fnv1a64 = ColumnarParquetBatch::compute_fnv1a64(&buf[..88]);
        self
    }

    #[must_use]
    pub fn to_bytes_96(&self) -> [u8; VKL1_RECORD_BYTES] {
        let mut buf = [0_u8; VKL1_RECORD_BYTES];
        self.encode_prefix_88(&mut buf);
        let crc = if self.fnv1a64 != 0 { self.fnv1a64 } else { ColumnarParquetBatch::compute_fnv1a64(&buf[..88]) };
        buf[88..96].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decodes and verifies a 96-byte `VKL1` record against magic header & FNV-1a 64-bit checksum.
    ///
    /// # Errors
    /// Returns `VortexError::ColumnarFormatCorrupted` on magic or FNV-1a mismatch.
    pub fn from_bytes_96(b: &[u8; VKL1_RECORD_BYTES]) -> Result<Self, VortexError> {
        let magic = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        let stored_crc = u64::from_le_bytes([b[88], b[89], b[90], b[91], b[92], b[93], b[94], b[95]]);
        if magic != VKL1_MAGIC || CandleTimeframe::from_code(b[5]).is_none() {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        if ColumnarParquetBatch::compute_fnv1a64(&b[..88]) != stored_crc {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        Ok(Self {
            magic, slot_idx: b[4], tf_code: b[5], wave_regime: b[6], flags: b[7],
            open_ts_ns: get_i64(b, 8), open_ticks: get_i64(b, 16), high_ticks: get_i64(b, 24),
            low_ticks: get_i64(b, 32), close_ticks: get_i64(b, 40), volume_lots: get_i64(b, 48),
            open_interest_x100: get_i64(b, 56),
            trade_count: u32::from_le_bytes([b[64], b[65], b[66], b[67]]),
            ofi_mean_pm: get_i16(b, 68), tfi_mean_pm: get_i16(b, 70), geom_mean_pm: get_i16(b, 72),
            funding_sbps: get_i16(b, 74), hurst_pm: get_u16(b, 76), ob_bull_pm: get_u16(b, 78),
            ob_bear_pm: get_u16(b, 80), spoof_vetoes: get_u16(b, 82),
            liq_buy_lots_c: get_u16(b, 84), liq_sell_lots_c: get_u16(b, 86), fnv1a64: stored_crc,
        })
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct LiveCandleAccumulator {
    pub has_open: bool,
    pub open_ts_ns: i64,
    pub open_ticks: i64,
    pub high_ticks: i64,
    pub low_ticks: i64,
    pub close_ticks: i64,
    pub vol_lots: i64,
    pub oi_x100: i64,
    pub trade_count: u32,
    pub obs_count: u32,
    pub ofi_sum_pm: i64,
    pub tfi_sum_pm: i64,
    pub geom_sum_pm: i64,
    pub funding_sbps: i16,
    pub hurst_pm: u16,
    pub ob_bull_pm: u16,
    pub ob_bear_pm: u16,
    pub spoof_vetoes: u16,
    pub liq_buy_c: u16,
    pub liq_sell_c: u16,
    pub wave_regime: u8,
    pub flags: u8,
}

impl LiveCandleAccumulator {
    #[must_use]
    pub fn to_record(&self, slot_idx: u8, tf: CandleTimeframe) -> Option<Vkl1CandleRecord> {
        if !self.has_open || self.open_ticks <= 0 { return None; }
        let n = i64::from(self.obs_count.max(1));
        Some(Vkl1CandleRecord {
            magic: VKL1_MAGIC, slot_idx, tf_code: tf as u8, wave_regime: self.wave_regime, flags: self.flags,
            open_ts_ns: self.open_ts_ns, open_ticks: self.open_ticks, high_ticks: self.high_ticks,
            low_ticks: self.low_ticks, close_ticks: self.close_ticks, volume_lots: self.vol_lots,
            open_interest_x100: self.oi_x100, trade_count: self.trade_count,
            ofi_mean_pm: (self.ofi_sum_pm / n).clamp(-1_000, 1_000) as i16,
            tfi_mean_pm: (self.tfi_sum_pm / n).clamp(-1_000, 1_000) as i16,
            geom_mean_pm: (self.geom_sum_pm / n).clamp(-1_000, 1_000) as i16,
            funding_sbps: self.funding_sbps, hurst_pm: self.hurst_pm,
            ob_bull_pm: self.ob_bull_pm, ob_bear_pm: self.ob_bear_pm, spoof_vetoes: self.spoof_vetoes,
            liq_buy_lots_c: self.liq_buy_c, liq_sell_lots_c: self.liq_sell_c, fnv1a64: 0,
        }.seal_with_checksum())
    }

    pub fn step_snapshot(
        &mut self, slot_idx: u8, tf: CandleTimeframe, snap: &SymbolLobSnapshot,
        trade_lots_delta: LotQty, is_spoof_veto: bool, liq_buy_lots: u16, liq_sell_lots: u16,
    ) -> Option<Vkl1CandleRecord> {
        let px = if snap.last_trade_price.0 > 0 { snap.last_trade_price.0 } else if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 { i64::midpoint(snap.bids[0].price.0, snap.asks[0].price.0) } else { snap.mark_price.0 };
        if px <= 0 || snap.ts_local_ns.0 <= 0 { return None; }
        let bucket = tf.bucket_open_ns(snap.ts_local_ns);
        let closed = if self.has_open && bucket > self.open_ts_ns {
            let out = self.to_record(slot_idx, tf);
            *self = Self::default();
            out
        } else {
            None
        };
        if !self.has_open {
            self.has_open = true; self.open_ts_ns = bucket;
            self.open_ticks = px; self.high_ticks = px; self.low_ticks = px;
        } else if bucket < self.open_ts_ns {
            return None;
        }
        if px > self.high_ticks { self.high_ticks = px; }
        if px < self.low_ticks { self.low_ticks = px; }
        self.close_ticks = px;
        if trade_lots_delta.0 > 0 {
            self.vol_lots = self.vol_lots.saturating_add(trade_lots_delta.0);
            self.trade_count = self.trade_count.saturating_add(1);
        }
        if snap.open_interest_x100 > 0 { self.oi_x100 = snap.open_interest_x100; }
        self.obs_count = self.obs_count.saturating_add(1);
        self.ofi_sum_pm = self.ofi_sum_pm.saturating_add((snap.factor_ofi.get() * 1_000.0).round() as i64);
        self.tfi_sum_pm = self.tfi_sum_pm.saturating_add((snap.factor_tfi.get() * 1_000.0).round() as i64);
        self.geom_sum_pm = self.geom_sum_pm.saturating_add((snap.factor_geom.get() * 1_000.0).round() as i64);
        self.funding_sbps = snap.funding_rate_scaled_bps.raw().clamp(-30_000, 30_000) as i16;
        let w = snap.wave_observables();
        self.hurst_pm = ((w.hurst_exponent() * 1_000.0).round() as i64).clamp(250, 850) as u16;
        self.ob_bull_pm = u16::try_from(if snap.ob_bull_efficacy_pm > 0 { snap.ob_bull_efficacy_pm.clamp(382, 854) } else { 618 }).unwrap_or(618);
        self.ob_bear_pm = u16::try_from(if snap.ob_bear_efficacy_pm > 0 { snap.ob_bear_efficacy_pm.clamp(382, 854) } else { 618 }).unwrap_or(618);
        if is_spoof_veto { self.spoof_vetoes = self.spoof_vetoes.saturating_add(1); }
        self.liq_buy_c = self.liq_buy_c.saturating_add(liq_buy_lots);
        self.liq_sell_c = self.liq_sell_c.saturating_add(liq_sell_lots);
        self.wave_regime = w.regime as u8;
        self.flags |= u8::from(w.climax_active);
        closed
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct SlotMultiTfAggregator {
    pub tf_bars: [LiveCandleAccumulator; CANDLE_TF_COUNT],
}

impl SlotMultiTfAggregator {
    #[inline] #[must_use] pub const fn new() -> Self { Self { tf_bars: [LiveCandleAccumulator { has_open: false, open_ts_ns: 0, open_ticks: 0, high_ticks: 0, low_ticks: 0, close_ticks: 0, vol_lots: 0, oi_x100: 0, trade_count: 0, obs_count: 0, ofi_sum_pm: 0, tfi_sum_pm: 0, geom_sum_pm: 0, funding_sbps: 0, hurst_pm: 500, ob_bull_pm: 618, ob_bear_pm: 618, spoof_vetoes: 0, liq_buy_c: 0, liq_sell_c: 0, wave_regime: 0, flags: 0 }; CANDLE_TF_COUNT] } }
    #[inline] #[must_use] pub const fn new_12_slot_array() -> [Self; MAX_ACTIVE_PAIRS] { [Self::new(); MAX_ACTIVE_PAIRS] }

    pub fn ingest_snapshot(
        &mut self, slot_idx: u8, snap: &SymbolLobSnapshot, trade_lots_delta: LotQty,
        is_spoof_veto: bool, liq_buy_lots: u16, liq_sell_lots: u16,
    ) -> [Option<Vkl1CandleRecord>; CANDLE_TF_COUNT] {
        core::array::from_fn(|idx| {
            self.tf_bars[idx].step_snapshot(
                slot_idx, CandleTimeframe::ALL[idx], snap, trade_lots_delta,
                is_spoof_veto, liq_buy_lots, liq_sell_lots,
            )
        })
    }

    #[must_use]
    pub fn active_candle(&self, slot_idx: u8, tf: CandleTimeframe) -> Option<Vkl1CandleRecord> {
        self.tf_bars[tf as usize].to_record(slot_idx, tf)
    }
}

use core::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::types::APPROVED_SPECS;

pub const VKL1_FLAG_REST_BACKFILL: u8 = 0x80;

#[derive(Clone, Debug)]
pub struct PerpetualCandleStore {
    base_dir: PathBuf,
}

#[allow(clippy::cast_possible_wrap)]
impl PerpetualCandleStore {
    #[inline]
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self { base_dir: base_dir.into() }
    }

    #[inline]
    #[must_use]
    pub fn default_store() -> Self {
        Self::new(DEFAULT_CANDLE_DIR)
    }

    #[inline]
    #[must_use]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    #[must_use]
    pub fn file_path(&self, slot_idx: usize, tf: CandleTimeframe) -> PathBuf {
        let sym = APPROVED_SPECS[slot_idx.min(MAX_ACTIVE_PAIRS - 1)].symbol_id.name();
        let tf_str = tf.as_str();
        self.base_dir.join(format!("{sym}_{tf_str}.vkl1"))
    }

    fn read_at(file: &mut File, idx: u64) -> Result<Vkl1CandleRecord, VortexError> {
        file.seek(SeekFrom::Start(idx.saturating_mul(VKL1_RECORD_BYTES as u64)))
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let mut buf = [0_u8; VKL1_RECORD_BYTES];
        file.read_exact(&mut buf).map_err(|_| VortexError::ColumnarIoFailure)?;
        Vkl1CandleRecord::from_bytes_96(&buf)
    }

    #[must_use]
    pub fn record_count(&self, slot_idx: usize, tf: CandleTimeframe) -> u64 {
        fs::metadata(self.file_path(slot_idx, tf))
            .map_or(0, |m| m.len() / (VKL1_RECORD_BYTES as u64))
    }

    /// Appends a chronological `VKL1` candle or updates the trailing bucket in place (`O(1)`).
    ///
    /// # Errors
    /// Returns `VortexError` on I/O failure or trailing record checksum corruption.
    pub fn append_or_update(&self, rec: &Vkl1CandleRecord) -> Result<(), VortexError> {
        let Some(tf) = CandleTimeframe::from_code(rec.tf_code) else {
            return Err(VortexError::ColumnarFormatCorrupted);
        };
        fs::create_dir_all(&self.base_dir).map_err(|_| VortexError::ColumnarIoFailure)?;
        let path = self.file_path(usize::from(rec.slot_idx), tf);
        let mut file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(false)
            .open(&path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let raw_len = file.metadata().map_err(|_| VortexError::ColumnarIoFailure)?.len();
        let aligned = (raw_len / (VKL1_RECORD_BYTES as u64)) * (VKL1_RECORD_BYTES as u64);
        if raw_len != aligned {
            file.set_len(aligned).map_err(|_| VortexError::ColumnarIoFailure)?;
        }
        let sealed = (*rec).seal_with_checksum().to_bytes_96();
        let count = aligned / (VKL1_RECORD_BYTES as u64);
        if count > 0 {
            let last = Self::read_at(&mut file, count - 1)?;
            match rec.open_ts_ns.cmp(&last.open_ts_ns) {
                Ordering::Less => return self.merge_sorted_records(usize::from(rec.slot_idx), tf, &[*rec]).map(|_| ()),
                Ordering::Equal => {
                    file.seek(SeekFrom::Start(aligned - (VKL1_RECORD_BYTES as u64)))
                        .map_err(|_| VortexError::ColumnarIoFailure)?;
                    return file.write_all(&sealed).map_err(|_| VortexError::ColumnarIoFailure);
                }
                Ordering::Greater => {}
            }
        }
        file.seek(SeekFrom::Start(aligned)).map_err(|_| VortexError::ColumnarIoFailure)?;
        file.write_all(&sealed).map_err(|_| VortexError::ColumnarIoFailure)
    }

    /// Atomically merges sorted/backfilled candles via `.tmp -> sync_all -> rename` (Law IV).
    ///
    /// # Errors
    /// Returns `VortexError` if reading existing records or atomic replacement fails.
    pub fn merge_sorted_records(
        &self, slot_idx: usize, tf: CandleTimeframe, incoming: &[Vkl1CandleRecord],
    ) -> Result<usize, VortexError> {
        if incoming.is_empty() { return Ok(0); }
        fs::create_dir_all(&self.base_dir).map_err(|_| VortexError::ColumnarIoFailure)?;
        let mut all = self.query_range(slot_idx, tf, 0, i64::MAX, usize::MAX).unwrap_or_default();
        all.extend_from_slice(incoming);
        all.sort_by(|a, b| {
            a.open_ts_ns.cmp(&b.open_ts_ns).then_with(|| {
                (a.flags & VKL1_FLAG_REST_BACKFILL).cmp(&(b.flags & VKL1_FLAG_REST_BACKFILL)).reverse()
            })
        });
        let mut dedup: Vec<Vkl1CandleRecord> = Vec::with_capacity(all.len());
        for r in all {
            if let Some(prev) = dedup.last_mut() {
                if prev.open_ts_ns == r.open_ts_ns {
                    *prev = r.seal_with_checksum();
                    continue;
                }
            }
            dedup.push(r.seal_with_checksum());
        }
        let path = self.file_path(slot_idx, tf);
        let tmp = path.with_extension("vkl1.tmp");
        {
            let mut f = File::create(&tmp).map_err(|_| VortexError::ColumnarIoFailure)?;
            for r in &dedup {
                f.write_all(&r.to_bytes_96()).map_err(|_| VortexError::ColumnarIoFailure)?;
            }
            f.sync_all().map_err(|_| VortexError::ColumnarIoFailure)?;
        }
        fs::rename(&tmp, &path).map_err(|_| VortexError::ColumnarIoFailure)?;
        Ok(dedup.len())
    }

    /// `O(log N)` disk binary-search range query over `[start_ts_ns, end_ts_ns]`.
    ///
    /// # Errors
    /// Returns `VortexError` on I/O error or corrupted `FNV-1a` frame.
    pub fn query_range(
        &self, slot_idx: usize, tf: CandleTimeframe, start_ts_ns: i64, end_ts_ns: i64, max_out: usize,
    ) -> Result<Vec<Vkl1CandleRecord>, VortexError> {
        let path = self.file_path(slot_idx, tf);
        let Ok(mut file) = File::open(&path) else { return Ok(Vec::new()) };
        let count = file.metadata().map_err(|_| VortexError::ColumnarIoFailure)?.len() / (VKL1_RECORD_BYTES as u64);
        if count == 0 || max_out == 0 || end_ts_ns < start_ts_ns { return Ok(Vec::new()); }
        let (mut lo, mut hi) = (0_u64, count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if Self::read_at(&mut file, mid)?.open_ts_ns < start_ts_ns { lo = mid + 1; } else { hi = mid; }
        }
        let cap = usize::try_from(count.saturating_sub(lo)).unwrap_or(max_out).min(max_out).min(16_384);
        let mut out = Vec::with_capacity(cap);
        let mut idx = lo;
        while idx < count && out.len() < max_out {
            let rec = Self::read_at(&mut file, idx)?;
            if rec.open_ts_ns > end_ts_ns { break; }
            out.push(rec);
            idx += 1;
        }
        Ok(out)
    }

    /// Reads the latest `limit` candles in `O(limit)` via direct tail seek.
    ///
    /// # Errors
    /// Returns `VortexError` on I/O failure or checksum mismatch.
    pub fn read_tail(
        &self, slot_idx: usize, tf: CandleTimeframe, limit: usize,
    ) -> Result<Vec<Vkl1CandleRecord>, VortexError> {
        let path = self.file_path(slot_idx, tf);
        let Ok(mut file) = File::open(&path) else { return Ok(Vec::new()) };
        let count = file.metadata().map_err(|_| VortexError::ColumnarIoFailure)?.len() / (VKL1_RECORD_BYTES as u64);
        let take = (limit as u64).min(count);
        let start = count.saturating_sub(take);
        let mut out = Vec::with_capacity(usize::try_from(take).unwrap_or(0));
        for idx in start..count {
            out.push(Self::read_at(&mut file, idx)?);
        }
        Ok(out)
    }

    /// Hydrates `.vkl1` history from Bybit `/v5/market/kline` JSON without overwriting live microstructure.
    ///
    /// # Errors
    /// Returns `VortexError` if disk merge fails.
    pub fn backfill_from_bybit_kline_json(
        &self, slot_idx: usize, tf: CandleTimeframe, payload: &serde_json::Value,
    ) -> Result<usize, VortexError> {
        let s_idx = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_idx];
        let Some(rows) = payload.get("result").and_then(|r| r.get("list")).and_then(|l| l.as_array()) else {
            return Ok(0);
        };
        let mut parsed = Vec::with_capacity(rows.len());
        for row in rows.iter().rev() {
            let Some(arr) = row.as_array() else { continue };
            if arr.len() < 6 { continue; }
            let ts_ms = arr[0].as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let (Ok(op), Ok(hi), Ok(lo), Ok(cl)) = (
                spec.parse_price_ticks(arr[1].as_str().unwrap_or("")),
                spec.parse_price_ticks(arr[2].as_str().unwrap_or("")),
                spec.parse_price_ticks(arr[3].as_str().unwrap_or("")),
                spec.parse_price_ticks(arr[4].as_str().unwrap_or("")),
            ) else { continue };
            let vol_f = arr[5].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            let lots = ((vol_f.max(0.0) * 100_000_000.0) / (spec.lot_step_1e8.max(1) as f64)).round() as i64;
            if ts_ms <= 0 || op.0 <= 0 { continue; }
            parsed.push(Vkl1CandleRecord {
                magic: VKL1_MAGIC, slot_idx: s_idx as u8, tf_code: tf as u8, wave_regime: 0,
                flags: VKL1_FLAG_REST_BACKFILL, open_ts_ns: tf.bucket_open_ns(TimestampNs(ts_ms.saturating_mul(1_000_000))),
                open_ticks: op.0, high_ticks: hi.0, low_ticks: lo.0, close_ticks: cl.0,
                volume_lots: lots, open_interest_x100: 0, trade_count: 0, ofi_mean_pm: 0,
                tfi_mean_pm: 0, geom_mean_pm: 0, funding_sbps: 0, hurst_pm: 500,
                ob_bull_pm: 618, ob_bear_pm: 618, spoof_vetoes: 0, liq_buy_lots_c: 0,
                liq_sell_lots_c: 0, fnv1a64: 0,
            }.seal_with_checksum());
        }
        self.merge_sorted_records(s_idx, tf, &parsed)
    }

    /// Exports local `.vkl1` tail as a Bybit-compatible JSON bundle for zero-REST prewarm.
    ///
    /// # Errors
    /// Returns `VortexError` if reading local `.vkl1` tail fails.
    pub fn export_bybit_kline_bundle(
        &self, slot_idx: usize, tf: CandleTimeframe, limit: usize,
    ) -> Result<serde_json::Value, VortexError> {
        let s_idx = slot_idx.min(MAX_ACTIVE_PAIRS - 1);
        let spec = APPROVED_SPECS[s_idx];
        let tail = self.read_tail(s_idx, tf, limit)?;
        let fmt_px = |ticks: i64| -> String {
            let v1e8 = i128::from(ticks) * i128::from(spec.tick_size_1e8);
            format!("{}.{:08}", v1e8 / 100_000_000, (v1e8 % 100_000_000).abs())
        };
        let mut list = Vec::with_capacity(tail.len());
        for r in tail.iter().rev() {
            let vol_coins = (r.volume_lots as f64) * (spec.lot_step_1e8 as f64) / 100_000_000.0;
            let turn_usdt = vol_coins * (r.close_ticks as f64) * (spec.tick_size_1e8 as f64) / 100_000_000.0;
            list.push(serde_json::json!([
                (r.open_ts_ns / 1_000_000).to_string(),
                fmt_px(r.open_ticks), fmt_px(r.high_ticks), fmt_px(r.low_ticks), fmt_px(r.close_ticks),
                format!("{vol_coins:.4}"), format!("{turn_usdt:.2}")
            ]));
        }
        Ok(serde_json::json!({ "result": { "list": list } }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_block::CausalOrderBlockEngine;
    use crate::types::{BoundedScore, ScaledBps};

    #[test]
    fn test_vkl1_96b_fnv_aggregator_binary_search_and_local_prewarm() {
        let dir = std::env::temp_dir().join(format!("vortex_vkl1_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = PerpetualCandleStore::new(&dir);

        let k5 = serde_json::json!({ "result": { "list": [
            ["1700003000000", "66380.0", "66390.0", "66100.0", "66120.0", "50", "3500000"],
            ["1700002700000", "66580.0", "66585.0", "66350.0", "66380.0", "60", "4200000"],
            ["1700002400000", "66420.0", "66620.0", "66410.0", "66580.0", "45", "3100000"],
            ["1700002100000", "66430.0", "66450.0", "66400.0", "66420.0", "15", "1000000"],
            ["1700001800000", "66420.0", "66445.0", "66395.0", "66430.0", "15", "1000000"],
            ["1700001500000", "66425.0", "66450.0", "66400.0", "66420.0", "15", "1000000"],
            ["1700001200000", "66420.0", "66445.0", "66395.0", "66425.0", "15", "1000000"],
            ["1700000900000", "66415.0", "66440.0", "66390.0", "66420.0", "15", "1000000"]
        ]}} );
        assert_eq!(store.backfill_from_bybit_kline_json(0, CandleTimeframe::Min5, &k5), Ok(8));
        assert_eq!(store.record_count(0, CandleTimeframe::Min5), 8);

        let sub = store.query_range(0, CandleTimeframe::Min5, 1_700_001_600_000_000_000, 1_700_002_500_000_000_000, 10);
        assert_eq!(sub.map_or(0, |v| v.len()), 4);

        // Live snapshot overwrites REST backfill at same bucket & preserves live OFI/TFI.
        let mut agg = SlotMultiTfAggregator::new();
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.ts_local_ns = TimestampNs(1_700_003_010_000_000_000);
        snap.last_trade_price = PriceTicks(661_500);
        snap.factor_ofi = BoundedScore::new_clamped(0.75);
        snap.factor_tfi = BoundedScore::new_clamped(0.82);
        snap.funding_rate_scaled_bps = ScaledBps(100);
        let _ = agg.ingest_snapshot(0, &snap, LotQty(25), true, 4, 0);
        if let Some(live_bar) = agg.active_candle(0, CandleTimeframe::Min5) {
            let mut b96 = live_bar.to_bytes_96();
            assert!(Vkl1CandleRecord::from_bytes_96(&b96).is_ok());
            b96[20] ^= 0xFF;
            assert!(Vkl1CandleRecord::from_bytes_96(&b96).is_err());
            assert!(store.append_or_update(&live_bar).is_ok());
        }
        assert_eq!(store.record_count(0, CandleTimeframe::Min5), 8);
        let tail = store.read_tail(0, CandleTimeframe::Min5, 1).unwrap_or_default();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].ofi_mean_pm, 750);
        assert_eq!(tail[0].flags & VKL1_FLAG_REST_BACKFILL, 0);

        // Local prewarm export hydrates CausalOrderBlockEngine with 0 network calls.
        let bundle = store.export_bybit_kline_bundle(0, CandleTimeframe::Min5, 8);
        assert!(bundle.is_ok());
        if let Ok(b_json) = bundle {
            let mut ob_eng = CausalOrderBlockEngine::new();
            assert_eq!(ob_eng.warm_start_from_bybit_bundle(APPROVED_SPECS[0], Some(&b_json), None, None), 8);
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
