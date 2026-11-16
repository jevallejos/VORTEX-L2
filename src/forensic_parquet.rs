//! Columnar PAR2 Lossless L1..L5 + Ticker/Liq Recorder (PAR1 Retro-Compat) & Dynamic VIP ParitySyncLedger.
#![allow(clippy::manual_clamp, clippy::needless_range_loop, clippy::too_many_arguments)]

use core::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs, VortexError,
};

pub const COLUMNAR_BATCH_CAP: usize = 256;
pub const DEEP_BOOK_LEVELS: usize = 3;
pub const PAR2_DEEP_BOOK_LEVELS: usize = 4;
pub const PARQUET_MAGIC_HEADER: [u8; 4] = *b"PAR1";
pub const PAR2_MAGIC_HEADER: [u8; 4] = *b"PAR2";
pub const PAR1_LEGACY_V1_ROW_BYTES: usize = 96;
pub const PAR1_L3_ROW_BYTES: usize = 200;
pub const PAR2_LOSSLESS_ROW_BYTES: usize = 256;
pub const MAX_SINKING_FUND_SCALED_BPS: i64 = 25_000;

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MarketEventKind {
    L2BookSnapshot = 1,
    PublicTradePrint = 2,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Par2EventKind {
    #[default]
    L2BookSnapshot = 1,
    PublicTradePrint = 2,
    TickerMeta = 3,
    LiquidationPrint = 4,
}

impl Par2EventKind {
    #[inline]
    #[must_use]
    pub const fn from_wire_byte(b: u8) -> Self {
        match b {
            2 => Self::PublicTradePrint,
            3 => Self::TickerMeta,
            4 => Self::LiquidationPrint,
            _ => Self::L2BookSnapshot,
        }
    }

    #[inline]
    #[must_use]
    pub const fn legacy_event_kind(self) -> MarketEventKind {
        match self {
            Self::PublicTradePrint => MarketEventKind::PublicTradePrint,
            Self::L2BookSnapshot | Self::TickerMeta | Self::LiquidationPrint => {
                MarketEventKind::L2BookSnapshot
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ColumnarMarketRow {
    pub symbol_id: SymbolId,
    pub event_kind: MarketEventKind,
    pub par2_kind: Par2EventKind,
    pub side: Side,
    pub sequence_id: u64,
    pub ts_exchange_ns: TimestampNs,
    pub ts_local_ns: TimestampNs,
    pub price_ticks: PriceTicks,
    pub lot_qty: LotQty,
    pub best_bid_ticks: PriceTicks,
    pub best_ask_ticks: PriceTicks,
    pub best_bid_qty: LotQty,
    pub best_ask_qty: LotQty,
    pub mark_price_ticks: PriceTicks,
    pub realized_vol_5m: ScaledBps,
    pub open_interest_x100: i64,
    pub funding_rate_scaled_bps: ScaledBps,
    pub next_funding_ts_ns: TimestampNs,
    pub deep_bid_ticks: [PriceTicks; PAR2_DEEP_BOOK_LEVELS],
    pub deep_bid_qty: [LotQty; PAR2_DEEP_BOOK_LEVELS],
    pub deep_ask_ticks: [PriceTicks; PAR2_DEEP_BOOK_LEVELS],
    pub deep_ask_qty: [LotQty; PAR2_DEEP_BOOK_LEVELS],
    pub ofi_bps: i32,
    pub tfi_bps: i32,
    pub geom_bps: i32,
}

impl ColumnarMarketRow {
    #[inline]
    #[must_use]
    pub fn from_lob_snapshot(
        snap: &SymbolLobSnapshot,
        event_kind: MarketEventKind,
        side: Side,
        price_ticks: PriceTicks,
        lot_qty: LotQty,
    ) -> Self {
        let par2_kind = match event_kind {
            MarketEventKind::L2BookSnapshot => Par2EventKind::L2BookSnapshot,
            MarketEventKind::PublicTradePrint => Par2EventKind::PublicTradePrint,
        };
        Self {
            symbol_id: snap.symbol_id,
            event_kind,
            par2_kind,
            side,
            sequence_id: snap.sequence_id,
            ts_exchange_ns: snap.ts_exchange_ns,
            ts_local_ns: snap.ts_local_ns,
            price_ticks,
            lot_qty,
            best_bid_ticks: snap.bids[0].price,
            best_ask_ticks: snap.asks[0].price,
            best_bid_qty: snap.bids[0].qty,
            best_ask_qty: snap.asks[0].qty,
            mark_price_ticks: snap.mark_price,
            realized_vol_5m: snap.realized_vol_5m,
            open_interest_x100: 0,
            funding_rate_scaled_bps: ScaledBps(0),
            next_funding_ts_ns: TimestampNs(0),
            deep_bid_ticks: [
                snap.bids[1].price,
                snap.bids[2].price,
                snap.bids[3].price,
                snap.bids[4].price,
            ],
            deep_bid_qty: [
                snap.bids[1].qty,
                snap.bids[2].qty,
                snap.bids[3].qty,
                snap.bids[4].qty,
            ],
            deep_ask_ticks: [
                snap.asks[1].price,
                snap.asks[2].price,
                snap.asks[3].price,
                snap.asks[4].price,
            ],
            deep_ask_qty: [
                snap.asks[1].qty,
                snap.asks[2].qty,
                snap.asks[3].qty,
                snap.asks[4].qty,
            ],
            ofi_bps: snap.factor_ofi.to_bps_i32(),
            tfi_bps: snap.factor_tfi.to_bps_i32(),
            geom_bps: snap.factor_geom.to_bps_i32(),
        }
    }

    #[inline]
    #[must_use]
    pub fn from_ticker_meta(
        snap: &SymbolLobSnapshot,
        mark_price_ticks: PriceTicks,
        open_interest_x100: i64,
        funding_rate_scaled_bps: ScaledBps,
        next_funding_ts_ns: TimestampNs,
    ) -> Self {
        let eff_mark = if mark_price_ticks.0 > 0 {
            mark_price_ticks
        } else {
            snap.mark_price
        };
        let mut row = Self::from_lob_snapshot(
            snap,
            MarketEventKind::L2BookSnapshot,
            Side::Bid,
            eff_mark,
            LotQty(0),
        );
        row.par2_kind = Par2EventKind::TickerMeta;
        row.mark_price_ticks = eff_mark;
        row.open_interest_x100 = open_interest_x100.max(0);
        row.funding_rate_scaled_bps = funding_rate_scaled_bps;
        row.next_funding_ts_ns = next_funding_ts_ns;
        row
    }

    #[inline]
    #[must_use]
    pub fn from_liquidation_print(
        snap: &SymbolLobSnapshot,
        forced_book_side: Side,
        liq_price_ticks: PriceTicks,
        liq_lot_qty: LotQty,
        open_interest_x100: i64,
    ) -> Self {
        let mut row = Self::from_lob_snapshot(
            snap,
            MarketEventKind::L2BookSnapshot,
            forced_book_side,
            liq_price_ticks,
            liq_lot_qty,
        );
        row.par2_kind = Par2EventKind::LiquidationPrint;
        row.open_interest_x100 = open_interest_x100.max(0);
        row
    }

    #[inline]
    pub fn apply_to_lob_snapshot(&self, snap: &mut SymbolLobSnapshot) {
        snap.symbol_id = self.symbol_id;
        snap.sequence_id = self.sequence_id;
        snap.ts_exchange_ns = self.ts_exchange_ns;
        snap.ts_local_ns = self.ts_local_ns;
        if self.mark_price_ticks.0 > 0
            && (self.par2_kind != Par2EventKind::LiquidationPrint || snap.mark_price.0 <= 0)
        {
            snap.mark_price = self.mark_price_ticks;
        }
        if self.realized_vol_5m.0 > 0 {
            snap.realized_vol_5m = self.realized_vol_5m;
        }
        if self.best_bid_ticks.0 > 0 && self.best_ask_ticks.0 > 0 {
            snap.bids[0] = BookLevel {
                price: self.best_bid_ticks,
                qty: self.best_bid_qty,
            };
            snap.asks[0] = BookLevel {
                price: self.best_ask_ticks,
                qty: self.best_ask_qty,
            };
            for lvl in 0..PAR2_DEEP_BOOK_LEVELS {
                snap.bids[lvl + 1] = BookLevel {
                    price: self.deep_bid_ticks[lvl],
                    qty: self.deep_bid_qty[lvl],
                };
                snap.asks[lvl + 1] = BookLevel {
                    price: self.deep_ask_ticks[lvl],
                    qty: self.deep_ask_qty[lvl],
                };
            }
            snap.refresh_deep_l1_l5_totals();
        }
        match self.par2_kind {
            Par2EventKind::PublicTradePrint => {
                if self.price_ticks.0 > 0 {
                    snap.last_trade_price = self.price_ticks;
                }
                if self.lot_qty.0 > 0 {
                    match self.side {
                        Side::Bid => {
                            snap.cum_buy_trade_lots =
                                LotQty(snap.cum_buy_trade_lots.0.saturating_add(self.lot_qty.0));
                        }
                        Side::Ask => {
                            snap.cum_sell_trade_lots =
                                LotQty(snap.cum_sell_trade_lots.0.saturating_add(self.lot_qty.0));
                        }
                    }
                }
            }
            Par2EventKind::L2BookSnapshot => {
                if snap.last_trade_price.0 <= 0 {
                    if self.best_bid_ticks.0 > 0 && self.best_ask_ticks.0 > 0 {
                        snap.last_trade_price = PriceTicks(i64::midpoint(
                            self.best_bid_ticks.0,
                            self.best_ask_ticks.0,
                        ));
                    } else if self.price_ticks.0 > 0 {
                        snap.last_trade_price = self.price_ticks;
                    }
                }
            }
            Par2EventKind::TickerMeta => {
                if self.price_ticks.0 > 0 && self.mark_price_ticks.0 <= 0 {
                    snap.mark_price = self.price_ticks;
                }
            }
            Par2EventKind::LiquidationPrint => {}
        }
        snap.factor_ofi = BoundedScore::new_clamped((self.ofi_bps as f64) / 10_000.0);
        snap.factor_tfi = BoundedScore::new_clamped((self.tfi_bps as f64) / 10_000.0);
        snap.factor_geom = BoundedScore::new_clamped((self.geom_bps as f64) / 10_000.0);
    }
}

#[derive(Clone, Debug)]
pub struct ColumnarParquetBatch {
    len: usize,
    pub symbol_id: [u16; COLUMNAR_BATCH_CAP],
    pub par2_kind: [u8; COLUMNAR_BATCH_CAP],
    pub side: [u8; COLUMNAR_BATCH_CAP],
    pub sequence_id: [u64; COLUMNAR_BATCH_CAP],
    pub ts_exchange_ns: [i64; COLUMNAR_BATCH_CAP],
    pub ts_local_ns: [i64; COLUMNAR_BATCH_CAP],
    pub price_ticks: [i64; COLUMNAR_BATCH_CAP],
    pub lot_qty: [i64; COLUMNAR_BATCH_CAP],
    pub best_bid_ticks: [i64; COLUMNAR_BATCH_CAP],
    pub best_ask_ticks: [i64; COLUMNAR_BATCH_CAP],
    pub best_bid_qty: [i64; COLUMNAR_BATCH_CAP],
    pub best_ask_qty: [i64; COLUMNAR_BATCH_CAP],
    pub mark_price_ticks: [i64; COLUMNAR_BATCH_CAP],
    pub realized_vol_5m_bps: [i64; COLUMNAR_BATCH_CAP],
    pub open_interest_x100: [i64; COLUMNAR_BATCH_CAP],
    pub funding_rate_scaled_bps: [i64; COLUMNAR_BATCH_CAP],
    pub next_funding_ts_ns: [i64; COLUMNAR_BATCH_CAP],
    pub deep_bid_ticks: [[i64; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_bid_qty: [[i64; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_ask_ticks: [[i64; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_ask_qty: [[i64; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub ofi_bps: [i32; COLUMNAR_BATCH_CAP],
    pub tfi_bps: [i32; COLUMNAR_BATCH_CAP],
    pub geom_bps: [i32; COLUMNAR_BATCH_CAP],
}

impl Default for ColumnarParquetBatch {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarParquetBatch {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            len: 0,
            symbol_id: [0; COLUMNAR_BATCH_CAP],
            par2_kind: [0; COLUMNAR_BATCH_CAP],
            side: [0; COLUMNAR_BATCH_CAP],
            sequence_id: [0; COLUMNAR_BATCH_CAP],
            ts_exchange_ns: [0; COLUMNAR_BATCH_CAP],
            ts_local_ns: [0; COLUMNAR_BATCH_CAP],
            price_ticks: [0; COLUMNAR_BATCH_CAP],
            lot_qty: [0; COLUMNAR_BATCH_CAP],
            best_bid_ticks: [0; COLUMNAR_BATCH_CAP],
            best_ask_ticks: [0; COLUMNAR_BATCH_CAP],
            best_bid_qty: [0; COLUMNAR_BATCH_CAP],
            best_ask_qty: [0; COLUMNAR_BATCH_CAP],
            mark_price_ticks: [0; COLUMNAR_BATCH_CAP],
            realized_vol_5m_bps: [0; COLUMNAR_BATCH_CAP],
            open_interest_x100: [0; COLUMNAR_BATCH_CAP],
            funding_rate_scaled_bps: [0; COLUMNAR_BATCH_CAP],
            next_funding_ts_ns: [0; COLUMNAR_BATCH_CAP],
            deep_bid_ticks: [[0; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_bid_qty: [[0; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_ask_ticks: [[0; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_ask_qty: [[0; PAR2_DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            ofi_bps: [0; COLUMNAR_BATCH_CAP],
            tfi_bps: [0; COLUMNAR_BATCH_CAP],
            geom_bps: [0; COLUMNAR_BATCH_CAP],
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn push_row(&mut self, row: &ColumnarMarketRow) -> Result<(), VortexError> {
        if self.len >= COLUMNAR_BATCH_CAP {
            return Err(VortexError::BatchCapacityExceeded);
        }
        let i = self.len;
        self.symbol_id[i] = row.symbol_id.0;
        self.par2_kind[i] = row.par2_kind as u8;
        self.side[i] = row.side as u8;
        self.sequence_id[i] = row.sequence_id;
        self.ts_exchange_ns[i] = row.ts_exchange_ns.0;
        self.ts_local_ns[i] = row.ts_local_ns.0;
        self.price_ticks[i] = row.price_ticks.0;
        self.lot_qty[i] = row.lot_qty.0;
        self.best_bid_ticks[i] = row.best_bid_ticks.0;
        self.best_ask_ticks[i] = row.best_ask_ticks.0;
        self.best_bid_qty[i] = row.best_bid_qty.0;
        self.best_ask_qty[i] = row.best_ask_qty.0;
        self.mark_price_ticks[i] = row.mark_price_ticks.0;
        self.realized_vol_5m_bps[i] = row.realized_vol_5m.0;
        self.open_interest_x100[i] = row.open_interest_x100;
        self.funding_rate_scaled_bps[i] = row.funding_rate_scaled_bps.0;
        self.next_funding_ts_ns[i] = row.next_funding_ts_ns.0;
        for lvl in 0..PAR2_DEEP_BOOK_LEVELS {
            self.deep_bid_ticks[i][lvl] = row.deep_bid_ticks[lvl].0;
            self.deep_bid_qty[i][lvl] = row.deep_bid_qty[lvl].0;
            self.deep_ask_ticks[i][lvl] = row.deep_ask_ticks[lvl].0;
            self.deep_ask_qty[i][lvl] = row.deep_ask_qty[lvl].0;
        }
        self.ofi_bps[i] = row.ofi_bps;
        self.tfi_bps[i] = row.tfi_bps;
        self.geom_bps[i] = row.geom_bps;
        self.len += 1;
        Ok(())
    }

    #[inline]
    #[must_use]
    pub const fn get_row(&self, idx: usize) -> Option<ColumnarMarketRow> {
        if idx >= self.len {
            return None;
        }
        let par2_kind = Par2EventKind::from_wire_byte(self.par2_kind[idx]);
        let event_kind = par2_kind.legacy_event_kind();
        let side = if self.side[idx] == 1 {
            Side::Ask
        } else {
            Side::Bid
        };
        let db_p = self.deep_bid_ticks[idx];
        let db_q = self.deep_bid_qty[idx];
        let da_p = self.deep_ask_ticks[idx];
        let da_q = self.deep_ask_qty[idx];
        Some(ColumnarMarketRow {
            symbol_id: SymbolId(self.symbol_id[idx]),
            event_kind,
            par2_kind,
            side,
            sequence_id: self.sequence_id[idx],
            ts_exchange_ns: TimestampNs(self.ts_exchange_ns[idx]),
            ts_local_ns: TimestampNs(self.ts_local_ns[idx]),
            price_ticks: PriceTicks(self.price_ticks[idx]),
            lot_qty: LotQty(self.lot_qty[idx]),
            best_bid_ticks: PriceTicks(self.best_bid_ticks[idx]),
            best_ask_ticks: PriceTicks(self.best_ask_ticks[idx]),
            best_bid_qty: LotQty(self.best_bid_qty[idx]),
            best_ask_qty: LotQty(self.best_ask_qty[idx]),
            mark_price_ticks: PriceTicks(self.mark_price_ticks[idx]),
            realized_vol_5m: ScaledBps(self.realized_vol_5m_bps[idx]),
            open_interest_x100: self.open_interest_x100[idx],
            funding_rate_scaled_bps: ScaledBps(self.funding_rate_scaled_bps[idx]),
            next_funding_ts_ns: TimestampNs(self.next_funding_ts_ns[idx]),
            deep_bid_ticks: [
                PriceTicks(db_p[0]),
                PriceTicks(db_p[1]),
                PriceTicks(db_p[2]),
                PriceTicks(db_p[3]),
            ],
            deep_bid_qty: [
                LotQty(db_q[0]),
                LotQty(db_q[1]),
                LotQty(db_q[2]),
                LotQty(db_q[3]),
            ],
            deep_ask_ticks: [
                PriceTicks(da_p[0]),
                PriceTicks(da_p[1]),
                PriceTicks(da_p[2]),
                PriceTicks(da_p[3]),
            ],
            deep_ask_qty: [
                LotQty(da_q[0]),
                LotQty(da_q[1]),
                LotQty(da_q[2]),
                LotQty(da_q[3]),
            ],
            ofi_bps: self.ofi_bps[idx],
            tfi_bps: self.tfi_bps[idx],
            geom_bps: self.geom_bps[idx],
        })
    }

    #[must_use]
    pub fn serialize_payload(&self) -> Vec<u8> {
        let n = self.len;
        let mut buf = Vec::with_capacity(20 + n * PAR2_LOSSLESS_ROW_BYTES);
        buf.extend_from_slice(&PAR2_MAGIC_HEADER);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        for i in 0..n {
            buf.extend_from_slice(&self.symbol_id[i].to_le_bytes());
            buf.push(self.par2_kind[i]);
            buf.push(self.side[i]);
            buf.extend_from_slice(&self.sequence_id[i].to_le_bytes());
            buf.extend_from_slice(&self.ts_exchange_ns[i].to_le_bytes());
            buf.extend_from_slice(&self.ts_local_ns[i].to_le_bytes());
            buf.extend_from_slice(&self.price_ticks[i].to_le_bytes());
            buf.extend_from_slice(&self.lot_qty[i].to_le_bytes());
            buf.extend_from_slice(&self.best_bid_ticks[i].to_le_bytes());
            buf.extend_from_slice(&self.best_ask_ticks[i].to_le_bytes());
            buf.extend_from_slice(&self.best_bid_qty[i].to_le_bytes());
            buf.extend_from_slice(&self.best_ask_qty[i].to_le_bytes());
            buf.extend_from_slice(&self.mark_price_ticks[i].to_le_bytes());
            buf.extend_from_slice(&self.ofi_bps[i].to_le_bytes());
            buf.extend_from_slice(&self.tfi_bps[i].to_le_bytes());
            buf.extend_from_slice(&self.geom_bps[i].to_le_bytes());
            buf.extend_from_slice(&self.realized_vol_5m_bps[i].to_le_bytes());
            buf.extend_from_slice(&self.open_interest_x100[i].to_le_bytes());
            buf.extend_from_slice(&self.funding_rate_scaled_bps[i].to_le_bytes());
            buf.extend_from_slice(&self.next_funding_ts_ns[i].to_le_bytes());
            for lvl in 0..PAR2_DEEP_BOOK_LEVELS {
                buf.extend_from_slice(&self.deep_bid_ticks[i][lvl].to_le_bytes());
                buf.extend_from_slice(&self.deep_bid_qty[i][lvl].to_le_bytes());
                buf.extend_from_slice(&self.deep_ask_ticks[i][lvl].to_le_bytes());
                buf.extend_from_slice(&self.deep_ask_qty[i][lvl].to_le_bytes());
            }
        }
        buf
    }

    #[inline]
    #[must_use]
    pub fn compute_fnv1a64(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in bytes {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    pub fn write_to_file(&self, path: &Path) -> Result<u64, VortexError> {
        let mut payload = self.serialize_payload();
        let checksum = Self::compute_fnv1a64(&payload);
        payload.extend_from_slice(&checksum.to_le_bytes());
        payload.extend_from_slice(&PAR2_MAGIC_HEADER);
        let mut file = File::create(path).map_err(|_| VortexError::ColumnarIoFailure)?;
        file.write_all(&payload)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        Ok(checksum)
    }

    #[inline]
    fn read_i64_le(buf: &[u8], pos: &mut usize) -> i64 {
        let mut b8 = [0_u8; 8];
        b8.copy_from_slice(&buf[*pos..*pos + 8]);
        *pos += 8;
        i64::from_le_bytes(b8)
    }

    #[inline]
    fn read_i32_le(buf: &[u8], pos: &mut usize) -> i32 {
        let mut b4 = [0_u8; 4];
        b4.copy_from_slice(&buf[*pos..*pos + 4]);
        *pos += 4;
        i32::from_le_bytes(b4)
    }

    pub fn read_from_file(path: &Path) -> Result<(Self, u64), VortexError> {
        let mut file = File::open(path).map_err(|_| VortexError::ColumnarIoFailure)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        if buf.len() < 20 {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        let head = &buf[0..4];
        let tail = &buf[buf.len() - 4..];
        let is_par2 = if head == PAR2_MAGIC_HEADER && tail == PAR2_MAGIC_HEADER {
            true
        } else if head == PARQUET_MAGIC_HEADER && tail == PARQUET_MAGIC_HEADER {
            false
        } else {
            return Err(VortexError::ColumnarFormatCorrupted);
        };

        let payload_end = buf.len() - 12;
        let mut c_bytes = [0_u8; 8];
        c_bytes.copy_from_slice(&buf[payload_end..payload_end + 8]);
        let stored_checksum = u64::from_le_bytes(c_bytes);
        let calc_checksum = Self::compute_fnv1a64(&buf[..payload_end]);
        if stored_checksum != calc_checksum {
            return Err(VortexError::ColumnarFormatCorrupted);
        }

        let mut n_bytes = [0_u8; 4];
        n_bytes.copy_from_slice(&buf[4..8]);
        let n = u32::from_le_bytes(n_bytes) as usize;
        if n > COLUMNAR_BATCH_CAP {
            return Err(VortexError::ColumnarFormatCorrupted);
        }

        let row_stride = if n == 0 {
            if buf.len() != 20 {
                return Err(VortexError::ColumnarFormatCorrupted);
            }
            PAR2_LOSSLESS_ROW_BYTES
        } else if is_par2 && buf.len() == 8 + n * PAR2_LOSSLESS_ROW_BYTES + 12 {
            PAR2_LOSSLESS_ROW_BYTES
        } else if !is_par2 && buf.len() == 8 + n * PAR1_L3_ROW_BYTES + 12 {
            PAR1_L3_ROW_BYTES
        } else if !is_par2 && buf.len() == 8 + n * PAR1_LEGACY_V1_ROW_BYTES + 12 {
            PAR1_LEGACY_V1_ROW_BYTES
        } else {
            return Err(VortexError::ColumnarFormatCorrupted);
        };

        let mut batch = Self::new();
        let mut pos = 8_usize;
        for _ in 0..n {
            let mut u16_b = [0_u8; 2];
            u16_b.copy_from_slice(&buf[pos..pos + 2]);
            let sym_id = SymbolId(u16::from_le_bytes(u16_b));
            let par2_k = if is_par2 {
                Par2EventKind::from_wire_byte(buf[pos + 2])
            } else if buf[pos + 2] == 2 {
                Par2EventKind::PublicTradePrint
            } else {
                Par2EventKind::L2BookSnapshot
            };
            let side = if buf[pos + 3] == 1 { Side::Ask } else { Side::Bid };
            pos += 4;
            let seq_id = Self::read_i64_le(&buf, &mut pos) as u64;
            let ts_exch = TimestampNs(Self::read_i64_le(&buf, &mut pos));
            let ts_loc = TimestampNs(Self::read_i64_le(&buf, &mut pos));
            let px_t = PriceTicks(Self::read_i64_le(&buf, &mut pos));
            let l_qty = LotQty(Self::read_i64_le(&buf, &mut pos));
            let bb_t = PriceTicks(Self::read_i64_le(&buf, &mut pos));
            let ba_t = PriceTicks(Self::read_i64_le(&buf, &mut pos));
            let bb_q = LotQty(Self::read_i64_le(&buf, &mut pos));
            let ba_q = LotQty(Self::read_i64_le(&buf, &mut pos));
            let mp_t = PriceTicks(Self::read_i64_le(&buf, &mut pos));
            let ofi_b = Self::read_i32_le(&buf, &mut pos);
            let tfi_b = Self::read_i32_le(&buf, &mut pos);
            let geom_b = Self::read_i32_le(&buf, &mut pos);

            let mut vol_5m = ScaledBps(6_500);
            let mut oi_x100 = 0_i64;
            let mut fund_bps = ScaledBps(0);
            let mut next_fund_ns = TimestampNs(0);
            let mut db_t = [PriceTicks(0); PAR2_DEEP_BOOK_LEVELS];
            let mut db_q = [LotQty(0); PAR2_DEEP_BOOK_LEVELS];
            let mut da_t = [PriceTicks(0); PAR2_DEEP_BOOK_LEVELS];
            let mut da_q = [LotQty(0); PAR2_DEEP_BOOK_LEVELS];

            if row_stride == PAR2_LOSSLESS_ROW_BYTES {
                vol_5m = ScaledBps(Self::read_i64_le(&buf, &mut pos));
                oi_x100 = Self::read_i64_le(&buf, &mut pos);
                fund_bps = ScaledBps(Self::read_i64_le(&buf, &mut pos));
                next_fund_ns = TimestampNs(Self::read_i64_le(&buf, &mut pos));
                for lvl in 0..PAR2_DEEP_BOOK_LEVELS {
                    db_t[lvl] = PriceTicks(Self::read_i64_le(&buf, &mut pos));
                    db_q[lvl] = LotQty(Self::read_i64_le(&buf, &mut pos));
                    da_t[lvl] = PriceTicks(Self::read_i64_le(&buf, &mut pos));
                    da_q[lvl] = LotQty(Self::read_i64_le(&buf, &mut pos));
                }
            } else if row_stride == PAR1_L3_ROW_BYTES {
                vol_5m = ScaledBps(Self::read_i64_le(&buf, &mut pos));
                for lvl in 0..DEEP_BOOK_LEVELS {
                    db_t[lvl] = PriceTicks(Self::read_i64_le(&buf, &mut pos));
                    db_q[lvl] = LotQty(Self::read_i64_le(&buf, &mut pos));
                    da_t[lvl] = PriceTicks(Self::read_i64_le(&buf, &mut pos));
                    da_q[lvl] = LotQty(Self::read_i64_le(&buf, &mut pos));
                }
            }

            let row = ColumnarMarketRow {
                symbol_id: sym_id,
                event_kind: par2_k.legacy_event_kind(),
                par2_kind: par2_k,
                side,
                sequence_id: seq_id,
                ts_exchange_ns: ts_exch,
                ts_local_ns: ts_loc,
                price_ticks: px_t,
                lot_qty: l_qty,
                best_bid_ticks: bb_t,
                best_ask_ticks: ba_t,
                best_bid_qty: bb_q,
                best_ask_qty: ba_q,
                mark_price_ticks: mp_t,
                realized_vol_5m: vol_5m,
                open_interest_x100: oi_x100,
                funding_rate_scaled_bps: fund_bps,
                next_funding_ts_ns: next_fund_ns,
                deep_bid_ticks: db_t,
                deep_bid_qty: db_q,
                deep_ask_ticks: da_t,
                deep_ask_qty: da_q,
                ofi_bps: ofi_b,
                tfi_bps: tfi_b,
                geom_bps: geom_b,
            };
            let _ = batch.push_row(&row);
        }
        Ok((batch, stored_checksum))
    }
}

/// Law I: Exact 1e8 Accounting Parity Ledger with Dynamic VIP Fees, Funding Attribution & 70/30 Vault.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ParitySyncLedger {
    pub initial_equity_1e8: i64,
    pub cumulative_net_pnl_1e8: i64,
    pub cumulative_fees_1e8: i64,
    pub cumulative_funding_1e8: i64,
    pub closed_trades_count: u64,
    pub sinking_fund_reserve_bps_x100: i64,
    pub vault_locked_profit_bps_x100: i64,
    pub active_maker_fee_bps: ScaledBps,
    pub active_taker_fee_bps: ScaledBps,
}

impl ParitySyncLedger {
    #[inline]
    #[must_use]
    pub const fn new(initial_equity_1e8: i64) -> Self {
        Self {
            initial_equity_1e8,
            cumulative_net_pnl_1e8: 0,
            cumulative_fees_1e8: 0,
            cumulative_funding_1e8: 0,
            closed_trades_count: 0,
            sinking_fund_reserve_bps_x100: 0,
            vault_locked_profit_bps_x100: 0,
            active_maker_fee_bps: ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS,
            active_taker_fee_bps: ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS,
        }
    }

    #[inline]
    #[must_use]
    pub const fn current_equity_1e8(self) -> i64 {
        self.initial_equity_1e8
            .saturating_add(self.cumulative_net_pnl_1e8)
    }

    #[inline]
    pub fn set_live_vip_fee_rates(&mut self, maker_fee: ScaledBps, taker_fee: ScaledBps) {
        self.active_maker_fee_bps = ScaledBps(maker_fee.0.clamp(-50, 250));
        self.active_taker_fee_bps = ScaledBps(taker_fee.0.clamp(100, 1_000));
    }

    #[inline]
    #[must_use]
    pub const fn expected_round_trip_fee_bps(self, is_maker_exit: bool) -> ScaledBps {
        if is_maker_exit {
            ScaledBps(self.active_maker_fee_bps.0.saturating_mul(2))
        } else {
            ScaledBps(self.active_maker_fee_bps.0.saturating_add(self.active_taker_fee_bps.0))
        }
    }

    #[inline]
    #[must_use]
    pub const fn elastic_sinking_fund_cap_bps(self) -> i64 {
        let vip0_sum =
            ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.0 + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.0;
        let cur_sum = self.active_maker_fee_bps.0 + self.active_taker_fee_bps.0;
        let eff_sum = if cur_sum > 150 { cur_sum } else { 150 };
        let scaled = (MAX_SINKING_FUND_SCALED_BPS * eff_sum) / vip0_sum;
        if scaled < 10_000 {
            10_000
        } else if scaled > 40_000 {
            40_000
        } else {
            scaled
        }
    }

    #[inline]
    fn apply_vault_70_30_bps_delta(&mut self, net_bps_x100: i64) {
        let cap = self.elastic_sinking_fund_cap_bps();
        match net_bps_x100.cmp(&0) {
            Ordering::Greater => {
                let locked_70 = (net_bps_x100 * 70) / 100;
                let reserve_30 = net_bps_x100 - locked_70;
                self.vault_locked_profit_bps_x100 =
                    self.vault_locked_profit_bps_x100.saturating_add(locked_70);
                self.sinking_fund_reserve_bps_x100 = self
                    .sinking_fund_reserve_bps_x100
                    .saturating_add(reserve_30)
                    .min(cap);
            }
            Ordering::Less => {
                self.sinking_fund_reserve_bps_x100 = self
                    .sinking_fund_reserve_bps_x100
                    .saturating_add(net_bps_x100)
                    .max(0);
            }
            Ordering::Equal => {}
        }
    }

    #[inline]
    pub fn record_closed_trade(&mut self, net_pnl_1e8: i64, fee_1e8: i64) {
        self.cumulative_net_pnl_1e8 = self.cumulative_net_pnl_1e8.saturating_add(net_pnl_1e8);
        self.cumulative_fees_1e8 = self.cumulative_fees_1e8.saturating_add(fee_1e8);
        self.closed_trades_count = self.closed_trades_count.saturating_add(1);
    }

    /// Consolidates 1 closed position (single-leg or multi-tranche) with monotonic 70% vault.
    #[inline]
    pub fn record_closed_trade_with_sinking_split(
        &mut self,
        net_pnl_1e8: i64,
        fee_1e8: i64,
        consolidated_net_bps: ScaledBps,
    ) {
        self.record_closed_trade(net_pnl_1e8, fee_1e8);
        self.apply_vault_70_30_bps_delta(consolidated_net_bps.raw());
    }

    /// Settles exact 1e8 funding cashflow (positive = received, negative = paid) with Drift == 0.
    #[inline]
    pub fn record_funding_settlement(&mut self, funding_net_1e8: i64, position_notional_1e8: i64) {
        self.cumulative_funding_1e8 =
            self.cumulative_funding_1e8.saturating_add(funding_net_1e8);
        self.cumulative_net_pnl_1e8 =
            self.cumulative_net_pnl_1e8.saturating_add(funding_net_1e8);
        let denom = position_notional_1e8.max(1) as i128;
        let funding_bps = ((funding_net_1e8 as i128 * 1_000_000) / denom) as i64;
        self.apply_vault_70_30_bps_delta(funding_bps);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par1_l3_depth_roundtrip_legacy_v1_compat_and_monotonic_70_vault() {
        // 1. Legacy PAR1 L3 (200B/row) binary backward-compatibility check
        let mut l3_payload = Vec::with_capacity(230);
        l3_payload.extend_from_slice(&PARQUET_MAGIC_HEADER);
        l3_payload.extend_from_slice(&1_u32.to_le_bytes());
        l3_payload.extend_from_slice(&4_u16.to_le_bytes());
        l3_payload.push(1_u8);
        l3_payload.push(0_u8);
        for val in [1_001_i64, 100, 101, 8_200, 300, 8_198, 8_199, 300, 240, 8_200] {
            l3_payload.extend_from_slice(&val.to_le_bytes());
        }
        for bval in [6_000_i32, 7_200, 3_500] {
            l3_payload.extend_from_slice(&bval.to_le_bytes());
        }
        l3_payload.extend_from_slice(&7_400_i64.to_le_bytes());
        for lvl in 0..DEEP_BOOK_LEVELS {
            let bp = 8_197 - (lvl as i64);
            let ap = 8_201 + (lvl as i64) * 2;
            for v in [bp, 320, ap, 255] {
                l3_payload.extend_from_slice(&v.to_le_bytes());
            }
        }
        let l3_crc = ColumnarParquetBatch::compute_fnv1a64(&l3_payload);
        l3_payload.extend_from_slice(&l3_crc.to_le_bytes());
        l3_payload.extend_from_slice(&PARQUET_MAGIC_HEADER);
        let tmp_l3 = std::env::temp_dir().join("vortex_par1_l3_compat_v45.parquet");
        assert!(std::fs::write(&tmp_l3, &l3_payload).is_ok());
        let l3_loaded = ColumnarParquetBatch::read_from_file(&tmp_l3);
        assert!(l3_loaded.is_ok());
        if let Ok((b_l3, crc_out)) = l3_loaded {
            assert_eq!(crc_out, l3_crc);
            assert_eq!(b_l3.len(), 1);
            let mut restored = SymbolLobSnapshot::new_empty();
            if let Some(r0) = b_l3.get_row(0) {
                r0.apply_to_lob_snapshot(&mut restored);
            }
            assert_eq!(restored.realized_vol_5m, ScaledBps(7_400));
            assert_eq!(restored.asks[3].price, PriceTicks(8_205));
            assert_eq!(restored.asks[4].price, PriceTicks(0));
        }
        let _ = std::fs::remove_file(&tmp_l3);

        // 2. Legacy PAR1 v1 (96B/row) binary backward-compatibility check
        let mut v1_payload = Vec::with_capacity(120);
        v1_payload.extend_from_slice(&PARQUET_MAGIC_HEADER);
        v1_payload.extend_from_slice(&1_u32.to_le_bytes());
        v1_payload.extend_from_slice(&4_u16.to_le_bytes());
        v1_payload.push(1_u8);
        v1_payload.push(0_u8);
        for val in [99_i64, 10, 11, 8_198, 25, 8_198, 8_199, 300, 250, 8_198] {
            v1_payload.extend_from_slice(&val.to_le_bytes());
        }
        for bval in [5_000_i32, 6_000, 2_500] {
            v1_payload.extend_from_slice(&bval.to_le_bytes());
        }
        let v1_crc = ColumnarParquetBatch::compute_fnv1a64(&v1_payload);
        v1_payload.extend_from_slice(&v1_crc.to_le_bytes());
        v1_payload.extend_from_slice(&PARQUET_MAGIC_HEADER);
        let tmp_v1 = std::env::temp_dir().join("vortex_par1_v1_compat_v45.parquet");
        assert!(std::fs::write(&tmp_v1, &v1_payload).is_ok());
        if let Ok((vb, _)) = ColumnarParquetBatch::read_from_file(&tmp_v1) {
            assert_eq!(vb.len(), 1);
            if let Some(vr) = vb.get_row(0) {
                assert_eq!(vr.realized_vol_5m, ScaledBps(6_500));
                assert_eq!(vr.best_bid_ticks, PriceTicks(8_198));
            }
        }
        let _ = std::fs::remove_file(&tmp_v1);

        // 3. Monotonic 70/30 vault invariant check
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        ledger.record_closed_trade_with_sinking_split(250_000, 500_000, ScaledBps(200));
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 60);
        assert_eq!(ledger.current_equity_1e8(), 6_628_330_000);
        ledger.record_closed_trade_with_sinking_split(-900_000, 750_000, ScaledBps(-1_200));
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 0);
    }

    #[test]
    fn test_par2_lossless_l5_ticker_liq_and_dynamic_vip_funding_ledger() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = SymbolId(6);
        snap.sequence_id = 42_000;
        snap.ts_exchange_ns = TimestampNs(1_794_700_000_000_000_000);
        snap.ts_local_ns = TimestampNs(1_794_700_000_002_000_000);
        snap.mark_price = PriceTicks(6_860);
        snap.realized_vol_5m = ScaledBps(6_541);
        for lvl in 0..5 {
            snap.bids[lvl] = BookLevel {
                price: PriceTicks(6_860 - lvl as i64),
                qty: LotQty(500 + (lvl as i64) * 50),
            };
            snap.asks[lvl] = BookLevel {
                price: PriceTicks(6_861 + lvl as i64),
                qty: LotQty(420 + (lvl as i64) * 40),
            };
        }
        snap.refresh_deep_l1_l5_totals();
        snap.factor_ofi = BoundedScore::new_clamped(0.54);
        snap.factor_tfi = BoundedScore::new_clamped(0.71);
        snap.factor_geom = BoundedScore::new_clamped(0.28);

        let r_l2 = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::L2BookSnapshot,
            Side::Bid,
            PriceTicks(6_860),
            LotQty(500),
        );
        let r_tr = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::PublicTradePrint,
            Side::Bid,
            PriceTicks(6_861),
            LotQty(85),
        );
        let r_tk = ColumnarMarketRow::from_ticker_meta(
            &snap,
            PriceTicks(6_862),
            12_450_000,
            ScaledBps(-135),
            TimestampNs(1_794_700_800_000_000_000),
        );
        let r_lq = ColumnarMarketRow::from_liquidation_print(
            &snap,
            Side::Bid,
            PriceTicks(6_875),
            LotQty(2_400),
            12_300_000,
        );

        let mut batch = ColumnarParquetBatch::new();
        for row in [&r_l2, &r_tr, &r_tk, &r_lq] {
            assert!(batch.push_row(row).is_ok());
        }
        let payload = batch.serialize_payload();
        assert_eq!(payload.len(), 8 + 4 * PAR2_LOSSLESS_ROW_BYTES);

        let tmp_p2 = std::env::temp_dir().join(format!("vortex_par2_v45_{}.parquet", std::process::id()));
        let crc_w = batch.write_to_file(&tmp_p2);
        assert!(crc_w.is_ok());
        let loaded = ColumnarParquetBatch::read_from_file(&tmp_p2);
        let _ = std::fs::remove_file(&tmp_p2);
        assert!(loaded.is_ok());
        if let Ok((b2, crc_r)) = loaded {
            assert_eq!(Ok(crc_r), crc_w);
            assert_eq!(b2.len(), 4);
            assert_eq!(b2.get_row(0), Some(r_l2));
            assert_eq!(b2.get_row(1), Some(r_tr));
            assert_eq!(b2.get_row(2), Some(r_tk));
            assert_eq!(b2.get_row(3), Some(r_lq));

            let mut replayed = SymbolLobSnapshot::new_empty();
            for idx in 0..4 {
                if let Some(row) = b2.get_row(idx) {
                    row.apply_to_lob_snapshot(&mut replayed);
                }
            }
            // Full L1..L5 preserved (including Level 5 = index 4) & liquidation did not pollute trade sum
            assert_eq!(replayed.bids[4].price, PriceTicks(6_856));
            assert_eq!(replayed.asks[4].qty, LotQty(580));
            assert_eq!(replayed.deep_bid_lots_l1_l5, snap.deep_bid_lots_l1_l5);
            assert_eq!(replayed.mark_price, PriceTicks(6_862));
            assert_eq!(replayed.cum_buy_trade_lots, LotQty(85));
        }

        // Dynamic VIP Fee + Exact 1e8 Funding Settlement Parity (Drift == 0)
        let mut ledger = ParitySyncLedger::new(6_541_160_000);
        ledger.set_live_vip_fee_rates(ScaledBps(140), ScaledBps(400));
        assert_eq!(ledger.expected_round_trip_fee_bps(true), ScaledBps(280));
        assert_eq!(ledger.expected_round_trip_fee_bps(false), ScaledBps(540));
        ledger.record_funding_settlement(380_000, 1_900_000_000); // +2.00 bps received on 19.00 USDT
        assert_eq!(ledger.cumulative_funding_1e8, 380_000);
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 60);
        assert_eq!(
            ledger.current_equity_1e8() - ledger.initial_equity_1e8,
            ledger.cumulative_net_pnl_1e8
        );
    }
}
