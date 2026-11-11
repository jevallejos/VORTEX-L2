//! Columnar PAR1 L1..L4 Market Recorder and ParitySyncLedger with 30%/70% Sinking Fund.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::lob_shm::{BookLevel, SymbolLobSnapshot};
use crate::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs, VortexError,
};

pub const COLUMNAR_BATCH_CAP: usize = 256;
pub const DEEP_BOOK_LEVELS: usize = 3;
pub const PARQUET_MAGIC_HEADER: [u8; 4] = *b"PAR1";
pub const PAR1_LEGACY_V1_ROW_BYTES: usize = 96;
pub const PAR1_L3_ROW_BYTES: usize = 200;
pub const MAX_SINKING_FUND_SCALED_BPS: i64 = 25_000;

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MarketEventKind {
    L2BookSnapshot = 1,
    PublicTradePrint = 2,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ColumnarMarketRow {
    pub symbol_id: SymbolId,
    pub event_kind: MarketEventKind,
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
    pub deep_bid_ticks: [PriceTicks; DEEP_BOOK_LEVELS],
    pub deep_bid_qty: [LotQty; DEEP_BOOK_LEVELS],
    pub deep_ask_ticks: [PriceTicks; DEEP_BOOK_LEVELS],
    pub deep_ask_qty: [LotQty; DEEP_BOOK_LEVELS],
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
        Self {
            symbol_id: snap.symbol_id,
            event_kind,
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
            deep_bid_ticks: [snap.bids[1].price, snap.bids[2].price, snap.bids[3].price],
            deep_bid_qty: [snap.bids[1].qty, snap.bids[2].qty, snap.bids[3].qty],
            deep_ask_ticks: [snap.asks[1].price, snap.asks[2].price, snap.asks[3].price],
            deep_ask_qty: [snap.asks[1].qty, snap.asks[2].qty, snap.asks[3].qty],
            ofi_bps: snap.factor_ofi.to_bps_i32(),
            tfi_bps: snap.factor_tfi.to_bps_i32(),
            geom_bps: snap.factor_geom.to_bps_i32(),
        }
    }

    #[inline]
    pub fn apply_to_lob_snapshot(&self, snap: &mut SymbolLobSnapshot) {
        snap.symbol_id = self.symbol_id;
        snap.sequence_id = self.sequence_id;
        snap.ts_exchange_ns = self.ts_exchange_ns;
        snap.ts_local_ns = self.ts_local_ns;
        if self.mark_price_ticks.0 > 0 {
            snap.mark_price = self.mark_price_ticks;
        }
        if self.realized_vol_5m.0 > 0 {
            snap.realized_vol_5m = self.realized_vol_5m;
        }
        snap.bids[0] = BookLevel {
            price: self.best_bid_ticks,
            qty: self.best_bid_qty,
        };
        snap.asks[0] = BookLevel {
            price: self.best_ask_ticks,
            qty: self.best_ask_qty,
        };
        let mut lvl = 0_usize;
        while lvl < DEEP_BOOK_LEVELS {
            snap.bids[lvl + 1] = BookLevel {
                price: self.deep_bid_ticks[lvl],
                qty: self.deep_bid_qty[lvl],
            };
            snap.asks[lvl + 1] = BookLevel {
                price: self.deep_ask_ticks[lvl],
                qty: self.deep_ask_qty[lvl],
            };
            lvl += 1;
        }
        snap.refresh_deep_l1_l5_totals();
        match self.event_kind {
            MarketEventKind::PublicTradePrint => {
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
            MarketEventKind::L2BookSnapshot => {
                if snap.last_trade_price.0 <= 0 {
                    if self.best_bid_ticks.0 > 0 && self.best_ask_ticks.0 > 0 {
                        snap.last_trade_price = PriceTicks(
                            (self.best_bid_ticks.0 + self.best_ask_ticks.0) / 2,
                        );
                    } else if self.price_ticks.0 > 0 {
                        snap.last_trade_price = self.price_ticks;
                    }
                }
            }
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
    pub event_kind: [u8; COLUMNAR_BATCH_CAP],
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
    pub deep_bid_ticks: [[i64; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_bid_qty: [[i64; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_ask_ticks: [[i64; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
    pub deep_ask_qty: [[i64; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
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
            event_kind: [0; COLUMNAR_BATCH_CAP],
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
            deep_bid_ticks: [[0; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_bid_qty: [[0; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_ask_ticks: [[0; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
            deep_ask_qty: [[0; DEEP_BOOK_LEVELS]; COLUMNAR_BATCH_CAP],
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
        self.event_kind[i] = row.event_kind as u8;
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
        let mut lvl = 0_usize;
        while lvl < DEEP_BOOK_LEVELS {
            self.deep_bid_ticks[i][lvl] = row.deep_bid_ticks[lvl].0;
            self.deep_bid_qty[i][lvl] = row.deep_bid_qty[lvl].0;
            self.deep_ask_ticks[i][lvl] = row.deep_ask_ticks[lvl].0;
            self.deep_ask_qty[i][lvl] = row.deep_ask_qty[lvl].0;
            lvl += 1;
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
        let event_kind = if self.event_kind[idx] == 2 {
            MarketEventKind::PublicTradePrint
        } else {
            MarketEventKind::L2BookSnapshot
        };
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
            deep_bid_ticks: [PriceTicks(db_p[0]), PriceTicks(db_p[1]), PriceTicks(db_p[2])],
            deep_bid_qty: [LotQty(db_q[0]), LotQty(db_q[1]), LotQty(db_q[2])],
            deep_ask_ticks: [PriceTicks(da_p[0]), PriceTicks(da_p[1]), PriceTicks(da_p[2])],
            deep_ask_qty: [LotQty(da_q[0]), LotQty(da_q[1]), LotQty(da_q[2])],
            ofi_bps: self.ofi_bps[idx],
            tfi_bps: self.tfi_bps[idx],
            geom_bps: self.geom_bps[idx],
        })
    }

    #[must_use]
    pub fn serialize_payload(&self) -> Vec<u8> {
        let n = self.len;
        let mut buf = Vec::with_capacity(16 + n * PAR1_L3_ROW_BYTES);
        buf.extend_from_slice(&PARQUET_MAGIC_HEADER);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        for i in 0..n {
            buf.extend_from_slice(&self.symbol_id[i].to_le_bytes());
            buf.push(self.event_kind[i]);
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
            for lvl in 0..DEEP_BOOK_LEVELS {
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
        payload.extend_from_slice(&PARQUET_MAGIC_HEADER);
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
        if buf[0..4] != PARQUET_MAGIC_HEADER || buf[buf.len() - 4..] != PARQUET_MAGIC_HEADER {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
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
        let has_l3_ext = if n == 0 {
            if buf.len() != 20 {
                return Err(VortexError::ColumnarFormatCorrupted);
            }
            true
        } else if buf.len() == 8 + n * PAR1_L3_ROW_BYTES + 12 {
            true
        } else if buf.len() == 8 + n * PAR1_LEGACY_V1_ROW_BYTES + 12 {
            false
        } else {
            return Err(VortexError::ColumnarFormatCorrupted);
        };

        let mut batch = Self::new();
        let mut pos = 8_usize;
        for _ in 0..n {
            let mut u16_b = [0_u8; 2];
            u16_b.copy_from_slice(&buf[pos..pos + 2]);
            let sym_id = SymbolId(u16::from_le_bytes(u16_b));
            let ev_k = if buf[pos + 2] == 2 {
                MarketEventKind::PublicTradePrint
            } else {
                MarketEventKind::L2BookSnapshot
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
            let mut db_t = [PriceTicks(0); DEEP_BOOK_LEVELS];
            let mut db_q = [LotQty(0); DEEP_BOOK_LEVELS];
            let mut da_t = [PriceTicks(0); DEEP_BOOK_LEVELS];
            let mut da_q = [LotQty(0); DEEP_BOOK_LEVELS];
            if has_l3_ext {
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
                event_kind: ev_k,
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

/// Accounting Parity Ledger with Monotonic 70% Locked Vault & 30% Sinking Reserve (F-05).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ParitySyncLedger {
    pub initial_equity_1e8: i64,
    pub cumulative_net_pnl_1e8: i64,
    pub cumulative_fees_1e8: i64,
    pub closed_trades_count: u64,
    pub sinking_fund_reserve_bps_x100: i64,
    pub vault_locked_profit_bps_x100: i64,
}

impl ParitySyncLedger {
    #[inline]
    #[must_use]
    pub const fn new(initial_equity_1e8: i64) -> Self {
        Self {
            initial_equity_1e8,
            cumulative_net_pnl_1e8: 0,
            cumulative_fees_1e8: 0,
            closed_trades_count: 0,
            sinking_fund_reserve_bps_x100: 0,
            vault_locked_profit_bps_x100: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn current_equity_1e8(self) -> i64 {
        self.initial_equity_1e8
            .saturating_add(self.cumulative_net_pnl_1e8)
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
        let n_bps = consolidated_net_bps.raw();
        if n_bps > 0 {
            let locked_70 = (n_bps * 70) / 100;
            let reserve_30 = n_bps - locked_70;
            self.vault_locked_profit_bps_x100 =
                self.vault_locked_profit_bps_x100.saturating_add(locked_70);
            self.sinking_fund_reserve_bps_x100 = self
                .sinking_fund_reserve_bps_x100
                .saturating_add(reserve_30)
                .min(MAX_SINKING_FUND_SCALED_BPS);
        } else if n_bps < 0 {
            self.sinking_fund_reserve_bps_x100 = self
                .sinking_fund_reserve_bps_x100
                .saturating_add(n_bps)
                .max(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par1_l3_depth_roundtrip_legacy_v1_compat_and_monotonic_70_vault() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = SymbolId(4);
        snap.sequence_id = 1_001;
        snap.ts_exchange_ns = TimestampNs(1_700_000_000_000_000_000);
        snap.ts_local_ns = TimestampNs(1_700_000_000_001_000_000);
        snap.mark_price = PriceTicks(8_200);
        snap.realized_vol_5m = ScaledBps(7_400);
        for lvl in 0..4 {
            snap.bids[lvl] = BookLevel {
                price: PriceTicks(8_198 - lvl as i64),
                qty: LotQty(300 + (lvl as i64) * 20),
            };
            snap.asks[lvl] = BookLevel {
                price: PriceTicks(8_199 + (lvl as i64) * 2),
                qty: LotQty(240 + (lvl as i64) * 15),
            };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.60);
        snap.factor_tfi = BoundedScore::new_clamped(0.72);
        snap.factor_geom = BoundedScore::new_clamped(0.35);

        let tr_row = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::PublicTradePrint,
            Side::Bid,
            PriceTicks(8_205),
            LotQty(18),
        );
        let l2_row = ColumnarMarketRow::from_lob_snapshot(
            &snap,
            MarketEventKind::L2BookSnapshot,
            Side::Bid,
            PriceTicks(8_200),
            LotQty(300),
        );

        let mut batch = ColumnarParquetBatch::new();
        assert!(batch.push_row(&tr_row).is_ok());
        assert!(batch.push_row(&l2_row).is_ok());
        let tmp_l3 = std::env::temp_dir().join("vortex_l3_chat4_depth.parquet");
        let crc = batch.write_to_file(&tmp_l3);
        assert!(crc.is_ok());
        let loaded = ColumnarParquetBatch::read_from_file(&tmp_l3);
        assert!(loaded.is_ok());
        if let Ok((b2, crc2)) = loaded {
            assert_eq!(Ok(crc2), crc);
            assert_eq!(b2.len(), 2);
            assert_eq!(b2.get_row(0), Some(tr_row));
            assert_eq!(b2.get_row(1), Some(l2_row));
            let mut restored = SymbolLobSnapshot::new_empty();
            if let Some(r0) = b2.get_row(0) {
                r0.apply_to_lob_snapshot(&mut restored);
            }
            if let Some(r1) = b2.get_row(1) {
                r1.apply_to_lob_snapshot(&mut restored);
            }
            assert_eq!(restored.last_trade_price, PriceTicks(8_205));
            assert_eq!(restored.cum_buy_trade_lots, LotQty(18));
            assert_eq!(restored.realized_vol_5m, ScaledBps(7_400));
            assert_eq!(restored.asks[3].price, PriceTicks(8_205));
        }
        let _ = std::fs::remove_file(&tmp_l3);

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
        let tmp_v1 = std::env::temp_dir().join("vortex_l3_chat4_legacy_v1.parquet");
        assert!(std::fs::write(&tmp_v1, &v1_payload).is_ok());
        let v1_loaded = ColumnarParquetBatch::read_from_file(&tmp_v1);
        assert!(v1_loaded.is_ok());
        if let Ok((vb, _)) = v1_loaded {
            assert_eq!(vb.len(), 1);
            if let Some(vr) = vb.get_row(0) {
                assert_eq!(vr.realized_vol_5m, ScaledBps(6_500));
                assert_eq!(vr.best_bid_ticks, PriceTicks(8_198));
            }
        }
        let _ = std::fs::remove_file(&tmp_v1);

        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        ledger.record_closed_trade_with_sinking_split(250_000, 500_000, ScaledBps(200));
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 60);
        assert_eq!(ledger.current_equity_1e8(), 6_628_330_000);
        ledger.record_closed_trade_with_sinking_split(-900_000, 750_000, ScaledBps(-1_200));
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 0);
    }
}
