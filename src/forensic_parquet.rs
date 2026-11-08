//! Columnar PAR1 Market Recorder and ParitySyncLedger with 30%/70% Sinking Fund.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::types::{LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs, VortexError};

pub const COLUMNAR_BATCH_CAP: usize = 256;
pub const PARQUET_MAGIC_HEADER: [u8; 4] = *b"PAR1";
pub const MAX_SINKING_FUND_SCALED_BPS: i64 = 3_500;

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
    pub ofi_bps: i32,
    pub tfi_bps: i32,
    pub geom_bps: i32,
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
            ofi_bps: self.ofi_bps[idx],
            tfi_bps: self.tfi_bps[idx],
            geom_bps: self.geom_bps[idx],
        })
    }

    #[must_use]
    pub fn serialize_payload(&self) -> Vec<u8> {
        let n = self.len;
        let mut buf = Vec::with_capacity(16 + n * 96);
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
        file.write_all(&payload).map_err(|_| VortexError::ColumnarIoFailure)?;
        Ok(checksum)
    }

    pub fn read_from_file(path: &Path) -> Result<(Self, u64), VortexError> {
        let mut file = File::open(path).map_err(|_| VortexError::ColumnarIoFailure)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).map_err(|_| VortexError::ColumnarIoFailure)?;
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
        if n > COLUMNAR_BATCH_CAP || buf.len() != 8 + n * 96 + 12 {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
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
            let mut i64_vals = [0_i64; 10];
            for v in &mut i64_vals {
                let mut b8 = [0_u8; 8];
                b8.copy_from_slice(&buf[pos..pos + 8]);
                *v = i64::from_le_bytes(b8);
                pos += 8;
            }
            let mut i32_vals = [0_i32; 3];
            for v in &mut i32_vals {
                let mut b4 = [0_u8; 4];
                b4.copy_from_slice(&buf[pos..pos + 4]);
                *v = i32::from_le_bytes(b4);
                pos += 4;
            }
            let row = ColumnarMarketRow {
                symbol_id: sym_id,
                event_kind: ev_k,
                side,
                sequence_id: i64_vals[0] as u64,
                ts_exchange_ns: TimestampNs(i64_vals[1]),
                ts_local_ns: TimestampNs(i64_vals[2]),
                price_ticks: PriceTicks(i64_vals[3]),
                lot_qty: LotQty(i64_vals[4]),
                best_bid_ticks: PriceTicks(i64_vals[5]),
                best_ask_ticks: PriceTicks(i64_vals[6]),
                best_bid_qty: LotQty(i64_vals[7]),
                best_ask_qty: LotQty(i64_vals[8]),
                mark_price_ticks: PriceTicks(i64_vals[9]),
                ofi_bps: i32_vals[0],
                tfi_bps: i32_vals[1],
                geom_bps: i32_vals[2],
            };
            let _ = batch.push_row(&row);
        }
        Ok((batch, stored_checksum))
    }
}

/// Accounting Parity Ledger with Native 30% Sinking Fund & 70% Locked Vault.
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
    pub fn record_closed_trade(&mut self, net_pnl_1e8: i64, fee_1e8: i64) {
        self.cumulative_net_pnl_1e8 = self.cumulative_net_pnl_1e8.saturating_add(net_pnl_1e8);
        self.cumulative_fees_1e8 = self.cumulative_fees_1e8.saturating_add(fee_1e8);
        self.closed_trades_count = self.closed_trades_count.saturating_add(1);
    }

    #[inline]
    pub fn record_closed_trade_with_sinking_split(
        &mut self,
        net_pnl_1e8: i64,
        fee_1e8: i64,
        net_scaled_bps: ScaledBps,
    ) {
        self.record_closed_trade(net_pnl_1e8, fee_1e8);
        let n_bps = net_scaled_bps.raw();
        if n_bps > 0 {
            let locked_70 = (n_bps * 70) / 100;
            let reserve_30 = n_bps - locked_70;
            self.vault_locked_profit_bps_x100 =
                self.vault_locked_profit_bps_x100.saturating_add(locked_70);
            self.sinking_fund_reserve_bps_x100 = self
                .sinking_fund_reserve_bps_x100
                .saturating_add(reserve_30)
                .min(MAX_SINKING_FUND_SCALED_BPS);
        }
    }

    #[inline]
    pub fn try_consume_sinking_fund_subsidy(&mut self, requested_scaled_bps: i64) -> i64 {
        if requested_scaled_bps <= 0 || self.sinking_fund_reserve_bps_x100 <= 0 {
            return 0;
        }
        let granted = requested_scaled_bps.min(self.sinking_fund_reserve_bps_x100);
        self.sinking_fund_reserve_bps_x100 -= granted;
        granted
    }

    #[inline]
    #[must_use]
    pub const fn expected_exchange_balance_1e8(&self) -> i64 {
        self.initial_equity_1e8.saturating_add(self.cumulative_net_pnl_1e8)
    }

    #[inline]
    pub const fn verify_parity_against_exchange(
        &self,
        exchange_balance_1e8: i64,
        tolerance_units: i64,
    ) -> Result<i64, VortexError> {
        let expected = self.expected_exchange_balance_1e8();
        let drift = (expected - exchange_balance_1e8).abs();
        if drift > tolerance_units {
            return Err(VortexError::AccountingParityDrift {
                internal_balance_1e8: expected,
                exchange_balance_1e8,
                drift_1e8: drift,
            });
        }
        Ok(drift)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par1_roundtrip_and_30_70_sinking_fund_invariants() {
        let mut batch = ColumnarParquetBatch::new();
        let row = ColumnarMarketRow {
            symbol_id: SymbolId(4),
            event_kind: MarketEventKind::L2BookSnapshot,
            side: Side::Bid,
            sequence_id: 999,
            ts_exchange_ns: TimestampNs(1_700_000_000_000_000_000),
            ts_local_ns: TimestampNs(1_700_000_000_001_000_000),
            price_ticks: PriceTicks(8_198),
            lot_qty: LotQty(26),
            best_bid_ticks: PriceTicks(8_198),
            best_ask_ticks: PriceTicks(8_199),
            best_bid_qty: LotQty(300),
            best_ask_qty: LotQty(250),
            mark_price_ticks: PriceTicks(8_198),
            ofi_bps: 6000,
            tfi_bps: 7200,
            geom_bps: 3500,
        };
        assert!(batch.push_row(&row).is_ok());
        let tmp_p = std::env::temp_dir().join("vortex_l3_step2_par1.parquet");
        let c1 = batch.write_to_file(&tmp_p);
        assert!(c1.is_ok());
        let loaded = ColumnarParquetBatch::read_from_file(&tmp_p);
        assert!(loaded.is_ok());
        if let Ok((b2, c2)) = loaded {
            assert_eq!(Ok(c2), c1);
            assert_eq!(b2.get_row(0), Some(row));
        }
        let _ = std::fs::remove_file(&tmp_p);

        let mut ledger = ParitySyncLedger::new(7_088_000_000);
        ledger.record_closed_trade_with_sinking_split(250_000, 500_000, ScaledBps(200));
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 60);
        let used = ledger.try_consume_sinking_fund_subsidy(45);
        assert_eq!(used, 45);
        assert_eq!(ledger.sinking_fund_reserve_bps_x100, 15);
        assert_eq!(ledger.vault_locked_profit_bps_x100, 140);
    }
}
