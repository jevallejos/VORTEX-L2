//! Lock-Free Seqlock Shared Memory Arena (SHM v12) with 12-Slot Native Telemetry.

use core::sync::atomic::{AtomicU64, Ordering};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::types::{
    BoundedScore, LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs, VortexError,
};

pub const LOB_DEPTH: usize = 50;
pub const MAX_ACTIVE_PAIRS: usize = 12;
pub const VORTEX_SHM_MAGIC: u64 = 0x564F_5254_4558_4C33;
pub const VORTEX_SHM_VERSION: u32 = 12;
pub const DEFAULT_LINUX_SHM_PATH: &str = "/dev/shm/vortex_lob.mmap";

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct BookLevel {
    pub price: PriceTicks,
    pub qty: LotQty,
}

#[repr(C, align(64))]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SymbolLobSnapshot {
    pub symbol_id: SymbolId,
    pub is_eligible: bool,
    pub sequence_id: u64,
    pub ts_exchange_ns: TimestampNs,
    pub ts_local_ns: TimestampNs,
    pub mark_price: PriceTicks,
    pub last_trade_price: PriceTicks,
    pub cum_buy_trade_lots: LotQty,
    pub cum_sell_trade_lots: LotQty,
    pub deep_bid_lots_l1_l5: LotQty,
    pub deep_ask_lots_l1_l5: LotQty,
    pub realized_vol_5m: ScaledBps,
    pub factor_ofi: BoundedScore,
    pub factor_tfi: BoundedScore,
    pub factor_geom: BoundedScore,
    pub bids: [BookLevel; LOB_DEPTH],
    pub asks: [BookLevel; LOB_DEPTH],
}

impl Default for SymbolLobSnapshot {
    #[inline]
    fn default() -> Self {
        Self::new_empty()
    }
}

impl SymbolLobSnapshot {
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self {
            symbol_id: SymbolId(0),
            is_eligible: false,
            sequence_id: 0,
            ts_exchange_ns: TimestampNs(0),
            ts_local_ns: TimestampNs(0),
            mark_price: PriceTicks(0),
            last_trade_price: PriceTicks(0),
            cum_buy_trade_lots: LotQty(0),
            cum_sell_trade_lots: LotQty(0),
            deep_bid_lots_l1_l5: LotQty(0),
            deep_ask_lots_l1_l5: LotQty(0),
            realized_vol_5m: ScaledBps(0),
            factor_ofi: BoundedScore::ZERO,
            factor_tfi: BoundedScore::ZERO,
            factor_geom: BoundedScore::ZERO,
            bids: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; LOB_DEPTH],
            asks: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; LOB_DEPTH],
        }
    }

    #[inline]
    #[must_use]
    pub const fn bbo_and_spread_ticks(&self) -> Option<(BookLevel, BookLevel, i64)> {
        let best_bid = self.bids[0];
        let best_ask = self.asks[0];
        if best_bid.price.0 <= 0
            || best_ask.price.0 <= best_bid.price.0
            || best_bid.qty.0 <= 0
            || best_ask.qty.0 <= 0
        {
            return None;
        }
        Some((best_bid, best_ask, best_ask.price.0 - best_bid.price.0))
    }

    #[inline]
    pub fn refresh_deep_l1_l5_totals(&mut self) {
        let mut b_sum = 0_i64;
        let mut a_sum = 0_i64;
        let mut i = 0_usize;
        while i < 5 {
            if self.bids[i].price.0 > 0 && self.bids[i].qty.0 > 0 {
                b_sum = b_sum.saturating_add(self.bids[i].qty.0);
            }
            if self.asks[i].price.0 > 0 && self.asks[i].qty.0 > 0 {
                a_sum = a_sum.saturating_add(self.asks[i].qty.0);
            }
            i += 1;
        }
        self.deep_bid_lots_l1_l5 = LotQty(b_sum);
        self.deep_ask_lots_l1_l5 = LotQty(a_sum);
    }

    pub fn apply_level_update(&mut self, side: Side, price: PriceTicks, qty: LotQty) {
        if price.0 <= 0 {
            return;
        }
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let mut existing_idx: Option<usize> = None;
        for (i, lvl) in levels.iter().enumerate() {
            if lvl.price.0 == price.0 {
                existing_idx = Some(i);
                break;
            }
            if lvl.price.0 == 0 {
                break;
            }
        }
        if qty.0 <= 0 {
            if let Some(idx) = existing_idx {
                let mut j = idx;
                while j + 1 < LOB_DEPTH {
                    levels[j] = levels[j + 1];
                    j += 1;
                }
                levels[LOB_DEPTH - 1] = BookLevel::default();
            }
            self.refresh_deep_l1_l5_totals();
            return;
        }
        if let Some(idx) = existing_idx {
            levels[idx].qty = qty;
            self.refresh_deep_l1_l5_totals();
            return;
        }
        let mut insert_at = LOB_DEPTH;
        for (i, lvl) in levels.iter().enumerate() {
            if lvl.price.0 == 0 {
                insert_at = i;
                break;
            }
            let better = match side {
                Side::Bid => price.0 > lvl.price.0,
                Side::Ask => price.0 < lvl.price.0,
            };
            if better {
                insert_at = i;
                break;
            }
        }
        if insert_at < LOB_DEPTH {
            let mut j = LOB_DEPTH - 1;
            while j > insert_at {
                levels[j] = levels[j - 1];
                j -= 1;
            }
            levels[insert_at] = BookLevel { price, qty };
        }
        self.refresh_deep_l1_l5_totals();
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct SlotExecTelemetry {
    pub state_code: AtomicU64,
    pub side_code: AtomicU64,
    pub entry_ticks: AtomicU64,
    pub avg_price_ticks: AtomicU64,
    pub peak_ticks: AtomicU64,
    pub tp_ticks: AtomicU64,
    pub sl_ticks: AtomicU64,
    pub open_lots: AtomicU64,
    pub initial_lots: AtomicU64,
    pub peristaltic_cycles: AtomicU64,
    pub tranche1_harvested: AtomicU64,
    pub resting_since_ns: AtomicU64,
    pub queue_ahead_lots: AtomicU64,
}

impl Default for SlotExecTelemetry {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SlotExecTelemetry {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state_code: AtomicU64::new(0),
            side_code: AtomicU64::new(0),
            entry_ticks: AtomicU64::new(0),
            avg_price_ticks: AtomicU64::new(0),
            peak_ticks: AtomicU64::new(0),
            tp_ticks: AtomicU64::new(0),
            sl_ticks: AtomicU64::new(0),
            open_lots: AtomicU64::new(0),
            initial_lots: AtomicU64::new(0),
            peristaltic_cycles: AtomicU64::new(0),
            tranche1_harvested: AtomicU64::new(0),
            resting_since_ns: AtomicU64::new(0),
            queue_ahead_lots: AtomicU64::new(0),
        }
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct SeqlockSymbolSlot {
    seq: AtomicU64,
    payload: core::cell::UnsafeCell<SymbolLobSnapshot>,
}

unsafe impl Sync for SeqlockSymbolSlot {}

impl Default for SeqlockSymbolSlot {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SeqlockSymbolSlot {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            payload: core::cell::UnsafeCell::new(SymbolLobSnapshot::new_empty()),
        }
    }

    #[inline]
    pub fn write_snapshot(&self, snap: &SymbolLobSnapshot) {
        let s0 = self.seq.load(Ordering::Relaxed);
        let s_odd = s0.wrapping_add(1);
        self.seq.store(s_odd, Ordering::Release);
        core::sync::atomic::fence(Ordering::Release);
        unsafe {
            core::ptr::write_volatile(self.payload.get(), *snap);
        }
        self.seq.store(s_odd.wrapping_add(1), Ordering::Release);
    }

    #[inline]
    pub fn read_snapshot(&self, max_retries: usize) -> Result<SymbolLobSnapshot, VortexError> {
        for _ in 0..max_retries {
            let s1 = self.seq.load(Ordering::Acquire);
            if (s1 & 1) != 0 {
                core::hint::spin_loop();
                continue;
            }
            let copy = unsafe { core::ptr::read_volatile(self.payload.get()) };
            core::sync::atomic::fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                return Ok(copy);
            }
            core::hint::spin_loop();
        }
        Err(VortexError::SharedMemorySeqConflict)
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct VortexShmArena {
    pub magic: u64,
    pub version: u32,
    pub active_pairs: u32,
    pub heartbeat_ns: AtomicU64,
    pub exec_armed_count: AtomicU64,
    pub exec_abort_count: AtomicU64,
    pub exec_fill_count: AtomicU64,
    pub exec_closed_count: AtomicU64,
    pub slot_telemetry: [SlotExecTelemetry; MAX_ACTIVE_PAIRS],
    pub slots: [SeqlockSymbolSlot; MAX_ACTIVE_PAIRS],
}

impl Default for VortexShmArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl VortexShmArena {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            magic: VORTEX_SHM_MAGIC,
            version: VORTEX_SHM_VERSION,
            active_pairs: MAX_ACTIVE_PAIRS as u32,
            heartbeat_ns: AtomicU64::new(0),
            exec_armed_count: AtomicU64::new(0),
            exec_abort_count: AtomicU64::new(0),
            exec_fill_count: AtomicU64::new(0),
            exec_closed_count: AtomicU64::new(0),
            slot_telemetry: [
                SlotExecTelemetry::new(), SlotExecTelemetry::new(), SlotExecTelemetry::new(),
                SlotExecTelemetry::new(), SlotExecTelemetry::new(), SlotExecTelemetry::new(),
                SlotExecTelemetry::new(), SlotExecTelemetry::new(), SlotExecTelemetry::new(),
                SlotExecTelemetry::new(), SlotExecTelemetry::new(), SlotExecTelemetry::new(),
            ],
            slots: [
                SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(), SeqlockSymbolSlot::new(),
            ],
        }
    }

    #[inline]
    pub fn slot(&self, idx: usize) -> Result<&SeqlockSymbolSlot, VortexError> {
        self.slots.get(idx).ok_or(VortexError::InvalidSymbolSlot)
    }

    #[inline]
    pub fn telemetry(&self, idx: usize) -> Result<&SlotExecTelemetry, VortexError> {
        self.slot_telemetry.get(idx).ok_or(VortexError::InvalidSymbolSlot)
    }
}

#[cfg(unix)]
extern "C" {
    fn mmap(
        addr: *mut core::ffi::c_void,
        length: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut core::ffi::c_void;
    fn munmap(addr: *mut core::ffi::c_void, length: usize) -> i32;
}

#[derive(Debug)]
pub struct MappedShmRegion {
    #[cfg(unix)]
    ptr: *mut VortexShmArena,
    #[cfg(unix)]
    len: usize,
    #[cfg(not(unix))]
    arena: Box<VortexShmArena>,
}

impl MappedShmRegion {
    #[must_use]
    pub fn default_shm_path() -> PathBuf {
        let dev_shm = Path::new("/dev/shm");
        if dev_shm.exists() {
            dev_shm.join("vortex_lob.mmap")
        } else {
            crate::types::portable_temp_dir().join("vortex_lob.mmap")
        }
    }

    pub fn open_or_create(path: &Path, initialize_writer: bool) -> Result<Self, VortexError> {
        let size = core::mem::size_of::<VortexShmArena>();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        file.set_len(size as u64).map_err(|_| VortexError::ColumnarIoFailure)?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            const PROT_READ: i32 = 0x1;
            const PROT_WRITE: i32 = 0x2;
            const MAP_SHARED: i32 = 0x01;
            let raw_ptr = unsafe {
                mmap(
                    core::ptr::null_mut(),
                    size,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            if raw_ptr.is_null() || (raw_ptr as usize) == usize::MAX {
                return Err(VortexError::ColumnarIoFailure);
            }
            let arena_ptr = raw_ptr.cast::<VortexShmArena>();
            unsafe {
                let current_magic = core::ptr::read_volatile(&(*arena_ptr).magic);
                let current_ver = core::ptr::read_volatile(&(*arena_ptr).version);
                if initialize_writer
                    || current_magic != VORTEX_SHM_MAGIC
                    || current_ver != VORTEX_SHM_VERSION
                {
                    core::ptr::write(arena_ptr, VortexShmArena::new());
                }
            }
            Ok(Self { ptr: arena_ptr, len: size })
        }
        #[cfg(not(unix))]
        {
            let _ = initialize_writer;
            Ok(Self { arena: Box::new(VortexShmArena::new()) })
        }
    }

    #[inline]
    #[must_use]
    pub fn arena(&self) -> &VortexShmArena {
        #[cfg(unix)]
        {
            unsafe { &*self.ptr }
        }
        #[cfg(not(unix))]
        {
            &self.arena
        }
    }
}

#[cfg(unix)]
impl Drop for MappedShmRegion {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            unsafe {
                let _ = munmap(self.ptr.cast::<core::ffi::c_void>(), self.len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_v12_12slots_and_deep_l1_l5_volume() {
        let shm_path = crate::types::portable_temp_dir().join("vortex_l3_step1_test.mmap");
        let region = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(region.is_ok());
        if let Ok(reg) = region {
            let arena = reg.arena();
            assert_eq!(arena.version, 12);
            assert_eq!(arena.active_pairs, 12);
            let mut snap = SymbolLobSnapshot::new_empty();
            snap.symbol_id = SymbolId(4);
            snap.apply_level_update(Side::Bid, PriceTicks(8_170), LotQty(200));
            snap.apply_level_update(Side::Bid, PriceTicks(8_169), LotQty(300));
            snap.apply_level_update(Side::Ask, PriceTicks(8_171), LotQty(150));
            assert_eq!(snap.deep_bid_lots_l1_l5, LotQty(500));
            assert_eq!(snap.deep_ask_lots_l1_l5, LotQty(150));
            if let Ok(slot) = arena.slot(11) {
                slot.write_snapshot(&snap);
                assert_eq!(slot.read_snapshot(4), Ok(snap));
            }
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
