//! Lock-Free Seqlock Shared Memory Arena (SHM v12) with Tear-Free 12-Slot Telemetry.

use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs::OpenOptions;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::types::{
    now_epoch_ns, BoundedScore, LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs,
    VortexError,
};

pub const LOB_DEPTH: usize = 50;
pub const MAX_ACTIVE_PAIRS: usize = 12;
pub const VORTEX_SHM_MAGIC: u64 = 0x564F_5254_4558_4C33;
pub const VORTEX_SHM_VERSION: u32 = 12;
pub const DEFAULT_LINUX_SHM_PATH: &str = "/dev/shm/vortex_lob.mmap";
pub const MAX_FEED_STALE_NS: u64 = 2_500_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum L2SeqDecision {
    ResetAndApplySnapshot,
    ApplyMonotonicDelta,
    IgnorePreSnapshotOrStaleDelta,
}

#[inline]
#[must_use]
pub const fn classify_l2_sequence_transition(
    has_snapshot: bool,
    current_seq: u64,
    is_snapshot_msg: bool,
    incoming_u: u64,
) -> L2SeqDecision {
    if is_snapshot_msg || incoming_u == 1 {
        return L2SeqDecision::ResetAndApplySnapshot;
    }
    if !has_snapshot || incoming_u == 0 || incoming_u <= current_seq {
        return L2SeqDecision::IgnorePreSnapshotOrStaleDelta;
    }
    L2SeqDecision::ApplyMonotonicDelta
}

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
    pub const fn is_fresh_at(&self, now_ns: TimestampNs, max_stale_ns: u64) -> bool {
        if self.sequence_id == 0 || self.ts_local_ns.0 <= 0 || now_ns.0 <= 0 {
            return false;
        }
        let age = if now_ns.0 >= self.ts_local_ns.0 {
            (now_ns.0 - self.ts_local_ns.0) as u64
        } else {
            0
        };
        age <= max_stale_ns
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
            let Some(idx) = existing_idx else {
                return;
            };
            let mut j = idx;
            while j + 1 < LOB_DEPTH {
                levels[j] = levels[j + 1];
                j += 1;
            }
            levels[LOB_DEPTH - 1] = BookLevel::default();
            self.refresh_deep_l1_l5_totals();
            return;
        }
        if let Some(idx) = existing_idx {
            levels[idx].qty = qty;
            self.purge_crossed_levels(side, price);
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
        self.purge_crossed_levels(side, price);
        self.refresh_deep_l1_l5_totals();
    }

    #[inline]
    fn purge_crossed_levels(&mut self, updated_side: Side, price: PriceTicks) {
        match updated_side {
            Side::Bid => {
                while self.asks[0].price.0 > 0 && self.asks[0].price.0 <= price.0 {
                    let mut k = 0_usize;
                    while k + 1 < LOB_DEPTH {
                        self.asks[k] = self.asks[k + 1];
                        k += 1;
                    }
                    self.asks[LOB_DEPTH - 1] = BookLevel::default();
                }
            }
            Side::Ask => {
                while self.bids[0].price.0 > 0 && self.bids[0].price.0 >= price.0 {
                    let mut k = 0_usize;
                    while k + 1 < LOB_DEPTH {
                        self.bids[k] = self.bids[k + 1];
                        k += 1;
                    }
                    self.bids[LOB_DEPTH - 1] = BookLevel::default();
                }
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotTelemetrySnapshot {
    pub state_code: u8,
    pub side_code: u64,
    pub entry_ticks: i64,
    pub avg_price_ticks: i64,
    pub peak_ticks: i64,
    pub tp_ticks: i64,
    pub sl_ticks: i64,
    pub open_lots: i64,
    pub initial_lots: i64,
    pub lvdbs_surfs: u64,
    pub tranche1_harvested: bool,
    pub resting_since_ns: u64,
    pub queue_ahead_lots: i64,
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
    pub lvdbs_surfs: AtomicU64,
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
            lvdbs_surfs: AtomicU64::new(0),
            tranche1_harvested: AtomicU64::new(0),
            resting_since_ns: AtomicU64::new(0),
            queue_ahead_lots: AtomicU64::new(0),
        }
    }

    #[inline]
    #[must_use]
    pub fn peek_state_code(&self) -> u8 {
        (self.state_code.load(Ordering::Acquire) & 0xFF) as u8
    }

    #[inline]
    pub fn heal_if_odd_locked(&self) {
        let raw = self.state_code.load(Ordering::Acquire);
        if ((raw >> 8) & 1) != 0 {
            self.write_consistent(&SlotTelemetrySnapshot::default());
        }
    }

    #[inline]
    pub fn store_queue_ahead_lots(&self, qty: LotQty) {
        self.queue_ahead_lots
            .store(qty.0.max(0) as u64, Ordering::Release);
    }

    #[inline]
    #[must_use]
    pub fn queue_ahead_lots_relaxed(&self) -> i64 {
        self.queue_ahead_lots.load(Ordering::Relaxed) as i64
    }

    #[inline]
    pub fn write_consistent(&self, snap: &SlotTelemetrySnapshot) {
        let prev = self.state_code.load(Ordering::Relaxed);
        let seq_odd = ((prev >> 8).wrapping_add(1)) | 1;
        let code_u64 = u64::from(snap.state_code);
        self.state_code
            .store((seq_odd << 8) | code_u64, Ordering::Release);
        core::sync::atomic::fence(Ordering::SeqCst);

        self.side_code.store(snap.side_code, Ordering::Relaxed);
        self.entry_ticks
            .store(snap.entry_ticks.max(0) as u64, Ordering::Relaxed);
        self.avg_price_ticks
            .store(snap.avg_price_ticks.max(0) as u64, Ordering::Relaxed);
        self.peak_ticks
            .store(snap.peak_ticks.max(0) as u64, Ordering::Relaxed);
        self.tp_ticks
            .store(snap.tp_ticks.max(0) as u64, Ordering::Relaxed);
        self.sl_ticks
            .store(snap.sl_ticks.max(0) as u64, Ordering::Relaxed);
        self.open_lots
            .store(snap.open_lots.max(0) as u64, Ordering::Relaxed);
        self.initial_lots
            .store(snap.initial_lots.max(0) as u64, Ordering::Relaxed);
        self.lvdbs_surfs.store(snap.lvdbs_surfs, Ordering::Relaxed);
        self.tranche1_harvested
            .store(u64::from(snap.tranche1_harvested), Ordering::Relaxed);
        self.resting_since_ns
            .store(snap.resting_since_ns, Ordering::Relaxed);
        self.queue_ahead_lots
            .store(snap.queue_ahead_lots.max(0) as u64, Ordering::Relaxed);

        core::sync::atomic::fence(Ordering::Release);
        let seq_even = seq_odd.wrapping_add(1);
        self.state_code
            .store((seq_even << 8) | code_u64, Ordering::Release);
    }

    #[inline]
    pub fn read_consistent(
        &self,
        max_retries: usize,
    ) -> Result<SlotTelemetrySnapshot, VortexError> {
        let tries = max_retries.max(1);
        for _ in 0..tries {
            let s1 = self.state_code.load(Ordering::Acquire);
            if ((s1 >> 8) & 1) != 0 {
                core::hint::spin_loop();
                continue;
            }
            let snap = SlotTelemetrySnapshot {
                state_code: (s1 & 0xFF) as u8,
                side_code: self.side_code.load(Ordering::Relaxed),
                entry_ticks: self.entry_ticks.load(Ordering::Relaxed) as i64,
                avg_price_ticks: self.avg_price_ticks.load(Ordering::Relaxed) as i64,
                peak_ticks: self.peak_ticks.load(Ordering::Relaxed) as i64,
                tp_ticks: self.tp_ticks.load(Ordering::Relaxed) as i64,
                sl_ticks: self.sl_ticks.load(Ordering::Relaxed) as i64,
                open_lots: self.open_lots.load(Ordering::Relaxed) as i64,
                initial_lots: self.initial_lots.load(Ordering::Relaxed) as i64,
                lvdbs_surfs: self.lvdbs_surfs.load(Ordering::Relaxed),
                tranche1_harvested: self.tranche1_harvested.load(Ordering::Relaxed) != 0,
                resting_since_ns: self.resting_since_ns.load(Ordering::Relaxed),
                queue_ahead_lots: self.queue_ahead_lots.load(Ordering::Relaxed) as i64,
            };
            core::sync::atomic::fence(Ordering::Acquire);
            let s2 = self.state_code.load(Ordering::Acquire);
            if s1 == s2 {
                return Ok(snap);
            }
            core::hint::spin_loop();
        }
        Err(VortexError::SharedMemorySeqConflict)
    }

    #[inline]
    #[must_use]
    pub fn read_consistent_or_default(&self, max_retries: usize) -> SlotTelemetrySnapshot {
        self.read_consistent(max_retries).unwrap_or_default()
    }
}
#[repr(C, align(64))]
#[derive(Debug)]
pub struct SeqlockSymbolSlot {
    seq: AtomicU64,
    payload: core::cell::UnsafeCell<MaybeUninit<SymbolLobSnapshot>>,
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
            payload: core::cell::UnsafeCell::new(MaybeUninit::new(
                SymbolLobSnapshot::new_empty(),
            )),
        }
    }

    #[inline]
    pub fn heal_if_odd_locked(&self) {
        if (self.seq.load(Ordering::Acquire) & 1) != 0 {
            self.write_snapshot(&SymbolLobSnapshot::new_empty());
        }
    }

    #[inline]
    pub fn write_snapshot(&self, snap: &SymbolLobSnapshot) {
        let s0 = self.seq.load(Ordering::Relaxed);
        let s_odd = s0.wrapping_add(1) | 1;
        self.seq.store(s_odd, Ordering::Release);
        core::sync::atomic::fence(Ordering::SeqCst);
        unsafe {
            core::ptr::write_volatile(self.payload.get(), MaybeUninit::new(*snap));
        }
        core::sync::atomic::fence(Ordering::Release);
        self.seq.store(s_odd.wrapping_add(1), Ordering::Release);
    }

    #[inline]
    pub fn read_snapshot(&self, max_retries: usize) -> Result<SymbolLobSnapshot, VortexError> {
        let tries = max_retries.max(1);
        for _ in 0..tries {
            let s1 = self.seq.load(Ordering::Acquire);
            if (s1 & 1) != 0 {
                core::hint::spin_loop();
                continue;
            }
            let mut raw_copy = unsafe { core::ptr::read_volatile(self.payload.get()) };
            core::sync::atomic::fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                let base_ptr = raw_copy.as_mut_ptr().cast::<u8>();
                let elig_offset = core::mem::offset_of!(SymbolLobSnapshot, is_eligible);
                unsafe {
                    let b_ptr = base_ptr.add(elig_offset);
                    if core::ptr::read(b_ptr) > 1 {
                        core::ptr::write(b_ptr, 1_u8);
                    }
                    return Ok(raw_copy.assume_init());
                }
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
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
                SlotExecTelemetry::new(),
            ],
            slots: [
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
                SeqlockSymbolSlot::new(),
            ],
        }
    }

    #[inline]
    pub fn slot(&self, idx: usize) -> Result<&SeqlockSymbolSlot, VortexError> {
        self.slots.get(idx).ok_or(VortexError::InvalidSymbolSlot)
    }

    #[inline]
    pub fn telemetry(&self, idx: usize) -> Result<&SlotExecTelemetry, VortexError> {
        self.slot_telemetry
            .get(idx)
            .ok_or(VortexError::InvalidSymbolSlot)
    }

    #[inline]
    #[must_use]
    pub fn heartbeat_age_ns(&self, now_ns: TimestampNs) -> Option<u64> {
        let hb = self.heartbeat_ns.load(Ordering::Acquire);
        if hb == 0 || now_ns.0 <= 0 {
            return None;
        }
        Some((now_ns.0 as u64).saturating_sub(hb))
    }

    #[inline]
    #[must_use]
    pub fn is_feed_fresh(&self, now_ns: TimestampNs, max_stale_ns: u64) -> bool {
        match self.heartbeat_age_ns(now_ns) {
            Some(age) => age <= max_stale_ns,
            None => false,
        }
    }

    #[inline]
    pub fn heal_poisoned_slots(&self) {
        for slot in &self.slots {
            slot.heal_if_odd_locked();
        }
        for tel in &self.slot_telemetry {
            tel.heal_if_odd_locked();
        }
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
    #[cfg(not(unix))]
    backing_path: Option<PathBuf>,
}

impl MappedShmRegion {
    #[must_use]
    pub fn default_shm_path() -> PathBuf {
        let dev_shm = Path::new("/dev/shm");
        if dev_shm.exists() {
            PathBuf::from(DEFAULT_LINUX_SHM_PATH)
        } else {
            std::env::temp_dir().join("vortex_lob.mmap")
        }
    }

    #[must_use]
    pub fn ephemeral_self_test_path(tag: &str) -> PathBuf {
        let ts = now_epoch_ns().0.unsigned_abs();
        let pid = std::process::id();
        std::env::temp_dir().join(format!("vortex_l3_{}_{}_{}.mmap", tag, pid, ts))
    }

    #[cfg(not(unix))]
    fn alloc_zero_padded_arena() -> Box<VortexShmArena> {
        let mut uninit: Box<MaybeUninit<VortexShmArena>> = Box::new(MaybeUninit::zeroed());
        unsafe {
            let mut fresh = VortexShmArena::new();
            fresh.magic = VORTEX_SHM_MAGIC;
            core::ptr::write(uninit.as_mut_ptr(), fresh);
            uninit.assume_init()
        }
    }

    pub fn open_existing_read_only(path: &Path) -> Result<Self, VortexError> {
        let size = core::mem::size_of::<VortexShmArena>();
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let cur_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if cur_len != (size as u64) {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        #[cfg(unix)]
        {
            let _ = &mut file;
            use std::os::unix::io::AsRawFd;
            const PROT_READ: i32 = 0x1;
            const MAP_SHARED: i32 = 0x01;
            let raw_ptr = unsafe {
                mmap(
                    core::ptr::null_mut(),
                    size,
                    PROT_READ,
                    MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            if raw_ptr.is_null() || (raw_ptr as usize) == usize::MAX {
                return Err(VortexError::ColumnarIoFailure);
            }
            let arena_ptr = raw_ptr.cast::<VortexShmArena>();
            let valid = unsafe {
                let mg = core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).magic));
                core::sync::atomic::fence(Ordering::Acquire);
                let vr = core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).version));
                let ap =
                    core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).active_pairs));
                mg == VORTEX_SHM_MAGIC
                    && vr == VORTEX_SHM_VERSION
                    && ap == (MAX_ACTIVE_PAIRS as u32)
            };
            if !valid {
                unsafe {
                    let _ = munmap(raw_ptr, size);
                }
                return Err(VortexError::ColumnarFormatCorrupted);
            }
            Ok(Self { ptr: arena_ptr, len: size })
        }
        #[cfg(not(unix))]
        {
            let mut uninit: Box<MaybeUninit<VortexShmArena>> =
                Box::new(MaybeUninit::zeroed());
            let dst = unsafe {
                core::slice::from_raw_parts_mut(uninit.as_mut_ptr().cast::<u8>(), size)
            };
            file.read_exact(dst).map_err(|_| VortexError::ColumnarIoFailure)?;
            let arena = unsafe { uninit.assume_init() };
            if arena.magic != VORTEX_SHM_MAGIC
                || arena.version != VORTEX_SHM_VERSION
                || arena.active_pairs != (MAX_ACTIVE_PAIRS as u32)
            {
                return Err(VortexError::ColumnarFormatCorrupted);
            }
            Ok(Self { arena, backing_path: None })
        }
    }

    pub fn open_or_create(path: &Path, initialize_writer: bool) -> Result<Self, VortexError> {
        let size = core::mem::size_of::<VortexShmArena>();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let cur_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if cur_len != (size as u64) {
            file.set_len(size as u64)
                .map_err(|_| VortexError::ColumnarIoFailure)?;
        }
        #[cfg(unix)]
        {
            let _ = &mut file;
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
                let current_magic =
                    core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).magic));
                core::sync::atomic::fence(Ordering::Acquire);
                let current_ver =
                    core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).version));
                let current_pairs =
                    core::ptr::read_volatile(core::ptr::addr_of!((*arena_ptr).active_pairs));
                let header_valid = cur_len == (size as u64)
                    && current_magic == VORTEX_SHM_MAGIC
                    && current_ver == VORTEX_SHM_VERSION
                    && current_pairs == (MAX_ACTIVE_PAIRS as u32);
                if !header_valid {
                    core::ptr::write_volatile(
                        core::ptr::addr_of_mut!((*arena_ptr).magic),
                        0_u64,
                    );
                    core::sync::atomic::fence(Ordering::SeqCst);
                    let mut fresh = VortexShmArena::new();
                    fresh.magic = 0;
                    core::ptr::write(arena_ptr, fresh);
                    core::sync::atomic::fence(Ordering::Release);
                    core::ptr::write_volatile(
                        core::ptr::addr_of_mut!((*arena_ptr).magic),
                        VORTEX_SHM_MAGIC,
                    );
                    core::sync::atomic::fence(Ordering::SeqCst);
                } else if initialize_writer {
                    (*arena_ptr).heal_poisoned_slots();
                }
            }
            Ok(Self { ptr: arena_ptr, len: size })
        }
        #[cfg(not(unix))]
        {
            let mut loaded_valid: Option<Box<VortexShmArena>> = None;
            if cur_len == (size as u64) {
                let mut uninit: Box<MaybeUninit<VortexShmArena>> =
                    Box::new(MaybeUninit::zeroed());
                let dst = unsafe {
                    core::slice::from_raw_parts_mut(uninit.as_mut_ptr().cast::<u8>(), size)
                };
                if file.seek(SeekFrom::Start(0)).is_ok() && file.read_exact(dst).is_ok() {
                    let cand = unsafe { uninit.assume_init() };
                    if cand.magic == VORTEX_SHM_MAGIC
                        && cand.version == VORTEX_SHM_VERSION
                        && cand.active_pairs == (MAX_ACTIVE_PAIRS as u32)
                    {
                        if initialize_writer {
                            cand.heal_poisoned_slots();
                        }
                        loaded_valid = Some(cand);
                    }
                }
            }
            let arena = match loaded_valid {
                Some(existing) => existing,
                None => {
                    let fresh = Self::alloc_zero_padded_arena();
                    let bytes = unsafe {
                        core::slice::from_raw_parts(
                            (&*fresh as *const VortexShmArena).cast::<u8>(),
                            size,
                        )
                    };
                    let _ = file.seek(SeekFrom::Start(0));
                    let _ = file.write_all(bytes);
                    fresh
                }
            };
            Ok(Self {
                arena,
                backing_path: Some(path.to_path_buf()),
            })
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

#[cfg(not(unix))]
impl Drop for MappedShmRegion {
    fn drop(&mut self) {
        if let Some(ref path) = self.backing_path {
            if path.exists() {
                if let Ok(mut f) = OpenOptions::new().write(true).open(path) {
                    let size = core::mem::size_of::<VortexShmArena>();
                    let bytes = unsafe {
                        core::slice::from_raw_parts(
                            (&*self.arena as *const VortexShmArena).cast::<u8>(),
                            size,
                        )
                    };
                    let _ = f.seek(SeekFrom::Start(0));
                    let _ = f.write_all(bytes);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_v12_12slots_and_deep_l1_l5_volume() {
        let shm_path = MappedShmRegion::ephemeral_self_test_path("step1_v12");
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

    #[test]
    fn test_seqlock_odd_recovery_and_ask_cross_purge_zero_guard() {
        let slot = SeqlockSymbolSlot::new();
        slot.seq.store(7, Ordering::Release);
        assert_eq!(slot.read_snapshot(2), Err(VortexError::SharedMemorySeqConflict));
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = SymbolId(6);
        snap.is_eligible = true;
        snap.apply_level_update(Side::Ask, PriceTicks(8_510), LotQty(120));
        snap.apply_level_update(Side::Bid, PriceTicks(8_512), LotQty(90));
        snap.apply_level_update(Side::Ask, PriceTicks(8_509), LotQty(80));
        assert_eq!(snap.bids[0].price, PriceTicks(0));
        assert_eq!(snap.asks[0].price, PriceTicks(8_509));
        slot.write_snapshot(&snap);
        assert_eq!(slot.seq.load(Ordering::Acquire) & 1, 0);
        assert_eq!(slot.read_snapshot(4), Ok(snap));
    }

    #[test]
    fn test_shm_hot_reattach_read_only_and_tear_free_telemetry_f09() {
        let shm_path = MappedShmRegion::ephemeral_self_test_path("hot_reattach_f09");
        {
            let reg1 = MappedShmRegion::open_or_create(&shm_path, true);
            assert!(reg1.is_ok());
            if let Ok(r1) = reg1 {
                let a1 = r1.arena();
                a1.exec_closed_count.store(42, Ordering::Release);
                a1.heartbeat_ns.store(1_700_000_000_000_000_000, Ordering::Release);
                let mut snap0 = SymbolLobSnapshot::new_empty();
                snap0.symbol_id = SymbolId(1);
                snap0.sequence_id = 99_123;
                snap0.ts_local_ns = TimestampNs(1_700_000_000_000_000_000);
                snap0.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(200) };
                snap0.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(180) };
                if let Ok(s0) = a1.slot(0) {
                    s0.write_snapshot(&snap0);
                }
                if let Ok(s1) = a1.slot(1) {
                    s1.seq.store(13, Ordering::Release);
                }
                if let Ok(t0) = a1.telemetry(0) {
                    for cyc in 1..=3_u64 {
                        t0.write_consistent(&SlotTelemetrySnapshot {
                            state_code: 4,
                            side_code: 0,
                            entry_ticks: 759_000,
                            avg_price_ticks: 759_000,
                            peak_ticks: 759_120,
                            tp_ticks: 759_400,
                            sl_ticks: 750_000,
                            open_lots: 1,
                            initial_lots: 2,
                            lvdbs_surfs: cyc,
                            tranche1_harvested: true,
                            resting_since_ns: 1_700_000_000_000_000_000,
                            queue_ahead_lots: 0,
                        });
                    }
                    assert_eq!(t0.peek_state_code(), 4);
                }
                if let Ok(t1) = a1.telemetry(1) {
                    t1.state_code.store((5_u64 << 8) | 3, Ordering::Release);
                }
            }
        }
        {
            let ro = MappedShmRegion::open_existing_read_only(&shm_path);
            assert!(ro.is_ok());
            if let Ok(ro_reg) = ro {
                assert!(ro_reg.arena().is_feed_fresh(
                    TimestampNs(1_700_000_001_000_000_000),
                    MAX_FEED_STALE_NS,
                ));
                if let Ok(t0) = ro_reg.arena().telemetry(0) {
                    let snap = t0.read_consistent_or_default(4);
                    assert_eq!(snap.state_code, 4);
                    assert_eq!(snap.lvdbs_surfs, 3);
                    assert!(snap.tranche1_harvested);
                }
            }
        }
        {
            let reg2 = MappedShmRegion::open_or_create(&shm_path, true);
            assert!(reg2.is_ok());
            if let Ok(r2) = reg2 {
                let a2 = r2.arena();
                assert_eq!(a2.exec_closed_count.load(Ordering::Acquire), 42);
                if let Ok(s1) = a2.slot(1) {
                    assert_eq!(s1.seq.load(Ordering::Acquire) & 1, 0);
                    assert!(s1.read_snapshot(4).is_ok());
                }
                if let Ok(t1) = a2.telemetry(1) {
                    assert_eq!((t1.state_code.load(Ordering::Acquire) >> 8) & 1, 0);
                    assert_eq!(t1.peek_state_code(), 0);
                    assert!(t1.read_consistent(4).is_ok());
                }
                if let Ok(t0) = a2.telemetry(0) {
                    t0.store_queue_ahead_lots(LotQty(77));
                    assert_eq!(t0.queue_ahead_lots_relaxed(), 77);
                    let ts = t0.read_consistent(4);
                    assert!(ts.is_ok());
                    if let Ok(tel_snap) = ts {
                        assert_eq!(tel_snap.state_code, 4);
                        assert_eq!(tel_snap.avg_price_ticks, 759_000);
                        assert_eq!(tel_snap.lvdbs_surfs, 3);
                    }
                }
            }
        }
        let _ = std::fs::remove_file(&shm_path);
    }

    #[test]
    fn test_l2_sequence_transition_and_telemetry_seqlock_odd_guard() {
        assert_eq!(
            classify_l2_sequence_transition(false, 0, false, 500),
            L2SeqDecision::IgnorePreSnapshotOrStaleDelta
        );
        assert_eq!(
            classify_l2_sequence_transition(false, 0, true, 500),
            L2SeqDecision::ResetAndApplySnapshot
        );
        assert_eq!(
            classify_l2_sequence_transition(true, 500, false, 500),
            L2SeqDecision::IgnorePreSnapshotOrStaleDelta
        );
        assert_eq!(
            classify_l2_sequence_transition(true, 500, false, 501),
            L2SeqDecision::ApplyMonotonicDelta
        );
        assert_eq!(
            classify_l2_sequence_transition(true, 999, false, 1),
            L2SeqDecision::ResetAndApplySnapshot
        );
        let tel = SlotExecTelemetry::new();
        tel.state_code.store((1_u64 << 8) | 2, Ordering::Release);
        assert_eq!(tel.peek_state_code(), 2);
        assert_eq!(tel.read_consistent(2), Err(VortexError::SharedMemorySeqConflict));
    }
}
