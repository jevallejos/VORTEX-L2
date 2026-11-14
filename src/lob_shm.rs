//! Lock-Free 12-Slot Seqlock LOB Shared Memory Arena (SHM v13) & Tear-Free Telemetry.

#![allow(
    clippy::borrow_as_ptr,
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::ptr_as_ptr,
    clippy::too_many_arguments
)]

use core::cell::UnsafeCell;
use core::ffi::c_void;
use core::ptr;
use core::sync::atomic::{fence, AtomicI64, AtomicU64, Ordering};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crate::types::{
    now_epoch_ns, BoundedScore, LotQty, PriceTicks, ScaledBps, Side, SymbolId, TimestampNs,
    VortexError,
};
use crate::wave_elliott::{WaveObservables, WaveRegime};

pub const MAX_ACTIVE_PAIRS: usize = 12;
pub const VORTEX_SHM_VERSION: u32 = 13;
const VORTEX_SHM_MAGIC: u64 = 0x564F_5254_4558_4C33;

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct BookLevel {
    pub price: PriceTicks,
    pub qty: LotQty,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum L2SeqDecision {
    ResetAndApplySnapshot,
    ApplyMonotonicDelta,
    IgnorePreSnapshotOrStaleDelta,
}

#[inline]
#[must_use]
pub const fn classify_l2_sequence_transition(
    has_book_snapshot: bool,
    prev_sequence_id: u64,
    is_snapshot_msg: bool,
    incoming_u: u64,
) -> L2SeqDecision {
    if is_snapshot_msg || incoming_u == 1 {
        L2SeqDecision::ResetAndApplySnapshot
    } else if !has_book_snapshot || incoming_u <= prev_sequence_id {
        L2SeqDecision::IgnorePreSnapshotOrStaleDelta
    } else {
        L2SeqDecision::ApplyMonotonicDelta
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct SymbolLobSnapshot {
    pub symbol_id: SymbolId,
    pub is_eligible: bool,
    pub wave_regime_code: u8,
    pub wave_climax_active: bool,
    pub sequence_id: u64,
    pub ts_exchange_ns: TimestampNs,
    pub ts_local_ns: TimestampNs,
    pub bids: [BookLevel; 50],
    pub asks: [BookLevel; 50],
    pub mark_price: PriceTicks,
    pub last_trade_price: PriceTicks,
    pub realized_vol_5m: ScaledBps,
    pub factor_ofi: BoundedScore,
    pub factor_tfi: BoundedScore,
    pub factor_geom: BoundedScore,
    pub deep_bid_lots_l1_l5: LotQty,
    pub deep_ask_lots_l1_l5: LotQty,
    pub cum_buy_trade_lots: LotQty,
    pub cum_sell_trade_lots: LotQty,
    pub wave_hurst: BoundedScore,
    pub wave_macro_phase: BoundedScore,
    pub wave_oi_impulse: BoundedScore,
    pub wave_liq_imbalance: BoundedScore,
    pub wave_fib_proj_bps: ScaledBps,
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
            wave_regime_code: 0,
            wave_climax_active: false,
            sequence_id: 0,
            ts_exchange_ns: TimestampNs(0),
            ts_local_ns: TimestampNs(0),
            bids: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; 50],
            asks: [BookLevel { price: PriceTicks(0), qty: LotQty(0) }; 50],
            mark_price: PriceTicks(0),
            last_trade_price: PriceTicks(0),
            realized_vol_5m: ScaledBps(0),
            factor_ofi: BoundedScore::ZERO,
            factor_tfi: BoundedScore::ZERO,
            factor_geom: BoundedScore::ZERO,
            deep_bid_lots_l1_l5: LotQty(0),
            deep_ask_lots_l1_l5: LotQty(0),
            cum_buy_trade_lots: LotQty(0),
            cum_sell_trade_lots: LotQty(0),
            wave_hurst: BoundedScore::ZERO,
            wave_macro_phase: BoundedScore::ZERO,
            wave_oi_impulse: BoundedScore::ZERO,
            wave_liq_imbalance: BoundedScore::ZERO,
            wave_fib_proj_bps: ScaledBps(0),
        }
    }

    #[inline]
    pub fn set_wave_observables(&mut self, obs: WaveObservables) {
        self.wave_regime_code = obs.regime as u8;
        self.wave_climax_active = obs.climax_active;
        self.wave_hurst = obs.hurst;
        self.wave_macro_phase = obs.macro_phase;
        self.wave_oi_impulse = obs.oi_impulse;
        self.wave_liq_imbalance = obs.liq_imbalance;
        self.wave_fib_proj_bps = obs.fib_proj_bps;
    }

    #[inline]
    #[must_use]
    pub const fn wave_observables(&self) -> WaveObservables {
        WaveObservables {
            regime: WaveRegime::from_code(self.wave_regime_code),
            climax_active: self.wave_climax_active,
            hurst: self.wave_hurst,
            macro_phase: self.wave_macro_phase,
            oi_impulse: self.wave_oi_impulse,
            liq_imbalance: self.wave_liq_imbalance,
            fib_proj_bps: self.wave_fib_proj_bps,
        }
    }

    #[inline]
    pub fn refresh_deep_l1_l5_totals(&mut self) {
        let mut bid_sum = 0_i64;
        let mut ask_sum = 0_i64;
        for i in 0..5 {
            if self.bids[i].price.0 > 0 && self.bids[i].qty.0 > 0 {
                bid_sum = bid_sum.saturating_add(self.bids[i].qty.0);
            }
            if self.asks[i].price.0 > 0 && self.asks[i].qty.0 > 0 {
                ask_sum = ask_sum.saturating_add(self.asks[i].qty.0);
            }
        }
        self.deep_bid_lots_l1_l5 = LotQty(bid_sum);
        self.deep_ask_lots_l1_l5 = LotQty(ask_sum);
    }

    #[inline]
    #[must_use]
    pub const fn bbo_and_spread_ticks(&self) -> Option<(BookLevel, BookLevel, i64)> {
        let b0 = self.bids[0];
        let a0 = self.asks[0];
        if b0.price.0 <= 0 || b0.qty.0 <= 0 || a0.price.0 <= b0.price.0 || a0.qty.0 <= 0 {
            return None;
        }
        Some((b0, a0, a0.price.0 - b0.price.0))
    }

    pub fn apply_level_update(&mut self, side: Side, price: PriceTicks, qty: LotQty) {
        if price.0 <= 0 {
            return;
        }
        match side {
            Side::Bid => {
                if qty.0 <= 0 {
                    if let Some(pos) = self.bids.iter().position(|l| l.price == price) {
                        for i in pos..49 {
                            self.bids[i] = self.bids[i + 1];
                        }
                        self.bids[49] = BookLevel::default();
                    }
                } else {
                    if let Some(pos) = self.bids.iter().position(|l| l.price == price) {
                        self.bids[pos].qty = qty;
                    } else if let Some(ins) = self
                        .bids
                        .iter()
                        .position(|l| l.qty.0 <= 0 || l.price.0 < price.0)
                    {
                        for i in (ins + 1..50).rev() {
                            self.bids[i] = self.bids[i - 1];
                        }
                        self.bids[ins] = BookLevel { price, qty };
                    }
                    let mut shift = 0_usize;
                    while shift < 50
                        && self.asks[shift].price.0 > 0
                        && self.asks[shift].price.0 <= price.0
                    {
                        shift += 1;
                    }
                    if shift > 0 {
                        for i in 0..(50 - shift) {
                            self.asks[i] = self.asks[i + shift];
                        }
                        for i in (50 - shift)..50 {
                            self.asks[i] = BookLevel::default();
                        }
                    }
                }
            }
            Side::Ask => {
                if qty.0 <= 0 {
                    if let Some(pos) = self.asks.iter().position(|l| l.price == price) {
                        for i in pos..49 {
                            self.asks[i] = self.asks[i + 1];
                        }
                        self.asks[49] = BookLevel::default();
                    }
                } else {
                    if let Some(pos) = self.asks.iter().position(|l| l.price == price) {
                        self.asks[pos].qty = qty;
                    } else if let Some(ins) = self
                        .asks
                        .iter()
                        .position(|l| l.qty.0 <= 0 || l.price.0 > price.0)
                    {
                        for i in (ins + 1..50).rev() {
                            self.asks[i] = self.asks[i - 1];
                        }
                        self.asks[ins] = BookLevel { price, qty };
                    }
                    let mut shift = 0_usize;
                    while shift < 50 && self.bids[shift].price.0 >= price.0 {
                        shift += 1;
                    }
                    if shift > 0 {
                        for i in 0..(50 - shift) {
                            self.bids[i] = self.bids[i + shift];
                        }
                        for i in (50 - shift)..50 {
                            self.bids[i] = BookLevel::default();
                        }
                    }
                }
            }
        }
        self.refresh_deep_l1_l5_totals();
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotTelemetrySnapshot {
    pub state_code: u8,
    pub tranche1_harvested: bool,
    pub side_code: u64,
    pub entry_ticks: i64,
    pub avg_price_ticks: i64,
    pub peak_ticks: i64,
    pub tp_ticks: i64,
    pub sl_ticks: i64,
    pub open_lots: i64,
    pub initial_lots: i64,
    pub lvdbs_surfs: u64,
    pub resting_since_ns: u64,
    pub queue_ahead_lots: i64,
}

#[repr(C, align(64))]
pub struct SlotTelemetryCell {
    seq: AtomicU64,
    queue_ahead_atomic: AtomicI64,
    payload: UnsafeCell<SlotTelemetrySnapshot>,
}

impl core::fmt::Debug for SlotTelemetryCell {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotTelemetryCell")
            .field("seq", &self.seq.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SlotTelemetryCell {
    #[inline]
    pub fn store_queue_ahead_lots(&self, lots: LotQty) {
        self.queue_ahead_atomic.store(lots.0, Ordering::Relaxed);
    }

    #[inline]
    #[must_use]
    pub fn queue_ahead_lots_relaxed(&self) -> i64 {
        self.queue_ahead_atomic.load(Ordering::Relaxed)
    }

    pub fn write_consistent(&self, snap: &SlotTelemetrySnapshot) {
        let prev = self.seq.load(Ordering::Relaxed);
        let odd = if (prev & 1) == 0 {
            prev.wrapping_add(1)
        } else {
            prev.wrapping_add(2)
        };
        self.seq.store(odd, Ordering::Release);
        fence(Ordering::AcqRel);
        unsafe {
            ptr::write_volatile(self.payload.get(), *snap);
        }
        self.queue_ahead_atomic
            .store(snap.queue_ahead_lots, Ordering::Relaxed);
        self.seq.store(odd.wrapping_add(1), Ordering::Release);
    }

    #[must_use]
    pub fn read_consistent_or_default(&self, max_retries: usize) -> SlotTelemetrySnapshot {
        let tries = max_retries.max(1);
        for _ in 0..tries {
            let s1 = self.seq.load(Ordering::Acquire);
            if (s1 & 1) != 0 {
                core::hint::spin_loop();
                continue;
            }
            let mut val = unsafe { ptr::read_volatile(self.payload.get()) };
            fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                if val.state_code == 1 || val.state_code == 2 {
                    val.queue_ahead_lots = self.queue_ahead_atomic.load(Ordering::Relaxed);
                }
                return val;
            }
        }
        SlotTelemetrySnapshot::default()
    }
}

#[repr(C, align(64))]
pub struct SeqlockLobSlot {
    seq: AtomicU64,
    payload: UnsafeCell<SymbolLobSnapshot>,
}

impl core::fmt::Debug for SeqlockLobSlot {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SeqlockLobSlot")
            .field("seq", &self.seq.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SeqlockLobSlot {
    pub fn write_snapshot(&self, snap: &SymbolLobSnapshot) {
        let prev = self.seq.load(Ordering::Relaxed);
        let odd = if (prev & 1) == 0 {
            prev.wrapping_add(1)
        } else {
            prev.wrapping_add(2)
        };
        self.seq.store(odd, Ordering::Release);
        fence(Ordering::AcqRel);
        unsafe {
            ptr::write_volatile(self.payload.get(), *snap);
        }
        self.seq.store(odd.wrapping_add(1), Ordering::Release);
    }

    pub fn read_snapshot(&self, max_retries: usize) -> Result<SymbolLobSnapshot, VortexError> {
        let tries = max_retries.max(1);
        for _ in 0..tries {
            let s1 = self.seq.load(Ordering::Acquire);
            if (s1 & 1) != 0 {
                core::hint::spin_loop();
                continue;
            }
            let snap = unsafe { ptr::read_volatile(self.payload.get()) };
            fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                return Ok(snap);
            }
        }
        Err(VortexError::SharedMemorySeqConflict)
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct VortexShmArena {
    pub magic: u64,
    pub version: u32,
    pub heartbeat_ns: AtomicU64,
    pub exec_armed_count: AtomicU64,
    pub exec_abort_count: AtomicU64,
    pub exec_fill_count: AtomicU64,
    pub exec_closed_count: AtomicU64,
    slots: [SeqlockLobSlot; MAX_ACTIVE_PAIRS],
    telemetry_cells: [SlotTelemetryCell; MAX_ACTIVE_PAIRS],
}

unsafe impl Sync for VortexShmArena {}
unsafe impl Send for VortexShmArena {}

impl VortexShmArena {
    #[inline]
    pub fn slot(&self, slot_idx: usize) -> Result<&SeqlockLobSlot, VortexError> {
        self.slots.get(slot_idx).ok_or(VortexError::InvalidSymbolSlot)
    }

    #[inline]
    pub fn telemetry(&self, slot_idx: usize) -> Result<&SlotTelemetryCell, VortexError> {
        self.telemetry_cells
            .get(slot_idx)
            .ok_or(VortexError::InvalidSymbolSlot)
    }

    #[inline]
    #[must_use]
    pub fn heartbeat_age_ns(&self, now: TimestampNs) -> Option<u64> {
        let hb = self.heartbeat_ns.load(Ordering::Acquire);
        if hb == 0 {
            None
        } else {
            Some((now.0.max(0) as u64).saturating_sub(hb))
        }
    }
}

#[cfg(target_os = "linux")]
extern "C" {
    fn mmap(
        addr: *mut c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut c_void;
    fn munmap(addr: *mut c_void, len: usize) -> i32;
}

#[cfg(not(target_os = "linux"))]
extern "system" {
    fn CreateFileMappingW(
        h_file: *mut c_void,
        lp_attributes: *const c_void,
        fl_protect: u32,
        dw_max_size_high: u32,
        dw_max_size_low: u32,
        lp_name: *const u16,
    ) -> *mut c_void;
    fn MapViewOfFile(
        h_mapping: *mut c_void,
        dw_access: u32,
        dw_offset_high: u32,
        dw_offset_low: u32,
        dw_bytes: usize,
    ) -> *mut c_void;
    fn UnmapViewOfFile(lp_base: *const c_void) -> i32;
    fn CloseHandle(h_object: *mut c_void) -> i32;
}

pub struct MappedShmRegion {
    ptr: *mut VortexShmArena,
    #[cfg(not(target_os = "linux"))]
    map_handle: *mut c_void,
}

unsafe impl Send for MappedShmRegion {}
unsafe impl Sync for MappedShmRegion {}

impl core::fmt::Debug for MappedShmRegion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MappedShmRegion")
            .field("non_null", &(!self.ptr.is_null()))
            .finish_non_exhaustive()
    }
}

impl MappedShmRegion {
    #[must_use]
    pub fn default_shm_path() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/dev/shm/vortex_lob.mmap")
        }
        #[cfg(not(target_os = "linux"))]
        {
            std::env::temp_dir().join("vortex_lob.mmap")
        }
    }

    #[must_use]
    pub fn ephemeral_self_test_path(tag: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let ts = now_epoch_ns().0.unsigned_abs();
        std::env::temp_dir().join(format!(
            "vortex_shm_test_{}_{}_{}_{}.mmap",
            tag,
            std::process::id(),
            ts,
            id
        ))
    }

    pub fn open_or_create(path: &Path, reset_zero: bool) -> Result<Self, VortexError> {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let arena_len = core::mem::size_of::<VortexShmArena>();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        file.set_len(arena_len as u64)
            .map_err(|_| VortexError::ColumnarIoFailure)?;

        #[cfg(target_os = "linux")]
        let reg = {
            use std::os::unix::io::AsRawFd;
            let raw = unsafe { mmap(ptr::null_mut(), arena_len, 3, 1, file.as_raw_fd(), 0) };
            if raw.is_null() || (raw as usize) == usize::MAX {
                return Err(VortexError::ColumnarIoFailure);
            }
            Self {
                ptr: raw.cast::<VortexShmArena>(),
            }
        };

        #[cfg(not(target_os = "linux"))]
        let reg = {
            use std::os::windows::io::AsRawHandle;
            let mh = unsafe {
                CreateFileMappingW(
                    file.as_raw_handle().cast(),
                    ptr::null(),
                    0x04,
                    0,
                    arena_len as u32,
                    ptr::null(),
                )
            };
            if mh.is_null() {
                return Err(VortexError::ColumnarIoFailure);
            }
            let view = unsafe { MapViewOfFile(mh, 0x06, 0, 0, arena_len) };
            if view.is_null() {
                let _ = unsafe { CloseHandle(mh) };
                return Err(VortexError::ColumnarIoFailure);
            }
            Self {
                ptr: view.cast::<VortexShmArena>(),
                map_handle: mh,
            }
        };

        unsafe {
            let need_init = reset_zero
                || (*reg.ptr).magic != VORTEX_SHM_MAGIC
                || (*reg.ptr).version != VORTEX_SHM_VERSION;
            if need_init {
                ptr::write_bytes(reg.ptr, 0, 1);
                (*reg.ptr).magic = VORTEX_SHM_MAGIC;
                (*reg.ptr).version = VORTEX_SHM_VERSION;
                fence(Ordering::Release);
            }
        }
        Ok(reg)
    }

    pub fn open_existing_read_only(path: &Path) -> Result<Self, VortexError> {
        let arena_len = core::mem::size_of::<VortexShmArena>();
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| VortexError::ColumnarIoFailure)?;
        let f_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if f_len < (arena_len as u64) {
            return Err(VortexError::ColumnarFormatCorrupted);
        }

        #[cfg(target_os = "linux")]
        let reg = {
            use std::os::unix::io::AsRawFd;
            let raw = unsafe { mmap(ptr::null_mut(), arena_len, 1, 1, file.as_raw_fd(), 0) };
            if raw.is_null() || (raw as usize) == usize::MAX {
                return Err(VortexError::ColumnarIoFailure);
            }
            Self {
                ptr: raw.cast::<VortexShmArena>(),
            }
        };

        #[cfg(not(target_os = "linux"))]
        let reg = {
            use std::os::windows::io::AsRawHandle;
            let mh = unsafe {
                CreateFileMappingW(
                    file.as_raw_handle().cast(),
                    ptr::null(),
                    0x02,
                    0,
                    arena_len as u32,
                    ptr::null(),
                )
            };
            if mh.is_null() {
                return Err(VortexError::ColumnarIoFailure);
            }
            let view = unsafe { MapViewOfFile(mh, 0x04, 0, 0, arena_len) };
            if view.is_null() {
                let _ = unsafe { CloseHandle(mh) };
                return Err(VortexError::ColumnarIoFailure);
            }
            Self {
                ptr: view.cast::<VortexShmArena>(),
                map_handle: mh,
            }
        };

        let valid = unsafe {
            (*reg.ptr).magic == VORTEX_SHM_MAGIC && (*reg.ptr).version == VORTEX_SHM_VERSION
        };
        if !valid {
            return Err(VortexError::ColumnarFormatCorrupted);
        }
        Ok(reg)
    }

    #[inline]
    #[must_use]
    pub fn arena(&self) -> &VortexShmArena {
        unsafe { &*self.ptr }
    }
}

impl Drop for MappedShmRegion {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = munmap(self.ptr.cast(), core::mem::size_of::<VortexShmArena>());
            }
            #[cfg(not(target_os = "linux"))]
            unsafe {
                let _ = UnmapViewOfFile(self.ptr.cast());
                if !self.map_handle.is_null() {
                    let _ = CloseHandle(self.map_handle);
                }
            }
            self.ptr = ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::APPROVED_SPECS;

    #[test]
    fn test_shm_v13_12slots_wave_fib_and_deep_l1_l5_volume() {
        let p = MappedShmRegion::ephemeral_self_test_path("v13_slots");
        let reg = MappedShmRegion::open_or_create(&p, true);
        assert!(reg.is_ok());
        if let Ok(r) = reg {
            assert_eq!(r.arena().version, VORTEX_SHM_VERSION);
            let mut snap = SymbolLobSnapshot::new_empty();
            snap.symbol_id = APPROVED_SPECS[3].symbol_id;
            for i in 0..5 {
                snap.apply_level_update(Side::Bid, PriceTicks(8_200 - i), LotQty(100));
                snap.apply_level_update(Side::Ask, PriceTicks(8_201 + i), LotQty(80));
            }
            assert_eq!(snap.bbo_and_spread_ticks().map(|(_, _, sp)| sp), Some(1));
            snap.set_wave_observables(WaveObservables {
                regime: WaveRegime::ImpulseWave3,
                climax_active: false,
                hurst: WaveObservables::encode_hurst(0.64),
                macro_phase: BoundedScore::new_clamped(0.85),
                oi_impulse: BoundedScore::new_clamped(0.40),
                liq_imbalance: BoundedScore::new_clamped(0.72),
                fib_proj_bps: ScaledBps(4_250),
            });
            if let Ok(s3) = r.arena().slot(3) {
                s3.write_snapshot(&snap);
                let out = s3.read_snapshot(4);
                assert!(out.is_ok());
                if let Ok(rd) = out {
                    assert_eq!(rd.wave_observables().fib_proj_bps, ScaledBps(4_250));
                    assert!((rd.wave_observables().liq_imbalance.get() - 0.72).abs() < 1e-9);
                }
            }
        }
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn test_l2_sequence_transition_and_telemetry_seqlock_odd_guard() {
        assert_eq!(
            classify_l2_sequence_transition(false, 0, false, 10),
            L2SeqDecision::IgnorePreSnapshotOrStaleDelta
        );
        assert_eq!(
            classify_l2_sequence_transition(false, 0, true, 10),
            L2SeqDecision::ResetAndApplySnapshot
        );
        assert_eq!(
            classify_l2_sequence_transition(true, 10, false, 1),
            L2SeqDecision::ResetAndApplySnapshot
        );
        assert_eq!(
            classify_l2_sequence_transition(true, 10, false, 11),
            L2SeqDecision::ApplyMonotonicDelta
        );
    }

    #[test]
    fn test_seqlock_odd_recovery_and_ask_cross_purge_zero_guard() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.apply_level_update(Side::Bid, PriceTicks(0), LotQty(500));
        assert_eq!(snap.bids[0].price, PriceTicks(0));

        snap.apply_level_update(Side::Ask, PriceTicks(8_200), LotQty(100));
        snap.apply_level_update(Side::Ask, PriceTicks(8_205), LotQty(150));
        snap.apply_level_update(Side::Bid, PriceTicks(8_202), LotQty(200));
        assert_eq!(snap.asks[0].price, PriceTicks(8_205));

        let p = MappedShmRegion::ephemeral_self_test_path("odd_rec");
        if let Ok(r) = MappedShmRegion::open_or_create(&p, true) {
            if let Ok(s0) = r.arena().slot(0) {
                s0.seq.store(7, Ordering::Release);
                assert!(s0.read_snapshot(2).is_err());
                s0.write_snapshot(&snap);
                assert!(s0.read_snapshot(2).is_ok());
            }
        }
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn test_shm_hot_reattach_read_only_and_tear_free_telemetry_f09() {
        let p = MappedShmRegion::ephemeral_self_test_path("hot_ro");
        if let Ok(rw) = MappedShmRegion::open_or_create(&p, true) {
            let ro = MappedShmRegion::open_existing_read_only(&p);
            assert!(ro.is_ok());
            if let Ok(r_only) = ro {
                if let Ok(tel_w) = rw.arena().telemetry(2) {
                    tel_w.write_consistent(&SlotTelemetrySnapshot {
                        state_code: 4,
                        open_lots: 12,
                        tranche1_harvested: true,
                        ..SlotTelemetrySnapshot::default()
                    });
                }
                if let Ok(tel_r) = r_only.arena().telemetry(2) {
                    let ts = tel_r.read_consistent_or_default(4);
                    assert_eq!(ts.state_code, 4);
                    assert_eq!(ts.open_lots, 12);
                    assert!(ts.tranche1_harvested);
                }
            }
        }
        let _ = fs::remove_file(&p);
    }
}
