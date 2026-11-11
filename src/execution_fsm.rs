//! Execution FSM Primitives, Quantum Floors, Moallemi dQ/dt Shield & 12x2 Adaptive Governor.

use crate::alpha_3factor::OrthogonalFactorState;
use crate::forensic_parquet::ParitySyncLedger;
use crate::lob_shm::{
    SlotTelemetrySnapshot, SymbolLobSnapshot, VortexShmArena, MAX_ACTIVE_PAIRS,
};
use crate::types::{
    LotQty, PairSpec, PriceTicks, ScaledBps, Side, TimestampNs, VortexError, APPROVED_SPECS,
};

pub const MAX_CLOSE_RECONCILE_RETRIES: u8 = 3;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedTradeKind {
    MakerPassiveUnwind,
    ServerMarkPriceStopLoss,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExitSubReason {
    Tranche1Harvest50,
    LvdbsDeepVacuumSurf,
    AdverseScratchEscape,
    WaldBaseStopLoss,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClosedTradeSummary {
    pub kind: ClosedTradeKind,
    pub side: Side,
    pub entry_price: PriceTicks,
    pub exit_price: PriceTicks,
    pub closed_lots: LotQty,
    pub legs_consolidated: u32,
    pub gross_scaled_bps: ScaledBps,
    pub fee_scaled_bps: ScaledBps,
    pub net_scaled_bps: ScaledBps,
    pub net_pnl_1e8: i64,
    pub fee_1e8: i64,
    pub is_maker_exit: bool,
}

/// Explicit role of the active maker exit order on the exchange.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ExitOrderRole {
    #[default]
    None = 0,
    Tranche1Harvest50 = 1,
    LvdbsDeepSurf50 = 2,
    StandardMakerExit100 = 3,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CanonicalExitQuote {
    pub role: ExitOrderRole,
    pub sub_reason: ExitSubReason,
    pub target_price: PriceTicks,
    pub order_lots: LotQty,
    pub vac_level: usize,
    pub cur_gross_bps: i64,
    pub wald_pct: i64,
    pub defensive_lock_active: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct VerifiedCloseResolution {
    pub kind: ClosedTradeKind,
    pub sub_reason: ExitSubReason,
    pub verified_entry_price: PriceTicks,
    pub verified_exit_price: PriceTicks,
    pub closed_lots: LotQty,
    pub legs_count: u32,
    pub exchange_closed_pnl_1e8: Option<i64>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ClosedPnlQueryOutcome {
    Verified(VerifiedCloseResolution),
    PendingIndexerRetry,
    TransportOrApiError,
}

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[allow(clippy::needless_range_loop)]
fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = Vec::with_capacity(data.len() + 72);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0_u32; 64];
        for i in 0..16 {
            let j = i * 4;
            w[i] = u32::from_be_bytes([chunk[j], chunk[j + 1], chunk[j + 2], chunk[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0_u8; 32];
    for (i, word) in h.iter().enumerate() {
        let bytes = word.to_be_bytes();
        out[i * 4..i * 4 + 4].copy_from_slice(&bytes);
    }
    out
}

#[must_use]
pub fn sign_bybit_v5_hmac_sha256(secret: &str, payload: &str) -> String {
    let mut key_block = [0_u8; 64];
    let sec_bytes = secret.as_bytes();
    if sec_bytes.len() > 64 {
        let hashed = sha256_digest(sec_bytes);
        key_block[..32].copy_from_slice(&hashed);
    } else {
        key_block[..sec_bytes.len()].copy_from_slice(sec_bytes);
    }
    let mut ipad = [0x36_u8; 64];
    let mut opad = [0x5c_u8; 64];
    for i in 0..64 {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }
    let mut inner_data = Vec::with_capacity(64 + payload.len());
    inner_data.extend_from_slice(&ipad);
    inner_data.extend_from_slice(payload.as_bytes());
    let inner_hash = sha256_digest(&inner_data);

    let mut outer_data = Vec::with_capacity(96);
    outer_data.extend_from_slice(&opad);
    outer_data.extend_from_slice(&inner_hash);
    let digest = sha256_digest(&outer_data);

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex_str = String::with_capacity(64);
    for &b in &digest {
        hex_str.push(HEX[(b >> 4) as usize] as char);
        hex_str.push(HEX[(b & 0x0f) as usize] as char);
    }
    hex_str
}

/// Parses Bybit V5 avgPrice into conservative directional PriceTicks (ceil Bid, floor Ask).
pub fn parse_verified_avg_price_ticks(
    spec: PairSpec,
    side: Side,
    decimal_str: &str,
) -> Result<PriceTicks, VortexError> {
    let s = decimal_str.trim();
    if s.is_empty() || spec.tick_size_1e8 <= 0 {
        return Err(VortexError::AsciiDecimalParseError);
    }
    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u32 = 0;
    let mut in_frac = false;
    let mut has_extra_nonzero_frac = false;

    for b in s.bytes() {
        if b == b'.' {
            if in_frac {
                return Err(VortexError::AsciiDecimalParseError);
            }
            in_frac = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return Err(VortexError::AsciiDecimalParseError);
        }
        let d = i128::from(b - b'0');
        if !in_frac {
            whole = whole.saturating_mul(10).saturating_add(d);
        } else if frac_digits < 8 {
            frac = frac.saturating_mul(10).saturating_add(d);
            frac_digits += 1;
        } else if d > 0 {
            has_extra_nonzero_frac = true;
        }
    }
    while frac_digits < 8 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }

    let total_1e8 = whole.saturating_mul(100_000_000).saturating_add(frac);
    if total_1e8 <= 0 && !has_extra_nonzero_frac {
        return Err(VortexError::InvalidPriceOrTickSpec);
    }
    let tick = spec.tick_size_1e8 as i128;
    let ticks = match side {
        Side::Bid => {
            let extra = i128::from(has_extra_nonzero_frac);
            (total_1e8 + extra + tick - 1) / tick
        }
        Side::Ask => total_1e8 / tick,
    };
    if ticks <= 0 {
        return Err(VortexError::InvalidPriceOrTickSpec);
    }
    Ok(PriceTicks(ticks as i64))
}

/// Parses Bybit V5 avgExitPrice conservatively (floor for Long Bid, ceil for Short Ask).
#[inline]
pub fn parse_verified_exit_price_ticks(
    spec: PairSpec,
    open_side: Side,
    decimal_str: &str,
) -> Result<PriceTicks, VortexError> {
    parse_verified_avg_price_ticks(spec, open_side.opposite(), decimal_str)
}

/// Parses signed decimal strings (e.g. Bybit closedPnl) into exact integer 1e8 units.
pub fn parse_signed_decimal_1e8(decimal_str: &str) -> Result<i64, VortexError> {
    let s = decimal_str.trim();
    if s.is_empty() {
        return Err(VortexError::AsciiDecimalParseError);
    }
    let bytes = s.as_bytes();
    let (neg, start_idx) = if bytes[0] == b'-' {
        (true, 1_usize)
    } else if bytes[0] == b'+' {
        (false, 1_usize)
    } else {
        (false, 0_usize)
    };
    if start_idx >= bytes.len() {
        return Err(VortexError::AsciiDecimalParseError);
    }

    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u32 = 0;
    let mut in_frac = false;
    let mut saw_digit = false;

    for &b in &bytes[start_idx..] {
        if b == b'.' {
            if in_frac {
                return Err(VortexError::AsciiDecimalParseError);
            }
            in_frac = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return Err(VortexError::AsciiDecimalParseError);
        }
        saw_digit = true;
        let d = i128::from(b - b'0');
        if !in_frac {
            whole = whole.saturating_mul(10).saturating_add(d);
        } else if frac_digits < 8 {
            frac = frac.saturating_mul(10).saturating_add(d);
            frac_digits += 1;
        }
    }
    if !saw_digit {
        return Err(VortexError::AsciiDecimalParseError);
    }
    while frac_digits < 8 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }

    let mag = whole.saturating_mul(100_000_000).saturating_add(frac);
    let clamped = mag.min(i64::MAX as i128) as i64;
    Ok(if neg { -clamped } else { clamped })
}

/// Tick-quantized Zero-Loss Quantum Floor (4.00 bps Maker-Maker fee + >= 0.40 bps net).
#[inline]
#[must_use]
pub const fn defensive_zero_loss_maker_exit_price(
    side: Side,
    entry_price: PriceTicks,
) -> PriceTicks {
    let min_gross_bps = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2 + 40;
    let quantum_ticks = (entry_price.0.saturating_mul(min_gross_bps) + 999_999) / 1_000_000;
    let min_delta = if quantum_ticks > 2 { quantum_ticks } else { 2 };
    match side {
        Side::Bid => PriceTicks(entry_price.0 + min_delta),
        Side::Ask => {
            let p = entry_price.0 - min_delta;
            PriceTicks(if p > 1 { p } else { 1 })
        }
    }
}

#[inline]
#[must_use]
pub const fn dynamic_maker_floor_price(
    side: Side,
    entry_price: PriceTicks,
    reference_gross_bps: ScaledBps,
) -> PriceTicks {
    let quantum_floor = defensive_zero_loss_maker_exit_price(side, entry_price);
    let fee_rt_bps = ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2;
    let g = reference_gross_bps.raw();
    if g <= fee_rt_bps {
        return quantum_floor;
    }
    let target_ticks = (entry_price.0.saturating_mul(g) + 999_999) / 1_000_000;
    let min_delta = if target_ticks > 2 { target_ticks } else { 2 };
    match side {
        Side::Bid => {
            let cand = entry_price.0 + min_delta;
            PriceTicks(if cand > quantum_floor.0 { cand } else { quantum_floor.0 })
        }
        Side::Ask => {
            let p = entry_price.0 - min_delta;
            let cand = if p > 1 { p } else { 1 };
            PriceTicks(if cand < quantum_floor.0 { cand } else { quantum_floor.0 })
        }
    }
}

/// Moallemi-Yuan L1 Queue Depletion Velocity Shield (dQ/dt).
#[inline]
#[must_use]
pub fn check_queue_depletion_velocity_abort(
    side: Side,
    init_queue_ahead: LotQty,
    current_queue_ahead: LotQty,
    snap: &SymbolLobSnapshot,
) -> bool {
    let dir_s = side.sign_f64();
    let signed_ofi = snap.factor_ofi.get() * dir_s;
    let signed_tfi = snap.factor_tfi.get() * dir_s;
    let flow_impulse = 0.55 * signed_ofi + 0.45 * signed_tfi;
    if flow_impulse <= -0.48 {
        return true;
    }
    if init_queue_ahead.0 <= 0 {
        return false;
    }
    let vol_ratio = (snap.realized_vol_5m.raw().clamp(4_000, 12_000) as f64) / 6_000.0;
    let nu_sigma = vol_ratio.sqrt().clamp(0.80, 1.35);
    let depleted_lots = (init_queue_ahead.0 - current_queue_ahead.0).max(0) as f64;
    let depleted_frac = depleted_lots / (init_queue_ahead.0 as f64);
    depleted_frac >= (0.55 * nu_sigma) && flow_impulse <= (-0.05 * nu_sigma)
}
#[inline]
fn extract_matrix_i64_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[i64; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_i64() {
                        dst[s][d] = n;
                    }
                }
            }
        }
    }
}

#[inline]
fn extract_matrix_u32_12x2(
    val: &serde_json::Value,
    key: &str,
    dst: &mut [[u32; 2]; MAX_ACTIVE_PAIRS],
) {
    if let Some(rows) = val.get(key).and_then(|v| v.as_array()) {
        for (s, row_v) in rows.iter().take(MAX_ACTIVE_PAIRS).enumerate() {
            if let Some(cols) = row_v.as_array() {
                for (d, cell) in cols.iter().take(2).enumerate() {
                    if let Some(n) = cell.as_u64() {
                        dst[s][d] = n.min(u64::from(u32::MAX)) as u32;
                    }
                }
            }
        }
    }
}

/// 12x2 Native Online Adaptive Governor with 6 Shields, E_kin Surface & Scar Memory.
#[derive(Clone, Debug)]
pub struct OnlineAdaptiveGovernor {
    ewma_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    cum_net_bps_x100: [[i64; 2]; MAX_ACTIVE_PAIRS],
    sl_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    win_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    directional_cooldown_until_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
    global_dir_debt_bps_x100: [i64; 2],
    global_dir_sl_streak: [u32; 2],
    global_dir_win_streak: [u32; 2],
    global_dir_cooldown_until_ns: [i64; 2],
    last_global_win_dir: u8,
    ewma_fast_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    ewma_slow_px_1e4: [i64; MAX_ACTIVE_PAIRS],
    last_price_sample_ts_ns: [i64; MAX_ACTIVE_PAIRS],
    wave_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_streak: [[u32; 2]; MAX_ACTIVE_PAIRS],
    abort_anchor_px_1e4: [[i64; 2]; MAX_ACTIVE_PAIRS],
    abort_last_ts_ns: [[i64; 2]; MAX_ACTIVE_PAIRS],
}

impl Default for OnlineAdaptiveGovernor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::manual_clamp, clippy::manual_range_contains, clippy::too_many_arguments)]
impl OnlineAdaptiveGovernor {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ewma_net_bps_x100: [[200; 2]; MAX_ACTIVE_PAIRS],
            cum_net_bps_x100: [[0; 2]; MAX_ACTIVE_PAIRS],
            sl_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            win_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            directional_cooldown_until_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
            global_dir_debt_bps_x100: [0; 2],
            global_dir_sl_streak: [0; 2],
            global_dir_win_streak: [0; 2],
            global_dir_cooldown_until_ns: [0; 2],
            last_global_win_dir: 255,
            ewma_fast_px_1e4: [0; MAX_ACTIVE_PAIRS],
            ewma_slow_px_1e4: [0; MAX_ACTIVE_PAIRS],
            last_price_sample_ts_ns: [0; MAX_ACTIVE_PAIRS],
            wave_anchor_px_1e4: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_streak: [[0; 2]; MAX_ACTIVE_PAIRS],
            abort_anchor_px_1e4: [[i64::MIN; 2]; MAX_ACTIVE_PAIRS],
            abort_last_ts_ns: [[0; 2]; MAX_ACTIVE_PAIRS],
        }
    }

    #[inline]
    #[must_use]
    pub const fn dir_idx(side: Side) -> usize {
        match side {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }

    #[inline]
    pub fn observe_market_price(&mut self, slot_idx: usize, snap: &SymbolLobSnapshot) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let mid_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            (snap.bids[0].price.0 + snap.asks[0].price.0) / 2
        } else {
            snap.mark_price.0
        };
        if mid_ticks <= 0 {
            return;
        }
        let px_1e4 = mid_ticks.saturating_mul(10_000);
        let fast_prev = self.ewma_fast_px_1e4[slot_idx];
        let slow_prev = self.ewma_slow_px_1e4[slot_idx];
        if fast_prev <= 0 || slow_prev <= 0 {
            self.ewma_fast_px_1e4[slot_idx] = px_1e4;
            self.ewma_slow_px_1e4[slot_idx] = px_1e4;
            self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
            return;
        }
        let sample_dt = snap.ts_local_ns.0.saturating_sub(self.last_price_sample_ts_ns[slot_idx]);
        if sample_dt < 1_000_000_000 {
            return;
        }
        self.last_price_sample_ts_ns[slot_idx] = snap.ts_local_ns.0;
        self.ewma_fast_px_1e4[slot_idx] = (fast_prev * 19 + px_1e4) / 20;
        self.ewma_slow_px_1e4[slot_idx] = (slow_prev * 299 + px_1e4) / 300;
    }

    #[inline]
    #[must_use]
    pub const fn price_drift_and_extension_scaled_bps(
        &self,
        slot_idx: usize,
        snap: &SymbolLobSnapshot,
    ) -> (i64, i64) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0);
        }
        let fast = self.ewma_fast_px_1e4[slot_idx];
        let slow = self.ewma_slow_px_1e4[slot_idx];
        if fast <= 0 || slow <= 0 {
            return (0, 0);
        }
        let drift_bps = ((fast - slow).saturating_mul(1_000_000)) / slow;
        let cur_ticks = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 0 {
            (snap.bids[0].price.0 + snap.asks[0].price.0) / 2
        } else {
            snap.mark_price.0
        };
        let cur_1e4 = cur_ticks.saturating_mul(10_000);
        let ext_bps = if cur_1e4 > 0 {
            ((cur_1e4 - slow).saturating_mul(1_000_000)) / slow
        } else {
            0
        };
        (drift_bps, ext_bps)
    }

    #[inline]
    #[must_use]
    pub const fn slot_direction_diagnostics(&self, slot_idx: usize, side: Side) -> (i64, u32, u32) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return (0, 0, 0);
        }
        let d = Self::dir_idx(side);
        (
            self.ewma_net_bps_x100[slot_idx][d],
            self.sl_streak[slot_idx][d],
            self.win_streak[slot_idx][d],
        )
    }

    #[inline]
    #[must_use]
    pub const fn wave_anchor_ticks(&self, slot_idx: usize, side: Side) -> i64 {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return 0;
        }
        self.wave_anchor_px_1e4[slot_idx][Self::dir_idx(side)] / 10_000
    }

    #[inline]
    #[must_use]
    pub const fn is_in_scar_debt(&self, slot_idx: usize, side: Side) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let d = Self::dir_idx(side);
        self.cum_net_bps_x100[slot_idx][d] < -1_500
            || self.sl_streak[slot_idx][d] > 0
            || self.global_dir_debt_bps_x100[d] < -6_000
    }

    #[inline]
    #[must_use]
    pub const fn audit_historical_entry_veto(
        &self,
        slot_idx: usize,
        side: Side,
        entry_ticks: i64,
        check_ts_ns: i64,
    ) -> Option<&'static str> {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return Some("VETO_INVALID_SLOT");
        }
        let d = Self::dir_idx(side);
        if check_ts_ns > 0 && check_ts_ns < self.directional_cooldown_until_ns[slot_idx][d] {
            return Some("VETO_SLOT_COOLDOWN");
        }
        if check_ts_ns > 0 && check_ts_ns < self.global_dir_cooldown_until_ns[d] {
            return Some("VETO_GLOBAL_WAVE_OR_SL_BREATHER");
        }
        let anchor_1e4 = self.wave_anchor_px_1e4[slot_idx][d];
        let cur_1e4 = entry_ticks.saturating_mul(10_000);
        let wave_active = self.win_streak[slot_idx][d] >= 3
            || (self.win_streak[slot_idx][d] >= 1 && self.global_dir_win_streak[d] >= 3);
        if wave_active && anchor_1e4 > 0 && cur_1e4 > 0 {
            let chased_bps = match side {
                Side::Bid => ((cur_1e4 - anchor_1e4).saturating_mul(1_000_000)) / anchor_1e4,
                Side::Ask => ((anchor_1e4 - cur_1e4).saturating_mul(1_000_000)) / anchor_1e4,
            };
            if chased_bps > 2_500 && chased_bps < 15_000 {
                return Some("VETO_WAVE_STAIRCASE_25BPS");
            }
        }
        if self.abort_streak[slot_idx][d] >= 4
            && self.abort_anchor_px_1e4[slot_idx][d] > 0
            && check_ts_ns > 0
            && check_ts_ns.saturating_sub(self.abort_last_ts_ns[slot_idx][d]) < 15_000_000_000
        {
            return Some("VETO_ABORT_STREAK_BREATHER");
        }
        if self.sl_streak[slot_idx][d] >= 1 && self.cum_net_bps_x100[slot_idx][d] <= -4_500 {
            return Some("VETO_DEEP_SCAR_DEBT_QUARANTINE");
        }
        if slot_idx > 0 {
            if self.global_dir_sl_streak[d] >= 1 && self.last_global_win_dir == ((1 - d) as u8) {
                return Some("VETO_POST_SL_OPPOSITE_DOMINANCE");
            }
            let symbol_total_net =
                self.cum_net_bps_x100[slot_idx][0] + self.cum_net_bps_x100[slot_idx][1];
            if symbol_total_net <= -2_500
                && self.global_dir_debt_bps_x100[d] <= -10_000
                && self.cum_net_bps_x100[slot_idx][d] <= 0
            {
                return Some("VETO_SYMBOL_AND_GLOBAL_DEBT_CONTAGION");
            }
        }
        None
    }

    /// Evaluates continuous Kinematic Free-Energy Surface (E_kin >= E_min).
    #[inline]
    pub fn evaluate_kinematic_energy_surface(
        &self,
        slot_idx: usize,
        side: Side,
        drift_bps: i64,
        ext_bps: i64,
        signed_geom: f64,
        snr_abs: f64,
    ) -> Result<(i64, i64), &'static str> {
        let min_geom_floor = if snr_abs >= 0.50 {
            (0.15 - (snr_abs - 0.50) * 0.55).clamp(0.05, 0.15)
        } else {
            0.15
        };
        if signed_geom < min_geom_floor {
            return Err("VETO_LOW_L2_GEOM_VACUUM (<min_geom)");
        }
        let d_sign = side.sign_i64();
        let s_drift = drift_bps.saturating_mul(d_sign);
        let s_ext = ext_bps.saturating_mul(d_sign);
        let lead_impulse = s_ext.saturating_sub(s_drift);
        let elastic_lead_cap = 48 + ((snr_abs - 0.58).max(0.0) * 110.0).round() as i64;
        if s_drift < -15
            || s_ext < -15
            || s_ext > 1_200
            || (lead_impulse > 0 && lead_impulse > elastic_lead_cap && s_ext >= 115)
            || lead_impulse < -420
        {
            return Err("VETO_KINEMATIC_PHASE_OR_EXHAUSTION");
        }
        let d = Self::dir_idx(side);
        let learned_edge = if slot_idx < MAX_ACTIVE_PAIRS {
            self.ewma_net_bps_x100[slot_idx][d]
        } else {
            180
        };
        let impulse_contrib = if lead_impulse >= 0 {
            lead_impulse.min(75) + (s_drift.max(0) / 2).min(85)
        } else if snr_abs >= 0.50 && s_drift > 0 && s_ext >= -10 {
            ((s_drift / 4) + ((snr_abs - 0.50) * 220.0).round() as i64).min(115)
        } else {
            (lead_impulse / 2).max(-60)
        };
        let geom_contrib = ((signed_geom - min_geom_floor).max(0.0) * 240.0).round() as i64;
        let snr_contrib = ((snr_abs - 0.42).max(0.0) * 280.0).round() as i64;
        let fresh_init_bonus = if s_drift >= 0
            && s_drift <= 140
            && s_ext >= -10
            && s_ext <= 165
            && lead_impulse >= -20
            && lead_impulse <= elastic_lead_cap
            && snr_abs >= 0.45
        {
            95_i64
        } else {
            0_i64
        };
        let e_kin = impulse_contrib + geom_contrib + snr_contrib + fresh_init_bonus;
        let req_e_min = (125 + ((200 - learned_edge).max(0) / 4)).clamp(110, 260);
        if e_kin < req_e_min {
            return Err("VETO_KINEMATIC_ENERGY_SURFACE (E_kin < E_min)");
        }
        Ok((e_kin, req_e_min))
    }

    /// F-07 Fix: Records closed trade with real entry_price to arm wave_anchor_px_1e4.
    pub fn observe_closed_trade_at_entry(
        &mut self,
        slot_idx: usize,
        side: Side,
        entry_price: PriceTicks,
        net_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let prev = self.ewma_net_bps_x100[slot_idx][d];
        let edge_clamped = prev.clamp(80, 450);
        self.ewma_net_bps_x100[slot_idx][d] = (prev * 75 + net_scaled_bps.raw() * 25) / 100;
        self.cum_net_bps_x100[slot_idx][d] = self.cum_net_bps_x100[slot_idx][d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-24_000, 24_000);
        self.global_dir_debt_bps_x100[d] = self.global_dir_debt_bps_x100[d]
            .saturating_add(net_scaled_bps.raw())
            .clamp(-30_000, 12_000);

        if !is_maker_exit || net_scaled_bps.raw() < -50 {
            let s = self.sl_streak[slot_idx][d].saturating_add(1).min(4);
            self.sl_streak[slot_idx][d] = s;
            self.win_streak[slot_idx][d] = 0;
            self.global_dir_win_streak[d] = 0;
            self.wave_anchor_px_1e4[slot_idx][d] = 0;
            if ts_ns.0 > 0 {
                let backoff_ns = 150_000_000_000_i64.saturating_mul(1_i64 << s.min(4));
                self.directional_cooldown_until_ns[slot_idx][d] =
                    ts_ns.0.saturating_add(backoff_ns);
            }
            let gs = self.global_dir_sl_streak[d].saturating_add(1).min(4);
            self.global_dir_sl_streak[d] = gs;
            if ts_ns.0 > 0 {
                let global_pause_ns = 180_000_000_000_i64.saturating_mul(i64::from(gs));
                self.global_dir_cooldown_until_ns[d] = ts_ns.0.saturating_add(global_pause_ns);
            }
        } else if net_scaled_bps.raw() < 100 {
            if ts_ns.0 > 0 {
                let scratch_secs = (115 - (edge_clamped / 20)).clamp(60, 130);
                let scratch_cooldown_ns = ts_ns.0.saturating_add(scratch_secs * 1_000_000_000);
                if scratch_cooldown_ns > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = scratch_cooldown_ns;
                }
            }
        } else {
            self.clear_abort_streak(slot_idx, side);
            if entry_price.0 > 0 && self.wave_anchor_px_1e4[slot_idx][d] <= 0 {
                self.wave_anchor_px_1e4[slot_idx][d] = entry_price.0.saturating_mul(10_000);
            }
            let w = self.win_streak[slot_idx][d].saturating_add(1);
            self.win_streak[slot_idx][d] = w;
            self.win_streak[slot_idx][1 - d] = 0;
            self.wave_anchor_px_1e4[slot_idx][1 - d] = 0;
            let gw = self.global_dir_win_streak[d].saturating_add(1);
            self.global_dir_win_streak[d] = gw;
            self.global_dir_win_streak[1 - d] = 0;
            self.last_global_win_dir = d as u8;
            if gw >= 6 {
                self.global_dir_win_streak[d] = 0;
            }
            if ts_ns.0 > 0 {
                let breather_secs =
                    (25 - edge_clamped / 40).clamp(12, 30) * (w.min(2) as i64);
                let win_breather_ns = ts_ns.0.saturating_add(breather_secs * 1_000_000_000);
                if win_breather_ns > self.directional_cooldown_until_ns[slot_idx][d] {
                    self.directional_cooldown_until_ns[slot_idx][d] = win_breather_ns;
                }
            }
            if self.sl_streak[slot_idx][d] > 0
                && w >= 5
                && self.cum_net_bps_x100[slot_idx][d] >= -1_500
            {
                self.sl_streak[slot_idx][d] -= 1;
                self.win_streak[slot_idx][d] = 0;
            }
            if self.global_dir_sl_streak[d] > 0 && self.global_dir_debt_bps_x100[d] >= -3_000 {
                self.global_dir_sl_streak[d] -= 1;
            }
        }
    }

    #[inline]
    pub fn observe_closed_trade(
        &mut self,
        slot_idx: usize,
        side: Side,
        net_scaled_bps: ScaledBps,
        is_maker_exit: bool,
        ts_ns: TimestampNs,
    ) {
        self.observe_closed_trade_at_entry(
            slot_idx,
            side,
            PriceTicks(0),
            net_scaled_bps,
            is_maker_exit,
            ts_ns,
        );
    }

    /// F-07 Fix: Resets pre-fill abort streak on fill or after a 60s calm window.
    #[inline]
    pub fn clear_abort_streak(&mut self, slot_idx: usize, side: Side) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        self.abort_streak[slot_idx][d] = 0;
        self.abort_anchor_px_1e4[slot_idx][d] = i64::MIN;
        self.abort_last_ts_ns[slot_idx][d] = 0;
    }

    /// Records a pre-fill queue abort with 60s window decay so old aborts do not lock forever.
    #[inline]
    pub fn observe_entry_abort(
        &mut self,
        slot_idx: usize,
        side: Side,
        aborted_price: PriceTicks,
        ts_ns: TimestampNs,
    ) {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return;
        }
        let d = Self::dir_idx(side);
        let prev_ts = self.abort_last_ts_ns[slot_idx][d];
        if prev_ts > 0 && ts_ns.0 > prev_ts && ts_ns.0.saturating_sub(prev_ts) >= 60_000_000_000 {
            self.abort_streak[slot_idx][d] = 0;
        }
        self.abort_streak[slot_idx][d] = self.abort_streak[slot_idx][d].saturating_add(1).min(8);
        self.abort_anchor_px_1e4[slot_idx][d] = aborted_price.0.saturating_mul(10_000);
        self.abort_last_ts_ns[slot_idx][d] = ts_ns.0;
    }

    /// Exports immutable 12x2 scar snapshot JSON for vortex_governor_scar_memory.jsonl.
    #[must_use]
    pub fn to_scar_snapshot_json(&self, ts_ns: TimestampNs) -> serde_json::Value {
        serde_json::json!({
            "event": "GOV_SCAR_SNAPSHOT",
            "ts_ns": ts_ns.0,
            "ewma_net_bps_x100": self.ewma_net_bps_x100,
            "cum_net_bps_x100": self.cum_net_bps_x100,
            "sl_streak": self.sl_streak,
            "win_streak": self.win_streak,
            "directional_cooldown_until_ns": self.directional_cooldown_until_ns,
            "wave_anchor_px_1e4": self.wave_anchor_px_1e4,
            "global_dir_debt_bps_x100": self.global_dir_debt_bps_x100,
            "global_dir_sl_streak": self.global_dir_sl_streak,
            "global_dir_win_streak": self.global_dir_win_streak,
            "global_dir_cooldown_until_ns": self.global_dir_cooldown_until_ns,
            "last_global_win_dir": self.last_global_win_dir,
        })
    }

    /// Restores 12x2 scar tissue from JSON and returns snapshot_ts_ns for deduped replay.
    pub fn apply_scar_snapshot_json(&mut self, val: &serde_json::Value) -> Option<TimestampNs> {
        if val.get("event").and_then(|v| v.as_str()) != Some("GOV_SCAR_SNAPSHOT") {
            return None;
        }
        let snap_ts = val.get("ts_ns").and_then(|v| v.as_i64()).unwrap_or(0);
        extract_matrix_i64_12x2(val, "ewma_net_bps_x100", &mut self.ewma_net_bps_x100);
        extract_matrix_i64_12x2(val, "cum_net_bps_x100", &mut self.cum_net_bps_x100);
        extract_matrix_u32_12x2(val, "sl_streak", &mut self.sl_streak);
        extract_matrix_u32_12x2(val, "win_streak", &mut self.win_streak);
        extract_matrix_i64_12x2(
            val,
            "directional_cooldown_until_ns",
            &mut self.directional_cooldown_until_ns,
        );
        extract_matrix_i64_12x2(val, "wave_anchor_px_1e4", &mut self.wave_anchor_px_1e4);
        if let Some(arr) = val.get("global_dir_debt_bps_x100").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_debt_bps_x100[d] = n;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_sl_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_sl_streak[d] = n.min(4) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_win_streak").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_u64() {
                    self.global_dir_win_streak[d] = n.min(6) as u32;
                }
            }
        }
        if let Some(arr) = val.get("global_dir_cooldown_until_ns").and_then(|v| v.as_array()) {
            for (d, cell) in arr.iter().take(2).enumerate() {
                if let Some(n) = cell.as_i64() {
                    self.global_dir_cooldown_until_ns[d] = n;
                }
            }
        }
        if let Some(ld) = val.get("last_global_win_dir").and_then(|v| v.as_u64()) {
            self.last_global_win_dir = ld.min(255) as u8;
        }
        Some(TimestampNs(snap_ts))
    }

    #[must_use]
    pub fn authorizes_adaptive_entry_with_wallet(
        &self,
        slot_idx: usize,
        side: Side,
        wallet_usdt_1e8: i64,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        if slot_idx >= MAX_ACTIVE_PAIRS {
            return false;
        }
        let has_bbo =
            candidate_snap.bids[0].price.0 > 0 && candidate_snap.asks[0].price.0 > 0;
        let entry_px = if has_bbo {
            side.select_entry_bbo_price(
                candidate_snap.bids[0].price,
                candidate_snap.asks[0].price,
            )
        } else {
            candidate_snap.mark_price
        };
        if entry_px.0 <= 0 {
            return false;
        }
        if self
            .audit_historical_entry_veto(
                slot_idx,
                side,
                entry_px.0,
                candidate_snap.ts_local_ns.0,
            )
            .is_some()
        {
            return false;
        }
        let d = Self::dir_idx(side);
        let spec = APPROVED_SPECS[slot_idx];
        let (own_q, opp_q) = match side {
            Side::Bid => (candidate_snap.bids[0].qty.0, candidate_snap.asks[0].qty.0),
            Side::Ask => (candidate_snap.asks[0].qty.0, candidate_snap.bids[0].qty.0),
        };
        if !spec.passes_l1_queue_depth_and_wall_guard(
            entry_px,
            candidate_snap.realized_vol_5m,
            self.ewma_net_bps_x100[slot_idx][d],
            own_q,
            opp_q,
            candidate_snap.factor_ofi.get().abs(),
            candidate_snap.factor_tfi.get().abs(),
        ) {
            return false;
        }
        let (self_drift_bps, self_ext_bps) =
            self.price_drift_and_extension_scaled_bps(slot_idx, candidate_snap);
        let dir_s = side.sign_f64();
        let signed_geom = candidate_snap.factor_geom.get() * dir_s;
        let cand_comp = OrthogonalFactorState::snr_composite_score(candidate_snap).abs();
        let in_scar = self.is_in_scar_debt(slot_idx, side);
        if !spec.passes_oversized_lot_conviction_guard(
            wallet_usdt_1e8,
            entry_px,
            cand_comp,
            in_scar,
        ) {
            return false;
        }
        if self
            .evaluate_kinematic_energy_surface(
                slot_idx,
                side,
                self_drift_bps,
                self_ext_bps,
                signed_geom,
                cand_comp,
            )
            .is_err()
        {
            return false;
        }
        let sl_s = self.sl_streak[slot_idx][d] as f64;
        let win_s = (self.win_streak[slot_idx][d].min(4)) as f64;
        let debt_bps = self.cum_net_bps_x100[slot_idx][d] as f64;
        let debt_penalty = if debt_bps < 0.0 {
            ((-debt_bps) / 16_000.0).min(0.28)
        } else {
            0.0
        };
        let global_debt_bps = self.global_dir_debt_bps_x100[d] as f64;
        let global_penalty = if global_debt_bps < -2_000.0 {
            ((-global_debt_bps - 2_000.0) / 35_000.0).min(0.14)
        } else {
            0.0
        };
        let staircase_penalty = if win_s >= 2.0 { 0.03 * (win_s - 1.0) } else { 0.0 };
        let learned_edge = (self.ewma_net_bps_x100[slot_idx][d].clamp(0, 450) as f64) / 10_000.0;
        if in_scar && candidate_snap.factor_tfi.get().abs() < 0.40 {
            return false;
        }
        if slot_idx > 0 {
            let btc_comp = OrthogonalFactorState::snr_composite_score(btc_leader_snap) * dir_s;
            let btc_ofi = btc_leader_snap.factor_ofi.get() * dir_s;
            let btc_tfi = btc_leader_snap.factor_tfi.get() * dir_s;
            let strong_independent = cand_comp >= 0.50 && sl_s == 0.0 && debt_penalty == 0.0;
            if strong_independent {
                if btc_comp < -0.08 || (btc_ofi < -0.18 && btc_tfi < -0.18) {
                    return false;
                }
            } else {
                let min_req_btc = 0.10 + 0.10 * sl_s + 0.55 * (debt_penalty + global_penalty);
                if btc_ofi < 0.0 || btc_tfi < 0.0 || btc_comp < min_req_btc {
                    return false;
                }
            }
        }
        let adaptive_threshold = (0.42
            + 0.08 * sl_s
            + debt_penalty
            + global_penalty
            + staircase_penalty
            - learned_edge)
            .clamp(0.38, 0.80);
        cand_comp >= adaptive_threshold
    }

    #[inline]
    #[must_use]
    pub fn authorizes_adaptive_entry(
        &self,
        slot_idx: usize,
        side: Side,
        candidate_snap: &SymbolLobSnapshot,
        btc_leader_snap: &SymbolLobSnapshot,
    ) -> bool {
        self.authorizes_adaptive_entry_with_wallet(
            slot_idx,
            side,
            20_000_000_000,
            candidate_snap,
            btc_leader_snap,
        )
    }

    /// Unified LOSS-LAB Verifier: Certifies 62/62 historical/V2/PAR1 losses + 2,160 manifold.
    #[must_use]
    pub fn audit_unified_62_losses_and_2160_manifold() -> (usize, usize) {
        let gov = Self::new();
        let cases: [(usize, Side, i64, i64, f64, f64); 62] = [
            (0, Side::Ask, -331, -347, 0.11, 0.55), (3, Side::Bid, 113, 120, 0.11, 0.52),
            (0, Side::Bid, 182, 248, 0.38, 0.58), (1, Side::Bid, 142, 267, 0.25, 0.54),
            (0, Side::Bid, 247, 157, 0.16, 0.44), (0, Side::Bid, 109, 36, 0.20, 0.45),
            (1, Side::Bid, 275, 209, 0.15, 0.43), (1, Side::Bid, 263, 155, 0.15, 0.43),
            (0, Side::Ask, 694, 302, 0.51, 0.79), (1, Side::Ask, -185, -240, 0.22, 0.50),
            (0, Side::Bid, 170, 235, 0.24, 0.51), (0, Side::Ask, -160, -220, 0.21, 0.49),
            (0, Side::Ask, -145, -210, 0.20, 0.48), (0, Side::Bid, 190, 255, 0.23, 0.52),
            (1, Side::Bid, 210, 275, 0.25, 0.53), (0, Side::Bid, 220, 145, 0.15, 0.42),
            (4, Side::Ask, -218, -148, 0.15, 0.42), (0, Side::Ask, -215, -140, 0.15, 0.42),
            (0, Side::Bid, 230, 150, 0.15, 0.42), (1, Side::Bid, 228, 152, 0.15, 0.42),
            (0, Side::Bid, 235, 165, 0.15, 0.42), (0, Side::Ask, -225, -155, 0.15, 0.42),
            (1, Side::Ask, -240, -168, 0.15, 0.42), (0, Side::Ask, -232, -160, 0.15, 0.42),
            (0, Side::Bid, 250, 162, 0.15, 0.42), (7, Side::Bid, -42, -68, -0.10, 0.39),
            (4, Side::Bid, 135, 195, 0.14, 0.49), (3, Side::Bid, 148, 212, 0.13, 0.50),
            (4, Side::Bid, 128, 184, 0.12, 0.47), (4, Side::Bid, 162, 229, 0.16, 0.51),
            (1, Side::Bid, 175, 244, 0.18, 0.52), (2, Side::Ask, -154, -218, 0.14, 0.48),
            (3, Side::Bid, 139, 204, 0.15, 0.49), (5, Side::Ask, -166, -231, 0.17, 0.50),
            (1, Side::Bid, 188, 262, 0.19, 0.53), (1, Side::Ask, -45, 18, 0.08, 0.41),
            (4, Side::Ask, -38, 22, 0.09, 0.42), (1, Side::Bid, 42, -19, 0.07, 0.41),
            (4, Side::Ask, -32, 25, 0.06, 0.40), (5, Side::Bid, 152, 219, 0.14, 0.48),
            (0, Side::Ask, -51, 14, 0.09, 0.43), (0, Side::Ask, -48, 16, 0.08, 0.42),
            (4, Side::Ask, -172, -245, 0.16, 0.51), (0, Side::Ask, -184, -258, 0.18, 0.52),
            (1, Side::Bid, 168, 239, 0.15, 0.50), (0, Side::Ask, -179, -251, 0.17, 0.51),
            (0, Side::Bid, 36, -24, 0.07, 0.41), (1, Side::Bid, -28, -55, 0.05, 0.43),
            (5, Side::Bid, -34, -62, 0.06, 0.44), (1, Side::Bid, -49, -84, 0.04, 0.45),
            (0, Side::Bid, 144, 208, 0.14, 0.48), (1, Side::Ask, -169, -238, 0.16, 0.50),
            (0, Side::Bid, 158, 226, 0.15, 0.49), (1, Side::Ask, -163, -232, 0.15, 0.49),
            (0, Side::Ask, -174, -246, 0.17, 0.51), (0, Side::Bid, 132, 194, 0.13, 0.47),
            (0, Side::Ask, -136, -198, 0.13, 0.47), (3, Side::Ask, -151, -216, 0.14, 0.48),
            (0, Side::Ask, -165, -234, 0.16, 0.50), (1, Side::Ask, -159, -225, 0.15, 0.49),
            (0, Side::Ask, -161, -228, 0.15, 0.49), (4, Side::Bid, 146, 211, 0.14, 0.48),
        ];
        let mut mitigated_62 = 0_usize;
        for &(s_idx, side, dr, ex, gm, snr) in &cases {
            if gov.evaluate_kinematic_energy_surface(s_idx, side, dr, ex, gm, snr).is_err() {
                mitigated_62 += 1;
            }
        }
        let mut manifold_blocked = 0_usize;
        for s_idx in 0..MAX_ACTIVE_PAIRS {
            for &side in &[Side::Bid, Side::Ask] {
                for step in 0..90_i64 {
                    let dr = if matches!(side, Side::Bid) { 120 + step } else { -(120 + step) };
                    let ex = if matches!(side, Side::Bid) {
                        190 + step * 2
                    } else {
                        -(190 + step * 2)
                    };
                    if gov
                        .evaluate_kinematic_energy_surface(s_idx, side, dr, ex, 0.12, 0.44)
                        .is_err()
                    {
                        manifold_blocked += 1;
                    }
                }
            }
        }
        (mitigated_62, manifold_blocked)
    }
}
#[inline]
fn extract_json_epoch_ms(item: &serde_json::Value, key: &str) -> i64 {
    item.get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        })
        .unwrap_or(0)
}

/// Per-Slot Autonomous Lifecycle State Machine (Monotonic Tranche & LVDBS Native).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OrderLifecycleState {
    #[default]
    Idle,
    PlacingEntryMaker {
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        target_excursion: ScaledBps,
        sl_trigger_price: PriceTicks,
        submitted_ts: TimestampNs,
    },
    RestingEntryQueue {
        side: Side,
        entry_price: PriceTicks,
        qty: LotQty,
        target_excursion: ScaledBps,
        sl_trigger_price: PriceTicks,
        resting_since_ts: TimestampNs,
    },
    PositionOpenMakerExit {
        side: Side,
        entry_price: PriceTicks,
        avg_price_ticks: PriceTicks,
        peak_favorable_price: PriceTicks,
        worst_adverse_price: PriceTicks,
        initial_lots: LotQty,
        open_lots: LotQty,
        tranche1_harvested: bool,
        tranche1_lots: LotQty,
        tranche1_exit_price: PriceTicks,
        defensive_floor_locked: bool,
        active_exit_role: ExitOrderRole,
        active_exit_price: PriceTicks,
        active_exit_lots: LotQty,
        structural_tp_price: PriceTicks,
        resting_exit_price: PriceTicks,
        sl_trigger_price: PriceTicks,
        opened_ts: TimestampNs,
    },
}

/// Autonomous Per-Slot Execution Engine (1 Instance per Slot in [SlotExecutionEngine; 12]).
#[derive(Clone, Debug)]
pub struct SlotExecutionEngine {
    pub slot_idx: usize,
    pub state: OrderLifecycleState,
    pub cooldown_until_ns: TimestampNs,
    pub maker_exits_count: u64,
    pub taker_sl_exits_count: u64,
    pub tranche1_harvests_count: u64,
    pub lvdbs_deep_surfs_count: u64,
    pub gross_profit_accum_1e8: i64,
    pub gross_loss_accum_1e8: i64,
    pub learned_mfe_bps: i64,
    pub last_eff_sl_ticks: PriceTicks,
    pub last_closed_mfe_bps: i64,
    pub last_closed_mae_bps: i64,
    pub last_closed_peak_ticks: PriceTicks,
    pub last_closed_worst_ticks: PriceTicks,
    pub last_closed_wald_pct: i64,
    pub last_exit_sub_reason: ExitSubReason,
    pub last_opened_ts: TimestampNs,
    pub next_peg_allowed_ts_ns: i64,
    pub exit_peg_attempts: u32,
    pub close_reconcile_retries: u8,
}

#[allow(clippy::manual_clamp, clippy::too_many_arguments)]
impl SlotExecutionEngine {
    #[inline]
    #[must_use]
    pub const fn new(slot_idx: usize) -> Self {
        Self {
            slot_idx,
            state: OrderLifecycleState::Idle,
            cooldown_until_ns: TimestampNs(0),
            maker_exits_count: 0,
            taker_sl_exits_count: 0,
            tranche1_harvests_count: 0,
            lvdbs_deep_surfs_count: 0,
            gross_profit_accum_1e8: 0,
            gross_loss_accum_1e8: 0,
            learned_mfe_bps: 680,
            last_eff_sl_ticks: PriceTicks(0),
            last_closed_mfe_bps: 0,
            last_closed_mae_bps: 0,
            last_closed_peak_ticks: PriceTicks(0),
            last_closed_worst_ticks: PriceTicks(0),
            last_closed_wald_pct: 0,
            last_exit_sub_reason: ExitSubReason::AdverseScratchEscape,
            last_opened_ts: TimestampNs(0),
            next_peg_allowed_ts_ns: 0,
            exit_peg_attempts: 0,
            close_reconcile_retries: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_12_slot_array() -> [Self; MAX_ACTIVE_PAIRS] {
        [
            Self::new(0),
            Self::new(1),
            Self::new(2),
            Self::new(3),
            Self::new(4),
            Self::new(5),
            Self::new(6),
            Self::new(7),
            Self::new(8),
            Self::new(9),
            Self::new(10),
            Self::new(11),
        ]
    }

    #[inline]
    #[must_use]
    pub fn active_side(&self) -> Option<Side> {
        match self.state {
            OrderLifecycleState::Idle => None,
            OrderLifecycleState::PlacingEntryMaker { side, .. }
            | OrderLifecycleState::RestingEntryQueue { side, .. }
            | OrderLifecycleState::PositionOpenMakerExit { side, .. } => Some(side),
        }
    }

    #[inline]
    #[must_use]
    pub fn elastic_preflight_window_ns(&self, realized_vol_5m: ScaledBps) -> i64 {
        let vol_c = realized_vol_5m.raw().clamp(4_000, 12_500);
        let mfe_c = self.learned_mfe_bps.clamp(560, 1_200);
        let raw_ns = ((25_000_000_i64 * 6_000) / vol_c * mfe_c) / 720;
        raw_ns.clamp(15_000_000, 35_000_000)
    }

    #[inline]
    pub fn observe_mfe_sample(&mut self, gross_mfe_scaled_bps: i64) {
        if gross_mfe_scaled_bps >= 500 {
            let clamped = gross_mfe_scaled_bps.clamp(560, 1_600);
            self.learned_mfe_bps = (self.learned_mfe_bps * 80 + clamped * 20) / 100;
        }
    }

    /// F-06 Fix: Enforces 110..145 bps server SL distance & >= 2-tick Mark/Last cushion.
    #[inline]
    pub fn compute_fill_or_adopt_sl_ticks(
        side: Side,
        verified_avg_price: PriceTicks,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        if verified_avg_price.0 <= 0 {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let sl_bps = PairSpec::elastic_server_sl_distance_bps(snap.realized_vol_5m).raw();
        let sl_delta = ((verified_avg_price.0.saturating_mul(sl_bps)) / 1_000_000).max(4);
        let ref_mark = if snap.mark_price.0 > 0 {
            snap.mark_price
        } else {
            verified_avg_price
        };
        let ref_last = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            ref_mark
        };
        let raw_sl = match side {
            Side::Bid => {
                let nominal = verified_avg_price.0 - sl_delta;
                let live_cap = ref_mark.0.min(ref_last.0) - 3;
                PriceTicks(nominal.min(live_cap).max(1))
            }
            Side::Ask => {
                let nominal = verified_avg_price.0.saturating_add(sl_delta);
                let live_floor = ref_mark.0.max(ref_last.0).saturating_add(3);
                PriceTicks(nominal.max(live_floor))
            }
        };
        PairSpec::validate_stop_loss_geometry(side, raw_sl, ref_last, ref_mark)
    }

    /// F-09 Fix: Publishes tear-free Seqlock telemetry via SlotExecTelemetry::write_consistent.
    pub fn sync_to_shm_telemetry(&self, arena: &VortexShmArena) {
        let Ok(tel) = arena.telemetry(self.slot_idx) else {
            return;
        };
        let snap = match self.state {
            OrderLifecycleState::Idle => SlotTelemetrySnapshot {
                state_code: 0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::PlacingEntryMaker {
                side,
                entry_price,
                qty,
                sl_trigger_price,
                submitted_ts,
                ..
            } => SlotTelemetrySnapshot {
                state_code: 1,
                side_code: side as u64,
                entry_ticks: entry_price.0,
                avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0,
                initial_lots: qty.0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: submitted_ts.0.max(0) as u64,
                queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::RestingEntryQueue {
                side,
                entry_price,
                qty,
                sl_trigger_price,
                resting_since_ts,
                ..
            } => SlotTelemetrySnapshot {
                state_code: 2,
                side_code: side as u64,
                entry_ticks: entry_price.0,
                avg_price_ticks: entry_price.0,
                sl_ticks: sl_trigger_price.0,
                initial_lots: qty.0,
                lvdbs_surfs: self.lvdbs_deep_surfs_count,
                resting_since_ns: resting_since_ts.0.max(0) as u64,
                queue_ahead_lots: tel.queue_ahead_lots_relaxed(),
                ..SlotTelemetrySnapshot::default()
            },
            OrderLifecycleState::PositionOpenMakerExit {
                side,
                entry_price,
                avg_price_ticks,
                peak_favorable_price,
                initial_lots,
                open_lots,
                tranche1_harvested,
                active_exit_price,
                resting_exit_price,
                sl_trigger_price,
                opened_ts,
                ..
            } => {
                let state_code = if tranche1_harvested {
                    4_u8
                } else if open_lots.0 < initial_lots.0 {
                    5_u8
                } else {
                    3_u8
                };
                let eff_tp = if resting_exit_price.0 > 0 {
                    resting_exit_price.0
                } else {
                    active_exit_price.0
                };
                let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
                    self.last_eff_sl_ticks.0
                } else {
                    sl_trigger_price.0
                };
                SlotTelemetrySnapshot {
                    state_code,
                    side_code: side as u64,
                    entry_ticks: entry_price.0,
                    avg_price_ticks: avg_price_ticks.0,
                    peak_ticks: peak_favorable_price.0,
                    tp_ticks: eff_tp,
                    sl_ticks: eff_sl,
                    open_lots: open_lots.0,
                    initial_lots: initial_lots.0,
                    lvdbs_surfs: self.lvdbs_deep_surfs_count,
                    tranche1_harvested,
                    resting_since_ns: opened_ts.0.max(0) as u64,
                    queue_ahead_lots: 0,
                }
            }
        };
        tel.write_consistent(&snap);
    }

    pub fn try_arm_maker_entry(
        &mut self,
        snap: &SymbolLobSnapshot,
        verdict: crate::alpha_3factor::SignalVerdict,
        qty: LotQty,
    ) -> Result<OrderLifecycleState, VortexError> {
        if !matches!(self.state, OrderLifecycleState::Idle) {
            return Ok(self.state);
        }
        if snap.ts_local_ns.0 < self.cooldown_until_ns.0 {
            return Ok(self.state);
        }
        let crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side,
            target_excursion_scaled_bps,
        } = verdict
        else {
            return Ok(self.state);
        };
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Ok(self.state);
        };
        let entry_price = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        let sl_trigger_price = Self::compute_fill_or_adopt_sl_ticks(side, entry_price, snap)?;

        self.state = OrderLifecycleState::PlacingEntryMaker {
            side,
            entry_price,
            qty: LotQty(qty.0.max(1)),
            target_excursion: target_excursion_scaled_bps,
            sl_trigger_price,
            submitted_ts: snap.ts_local_ns,
        };
        Ok(self.state)
    }

    /// F-02 Fix: Mutates PlacingEntryMaker in-place with post-preflight L1 price & SL.
    pub fn sync_preflight_l1_entry(
        &mut self,
        snap: &SymbolLobSnapshot,
        max_drift_ticks: i64,
    ) -> Result<(PriceTicks, PriceTicks), VortexError> {
        let OrderLifecycleState::PlacingEntryMaker {
            side,
            entry_price: armed_px,
            ..
        } = self.state
        else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        let Some((best_bid, best_ask, _)) = snap.bbo_and_spread_ticks() else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        let cur_l1_px = side.select_entry_bbo_price(best_bid.price, best_ask.price);
        if cur_l1_px.0 <= 0 || (cur_l1_px.0 - armed_px.0).abs() > max_drift_ticks {
            return Err(VortexError::InvalidPriceOrTickSpec);
        }
        let new_sl = Self::compute_fill_or_adopt_sl_ticks(side, cur_l1_px, snap)?;
        if let OrderLifecycleState::PlacingEntryMaker {
            ref mut entry_price,
            ref mut sl_trigger_price,
            ..
        } = self.state
        {
            *entry_price = cur_l1_px;
            *sl_trigger_price = new_sl;
        }
        Ok((cur_l1_px, new_sl))
    }

    /// Validates Stop-Loss geometry at a new L1 price prior to re-pegging a resting order.
    pub fn compute_repeg_sl_trigger(
        &self,
        snap: &SymbolLobSnapshot,
        new_l1_price: PriceTicks,
    ) -> Result<PriceTicks, VortexError> {
        let OrderLifecycleState::RestingEntryQueue { side, .. } = self.state else {
            return Err(VortexError::InvalidPriceOrTickSpec);
        };
        Self::compute_fill_or_adopt_sl_ticks(side, new_l1_price, snap)
    }

    #[inline]
    pub fn apply_resting_repeg(
        &mut self,
        new_l1_price: PriceTicks,
        new_sl_price: PriceTicks,
        now_ts: TimestampNs,
    ) {
        if let OrderLifecycleState::RestingEntryQueue {
            ref mut entry_price,
            ref mut sl_trigger_price,
            ref mut resting_since_ts,
            ..
        } = self.state
        {
            *entry_price = new_l1_price;
            *sl_trigger_price = new_sl_price;
            *resting_since_ts = now_ts;
        }
    }

    pub fn on_entry_order_resting(
        &mut self,
        resting_ts: TimestampNs,
    ) -> Result<OrderLifecycleState, VortexError> {
        if let OrderLifecycleState::PlacingEntryMaker {
            side,
            entry_price,
            qty,
            target_excursion,
            sl_trigger_price,
            ..
        } = self.state
        {
            self.state = OrderLifecycleState::RestingEntryQueue {
                side,
                entry_price,
                qty,
                target_excursion,
                sl_trigger_price,
                resting_since_ts: resting_ts,
            };
        }
        Ok(self.state)
    }

    /// F-02 & F-06 Fix: Promotes entry to PositionOpenMakerExit with 110..145 bps SL.
    pub fn on_maker_entry_filled_verified(
        &mut self,
        fill_ts: TimestampNs,
        verified_side: Option<Side>,
        verified_avg_price: Option<PriceTicks>,
        verified_open_lots: Option<LotQty>,
    ) -> Result<OrderLifecycleState, VortexError> {
        let (state_side, state_entry_px, state_qty, target_excursion, state_sl_px) =
            match self.state {
                OrderLifecycleState::RestingEntryQueue {
                    side,
                    entry_price,
                    qty,
                    target_excursion,
                    sl_trigger_price,
                    ..
                }
                | OrderLifecycleState::PlacingEntryMaker {
                    side,
                    entry_price,
                    qty,
                    target_excursion,
                    sl_trigger_price,
                    ..
                } => (side, entry_price, qty, target_excursion, sl_trigger_price),
                _ => return Ok(self.state),
            };

        let side = verified_side.unwrap_or(state_side);
        let eff_entry_px = match verified_avg_price {
            Some(px) if px.0 > 0 => px,
            _ => state_entry_px,
        };
        let eff_lots = match verified_open_lots {
            Some(lq) if lq.0 > 0 => lq,
            _ => LotQty(state_qty.0.max(1)),
        };
        let eff_sl_px = if verified_avg_price.is_some()
            && (eff_entry_px != state_entry_px || side != state_side)
        {
            let sl_dist = (state_entry_px.0 - state_sl_px.0).abs().max(4);
            match side {
                Side::Bid => PriceTicks((eff_entry_px.0 - sl_dist).max(1)),
                Side::Ask => PriceTicks(eff_entry_px.0.saturating_add(sl_dist)),
            }
        } else {
            state_sl_px
        };

        let tp_bps = target_excursion.raw().max(520);
        let structural_tp_price =
            dynamic_maker_floor_price(side, eff_entry_px, ScaledBps(tp_bps));
        let q_floor = defensive_zero_loss_maker_exit_price(side, eff_entry_px);
        let can_split = self.slot_idx >= 1 && eff_lots.0 >= 2;
        let (init_role, init_exit_px, init_exit_lots) = if can_split {
            (ExitOrderRole::Tranche1Harvest50, q_floor, LotQty(eff_lots.0 / 2))
        } else {
            (ExitOrderRole::StandardMakerExit100, structural_tp_price, eff_lots)
        };

        self.last_opened_ts = fill_ts;
        self.last_eff_sl_ticks = eff_sl_px;
        self.tranche1_harvests_count = 0;
        self.lvdbs_deep_surfs_count = 0;
        self.last_closed_mfe_bps = 0;
        self.last_closed_mae_bps = 0;
        self.last_closed_wald_pct = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = fill_ts.0;
        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price: eff_entry_px,
            avg_price_ticks: eff_entry_px,
            peak_favorable_price: eff_entry_px,
            worst_adverse_price: eff_entry_px,
            initial_lots: eff_lots,
            open_lots: eff_lots,
            tranche1_harvested: false,
            tranche1_lots: LotQty(0),
            tranche1_exit_price: PriceTicks(0),
            defensive_floor_locked: false,
            active_exit_role: init_role,
            active_exit_price: init_exit_px,
            active_exit_lots: init_exit_lots,
            structural_tp_price,
            resting_exit_price: PriceTicks(0),
            sl_trigger_price: eff_sl_px,
            opened_ts: fill_ts,
        };
        Ok(self.state)
    }

    pub fn on_maker_entry_filled(
        &mut self,
        fill_ts: TimestampNs,
    ) -> Result<OrderLifecycleState, VortexError> {
        self.on_maker_entry_filled_verified(fill_ts, None, None, None)
    }

    /// F-06 Fix: Adopts live Bybit position on startup with validated 2-tick SL geometry.
    pub fn adopt_live_open_position(
        &mut self,
        side: Side,
        verified_avg_price: PriceTicks,
        initial_lots: LotQty,
        open_lots: LotQty,
        tranche1_harvested: bool,
        peak_price: PriceTicks,
        worst_price: PriceTicks,
        opened_ts: TimestampNs,
        snap: &SymbolLobSnapshot,
    ) -> Result<PriceTicks, VortexError> {
        let sl_t = Self::compute_fill_or_adopt_sl_ticks(side, verified_avg_price, snap)?;
        let eff_open = LotQty(open_lots.0.max(1));
        let eff_init = LotQty(initial_lots.0.max(eff_open.0));
        let q_fl = defensive_zero_loss_maker_exit_price(side, verified_avg_price);
        let struct_tp =
            dynamic_maker_floor_price(side, verified_avg_price, ScaledBps(540));
        let harvested_lots = LotQty((eff_init.0 - eff_open.0).max(0));
        let t1_done = tranche1_harvested || harvested_lots.0 > 0;
        let t1_px = if t1_done { q_fl } else { PriceTicks(0) };
        let can_split = self.slot_idx >= 1 && !t1_done && eff_open.0 >= 2;
        let (role, init_px, init_lots) = if can_split {
            (ExitOrderRole::Tranche1Harvest50, q_fl, LotQty(eff_open.0 / 2))
        } else if t1_done {
            (ExitOrderRole::LvdbsDeepSurf50, struct_tp, eff_open)
        } else {
            (ExitOrderRole::StandardMakerExit100, struct_tp, eff_open)
        };

        self.last_opened_ts = opened_ts;
        self.last_eff_sl_ticks = sl_t;
        self.tranche1_harvests_count = u64::from(t1_done);
        self.lvdbs_deep_surfs_count = 0;
        self.exit_peg_attempts = 0;
        self.close_reconcile_retries = 0;
        self.next_peg_allowed_ts_ns = opened_ts.0;
        self.state = OrderLifecycleState::PositionOpenMakerExit {
            side,
            entry_price: verified_avg_price,
            avg_price_ticks: verified_avg_price,
            peak_favorable_price: if peak_price.0 > 0 { peak_price } else { verified_avg_price },
            worst_adverse_price: if worst_price.0 > 0 {
                worst_price
            } else {
                verified_avg_price
            },
            initial_lots: eff_init,
            open_lots: eff_open,
            tranche1_harvested: t1_done,
            tranche1_lots: harvested_lots,
            tranche1_exit_price: t1_px,
            defensive_floor_locked: false,
            active_exit_role: role,
            active_exit_price: init_px,
            active_exit_lots: init_lots,
            structural_tp_price: struct_tp,
            resting_exit_price: PriceTicks(0),
            sl_trigger_price: sl_t,
            opened_ts,
        };
        Ok(sl_t)
    }

    /// Keeps open position entry_price, avg_price_ticks & structural_tp locked to Bybit avgPrice.
    #[inline]
    pub fn sync_open_avg_price_ticks(&mut self, verified_avg_price: PriceTicks) {
        if verified_avg_price.0 <= 0 {
            return;
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            side,
            ref mut entry_price,
            ref mut avg_price_ticks,
            ref mut peak_favorable_price,
            ref mut worst_adverse_price,
            ref mut structural_tp_price,
            ..
        } = self.state
        {
            if *avg_price_ticks != verified_avg_price {
                let prev_avg = avg_price_ticks.0.max(1);
                let prev_diff = (structural_tp_price.0 - prev_avg).abs();
                let prev_bps = ((prev_diff.saturating_mul(1_000_000)) / prev_avg).max(520);
                *entry_price = verified_avg_price;
                *avg_price_ticks = verified_avg_price;
                *structural_tp_price = dynamic_maker_floor_price(
                    side,
                    verified_avg_price,
                    ScaledBps(prev_bps),
                );
                match side {
                    Side::Bid => {
                        if peak_favorable_price.0 < verified_avg_price.0 {
                            *peak_favorable_price = verified_avg_price;
                        }
                        if worst_adverse_price.0 > verified_avg_price.0 {
                            *worst_adverse_price = verified_avg_price;
                        }
                    }
                    Side::Ask => {
                        if peak_favorable_price.0 > verified_avg_price.0 {
                            *peak_favorable_price = verified_avg_price;
                        }
                        if worst_adverse_price.0 < verified_avg_price.0 {
                            *worst_adverse_price = verified_avg_price;
                        }
                    }
                }
            }
        }
    }

    /// Monotonic 1-way latch: once defensive mode activates, 50%<->100% oscillation is banned.
    #[inline]
    pub fn latch_defensive_floor_if_needed(&mut self, lock_active: bool) {
        if !lock_active {
            return;
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            ref mut defensive_floor_locked,
            ..
        } = self.state
        {
            *defensive_floor_locked = true;
        }
    }

    /// F-05 Fix: Records Tranche 1 (50%) harvest without premature ledger double-crediting.
    pub fn record_tranche1_harvest(&mut self, fill_price: PriceTicks) -> bool {
        let OrderLifecycleState::PositionOpenMakerExit {
            ref mut open_lots,
            ref mut tranche1_harvested,
            ref mut tranche1_lots,
            ref mut tranche1_exit_price,
            ref mut active_exit_role,
            ref mut active_exit_lots,
            ..
        } = self.state
        else {
            return false;
        };
        if *tranche1_harvested || open_lots.0 < 2 {
            return false;
        }
        let harvested = LotQty(open_lots.0 / 2);
        *open_lots = LotQty(open_lots.0 - harvested.0);
        *tranche1_harvested = true;
        *tranche1_lots = harvested;
        *tranche1_exit_price = fill_price;
        *active_exit_role = ExitOrderRole::LvdbsDeepSurf50;
        *active_exit_lots = *open_lots;
        self.tranche1_harvests_count = self.tranche1_harvests_count.saturating_add(1);
        true
    }
    #[inline]
    fn classify_maker_exit_sub_reason(
        side: Side,
        entry_px: PriceTicks,
        exit_px: PriceTicks,
        tranche1_harvested: bool,
        legs_count: u32,
        vac_level_hint: usize,
    ) -> ExitSubReason {
        let raw_d = match side {
            Side::Bid => exit_px.0 - entry_px.0,
            Side::Ask => entry_px.0 - exit_px.0,
        };
        let gross_bps = (raw_d.saturating_mul(1_000_000)) / entry_px.0.max(1);
        if gross_bps >= 500 || vac_level_hint >= 2 {
            ExitSubReason::LvdbsDeepVacuumSurf
        } else if tranche1_harvested || legs_count >= 2 {
            ExitSubReason::Tranche1Harvest50
        } else {
            ExitSubReason::AdverseScratchEscape
        }
    }

    /// F-03 Fix: Reconciles position closure deterministically against /v5/position/closed-pnl.
    #[must_use]
    pub fn reconcile_closed_pnl_payload(
        &self,
        payload: &serde_json::Value,
        vac_level_hint: usize,
        allow_partial_lot_coverage: bool,
    ) -> ClosedPnlQueryOutcome {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            initial_lots,
            open_lots,
            tranche1_harvested,
            opened_ts,
            ..
        } = self.state
        else {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        };
        if self.slot_idx >= MAX_ACTIVE_PAIRS {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        }
        if payload.get("retCode").and_then(|c| c.as_i64()) != Some(0) {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        }
        let Some(list) = payload
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        else {
            return ClosedPnlQueryOutcome::TransportOrApiError;
        };
        if list.is_empty() {
            return ClosedPnlQueryOutcome::PendingIndexerRetry;
        }

        let spec = APPROVED_SPECS[self.slot_idx];
        let sym_expected = spec.symbol_id.name();
        let expected_close_side = match side {
            Side::Bid => "Sell",
            Side::Ask => "Buy",
        };
        let opened_ms = (opened_ts.0 / 1_000_000).max(0);
        let min_latest_ms = opened_ms.saturating_sub(1_500);
        let min_adopt_leg_ms = if tranche1_harvested {
            opened_ms.saturating_sub(86_400_000)
        } else {
            min_latest_ms
        };
        let max_entry_tol_ticks = (avg_price_ticks.0.max(1) / 400).max(25);
        let target_total_lots = initial_lots.0.max(open_lots.0).max(1);
        let min_req_lots = if allow_partial_lot_coverage {
            open_lots.0.max(1)
        } else {
            target_total_lots
        };

        let mut accum_lots: i64 = 0;
        let mut weighted_entry_sum: i128 = 0;
        let mut weighted_exit_sum: i128 = 0;
        let mut total_closed_pnl_1e8: i64 = 0;
        let mut has_pnl_field = false;
        let mut any_market_or_forced = false;
        let mut legs_count: u32 = 0;

        for item in list {
            if accum_lots >= target_total_lots {
                break;
            }
            if item
                .get("symbol")
                .and_then(|v| v.as_str())
                .is_some_and(|sym_s| sym_s != sym_expected)
            {
                continue;
            }
            if item
                .get("side")
                .and_then(|v| v.as_str())
                .is_some_and(|cs| cs != expected_close_side)
            {
                continue;
            }
            let upd_ms = extract_json_epoch_ms(item, "updatedTime");
            let crt_ms = extract_json_epoch_ms(item, "createdTime");
            let rec_ms = upd_ms.max(crt_ms);
            let min_bound_ms = if legs_count == 0 {
                min_latest_ms
            } else {
                min_adopt_leg_ms
            };
            if rec_ms > 0 && min_bound_ms > 0 && rec_ms < min_bound_ms {
                continue;
            }

            let Some(avg_en_s) = item.get("avgEntryPrice").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(avg_ex_s) = item.get("avgExitPrice").and_then(|v| v.as_str()) else {
                continue;
            };
            let Ok(leg_en_t) = parse_verified_avg_price_ticks(spec, side, avg_en_s) else {
                continue;
            };
            let Ok(leg_ex_t) = parse_verified_exit_price_ticks(spec, side, avg_ex_s) else {
                continue;
            };
            if (leg_en_t.0 - avg_price_ticks.0).abs() > max_entry_tol_ticks {
                continue;
            }

            let raw_sz_s = item
                .get("closedSize")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty() && s.trim() != "0")
                .or_else(|| item.get("qty").and_then(|v| v.as_str()))
                .unwrap_or("0");
            let parsed_lots = spec.parse_lot_qty(raw_sz_s).0;
            if parsed_lots <= 0 {
                continue;
            }
            let rem_cap = (target_total_lots - accum_lots).max(1);
            let take_lots = parsed_lots.min(rem_cap);

            let order_type_s = item
                .get("orderType")
                .and_then(|v| v.as_str())
                .unwrap_or("Limit");
            let exec_type_s = item
                .get("execType")
                .and_then(|v| v.as_str())
                .unwrap_or("Trade");
            let is_forced_exec = matches!(
                exec_type_s,
                "BustTrade" | "AdlTrade" | "TakeOver" | "Settle"
            );
            if order_type_s != "Limit" || is_forced_exec {
                any_market_or_forced = true;
            }

            if let Some(Ok(leg_pnl)) = item
                .get("closedPnl")
                .and_then(|v| v.as_str())
                .map(parse_signed_decimal_1e8)
            {
                let scaled_pnl = if take_lots < parsed_lots {
                    ((leg_pnl as i128 * take_lots as i128) / (parsed_lots as i128)) as i64
                } else {
                    leg_pnl
                };
                total_closed_pnl_1e8 = total_closed_pnl_1e8.saturating_add(scaled_pnl);
                has_pnl_field = true;
            }

            accum_lots = accum_lots.saturating_add(take_lots);
            weighted_entry_sum += (take_lots as i128) * (leg_en_t.0 as i128);
            weighted_exit_sum += (take_lots as i128) * (leg_ex_t.0 as i128);
            legs_count = legs_count.saturating_add(1);
        }

        if legs_count == 0 || accum_lots < min_req_lots {
            return ClosedPnlQueryOutcome::PendingIndexerRetry;
        }

        let denom = accum_lots as i128;
        let verified_entry_ticks = match side {
            Side::Bid => ((weighted_entry_sum + denom - 1) / denom) as i64,
            Side::Ask => (weighted_entry_sum / denom).max(1) as i64,
        };
        let verified_exit_ticks = match side {
            Side::Bid => (weighted_exit_sum / denom).max(1) as i64,
            Side::Ask => ((weighted_exit_sum + denom - 1) / denom) as i64,
        };

        let verified_entry_price = PriceTicks(verified_entry_ticks.max(1));
        let verified_exit_price = PriceTicks(verified_exit_ticks.max(1));
        let (kind, sub_reason) = if any_market_or_forced {
            (
                ClosedTradeKind::ServerMarkPriceStopLoss,
                ExitSubReason::WaldBaseStopLoss,
            )
        } else {
            let sub = Self::classify_maker_exit_sub_reason(
                side,
                verified_entry_price,
                verified_exit_price,
                tranche1_harvested,
                legs_count,
                vac_level_hint,
            );
            (ClosedTradeKind::MakerPassiveUnwind, sub)
        };

        ClosedPnlQueryOutcome::Verified(VerifiedCloseResolution {
            kind,
            sub_reason,
            verified_entry_price,
            verified_exit_price,
            closed_lots: LotQty(accum_lots),
            legs_count,
            exchange_closed_pnl_1e8: if has_pnl_field {
                Some(total_closed_pnl_1e8)
            } else {
                None
            },
        })
    }

    /// F-03 Safe Fallback: Used only if /v5/position/closed-pnl fails after bounded retries.
    #[must_use]
    pub fn resolve_fallback_unverified_close(
        &self,
        snap: &SymbolLobSnapshot,
        vac_level_hint: usize,
    ) -> Option<VerifiedCloseResolution> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            worst_adverse_price,
            initial_lots,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            resting_exit_price,
            sl_trigger_price,
            ..
        } = self.state
        else {
            return None;
        };

        let eff_sl = if self.last_eff_sl_ticks.0 > 0 {
            self.last_eff_sl_ticks
        } else {
            sl_trigger_price
        };
        let cur_bbo = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let mkt_ref = if snap.mark_price.0 > 0 {
            snap.mark_price
        } else if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            cur_bbo
        };

        let sl_breached = match side {
            Side::Bid => {
                (mkt_ref.0 > 0 && mkt_ref.0 <= eff_sl.0.saturating_add(2))
                    || (worst_adverse_price.0 > 0 && worst_adverse_price.0 <= eff_sl.0)
            }
            Side::Ask => {
                (mkt_ref.0 > 0 && mkt_ref.0 >= eff_sl.0.saturating_sub(2))
                    || worst_adverse_price.0 >= eff_sl.0
            }
        };
        let no_resting_tp_underwater = resting_exit_price.0 <= 0
            && match side {
                Side::Bid => mkt_ref.0 > 0 && mkt_ref.0 < avg_price_ticks.0,
                Side::Ask => mkt_ref.0 > avg_price_ticks.0,
            };

        let tot_lots = initial_lots.max(open_lots).max(LotQty(1));
        if sl_breached || no_resting_tp_underwater {
            let sl_exit_px = if sl_breached {
                eff_sl
            } else {
                PriceTicks(mkt_ref.0.max(1))
            };
            return Some(VerifiedCloseResolution {
                kind: ClosedTradeKind::ServerMarkPriceStopLoss,
                sub_reason: ExitSubReason::WaldBaseStopLoss,
                verified_entry_price: avg_price_ticks,
                verified_exit_price: sl_exit_px,
                closed_lots: tot_lots,
                legs_count: if tranche1_harvested { 2 } else { 1 },
                exchange_closed_pnl_1e8: None,
            });
        }

        let q_fl = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let t2_px = if resting_exit_price.0 > 0 {
            resting_exit_price
        } else {
            q_fl
        };
        let exit_px = if tranche1_harvested && tranche1_lots.0 > 0 {
            let t1_px = if tranche1_exit_price.0 > 0 {
                tranche1_exit_price
            } else {
                q_fl
            };
            let q1 = tranche1_lots.0 as i128;
            let q2 = open_lots.0.max(1) as i128;
            let w_sum = q1 * (t1_px.0 as i128) + q2 * (t2_px.0 as i128);
            let denom = (q1 + q2).max(1);
            let avg_ex = match side {
                Side::Bid => (w_sum / denom).max(1) as i64,
                Side::Ask => ((w_sum + denom - 1) / denom) as i64,
            };
            PriceTicks(avg_ex)
        } else {
            t2_px
        };
        let sub_reason = Self::classify_maker_exit_sub_reason(
            side,
            avg_price_ticks,
            exit_px,
            tranche1_harvested,
            if tranche1_harvested { 2 } else { 1 },
            vac_level_hint,
        );

        Some(VerifiedCloseResolution {
            kind: ClosedTradeKind::MakerPassiveUnwind,
            sub_reason,
            verified_entry_price: avg_price_ticks,
            verified_exit_price: exit_px,
            closed_lots: tot_lots,
            legs_count: if tranche1_harvested { 2 } else { 1 },
            exchange_closed_pnl_1e8: None,
        })
    }

    /// Applies a VerifiedCloseResolution atomically and records the canonical close summary.
    pub fn apply_verified_close_resolution(
        &mut self,
        resolution: VerifiedCloseResolution,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit { side, .. } = self.state else {
            return None;
        };
        self.last_exit_sub_reason = resolution.sub_reason;
        if matches!(resolution.kind, ClosedTradeKind::MakerPassiveUnwind)
            && matches!(resolution.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf)
        {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
        }
        Some(self.finalize_close_with_lots(
            resolution.kind,
            side,
            resolution.verified_entry_price,
            resolution.verified_exit_price,
            resolution.closed_lots,
            resolution.legs_count,
            resolution.exchange_closed_pnl_1e8,
            closed_ts,
            ledger,
        ))
    }

    /// F-04 Fix: Unified Canonical Exit Quoter (1:1 SSOT for live daemon & offline simulator).
    #[must_use]
    pub fn compute_canonical_exit_quote(
        &self,
        snap: &SymbolLobSnapshot,
    ) -> Option<CanonicalExitQuote> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            initial_lots,
            open_lots,
            tranche1_harvested,
            defensive_floor_locked,
            structural_tp_price,
            opened_ts,
            ..
        } = self.state
        else {
            return None;
        };
        if self.slot_idx >= MAX_ACTIVE_PAIRS {
            return None;
        }

        let spec = APPROVED_SPECS[self.slot_idx];
        let avg_denom = avg_price_ticks.0.max(1);
        let cur_bbo = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let cur_diff = match side {
            Side::Bid => cur_bbo.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - cur_bbo.0,
        };
        let cur_gross_bps = (cur_diff.saturating_mul(1_000_000)) / avg_denom;
        let hold_ns = snap.ts_local_ns.0.saturating_sub(opened_ts.0).max(0);
        let hold_s = hold_ns / 1_000_000_000;

        let dir_s = side.sign_f64();
        let s_ofi = snap.factor_ofi.get() * dir_s;
        let s_tfi = snap.factor_tfi.get() * dir_s;
        let s_geom = snap.factor_geom.get() * dir_s;
        let signed_flow = 0.55 * s_ofi + 0.45 * s_tfi;
        let joint_adverse = s_ofi < -0.18 && s_tfi < -0.18;

        let time_c = (hold_s / 60).min(18) * 12;
        let flow_c = if signed_flow < -0.35 {
            ((-signed_flow - 0.35) * 280.0).round() as i64
        } else {
            0
        };
        let depth_c = if s_geom < -0.25 {
            ((-s_geom - 0.25) * 180.0).round() as i64
        } else {
            0
        };
        let wald_pct = ((time_c + flow_c + depth_c) / 10).clamp(0, 48);

        let peak_gross_bps = self.last_closed_mfe_bps;
        let worst_mae_bps = self.last_closed_mae_bps;
        let vol_c = snap.realized_vol_5m.raw().clamp(4_000, 12_500);
        let mfe_c = self.learned_mfe_bps.clamp(540, 1_250);
        let tau_bps = spec
            .tick_to_price_scaled_bps(avg_price_ticks)
            .unwrap_or(ScaledBps(50))
            .raw();
        let mae_trig_bps = -((3 * tau_bps).max(vol_c / 18));
        let t_def_s = (((75_i64 * 6_500) / vol_c) * mfe_c / 680).clamp(50, 110);

        let defensive_now = if tranche1_harvested {
            let t_def_t2 = (t_def_s + 30).clamp(80, 140);
            defensive_floor_locked
                || worst_mae_bps <= (mae_trig_bps - 120)
                || hold_s >= t_def_t2
                || (hold_s >= 45 && joint_adverse)
        } else {
            defensive_floor_locked
                || worst_mae_bps <= mae_trig_bps
                || hold_s >= t_def_s
                || (hold_s >= 25 && joint_adverse)
        };

        let mfe_ratchet_lock = if tranche1_harvested {
            (hold_s >= 45 && cur_gross_bps >= 485 && signed_flow < -0.10)
                || (peak_gross_bps >= 900
                    && cur_gross_bps >= 520
                    && cur_gross_bps <= peak_gross_bps - 250
                    && signed_flow < 0.05)
        } else {
            peak_gross_bps >= 560
                && cur_gross_bps >= 465
                && signed_flow < 0.12
                && (cur_gross_bps <= peak_gross_bps - 60 || signed_flow < -0.05)
        };
        let ratchet_bps = if peak_gross_bps > 800 {
            let r70 = 400 + ((peak_gross_bps - 400) * 70) / 100;
            r70.min(cur_gross_bps.max(465))
        } else {
            cur_gross_bps.clamp(465, 540)
        };

        let can_split_tranche1 = self.slot_idx >= 1
            && !tranche1_harvested
            && initial_lots.0 >= 2
            && open_lots.0 >= 2
            && !defensive_now
            && !mfe_ratchet_lock;

        let q_floor = defensive_zero_loss_maker_exit_price(side, avg_price_ticks);
        let struct_floor = if structural_tp_price.0 > 0 {
            structural_tp_price
        } else {
            dynamic_maker_floor_price(side, avg_price_ticks, ScaledBps(520))
        };

        let (role, sub_reason, raw_target_px, order_lots, vac_level, def_lock) =
            if can_split_tranche1 {
                (
                    ExitOrderRole::Tranche1Harvest50,
                    ExitSubReason::Tranche1Harvest50,
                    q_floor,
                    LotQty((open_lots.0 / 2).max(1)),
                    1_usize,
                    false,
                )
            } else if defensive_now || mfe_ratchet_lock {
                let fl_bps = if mfe_ratchet_lock {
                    ScaledBps(ratchet_bps)
                } else {
                    ScaledBps(if tranche1_harvested { 465 } else { 440 })
                };
                let fl_px = dynamic_maker_floor_price(side, avg_price_ticks, fl_bps);
                let sub = if fl_bps.raw() >= 500 {
                    ExitSubReason::LvdbsDeepVacuumSurf
                } else {
                    ExitSubReason::AdverseScratchEscape
                };
                (
                    ExitOrderRole::StandardMakerExit100,
                    sub,
                    fl_px,
                    open_lots,
                    1_usize,
                    defensive_now,
                )
            } else {
                let (vac_lvl, lvdbs_px, _) =
                    OrthogonalFactorState::compute_l2_l3_vacuum_surf_target_with_structural(
                        side,
                        snap,
                        q_floor,
                        struct_floor,
                    );
                let role_surf = if tranche1_harvested {
                    ExitOrderRole::LvdbsDeepSurf50
                } else {
                    ExitOrderRole::StandardMakerExit100
                };
                let sub_surf = if vac_lvl >= 2 || struct_floor.0 != q_floor.0 {
                    ExitSubReason::LvdbsDeepVacuumSurf
                } else {
                    ExitSubReason::AdverseScratchEscape
                };
                (role_surf, sub_surf, lvdbs_px, open_lots, vac_lvl, false)
            };

        let min_sell_px = snap.asks[0].price.0.max(snap.bids[0].price.0 + 1).max(1);
        let max_buy_px = if snap.bids[0].price.0 > 0 && snap.asks[0].price.0 > 1 {
            snap.bids[0].price.0.min(snap.asks[0].price.0 - 1).max(1)
        } else {
            snap.bids[0].price.0.max(1)
        };
        let target_price = match side {
            Side::Bid => PriceTicks(raw_target_px.0.max(min_sell_px).max(q_floor.0)),
            Side::Ask => PriceTicks(raw_target_px.0.min(max_buy_px).min(q_floor.0).max(1)),
        };

        Some(CanonicalExitQuote {
            role,
            sub_reason,
            target_price,
            order_lots,
            vac_level,
            cur_gross_bps,
            wald_pct,
            defensive_lock_active: def_lock,
        })
    }

    /// F-05 Fix: Exact lot-notional close accounting (1 Entry = 1 Consolidated Ledger Record).
    pub fn finalize_close_with_lots(
        &mut self,
        kind: ClosedTradeKind,
        side: Side,
        entry_price: PriceTicks,
        exit_price: PriceTicks,
        closed_lots: LotQty,
        legs_consolidated: u32,
        exchange_closed_pnl_1e8: Option<i64>,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> ClosedTradeSummary {
        let is_maker_exit = !matches!(kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee_scaled_bps = if is_maker_exit {
            ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2)
        } else {
            ScaledBps(
                ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw()
                    + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.raw(),
            )
        };
        let eff_lots = LotQty(closed_lots.0.max(1));
        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let (calc_pnl_1e8, fee_1e8, gross_scaled_bps, net_scaled_bps) =
            spec.compute_leg_pnl_and_fee_1e8(
                side,
                eff_lots,
                entry_price,
                exit_price,
                fee_scaled_bps,
            );
        let net_pnl_1e8 = exchange_closed_pnl_1e8.unwrap_or(calc_pnl_1e8);

        if is_maker_exit {
            self.maker_exits_count = self.maker_exits_count.saturating_add(1);
        } else {
            self.taker_sl_exits_count = self.taker_sl_exits_count.saturating_add(1);
        }
        if net_pnl_1e8 >= 0 {
            self.gross_profit_accum_1e8 =
                self.gross_profit_accum_1e8.saturating_add(net_pnl_1e8);
        } else {
            self.gross_loss_accum_1e8 =
                self.gross_loss_accum_1e8.saturating_add(net_pnl_1e8.abs());
        }

        ledger.record_closed_trade_with_sinking_split(
            net_pnl_1e8,
            fee_1e8,
            net_scaled_bps,
        );
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_scaled_bps.raw()));
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + 3_000_000_000);
        self.close_reconcile_retries = 0;
        self.state = OrderLifecycleState::Idle;

        ClosedTradeSummary {
            kind,
            side,
            entry_price,
            exit_price,
            closed_lots: eff_lots,
            legs_consolidated: legs_consolidated.max(1),
            gross_scaled_bps,
            fee_scaled_bps,
            net_scaled_bps,
            net_pnl_1e8,
            fee_1e8,
            is_maker_exit,
        }
    }

    /// Consolidates Leg 1 (if harvested) + Leg 2 using exact PairSpec lot notional (F-05).
    pub fn finalize_open_position_close(
        &mut self,
        leg2_kind: ClosedTradeKind,
        leg2_exit_price: PriceTicks,
        closed_ts: TimestampNs,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            open_lots,
            tranche1_harvested,
            tranche1_lots,
            tranche1_exit_price,
            ..
        } = self.state
        else {
            return None;
        };

        let q2 = LotQty(open_lots.0.max(1));
        if !tranche1_harvested || tranche1_lots.0 <= 0 || tranche1_exit_price.0 <= 0 {
            return Some(self.finalize_close_with_lots(
                leg2_kind,
                side,
                avg_price_ticks,
                leg2_exit_price,
                q2,
                1,
                None,
                closed_ts,
                ledger,
            ));
        }

        let spec = APPROVED_SPECS[self.slot_idx.min(MAX_ACTIVE_PAIRS - 1)];
        let q1 = tranche1_lots;
        let fee1_bps = ScaledBps(ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw() * 2);
        let is_maker_exit = !matches!(leg2_kind, ClosedTradeKind::ServerMarkPriceStopLoss);
        let fee2_bps = if is_maker_exit {
            fee1_bps
        } else {
            ScaledBps(
                ScaledBps::BYBIT_VIP0_MAKER_FEE_BPS.raw()
                    + ScaledBps::BYBIT_VIP0_TAKER_FEE_BPS.raw(),
            )
        };

        let (pnl1, fee1, g1, _) = spec.compute_leg_pnl_and_fee_1e8(
            side,
            q1,
            avg_price_ticks,
            tranche1_exit_price,
            fee1_bps,
        );
        let (pnl2, fee2, g2, _) = spec.compute_leg_pnl_and_fee_1e8(
            side,
            q2,
            avg_price_ticks,
            leg2_exit_price,
            fee2_bps,
        );

        let tot_q = (q1.0 + q2.0).max(1);
        let tot_q_128 = tot_q as i128;
        let w_exit = (q1.0 as i128) * (tranche1_exit_price.0 as i128)
            + (q2.0 as i128) * (leg2_exit_price.0 as i128);
        let avg_exit_ticks = match side {
            Side::Bid => (w_exit / tot_q_128).max(1) as i64,
            Side::Ask => ((w_exit + tot_q_128 - 1) / tot_q_128) as i64,
        };
        let gross_bps = (q1.0 * g1.raw() + q2.0 * g2.raw()) / tot_q;
        let fee_bps = (q1.0 * fee1_bps.raw() + q2.0 * fee2_bps.raw()) / tot_q;
        let net_bps = gross_bps - fee_bps;
        let net_pnl_1e8 = pnl1.saturating_add(pnl2);
        let fee_1e8 = fee1.saturating_add(fee2);

        if is_maker_exit {
            self.maker_exits_count = self.maker_exits_count.saturating_add(1);
        } else {
            self.taker_sl_exits_count = self.taker_sl_exits_count.saturating_add(1);
        }
        if net_pnl_1e8 >= 0 {
            self.gross_profit_accum_1e8 =
                self.gross_profit_accum_1e8.saturating_add(net_pnl_1e8);
        } else {
            self.gross_loss_accum_1e8 =
                self.gross_loss_accum_1e8.saturating_add(net_pnl_1e8.abs());
        }

        ledger.record_closed_trade_with_sinking_split(
            net_pnl_1e8,
            fee_1e8,
            ScaledBps(net_bps),
        );
        self.observe_mfe_sample(self.last_closed_mfe_bps.max(gross_bps));
        self.cooldown_until_ns = TimestampNs(closed_ts.0 + 3_000_000_000);
        self.close_reconcile_retries = 0;
        self.state = OrderLifecycleState::Idle;

        Some(ClosedTradeSummary {
            kind: leg2_kind,
            side,
            entry_price: avg_price_ticks,
            exit_price: PriceTicks(avg_exit_ticks),
            closed_lots: LotQty(tot_q),
            legs_consolidated: 2,
            gross_scaled_bps: ScaledBps(gross_bps),
            fee_scaled_bps: ScaledBps(fee_bps),
            net_scaled_bps: ScaledBps(net_bps),
            net_pnl_1e8,
            fee_1e8,
            is_maker_exit,
        })
    }

    pub fn step_open_position(
        &mut self,
        snap: &SymbolLobSnapshot,
        _btc_leader_snap: &SymbolLobSnapshot,
        _gov: &OnlineAdaptiveGovernor,
        ledger: &mut ParitySyncLedger,
    ) -> Option<ClosedTradeSummary> {
        let OrderLifecycleState::PositionOpenMakerExit {
            side,
            avg_price_ticks,
            mut peak_favorable_price,
            mut worst_adverse_price,
            sl_trigger_price,
            ..
        } = self.state
        else {
            return None;
        };
        let cur_bbo_ref = match side {
            Side::Bid => snap.asks[0].price,
            Side::Ask => snap.bids[0].price,
        };
        let ref_trade = if snap.last_trade_price.0 > 0 {
            snap.last_trade_price
        } else {
            cur_bbo_ref
        };
        match side {
            Side::Bid => {
                if ref_trade.0 > peak_favorable_price.0 {
                    peak_favorable_price = ref_trade;
                }
                if ref_trade.0 > 0 && ref_trade.0 < worst_adverse_price.0 {
                    worst_adverse_price = ref_trade;
                }
            }
            Side::Ask => {
                if ref_trade.0 > 0 && ref_trade.0 < peak_favorable_price.0 {
                    peak_favorable_price = ref_trade;
                }
                if ref_trade.0 > worst_adverse_price.0 {
                    worst_adverse_price = ref_trade;
                }
            }
        }
        if let OrderLifecycleState::PositionOpenMakerExit {
            peak_favorable_price: ref mut pk,
            worst_adverse_price: ref mut wr,
            ..
        } = self.state
        {
            *pk = peak_favorable_price;
            *wr = worst_adverse_price;
        }
        let peak_diff = match side {
            Side::Bid => peak_favorable_price.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - peak_favorable_price.0,
        };
        let worst_diff = match side {
            Side::Bid => worst_adverse_price.0 - avg_price_ticks.0,
            Side::Ask => avg_price_ticks.0 - worst_adverse_price.0,
        };
        let avg_denom = avg_price_ticks.0.max(1);
        self.last_closed_mfe_bps = (peak_diff.saturating_mul(1_000_000)) / avg_denom;
        self.last_closed_mae_bps = (worst_diff.saturating_mul(1_000_000)) / avg_denom;
        self.last_closed_peak_ticks = peak_favorable_price;
        self.last_closed_worst_ticks = worst_adverse_price;
        self.last_eff_sl_ticks = sl_trigger_price;

        let sl_hit = match side {
            Side::Bid => snap.mark_price.0 > 0 && snap.mark_price.0 <= sl_trigger_price.0,
            Side::Ask => snap.mark_price.0 > 0 && snap.mark_price.0 >= sl_trigger_price.0,
        };
        if sl_hit {
            self.last_exit_sub_reason = ExitSubReason::WaldBaseStopLoss;
            return self.finalize_open_position_close(
                ClosedTradeKind::ServerMarkPriceStopLoss,
                sl_trigger_price,
                snap.ts_local_ns,
                ledger,
            );
        }

        let quote = self.compute_canonical_exit_quote(snap)?;
        self.last_closed_wald_pct = quote.wald_pct;
        self.latch_defensive_floor_if_needed(quote.defensive_lock_active);

        let reached_target = match side {
            Side::Bid => ref_trade.0 >= quote.target_price.0,
            Side::Ask => ref_trade.0 > 0 && ref_trade.0 <= quote.target_price.0,
        };
        if !reached_target {
            return None;
        }

        if matches!(quote.role, ExitOrderRole::Tranche1Harvest50) {
            let _ = self.record_tranche1_harvest(quote.target_price);
            return None;
        }

        self.exit_peg_attempts = self.exit_peg_attempts.saturating_add(1);
        if quote.vac_level >= 2 || matches!(quote.sub_reason, ExitSubReason::LvdbsDeepVacuumSurf) {
            self.lvdbs_deep_surfs_count = self.lvdbs_deep_surfs_count.saturating_add(1);
            self.last_exit_sub_reason = ExitSubReason::LvdbsDeepVacuumSurf;
        } else {
            self.last_exit_sub_reason = quote.sub_reason;
        }
        self.finalize_open_position_close(
            ClosedTradeKind::MakerPassiveUnwind,
            quote.target_price,
            snap.ts_local_ns,
            ledger,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lob_shm::{BookLevel, MappedShmRegion};
    use crate::types::BoundedScore;

    #[test]
    fn test_hmac_sha256_bybit_v5_signature_and_quantum_floors() {
        let sig = sign_bybit_v5_hmac_sha256("test_secret", "1700000000000api_key5000");
        assert_eq!(sig.len(), 64);
        let floor_bid = defensive_zero_loss_maker_exit_price(Side::Bid, PriceTicks(8_500));
        assert!(floor_bid.0 >= 8_504);
        let floor_ask = defensive_zero_loss_maker_exit_price(Side::Ask, PriceTicks(8_500));
        assert!(floor_ask.0 <= 8_496);
    }

    #[test]
    fn test_moallemi_queue_depletion_velocity_shield() {
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.realized_vol_5m = ScaledBps(6_000);
        snap.factor_ofi = BoundedScore::new_clamped(-0.60);
        snap.factor_tfi = BoundedScore::new_clamped(-0.55);
        assert!(check_queue_depletion_velocity_abort(
            Side::Bid,
            LotQty(1_000),
            LotQty(300),
            &snap,
        ));
    }

    #[test]
    fn test_governor_12x2_loss_lab_62_of_62_and_2160_manifold() {
        let (mitigated_62, manifold_2160) =
            OnlineAdaptiveGovernor::audit_unified_62_losses_and_2160_manifold();
        assert_eq!(mitigated_62, 62);
        assert_eq!(manifold_2160, 2_160);
        let gov = OnlineAdaptiveGovernor::new();
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 105, 148, 0.24, 0.60)
            .is_ok());
        assert!(gov
            .evaluate_kinematic_energy_surface(1, Side::Bid, 220, 150, 0.22, 0.47)
            .is_err());
    }

    #[test]
    fn test_f07_wave_anchor_staircase_25bps_abort_hygiene_and_scar_json() {
        let mut gov = OnlineAdaptiveGovernor::new();
        let t0 = 1_700_000_000_000_000_000_i64;
        for step in 0..3_i64 {
            gov.observe_closed_trade_at_entry(
                3,
                Side::Bid,
                PriceTicks(8_200 + step * 5),
                ScaledBps(220),
                true,
                TimestampNs(t0 + step * 120_000_000_000),
            );
        }
        assert_eq!(gov.wave_anchor_ticks(3, Side::Bid), 8_200);
        gov.observe_closed_trade_at_entry(
            3,
            Side::Ask,
            PriceTicks(8_212),
            ScaledBps(45),
            true,
            TimestampNs(t0 + 400_000_000_000),
        );
        assert_eq!(gov.wave_anchor_ticks(3, Side::Bid), 8_200);
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_225, t0 + 600_000_000_000),
            Some("VETO_WAVE_STAIRCASE_25BPS")
        );

        for a in 0..4_i64 {
            gov.observe_entry_abort(
                3,
                Side::Bid,
                PriceTicks(8_205),
                TimestampNs(t0 + 700_000_000_000 + a * 1_000_000_000),
            );
        }
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            Some("VETO_ABORT_STREAK_BREATHER")
        );
        gov.clear_abort_streak(3, Side::Bid);
        assert_eq!(
            gov.audit_historical_entry_veto(3, Side::Bid, 8_205, t0 + 705_000_000_000),
            None
        );

        gov.observe_closed_trade_at_entry(
            4,
            Side::Ask,
            PriceTicks(22_400),
            ScaledBps(-1_200),
            false,
            TimestampNs(t0 + 800_000_000_000),
        );
        let scar_json = gov.to_scar_snapshot_json(TimestampNs(t0 + 800_000_000_000));
        let mut restored = OnlineAdaptiveGovernor::new();
        assert_eq!(
            restored.apply_scar_snapshot_json(&scar_json),
            Some(TimestampNs(t0 + 800_000_000_000))
        );
        assert_eq!(restored.wave_anchor_ticks(3, Side::Bid), 8_200);
        assert_eq!(restored.slot_direction_diagnostics(4, Side::Ask).1, 1);
    }

    #[test]
    fn test_f02_f06_preflight_sync_verified_avg_price_110_145_sl_and_live_adopt() {
        let avax = APPROVED_SPECS[3];
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Bid, "8.20333333"),
            Ok(PriceTicks(8_204))
        );
        assert_eq!(
            parse_verified_avg_price_ticks(avax, Side::Ask, "8.20366667"),
            Ok(PriceTicks(8_203))
        );

        let mut eng = SlotExecutionEngine::new(3);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = avax.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        if let OrderLifecycleState::PlacingEntryMaker { sl_trigger_price, .. } = eng.state {
            let sl_dist_bps = ((8_200 - sl_trigger_price.0) * 1_000_000) / 8_200;
            assert!((11_000..=14_500).contains(&sl_dist_bps));
        } else {
            assert_eq!(eng.slot_idx, 999);
        }

        snap.bids[0] = BookLevel { price: PriceTicks(8_204), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_205), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_204);
        snap.last_trade_price = PriceTicks(8_204);
        assert_eq!(
            eng.sync_preflight_l1_entry(&snap, 8).map(|p| p.0),
            Ok(PriceTicks(8_204))
        );
        assert!(eng.on_entry_order_resting(TimestampNs(1_030_000_000)).is_ok());
        assert!(eng
            .on_maker_entry_filled_verified(
                TimestampNs(2_000_000_000),
                Some(Side::Bid),
                Some(PriceTicks(8_206)),
                Some(LotQty(24)),
            )
            .is_ok());
        assert_eq!(eng.active_side(), Some(Side::Bid));

        let mut adopt_eng = SlotExecutionEngine::new(3);
        snap.mark_price = PriceTicks(8_100);
        snap.last_trade_price = PriceTicks(8_098);
        let adopted_sl = adopt_eng.adopt_live_open_position(
            Side::Bid,
            PriceTicks(8_200),
            LotQty(26),
            LotQty(13),
            true,
            PriceTicks(8_205),
            PriceTicks(8_098),
            TimestampNs(1_000_000_000),
            &snap,
        );
        assert_eq!(adopted_sl, Ok(PriceTicks(8_095)));
    }

    #[test]
    fn test_f03_closed_pnl_reconciler_eradicates_false_sl_and_catches_real_sl() {
        assert_eq!(parse_signed_decimal_1e8("-0.2845"), Ok(-28_450_000));
        let spec = APPROVED_SPECS[3];
        let mut eng = SlotExecutionEngine::new(3);
        let mut ledger = ParitySyncLedger::new(6_628_080_000);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = spec.symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_700_000_010_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_200), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_200);
        snap.last_trade_price = PriceTicks(8_200);

        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(eng
            .on_maker_entry_filled(TimestampNs(1_700_000_010_500_000_000))
            .is_ok());
        if let OrderLifecycleState::PositionOpenMakerExit {
            ref mut resting_exit_price,
            ..
        } = eng.state
        {
            *resting_exit_price = PriceTicks(8_205);
        }

        snap.mark_price = PriceTicks(8_195);
        let fb = eng.resolve_fallback_unverified_close(&snap, 2);
        assert!(fb.is_some());
        if let Some(fb_res) = fb {
            assert_eq!(fb_res.kind, ClosedTradeKind::MakerPassiveUnwind);
            assert_eq!(fb_res.verified_exit_price, PriceTicks(8_205));
        }

        let win_payload = serde_json::json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "AVAXUSDT",
                    "side": "Sell",
                    "orderType": "Limit",
                    "execType": "Trade",
                    "closedSize": "2.6",
                    "avgEntryPrice": "8.200",
                    "avgExitPrice": "8.207",
                    "closedPnl": "0.00968000",
                    "updatedTime": "1700000012000"
                }]
            }
        });
        let out1 = eng.reconcile_closed_pnl_payload(&win_payload, 2, false);
        if let ClosedPnlQueryOutcome::Verified(res1) = out1 {
            assert_eq!(res1.kind, ClosedTradeKind::MakerPassiveUnwind);
            let closed1 = eng.apply_verified_close_resolution(
                res1,
                TimestampNs(1_700_000_012_500_000_000),
                &mut ledger,
            );
            assert!(closed1.is_some());
            if let Some(s1) = closed1 {
                assert!(s1.is_maker_exit);
                assert_eq!(s1.net_pnl_1e8, 968_000);
            }
        } else {
            assert_eq!(eng.slot_idx, 999);
        }
    }

    #[test]
    fn test_f04_f05_f09_lvdbs_btc_unlock_exact_lot_consolidation_and_tear_free_shm() {
        let gov = OnlineAdaptiveGovernor::new();
        let mut ledger = ParitySyncLedger::new(6_628_080_000);

        let mut btc_eng = SlotExecutionEngine::new(0);
        let mut btc_snap = SymbolLobSnapshot::new_empty();
        btc_snap.symbol_id = APPROVED_SPECS[0].symbol_id;
        btc_snap.realized_vol_5m = ScaledBps(6_500);
        btc_snap.ts_local_ns = TimestampNs(1_000_000_000);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_000), qty: LotQty(100) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_001), qty: LotQty(100) };
        btc_snap.mark_price = PriceTicks(759_000);
        btc_snap.last_trade_price = PriceTicks(759_000);
        let btc_v = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(600),
        };
        assert!(btc_eng.try_arm_maker_entry(&btc_snap, btc_v, LotQty(1)).is_ok());
        assert!(btc_eng.on_maker_entry_filled(TimestampNs(2_000_000_000)).is_ok());

        btc_snap.ts_local_ns = TimestampNs(20_000_000_000);
        btc_snap.bids[0] = BookLevel { price: PriceTicks(759_333), qty: LotQty(100) };
        btc_snap.asks[0] = BookLevel { price: PriceTicks(759_334), qty: LotQty(100) };
        btc_snap.mark_price = PriceTicks(759_334);
        btc_snap.last_trade_price = PriceTicks(759_334);
        assert!(btc_eng
            .step_open_position(&btc_snap, &btc_snap, &gov, &mut ledger)
            .is_none());

        btc_snap.ts_local_ns = TimestampNs(125_000_000_000);
        let btc_closed = btc_eng.step_open_position(&btc_snap, &btc_snap, &gov, &mut ledger);
        assert!(btc_closed.is_some());
        if let Some(bs) = btc_closed {
            assert_eq!(bs.exit_price, PriceTicks(759_334));
            assert!(bs.net_scaled_bps.raw() >= 40);
            assert_eq!(ledger.closed_trades_count, 1);
        }

        let shm_path = MappedShmRegion::ephemeral_self_test_path("chat6_fsm_f09");
        let reg = MappedShmRegion::open_or_create(&shm_path, true);
        assert!(reg.is_ok());

        let mut eng = SlotExecutionEngine::new(3);
        let mut snap = SymbolLobSnapshot::new_empty();
        snap.symbol_id = APPROVED_SPECS[3].symbol_id;
        snap.realized_vol_5m = ScaledBps(7_000);
        snap.ts_local_ns = TimestampNs(1_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_198), qty: LotQty(300) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_199), qty: LotQty(250) };
        snap.mark_price = PriceTicks(8_198);
        snap.last_trade_price = PriceTicks(8_198);
        let verdict = crate::alpha_3factor::SignalVerdict::AuthorizeMakerEntry {
            side: Side::Bid,
            target_excursion_scaled_bps: ScaledBps(650),
        };
        assert!(eng.try_arm_maker_entry(&snap, verdict, LotQty(26)).is_ok());
        assert!(eng.on_maker_entry_filled(TimestampNs(1_500_000_000)).is_ok());

        snap.ts_local_ns = TimestampNs(10_000_000_000);
        snap.bids[0] = BookLevel { price: PriceTicks(8_201), qty: LotQty(400) };
        snap.asks[0] = BookLevel { price: PriceTicks(8_202), qty: LotQty(200) };
        snap.mark_price = PriceTicks(8_202);
        snap.last_trade_price = PriceTicks(8_202);
        assert!(eng.step_open_position(&snap, &snap, &gov, &mut ledger).is_none());
        assert_eq!(eng.tranche1_harvests_count, 1);
        assert_eq!(ledger.closed_trades_count, 1);

        if let Ok(ref r) = reg {
            eng.sync_to_shm_telemetry(r.arena());
            if let Ok(tel) = r.arena().telemetry(3) {
                let ts = tel.read_consistent_or_default(4);
                assert_eq!(ts.state_code, 4);
                assert!(ts.tranche1_harvested);
                assert_eq!(ts.open_lots, 13);
                assert_eq!(ts.initial_lots, 26);
            }
        }

        let ask_px = [8_203, 8_205, 8_208, 8_211];
        for (i, &p) in ask_px.iter().enumerate() {
            snap.bids[i] = BookLevel {
                price: PriceTicks(8_202 - i as i64),
                qty: LotQty(350),
            };
            snap.asks[i] = BookLevel { price: PriceTicks(p), qty: LotQty(200) };
        }
        snap.factor_ofi = BoundedScore::new_clamped(0.65);
        snap.factor_tfi = BoundedScore::new_clamped(0.75);
        snap.mark_price = PriceTicks(8_211);
        snap.last_trade_price = PriceTicks(8_211);

        let closed = eng.step_open_position(&snap, &snap, &gov, &mut ledger);
        assert!(closed.is_some());
        if let Some(summary) = closed {
            assert_eq!(summary.closed_lots, LotQty(26));
            assert_eq!(summary.legs_consolidated, 2);
            assert_eq!(summary.exit_price, PriceTicks(8_206));
            assert_eq!(summary.net_scaled_bps, ScaledBps(636));
            assert_eq!(summary.net_pnl_1e8, 1_356_966);
            assert_eq!(ledger.closed_trades_count, 2);
        }
        let _ = std::fs::remove_file(&shm_path);
    }
}
